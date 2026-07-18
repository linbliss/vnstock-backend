import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from app.services.market_data import market_service
from app.services.alert_engine import run_alert_engine
from app.services.screener import screener_service, compute_market_rs_ratings
from app.services import ohlcv_store, user_store
from app.services.backfill import daily_update_scheduler
from app.routers import quotes, alerts, screener, admin, chart
from app.routers import auth, portfolio, watchlist_router, user_settings_router
from app.routers import shark

alert_task = None
daily_task = None
rs_task = None
shark_eod_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global alert_task, daily_task, rs_task, shark_eod_task
    ohlcv_store.init_db()
    user_store.init_db()
    # Cache tape trong phiên (bền vững) — init + dọn tape cũ > 5 ngày
    from app.services import tape_store
    tape_store.init_db()
    try:
        n = tape_store.cleanup(keep_days=5)
        if n:
            print(f"🧹 tape_store: dọn {n} tape cũ (>5 ngày)")
    except Exception as e:
        print(f"⚠️  tape_store.cleanup: {e}")
    await market_service.start()
    # DNSE feed (streaming market data) — chỉ bật khi có DNSE_API_KEY/SECRET; nếu không thì no-op
    from app.services import dnse_feed
    await dnse_feed.start()
    alert_task = asyncio.create_task(run_alert_engine())
    daily_task = asyncio.create_task(daily_update_scheduler())
    rs_task = asyncio.create_task(_rs_rating_scheduler())
    shark_eod_task = asyncio.create_task(_shark_eod_scheduler())
    # Worker nền làm mới tape Shark cho các mã đang xem (tách API khỏi request)
    from app.services import shark_monitor
    shark_refresh_task = asyncio.create_task(shark_monitor.refresh_loop())
    app.state.shark_refresh_task = shark_refresh_task
    # Load VNINDEX khi khởi động (không block, chạy background)
    asyncio.create_task(_warmup_vnindex())
    yield
    if alert_task:
        alert_task.cancel()
    if daily_task:
        daily_task.cancel()
    if rs_task:
        rs_task.cancel()
    if shark_eod_task:
        shark_eod_task.cancel()
    _srt = getattr(app.state, "shark_refresh_task", None)
    if _srt:
        _srt.cancel()
    from app.services import dnse_feed
    await dnse_feed.stop()
    await market_service.stop()


async def _shark_eod_scheduler():
    """Chốt điểm Shark cuối phiên cho MỌI mã trong watchlist.

    Sau khi thị trường đóng (~15:05), tính 1 lần điểm Shark cả phiên cho tất cả mã
    watchlist rồi lưu cache complete → ngoài giờ mở watchlist/Shark Action là có ngay,
    không phải tính lại trên hàng chục nghìn tick. Chỉ chạy 1 lần mỗi phiên."""
    from datetime import datetime
    from app.services import shark_monitor, user_store
    done_for = None
    await asyncio.sleep(60)   # chờ khởi động ổn định
    while True:
        try:
            now = datetime.now()
            after_close = now.weekday() < 5 and (now.hour * 60 + now.minute) >= 15 * 60 + 5
            today = now.strftime("%Y-%m-%d")
            if after_close and done_for != today:
                tickers = user_store.all_watchlist_tickers()
                ok = 0
                for tk in tickers:
                    try:
                        m = shark_monitor.compute_and_cache_signal(tk, force=True)
                        if not m.get("empty"):
                            ok += 1
                    except Exception as e:  # noqa: BLE001
                        print(f"⚠️  shark EOD {tk}: {type(e).__name__}: {e}", flush=True)
                    await asyncio.sleep(0.3)   # nhẹ tay, tránh dội nguồn dữ liệu
                done_for = today
                print(f"🦈 Shark EOD: đã chốt điểm {ok}/{len(tickers)} mã watchlist", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"⚠️  Shark EOD scheduler: {e}", flush=True)
        await asyncio.sleep(600)   # kiểm tra mỗi 10'

async def _warmup_vnindex():
    """Load VNINDEX data ngay khi server start, không block lifespan."""
    await asyncio.sleep(5)   # chờ market_service start xong, rate limiter ổn định
    try:
        await screener_service._ensure_index_data()
        rows = len(screener_service._index_data) if screener_service._index_data is not None else 0
        print(f"🔥 Warmup VNINDEX: {rows} rows")
    except Exception as e:
        print(f"⚠️  Warmup VNINDEX failed: {e}")


async def _rs_rating_scheduler():
    """Nightly batch job: compute RS Ratings percentile cho toàn thị trường.
    - Chạy ngay khi startup nếu data stale (>24h hoặc chưa có)
    - Sau đó lặp lại mỗi 24h (chạy lúc ~18:00 sau khi thị trường đóng cửa)
    """
    await asyncio.sleep(30)  # chờ warmup VNINDEX xong
    while True:
        try:
            if ohlcv_store.is_rs_ratings_stale():
                print("📊 RS Rating batch job starting...")
                count = await compute_market_rs_ratings()
                print(f"📊 RS Rating batch job done: {count} stocks ranked")
        except Exception as e:
            print(f"⚠️  RS Rating batch job failed: {e}")
        # Sleep 24h rồi chạy lại
        await asyncio.sleep(24 * 3600)

app = FastAPI(title="VN Stock API", version="0.3.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(quotes.router,               prefix="/api/quotes",      tags=["quotes"])
app.include_router(alerts.router,               prefix="/api/alerts",      tags=["alerts"])
app.include_router(screener.router,             prefix="/api/screener",    tags=["screener"])
app.include_router(admin.router,                prefix="/api/admin",       tags=["admin"])
app.include_router(chart.router,                prefix="/api/chart",       tags=["chart"])
app.include_router(auth.router,                 prefix="/api/auth",        tags=["auth"])
app.include_router(portfolio.router,            prefix="/api/portfolio",   tags=["portfolio"])
app.include_router(watchlist_router.router,     prefix="/api/watchlists",  tags=["watchlists"])
app.include_router(user_settings_router.router, prefix="/api/user",        tags=["user"])
app.include_router(shark.router)   # đã tự set prefix "/api/shark"

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/api/status")
async def api_status():
    # "/" giờ phục vụ frontend web (index.html), nên status chuyển sang /api/status
    from app.services import dnse_client, dnse_feed
    return {"status": "running", "version": "0.3.0",
            "dnse": dnse_client.enabled(),   # True = đã cấu hình key DNSE → dùng DNSE
            "dnse_ws": dnse_feed.stats(),    # kiểm chứng WS: connected/authenticated/ticks
            "fireant": bool(__import__("os").environ.get("FIREANT_TOKEN", "").strip())}


@app.get("/tickers/{exchange}")
async def get_tickers(exchange: str):
    ex = (exchange or "").strip().upper()
    if ex not in {"VN30", "HOSE", "HNX", "UPCOM"}:
        raise HTTPException(status_code=400, detail="exchange must be one of: VN30, HOSE, HNX, UPCOM")

    if ex == "VN30":
        tickers = [
            "VIC", "VHM", "HPG", "TCB", "VCB",
            "ACB", "MWG", "VNM", "FPT", "SSI",
            "MBB", "VPB", "HDB", "BCM", "MSN",
            "STB", "CTG", "BID", "GAS", "SAB",
            "VJC", "PLX", "POW", "VRE", "GVR",
        ]
        return {"exchange": ex, "tickers": tickers, "count": len(tickers)}

    # 1) stock_list trong SQLite — dữ liệu CỤC BỘ, KHÔNG đặt sau cổng use_dnse()
    saved = ohlcv_store.get_tickers_by_exchange(ex)
    if saved:
        return {"exchange": ex, "tickers": saved, "count": len(saved)}

    # 2) DNSE (nếu có key + REST dùng được)
    try:
        from app.services import dnse_client, data_source
        if data_source.use_dnse("ticker_list"):
            import asyncio as _a0
            tks = await _a0.get_event_loop().run_in_executor(
                None, dnse_client.get_tickers_by_exchange, ex)
            if tks:
                return {"exchange": ex, "tickers": tks, "count": len(tks)}
    except Exception as _e:  # noqa: BLE001
        print(f"⚠️  tickers {ex} DNSE: {type(_e).__name__}: {_e}", flush=True)

    try:
        def fetch():
            from vnstock import Listing

            df = Listing().symbols_by_exchange()
            if df is None or df.empty:
                return []

            # Debug columns + head + unique exchange values (Railway logs)
            try:
                print("DEBUG symbols_by_exchange columns:", df.columns.tolist())
                print("DEBUG symbols_by_exchange head:\n", df.head())
                if "exchange" in df.columns:
                    print("DEBUG symbols_by_exchange exchange unique:", df["exchange"].dropna().astype(str).str.upper().unique().tolist())
            except Exception as _e:
                print("DEBUG symbols_by_exchange logging failed:", _e)

            if "exchange" not in df.columns or "type" not in df.columns or "symbol" not in df.columns:
                missing = [c for c in ["exchange", "type", "symbol"] if c not in df.columns]
                raise HTTPException(status_code=502, detail=f"symbols_by_exchange missing columns: {missing}")

            filtered = df[
                (df["exchange"].astype(str).str.upper() == ex.upper())
                & (df["type"] == "stock")
            ]
            return filtered["symbol"].dropna().astype(str).str.upper().tolist()

        await market_service._limiter.acquire()   # bảo vệ quota vnai
        import asyncio as _aio
        tickers = await _aio.get_event_loop().run_in_executor(None, fetch)

        # unique + stable
        seen = set()
        tickers_unique = []
        for t in tickers:
            if t not in seen:
                seen.add(t)
                tickers_unique.append(t)

        return {"exchange": ex, "tickers": tickers_unique, "count": len(tickers_unique)}
    except HTTPException:
        raise
    # BaseException để nuốt SystemExit từ vnai.beam.quota
    except BaseException as e:
        raise HTTPException(status_code=502, detail=f"failed to fetch symbols for {ex}: {type(e).__name__}: {e}")


# ── Phục vụ frontend web (SPA HashRouter) ──
# KHÔNG mount catch-all "/" — nó nuốt mất redirect trailing-slash của FastAPI
# (vd /api/watchlists → /api/watchlists/) khiến API trả 404. HashRouter chỉ cần
# "/" (index.html) + "/assets/*"; mọi route nội bộ đi qua #/ phía client.
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
if os.path.isdir("webdist"):
    app.mount("/assets", StaticFiles(directory="webdist/assets"), name="assets")

    @app.get("/", include_in_schema=False)
    async def _spa_index():
        return FileResponse("webdist/index.html")

    @app.get("/favicon.png", include_in_schema=False)
    async def _favicon():
        return FileResponse("webdist/favicon.png")

    print("🌐 Serving web frontend from ./webdist")
else:
    print("ℹ️  webdist/ không có — chạy ở chế độ API-only")
