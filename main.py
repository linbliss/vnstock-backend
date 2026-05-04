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
from app.services import ohlcv_store
from app.services.backfill import daily_update_scheduler
from app.routers import quotes, alerts, screener, admin, chart

alert_task = None
daily_task = None
rs_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global alert_task, daily_task, rs_task
    ohlcv_store.init_db()
    await market_service.start()
    alert_task = asyncio.create_task(run_alert_engine())
    daily_task = asyncio.create_task(daily_update_scheduler())
    rs_task = asyncio.create_task(_rs_rating_scheduler())
    # Load VNINDEX khi khởi động (không block, chạy background)
    asyncio.create_task(_warmup_vnindex())
    yield
    if alert_task:
        alert_task.cancel()
    if daily_task:
        daily_task.cancel()
    if rs_task:
        rs_task.cancel()
    await market_service.stop()

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
app.include_router(quotes.router,   prefix="/api/quotes",   tags=["quotes"])
app.include_router(alerts.router,   prefix="/api/alerts",   tags=["alerts"])
app.include_router(screener.router, prefix="/api/screener", tags=["screener"])
app.include_router(admin.router,    prefix="/api/admin",    tags=["admin"])
app.include_router(chart.router,    prefix="/api/chart",    tags=["chart"])

@app.get("/")
async def root():
    return {"status": "running", "version": "0.3.0"}

@app.get("/health")
async def health():
    return {"status": "ok"}


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
