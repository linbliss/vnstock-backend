import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from app.services.market_data import market_service
from app.services.alert_engine import run_alert_engine
from app.services.screener import screener_service
from app.routers import quotes, alerts, screener

alert_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global alert_task
    await market_service.start()
    alert_task = asyncio.create_task(run_alert_engine())
    # Load VNINDEX khi khởi động (không block, chạy background)
    asyncio.create_task(_warmup_vnindex())
    yield
    if alert_task:
        alert_task.cancel()
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

        tickers = fetch()

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
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"failed to fetch symbols for {ex}: {e}")
