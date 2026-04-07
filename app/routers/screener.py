from fastapi import APIRouter, Query
from typing import List
from app.services.screener import screener_service

router = APIRouter()

VN30 = ["VIC","VHM","HPG","TCB","VCB","ACB","MWG","VNM","FPT","SSI",
        "MBB","VPB","HDB","BCM","MSN","STB","CTG","BID","GAS","SAB",
        "VJC","PLX","POW","VRE","GVR"]

async def get_tickers_by_exchange(exchange: str) -> List[str]:
    try:
        import asyncio
        loop = asyncio.get_event_loop()
        def fetch():
            from vnstock import Listing
            df = Listing().symbols_by_exchange()
            if df is None or df.empty:
                return []
            filtered = df[
                (df['exchange'].str.upper() == exchange.upper()) &
                (df['type'] == 'stock')
            ]
            tickers = filtered['symbol'].str.upper().tolist()
            print(f"✅ {exchange}: {len(tickers)} mã")
            return tickers
        return await loop.run_in_executor(None, fetch)
    except Exception as e:
        print(f"get_tickers error {exchange}: {e}")
        return []

@router.get("/tickers/{exchange}")
async def get_exchange_tickers(exchange: str):
    ex = exchange.upper()
    if ex == "VN30":
        return {"exchange": "VN30", "tickers": VN30, "count": len(VN30)}
    tickers = await get_tickers_by_exchange(ex)
    return {"exchange": ex, "tickers": tickers, "count": len(tickers)}

@router.get("/scan")
async def scan_market(
    tickers: str = Query(default=",".join(VN30)),
    min_score: int = Query(default=5, ge=0, le=8),
    min_rs: float = Query(default=0.0),
):
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    results = await screener_service.run_screener(
        ticker_list, min_trend_score=min_score, min_rs=min_rs
    )
    return {
        "total":      len(ticker_list),
        "passed":     len(results),
        "results":    results,
        "scanned_at": __import__('datetime').datetime.now().isoformat(),
    }

@router.get("/analyze/{ticker}")
async def analyze_ticker(ticker: str):
    result = await screener_service._analyze_ticker(ticker.upper())
    if not result:
        return {"error": f"Không đủ dữ liệu cho {ticker}"}
    return result
