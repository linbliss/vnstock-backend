from fastapi import APIRouter, Query
from typing import List, Dict, Any
import asyncio
from app.services.screener import screener_service

# Cache fundamental data theo session để tránh gọi vnstocks nhiều lần
_fundamental_cache: Dict[str, Any] = {}

def _fetch_fundamental_sync(ticker: str) -> Dict[str, Any]:
    """Lấy EPS & ROE theo quý từ vnstocks (chạy trong executor)."""
    empty = {'ticker': ticker, 'eps': [], 'roe': [], 'eps_growth': False,
             'roe_latest': 0.0, 'roe_growth': False}
    try:
        from vnstock import Vnstock
        stock = Vnstock().stock(symbol=ticker, source='VCI')
        df = stock.finance.ratio(period='quarterly', lang='en', dropna=False)
        if df is None or df.empty:
            return empty

        # Flatten multi-level columns
        df.columns = ['_'.join(str(c) for c in col).strip() if isinstance(col, tuple) else str(col)
                      for col in df.columns]

        # Tìm cột EPS và ROE
        eps_col = next((c for c in df.columns if 'eps' in c.lower() and 'vnd' in c.lower()), None) or \
                  next((c for c in df.columns if 'eps' in c.lower()), None)
        roe_col = next((c for c in df.columns if 'roe' in c.lower()), None)

        eps_vals, roe_vals = [], []

        if eps_col:
            raw = df[eps_col].dropna()
            raw = raw[raw != 0]
            eps_vals = [round(float(v), 0) for v in raw.tail(8).tolist()]

        if roe_col:
            raw = df[roe_col].dropna()
            raw = raw[raw != 0]
            # ROE từ vnstocks là decimal (0.246 = 24.6%) → nhân 100
            vals = raw.tail(8).tolist()
            roe_vals = [round(float(v) * 100 if abs(float(v)) < 5 else float(v), 2) for v in vals]

        # EPS tăng trưởng: ít nhất 2 quý gần nhất đều tăng
        eps_growth = (len(eps_vals) >= 2 and all(
            eps_vals[i] > eps_vals[i-1] for i in range(max(1, len(eps_vals)-2), len(eps_vals))
        ))
        roe_growth = (len(roe_vals) >= 2 and roe_vals[-1] > roe_vals[-2])
        roe_latest = roe_vals[-1] if roe_vals else 0.0

        return {
            'ticker':     ticker,
            'eps':        eps_vals,
            'roe':        roe_vals,
            'eps_growth': eps_growth,
            'roe_latest': roe_latest,
            'roe_growth': roe_growth,
        }
    except Exception as e:
        print(f"Fundamental error {ticker}: {e}")
        return empty

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

@router.get("/fundamental/{ticker}")
async def get_fundamental(ticker: str):
    """Lấy EPS & ROE theo quý cho 1 mã (cache session)."""
    sym = ticker.upper()
    if sym not in _fundamental_cache:
        loop = asyncio.get_event_loop()
        _fundamental_cache[sym] = await loop.run_in_executor(None, _fetch_fundamental_sync, sym)
    return _fundamental_cache[sym]

@router.post("/fundamental/batch")
async def get_fundamental_batch(tickers: List[str]):
    """Lấy EPS & ROE cho nhiều mã, chạy song song (max 5 concurrent)."""
    loop = asyncio.get_event_loop()
    sem  = asyncio.Semaphore(5)

    async def fetch_one(sym: str):
        if sym in _fundamental_cache:
            return _fundamental_cache[sym]
        async with sem:
            result = await loop.run_in_executor(None, _fetch_fundamental_sync, sym)
            _fundamental_cache[sym] = result
            return result

    results = await asyncio.gather(*[fetch_one(t.upper()) for t in tickers])
    return {'results': list(results)}

@router.delete("/fundamental/cache")
async def clear_fundamental_cache():
    """Xoá cache fundamental (dùng khi muốn refresh dữ liệu)."""
    _fundamental_cache.clear()
    return {'cleared': True}

@router.get("/debug/vnindex")
async def debug_vnindex():
    """Debug endpoint: kiểm tra trạng thái VNINDEX data"""
    from datetime import datetime, timedelta
    import asyncio
    now = datetime.now()
    end   = now.strftime("%Y-%m-%d")
    start = (now - timedelta(days=30)).strftime("%Y-%m-%d")  # chỉ lấy 30 ngày để test nhanh
    loop  = asyncio.get_event_loop()
    results = {}
    for symbol in ("VNINDEX", "VN-INDEX", "VNI", "^VNINDEX"):
        try:
            df = await loop.run_in_executor(None, screener_service._fetch_history, symbol, start, end)
            results[symbol] = {
                "rows": len(df) if df is not None else 0,
                "ok": df is not None and len(df) > 0,
                "columns": df.columns.tolist() if df is not None and not df.empty else [],
                "last_close": float(df['close'].iloc[-1]) if df is not None and not df.empty else None,
            }
        except Exception as e:
            results[symbol] = {"ok": False, "error": str(e)}
    return {
        "index_loaded": screener_service._index_data is not None,
        "index_rows": len(screener_service._index_data) if screener_service._index_data is not None else 0,
        "symbol_tests": results,
    }
