from fastapi import APIRouter, Query
from typing import List, Dict, Any
import asyncio
from app.services.screener import screener_service

# Cache fundamental data theo session để tránh gọi vnstocks nhiều lần
_fundamental_cache: Dict[str, Any] = {}

def _fetch_fundamental_sync(ticker: str) -> Dict[str, Any]:
    """Lấy EPS & ROE theo quý từ vnstocks (chạy trong executor).

    vnstocks trả về DataFrame GIẢM DẦN (mới nhất ở đầu).
    Cần lấy head() rồi đảo ngược để có thứ tự tăng dần (cũ→mới).
    """
    import math
    empty = {'ticker': ticker, 'eps': [], 'roe': [], 'quarters': [],
             'eps_growth': False, 'roe_latest': 0.0, 'roe_growth': False}
    try:
        from vnstock import Vnstock
        import pandas as pd
        stock = Vnstock().stock(symbol=ticker, source='VCI')
        df = stock.finance.ratio(period='quarterly', lang='en', dropna=False)
        if df is None or df.empty:
            return empty

        # Flatten multi-level columns
        flat_cols = ['_'.join(str(c) for c in col).strip() if isinstance(col, tuple) else str(col)
                     for col in df.columns]
        df.columns = flat_cols

        # Tìm các cột cần thiết
        eps_col  = next((c for c in flat_cols if 'eps' in c.lower() and 'vnd' in c.lower()), None) or \
                   next((c for c in flat_cols if 'eps' in c.lower()), None)
        roe_col  = next((c for c in flat_cols if 'roe' in c.lower()), None)
        year_col = next((c for c in flat_cols if 'yearreport' in c.lower() or 'year' in c.lower()), None)
        len_col  = next((c for c in flat_cols if 'lengthreport' in c.lower() or 'length' in c.lower()), None)

        # DataFrame là GIẢM DẦN (mới nhất ở hàng đầu)
        # Lấy 20 hàng (cần thêm để tính TTM: 8 TTM points cần 11 standalone quarters)
        recent = df.head(20).iloc[::-1].reset_index(drop=True)

        # ── Bước 1: Thu thập EPS/ROE standalone từng quý (cũ→mới) ──
        raw_eps, raw_roe, raw_quarters = [], [], []

        for _, row in recent.iterrows():
            eps_raw = row.get(eps_col) if eps_col else None
            roe_raw = row.get(roe_col) if roe_col else None
            year    = int(row.get(year_col, 0)) if year_col else 0
            quarter = int(row.get(len_col,  0)) if len_col  else 0

            if eps_raw is None or (isinstance(eps_raw, float) and math.isnan(eps_raw)):
                continue

            eps_f = float(eps_raw)

            roe_f = 0.0
            if roe_raw is not None and not (isinstance(roe_raw, float) and math.isnan(roe_raw)):
                roe_f = float(roe_raw)
                # vnstocks trả ROE dạng decimal (0.246 = 24.6%) → nhân 100
                if abs(roe_f) < 5:
                    roe_f = round(roe_f * 100, 2)

            raw_eps.append(eps_f)
            raw_roe.append(round(roe_f, 2))
            raw_quarters.append({'year': year, 'quarter': quarter})

        # ── Bước 2: Tính TTM EPS (Trailing Twelve Months = tổng 4 quý liên tiếp) ──
        # FireAnt / Vietstock hiển thị TTM EPS chứ không phải EPS quý đơn lẻ.
        # TTM tại quý i = raw_eps[i-3] + raw_eps[i-2] + raw_eps[i-1] + raw_eps[i]
        eps_ttm, roe_vals, quarters = [], [], []
        for i in range(len(raw_eps)):
            if i < 3:
                continue   # cần ít nhất 4 quý để tính TTM
            ttm = round(sum(raw_eps[i - 3: i + 1]), 0)
            eps_ttm.append(ttm)
            roe_vals.append(raw_roe[i])
            quarters.append(raw_quarters[i])

        # Giữ 8 quý TTM gần nhất
        eps_ttm  = eps_ttm[-8:]
        roe_vals = roe_vals[-8:]
        quarters = quarters[-8:]

        # ── Bước 3: Kiểm tra tăng trưởng theo TTM EPS ──
        def check_growth(vals: list, n: int) -> bool:
            if len(vals) < n + 1:
                return False
            return all(vals[i] > vals[i - 1] for i in range(len(vals) - n, len(vals)))

        eps_growth = check_growth(eps_ttm, 2)
        roe_growth = check_growth(roe_vals, 1)
        roe_latest = roe_vals[-1] if roe_vals else 0.0

        return {
            'ticker':     ticker,
            'eps':        eps_ttm,      # TTM EPS — khớp với FireAnt / Vietstock
            'roe':        roe_vals,
            'quarters':   quarters,     # [{"year":2025,"quarter":4}, ...]  cũ→mới
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
