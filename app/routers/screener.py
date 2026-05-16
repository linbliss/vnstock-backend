from fastapi import APIRouter, Query
from typing import List, Dict, Any
import asyncio
import time
from app.services.screener import screener_service
from app.services import ohlcv_store

# ── In-memory cache cho exchange ticker lists (TTL 24h) ──
_ticker_list_cache: Dict[str, Dict[str, Any]] = {}
_TICKER_CACHE_TTL = 86400  # 24 giờ


def _fetch_fundamental_via_api(ticker: str) -> Dict[str, Any]:
    """L��y EPS & ROE theo quý.
    Ưu tiên: FireAnt API (có token) → vnstock fallback.
    """
    import os
    import math
    import requests

    empty = {'ticker': ticker, 'eps': [], 'roe': [], 'quarters': [],
             'eps_growth': False, 'roe_latest': 0.0, 'roe_growth': False}

    # ── 1. FireAnt API (ưu tiên — reliable, có token trên server) ──
    fireant_token = os.environ.get("FIREANT_TOKEN", "").strip()
    if fireant_token:
        try:
            headers = {"Authorization": f"Bearer {fireant_token}"}

            # 1a. Lấy sharesOutstanding từ fundamental
            fund_resp = requests.get(
                f"https://restv2.fireant.vn/symbols/{ticker}/fundamental",
                timeout=15, headers=headers,
            )
            shares = 0
            roe_latest = 0.0
            fund_data = {}
            if fund_resp.status_code == 200:
                fund_data = fund_resp.json()
                shares = fund_data.get("sharesOutstanding", 0) or 0

            # 1a2. Lấy ROE từ financial-indicators — thu thập TẤT CẢ quý, không chỉ 1 điểm
            # API trả list [{shortName, period, value}, ...] mỗi item = 1 chỉ số 1 quý
            roe_by_period: dict = {}   # "Q4/2024" → roe_value
            try:
                ind_resp = requests.get(
                    f"https://restv2.fireant.vn/symbols/{ticker}/financial-indicators?type=quarterly&count=20",
                    timeout=15, headers=headers,
                )
                if ind_resp.status_code == 200:
                    indicators = ind_resp.json()
                    if isinstance(indicators, list):
                        for item in indicators:
                            if not isinstance(item, dict):
                                continue
                            sn = (item.get("shortName") or "").upper().replace(" ", "")
                            if sn in ("ROE", "ROE(%)"):
                                period = item.get("period") or item.get("quarter") or ""
                                val = item.get("value")
                                try:
                                    fval = float(val or 0)
                                    if period:
                                        roe_by_period[str(period)] = fval
                                    # Cập nhật roe_latest = quý mới nhất (item đầu tiên)
                                    if roe_latest == 0.0 and fval != 0:
                                        roe_latest = fval
                                except (ValueError, TypeError):
                                    pass
            except Exception as e_ind:
                print(f"⚠️  Fundamental {ticker} indicators error: {e_ind}", flush=True)

            n_roe = len(roe_by_period)
            print(f"📊 Fundamental {ticker}: roe_latest={roe_latest:.2f}, roe_by_period={n_roe} quý", flush=True)

            # 1b. Lấy quarterly NetProfit từ financial-reports
            report_resp = requests.get(
                f"https://restv2.fireant.vn/symbols/{ticker}/financial-reports?type=quarter&limit=20",
                timeout=15, headers=headers,
            )
            if report_resp.status_code == 200:
                report_data = report_resp.json()
                result = _parse_fireant_reports(ticker, report_data, shares, roe_latest, roe_by_period)
                if result:
                    print(f"✅ Fundamental {ticker} via FireAnt: OK", flush=True)
                    return result

            # 1c. Nếu financial-reports không có data, dùng snapshot
            if fund_resp.status_code == 200 and fund_data.get("eps"):
                eps_val = float(fund_data["eps"])
                # Chỉ có giá trị mới nhất → tạo 1-point array
                result = {
                    'ticker': ticker,
                    'eps': [round(eps_val, 0)],
                    'roe': [round(roe_latest, 2)] if roe_latest else [0.0],
                    'quarters': [{'year': 0, 'quarter': 0}],
                    'eps_growth': False,
                    'roe_latest': round(roe_latest, 2),
                    'roe_growth': False,
                }
                print(f"✅ Fundamental {ticker} via FireAnt (snapshot): eps={eps_val:.0f}", flush=True)
                return result

            print(f"⚠️  Fundamental {ticker} via FireAnt: no usable data", flush=True)
        except Exception as e:
            print(f"⚠️  Fundamental {ticker} via FireAnt: {type(e).__name__}: {e}", flush=True)

    # ── 2. Fallback: vnstock ──
    try:
        from vnstock import Vnstock
        for source in ('KBS', 'VCI'):
            try:
                stock = Vnstock().stock(symbol=ticker, source=source)
                if source == 'VCI':
                    df = stock.finance.ratio(period='quarterly', lang='en', dropna=False)
                else:
                    df = stock.finance.ratio(period='quarterly')
                if df is None or df.empty:
                    continue
                result = _parse_vnstock_fundamental(ticker, df)
                if result:
                    print(f"✅ Fundamental {ticker} via vnstock/{source}: OK", flush=True)
                    return result
            except BaseException as e:
                print(f"⚠️  Fundamental {ticker} via vnstock/{source}: {type(e).__name__}: {e}", flush=True)
    except ImportError:
        pass

    print(f"❌ Fundamental {ticker}: all sources failed", flush=True)
    return empty


def _parse_fireant_reports(ticker: str, report_data: Any, shares: float, roe_latest: float, roe_by_period: dict = None) -> Dict[str, Any]:
    """Parse FireAnt /financial-reports response → fundamental dict.

    Format:
      {
        "symbol": "VIC",
        "columns": ["Name", "Symbol", "Q4/2024", "Q1/2025", ...],
        "rows": [
          ["LNST", "NetProfit", 123456, 234567, ...],
          ["LNST (CĐ cty mẹ)", "NetProfit_PCSH", 111111, 222222, ...],
          ...
        ]
      }

    Ưu tiên NetProfit_PCSH (lợi nhuận thuộc về cổ đông công ty mẹ).
    EPS = NetProfit_PCSH / sharesOutstanding (đơn vị triệu → VND).
    """
    import math

    if not isinstance(report_data, dict):
        return None
    columns = report_data.get("columns", [])
    rows = report_data.get("rows", [])
    if not columns or not rows or len(columns) < 3:
        return None

    # Tìm row NetProfit_PCSH (ưu tiên) hoặc NetProfit
    net_profit_row = None
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 3:
            continue
        symbol_col = str(row[1]).strip() if len(row) > 1 else ""
        if symbol_col == "NetProfit_PCSH":
            net_profit_row = row
            break
    if net_profit_row is None:
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) < 3:
                continue
            symbol_col = str(row[1]).strip() if len(row) > 1 else ""
            if symbol_col == "NetProfit":
                net_profit_row = row
                break
    if net_profit_row is None:
        return None

    # Parse quarter labels từ columns (skip "Name", "Symbol")
    # Format: "Q4/2024", "Q1/2025", ...
    raw_eps, raw_roe, raw_quarters = [], [], []
    for i in range(2, min(len(columns), len(net_profit_row))):
        col_label = str(columns[i])  # e.g. "Q4/2024"
        val = net_profit_row[i]

        # Parse quarter label
        year, quarter = 0, 0
        try:
            parts = col_label.split("/")
            if len(parts) == 2 and parts[0].startswith("Q"):
                quarter = int(parts[0][1:])
                year = int(parts[1])
        except (ValueError, IndexError):
            pass

        # Parse net profit value (triệu VND)
        try:
            net_profit = float(val) if val is not None else 0.0
            if math.isnan(net_profit):
                net_profit = 0.0
        except (ValueError, TypeError):
            net_profit = 0.0

        # EPS = NetProfit (VND) / sharesOutstanding
        # FireAnt trả NetProfit đơn vị VND (không phải triệu)
        if shares and shares > 0:
            eps_q = round(net_profit / shares, 0)
        else:
            eps_q = 0.0

        # Gắn ROE thực từ roe_by_period nếu có (ưu tiên dữ liệu thực vs ước lượng)
        roe_q = 0.0
        if roe_by_period:
            # col_label format "Q4/2024" → khớp trực tiếp với key từ FireAnt
            roe_q = roe_by_period.get(col_label, 0.0)

        raw_eps.append(eps_q)
        raw_roe.append(round(roe_q, 2))
        raw_quarters.append({"year": year, "quarter": quarter})

    if not raw_eps:
        return None

    # Nếu ít hơn 4 quý, trả về raw data (không tính TTM)
    if len(raw_eps) < 4:
        # Nếu không có ROE thực, điền bằng roe_latest
        filled_roe = [r if r != 0.0 else round(roe_latest, 2) for r in raw_roe]
        return {
            "ticker": ticker,
            "eps": raw_eps,
            "roe": filled_roe,
            "quarters": raw_quarters,
            "eps_growth": False,
            "roe_latest": round(roe_latest, 2),
            "roe_growth": False,
        }

    # Tính TTM EPS; ROE dùng giá trị thực từ roe_by_period nếu có,
    # fallback ước lượng từ roe_latest nếu không có dữ liệu thực.
    has_real_roe = any(r != 0.0 for r in raw_roe)

    if has_real_roe:
        # Trường hợp có ROE thực từng quý → dùng trực tiếp (không cần ước lượng)
        result = _compute_ttm(ticker, raw_eps, raw_roe, raw_quarters)
    else:
        # Fallback: ước lượng ROE từ roe_latest snapshot
        result = _compute_ttm(ticker, raw_eps, [0.0] * len(raw_eps), raw_quarters)
        if result:
            eps_ttm_list = result["eps"]
            if roe_latest and roe_latest > 0 and eps_ttm_list and eps_ttm_list[-1] != 0:
                equity_ps = abs(eps_ttm_list[-1]) / (roe_latest / 100.0)
                roe_estimated = [round(e / equity_ps * 100, 2) if equity_ps > 0 else 0.0 for e in eps_ttm_list]
                result["roe"] = roe_estimated
                result["roe_latest"] = roe_estimated[-1] if roe_estimated else round(roe_latest, 2)
                if len(roe_estimated) >= 2:
                    result["roe_growth"] = roe_estimated[-1] > roe_estimated[-2]
            else:
                result["roe"] = [round(roe_latest, 2)] * len(eps_ttm_list)
                result["roe_latest"] = round(roe_latest, 2)

    return result


def _parse_fireant_fundamental(ticker: str, data: Any) -> Dict[str, Any]:
    """Parse FireAnt /symbols/{ticker}/fundamental response.
    FireAnt trả JSON object với các trường quarterly data.
    Cần khám phá format → log keys nếu không match.
    """
    import math

    # FireAnt trả về list hoặc dict tùy endpoint
    if isinstance(data, dict):
        # Thử extract quarterly EPS/ROE từ dict fields
        # FireAnt /fundamental trả EPS & ROE trực tiếp dạng số (giá trị mới nhất)
        # Cần dùng endpoint khác cho quarterly time series
        return None

    if isinstance(data, list) and data:
        raw_eps, raw_roe, raw_quarters = [], [], []
        for row in data:
            if not isinstance(row, dict):
                continue
            # Tìm EPS/ROE fields (FireAnt dùng nhiều tên khác nhau)
            eps_raw = None
            roe_raw = None
            year = 0
            quarter = 0
            for k, v in row.items():
                kl = k.lower()
                if 'eps' in kl and eps_raw is None:
                    eps_raw = v
                if 'roe' in kl and roe_raw is None:
                    roe_raw = v
                if kl in ('year', 'yearreport', 'nam'):
                    year = int(v) if v else 0
                if kl in ('quarter', 'lengthreport', 'quy', 'period'):
                    quarter = int(v) if v else 0

            if eps_raw is None:
                continue
            try:
                eps_f = float(eps_raw)
            except (ValueError, TypeError):
                continue
            roe_f = 0.0
            if roe_raw is not None:
                try:
                    roe_f = float(roe_raw)
                    if 0 < abs(roe_f) < 5:
                        roe_f = round(roe_f * 100, 2)
                except (ValueError, TypeError):
                    pass
            raw_eps.append(eps_f)
            raw_roe.append(round(roe_f, 2))
            raw_quarters.append({'year': year, 'quarter': quarter})

        if raw_eps:
            return _compute_ttm(ticker, raw_eps, raw_roe, raw_quarters)

    return None


def _parse_vnstock_fundamental(ticker: str, df) -> Dict[str, Any]:
    """Parse vnstock finance.ratio DataFrame → fundamental dict."""
    import math
    flat_cols = ['_'.join(str(c) for c in col).strip() if isinstance(col, tuple) else str(col)
                 for col in df.columns]
    df.columns = flat_cols
    eps_col  = next((c for c in flat_cols if 'eps' in c.lower() and 'vnd' in c.lower()), None) or \
               next((c for c in flat_cols if 'eps' in c.lower()), None)
    roe_col  = next((c for c in flat_cols if 'roe' in c.lower()), None)
    year_col = next((c for c in flat_cols if 'yearreport' in c.lower() or 'year' in c.lower()), None)
    len_col  = next((c for c in flat_cols if 'lengthreport' in c.lower() or 'length' in c.lower()), None)

    if not eps_col:
        return None

    recent = df.head(20).iloc[::-1].reset_index(drop=True)
    raw_eps, raw_roe, raw_quarters = [], [], []
    for _, row in recent.iterrows():
        eps_raw = row.get(eps_col)
        roe_raw = row.get(roe_col) if roe_col else None
        year    = int(row.get(year_col, 0)) if year_col else 0
        quarter = int(row.get(len_col,  0)) if len_col  else 0
        if eps_raw is None or (isinstance(eps_raw, float) and math.isnan(eps_raw)):
            continue
        eps_f = float(eps_raw)
        roe_f = 0.0
        if roe_raw is not None and not (isinstance(roe_raw, float) and math.isnan(roe_raw)):
            roe_f = float(roe_raw)
            if abs(roe_f) < 5:
                roe_f = round(roe_f * 100, 2)
        raw_eps.append(eps_f)
        raw_roe.append(round(roe_f, 2))
        raw_quarters.append({'year': year, 'quarter': quarter})

    return _compute_ttm(ticker, raw_eps, raw_roe, raw_quarters)


def _compute_ttm(ticker: str, raw_eps: list, raw_roe: list, raw_quarters: list) -> Dict[str, Any]:
    """Tính TTM EPS và growth từ raw quarterly data."""
    if not raw_eps or len(raw_eps) < 4:
        return None

    eps_ttm, roe_vals, quarters = [], [], []
    for i in range(len(raw_eps)):
        if i < 3:
            continue
        ttm = round(sum(raw_eps[i - 3: i + 1]), 0)
        eps_ttm.append(ttm)
        roe_vals.append(raw_roe[i] if i < len(raw_roe) else 0.0)
        quarters.append(raw_quarters[i] if i < len(raw_quarters) else {'year': 0, 'quarter': 0})

    eps_ttm  = eps_ttm[-8:]
    roe_vals = roe_vals[-8:]
    quarters = quarters[-8:]

    def check_growth(vals: list, n: int) -> bool:
        if len(vals) < n + 1:
            return False
        return all(vals[i] > vals[i - 1] for i in range(len(vals) - n, len(vals)))

    return {
        'ticker':     ticker,
        'eps':        eps_ttm,
        'roe':        roe_vals,
        'quarters':   quarters,
        'eps_growth': check_growth(eps_ttm, 2),
        'roe_latest': roe_vals[-1] if roe_vals else 0.0,
        'roe_growth': check_growth(roe_vals, 1),
    }


router = APIRouter()

VN30 = ["VIC","VHM","HPG","TCB","VCB","ACB","MWG","VNM","FPT","SSI",
        "MBB","VPB","HDB","BCM","MSN","STB","CTG","BID","GAS","SAB",
        "VJC","PLX","POW","VRE","GVR"]


async def get_tickers_by_exchange(exchange: str) -> List[str]:
    """Lấy danh sách mã theo sàn. Cache trong memory 24h."""
    ex = exchange.upper()
    now = time.time()

    # Check in-memory cache
    cached = _ticker_list_cache.get(ex)
    if cached and (now - cached["ts"]) < _TICKER_CACHE_TTL:
        return cached["tickers"]

    try:
        loop = asyncio.get_event_loop()
        def fetch():
            try:
                from vnstock import Listing
                df = Listing().symbols_by_exchange()
                if df is None or df.empty:
                    return []
                filtered = df[
                    (df['exchange'].str.upper() == ex) &
                    (df['type'] == 'stock')
                ]
                tickers = filtered['symbol'].str.upper().tolist()
                print(f"✅ {ex}: {len(tickers)} mã (cached for 24h)")
                return tickers
            except BaseException as e:
                print(f"Listing error: {type(e).__name__}: {e}")
                return []
        from app.services.market_data import market_service
        await market_service._limiter.acquire()
        tickers = await loop.run_in_executor(None, fetch)

        # Cache kết quả (kể cả rỗng, để tránh retry liên tục)
        if tickers:
            _ticker_list_cache[ex] = {"tickers": tickers, "ts": now}
        return tickers
    except BaseException as e:
        print(f"get_tickers error {exchange}: {type(e).__name__}: {e}")
        return []


@router.get("/tickers/{exchange}")
async def get_exchange_tickers(exchange: str):
    ex = exchange.upper()
    if ex == "VN30":
        return {"exchange": "VN30", "tickers": VN30, "count": len(VN30)}
    tickers = await get_tickers_by_exchange(ex)
    return {"exchange": ex, "tickers": tickers, "count": len(tickers)}

@router.delete("/cache")
async def clear_screener_cache():
    """Xóa screener analysis cache (force re-calculate RS, trend, etc)."""
    screener_service.clear_cache()
    return {"cleared": True}

@router.get("/debug/rs/{ticker}")
async def debug_rs(ticker: str):
    """Debug RS calculation — xem dữ liệu đầu vào."""
    from app.services.screener import compute_rs_rating, compute_rs_line
    sym = ticker.upper()
    await screener_service._ensure_index_data()

    # Lấy stock data
    from datetime import datetime
    end = datetime.now().strftime("%Y-%m-%d")
    df = await screener_service._fetch_history_async(sym, "2000-01-01", end, is_index=False)

    idx = screener_service._index_data
    result = {
        "ticker": sym,
        "index_loaded": idx is not None,
        "index_len": len(idx) if idx is not None else 0,
        "stock_len": len(df) if df is not None else 0,
    }
    if idx is not None and df is not None:
        stock_close = df['close']
        index_close = idx['close']
        result["stock_close_first5"] = stock_close.head(3).tolist()
        result["stock_close_last5"] = stock_close.tail(3).tolist()
        result["index_close_first5"] = index_close.head(3).tolist()
        result["index_close_last5"] = index_close.tail(3).tolist()
        result["stock_dtype"] = str(stock_close.dtype)
        result["index_dtype"] = str(index_close.dtype)

        # Compute cả 2
        result["rs_rating"] = compute_rs_rating(stock_close, index_close)
        result["rs_line"] = compute_rs_line(stock_close, index_close)

        # Manual debug RS line (v2 — same logic as compute_rs_line)
        import pandas as pd
        length = 20
        lookback = length * 3  # 60
        s = stock_close.iloc[-lookback:].reset_index(drop=True)
        i = index_close.iloc[-lookback:].reset_index(drop=True)
        result["lookback"] = lookback
        result["s_len"] = len(s)
        result["i_len"] = len(i)
        result["s_first"] = float(s.iloc[0])
        result["s_last"] = float(s.iloc[-1])
        result["i_first"] = float(i.iloc[0])
        result["i_last"] = float(i.iloc[-1])

        rs_ratio = s / i
        result["ratio_first"] = float(rs_ratio.iloc[0])
        result["ratio_last"] = float(rs_ratio.iloc[-1])
        result["ratio_min"] = float(rs_ratio.min())
        result["ratio_max"] = float(rs_ratio.max())
        result["ratio_has_inf"] = bool(rs_ratio.isin([float('inf'), float('-inf')]).any())
        result["ratio_has_nan"] = bool(rs_ratio.isna().any())
        result["i_min"] = float(i.min())
        result["i_has_zero"] = bool((i == 0).any())
        # Show all 60 ratio values to find spike
        result["ratio_all"] = [round(float(v), 6) for v in rs_ratio.tolist()]

        rs_sma = rs_ratio.rolling(window=length).mean()
        result["sma_last"] = float(rs_sma.iloc[-1])
        result["sma_isnan"] = bool(pd.isna(rs_sma.iloc[-1]))

        if float(rs_sma.iloc[-1]) != 0 and not pd.isna(rs_sma.iloc[-1]):
            rv = (float(rs_ratio.iloc[-1]) / float(rs_sma.iloc[-1]) - 1) * 100
            result["rs_value_v2"] = round(rv, 4)
            result["rs_mapped_v2"] = round(max(0, min(100, (rv + 10) * 100 / 20)), 1)
    return result

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

@router.post("/scan")
async def scan_market_post(body: dict):
    """POST version — tránh URL length limit khi gửi nhiều mã."""
    tickers_str = body.get("tickers", "")
    min_score = int(body.get("min_score", 5))
    min_rs = float(body.get("min_rs", 0.0))
    ticker_list = [t.strip().upper() for t in tickers_str.split(",") if t.strip()]
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

# ── Fundamental: static routes TRƯỚC dynamic {ticker} route ──

@router.get("/fundamental/stats")
async def get_fundamental_stats():
    """Thống kê fundamentals cache."""
    return ohlcv_store.get_fundamental_stats()

@router.delete("/fundamental/cache")
async def clear_fundamental_cache():
    """Xoá toàn bộ fundamental cache trong SQLite (force re-fetch lần sau)."""
    with ohlcv_store._lock, ohlcv_store._connect() as conn:
        conn.execute("DELETE FROM fundamentals")
    return {'cleared': True}

@router.get("/fundamental/debug/{ticker}")
async def debug_fundamental(ticker: str):
    """Debug: thử tất cả sources và trả về chi tiết."""
    import os
    import requests as req_lib
    sym = ticker.upper()
    sources = []

    # 1. Test FireAnt API — multiple endpoints
    fireant_token = os.environ.get("FIREANT_TOKEN", "").strip()
    if fireant_token:
        headers = {"Authorization": f"Bearer {fireant_token}"}
        for ep_name, ep_url in [
            ("fundamental", f"https://restv2.fireant.vn/symbols/{sym}/fundamental"),
            ("financial-indicators", f"https://restv2.fireant.vn/symbols/{sym}/financial-indicators?type=quarterly&count=20"),
            ("financial-reports", f"https://restv2.fireant.vn/symbols/{sym}/financial-reports?type=quarter&limit=20"),
        ]:
            try:
                resp = req_lib.get(ep_url, timeout=15, headers=headers)
                body = resp.json() if resp.status_code == 200 else resp.text[:200]
                info: Dict[str, Any] = {"source": f"FireAnt/{ep_name}", "status": resp.status_code, "ok": resp.status_code == 200}
                if isinstance(body, dict):
                    info["type"] = "dict"
                    info["keys"] = list(body.keys())[:20]
                    # Show EPS/ROE values if present
                    for k in body:
                        if 'eps' in k.lower() or 'roe' in k.lower():
                            info[k] = body[k]
                elif isinstance(body, list):
                    info["type"] = f"list[{len(body)}]"
                    if body and isinstance(body[0], dict):
                        info["keys"] = list(body[0].keys())[:15]
                        for k in body[0]:
                            if 'eps' in k.lower() or 'roe' in k.lower():
                                info[f"sample_{k}"] = body[0][k]
                # Show full columns/rows for financial-reports
                if isinstance(body, dict) and 'columns' in body and 'rows' in body:
                    info["columns"] = body["columns"]
                    info["rows_count"] = len(body["rows"]) if isinstance(body["rows"], list) else "?"
                    # Show all rows (name + symbol + values)
                    if isinstance(body["rows"], list):
                        info["all_rows"] = [[r[0], r[1]] + list(r[2:5]) for r in body["rows"]] if body["rows"] else []
                # Show all items for financial-indicators
                if isinstance(body, list):
                    info["all_items"] = [{"name": item.get("name","?"), "shortName": item.get("shortName","?"), "value": item.get("value")} for item in body[:30] if isinstance(item, dict)]
                else:
                    info["body"] = str(body)[:200]
                sources.append(info)
            except Exception as e:
                sources.append({"source": f"FireAnt/{ep_name}", "ok": False, "error": f"{type(e).__name__}: {e}"})
    else:
        sources.append({"source": "FireAnt", "ok": False, "error": "FIREANT_TOKEN not set"})

    # 1b. Test VNDirect API raw
    try:
        vnd_url = f"https://finfo-api.vndirect.com.vn/v4/ratios?q=code:{sym}~reportType:quarterly&size=20&sort=yearReport:desc"
        resp = req_lib.get(vnd_url, timeout=15)
        if resp.status_code == 200:
            body = resp.json()
            sources.append({
                "source": "VNDirect/raw",
                "ok": True,
                "type": type(body).__name__,
                "keys": list(body.keys())[:10] if isinstance(body, dict) else f"list[{len(body)}]",
            })
        else:
            sources.append({"source": "VNDirect/raw", "ok": False, "status": resp.status_code, "body": resp.text[:200]})
    except Exception as e:
        sources.append({"source": "VNDirect/raw", "ok": False, "error": f"{type(e).__name__}: {e}"})

    # 2. Test vnstock sources
    for vsrc in ('KBS', 'VCI'):
        try:
            from vnstock import Vnstock
            stock = Vnstock().stock(symbol=sym, source=vsrc)
            if vsrc == 'VCI':
                df = stock.finance.ratio(period='quarterly', lang='en', dropna=False)
            else:
                df = stock.finance.ratio(period='quarterly')
            if df is None or df.empty:
                sources.append({"source": f"vnstock/{vsrc}", "ok": False, "error": "empty"})
            else:
                flat_cols = ['_'.join(str(c) for c in col).strip() if isinstance(col, tuple) else str(col)
                             for col in df.columns]
                sources.append({"source": f"vnstock/{vsrc}", "ok": True, "shape": list(df.shape), "columns": flat_cols[:10]})
        except BaseException as e:
            sources.append({"source": f"vnstock/{vsrc}", "ok": False, "error": f"{type(e).__name__}: {e}"})

    # 3. Test full fetch pipeline
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _fetch_fundamental_via_api, sym)

    cached = ohlcv_store.get_fundamental(sym)
    return {
        "ticker": sym,
        "sources": sources,
        "fetch_result": {"eps_count": len(result.get("eps", [])), "has_data": bool(result.get("eps"))},
        "sqlite_cached": cached is not None,
    }

# ── Dynamic {ticker} route CUỐI CÙNG ──

@router.get("/fundamental/{ticker}")
async def get_fundamental(ticker: str):
    """Lấy EPS & ROE theo quý cho 1 mã.
    Đọc từ SQLite trước — chỉ gọi vnstock nếu chưa có hoặc data quá cũ (>7 ngày).
    """
    from app.services.market_data import market_service
    sym = ticker.upper()

    # 1. Đọc từ SQLite cache
    if not ohlcv_store.is_fundamental_stale(sym):
        cached = ohlcv_store.get_fundamental(sym)
        if cached:
            cached.pop("_updated_at", None)
            return cached

    # 2. Fetch mới từ vnstock → lưu SQLite
    await market_service._limiter.acquire()
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _fetch_fundamental_via_api, sym)
    # Chỉ lưu vào cache khi có EPS VÀ ROE hợp lệ
    # Nếu roe_latest=0 thì không cache → force refetch lần sau
    has_valid_roe = result.get("roe_latest", 0) != 0 or any(v != 0 for v in result.get("roe", []))
    if result.get("eps") and has_valid_roe:
        ohlcv_store.upsert_fundamental(sym, result)
    return result

@router.post("/fundamental/batch")
async def get_fundamental_batch(tickers: List[str]):
    """Lấy EPS & ROE cho nhiều mã. Đọc SQLite trước, chỉ fetch vnstock khi stale."""
    from app.services.market_data import market_service
    loop = asyncio.get_event_loop()

    async def fetch_one(sym: str) -> Dict[str, Any]:
        # Đọc cache SQLite trước
        if not ohlcv_store.is_fundamental_stale(sym):
            cached = ohlcv_store.get_fundamental(sym)
            if cached:
                cached.pop("_updated_at", None)
                return cached
        # Fetch mới
        await market_service._limiter.acquire()
        result = await loop.run_in_executor(None, _fetch_fundamental_via_api, sym)
        if result.get("eps"):
            ohlcv_store.upsert_fundamental(sym, result)
        return result

    results = []
    for t in tickers:
        results.append(await fetch_one(t.upper()))
    return {'results': results}

@router.post("/rs-ratings/compute")
async def trigger_rs_ratings():
    """Manually trigger RS Rating batch computation (percentile rank toàn thị trường)."""
    from app.services.screener import compute_market_rs_ratings
    try:
        count = await compute_market_rs_ratings()
        return {"status": "done", "stocks_ranked": count}
    except Exception as e:
        return {"status": "error", "message": f"{type(e).__name__}: {e}"}


@router.get("/rs-ratings/status")
async def rs_ratings_status():
    """Kiểm tra trạng thái RS Ratings (stale hay not, stats)."""
    stale = ohlcv_store.is_rs_ratings_stale()
    with ohlcv_store._connect() as conn:
        row = conn.execute("SELECT COUNT(*) c, MAX(updated_at) m FROM rs_ratings").fetchone()
    return {
        "stale": stale,
        "count": row["c"] if row else 0,
        "last_updated": row["m"] if row else None,
    }


@router.get("/rs-ratings/{ticker}")
async def get_rs_rating(ticker: str):
    """Lấy RS Rating (percentile) cho 1 mã."""
    result = ohlcv_store.get_rs_rating(ticker.upper())
    if not result:
        return {"ticker": ticker.upper(), "error": "No RS Rating data. Run /rs-ratings/compute first."}
    return {"ticker": ticker.upper(), **result}


@router.get("/debug/vnindex")
async def debug_vnindex():
    """Debug endpoint: kiểm tra trạng thái VNINDEX data"""
    from datetime import datetime, timedelta
    now = datetime.now()
    end   = now.strftime("%Y-%m-%d")
    start = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    results = {}
    for symbol in ("VNINDEX", "VN-INDEX", "VNI", "^VNINDEX"):
        try:
            df = await screener_service._fetch_history_async(symbol, start, end, is_index=True)
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
