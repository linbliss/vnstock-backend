import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from app.services.market_data import market_service
from app.services import ohlcv_store

# Dùng chung RateLimiter của market_service để tránh vượt quota vnai (60/phút).
_limiter = market_service._limiter

# ── Trend Template Minervini – 8 tiêu chí ──
# RS_MIN_VN: TTCK Việt Nam mẫu nhỏ (~1600 mã, thanh khoản mỏng) → dùng 55
# thay vì chuẩn Minervini gốc 70. Giá trị có thể đổi trong alert settings.
RS_MIN_VN = 55.0

def check_trend_template(df: pd.DataFrame, rs_rating: float = 0.0) -> Dict:
    """
    df: OHLCV daily data, columns: open, high, low, close, volume
    rs_rating: tính trước bằng compute_rs_rating(), truyền vào để tính c8
    Trả về dict điểm từng tiêu chí và tổng điểm
    """
    if df is None or len(df) < 200:
        return {"score": 0, "criteria": {}, "passed": False}

    close = df['close'].values
    high  = df['high'].values  if 'high' in df.columns else close
    low   = df['low'].values   if 'low'  in df.columns else close
    current = close[-1]

    # Tính các MA
    ma50  = float(pd.Series(close).rolling(50).mean().iloc[-1])
    ma150 = float(pd.Series(close).rolling(150).mean().iloc[-1])
    ma200 = float(pd.Series(close).rolling(200).mean().iloc[-1])
    ma200_1m_ago = float(pd.Series(close).rolling(200).mean().iloc[-22])  # ~1 tháng

    # 52 tuần — dùng high/low thật (không phải close) để khớp Minervini gốc
    high_52w = float(np.max(high[-252:]))
    low_52w  = float(np.min(low[-252:]))

    criteria = {
        "c1_price_above_ma200":  bool(current > ma200),
        "c2_ma200_trending_up":  bool(ma200 > ma200_1m_ago),
        "c3_price_above_ma150":  bool(current > ma150),
        "c4_ma_stack":           bool(ma50 > ma150 > ma200),
        "c5_price_above_ma50":   bool(current > ma50),
        "c6_above_52w_low_30":   bool(current >= low_52w  * 1.30),
        "c7_near_52w_high_25":   bool(current >= high_52w * 0.75),
        # c8: RS Rating ≥ 55 (TTCK VN mẫu nhỏ) — chuẩn Minervini là 70
        "c8_rs_rating_strong":   bool(rs_rating >= RS_MIN_VN),
    }

    score = sum(criteria.values())
    return {
        "score":    score,
        "criteria": criteria,
        "passed":   score >= 6,
        "ma50":     round(ma50, 0),
        "ma150":    round(ma150, 0),
        "ma200":    round(ma200, 0),
        "high_52w": round(high_52w, 0),
        "low_52w":  round(low_52w, 0),
        "rs_rating": round(rs_rating, 1),
    }


def compute_rs_rating(stock_close: pd.Series, index_close: pd.Series) -> float:
    """
    RS Rating kiểu IBD (0-100) — Weighted 12 tháng so với VN-Index.
    40% quý gần nhất (63d) + 20% mỗi quý còn lại (126/189/252d).
    Ưu tiên momentum gần để bắt đà mới sớm hơn.
    """
    try:
        def period_return(s: pd.Series, days: int) -> float:
            if len(s) < days:
                return 0.0
            return float((s.iloc[-1] / s.iloc[-days] - 1) * 100)

        stock_r = [period_return(stock_close, p) for p in [63, 126, 189, 252]]
        index_r = [period_return(index_close, p) for p in [63, 126, 189, 252]]
        weights = [0.4, 0.2, 0.2, 0.2]

        stock_score = sum(r * w for r, w in zip(stock_r, weights))
        index_score = sum(r * w for r, w in zip(index_r, weights))

        relative = stock_score - index_score
        rs = max(0, min(100, (relative + 50)))
        return round(rs, 1)
    except Exception:
        return 50.0


def compute_rs_line(stock_close: pd.Series, index_close: pd.Series, length: int = 20) -> float:
    """
    RS Line kiểu FireAnt (length=20) — đo sức mạnh tương đối ngắn hạn.

    Công thức:
      RS Line = Stock Close / Index Close (normalized)
      RS SMA  = SMA(RS Line, length)
      RS Value = ((RS Line hiện tại / RS SMA) - 1) * 100

    Chuyển sang thang 0-100: Clamp [-20, +20] → [0, 100]
    """
    try:
        if len(stock_close) < length + 5 or len(index_close) < length + 5:
            return 50.0

        n = min(len(stock_close), len(index_close))
        stock = stock_close.iloc[-n:].reset_index(drop=True)
        index = index_close.iloc[-n:].reset_index(drop=True)

        stock_norm = stock / stock.iloc[0] * 100
        index_norm = index / index.iloc[0] * 100
        rs_line = stock_norm / index_norm

        rs_sma = rs_line.rolling(window=length).mean()

        rs_current = float(rs_line.iloc[-1])
        rs_sma_val = float(rs_sma.iloc[-1])
        if rs_sma_val == 0:
            return 50.0

        rs_value = (rs_current / rs_sma_val - 1) * 100
        rs = max(0, min(100, (rs_value + 20) * 100 / 40))
        return round(rs, 1)
    except Exception:
        return 50.0


def detect_vcp(df: pd.DataFrame) -> Dict:
    """
    Nhận diện VCP (Volatility Contraction Pattern) — Mức A (vá nhanh).
    Thuật toán slice cố định 10 ngày còn thô; Mức B sẽ viết lại bằng swing-point.
    Trả về: is_vcp, contracting, vol_contracting, uptrend_ok, tightness, pivot_buy, ...
    """
    if df is None or len(df) < 130:
        # Cần ≥ 130 phiên để kiểm tra uptrend 6 tháng
        return {"is_vcp": False, "stage": "unknown"}

    close  = df['close'].values
    high   = df['high'].values
    low    = df['low'].values
    volume = df['volume'].values

    # ── Uptrend check: phải +15% trong 6 tháng trước đó (Stage 2 filter thô) ──
    price_6m_ago = float(close[-126]) if len(close) >= 126 else float(close[0])
    current_close = float(close[-1])
    uptrend_ok = current_close >= price_6m_ago * 1.15

    # Tìm pivot highs/lows trong 60 ngày gần nhất
    window = min(60, len(close))
    c = close[-window:]
    h = high[-window:]
    l = low[-window:]
    v = volume[-window:]

    # Tính độ rộng (biên dao động) theo từng đoạn 10 ngày
    segments = []
    seg_size = 10
    for i in range(0, window - seg_size, seg_size):
        seg_h = float(np.max(h[i:i+seg_size]))
        seg_l = float(np.min(l[i:i+seg_size]))
        seg_v = float(np.mean(v[i:i+seg_size]))
        width = (seg_h - seg_l) / seg_h * 100 if seg_h > 0 else 0
        segments.append({"width": width, "high": seg_h, "low": seg_l, "avg_vol": seg_v})

    if len(segments) < 3:
        return {"is_vcp": False, "stage": "insufficient_data"}

    # Kiểm tra co lại dần (mỗi đoạn sau hẹp hơn đoạn trước)
    contracting = all(
        segments[i+1]["width"] < segments[i]["width"]
        for i in range(len(segments) - 1)
    )
    vol_contracting = all(
        segments[i+1]["avg_vol"] < segments[i]["avg_vol"]
        for i in range(len(segments) - 1)
    )

    # Đoạn cuối cùng (tight area)
    last_seg = segments[-1]
    tightness = last_seg["width"]  # % — càng nhỏ càng tốt

    # Pivot buy point = đỉnh handle + 1 tick
    pivot_buy = round(last_seg["high"] * 1.005, 0)  # +0.5%

    # Giá hiện tại
    current = float(c[-1])
    near_pivot = abs(current - pivot_buy) / pivot_buy * 100 < 3  # trong 3%

    # Volume hiện tại vs MA30
    vol_ma30 = float(np.mean(v[-30:]))
    current_vol = float(v[-1])
    vol_ratio = current_vol / vol_ma30 if vol_ma30 > 0 else 0

    # Mức A: siết điều kiện VCP — cần hội đủ 3 yếu tố:
    #   1) biên co dần (contracting)
    #   2) volume cạn dần (vol_contracting)
    #   3) biên cuối < 10% (tightness chặt, chuẩn Minervini 3-10%)
    #   4) có uptrend 6 tháng trước đó (Stage 2)
    is_vcp = contracting and vol_contracting and tightness < 10 and uptrend_ok

    return {
        "is_vcp":          is_vcp,
        "contracting":     contracting,
        "vol_contracting": vol_contracting,
        "uptrend_ok":      uptrend_ok,
        "tightness":       round(tightness, 2),
        "pivot_buy":       pivot_buy,
        "near_pivot":      near_pivot,
        "vol_ratio":       round(vol_ratio, 2),
        "vol_confirmed":   vol_ratio >= 1.3,
        "segments":        len(segments),
        "stage": "vcp" if is_vcp else (
            "contracting" if contracting else
            "no_uptrend" if not uptrend_ok else "base_forming"
        ),
    }


class ScreenerService:
    def __init__(self):
        self._cache: Dict[str, dict] = {}
        self._cache_time: Dict[str, datetime] = {}
        self._index_data: Optional[pd.DataFrame] = None
        self._index_fetched_at: Optional[datetime] = None
        self.CACHE_TTL = 900        # 15 phút cho mã thường (giảm churn gọi lại)
        self.INDEX_TTL = 3600       # 1 giờ cho VNINDEX

    async def _ensure_index_data(self):
        """Load VNINDEX (refresh 1 giờ). Chỉ dùng symbol 'VNINDEX' — đã verify OK qua KBS."""
        now = datetime.now()
        if (
            self._index_data is not None
            and self._index_fetched_at is not None
            and (now - self._index_fetched_at).seconds < self.INDEX_TTL
        ):
            return
        end   = now.strftime("%Y-%m-%d")
        start = "2000-01-01"

        df = await self._fetch_history_async("VNINDEX", start, end, is_index=True)
        if df is not None and len(df) >= 60:
            self._index_data = df
            self._index_fetched_at = now
            print(f"✅ VNINDEX loaded: {len(df)} rows")
        else:
            print("⚠️  Không load được VNINDEX data")

    async def run_screener(
        self,
        tickers: List[str],
        min_trend_score: int = 6,
        min_rs: float = 60.0,
    ) -> List[dict]:
        """Chạy screener cho danh sách mã, trả về kết quả có điểm"""
        await self._ensure_index_data()
        results = []
        for ticker in tickers:
            try:
                result = await self._analyze_ticker(ticker)
                if result and result.get("trend_score", 0) >= min_trend_score:
                    results.append(result)
            except Exception as e:
                print(f"Screener error {ticker}: {e}")

        # Sắp xếp theo tổng điểm
        results.sort(key=lambda x: x.get("total_score", 0), reverse=True)
        return results

    async def _analyze_ticker(self, ticker: str) -> Optional[dict]:
        """Phân tích một mã: Trend Template + VCP + RS"""
        # Kiểm tra cache
        now = datetime.now()
        if ticker in self._cache:
            cached_time = self._cache_time.get(ticker)
            if cached_time and (now - cached_time).seconds < self.CACHE_TTL:
                return self._cache[ticker]

        # Lấy toàn bộ lịch sử có sẵn – càng nhiều càng tin cậy cho MA200, RS
        end   = now.strftime("%Y-%m-%d")
        start = "2000-01-01"

        await self._ensure_index_data()
        df = await self._fetch_history_async(ticker, start, end, is_index=False)

        if df is None or len(df) < 60:
            return None

        # RS Rating IBD (weighted 12 tháng) + RS Line FireAnt (length=20)
        rs_rating = 50.0
        rs_line_val = 50.0
        if self._index_data is not None and len(self._index_data) >= 60:
            rs_rating = compute_rs_rating(
                df['close'], self._index_data['close']
            )
            rs_line_val = compute_rs_line(
                df['close'], self._index_data['close']
            )

        # Trend Template (c8 dùng rs_rating IBD ≥ 55)
        trend = check_trend_template(df, rs_rating=rs_rating)

        # VCP
        vcp = detect_vcp(df)

        # Giá hiện tại từ cache quotes
        quote = market_service.quotes.get(ticker, {})
        current_price = float(quote.get("price", df['close'].iloc[-1]))
        change_pct    = float(quote.get("change_pct", 0))
        volume        = int(quote.get("volume", 0))

        # Volume MA — frontend cần để hiển thị + filter theo Settings
        vol = df['volume'].values
        vol_ma20 = int(float(pd.Series(vol).rolling(20).mean().iloc[-1])) if len(vol) >= 20 else 0
        vol_ma50 = int(float(pd.Series(vol).rolling(50).mean().iloc[-1])) if len(vol) >= 50 else 0

        # Tổng điểm (0-100)
        trend_pts = trend["score"] * 5          # max 40
        rs_pts    = min(rs_rating * 0.3, 30)    # max 30
        vcp_pts   = 20 if vcp["is_vcp"] else (10 if vcp["contracting"] else 0)
        near_pts  = 10 if vcp.get("near_pivot") else 0
        total     = round(trend_pts + rs_pts + vcp_pts + near_pts, 1)

        result = {
            "ticker":       ticker,
            "price":        current_price,
            "change_pct":   change_pct,
            "volume":       volume,
            "trend_score":  trend["score"],
            "trend_passed": trend["passed"],
            "criteria":     trend["criteria"],
            "ma50":         trend.get("ma50", 0),
            "ma150":        trend.get("ma150", 0),
            "ma200":        trend.get("ma200", 0),
            "rs_rating":    rs_rating,
            "rs_line":      rs_line_val,
            "vcp":          vcp,
            "total_score":  total,
            "vol_ma20":     vol_ma20,
            "vol_ma50":     vol_ma50,
            "analysis_time": now.isoformat(),
        }

        self._cache[ticker] = result
        self._cache_time[ticker] = now
        return result

    async def _fetch_history_async(
        self, ticker: str, start: str, end: str, is_index: bool = False
    ) -> Optional[pd.DataFrame]:
        """Đọc lịch sử OHLCV. Ưu tiên OHLCV store (SQLite) để tránh gọi vnstock.
        Chỉ fallback vnstock cho: indices (VNINDEX) hoặc ticker CHƯA có trong store.
        """
        # ── Store first (stocks) ──
        if not is_index:
            df_store = ohlcv_store.get_ohlcv(ticker, start, end)
            if df_store is not None and len(df_store) >= 60:
                return df_store

        loop = asyncio.get_event_loop()
        sources = ['kbs', 'vci', 'msn'] if is_index else ['kbs', 'vci']
        for source in sources:
            await _limiter.acquire()   # 1 acquire = 1 API call
            df, stopped = await loop.run_in_executor(
                None, self._fetch_one_source, ticker, source, start, end, is_index
            )
            if df is not None and not df.empty:
                print(f"✅ {ticker} fetched via {source}: {len(df)} rows")
                return df
            if stopped:
                break   # raise → bỏ luôn
        return None

    def _fetch_one_source(
        self, ticker: str, source: str, start: str, end: str, is_index: bool
    ):
        """Sync worker — gọi vnstock 1 lần. Trả (df, stopped).
        stopped=True nếu có exception → caller bỏ source khác.
        """
        try:
            from vnstock import Quote
            sym = "VNINDEX" if is_index else ticker.upper()
            raw = Quote(symbol=sym, source=source).history(
                start=start, end=end, interval='1D'
            )
            if raw is None or raw.empty:
                return (None, False)
            return (self._normalize(raw), False)
        # BaseException để nuốt SystemExit từ vnai.beam.quota
        except BaseException as e:
            print(f"⚠️  {ticker} error from {source}: {type(e).__name__}: {e}")
            return (None, True)

    def _normalize(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Chuẩn hoá OHLCV columns → close/open/high/low/volume."""
        try:
            df = df.reset_index()
            col_map = {}
            for col in df.columns:
                cl = str(col).lower()
                if   'close'  in cl: col_map[col] = 'close'
                elif 'open'   in cl: col_map[col] = 'open'
                elif 'high'   in cl: col_map[col] = 'high'
                elif 'low'    in cl: col_map[col] = 'low'
                elif 'volume' in cl: col_map[col] = 'volume'
            df = df.rename(columns=col_map)
            for c in ['close', 'open', 'high', 'low']:
                if c not in df.columns:
                    return None
            if 'volume' not in df.columns:
                df['volume'] = 0
            return df
        except BaseException as e:
            print(f"normalize error: {type(e).__name__}: {e}")
            return None


screener_service = ScreenerService()
