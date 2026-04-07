import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from app.services.market_data import market_service

# ── Trend Template Minervini – 8 tiêu chí ──
def check_trend_template(df: pd.DataFrame) -> Dict:
    """
    df: OHLCV daily data, columns: open, high, low, close, volume
    Trả về dict điểm từng tiêu chí và tổng điểm
    """
    if df is None or len(df) < 200:
        return {"score": 0, "criteria": {}, "passed": False}

    close = df['close'].values
    volume = df['volume'].values
    current = close[-1]

    # Tính các MA
    ma50  = float(pd.Series(close).rolling(50).mean().iloc[-1])
    ma150 = float(pd.Series(close).rolling(150).mean().iloc[-1])
    ma200 = float(pd.Series(close).rolling(200).mean().iloc[-1])
    ma200_1m_ago = float(pd.Series(close).rolling(200).mean().iloc[-22])  # ~1 tháng

    # 52 tuần
    high_52w = float(np.max(close[-252:]))
    low_52w  = float(np.min(close[-252:]))

    # Volume MA30
    vol_ma30 = float(pd.Series(volume).rolling(30).mean().iloc[-1])

    criteria = {
        "c1_price_above_ma200":  bool(current > ma200),
        "c2_ma200_trending_up":  bool(ma200 > ma200_1m_ago),
        "c3_price_above_ma150":  bool(current > ma150),
        "c4_ma_stack":           bool(ma50 > ma150 > ma200),
        "c5_price_above_ma50":   bool(current > ma50),
        "c6_above_52w_low_30":   bool(current >= low_52w  * 1.30),
        "c7_near_52w_high_25":   bool(current >= high_52w * 0.75),
        "c8_volume_sufficient":  bool(vol_ma30 > 100_000),
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
        "vol_ma30": round(vol_ma30, 0),
    }


def compute_rs_rating(stock_returns: pd.Series, index_returns: pd.Series) -> float:
    """
    Tính RS Rating (0-100) so với VN-Index
    Dựa trên hiệu suất tương đối 12 tháng (weighted: 25% Q4, 25% Q3, 25% Q2, 25% Q1)
    """
    try:
        # Lấy returns 63, 126, 189, 252 ngày
        def period_return(s: pd.Series, days: int) -> float:
            if len(s) < days:
                return 0.0
            return float((s.iloc[-1] / s.iloc[-days] - 1) * 100)

        stock_r  = [period_return(stock_returns,  p) for p in [63, 126, 189, 252]]
        index_r  = [period_return(index_returns,  p) for p in [63, 126, 189, 252]]
        weights  = [0.4, 0.2, 0.2, 0.2]

        stock_score = sum(r * w for r, w in zip(stock_r,  weights))
        index_score = sum(r * w for r, w in zip(index_r,  weights))

        # RS tương đối: > 0 là outperform
        relative = stock_score - index_score
        # Chuyển về thang 0-100 (clamp -50..+50 → 0..100)
        rs = max(0, min(100, (relative + 50)))
        return round(rs, 1)
    except:
        return 50.0


def detect_vcp(df: pd.DataFrame) -> Dict:
    """
    Nhận diện VCP (Volatility Contraction Pattern)
    Trả về: is_vcp, contraction_count, tightness, pivot_buy_point
    """
    if df is None or len(df) < 60:
        return {"is_vcp": False, "stage": "unknown"}

    close  = df['close'].values
    high   = df['high'].values
    low    = df['low'].values
    volume = df['volume'].values

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

    is_vcp = contracting and tightness < 15  # biên hẹp < 15%

    return {
        "is_vcp":        is_vcp,
        "contracting":   contracting,
        "tightness":     round(tightness, 2),
        "pivot_buy":     pivot_buy,
        "near_pivot":    near_pivot,
        "vol_ratio":     round(vol_ratio, 2),
        "vol_confirmed": vol_ratio >= 1.3,
        "segments":      len(segments),
        "stage": "vcp" if is_vcp else (
            "contracting" if contracting else "base_forming"
        ),
    }


class ScreenerService:
    def __init__(self):
        self._cache: Dict[str, dict] = {}
        self._cache_time: Dict[str, datetime] = {}
        self._index_data: Optional[pd.DataFrame] = None
        self._index_fetched_at: Optional[datetime] = None
        self.CACHE_TTL = 300        # 5 phút cho mã thường
        self.INDEX_TTL = 3600       # 1 giờ cho VNINDEX

    async def _ensure_index_data(self):
        """Load VNINDEX data, refresh mỗi 1 giờ. Thử nhiều tên symbol."""
        now = datetime.now()
        if (
            self._index_data is not None
            and self._index_fetched_at is not None
            and (now - self._index_fetched_at).seconds < self.INDEX_TTL
        ):
            return
        end   = now.strftime("%Y-%m-%d")
        start = (now - timedelta(days=730)).strftime("%Y-%m-%d")
        loop  = asyncio.get_event_loop()

        # Thử nhiều tên symbol VNINDEX phổ biến trong vnstock
        for symbol in ("VNINDEX", "VN-INDEX", "VNI", "^VNINDEX"):
            df = await loop.run_in_executor(None, self._fetch_history, symbol, start, end)
            if df is not None and len(df) >= 60:
                self._index_data = df
                self._index_fetched_at = now
                print(f"✅ VNINDEX loaded as '{symbol}': {len(df)} rows")
                return

        print("⚠️  Không load được VNINDEX data với bất kỳ tên symbol nào")

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

        # Lấy dữ liệu lịch sử – cần 2 năm để đủ MA200 (~280 ngày lịch)
        end   = now.strftime("%Y-%m-%d")
        start = (now - timedelta(days=730)).strftime("%Y-%m-%d")

        await self._ensure_index_data()
        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(
            None, self._fetch_history, ticker, start, end
        )

        if df is None or len(df) < 60:
            return None

        # Trend Template
        trend = check_trend_template(df)

        # VCP
        vcp = detect_vcp(df)

        # Giá hiện tại từ cache quotes
        quote = market_service.quotes.get(ticker, {})
        current_price = float(quote.get("price", df['close'].iloc[-1]))
        change_pct    = float(quote.get("change_pct", 0))
        volume        = int(quote.get("volume", 0))

        # RS Rating (so với VN-Index)
        rs_rating = 50.0
        if self._index_data is not None and len(self._index_data) >= 60:
            rs_rating = compute_rs_rating(
                df['close'], self._index_data['close']
            )

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
            "vcp":          vcp,
            "total_score":  total,
            "analysis_time": now.isoformat(),
        }

        self._cache[ticker] = result
        self._cache_time[ticker] = now
        return result

    def _fetch_history(self, ticker: str, start: str, end: str) -> Optional[pd.DataFrame]:
        try:
            from vnstock import Quote
            df = Quote(symbol=ticker, source='KBS').history(
                start=start, end=end, interval='1D'
            )
            if df is None or df.empty:
                return None
            df = df.reset_index()
            # Chuẩn hoá tên cột
            col_map = {}
            for col in df.columns:
                cl = str(col).lower()
                if 'close' in cl: col_map[col] = 'close'
                elif 'open'  in cl: col_map[col] = 'open'
                elif 'high'  in cl: col_map[col] = 'high'
                elif 'low'   in cl: col_map[col] = 'low'
                elif 'volume' in cl: col_map[col] = 'volume'
            df = df.rename(columns=col_map)
            for c in ['close','open','high','low','volume']:
                if c not in df.columns:
                    return None
            return df
        except Exception as e:
            print(f"fetch_history error {ticker}: {e}")
            return None


screener_service = ScreenerService()
