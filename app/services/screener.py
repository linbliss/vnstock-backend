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


def _filter_outliers(stock: pd.Series, index: pd.Series) -> tuple:
    """Filter corrupt VNINDEX rows (FireAnt sometimes returns close/1000)."""
    stock = stock.astype(float)
    index = index.astype(float)
    median_idx = float(index.median())
    if median_idx > 0:
        valid_mask = (index > median_idx * 0.3) & (index < median_idx * 3)
        stock = stock[valid_mask].reset_index(drop=True)
        index = index[valid_mask].reset_index(drop=True)
    return stock, index


def compute_rs_rating(stock_close: pd.Series, index_close: pd.Series) -> float:
    """
    RS Rating kiểu IBD (0-100) — Weighted 12 tháng so với VN-Index.
    40% quý gần nhất (63d) + 20% mỗi quý còn lại (126/189/252d).
    Ưu tiên momentum gần để bắt đà mới sớm hơn.
    """
    try:
        stock_close, index_close = _filter_outliers(stock_close, index_close)
        if len(stock_close) < 252 or len(index_close) < 252:
            # Fallback: cần ít nhất 63 phiên
            if len(stock_close) < 63:
                return 50.0

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

        # Ch�� dùng 60 phiên gần nh���t (đủ cho SMA(20) + buffer)
        lookback = length * 3  # 60 phiên cho length=20
        stock = stock_close.iloc[-lookback:].reset_index(drop=True).astype(float)
        index = index_close.iloc[-lookback:].reset_index(drop=True).astype(float)

        # Filter outliers: VNINDEX bình thường 100-5000 points
        # FireAnt đôi khi trả giá trị lỗi (close bị chia 1000)
        median_idx = float(index.median())
        if median_idx > 0:
            valid_mask = (index > median_idx * 0.3) & (index < median_idx * 3)
            stock = stock[valid_mask].reset_index(drop=True)
            index = index[valid_mask].reset_index(drop=True)

        if len(stock) < length + 5:
            return 50.0

        # RS = Stock/Index ratio → so với SMA(20) của nó
        rs_ratio = stock / index
        rs_sma = rs_ratio.rolling(window=length).mean()

        rs_current = float(rs_ratio.iloc[-1])
        rs_sma_val = float(rs_sma.iloc[-1])
        if rs_sma_val == 0 or pd.isna(rs_sma_val):
            return 50.0

        # RS value = % chênh lệch RS hiện tại vs SMA
        rs_value = (rs_current / rs_sma_val - 1) * 100

        # Map sang 0-100: [-10, +10] → [0, 100]
        rs = max(0, min(100, (rs_value + 10) * 100 / 20))
        return round(rs, 1)
    except Exception:
        return 50.0


def _find_swing_points(high: np.ndarray, low: np.ndarray, order: int = 5) -> tuple:
    """
    Tìm swing highs và swing lows dùng phương pháp fractal (Minervini style).

    Fix A: Dùng order ĐỘNG — order lớn (5) cho phần đầu base, order nhỏ (2-3)
    cho phần cuối để bắt được "tay cầm" (handle) ngắn hạn.
    Quy tắc: 30% cuối base dùng order=2, phần còn lại dùng order truyền vào.

    Trả về: (swing_highs, swing_lows) — mỗi cái là list of (index, price).
    """
    swing_highs = []
    swing_lows = []
    n = len(high)

    # Ngưỡng chuyển đổi: 70% đầu dùng order gốc, 30% cuối dùng order nhỏ
    handle_zone_start = int(n * 0.7)
    handle_order = min(order, 2)  # order=2 cho vùng tay cầm

    for i in range(order, n - order):
        # Chọn order phù hợp với vị trí trong base
        local_order = handle_order if i >= handle_zone_start else order

        # Đảm bảo không vượt biên
        left = max(0, i - local_order)
        right = min(n, i + local_order + 1)

        # Swing high: bar cao nhất trong cửa sổ
        if high[i] == np.max(high[left:right]):
            swing_highs.append((i, float(high[i])))
        # Swing low: bar thấp nhất trong cửa sổ
        if low[i] == np.min(low[left:right]):
            swing_lows.append((i, float(low[i])))

    return swing_highs, swing_lows


def _find_contractions(
    swing_highs: list, swing_lows: list,
    vol: np.ndarray, vol_ma50: float
) -> list:
    """
    Tìm các contraction (T) trong base pattern.

    Fix B: Không lọc bằng depth >= 3%. Chỉ loại nhiễu bằng duration >= 2 phiên.
    (Tay cầm hoàn hảo có thể depth chỉ 1.5-2.5%).

    Fix C: Volume dry-up đo tại vùng TROUGH (2-3 phiên quanh đáy),
    so sánh với vol_ma50. Cạn cung = vol tại đáy < 60% vol_ma50.

    Minervini VCP: T1 > T2 > T3 (depth giảm dần = volatility contraction).
    """
    if not swing_highs or not swing_lows:
        return []

    contractions = []
    used_lows = set()

    for sh_idx, sh_price in swing_highs:
        # Tìm swing low SÂU swing high này
        best_low = None
        for sl_idx, sl_price in swing_lows:
            if sl_idx <= sh_idx:
                continue
            if sl_idx in used_lows:
                continue
            if sl_price >= sh_price:
                continue
            if best_low is None or sl_price < best_low[1]:
                best_low = (sl_idx, sl_price)
            if sl_idx - sh_idx > 60:
                break

        if best_low:
            depth = (sh_price - best_low[1]) / sh_price * 100
            duration = best_low[0] - sh_idx

            # Fix B: Chỉ lọc nhiễu bằng duration, KHÔNG lọc depth
            # Một contraction phải kéo dài ít nhất 2 phiên để có ý nghĩa
            if duration < 2:
                continue

            # Fix C: Đo volume tại vùng TROUGH (3 phiên quanh đáy)
            trough_idx = best_low[0]
            trough_start = max(0, trough_idx - 1)
            trough_end = min(len(vol), trough_idx + 2)
            trough_vol_slice = vol[trough_start:trough_end]
            trough_avg_vol = float(np.mean(trough_vol_slice)) if len(trough_vol_slice) > 0 else 0

            # Cạn cung = Volume tại đáy < 60% MA50 Volume
            is_volume_dry = trough_avg_vol < (vol_ma50 * 0.6) if vol_ma50 > 0 else False

            used_lows.add(best_low[0])
            contractions.append({
                "high_idx": sh_idx,
                "high_price": sh_price,
                "low_idx": best_low[0],
                "low_price": best_low[1],
                "depth": depth,             # % correction
                "duration": duration,       # bars
                "trough_avg_vol": trough_avg_vol,
                "is_volume_dry": is_volume_dry,
            })

    return contractions


def detect_vcp(df: pd.DataFrame) -> Dict:
    """
    Nhận diện VCP (Volatility Contraction Pattern) — Chuẩn IBD/Minervini.

    Thuật toán swing-point based:
    1. Stage 2 filter: giá phải trong uptrend (trên MA150, +30% trong 12 tháng)
    2. Tìm swing highs/lows thực sự (fractal method, order=5)
    3. Xác định các contraction (T1, T2, T3...) từ swing points
    4. Kiểm tra VCP: depth giảm dần (T1 > T2 > T3), ít nhất 2 contractions
    5. Tightness: contraction cuối phải < 15% (lý tưởng 3-10%)
    6. Volume dry-up: volume giảm dần qua các contraction
    7. Pivot point: đỉnh contraction cuối + 0.5%

    Điều kiện Minervini gốc:
    - First contraction (T1): 10-35% depth
    - Subsequent: mỗi cái shallower hơn trước ít nhất 30%
    - Final tightness: < 15% (tốt nhất < 10%)
    - Duration tổng thể: 3-65 tuần (15-325 ngày)
    - Ít nhất 2 contractions (T-count ≥ 2)
    """
    if df is None or len(df) < 130:
        return {"is_vcp": False, "stage": "unknown"}

    close  = df['close'].astype(float).values
    high   = df['high'].astype(float).values
    low    = df['low'].astype(float).values
    volume = df['volume'].astype(float).values
    n = len(close)

    # ══════════════════════════════════════════════════════════════════════
    # STAGE 2 FILTER — Cổ phiếu phải đang trong uptrend rõ ràng
    # ══════════════════════════════════════════════════════════════════════
    current_close = float(close[-1])
    ma50  = float(pd.Series(close).rolling(50).mean().iloc[-1])
    ma150 = float(pd.Series(close).rolling(150).mean().iloc[-1])

    # Điều kiện uptrend tối thiểu:
    # - Giá trên MA150 (đang trong Stage 2)
    # - Tăng ít nhất 30% trong 12 tháng (hoặc 20% trong 6 tháng)
    price_12m = float(close[-252]) if n >= 252 else float(close[0])
    price_6m  = float(close[-126]) if n >= 126 else float(close[0])
    gain_12m  = (current_close / price_12m - 1) * 100 if price_12m > 0 else 0
    gain_6m   = (current_close / price_6m - 1) * 100 if price_6m > 0 else 0

    uptrend_ok = (
        current_close > ma150 and
        (gain_12m >= 30 or gain_6m >= 20)
    )

    if not uptrend_ok:
        # Vẫn trả kết quả đầy đủ để frontend render
        vol_ma30 = float(np.mean(volume[-30:])) if len(volume) >= 30 else 1.0
        vol_ratio = float(volume[-1]) / vol_ma30 if vol_ma30 > 0 else 0
        return {
            "is_vcp": False,
            "contracting": False,
            "vol_contracting": False,
            "uptrend_ok": False,
            "tightness": 0.0,
            "pivot_buy": 0.0,
            "near_pivot": False,
            "vol_ratio": round(vol_ratio, 2),
            "vol_confirmed": vol_ratio >= 1.3,
            "segments": 0,
            "t_count": 0,
            "base_depth": 0.0,
            "base_length": 0,
            "stage": "no_uptrend",
        }

    # ══════════════════════════════════════════════════════════════════════
    # TÌM BASE PATTERN — scan 200 ngày gần nhất (covers most VCP bases)
    # ══════════════════════════════════════════════════════════════════════
    base_window = min(200, n)
    b_close = close[-base_window:]
    b_high  = high[-base_window:]
    b_low   = low[-base_window:]
    b_vol   = volume[-base_window:]

    # Tìm đỉnh cao nhất trong base → đó là khởi đầu base (left side)
    base_high_idx = int(np.argmax(b_high))
    base_high_price = float(b_high[base_high_idx])

    # Base depth = max drawdown từ đỉnh
    base_low_price = float(np.min(b_low[base_high_idx:]))
    base_depth = (base_high_price - base_low_price) / base_high_price * 100

    # Base length (từ đỉnh đến hiện tại)
    base_length = base_window - base_high_idx

    # Filter: base depth hợp lý (Minervini: typical 10-35%, max 50%)
    # TTCK VN volatile hơn → cho phép tới 50%
    if base_depth > 50 or base_depth < 8:
        vol_ma30 = float(np.mean(b_vol[-30:])) if len(b_vol) >= 30 else 1.0
        vol_ratio = float(b_vol[-1]) / vol_ma30 if vol_ma30 > 0 else 0
        return {
            "is_vcp": False,
            "contracting": False,
            "vol_contracting": False,
            "uptrend_ok": True,
            "tightness": round(base_depth, 2),
            "pivot_buy": 0.0,
            "near_pivot": False,
            "vol_ratio": round(vol_ratio, 2),
            "vol_confirmed": vol_ratio >= 1.3,
            "segments": 0,
            "t_count": 0,
            "base_depth": round(base_depth, 2),
            "base_length": base_length,
            "stage": "base_too_deep" if base_depth > 50 else "base_too_shallow",
        }

    # ══════════════════════════════════════════════════════════════════════
    # SWING POINT DETECTION — Tìm pivot highs/lows thực sự
    # ══════════════════════════════════════════════════════════════════════
    # Fix A: order=5 cho phần đầu base, tự động giảm xuống order=2
    # ở 30% cuối base để bắt được "tay cầm" (handle) ngắn hạn.
    analysis_high = b_high[base_high_idx:]
    analysis_low  = b_low[base_high_idx:]
    analysis_vol  = b_vol[base_high_idx:]

    swing_highs, swing_lows = _find_swing_points(analysis_high, analysis_low, order=5)

    # ══════════════════════════════════════════════════════════════════════
    # XÁC ĐỊNH CONTRACTIONS (T-count)
    # ══════════════════════════════════════════════════════════════════════
    # Tính vol_ma50 cho toàn bộ analysis zone — dùng làm benchmark cạn cung
    vol_ma50_val = float(np.mean(analysis_vol[-50:])) if len(analysis_vol) >= 50 else float(np.mean(analysis_vol))

    # Fix B+C: _find_contractions giờ lọc bằng duration (không depth),
    # và đo volume tại trough thay vì trung bình toàn nhịp
    contractions = _find_contractions(swing_highs, swing_lows, analysis_vol, vol_ma50_val)

    t_count = len(contractions)

    # ══════════════════════════════════════════════════════════════════════
    # KIỂM TRA VCP CONDITIONS
    # ══════════════════════════════════════════════════════════════════════
    # 1) Contracting: biên độ thu hẹp tổng thể
    # Minervini core: contraction ĐẦU phải sâu hơn contraction CUỐI đáng kể.
    # Cho phép micro-swing cùng depth (handle zone) miễn xu hướng tổng thể giảm.
    contracting = False
    if t_count >= 2:
        first_depth = contractions[0]["depth"]
        last_depth = contractions[-1]["depth"]
        # Điều kiện chính: depth giảm ít nhất 40% từ T1 đến T cuối
        overall_contraction = last_depth < first_depth * 0.6
        # Hoặc: pair-wise check truyền thống (cho phép violations)
        violations = 0
        for i in range(t_count - 1):
            if contractions[i + 1]["depth"] >= contractions[i]["depth"] * 1.05:
                violations += 1
        max_violations = t_count // 2  # cho phép ~50% violations (micro-swings)
        pairwise_ok = violations <= max_violations
        contracting = overall_contraction or pairwise_ok

    # 2) Volume dry-up: kiểm tra volume tại trough giảm dần
    # Fix C: Dùng is_volume_dry từ _find_contractions (đo tại đáy vs MA50)
    vol_contracting = False
    if t_count >= 2:
        # Cách 1: Ít nhất contraction cuối phải cạn cung
        last_dry = contractions[-1].get("is_volume_dry", False)
        # Cách 2: Trough volume giảm dần qua các contraction
        trough_vols = [c.get("trough_avg_vol", 0) for c in contractions]
        if trough_vols:
            vol_violations = 0
            for i in range(len(trough_vols) - 1):
                if trough_vols[i + 1] > trough_vols[i] * 1.2:  # cho phép 20% tolerance
                    vol_violations += 1
            # Cạn cung nếu: trough vol giảm dần HOẶC nhịp cuối cạn cung rõ ràng
            vol_contracting = (vol_violations == 0) or last_dry

    # 3) Tightness: contraction cuối (hoặc khoảng 10 ngày gần nhất)
    if t_count >= 1:
        tightness = contractions[-1]["depth"]
    else:
        # Fallback: biên 20 ngày gần nhất
        recent_h = float(np.max(b_high[-20:]))
        recent_l = float(np.min(b_low[-20:]))
        tightness = (recent_h - recent_l) / recent_h * 100 if recent_h > 0 else 0

    # 4) Pivot buy point
    if t_count >= 1:
        # Pivot = đỉnh contraction cuối cùng + 0.5%
        pivot_buy = round(contractions[-1]["high_price"] * 1.005, 2)
    else:
        # Fallback: đỉnh 20 ngày + 0.5%
        pivot_buy = round(float(np.max(b_high[-20:])) * 1.005, 2)

    # Giá hiện tại so với pivot
    near_pivot = abs(current_close - pivot_buy) / pivot_buy * 100 < 3 if pivot_buy > 0 else False

    # 5) Volume ratio hiện tại
    vol_ma30 = float(np.mean(b_vol[-30:])) if len(b_vol) >= 30 else 1.0
    current_vol = float(b_vol[-1])
    vol_ratio = current_vol / vol_ma30 if vol_ma30 > 0 else 0

    # ══════════════════════════════════════════════════════════════════════
    # FINAL VCP VERDICT
    # ══════════════════════════════════════════════════════════════════════
    # Chuẩn Minervini:
    #   - T-count >= 2 (ít nhất 2 contractions)
    #   - Contracting (depth giảm dần)
    #   - Tightness cuối < 15% (lý tưởng < 10%)
    #   - Volume dry-up
    #   - Uptrend (đã check ở trên)
    #   - Base length 15-325 days (3-65 tuần)
    is_vcp = (
        t_count >= 2 and
        contracting and
        tightness < 15 and
        uptrend_ok and
        15 <= base_length <= 325
    )

    # Stage classification chi tiết hơn
    if is_vcp and near_pivot:
        stage = "vcp_pivot"     # VCP hoàn chỉnh + gần pivot → sẵn sàng breakout
    elif is_vcp:
        stage = "vcp"           # VCP hoàn chỉnh, chờ tiến vào pivot
    elif t_count >= 2 and contracting:
        stage = "contracting"   # Đang co lại nhưng chưa đủ tight
    elif uptrend_ok and base_depth <= 50:
        stage = "base_forming"  # Uptrend, base đang hình thành
    else:
        stage = "no_pattern"

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
        "segments":        t_count,        # backward compat: segments → now = T-count
        "t_count":         t_count,        # new: explicit T-count
        "base_depth":      round(base_depth, 2),
        "base_length":     base_length,
        "stage":           stage,
    }


class ScreenerService:
    def __init__(self):
        self._cache: Dict[str, dict] = {}
        self._cache_time: Dict[str, datetime] = {}
        self._index_data: Optional[pd.DataFrame] = None
        self._index_fetched_at: Optional[datetime] = None
        self.CACHE_TTL = 900        # 15 phút cho mã thường (giảm churn gọi lại)
        self.INDEX_TTL = 3600       # 1 giờ cho VNINDEX

    def clear_cache(self):
        """Xóa toàn bộ screener cache (force re-analyze)."""
        self._cache.clear()
        self._cache_time.clear()
        self._index_data = None
        self._index_fetched_at = None

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
        """Chạy screener cho danh sách mã, trả về kết quả có điểm.
        Dùng semaphore để xử lý concurrent (nhanh cho mã đã có data trong store).
        """
        await self._ensure_index_data()
        results = []
        sem = asyncio.Semaphore(10)  # max 10 concurrent — FireAnt nhanh, store read instant

        async def analyze_one(ticker: str):
            async with sem:
                try:
                    result = await self._analyze_ticker(ticker)
                    if result and result.get("trend_score", 0) >= min_trend_score:
                        results.append(result)
                except Exception as e:
                    print(f"Screener error {ticker}: {e}")

        await asyncio.gather(*[analyze_one(t) for t in tickers])

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
        """Đọc lịch sử OHLCV. Ưu tiên OHLCV store (SQLite) → FireAnt → vnstock.
        """
        # ── Store first (tất cả, kể cả index) ──
        df_store = ohlcv_store.get_ohlcv(ticker if not is_index else "VNINDEX", start, end)
        if df_store is not None and len(df_store) >= 60:
            return df_store

        # ── FireAnt historical prices (index + stocks) ──
        sym_fa = "VNINDEX" if is_index else ticker.upper()
        df_fa = await self._fetch_fireant_ohlcv(sym_fa, start, end)
        if df_fa is not None and len(df_fa) >= 60:
            ohlcv_store.upsert_ohlcv(sym_fa, df_fa)
            print(f"✅ {sym_fa} via FireAnt: {len(df_fa)} rows → saved to store")
            return df_fa

        loop = asyncio.get_event_loop()
        sources = ['kbs', 'vci', 'msn'] if is_index else ['kbs', 'vci']
        for source in sources:
            await _limiter.acquire()   # 1 acquire = 1 API call
            df, stopped = await loop.run_in_executor(
                None, self._fetch_one_source, ticker, source, start, end, is_index
            )
            if df is not None and not df.empty:
                # Lưu vào SQLite store để lần sau không cần fetch lại
                sym = "VNINDEX" if is_index else ticker.upper()
                ohlcv_store.upsert_ohlcv(sym, df)
                print(f"✅ {ticker} fetched via {source}: {len(df)} rows → saved to store")
                return df
            if stopped:
                break   # raise → bỏ luôn
        return None

    async def _fetch_fireant_ohlcv(self, symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
        """Lấy historical prices từ FireAnt API (works for both stocks and index)."""
        import os
        import requests
        token = os.environ.get("FIREANT_TOKEN", "").strip()
        if not token:
            return None
        try:
            url = (
                f"https://restv2.fireant.vn/symbols/{symbol}/historical-quotes"
                f"?startDate={start}&endDate={end}&offset=0&limit=5000"
            )
            resp = requests.get(url, timeout=30, headers={
                "Authorization": f"Bearer {token}"
            })
            if resp.status_code != 200:
                print(f"⚠️  FireAnt {symbol}: HTTP {resp.status_code}")
                return None
            data = resp.json()
            if not isinstance(data, list) or not data:
                return None
            # Parse: FireAnt trả list of {date, priceOpen, priceHigh, priceLow, priceClose, ...}
            rows = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                d = item.get("date", "")
                if isinstance(d, str) and len(d) >= 10:
                    d = d[:10]  # "2024-01-15T00:00:00" → "2024-01-15"
                rows.append({
                    "date": d,
                    "open":   item.get("priceOpen", 0) or 0,
                    "high":   item.get("priceHigh", 0) or 0,
                    "low":    item.get("priceLow", 0) or 0,
                    "close":  item.get("priceClose", 0) or 0,
                    "volume": item.get("totalVolume", 0) or item.get("dealVolume", 0) or 0,
                })
            if not rows:
                return None
            df = pd.DataFrame(rows)
            df = df.sort_values("date").reset_index(drop=True)
            return df
        except Exception as e:
            print(f"⚠️  FireAnt {symbol}: {type(e).__name__}: {e}")
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
