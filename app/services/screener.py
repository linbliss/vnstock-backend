import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from app.services.market_data import market_service
from app.services import ohlcv_store

# Dùng chung RateLimiter của market_service để tránh vượt quota vnai (60/phút).
_limiter = market_service._limiter

# ── Trend Template Minervini – 8 tiêu chí ──
# RS_MIN_VN: TTCK Việt Nam mẫu nhỏ (~1600 mã, thanh khoản mỏng) → dùng 55
# thay vì chuẩn Minervini gốc 70. Giá trị có thể đổi trong alert settings.
RS_MIN_VN = 55.0

def check_trend_template(df: pd.DataFrame, rs_rating: float = 0.0,
                         current_price: float = None) -> Dict:
    """
    df: OHLCV daily data, columns: open, high, low, close, volume
    rs_rating: tính trước bằng compute_rs_rating(), truyền vào để tính c8
    current_price: nếu truyền vào (đơn vị nghìn VND, giống df.close), dùng làm
                   "giá hiện tại" thay vì close[-1] — cho phép realtime intraday.
    Trả về dict điểm từng tiêu chí và tổng điểm
    """
    if df is None or len(df) < 200:
        return {"score": 0, "criteria": {}, "passed": False}

    close = df['close'].values
    high  = df['high'].values  if 'high' in df.columns else close
    low   = df['low'].values   if 'low'  in df.columns else close
    current = float(current_price) if current_price and current_price > 0 else float(close[-1])

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
    """
    Filter corrupt VNINDEX rows (FireAnt sometimes returns close/1000).
    Align by date FIRST, then filter outliers — đảm bảo positional aligned sau khi return.
    """
    stock = stock.astype(float)
    index = index.astype(float)

    # Bước 1: Align by date (DatetimeIndex) hoặc tail-align (RangeIndex)
    stock_aligned, index_aligned = _align_by_date(stock, index)
    if len(index_aligned) == 0:
        return stock, index

    # Bước 2: Filter outliers trên index đã align
    median_idx = float(index_aligned.median())
    if median_idx > 0:
        valid_mask = (index_aligned > median_idx * 0.3) & (index_aligned < median_idx * 3)
        # Lọc đồng thời cả 2 series với cùng mask (positional aligned)
        if hasattr(valid_mask, 'values'):
            mask_vals = valid_mask.values
        else:
            mask_vals = valid_mask
        stock_aligned = stock_aligned[mask_vals]
        index_aligned = index_aligned[mask_vals]

    return stock_aligned, index_aligned


def _align_by_date(stock_close: pd.Series, index_close: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """
    Align 2 series. Ưu tiên DatetimeIndex (chính xác), fallback tail-align (xấp xỉ).

    - DatetimeIndex (cả 2): intersection theo ngày → đúng tuyệt đối
    - RangeIndex / không có ngày: lấy tail của cả 2 với độ dài min
      (giả định cùng frequency EOD, cùng ngày kết thúc — đúng với data từ ohlcv_store)
    """
    try:
        # Case 1: cả 2 đều DatetimeIndex
        if isinstance(stock_close.index, pd.DatetimeIndex) and isinstance(index_close.index, pd.DatetimeIndex):
            common = stock_close.index.intersection(index_close.index)
            if len(common) >= 63:
                return stock_close.loc[common].sort_index(), index_close.loc[common].sort_index()

        # Case 2: tail-align (default cho data từ get_ohlcv)
        n = min(len(stock_close), len(index_close))
        if n < 63:
            return stock_close, pd.Series(dtype=float)
        return (
            stock_close.iloc[-n:].reset_index(drop=True),
            index_close.iloc[-n:].reset_index(drop=True),
        )
    except Exception:
        return stock_close, index_close


def compute_rs_score(stock_close: pd.Series, index_close: Optional[pd.Series] = None) -> float:
    """
    IBD-style RS Score: weighted RELATIVE return vs benchmark (VNINDEX).

    Công thức:
      RS = 0.4×(R3m_stock − R3m_idx)
         + 0.2×(R6m_stock − R6m_idx)
         + 0.2×(R9m_stock − R9m_idx)
         + 0.2×(R12m_stock − R12m_idx)

    Lý do RELATIVE (không tuyệt đối):
      - Khi index uptrend mạnh, mã yếu hơn index vẫn có return dương cao
        → score cao "ảo" theo công thức tuyệt đối, không phản ánh leadership thực
      - IBD/Minervini chuẩn = excess return vs benchmark cùng kỳ

    Args:
      stock_close:  pd.Series với DatetimeIndex (close giá cổ phiếu)
      index_close:  pd.Series với DatetimeIndex (close VNINDEX). None → fallback tuyệt đối.

    Returns:
      RS Score thô (số có thể âm khi underperform). Cần percentile rank để thành Rating 1-99.
    """
    try:
        if len(stock_close) < 63:
            return 0.0

        # Align dates với index nếu có (đảm bảo so sánh return cùng kỳ)
        idx_close: Optional[pd.Series] = None
        if index_close is not None and len(index_close) >= 63:
            try:
                stock_aligned, idx_aligned = _align_by_date(stock_close, index_close)
                if len(stock_aligned) >= 63 and len(idx_aligned) >= 63:
                    stock_close = stock_aligned
                    idx_close = idx_aligned
            except Exception:
                pass   # rơi xuống fallback tuyệt đối

        def period_return(s: pd.Series, days: int) -> float:
            if len(s) < days:
                return 0.0
            return float((s.iloc[-1] / s.iloc[-days] - 1) * 100)

        stock_returns = [period_return(stock_close, p) for p in [63, 126, 189, 252]]

        if idx_close is not None:
            idx_returns = [period_return(idx_close, p) for p in [63, 126, 189, 252]]
            relative = [s - i for s, i in zip(stock_returns, idx_returns)]
        else:
            # Fallback: dùng absolute return (chỉ khi không có index data — không khuyến nghị)
            relative = stock_returns

        weights = [0.4, 0.2, 0.2, 0.2]
        return sum(r * w for r, w in zip(relative, weights))
    except Exception:
        return 0.0


def compute_rs_rating(stock_close: pd.Series, index_close: pd.Series) -> float:
    """
    Fallback RS Rating — chỉ dùng khi rs_ratings table chưa populate.
    Truyền cả index_close để compute_rs_score tính RELATIVE return chuẩn IBD.
    True RS Rating (percentile) phải lấy từ ohlcv_store.get_rs_rating().
    """
    try:
        stock_close, index_close = _filter_outliers(stock_close, index_close)
        if len(stock_close) < 63:
            return 50.0

        score = compute_rs_score(stock_close, index_close)
        # Fallback mapping: ép RS Score (relative) về 0-100
        # Khi outperform index nhiều → score cao → rating cao (và ngược lại)
        rs = max(1, min(99, score + 50))
        return round(rs, 1)
    except Exception:
        return 50.0


async def compute_market_rs_ratings(min_vol_ma20: int = 100_000) -> int:
    """
    ═══════════════════════════════════════════════════════════════════════
    TRUE RS RATING — Nightly Batch Job (Relative vs VNINDEX)
    ═══════════════════════════════════════════════════════════════════════

    Tính RS Rating (percentile rank) cho TOÀN BỘ thị trường.
    Chạy 1 lần/ngày (sau 16:00 hoặc khi startup nếu stale).

    Thuật toán (chuẩn IBD):
    1. Load VNINDEX 1 lần (benchmark cho relative return)
    2. Lấy tất cả mã có OHLCV data trong SQLite store
    3. Lọc mã có vol_ma20 >= min_vol_ma20 (loại mã thanh khoản kém)
    4. Tính RS Score = weighted RELATIVE return vs VNINDEX (40-20-20-20)
       → mã out-perform index → score dương cao
       → mã under-perform → score âm
    5. Sort toàn bộ theo score từ cao → thấp
    6. Gán percentile: RS_Rating = (total - rank) / total * 100
    7. Lưu vào rs_ratings table

    Returns: Số mã đã tính rating
    """
    print("🔄 Computing market-wide RS Ratings (RELATIVE vs VNINDEX, percentile)...")

    # Bước 1: Load VNINDEX 1 lần — dùng làm benchmark cho mọi mã
    # Set DatetimeIndex để align chính xác theo date (handle suspension gaps)
    end   = datetime.now().strftime("%Y-%m-%d")
    start = "2000-01-01"
    index_close: Optional[pd.Series] = None
    try:
        index_df = ohlcv_store.get_ohlcv("VNINDEX", start, end)
        if index_df is not None and len(index_df) >= 63:
            index_df = index_df.set_index(pd.to_datetime(index_df['date']))
            index_close = index_df['close'].astype(float)
            print(f"   ✅ VNINDEX loaded: {len(index_close)} days, latest={index_close.iloc[-1]:.2f}")
        else:
            print("⚠️  VNINDEX data thiếu — RS Score sẽ dùng absolute (KHÔNG chuẩn IBD)")
    except Exception as e:
        print(f"⚠️  VNINDEX load error: {e} — fallback absolute")

    # Bước 2: Lấy tất cả ticker có data trong store
    all_tickers = ohlcv_store.list_tickers()
    if not all_tickers:
        print("⚠️  RS Ratings: No tickers in OHLCV store")
        return 0

    # Loại bỏ index
    all_tickers = [t for t in all_tickers if t != "VNINDEX"]

    # Bước 3+4: Tính RS Score (RELATIVE) cho mỗi mã
    scores = []  # list of (ticker, rs_score)

    for ticker in all_tickers:
        try:
            df = ohlcv_store.get_ohlcv(ticker, start, end)
            if df is None or len(df) < 63:
                continue

            # Lọc thanh khoản: vol_ma20 >= threshold
            vol = df['volume'].values
            if len(vol) >= 20:
                vol_ma20 = float(pd.Series(vol).rolling(20).mean().iloc[-1])
                if vol_ma20 < min_vol_ma20:
                    continue

            # Set DatetimeIndex để align chính xác với VNINDEX theo date
            df = df.set_index(pd.to_datetime(df['date']))
            close = df['close'].astype(float)

            # Tính RS Score = weighted RELATIVE return vs VNINDEX
            score = compute_rs_score(close, index_close)
            scores.append((ticker, score))
        except Exception as e:
            print(f"  RS Score error {ticker}: {e}")
            continue

    if not scores:
        print("⚠️  RS Ratings: No valid scores computed")
        return 0

    # Bước 4: Sort từ cao xuống thấp
    scores.sort(key=lambda x: x[1], reverse=True)
    total = len(scores)

    # Bước 5: Gán percentile rank
    ratings = []
    for rank_idx, (ticker, score) in enumerate(scores):
        # rank_idx=0 → mã mạnh nhất → RS Rating cao nhất
        # Percentile = (total - rank) / total * 100
        # Clamp 1-99 (IBD standard)
        percentile = (total - rank_idx) / total * 100
        rs_rating = max(1, min(99, round(percentile, 1)))

        ratings.append({
            "ticker": ticker,
            "rs_score": round(score, 2),
            "rs_rating": rs_rating,
            "rank": rank_idx + 1,
            "total": total,
        })

    # Bước 6: Lưu vào database
    count = ohlcv_store.upsert_rs_ratings(ratings)
    print(f"✅ RS Ratings computed: {count} stocks ranked (top: {ratings[0]['ticker']}={ratings[0]['rs_rating']}, bottom: {ratings[-1]['ticker']}={ratings[-1]['rs_rating']})")
    return count


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


def _zigzag_swings(
    high: np.ndarray, low: np.ndarray, threshold_pct: float = 2.0,
) -> tuple:
    """
    ZigZag swing detection — chuẩn TradingView/professional VCP analysis.

    Khác fractal (window-based):
      - Fractal: high[i] == max(high[i-N:i+N+1]) → bỏ sót swings sát nhau
      - ZigZag: confirm swing khi giá đảo chiều ≥ threshold_pct% từ pivot
        → tự nhiên align với "contraction depth", bắt mọi swing có ý nghĩa

    Algorithm:
      1. Track running extreme (high tối đa hoặc low tối thiểu) tùy trend
      2. Khi đảo chiều ≥ threshold:
         - trend up + low giảm > threshold từ running high → confirm swing high
         - trend down + high tăng > threshold từ running low → confirm swing low
      3. Tail: append tentative pivot đang track (handle chưa hoàn chỉnh)

    threshold_pct: % minimum reversal để confirm swing.
      - 2.0 (default): bắt cả handle tight 2-3% — phù hợp VCP cuối cùng
      - 3.0: chỉ bắt swings sâu hơn — strict mode

    Returns: (swing_highs, swing_lows) — mỗi list of (idx, price), sort theo idx.
    Guarantee: swings xen kẽ H-L-H-L (zigzag không vi phạm).
    """
    n = len(high)
    if n < 3:
        return [], []

    threshold = threshold_pct / 100.0
    swing_highs: list = []
    swing_lows:  list = []

    # Running extremes
    cur_high_idx,  cur_high_price = 0, float(high[0])
    cur_low_idx,   cur_low_price  = 0, float(low[0])
    trend: Optional[str] = None  # 'up' | 'down' | None

    for i in range(1, n):
        hi, lo = float(high[i]), float(low[i])

        if trend is None:
            # Update both extremes
            if hi > cur_high_price:
                cur_high_idx, cur_high_price = i, hi
            if lo < cur_low_price:
                cur_low_idx,  cur_low_price  = i, lo
            # First reversal triggers initial trend
            if lo < cur_high_price * (1 - threshold):
                swing_highs.append((cur_high_idx, cur_high_price))
                cur_low_idx, cur_low_price = i, lo
                trend = 'down'
            elif hi > cur_low_price * (1 + threshold):
                swing_lows.append((cur_low_idx, cur_low_price))
                cur_high_idx, cur_high_price = i, hi
                trend = 'up'

        elif trend == 'up':
            # Trong uptrend: track new high, đợi đảo chiều xuống ≥ threshold
            if hi > cur_high_price:
                cur_high_idx, cur_high_price = i, hi
            if lo < cur_high_price * (1 - threshold):
                swing_highs.append((cur_high_idx, cur_high_price))
                cur_low_idx, cur_low_price = i, lo
                trend = 'down'

        else:   # trend == 'down'
            if lo < cur_low_price:
                cur_low_idx, cur_low_price = i, lo
            if hi > cur_low_price * (1 + threshold):
                swing_lows.append((cur_low_idx, cur_low_price))
                cur_high_idx, cur_high_price = i, hi
                trend = 'up'

    # ── Tail: append tentative pivot (swing đang hình thành chưa confirmed) ──
    # Cho phép walker tạo contraction cuối nếu đủ duration & higher-lows
    if trend == 'up':
        # Currently in uptrend, track running high — append as last swing high
        if not swing_highs or swing_highs[-1][0] != cur_high_idx:
            swing_highs.append((cur_high_idx, cur_high_price))
    elif trend == 'down':
        if not swing_lows or swing_lows[-1][0] != cur_low_idx:
            swing_lows.append((cur_low_idx, cur_low_price))

    return swing_highs, swing_lows


# Backward compat alias — nếu code khác còn dùng tên cũ
def _find_swing_points(high: np.ndarray, low: np.ndarray, order: int = 5) -> tuple:
    """DEPRECATED — gọi _zigzag_swings (threshold mặc định 2%)."""
    return _zigzag_swings(high, low, threshold_pct=2.0)


def _find_contractions(
    swing_highs: list, swing_lows: list,
    vol: np.ndarray, close: np.ndarray, vol_ma50_full: float,
) -> list:
    """
    Tìm contractions VCP — Sequential Walker.

    Thuật toán:
      1. Merge swing_highs + swing_lows, sort theo (idx, type) — H trước L cùng idx
      2. Walk tuần tự với state machine:
         - Gặp H: nhận làm pending_high (override nếu price cao hơn pending,
                  nghĩa là pattern chưa hình thành L)
         - Gặp L (sau pending_high):
           * price >= pending_high.price → bỏ qua
           * duration < 2 → bỏ qua (chờ L tiếp theo, không reset pending)
           * Đáp ứng → tạo contraction, reset pending

    Lưu ý — KHÔNG enforce Higher Lows ở mức swing:
      VCP textbook nói "Higher Lows" là đặc trưng IDEAL nhưng dữ liệu thực
      thường có shake-out (Minervini gọi "undercut & rally") — T₂ thủng nhẹ
      L của T₁ để rũ weak hands. Strict HL → reject T₂ → chuỗi T₃,T₄ cũng
      mắc kẹt vì last_low không update. Kết quả: chỉ thấy T1.

    Higher Lows được track như METADATA (`higher_low_vs_prev`) để frontend
    hiển thị quality, KHÔNG dùng làm gate. Gate chính là depth contraction
    (xử lý ở pair_reductions trong detect_vcp).

    Args:
      swing_highs / swing_lows: [(idx_local_to_analysis, price)]
      vol, close:    arrays của analysis_zone
      vol_ma50_full: MA50 volume trên TOÀN DATASET

    Returns:
      list contractions tuần tự H₁→L₁→H₂→L₂→... với H_i ≤ H_{i-1}.
      Mỗi item có `higher_low_vs_prev` (bool) cho post-hoc quality check.
    """
    if not swing_highs or not swing_lows:
        return []

    # Merge events, sort theo idx (H trước L nếu cùng idx để bắt cặp đúng)
    events = (
        [(i, p, 'H') for i, p in swing_highs] +
        [(i, p, 'L') for i, p in swing_lows]
    )
    events.sort(key=lambda x: (x[0], 0 if x[2] == 'H' else 1))

    contractions = []
    pending_high  = None     # (idx, price) — đỉnh đang chờ ghép
    prev_low      = None     # tracking để metadata higher_low_vs_prev

    for idx, price, typ in events:
        if typ == 'H':
            # Update pending_high khi chưa có HOẶC đỉnh mới cao hơn
            if pending_high is None or price > pending_high[1]:
                pending_high = (idx, price)
            continue

        # typ == 'L'
        if pending_high is None:
            continue
        sh_idx, sh_price = pending_high

        if price >= sh_price:
            continue

        duration = idx - sh_idx
        if duration < 2:
            continue   # Quá ngắn — đợi L tiếp theo, không reset pending

        depth = (sh_price - price) / sh_price * 100

        # Volume tại trough (3 phiên quanh đáy) so với MA50 toàn dataset
        trough_start = max(0, idx - 1)
        trough_end   = min(len(vol), idx + 2)
        trough_vol_slice = vol[trough_start:trough_end]
        trough_avg_vol   = float(np.mean(trough_vol_slice)) if len(trough_vol_slice) > 0 else 0
        is_volume_dry    = trough_avg_vol < (vol_ma50_full * 0.6) if vol_ma50_full > 0 else False

        # UP/DOWN volume trong segment [sh_idx, idx] (institutional accumulation)
        seg_end = min(idx + 1, len(close))
        up_vol_sum, down_vol_sum = 0.0, 0.0
        for i in range(sh_idx + 1, seg_end):
            if i >= len(vol) or i >= len(close):
                break
            chg = close[i] - close[i - 1]
            if chg > 0:
                up_vol_sum += float(vol[i])
            elif chg < 0:
                down_vol_sum += float(vol[i])
        down_up_ratio = down_vol_sum / up_vol_sum if up_vol_sum > 0 else 999.0

        # Higher Lows metadata (quality indicator, not a gate)
        higher_low = prev_low is None or price > prev_low

        contractions.append({
            "high_idx":         sh_idx,
            "high_price":       sh_price,
            "low_idx":          idx,
            "low_price":        price,
            "depth":            depth,
            "duration":         duration,
            "trough_avg_vol":   trough_avg_vol,
            "is_volume_dry":    is_volume_dry,
            "up_vol_sum":       up_vol_sum,
            "down_vol_sum":     down_vol_sum,
            "down_up_ratio":    down_up_ratio,
            "higher_low_vs_prev": higher_low,
        })
        prev_low     = price
        pending_high = None        # Đợi đỉnh tiếp theo

    return contractions


def _empty_vcp_result(stage: str, **extra) -> Dict:
    """Skeleton kết quả VCP rỗng (fail uptrend / base filter)."""
    base = {
        "is_vcp": False,
        "is_vcp_strict": False,
        "is_vcp_loose": False,
        "contracting": False,
        "vol_contracting": False,
        "uptrend_ok": False,
        "tightness": 0.0,
        "pivot_buy": 0.0,
        "near_pivot": False,
        "above_pivot": False,
        "vol_ratio": 0.0,
        "vol_ratio_ma50": 0.0,
        "vol_confirmed": False,
        "vol_confirmed_strict": False,
        "segments": 0,
        "t_count": 0,
        "base_depth": 0.0,
        "base_length": 0,
        "stop_loss": 0.0,
        "handle_low": 0.0,
        "hold_above_ma50": False,
        "stage": stage,
        "stage_strict": stage,
        "stage_loose": stage,
        "contractions": [],
        "base_start_date": None,
        "pivot_date": None,
    }
    base.update(extra)
    return base


def detect_vcp(df: pd.DataFrame, current_price: float = None) -> Dict:
    """
    Nhận diện VCP (Volatility Contraction Pattern) — chuẩn Minervini.
    Tính cả 2 mode đồng thời: STRICT (textbook) và LOOSE (relaxed cho VN).

    STRICT (Minervini textbook):
      - T-count >= 3
      - Mỗi cặp consecutive: depth phải giảm ≥ 50% (T2 ≤ 0.5×T1, T3 ≤ 0.5×T2 ...)
      - Tightness (handle) < 8%
      - Handle duration ≥ 5 phiên (1 tuần)
      - Volume MUST: trough giảm dần qua các T VÀ handle khô (< 60% MA50 toàn dataset)
      - DOWN volume < UP volume trong base (institutional accumulation)
      - Hold above MA50 ≥ 70% bars trong nửa sau base
      - Base length ≥ 35 ngày (5+ tuần)
      - Breakout volume ≥ 1.5× MA50

    LOOSE (relaxed cho VN, vẫn chặt hơn bản cũ):
      - T-count >= 2
      - Pair-wise: tối đa 1 violation (depth sau >= depth trước * 1.0)
        HOẶC overall T_cuối < 0.6 × T_đầu
      - Tightness < 12%
      - Handle duration ≥ 3 phiên
      - Volume contracting OR handle khô
      - Base length ≥ 25 ngày
      - Breakout volume ≥ 1.3× MA50

    Output thêm:
      - contractions: list [{t_index, high_idx, high_price, low_idx, low_price,
                             depth_pct, duration_bars, ..., date_high, date_low,
                             passes_strict_reduction, passes_loose_reduction}]
      - stop_loss = handle_low × 0.985
      - base_start_date, pivot_date (ISO yyyy-mm-dd)
    """
    if df is None or len(df) < 130:
        return _empty_vcp_result("unknown")

    close  = df['close'].astype(float).values
    high   = df['high'].astype(float).values
    low    = df['low'].astype(float).values
    volume = df['volume'].astype(float).values
    n = len(close)

    # ── Lấy dates_arr robust (Bug 3 fix) ──
    # df có thể có DatetimeIndex (set bởi caller) HOẶC RangeIndex + column 'date' (từ get_ohlcv)
    dates_arr: Optional[np.ndarray] = None
    try:
        if 'date' in df.columns:
            dates_arr = pd.to_datetime(df['date']).dt.strftime("%Y-%m-%d").values
        elif isinstance(df.index, pd.DatetimeIndex):
            dates_arr = df.index.strftime("%Y-%m-%d").values
    except Exception:
        dates_arr = None

    def _idx_to_date(global_idx: int) -> Optional[str]:
        if dates_arr is None:
            return None
        if 0 <= global_idx < len(dates_arr):
            return str(dates_arr[global_idx])
        return None

    # ══════════════════════════════════════════════════════════════════════
    # STAGE 2 FILTER (8 tiêu chí Minervini Trend Template — phần MA stack)
    # ══════════════════════════════════════════════════════════════════════
    current_close = float(current_price) if current_price and current_price > 0 else float(close[-1])
    ma50_arr  = pd.Series(close).rolling(50).mean().values
    ma150_arr = pd.Series(close).rolling(150).mean().values
    ma200_arr = pd.Series(close).rolling(200).mean().values
    ma50  = float(ma50_arr[-1])  if not np.isnan(ma50_arr[-1])  else 0
    ma150 = float(ma150_arr[-1]) if not np.isnan(ma150_arr[-1]) else 0
    ma200 = float(ma200_arr[-1]) if not np.isnan(ma200_arr[-1]) else 0

    price_12m = float(close[-252]) if n >= 252 else float(close[0])
    price_6m  = float(close[-126]) if n >= 126 else float(close[0])
    gain_12m  = (current_close / price_12m - 1) * 100 if price_12m > 0 else 0
    gain_6m   = (current_close / price_6m - 1) * 100 if price_6m > 0 else 0

    uptrend_ok = (
        current_close > ma150 and
        (gain_12m >= 30 or gain_6m >= 20)
    )
    # MA stack chặt hơn (cho strict): price > MA50 > MA150 > MA200
    ma_stack_ok = (
        current_close > ma50 > 0 and
        ma50 > ma150 > 0 and
        ma150 > ma200 > 0
    )

    if not uptrend_ok:
        return _empty_vcp_result("no_uptrend", uptrend_ok=False)

    # ══════════════════════════════════════════════════════════════════════
    # BASE PATTERN — scan FULL 200 ngày, KHÔNG cắt từ peak max
    # ══════════════════════════════════════════════════════════════════════
    # CRITICAL: Code cũ cắt analysis_zone từ argmax(b_high) → mọi contraction
    # TRƯỚC peak max bị mất. Trong uptrend (GMD/MBB/PHR), peak max thường là
    # đỉnh gần nhất → tất cả T trước đó bị xóa → chỉ còn T cuối làm "T1".
    # Fix: Scan zigzag trên TOÀN base_window, lọc contractions theo thời gian.
    base_window = min(200, n)
    b_close = close[-base_window:]
    b_high  = high[-base_window:]
    b_low   = low[-base_window:]
    b_vol   = volume[-base_window:]
    base_offset_global = n - base_window  # idx(local) + offset = idx(global df)

    # Sanity check: overall drawdown trong 200 ngày (loại bear market sâu)
    overall_max_idx   = int(np.argmax(b_high))
    overall_max_price = float(b_high[overall_max_idx])
    overall_low       = float(np.min(b_low[overall_max_idx:]))
    overall_drawdown  = (overall_max_price - overall_low) / overall_max_price * 100
    if overall_drawdown > 60:
        return _empty_vcp_result(
            "base_too_deep",
            uptrend_ok=True,
            base_depth=round(overall_drawdown, 2),
            base_length=base_window - overall_max_idx,
        )

    # vol_ma50 trên TOÀN dataset (không phải chỉ analysis_vol)
    vol_ma50_full = float(pd.Series(volume).rolling(50).mean().iloc[-1]) if n >= 50 else float(np.mean(volume))
    if np.isnan(vol_ma50_full) or vol_ma50_full <= 0:
        vol_ma50_full = float(np.mean(volume))

    # ══════════════════════════════════════════════════════════════════════
    # SWING POINTS + CONTRACTIONS — scan toàn 200 ngày
    # ══════════════════════════════════════════════════════════════════════
    swing_highs, swing_lows = _zigzag_swings(b_high, b_low, threshold_pct=2.0)

    all_contractions = _find_contractions(
        swing_highs, swing_lows, b_vol, b_close, vol_ma50_full,
    )

    # Lọc contractions theo thời gian: chỉ giữ những cái có low_idx
    # trong 150 phiên gần nhất (~7 tháng) → loại contractions từ chu kỳ cũ
    RECENT_LOOKBACK = 150
    cutoff_idx = max(0, base_window - RECENT_LOOKBACK)
    contractions = [c for c in all_contractions if c["low_idx"] >= cutoff_idx]
    t_count = len(contractions)

    # Base properties: tính từ FIRST contraction (start of VCP base)
    if t_count >= 1:
        base_high_idx_local = contractions[0]["high_idx"]
        base_high_price     = contractions[0]["high_price"]
        base_length         = base_window - base_high_idx_local
        base_start_global   = base_offset_global + base_high_idx_local
        # base_depth = max contraction depth (= T1 thường là sâu nhất)
        base_depth = max(c["depth"] for c in contractions)
    else:
        # Không có contraction nào → fallback dùng peak max
        base_high_idx_local = overall_max_idx
        base_high_price     = overall_max_price
        base_length         = base_window - overall_max_idx
        base_start_global   = base_offset_global + overall_max_idx
        base_depth          = overall_drawdown

    # Filter base_depth quá nông (không có contraction thực sự)
    if base_depth < 8:
        return _empty_vcp_result(
            "base_too_shallow",
            uptrend_ok=True,
            base_depth=round(base_depth, 2),
            base_length=base_length,
            base_start_date=_idx_to_date(base_start_global),
        )

    # Để hold_above_ma50 và metadata indices reference vào b_close,
    # analysis_offset_global giờ = base_offset_global (toàn base window)
    analysis_offset_global = base_offset_global
    analysis_close = b_close
    analysis_vol   = b_vol

    # ══════════════════════════════════════════════════════════════════════
    # PAIR-WISE REDUCTION ANALYSIS
    # ══════════════════════════════════════════════════════════════════════
    # Mỗi cặp consecutive: reduction% = (T_i.depth - T_{i+1}.depth) / T_i.depth × 100
    # STRICT: tất cả reductions ≥ 50%
    # LOOSE: tối đa 1 violation (T_{i+1}.depth >= T_i.depth, tức reduction <= 0)
    pair_reductions = []
    for i in range(t_count - 1):
        d1 = contractions[i]["depth"]
        d2 = contractions[i + 1]["depth"]
        reduction = (d1 - d2) / d1 * 100 if d1 > 0 else 0
        pair_reductions.append(reduction)
        contractions[i + 1]["reduction_from_prev_pct"] = round(reduction, 1)

    strict_pairwise_ok = bool(pair_reductions) and all(r >= 50 for r in pair_reductions)
    loose_violations = sum(1 for r in pair_reductions if r < 0)
    loose_pairwise_ok = loose_violations <= 1
    overall_loose_ok = (
        t_count >= 2 and
        contractions[-1]["depth"] < contractions[0]["depth"] * 0.6
    )
    contracting_strict = strict_pairwise_ok
    contracting_loose  = loose_pairwise_ok or overall_loose_ok

    # ══════════════════════════════════════════════════════════════════════
    # VOLUME CONTRACTION
    # ══════════════════════════════════════════════════════════════════════
    last_dry = contractions[-1]["is_volume_dry"] if t_count >= 1 else False
    trough_vols = [c["trough_avg_vol"] for c in contractions]
    vol_decline_violations = 0
    for i in range(len(trough_vols) - 1):
        if trough_vols[i + 1] > trough_vols[i] * 1.1:   # tolerance 10%
            vol_decline_violations += 1
    # Cho phép 1 violation khi t_count >= 4 (chuỗi dài, micro-bumps tự nhiên)
    max_vol_violations_strict = 1 if t_count >= 4 else 0
    vol_decreasing_strict = vol_decline_violations <= max_vol_violations_strict
    vol_decreasing_loose  = vol_decline_violations <= 1

    # STRICT: PHẢI cả 2: trough volume giảm dần AND handle khô
    vol_contracting_strict = vol_decreasing_strict and last_dry and t_count >= 2
    # LOOSE: 1 trong 2
    vol_contracting_loose  = (vol_decreasing_loose or last_dry) and t_count >= 2

    # DOWN/UP volume tổng thể trên TOÀN ANALYSIS_ZONE (không chỉ trong contractions)
    # Bao gồm cả rally legs giữa các T → bức tranh accumulation đầy đủ
    total_up_vol   = 0.0
    total_down_vol = 0.0
    for i in range(1, len(analysis_close)):
        chg = analysis_close[i] - analysis_close[i - 1]
        if chg > 0:
            total_up_vol += float(analysis_vol[i])
        elif chg < 0:
            total_down_vol += float(analysis_vol[i])
    base_down_up_ratio = total_down_vol / total_up_vol if total_up_vol > 0 else 999.0
    healthy_supply_demand = base_down_up_ratio < 1.0   # Minervini: down vol < up vol

    # ══════════════════════════════════════════════════════════════════════
    # HOLD ABOVE MA50 IN 2ND HALF OF BASE
    # ══════════════════════════════════════════════════════════════════════
    second_half_start = base_high_idx_local + max(base_length // 2, 1)
    if second_half_start < base_window:
        prices_2h = b_close[second_half_start:]
        ma50_in_base = ma50_arr[-base_window:][second_half_start:]
        valid_pairs = [(p, m) for p, m in zip(prices_2h, ma50_in_base) if not np.isnan(m)]
        if valid_pairs:
            days_above = sum(1 for p, m in valid_pairs if p >= m)
            hold_ratio = days_above / len(valid_pairs)
            hold_above_ma50 = hold_ratio >= 0.7
        else:
            hold_above_ma50 = False
    else:
        hold_above_ma50 = True   # base ngắn quá, không penalty

    # ══════════════════════════════════════════════════════════════════════
    # TIGHTNESS, HANDLE, PIVOT (FROZEN), STOP LOSS, FRESHNESS
    # ══════════════════════════════════════════════════════════════════════
    # CRITICAL: Pivot = đỉnh handle CỐ ĐỊNH, KHÔNG trượt theo giá hiện tại
    # (Bug cũ: pivot = max(handle_high, recent_high) → khi giá breakout vượt handle,
    #  recent_high tăng theo → pivot tăng theo → above_pivot KHÔNG BAO GIỜ true)
    if t_count >= 1:
        last_c     = contractions[-1]
        tightness  = last_c["depth"]
        handle_low = last_c["low_price"]
        handle_dur = last_c["duration"]
        pivot_high = last_c["high_price"]   # ← FROZEN tại đỉnh handle
        # Freshness: handle phải gần hiện tại (≤ 25 phiên = 5 tuần)
        last_high_global   = analysis_offset_global + last_c["high_idx"]
        days_since_handle  = (n - 1) - last_high_global
        pivot_fresh        = days_since_handle <= 25
    else:
        # Không có contraction → fallback dùng đỉnh 20 phiên (chỉ áp dụng khi t_count=0)
        recent_h   = float(np.max(b_high[-20:]))
        recent_l   = float(np.min(b_low[-20:]))
        tightness  = (recent_h - recent_l) / recent_h * 100 if recent_h > 0 else 0
        handle_low = recent_l
        handle_dur = 0
        pivot_high = recent_h
        days_since_handle = 0
        pivot_fresh = False

    pivot_buy = round(pivot_high, 2)
    stop_loss = round(handle_low * 0.985, 2)   # -1.5% dưới handle low

    diff_pivot_pct = (current_close - pivot_buy) / pivot_buy * 100 if pivot_buy > 0 else 0
    near_pivot  = abs(diff_pivot_pct) < 3 if pivot_buy > 0 else False
    above_pivot = diff_pivot_pct > 0 if pivot_buy > 0 else False

    # First contraction depth check (Minervini textbook): T₁ phải 10-50%
    # < 10%: chưa có correction đủ để gọi là contraction thực
    # > 50%: cú giảm quá sâu, không phải VCP base mà là recovery
    first_depth_ok = (
        10 <= contractions[0]["depth"] <= 50
        if contractions else False
    )

    # ══════════════════════════════════════════════════════════════════════
    # VOLUME RATIO HIỆN TẠI (vs MA50 toàn dataset)
    # ══════════════════════════════════════════════════════════════════════
    current_vol = float(volume[-1])
    vol_ratio_ma50 = current_vol / vol_ma50_full if vol_ma50_full > 0 else 0
    # Backward compat: vol_ratio so với MA30 trong base
    vol_ma30_local = float(np.mean(b_vol[-30:])) if len(b_vol) >= 30 else 1.0
    vol_ratio_legacy = current_vol / vol_ma30_local if vol_ma30_local > 0 else 0
    vol_confirmed_strict = vol_ratio_ma50 >= 1.5
    vol_confirmed_loose  = vol_ratio_ma50 >= 1.3

    # ══════════════════════════════════════════════════════════════════════
    # FINAL VCP VERDICT — STRICT vs LOOSE
    # ══════════════════════════════════════════════════════════════════════
    is_vcp_strict = (
        uptrend_ok and ma_stack_ok and
        t_count >= 3 and
        first_depth_ok and        # T₁ ∈ [10%, 50%]
        contracting_strict and
        tightness < 8 and
        handle_dur >= 5 and
        vol_contracting_strict and
        healthy_supply_demand and
        hold_above_ma50 and
        base_length >= 35 and
        pivot_fresh               # handle ≤ 25 phiên gần nhất
    )

    is_vcp_loose = (
        uptrend_ok and
        t_count >= 2 and
        first_depth_ok and        # T₁ ∈ [10%, 50%] (kể cả loose)
        contracting_loose and
        tightness < 12 and
        handle_dur >= 3 and
        vol_contracting_loose and
        base_length >= 25 and
        pivot_fresh               # handle phải mới (loose vẫn cần)
    )

    # Stage classification per mode
    def _stage(is_v: bool) -> str:
        if is_v and above_pivot: return "breakout"
        if is_v and near_pivot:  return "vcp_pivot"
        if is_v:                 return "vcp"
        if t_count >= 2 and (contracting_loose):  return "contracting"
        if uptrend_ok and base_depth <= 50:       return "base_forming"
        return "no_pattern"

    stage_strict = _stage(is_vcp_strict)
    stage_loose  = _stage(is_vcp_loose)

    # ══════════════════════════════════════════════════════════════════════
    # CONTRACTIONS METADATA — cho frontend chart annotations
    # ══════════════════════════════════════════════════════════════════════
    contractions_out = []
    for i, c in enumerate(contractions):
        gh_idx = analysis_offset_global + c["high_idx"]
        gl_idx = analysis_offset_global + c["low_idx"]
        # Reduction so với T trước (T1 không có)
        red_prev = c.get("reduction_from_prev_pct")
        passes_strict = red_prev is None or red_prev >= 50   # T1 luôn pass
        passes_loose  = red_prev is None or red_prev >= 0
        contractions_out.append({
            "t_index":        i + 1,
            "high_idx":       gh_idx,
            "high_price":     round(c["high_price"], 2),
            "low_idx":        gl_idx,
            "low_price":      round(c["low_price"], 2),
            "depth_pct":      round(c["depth"], 2),
            "duration_bars":  c["duration"],
            "trough_avg_vol": int(c["trough_avg_vol"]),
            "is_volume_dry":  c["is_volume_dry"],
            "down_up_ratio":  round(c["down_up_ratio"], 2),
            "reduction_from_prev_pct": round(red_prev, 1) if red_prev is not None else None,
            "passes_strict_reduction": passes_strict,
            "passes_loose_reduction":  passes_loose,
            "higher_low_vs_prev":      bool(c.get("higher_low_vs_prev", True)),
            "date_high":      _idx_to_date(gh_idx),
            "date_low":       _idx_to_date(gl_idx),
        })

    pivot_date = (
        contractions_out[-1]["date_high"] if contractions_out
        else _idx_to_date(n - 1)
    )

    return {
        # ── Legacy (default = loose for backward compat) ──────────────────
        "is_vcp":          is_vcp_loose,
        "stage":           stage_loose,
        "contracting":     contracting_loose,
        "vol_contracting": vol_contracting_loose,
        "vol_ratio":       round(vol_ratio_legacy, 2),
        "vol_confirmed":   vol_confirmed_loose,
        "segments":        t_count,

        # ── New: dual-mode flags ──────────────────────────────────────────
        "is_vcp_strict":   is_vcp_strict,
        "is_vcp_loose":    is_vcp_loose,
        "stage_strict":    stage_strict,
        "stage_loose":     stage_loose,
        "vol_confirmed_strict": vol_confirmed_strict,

        # ── Common metrics ────────────────────────────────────────────────
        "uptrend_ok":      uptrend_ok,
        "ma_stack_ok":     ma_stack_ok,
        "tightness":       round(tightness, 2),
        "pivot_buy":       pivot_buy,
        "near_pivot":      near_pivot,
        "above_pivot":     above_pivot,
        "vol_ratio_ma50":  round(vol_ratio_ma50, 2),
        "t_count":         t_count,
        "base_depth":      round(base_depth, 2),
        "base_length":     base_length,
        "stop_loss":       stop_loss,
        "handle_low":      round(handle_low, 2),
        "handle_duration": handle_dur,
        "hold_above_ma50": hold_above_ma50,
        "down_up_ratio":   round(base_down_up_ratio, 2),
        "healthy_supply_demand": healthy_supply_demand,
        "pivot_fresh":          pivot_fresh,
        "days_since_handle":    int(days_since_handle),
        "first_depth_ok":       first_depth_ok,
        "first_depth_pct":      round(contractions[0]["depth"], 2) if contractions else 0.0,

        # ── Detailed contractions for chart annotations ───────────────────
        "contractions":      contractions_out,
        "base_start_date":   _idx_to_date(base_start_global),
        "pivot_date":        pivot_date,
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
        # Kiểm tra cache — TTL ngắn trong giờ giao dịch để bám sát volume intraday
        now = datetime.now()
        is_trading_hour = (
            now.weekday() < 5 and (
                (now.hour == 9  and now.minute >= 0) or
                (10 <= now.hour <= 11) or
                (now.hour == 11 and now.minute <= 30) or
                (13 <= now.hour <= 14) or
                (now.hour == 15 and now.minute <= 1)
            )
        )
        cache_ttl = 60 if is_trading_hour else self.CACHE_TTL
        if ticker in self._cache:
            cached_time = self._cache_time.get(ticker)
            if cached_time and (now - cached_time).seconds < cache_ttl:
                return self._cache[ticker]

        # Lấy toàn bộ lịch sử có sẵn – càng nhiều càng tin cậy cho MA200, RS
        end   = now.strftime("%Y-%m-%d")
        start = "2000-01-01"

        await self._ensure_index_data()
        df = await self._fetch_history_async(ticker, start, end, is_index=False)

        if df is None or len(df) < 60:
            return None

        # ── Trộn VOLUME intraday vào row cuối ──
        # Store chỉ update sau 16:00; trong giờ giao dịch volume[-1] là T-1
        # → vol_ratio sai (luôn là tỉ lệ ngày hôm qua).
        #
        # CHIẾN LƯỢC ĐƠN GIẢN: chỉ overwrite VOLUME của row cuối với cumulative
        # volume hôm nay từ market_service.quotes. KHÔNG append row mới và
        # KHÔNG đè close — tránh thay đổi length df (lệch index gain_12m,
        # gain_6m, MA50/150/200) và tránh phá swing-point detection.
        #
        # Hệ quả:
        # ✅ vol_ratio = today's volume / vol_ma30 → đúng realtime
        # ✅ uptrend, MA stack, gain_12m, swing points → ổn định như EOD
        # ⚠️ close[-1] vẫn là T-1 close — nhưng alert/UI dùng quote.price
        #    (đã realtime), nên không bị ảnh hưởng.
        try:
            q_intra = market_service.quotes.get(ticker, {})
            intra_vol = int(q_intra.get("volume") or 0)
            if is_trading_hour and intra_vol > 0 and len(df) > 0:
                df = df.copy()
                df.at[df.index[-1], 'volume'] = intra_vol
        except Exception as e:
            print(f"⚠️  Intraday volume merge {ticker}: {type(e).__name__}: {e}")

        # RS Rating: đọc từ rs_ratings table (percentile rank, nightly batch)
        # Fallback: compute_rs_rating() nếu chưa có dữ liệu batch
        rs_rating = 50.0
        rs_line_val = 50.0
        stored_rs = ohlcv_store.get_rs_rating(ticker)
        if stored_rs:
            rs_rating = stored_rs["rs_rating"]
        elif self._index_data is not None and len(self._index_data) >= 60:
            # Set DatetimeIndex để align chuẩn theo date (handle suspension gaps)
            stk = df.set_index(pd.to_datetime(df['date']))['close']
            idx = self._index_data.set_index(pd.to_datetime(self._index_data['date']))['close']
            rs_rating = compute_rs_rating(stk, idx)
        if self._index_data is not None and len(self._index_data) >= 60:
            rs_line_val = compute_rs_line(
                df['close'], self._index_data['close']
            )

        # Giá hiện tại từ cache quotes (REALTIME) — truyền vào VCP/Trend
        # để mọi check (c1-c7, near_pivot, uptrend) phản ánh giá intraday.
        quote = market_service.quotes.get(ticker, {})
        current_price = float(quote.get("price", df['close'].iloc[-1]))
        change_pct    = float(quote.get("change_pct", 0))
        volume        = int(quote.get("volume", 0))

        # Quy đổi sang nghìn VND để khớp đơn vị df.close
        cur_kvnd = current_price / 1000.0 if current_price > 1000 else current_price

        # Trend Template (c8 dùng rs_rating IBD ≥ 55), c1/c3/c5/c6/c7 dùng giá realtime
        trend = check_trend_template(df, rs_rating=rs_rating, current_price=cur_kvnd)

        # VCP — current_price cho near_pivot + uptrend check
        vcp = detect_vcp(df, current_price=cur_kvnd)

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
