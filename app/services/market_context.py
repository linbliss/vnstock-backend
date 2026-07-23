"""market_context — LAYER 0 của Smart Money Platform (xem docs/smart-money-design.md).

Gắn NGỮ CẢNH cho mọi tín hiệu order flow: cùng một "large buy" mang ý nghĩa khác nhau ở
kháng cự vs hỗ trợ, trong uptrend vs downtrend, khi khối ngoại mua vs bán. Đây là lớp
biến hệ từ "đánh giá chỉ báo độc lập" thành "hiểu hành vi dòng tiền".

Nguồn dữ liệu (đều ĐÃ CÓ, chỉ nối lại):
  • ohlcv_store   — nến NGÀY → trend, MA20/50/100, hỗ trợ/kháng cự (swing), breakout.
  • tape intraday — giá hiện tại, VWAP, POC/Value Area (qua order_flow).
  • shark_history — khối ngoại / tự doanh ròng theo ngày (FireAnt) → foreign_dir/dealer_dir.
  • đồng hồ phiên — pha ATO/liên tục/ATC/PLO theo giờ của tick cuối (deterministic cho replay).

Thuần & phòng thủ: thiếu nguồn nào thì field đó về trung tính, KHÔNG ném lỗi.
Đơn vị giá: kVND ở CẢ intraday lẫn OHLCV ngày (đã kiểm) → so sánh trực tiếp.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional


# ── Schema (Context + Event) — dùng chung cho Layer 2/3 và persist ────────────────────
@dataclass
class Context:
    trend: str = "unknown"           # uptrend|downtrend|sideway|unknown
    regime: str = "unknown"          # trending_up|trending_down|sideway|unknown — TRẠNG THÁI
    #                                  thị trường (Layer 0). Quyết định trọng số detector ở
    #                                  Decision Engine: absorption trong sideway ≠ trong uptrend.
    ma_state: str = ""               # vd "MA20>MA50>MA100"
    location: str = "mid"            # support|resistance|inside_va|at_poc|breakout|mid
    vwap_side: str = "at"            # above|below|at
    session_phase: str = "continuous"  # pre|ato|continuous|lunch|atc|plo|post
    foreign_dir: float = 0.0         # -1..1 (dương = ngoại mua ròng)
    dealer_dir: float = 0.0          # -1..1 (dương = tự doanh mua ròng)
    # Tham chiếu định lượng (hiển thị & giải thích)
    price: Optional[float] = None
    ma20: Optional[float] = None
    ma50: Optional[float] = None
    ma100: Optional[float] = None
    support: Optional[float] = None
    resistance: Optional[float] = None
    poc: Optional[float] = None
    va_low: Optional[float] = None
    va_high: Optional[float] = None
    vwap: Optional[float] = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Event:
    """Sự kiện dòng tiền có ngữ cảnh — đơn vị để chấm điểm (Layer 3) & backtest (Phase D)."""
    type: str                        # "absorption" | "distribution" | ...
    ts: str
    strength: float                  # -1..1 (dương = bullish)
    confidence: float                # 0..1
    context: Context
    evidence: List[str] = field(default_factory=list)
    algo_version: int = 1

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


# ── Helpers ───────────────────────────────────────────────────────────────────────────
def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _ma(closes: List[float], n: int) -> Optional[float]:
    return round(sum(closes[-n:]) / n, 3) if len(closes) >= n else None


def _rel_close(a: float, b: float, tol: float = 0.01) -> bool:
    """a ≈ b trong sai số tương đối tol (mặc định 1%)."""
    return b != 0 and abs(a - b) / abs(b) < tol


def _classify_trend(m20: Optional[float], m50: Optional[float],
                    m100: Optional[float]) -> tuple[str, str]:
    """(trend, ma_state). Uptrend cần MA xếp bậc VÀ MA50 tách khỏi MA100 (>1%); nếu
    MA50≈MA100 (case HDB) → sideway dù MA20 nhỉnh hơn."""
    present = [x for x in (m20, m50, m100) if x is not None]
    if len(present) < 2:
        return "unknown", ""
    # ma_state mô tả
    parts = []
    labels = [("MA20", m20), ("MA50", m50), ("MA100", m100)]
    labels = [(lb, v) for lb, v in labels if v is not None]
    for i in range(len(labels) - 1):
        op = ">" if labels[i][1] > labels[i + 1][1] else ("<" if labels[i][1] < labels[i + 1][1] else "=")
        parts.append(f"{labels[i][0]}{op}{labels[i + 1][0]}")
    ma_state = " ".join(parts)

    if m20 is None or m50 is None or m100 is None:
        # fallback 2 MA
        a, b = (m20 or m50), (m50 if m20 else m100)
        if a and b:
            if a > b and not _rel_close(a, b):
                return "uptrend", ma_state
            if a < b and not _rel_close(a, b):
                return "downtrend", ma_state
        return "sideway", ma_state

    if _rel_close(m50, m100):            # MA50≈MA100 → chưa có xu hướng trung hạn
        return "sideway", ma_state
    if m20 > m50 > m100:
        return "uptrend", ma_state
    if m20 < m50 < m100:
        return "downtrend", ma_state
    return "sideway", ma_state


def _session_phase(ts: str) -> str:
    """Pha phiên theo GIỜ của tick cuối (deterministic, dùng được cho replay lịch sử).
    HOSE: ATO 09:00–09:15 · liên tục · nghỉ trưa 11:30–13:00 · ATC 14:30–14:45 · PLO 14:45–."""
    hhmm = ts[11:16] if len(ts) >= 16 else ""
    if not hhmm:
        return "continuous"
    if hhmm < "09:00":
        return "pre"
    if hhmm < "09:15":
        return "ato"
    if hhmm < "11:30":
        return "continuous"
    if hhmm < "13:00":
        return "lunch"
    if hhmm < "14:30":
        return "continuous"
    if hhmm <= "14:45":       # khớp ATC in dấu 14:45:xx → vẫn là phiên ATC
        return "atc"
    if hhmm <= "15:00":
        return "plo"
    return "post"


def _support_resistance(highs: List[float], lows: List[float], price: float,
                        lookback: int = 60) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """(support, resistance, recent_high) từ swing NGÀY.
      • resistance = đỉnh gần nhất PHÍA TRÊN giá (mức chặn overhead).
      • support    = đáy gần nhất PHÍA DƯỚI giá.
      • recent_high= đỉnh cao nhất trong lookback (để nhận breakout).
    """
    h = highs[-lookback:]
    lo = lows[-lookback:]
    if not h or not lo or not price:
        return None, None, None
    recent_high = max(h)
    overhead = [x for x in h if x > price * 1.002]
    below = [x for x in lo if x < price * 0.998]
    resistance = min(overhead) if overhead else recent_high
    support = max(below) if below else min(lo)
    return round(support, 3), round(resistance, 3), round(recent_high, 3)


def _location(price: float, support: Optional[float], resistance: Optional[float],
              poc: Optional[float], va_low: Optional[float], va_high: Optional[float],
              recent_high: Optional[float]) -> str:
    """Vị trí giá hiện tại — ưu tiên: breakout > resistance > support > at_poc > inside_va > mid."""
    if not price:
        return "mid"
    if recent_high and price > recent_high * 1.001:
        return "breakout"
    if resistance and price >= resistance * 0.995:
        return "resistance"
    if support and price <= support * 1.005:
        return "support"
    if poc and _rel_close(price, poc, 0.003):
        return "at_poc"
    if va_low is not None and va_high is not None and va_low <= price <= va_high:
        return "inside_va"
    return "mid"


def _foreign_dealer_dir(ticker: str, days_back: int = 20) -> tuple[float, float]:
    """(foreign_dir, dealer_dir) [-1..1] từ dòng tiền NGÀY (shark_history, cache 15').
    Thiếu token/dữ liệu → (0, 0)."""
    try:
        from app.services import shark_history
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=days_back * 2)).strftime("%Y-%m-%d")  # dư ngày lịch
        h = shark_history.get_history(ticker, start, end)
        days = h.get("days") or []
        if not days:
            return 0.0, 0.0
        days = days[-days_back:]
        fnet = sum(d.get("foreign_net", 0.0) for d in days)
        fabs = sum(abs(d.get("foreign_net", 0.0)) for d in days) or 1.0
        pnet = sum(d.get("prop_net", 0.0) for d in days)
        pabs = sum(abs(d.get("prop_net", 0.0)) for d in days) or 1.0
        return round(_clamp(fnet / fabs, -1, 1), 3), round(_clamp(pnet / pabs, -1, 1), 3)
    except Exception:  # noqa: BLE001
        return 0.0, 0.0


# ── Build ─────────────────────────────────────────────────────────────────────────────
def build_context(ticker: str, ticks: List[dict], of: Optional[dict] = None,
                  with_foreign: bool = True) -> Context:
    """Dựng Context cho 1 mã từ tape intraday + OHLCV ngày + dòng tiền ngày.

    `of` = kết quả order_flow.analyze (nếu đã có, tránh tính lại VWAP/VP); thiếu thì tự tính.
    `with_foreign=False` để bỏ qua gọi FireAnt (test/nhanh).
    """
    from app.services import ohlcv_store, order_flow
    tk = ticker.upper()

    price = float(ticks[-1]["price"]) if ticks else None
    last_ts = ticks[-1]["ts"] if ticks else datetime.now().isoformat()

    # VWAP + POC/VA — dùng lại `of` nếu có
    if of and not of.get("empty"):
        vwap = of.get("vwap")
        vp = of.get("volume_profile") or {}
    else:
        vwap = order_flow.vwap_final(ticks) if ticks else None
        vp = order_flow.volume_profile(ticks) if ticks else {}
    poc, va_low, va_high = vp.get("poc"), vp.get("va_low"), vp.get("va_high")

    # Trend / MA / S-R từ nến NGÀY
    m20 = m50 = m100 = support = resistance = recent_high = None
    trend, ma_state = "unknown", ""
    try:
        start = (datetime.now() - timedelta(days=260)).strftime("%Y-%m-%d")  # ~180 phiên cho MA100
        df = ohlcv_store.get_ohlcv(tk, start=start)
        if df is not None and not df.empty:
            closes = [float(x) for x in df["close"].tolist() if x]
            highs = [float(x) for x in df["high"].tolist() if x]
            lows = [float(x) for x in df["low"].tolist() if x]
            m20, m50, m100 = _ma(closes, 20), _ma(closes, 50), _ma(closes, 100)
            trend, ma_state = _classify_trend(m20, m50, m100)
            ref_price = price or (closes[-1] if closes else None)
            if ref_price:
                support, resistance, recent_high = _support_resistance(highs, lows, ref_price)
    except Exception:  # noqa: BLE001
        pass

    vwap_side = "at"
    if price and vwap:
        vwap_side = "above" if price > vwap * 1.001 else ("below" if price < vwap * 0.999 else "at")

    location = _location(price, support, resistance, poc, va_low, va_high, recent_high)
    session_phase = _session_phase(last_ts)
    foreign_dir, dealer_dir = _foreign_dealer_dir(tk) if with_foreign else (0.0, 0.0)
    regime = {"uptrend": "trending_up", "downtrend": "trending_down",
              "sideway": "sideway"}.get(trend, "unknown")

    return Context(
        trend=trend, regime=regime, ma_state=ma_state, location=location, vwap_side=vwap_side,
        session_phase=session_phase, foreign_dir=foreign_dir, dealer_dir=dealer_dir,
        price=price, ma20=m20, ma50=m50, ma100=m100,
        support=support, resistance=resistance,
        poc=poc, va_low=va_low, va_high=va_high, vwap=vwap,
    )
