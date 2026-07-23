"""patterns — LAYER 2 detector (xem docs/smart-money-design.md).

Mỗi detector là hàm THUẦN `(ticks, context, ...) -> List[Event]`. Khác Layer 1 (chỉ tính
chỉ số), Layer 2 DIỄN GIẢI: gắn strength[-1..1] (dương=bullish), confidence[0..1], ngữ
cảnh và bằng chứng cho từng sự kiện dòng tiền → đơn vị để chấm điểm (Layer 3) & backtest.

Hai cải tiến cốt lõi so với engine cũ:
  • Cửa sổ THEO THỜI GIAN (phút), không theo số tick — so sánh được giữa mã thanh khoản
    cao (STB vài nghìn tick) và thấp (vài chục tick). Số cửa sổ co theo độ dài phiên,
    không theo mật độ khớp.
  • Absorption CHẤM ĐIỂM LIÊN TỤC & THÍCH ỨNG THEO MÃ (impact-residual), thay hard-rule:
    ước lượng ĐỘ NHẠY GIÁ–IMBALANCE (k) của chính mã trong phiên, rồi đo phần dư. Giá
    giảm NHẸ hơn mức mô hình dự báo (do lực bán) vẫn tính là hấp thụ — đúng thực tế.
"""
from __future__ import annotations
import math
from dataclasses import replace
from datetime import datetime
from typing import Dict, List, Optional

from app.services.market_context import Context, Event, _location

ALGO_VERSION = 1


def _ctx_at(context: Context, price: float) -> Context:
    """Bản sao context với `location` tính lại theo GIÁ TẠI THỜI ĐIỂM event (một lệnh lúc
    sáng ở hỗ trợ khác lúc chiều ở kháng cự) — trend/MA/S-R giữ nguyên trong phiên."""
    if not price:
        return context
    loc = _location(price, context.support, context.resistance, context.poc,
                    context.va_low, context.va_high,
                    context.resistance)  # recent_high xấp xỉ = kháng cự phiên
    return replace(context, location=loc, price=price)


# ── Helpers ───────────────────────────────────────────────────────────────────────────
def _pt(ts: str) -> Optional[float]:
    try:
        return datetime.fromisoformat(ts).replace(tzinfo=None).timestamp()
    except (ValueError, TypeError):
        return None


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _median(xs: List[float]) -> float:
    if not xs:
        return 0.0
    s = sorted(xs); n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def _std(xs: List[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    m = sum(xs) / n
    return (sum((x - m) ** 2 for x in xs) / (n - 1)) ** 0.5


def _mad(xs: List[float]) -> float:
    """Median absolute deviation × 1.4826 (ước lượng σ bền với ngoại lai)."""
    if not xs:
        return 0.0
    med = _median(xs)
    return 1.4826 * _median([abs(x - med) for x in xs])


def time_windows(ticks: List[dict], minutes: float, big_thr: float = 0.0) -> List[dict]:
    """Chia tape thành cửa sổ THỜI GIAN không chồng lấn, mỗi cửa sổ gộp:
      b/s (KL mua/bán chủ động), imb=(b-s)/(b+s), chg_pct=(giá cuối−đầu)/đầu,
      val (giá trị), n (số khớp), big_buy/big_sell (số lệnh lớn ≥ big_thr mỗi chiều)."""
    if not ticks:
        return []
    t0 = _pt(ticks[0]["ts"])
    if t0 is None:
        return []
    width = max(30.0, minutes * 60.0)
    out: List[dict] = []
    cur: Optional[dict] = None
    for t in ticks:
        tp = _pt(t["ts"])
        if tp is None:
            continue
        idx = int((tp - t0) // width)
        if cur is None or idx != cur["idx"]:
            if cur is not None:
                out.append(_finalize_win(cur))
            cur = {"idx": idx, "ts": t["ts"], "p0": t["price"], "p1": t["price"],
                   "b": 0, "s": 0, "val": 0.0, "n": 0, "big_buy": 0, "big_sell": 0}
        cur["p1"] = t["price"]
        cur["n"] += 1
        cur["val"] += t["value"]
        if t["side"] == "B":
            cur["b"] += t["volume"]
        elif t["side"] == "S":
            cur["s"] += t["volume"]
        if big_thr and t["value"] >= big_thr:
            if t["side"] == "B":
                cur["big_buy"] += 1
            elif t["side"] == "S":
                cur["big_sell"] += 1
    if cur is not None:
        out.append(_finalize_win(cur))
    return out


def _finalize_win(w: dict) -> dict:
    bs = w["b"] + w["s"]
    w["bs"] = bs
    w["imb"] = (w["b"] - w["s"]) / bs if bs else 0.0
    w["chg_pct"] = (w["p1"] - w["p0"]) / w["p0"] * 100.0 if w["p0"] else 0.0
    return w


def _vol_pctile(windows: List[dict], v: float) -> float:
    """Vị trí percentile của KL cửa sổ (để chấm confidence: cửa sổ dày → tin hơn)."""
    vals = sorted(x["bs"] for x in windows)
    if not vals:
        return 0.0
    below = sum(1 for x in vals if x <= v)
    return below / len(vals)


# ── [1] ABSORPTION (impact-residual, graded, thích ứng theo mã) ───────────────────────
def detect_absorption(ticks: List[dict], context: Context, big_thr: float = 0.0,
                      minutes: float = 5.0, min_imb: float = 0.30,
                      z_thr: float = 1.0) -> List[Event]:
    """Hấp thụ = lực chủ động MỘT CHIỀU mà giá KHÔNG đi theo mức mô hình dự báo.

    Cách làm (thích ứng theo mã):
      1) Ước lượng độ nhạy k = median(chg_pct/imb) trên các cửa sổ có |imb|>0.15
         → 'imbalance 1 đơn vị thường đẩy giá k%'. Kẹp k≥prior dương (giá thường đi
         THEO lực chủ động; nếu k≤0 dùng prior nhỏ).
      2) residual = chg_pct − k·imb (giá thực − giá mô hình). σ = MAD(residuals).
      3) z = residual/σ. Bán áp đảo (imb≤−min_imb) mà z≥z_thr (giá giữ hơn dự báo) →
         BUY ABSORPTION (bullish). Mua áp đảo (imb≥min_imb) mà z≤−z_thr (giá không lên
         như dự báo) → SUPPLY/phân phối (bearish).
    """
    wins = time_windows(ticks, minutes, big_thr)
    usable = [w for w in wins if w["bs"] > 0 and w["n"] >= 4]
    if len(usable) < 6:
        return []
    ratios = [w["chg_pct"] / w["imb"] for w in usable if abs(w["imb"]) > 0.15]
    k = _median(ratios) if ratios else 0.0
    if k <= 0:                                   # prior: giá đi theo lực, biên độ nhỏ
        k = max(0.02, _median([abs(w["chg_pct"]) for w in usable]) / 0.5)
    residuals = [w["chg_pct"] - k * w["imb"] for w in usable]
    sigma = _mad(residuals) or _std(residuals)
    if sigma <= 0:
        return []

    events: List[Event] = []
    for w, res in zip(usable, residuals):
        z = res / sigma
        imb = w["imb"]
        cx = _ctx_at(context, w["p1"])            # ngữ cảnh TẠI event (vị trí theo giá lúc đó)
        if imb <= -min_imb and z >= z_thr:       # hấp thụ lực BÁN → gom
            dir_factor = min(1.0, -imb / 0.6)
            strength = _clamp(math.tanh(z / 2.0) * dir_factor, 0.0, 1.0)
            big_flag = 1.0 if w["big_buy"] >= 1 else 0.0
            conf = _clamp(0.30 + 0.40 * _vol_pctile(usable, w["bs"]) +
                          0.20 * big_flag + 0.10 * min(1.0, -imb), 0.0, 1.0)
            ev = [f"Bán chủ động {round(-imb*100)}% mà giá {w['chg_pct']:+.2f}% "
                  f"(mô hình dự báo {k*imb:+.2f}%)"]
            if w["big_buy"]:
                ev.append(f"{w['big_buy']} lệnh MUA lớn đỡ giá")
            if cx.location in ("support", "inside_va", "at_poc"):
                ev.append(f"tại {cx.location}")
            events.append(Event("absorption", w["ts"], round(strength, 3),
                                 round(conf, 3), cx, ev, ALGO_VERSION))
        elif imb >= min_imb and z <= -z_thr:     # mua áp đảo mà giá không lên → cung/xả
            dir_factor = min(1.0, imb / 0.6)
            strength = -_clamp(math.tanh(-z / 2.0) * dir_factor, 0.0, 1.0)
            big_flag = 1.0 if w["big_sell"] >= 1 else 0.0
            conf = _clamp(0.30 + 0.40 * _vol_pctile(usable, w["bs"]) +
                          0.20 * big_flag + 0.10 * min(1.0, imb), 0.0, 1.0)
            ev = [f"Mua chủ động {round(imb*100)}% mà giá {w['chg_pct']:+.2f}% "
                  f"(mô hình dự báo {k*imb:+.2f}%)"]
            if w["big_sell"]:
                ev.append(f"{w['big_sell']} lệnh BÁN lớn chặn giá")
            if cx.location in ("resistance", "breakout"):
                ev.append(f"tại {cx.location}")
            events.append(Event("supply_absorption", w["ts"], round(strength, 3),
                                 round(conf, 3), cx, ev, ALGO_VERSION))
    # giữ tối đa 40 event mạnh nhất (theo |strength|·confidence), sắp lại theo thời gian
    events.sort(key=lambda e: abs(e.strength) * e.confidence, reverse=True)
    events = events[:40]
    events.sort(key=lambda e: e.ts)
    return events


# ── [2] CVD / DELTA DIVERGENCE (graded, migrate order_flow._divergence) ───────────────
def detect_divergence(ticks: List[dict], context: Context, minutes: float = 5.0) -> List[Event]:
    """Phân kỳ giá vs CVD ở phần CUỐI phiên: giá lên mà CVD đi xuống (cầu suy → bearish),
    hoặc giá xuống mà CVD lên (cung cạn → bullish). Strength = mức đối nghịch (Spearman-ish)."""
    wins = time_windows(ticks, minutes)
    if len(wins) < 6:
        return []
    tail = wins[max(0, int(len(wins) * 0.6)):]   # ~40% cuối
    if len(tail) < 4:
        return []
    # chuỗi giá cuối cửa sổ & CVD luỹ kế cuối cửa sổ
    prices = [w["p1"] for w in tail]
    cvd = 0.0
    cvds = []
    for w in tail:
        cvd += w["b"] - w["s"]
        cvds.append(cvd)
    # tương quan hạng (bền hơn so max/min của bản cũ)
    corr = _rank_corr(prices, cvds)
    price_net = (prices[-1] - prices[0]) / prices[0] if prices[0] else 0.0
    ts = tail[0]["ts"]
    cx = _ctx_at(context, prices[-1])
    if corr <= -0.3 and price_net > 0:
        strength = -_clamp(abs(corr), 0.0, 1.0)   # bearish
        ev = [f"Giá +{price_net*100:.2f}% nhưng CVD ngược hướng (tương quan {corr:+.2f}) → cầu suy"]
        return [Event("cvd_divergence", ts, round(strength, 3), round(_clamp(abs(corr), 0, 1), 3),
                      cx, ev, ALGO_VERSION)]
    if corr <= -0.3 and price_net < 0:
        strength = _clamp(abs(corr), 0.0, 1.0)    # bullish
        ev = [f"Giá {price_net*100:.2f}% nhưng CVD ngược hướng (tương quan {corr:+.2f}) → cung cạn"]
        return [Event("cvd_divergence", ts, round(strength, 3), round(_clamp(abs(corr), 0, 1), 3),
                      cx, ev, ALGO_VERSION)]
    return []


def _rank_corr(a: List[float], b: List[float]) -> float:
    """Spearman rho đơn giản (tương quan hạng), trả 0 nếu suy biến."""
    n = len(a)
    if n < 3 or n != len(b):
        return 0.0
    def ranks(xs):
        order = sorted(range(len(xs)), key=lambda i: xs[i])
        r = [0.0] * len(xs)
        for pos, i in enumerate(order):
            r[i] = pos
        return r
    ra, rb = ranks(a), ranks(b)
    ma, mb = sum(ra) / n, sum(rb) / n
    num = sum((ra[i] - ma) * (rb[i] - mb) for i in range(n))
    da = (sum((x - ma) ** 2 for x in ra)) ** 0.5
    db = (sum((x - mb) ** 2 for x in rb)) ** 0.5
    return num / (da * db) if da and db else 0.0


# ── [3] INSTITUTION CLUSTER (cụm lệnh lớn dồn theo thời gian) ──────────────────────────
def detect_institution_cluster(ticks: List[dict], context: Context, big_thr: float,
                               minutes: float = 5.0, min_big: int = 3) -> List[Event]:
    """Cụm lệnh lớn BẤT THƯỜNG dồn theo thời gian. Với mã thanh khoản cao, cửa sổ nào cũng
    có vài lệnh lớn → nếu chỉ đòi ≥min_big thì tín hiệu 'luôn bật', vô nghĩa. Nên chỉ báo
    cửa sổ có số lệnh lớn ≥ max(min_big, P75 của các cửa sổ có lệnh lớn) — tức DỒN hơn
    bình thường. Confidence theo CƯỜNG ĐỘ tương đối (percentile), không bão hoà theo count."""
    if not big_thr:
        return []
    wins = time_windows(ticks, minutes, big_thr)
    counts = sorted(w["big_buy"] + w["big_sell"] for w in wins if (w["big_buy"] + w["big_sell"]) > 0)
    if len(counts) < 2:
        return []
    p75 = counts[min(int(len(counts) * 0.75), len(counts) - 1)]
    thr_cnt = max(min_big, p75)
    cmax = counts[-1]
    events: List[Event] = []
    for w in wins:
        cnt = w["big_buy"] + w["big_sell"]
        if cnt < thr_cnt:
            continue
        net = w["big_buy"] - w["big_sell"]
        intensity = (cnt - thr_cnt) / (cmax - thr_cnt) if cmax > thr_cnt else 1.0   # 0..1
        strength = _clamp(net / cnt, -1.0, 1.0) * (0.5 + 0.5 * intensity)
        conf = _clamp(0.45 + 0.45 * intensity, 0.0, 1.0)
        cx = _ctx_at(context, w["p1"])
        ev = [f"{w['big_buy']} mua lớn / {w['big_sell']} bán lớn dồn trong {minutes:.0f}' "
              f"(bình thường ≤{thr_cnt})"]
        if cx.location != "mid":
            ev.append(f"tại {cx.location}")
        events.append(Event("institution_cluster", w["ts"], round(strength, 3),
                             round(conf, 3), cx, ev, ALGO_VERSION))
    return events


# ── Orchestrator ─────────────────────────────────────────────────────────────────────
def detect_all(ticks: List[dict], context: Context, big_thr: float = 0.0,
               minutes: float = 5.0) -> List[Event]:
    """Chạy toàn bộ detector Layer 2 → gộp event (sắp theo thời gian)."""
    if not ticks:
        return []
    events: List[Event] = []
    events += detect_absorption(ticks, context, big_thr, minutes)
    events += detect_divergence(ticks, context, minutes)
    events += detect_institution_cluster(ticks, context, big_thr, minutes)
    events.sort(key=lambda e: e.ts)
    return events
