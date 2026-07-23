"""order_flow — Order Flow Analyzer chạy trên TAPE đã cache (không gọi API).

Mọi hàm là THUẦN (pure), một lượt O(n) trên list tick {ts, side('B'/'S'/'U'), price(kVND),
volume(cổ), value(VND)} đã KHỬ TRÙNG + SẮP THỜI GIAN (shark_monitor._clean_tape).

6 thành phần:
  1) Cumulative Delta (CVD)      — luỹ kế mua chủ động − bán chủ động; phân kỳ với giá.
  2) Volume Profile              — KL theo mức giá; POC + Value Area 70%.
  3) VWAP + vị trí lệnh lớn      — giá bình quân gia quyền; lệnh lớn trên/dưới VWAP.
  4) Large Order Detector        — ngưỡng THÍCH ỨNG theo mã (percentile), phân tầng.
  5) Absorption events           — cửa sổ bán mạnh mà giá giữ (gom) / mua mạnh mà giá tụt (xả).
  6) Iceberg heuristic           — nhiều khớp CÙNG CỠ tại CÙNG GIÁ liên tiếp (THỬ NGHIỆM,
                                   vì thiếu lịch sử độ sâu sổ lệnh để xác nhận 'nạp lại').

LƯU Ý TRUNG THỰC:
  • 1–3 là DỮ KIỆN khách quan (độ tin cao). 4–5 phụ thuộc chất lượng phân loại B/S.
  • 6 (Iceberg) chỉ là heuristic tape-based → dễ dương tính giả (lô chẵn) → gắn nhãn
    experimental; giá trị dự báo của MỌI tín hiệu vẫn phải đo bằng backtest.
"""
from __future__ import annotations
from datetime import datetime
from typing import Dict, List, Optional


def _pt(ts: str) -> Optional[float]:
    """ts 'YYYY-MM-DD HH:MM:SS[.mmm]' → epoch giây (bỏ tz cho nhất quán). None nếu lỗi."""
    try:
        return datetime.fromisoformat(ts).replace(tzinfo=None).timestamp()
    except (ValueError, TypeError):
        return None


# ── [4] Ngưỡng lệnh lớn THÍCH ỨNG (percentile của chính mã) ──────────────────────────
def big_thresholds(ticks: List[dict]) -> Dict[str, float]:
    """Ngưỡng theo giá trị (VND) tại P90/P97/P99 của chính mã → 'lớn/rất lớn/khủng'."""
    if len(ticks) < 50:
        return {"p90": 1e9, "p97": 1e9, "p99": 2e9}
    vals = sorted(t["value"] for t in ticks)
    n = len(vals)
    def q(p): return vals[min(int(n * p), n - 1)]
    return {"p90": float(q(0.90)), "p97": float(q(0.97)), "p99": float(q(0.99))}


# ── [1] Cumulative Delta ─────────────────────────────────────────────────────────────
def cumulative_delta(ticks: List[dict]) -> Dict:
    cvd = 0
    peak = trough = 0
    for t in ticks:
        if t["side"] == "B":
            cvd += t["volume"]
        elif t["side"] == "S":
            cvd -= t["volume"]
        peak = max(peak, cvd)
        trough = min(trough, cvd)
    return {"last": cvd, "peak": peak, "trough": trough}


def _divergence(ticks: List[dict], tail_frac: float = 0.30) -> Dict:
    """Phân kỳ giá vs CVD ở CUỐI phiên: giá tạo đỉnh cao hơn mà CVD tạo đỉnh thấp hơn
    (hoặc ngược lại) → cảnh báo. So nửa đầu vs nửa cuối của đoạn đuôi."""
    n = len(ticks)
    if n < 40:
        return {"type": "none", "note": "chưa đủ dữ liệu"}
    start = int(n * (1 - tail_frac))
    seg = ticks[start:]
    if len(seg) < 20:
        return {"type": "none"}
    mid = len(seg) // 2
    cvd = 0
    cvds = []
    for t in seg:
        cvd += t["volume"] if t["side"] == "B" else (-t["volume"] if t["side"] == "S" else 0)
        cvds.append(cvd)
    p1 = max(t["price"] for t in seg[:mid]); p2 = max(t["price"] for t in seg[mid:])
    c1 = max(cvds[:mid]); c2 = max(cvds[mid:])
    l1 = min(t["price"] for t in seg[:mid]); l2 = min(t["price"] for t in seg[mid:])
    d1 = min(cvds[:mid]); d2 = min(cvds[mid:])
    if p2 > p1 and c2 < c1:
        return {"type": "bearish", "note": "Giá tạo đỉnh cao hơn nhưng CVD yếu đi → cầu suy"}
    if l2 < l1 and d2 > d1:
        return {"type": "bullish", "note": "Giá tạo đáy thấp hơn nhưng CVD mạnh lên → cung cạn"}
    return {"type": "none"}


# ── [3] VWAP (luỹ kế) ────────────────────────────────────────────────────────────────
def vwap_final(ticks: List[dict]) -> float:
    pv = sum(t["price"] * t["volume"] for t in ticks)
    v = sum(t["volume"] for t in ticks)
    return round(pv / v, 3) if v else 0.0


# ── Chuỗi CVD/VWAP/giá đã DOWNSAMPLE theo thời gian (payload nhẹ cho biểu đồ) ──────────
def series(ticks: List[dict], n_points: int = 180) -> List[Dict]:
    if not ticks:
        return []
    t0, t1 = _pt(ticks[0]["ts"]), _pt(ticks[-1]["ts"])
    if not t0 or not t1 or t1 <= t0:
        t0, t1 = 0.0, float(len(ticks))
        keyf = lambda i, _t: float(i)   # noqa: E731 — fallback theo index
    else:
        keyf = lambda i, tk: _pt(tk["ts"]) or t0   # noqa: E731
    step = (t1 - t0) / n_points or 1.0
    out: List[Dict] = []
    cvd = 0
    pv = 0.0
    vol = 0
    next_edge = t0 + step
    for i, tk in enumerate(ticks):
        cvd += tk["volume"] if tk["side"] == "B" else (-tk["volume"] if tk["side"] == "S" else 0)
        pv += tk["price"] * tk["volume"]
        vol += tk["volume"]
        k = keyf(i, tk)
        while k >= next_edge and len(out) < n_points:
            out.append({"t": tk["ts"][11:19], "cvd": cvd,
                        "vwap": round(pv / vol, 3) if vol else tk["price"],
                        "price": tk["price"]})
            next_edge += step
    # điểm cuối
    out.append({"t": ticks[-1]["ts"][11:19], "cvd": cvd,
                "vwap": round(pv / vol, 3) if vol else ticks[-1]["price"],
                "price": ticks[-1]["price"]})
    return out


# ── [2] Volume Profile theo mức giá ──────────────────────────────────────────────────
def volume_profile(ticks: List[dict], max_bins: int = 40) -> Dict:
    if not ticks:
        return {"levels": [], "poc": None, "va_low": None, "va_high": None}
    prices = [t["price"] for t in ticks]
    lo, hi = min(prices), max(prices)
    if hi <= lo:
        v = sum(t["volume"] for t in ticks)
        return {"levels": [{"price": lo, "buy": 0, "sell": 0, "total": v}],
                "poc": lo, "va_low": lo, "va_high": lo}
    # bước bin = số đẹp theo dải giá; tối đa max_bins mức
    span = hi - lo
    bin_w = span / max_bins
    def binp(p): return round(lo + (int((p - lo) / bin_w)) * bin_w, 3)
    agg: Dict[float, List[int]] = {}
    for t in ticks:
        b = binp(t["price"])
        cell = agg.setdefault(b, [0, 0])   # [buy, sell]
        if t["side"] == "B":
            cell[0] += t["volume"]
        elif t["side"] == "S":
            cell[1] += t["volume"]
        else:
            cell[0] += t["volume"] // 2
            cell[1] += t["volume"] - t["volume"] // 2
    levels = [{"price": p, "buy": bs[0], "sell": bs[1], "total": bs[0] + bs[1]}
              for p, bs in sorted(agg.items())]
    poc = max(levels, key=lambda x: x["total"])["price"]
    # Value Area 70%: mở rộng quanh POC tới khi đạt 70% tổng KL
    total_vol = sum(x["total"] for x in levels)
    order = sorted(levels, key=lambda x: x["total"], reverse=True)
    acc = 0
    va_prices = []
    for lv in order:
        acc += lv["total"]
        va_prices.append(lv["price"])
        if acc >= 0.70 * total_vol:
            break
    return {"levels": levels, "poc": poc,
            "va_low": min(va_prices) if va_prices else poc,
            "va_high": max(va_prices) if va_prices else poc}


# ── [4] Large Order Detector (theo tầng percentile) + vị trí so VWAP ──────────────────
def large_orders(ticks: List[dict], thr: Dict[str, float], limit: int = 500) -> Dict:
    p97 = thr["p97"]
    big = [t for t in ticks if t["value"] >= p97]
    # VWAP luỹ kế để biết mỗi lệnh lớn khớp TRÊN hay DƯỚI giá bình quân
    pv = vol = 0
    vwap_at: Dict[int, float] = {}
    for i, t in enumerate(ticks):
        pv += t["price"] * t["volume"]; vol += t["volume"]
        vwap_at[id(t)] = pv / vol if vol else t["price"]
    def tier(v):
        return "huge" if v >= thr["p99"] else "large"
    orders = []
    for t in big[-limit:]:
        w = vwap_at.get(id(t), t["price"])
        orders.append({"ts": t["ts"], "side": t["side"], "volume": t["volume"],
                       "price": t["price"], "value": t["value"], "tier": tier(t["value"]),
                       "vs_vwap": "above" if t["price"] > w else ("below" if t["price"] < w else "at")})
    orders.reverse()
    buy_below = sum(1 for o in orders if o["side"] == "B" and o["vs_vwap"] == "below")
    sell_above = sum(1 for o in orders if o["side"] == "S" and o["vs_vwap"] == "above")
    return {
        "threshold_p97": round(p97), "threshold_p99": round(thr["p99"]),
        "count": len(big),
        "buy_val": sum(t["value"] for t in big if t["side"] == "B"),
        "sell_val": sum(t["value"] for t in big if t["side"] == "S"),
        "buy_below_vwap": buy_below,     # gom giá rẻ (tích luỹ)
        "sell_above_vwap": sell_above,   # xả giá cao (phân phối)
        "orders": orders,
    }


# ── [5] Absorption events (theo cửa sổ) ──────────────────────────────────────────────
def absorption_events(ticks: List[dict], window: int = 40, min_imb: float = 0.6) -> List[Dict]:
    """Cửa sổ trượt: BÁN chủ động áp đảo mà giá KHÔNG giảm → hấp thụ (gom); và gương lại."""
    n = len(ticks)
    if n < window:
        return []
    events: List[Dict] = []
    i = 0
    while i + window <= n:
        w = ticks[i:i + window]
        bv = sum(t["volume"] for t in w if t["side"] == "B")
        sv = sum(t["volume"] for t in w if t["side"] == "S")
        tot = bv + sv
        if tot:
            imb = (bv - sv) / tot
            p0, p1 = w[0]["price"], w[-1]["price"]
            chg = (p1 - p0) / p0 if p0 else 0
            if imb <= -min_imb and chg >= 0:      # bán áp đảo, giá giữ/tăng → GOM
                events.append({"ts": w[0]["ts"], "type": "buy_absorption",
                               "sell_share": round(-imb, 2), "price_chg_pct": round(chg * 100, 2),
                               "vol": tot})
                i += window; continue
            if imb >= min_imb and chg <= 0:       # mua áp đảo, giá giữ/giảm → XẢ
                events.append({"ts": w[0]["ts"], "type": "sell_absorption",
                               "buy_share": round(imb, 2), "price_chg_pct": round(chg * 100, 2),
                               "vol": tot})
                i += window; continue
        i += max(1, window // 2)
    return events[-50:]


# ── [6] Iceberg heuristic (THỬ NGHIỆM) ───────────────────────────────────────────────
def iceberg_candidates(ticks: List[dict], min_repeat: int = 5,
                       max_gap_s: float = 30.0) -> List[Dict]:
    """Nhiều khớp CÙNG (giá, cỡ, chiều) liên tiếp cách nhau ngắn → nghi 'nạp lại' iceberg.
    THIẾU lịch sử độ sâu sổ lệnh nên KHÔNG xác nhận được → dễ nhầm lô chẵn phổ biến.
    Chỉ giữ ứng viên có cỡ KHÔNG tầm thường (≥ trung vị KL) để bớt nhiễu."""
    n = len(ticks)
    if n < min_repeat:
        return []
    vols = sorted(t["volume"] for t in ticks)
    # Sàn cỡ = P75 KL (mạnh hơn trung vị) để bớt nhiễu lô chẵn nhỏ lặp nhiều lần.
    floor = vols[min(int(n * 0.75), n - 1)] or 1
    runs: Dict[tuple, dict] = {}
    out: List[Dict] = []
    for t in ticks:
        if t["volume"] < floor:          # chỉ xét lệnh cỡ LỚN (≥P75) lặp lại
            continue
        key = (round(t["price"], 3), t["volume"], t["side"])
        ts = _pt(t["ts"]) or 0.0
        r = runs.get(key)
        if r and ts - r["last"] <= max_gap_s:
            r["count"] += 1; r["last"] = ts; r["end"] = t["ts"]
        else:
            if r and r["count"] >= min_repeat:
                out.append({"price": key[0], "size": key[1], "side": key[2],
                            "repeat": r["count"], "from": r["start"], "to": r["end"],
                            "est_vol": key[1] * r["count"]})
            runs[key] = {"count": 1, "last": ts, "start": t["ts"], "end": t["ts"]}
    for key, r in runs.items():
        if r["count"] >= min_repeat:
            out.append({"price": key[0], "size": key[1], "side": key[2],
                        "repeat": r["count"], "from": r["start"], "to": r["end"],
                        "est_vol": key[1] * r["count"]})
    out.sort(key=lambda x: x["est_vol"], reverse=True)
    return out[:20]


# ── Gộp tất cả ───────────────────────────────────────────────────────────────────────
def analyze(ticks: List[dict]) -> Dict:
    """Tính TOÀN BỘ order flow trên tape đã cache. Một lượt gọi các hàm O(n)."""
    if not ticks:
        return {"empty": True}
    thr = big_thresholds(ticks)
    cvd = cumulative_delta(ticks)
    return {
        "empty": False,
        "n_ticks": len(ticks),
        "vwap": vwap_final(ticks),
        "cvd": cvd,
        "cvd_divergence": _divergence(ticks),
        "series": series(ticks),
        "volume_profile": volume_profile(ticks),
        "large_orders": large_orders(ticks, thr),
        "absorption": absorption_events(ticks),
        "iceberg": iceberg_candidates(ticks),   # experimental
        "updated_at": datetime.now().isoformat(),
    }
