"""decision — LAYER 3 Decision Engine (xem docs/smart-money-design.md).

Gộp EVENTS (Layer 2) + CONTEXT (Layer 0) + METRICS (Layer 1) thành các ĐIỂM diễn giải
và một Smart Money Report tự-giải-thích. MINH BẠCH: rule/score cộng trọng số, mỗi điểm
kèm `components` để truy vết vì sao — KHÔNG black-box (người dùng cần hiểu kết luận).

Điểm mấu chốt (đúng cho STB): khối ngoại BÁN mà giá được HẤP THỤ (giữ) thì KHÔNG phải
distribution → đóng góp của foreign vào distribution bị GATE bởi mức absorption bullish.

Đầu ra chính:
  accumulation_score, distribution_score, breakout_score, trend_quality,
  institution_activity, bull_strength, bear_strength, market_control,
  smart_money_confidence, wyckoff_phase, conclusion(text), evidence_chain, report(sao+cờ).
"""
from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

from app.services.market_context import Context


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _sat(x: float, scale: float) -> float:
    """Bão hoà về [-1,1] (tránh 1 loại event nhiều lấn át)."""
    return math.tanh(x / scale) if scale else 0.0


def _cd(context: Union[Context, dict]) -> dict:
    return context.to_dict() if isinstance(context, Context) else dict(context or {})


def _trend_num(trend: str) -> float:
    return {"uptrend": 1.0, "sideway": 0.0, "downtrend": -1.0}.get(trend, 0.0)


def _agg_events(events: List[dict]) -> dict:
    """Gộp event theo loại → tổng có dấu của strength·confidence + số lượng + conf tốt nhất."""
    g: Dict[str, dict] = {}
    for e in events:
        t = e.get("type", "")
        s = float(e.get("strength") or 0.0) * float(e.get("confidence") or 0.0)
        d = g.setdefault(t, {"sum": 0.0, "n": 0, "best": 0.0})
        d["sum"] += s
        d["n"] += 1
        d["best"] = max(d["best"], float(e.get("confidence") or 0.0))
    return g


# ── Explainability (F1): sổ cái đóng góp + độ tin theo detector ───────────────────────
# Reliability = độ tin CẤU TRÚC của mỗi loại tín hiệu (không phải confidence từng event).
# Cụm lệnh tổ chức / hấp thụ đáng tin hơn một nhịp Delta nhỏ hay POC dịch nhẹ.
RELIABILITY = {
    "cluster": 0.85, "absorption": 0.82, "supply": 0.82, "foreign": 0.75,
    "large": 0.72, "trend": 0.70, "flow": 0.70, "base": 0.68, "location": 0.66,
    "divergence": 0.65, "delta": 0.63, "dealer": 0.55, "poc": 0.55, "vol": 0.50,
}


@dataclass
class Contribution:
    source: str          # khoá reliability
    label: str           # ngôn ngữ người ("6 lần hấp thụ mua tại hỗ trợ")
    points: float        # +/− điểm đóng góp
    polarity: str        # pro | con
    reliability: float

    def to_dict(self) -> dict:
        return {"source": self.source, "label": self.label, "points": self.points,
                "polarity": self.polarity, "reliability": self.reliability}


def _mk(source: str, label: str, points: float, rel_mult: float = 1.0) -> Contribution:
    """rel_mult = hệ số NGỮ CẢNH nhân vào reliability (#5): hấp thụ tại hỗ trợ đáng tin hơn
    tại kháng cự → cùng một loại tín hiệu nhưng độ tin khác nhau tuỳ vị trí."""
    rel = _clamp(RELIABILITY.get(source, 0.6) * rel_mult, 0.3, 0.95)
    return Contribution(source, label, round(points, 1),
                        "pro" if points >= 0 else "con", round(rel, 3))


def _score_from(contribs: List[Contribution], base: float = 0.0):
    """(score 0-100, confidence 0-1, ledger đã lọc & sắp). Bỏ đóng góp < 0.5 điểm.
    confidence = bình quân reliability có trọng số theo |điểm| → điểm do bằng chứng
    độ-tin-cao chi phối thì confidence cao."""
    cs = [c for c in contribs if abs(c.points) >= 0.5]
    score = int(_clamp(round(base + sum(c.points for c in cs)), 0, 100))
    tot = sum(abs(c.points) for c in cs)
    conf = (sum(abs(c.points) * c.reliability for c in cs) / tot) if tot else 0.5
    cs.sort(key=lambda c: abs(c.points), reverse=True)
    return score, round(conf, 3), cs


def _event_counts(events: List[dict]) -> dict:
    c = {"absorption_buy": 0, "absorption_sell": 0, "cluster_buy": 0,
         "cluster_sell": 0, "divergence_bull": 0, "divergence_bear": 0}
    for e in events:
        t, s = e.get("type", ""), e.get("strength") or 0
        if t == "absorption" and s > 0:
            c["absorption_buy"] += 1
        elif t == "supply_absorption":
            c["absorption_sell"] += 1
        elif t == "institution_cluster":
            c["cluster_buy" if s >= 0 else "cluster_sell"] += 1
        elif t == "cvd_divergence":
            c["divergence_bull" if s > 0 else "divergence_bear"] += 1
    return c


def _poc_sig(poc_shift: float, up: bool) -> float:
    """POC shift thực tế rất nhỏ (±0.03) → khuếch đại về 0..1."""
    v = poc_shift if up else -poc_shift
    return _clamp(max(0.0, v) * 20.0, 0.0, 1.0)


def _evidence_engine(lean: str, phase: str, acc_led: List[Contribution],
                     dist_led: List[Contribution], acc_conf: float, dist_conf: float,
                     breakout_score: int, cx: dict, conflict: float) -> dict:
    """F2 — Evidence Engine: gom sổ cái của HƯỚNG CHI PHỐI thành ✓ (thuận) / ✗ (nghịch)
    bằng ngôn ngữ người, kèm bằng chứng NGỮ CẢNH bổ sung (VWAP, breakout) + confidence.
    Đây là đầu vào cho Hypothesis/Decision (F3)."""
    bearish = lean == "distribution"
    led = dist_led if bearish else acc_led
    base_conf = dist_conf if bearish else acc_conf

    def _row(c: Contribution) -> dict:
        return {"label": c.label, "points": c.points, "reliability": c.reliability}

    # Bỏ đóng góp "nền" (base) — là offset cấu trúc, không phải bằng chứng dòng tiền.
    supporting = [_row(c) for c in led if c.points > 0 and c.source != "base"]
    contradicting = [_row(c) for c in led if c.points < 0 and c.source != "base"]

    # Bằng chứng ngữ cảnh bổ sung (không nằm trong ledger điểm)
    vs = cx.get("vwap_side")
    if vs == "above":
        (contradicting if bearish else supporting).append(
            {"label": "Giá giữ trên VWAP", "points": None, "reliability": RELIABILITY["flow"]})
    elif vs == "below":
        (supporting if bearish else contradicting).append(
            {"label": "Giá dưới VWAP", "points": None, "reliability": RELIABILITY["flow"]})
    if not bearish and breakout_score < 45:
        contradicting.append({"label": "Breakout chưa xác nhận", "points": None,
                              "reliability": RELIABILITY["location"]})

    supporting.sort(key=lambda r: -(abs(r["points"]) if r["points"] else 0))
    contradicting.sort(key=lambda r: -(abs(r["points"]) if r["points"] else 0))
    lean_vi = {"accumulation": "Nghiêng TÍCH LUỸ", "distribution": "Nghiêng PHÂN PHỐI",
               "neutral": "Chưa rõ hướng"}.get(lean, lean)
    return {
        "conclusion": phase,
        "lean": lean,
        "lean_label": lean_vi,
        "supporting": supporting,
        "contradicting": contradicting,
        "confidence": round(100 * base_conf * (1.0 - 0.25 * conflict)),
    }


def _hypotheses(regime: str, location: str, acc: int, dist: int, brk: int, inst: int,
                absorp: float, supply: float, diverg: float, cluster: float, flow: float,
                conflict: float, memory: Optional[dict] = None) -> List[dict]:
    """F3 Hypothesis Engine — sinh nhiều giả thuyết SONG SONG rồi softmax → xác suất.
    Tái dùng điểm/tín hiệu đã có (không detector mới). Regime chi phối trọng số:
    absorption trong sideway ≠ trong uptrend. `memory` (nếu có) nhích giả thuyết theo
    diễn biến nhiều phiên — hiện là seam (None → bỏ qua)."""
    at_support = location in ("support", "inside_va", "at_poc")
    at_resist = location in ("resistance", "breakout")
    raw = {
        "Tích luỹ": acc * (1.15 if regime in ("sideway", "trending_down") else 0.9)
                    + (15 if at_support else 0) - 0.4 * dist,
        "Phân phối": dist * (1.15 if regime in ("trending_up", "sideway") else 0.9)
                     + (15 if at_resist else 0) + 20 * max(0.0, -diverg) - 0.4 * acc,
        "Markup": (58 if regime == "trending_up" else 12) + 0.4 * brk + 0.25 * acc + 18 * max(0.0, flow),
        "Markdown": (58 if regime == "trending_down" else 8) + 0.4 * dist - 22 * flow,
        "Rũ hàng (Shakeout)": (24 if at_support else 4)
                              + 45 * absorp * (1.0 if regime in ("sideway", "trending_down") else 0.5),
        "Cao trào mua": (18 if (regime == "trending_up" and at_resist) else 0)
                        + 30 * supply + 25 * max(0.0, -diverg) + 0.25 * inst,
        "Chưa rõ": 22 + 45 * conflict,
    }
    if memory:                                   # seam: nhích theo quá trình nhiều phiên
        raw["Tích luỹ"] += memory.get("accum_bias", 0.0)
        raw["Phân phối"] += memory.get("distrib_bias", 0.0)
    raw = {k: max(0.0, v) for k, v in raw.items()}
    T = 18.0
    exps = {k: math.exp(v / T) for k, v in raw.items()}
    tot = sum(exps.values()) or 1.0
    hyps = [{"name": k, "probability": round(100 * e / tot)} for k, e in exps.items()]
    hyps.sort(key=lambda h: -h["probability"])
    return [h for h in hyps if h["probability"] >= 3][:5]


# Ánh xạ giả thuyết → trạng thái ngắn gọn hiển thị
_STATE_VI = {
    "Tích luỹ": "Tích luỹ", "Phân phối": "Phân phối", "Markup": "Tăng giá",
    "Markdown": "Giảm giá", "Rũ hàng (Shakeout)": "Rũ hàng", "Cao trào mua": "Cao trào mua",
    "Chưa rõ": "Trung tính",
}
_BEARISH_HYP = {"Phân phối", "Markdown", "Cao trào mua"}


def _decision(hyps: List[dict], regime: str, bull: int, bear: int, inst: int,
              trend_quality: int, conflict: float, confidence: int, cx: dict,
              evidence: dict) -> dict:
    """F3 Decision — suy luận từ nhiều đầu vào (KHÔNG chỉ cộng điểm): giả thuyết + regime
    + conflict + context → State/Institution/Trend/Risk/Action(định tính+vùng giá)/Reason."""
    primary = hyps[0] if hyps else {"name": "Chưa rõ", "probability": 0}
    pname, pprob = primary["name"], primary["probability"]

    state = _STATE_VI.get(pname, pname)
    institution = ("Mua mạnh" if bull - bear > 25 else "Bán mạnh" if bear - bull > 25
                   else "Hoạt động" if inst >= 55 else "Trung tính")
    trend_label = ({"trending_up": "Tăng", "trending_down": "Giảm", "sideway": "Đi ngang"}
                   .get(regime, "—")) + (" (mạnh)" if trend_quality >= 60 else " (yếu)" if trend_quality < 35 else "")

    # Risk: mâu thuẫn cao + giả thuyết giảm + confidence thấp → rủi ro cao
    risk_num = (0.40 * conflict + 0.30 * (1.0 if pname in _BEARISH_HYP else 0.0)
                + 0.30 * (1.0 - confidence / 100.0))
    risk_level = "Cao" if risk_num >= 0.6 else ("Trung bình" if risk_num >= 0.35 else "Thấp")

    # Action ĐỊNH TÍNH (không lệnh mua/bán) — theo giả thuyết + confidence + risk
    if pname in _BEARISH_HYP and pprob >= 45:
        action = "Cảnh giác rủi ro"
    elif pname in ("Tích luỹ", "Rũ hàng (Shakeout)") and confidence >= 60 and risk_level != "Cao":
        action = "Theo dõi tích luỹ"
    elif pname == "Markup" and confidence >= 60 and risk_level != "Cao":
        action = "Theo dõi xu hướng tăng"
    elif conflict >= 0.6 or pname == "Chưa rõ":
        action = "Quan sát thêm"
    else:
        action = "Theo dõi"

    sup = cx.get("support"); res = cx.get("resistance"); vwap = cx.get("vwap")
    reference_zones = {"support": sup, "resistance": res, "vwap": vwap, "poc": cx.get("poc")}

    # Reason: ghép từ giả thuyết + bằng chứng mạnh nhất + mâu thuẫn
    sup_ev = [r["label"] for r in evidence.get("supporting", [])[:2]]
    con_ev = [r["label"] for r in evidence.get("contradicting", [])[:1]]
    bits = [f"Giả thuyết ưu thế: **{pname}** ({pprob}%)."]
    if sup_ev:
        bits.append("Ủng hộ: " + ", ".join(sup_ev).lower() + ".")
    if con_ev:
        bits.append("Nhưng: " + ", ".join(con_ev).lower() + ".")
    zone_hint = []
    if res:
        zone_hint.append(f"vượt {res:.2f} xác nhận xu hướng")
    if sup:
        zone_hint.append(f"giữ trên {sup:.2f} là an toàn")
    if zone_hint:
        bits.append("Mốc theo dõi: " + "; ".join(zone_hint) + ".")
    if conflict >= 0.6:
        bits.append("Tín hiệu mâu thuẫn cao → độ tin giảm, cần thêm xác nhận.")

    return {
        "state": state, "institution": institution, "trend": trend_label,
        "risk_level": risk_level, "action": action,
        "reference_zones": reference_zones, "reason": " ".join(bits),
    }


def decide(context: Union[Context, dict], of: dict, events: List[dict],
           vol_trend: float = 0.0, poc_shift: float = 0.0,
           delta_recent: float = 0.0, n_ticks: int = 0,
           memory: Optional[dict] = None) -> dict:
    """Tính toàn bộ Smart Money State. `of` = order_flow.analyze; events = list dict.
    `memory` = tổng hợp nhiều phiên (F3 seam, None = chưa dùng)."""
    cx = _cd(context)
    trend = cx.get("trend", "unknown")
    location = cx.get("location", "mid")
    foreign = float(cx.get("foreign_dir") or 0.0)
    dealer = float(cx.get("dealer_dir") or 0.0)

    g = _agg_events(events)
    absorp = _sat(g.get("absorption", {}).get("sum", 0.0), 1.5)          # 0..1 (bullish hấp thụ)
    supply = _sat(-g.get("supply_absorption", {}).get("sum", 0.0), 1.5)  # 0..1 (bearish cung)
    cluster = _sat(g.get("institution_cluster", {}).get("sum", 0.0), 2.0)  # -1..1
    diverg = 0.0
    if g.get("cvd_divergence"):
        # divergence sum đã gồm dấu (bullish>0 / bearish<0)
        diverg = _clamp(g["cvd_divergence"]["sum"], -1.0, 1.0)

    # ── Market control (bull vs bear) từ CVD + lệnh lớn ──
    cvd = of.get("cvd") or {}
    peak, trough = abs(cvd.get("peak", 0) or 0), abs(cvd.get("trough", 0) or 0)
    cvd_norm = _clamp((cvd.get("last", 0) or 0) / (max(peak, trough, 1)), -1, 1)
    lo = of.get("large_orders") or {}
    lo_tot = (lo.get("buy_val", 0) or 0) + (lo.get("sell_val", 0) or 0)
    lo_net = ((lo.get("buy_val", 0) or 0) - (lo.get("sell_val", 0) or 0)) / lo_tot if lo_tot else 0.0
    flow = 0.45 * cvd_norm + 0.30 * lo_net + 0.25 * (absorp - supply)
    bull_strength = round(100 * max(0.0, flow))
    bear_strength = round(100 * max(0.0, -flow))
    if bull_strength - bear_strength > 15:
        market_control = "Phe mua kiểm soát"
    elif bear_strength - bull_strength > 15:
        market_control = "Phe bán kiểm soát"
    else:
        market_control = "Giằng co"

    # ── Trend quality từ xếp lớp MA ──
    m20, m50, m100 = cx.get("ma20"), cx.get("ma50"), cx.get("ma100")
    trend_quality = 0
    trend_led: List[Contribution] = []
    if m20 and m50 and m100:
        sep = abs(m20 - m100) / m100
        aligned = (m20 > m50 > m100) or (m20 < m50 < m100)
        trend_quality = round(100 * _clamp(sep * 12.0, 0, 1) * (1.0 if aligned else 0.5))
        trend_led = [_mk("trend", f"MA {cx.get('ma_state', '')}"
                         + (" (xếp lớp rõ)" if aligned else " (đan xen)"), trend_quality)]
    trend_conf = RELIABILITY["trend"] if trend_quality else 0.5

    # Chuẩn bị: số lượng event (cho nhãn) + tín hiệu dương/âm
    ec = _event_counts(events)
    a_pos, s_pos = max(0.0, absorp), max(0.0, supply)
    c_pos, c_neg = max(0.0, cluster), max(0.0, -cluster)
    f_pos, f_neg = max(0.0, foreign), max(0.0, -foreign)
    d_pos = max(0.0, dealer)
    fl_pos = max(0.0, flow)
    dv_pos, dv_neg = max(0.0, diverg), max(0.0, -diverg)
    loc_vi = {"support": "hỗ trợ", "resistance": "kháng cự", "breakout": "breakout",
              "inside_va": "vùng giá trị", "at_poc": "POC", "mid": "vùng trung gian"}.get(location, location)
    # #5 Hệ số reliability theo NGỮ CẢNH: cùng tín hiệu, vị trí khác → độ tin khác.
    at_support = location in ("support", "inside_va", "at_poc")
    at_resist = location in ("resistance", "breakout")
    m_absorp = 1.18 if at_support else (0.72 if at_resist else 1.0)   # hấp thụ đáng tin tại hỗ trợ
    m_supply = 1.18 if at_resist else (0.72 if at_support else 1.0)   # cung đáng tin tại kháng cự
    m_clbuy = 1.12 if at_support else 1.0
    m_clsell = 1.12 if at_resist else 1.0

    # ── INSTITUTION ACTIVITY (mức độ hoạt động tổ chức) ──
    n_big = lo.get("count", 0) or 0
    big_density = _clamp(n_big / max(1, n_ticks) * 30, 0, 1)
    cluster_act = _sat(sum(abs(e.get("strength") or 0) * (e.get("confidence") or 0)
                           for e in events if e.get("type") == "institution_cluster"), 2.0)
    inst_l = [
        _mk("cluster", f"{ec['cluster_buy'] + ec['cluster_sell']} cụm lệnh tổ chức", 100 * 0.45 * cluster_act),
        _mk("large", f"Mật độ lệnh lớn ({n_big} lệnh)", 100 * 0.30 * big_density),
        _mk("foreign", "Cường độ khối ngoại", 100 * 0.15 * abs(foreign)),
        _mk("dealer", "Cường độ tự doanh", 100 * 0.10 * abs(dealer)),
    ]
    institution_activity, inst_conf, inst_led = _score_from(inst_l)

    # ── ACCUMULATION: gom âm thầm (hấp thụ, cụm mua, ngoại mua) − phản chứng ──
    base_factor = 1.0 if (trend in ("sideway", "downtrend") and
                          location in ("support", "inside_va", "at_poc")) else 0.35
    acc_l = [
        _mk("absorption", f"{ec['absorption_buy']} lần hấp thụ mua" + (f" tại {loc_vi}" if at_support else ""), 100 * 0.28 * a_pos, rel_mult=m_absorp),
        _mk("cluster", f"{ec['cluster_buy']} cụm lệnh tổ chức mua", 100 * 0.16 * c_pos, rel_mult=m_clbuy),
        _mk("foreign", "Khối ngoại mua ròng", 100 * 0.14 * f_pos),
        _mk("flow", "Lực mua chủ động tăng", 100 * 0.12 * fl_pos),
        _mk("delta", "Delta cải thiện cuối phiên", 100 * 0.10 * (1.0 if delta_recent > 0 else 0.0)),
        _mk("base", f"Nền tích luỹ tại {loc_vi}" if base_factor >= 1.0 else "Vị trí chưa lý tưởng để gom", 100 * 0.12 * base_factor),
        _mk("dealer", "Tự doanh mua ròng", 100 * 0.08 * d_pos),
        # phản chứng (con)
        _mk("foreign", "Khối ngoại bán ròng", -100 * 0.14 * f_neg),
        _mk("poc", "POC dịch xuống", -100 * 0.10 * _poc_sig(poc_shift, up=False)),
        _mk("supply", "Có cung chủ động chặn", -100 * 0.12 * s_pos, rel_mult=m_supply),
        _mk("trend", "Xu hướng giảm còn hiệu lực", -100 * 0.08 * (1.0 if trend == "downtrend" else 0.0)),
    ]
    accumulation_score, acc_conf, acc_led = _score_from(acc_l)

    # ── DISTRIBUTION: xả tại đỉnh — GATE bởi absorption (mấu chốt STB) ──
    absorbed = a_pos
    top_factor = 1.0 if (trend in ("uptrend", "sideway") and
                         location in ("resistance", "breakout")) else 0.35
    foreign_dist = f_neg * (1.0 - absorbed)                       # ngoại bán được hấp thụ → không tính
    dist_l = [
        _mk("supply", "Cung chủ động (giá không lên)", 100 * 0.30 * s_pos, rel_mult=m_supply),
        _mk("cluster", f"{ec['cluster_sell']} cụm lệnh tổ chức bán", 100 * 0.16 * c_neg, rel_mult=m_clsell),
        _mk("foreign", "Khối ngoại bán ròng (không được hấp thụ)", 100 * 0.16 * foreign_dist),
        _mk("divergence", "CVD phân kỳ giảm", 100 * 0.14 * dv_neg),
        _mk("poc", "POC dịch xuống", 100 * 0.08 * _poc_sig(poc_shift, up=False)),
        _mk("location", f"Xả tại {loc_vi}", 100 * 0.08 * (top_factor * s_pos)),
        # phản chứng (con)
        _mk("absorption", "Lực bán đang được hấp thụ", -100 * 0.18 * a_pos, rel_mult=m_absorp),
        _mk("foreign", "Khối ngoại mua ròng", -100 * 0.10 * f_pos),
    ]
    distribution_score, dist_conf, dist_led = _score_from(dist_l)

    # ── BREAKOUT probability ──
    if location == "breakout":
        loc_factor = 1.0
    elif location == "resistance" and flow > 0:
        loc_factor = 0.6
    else:
        loc_factor = 0.15
    brk_l = [
        _mk("location", f"Vị trí {loc_vi}", 100 * 0.34 * loc_factor),
        _mk("flow", "Dòng tiền mua áp đảo", 100 * 0.22 * fl_pos),
        _mk("cluster", "Cụm tổ chức mua", 100 * 0.16 * c_pos),
        _mk("poc", "POC dịch lên", 100 * 0.12 * _poc_sig(poc_shift, up=True)),
        _mk("vol", "Thanh khoản mở rộng", 100 * 0.10 * max(0.0, vol_trend)),
        _mk("supply", "Có cung chặn tại vùng cao", -100 * 0.10 * s_pos),
    ]
    breakout_score, brk_conf, brk_led = _score_from(brk_l)

    # ── Wyckoff MỞ RỘNG ──
    phase, phase_note = _wyckoff(trend, location, accumulation_score, distribution_score,
                                 breakout_score, bull_strength, bear_strength, absorp,
                                 supply, diverg, institution_activity, vol_trend)

    # Hướng chi phối (gom / xả / trung tính) — cho Evidence Engine
    if accumulation_score >= distribution_score + 8:
        lean = "accumulation"
    elif distribution_score >= accumulation_score + 8:
        lean = "distribution"
    else:
        lean = "neutral"

    # ── #3 CONFLICT: tín hiệu đối kháng cùng mạnh → mâu thuẫn cao → confidence giảm ──
    _csig = [(cvd_norm, 0.70), (flow, 0.70), (absorp - supply, 0.82),
             (foreign, 0.75), (cluster, 0.85), (diverg, 0.65)]
    cpos = sum(max(0.0, v) * r for v, r in _csig)
    cneg = sum(max(0.0, -v) * r for v, r in _csig)
    conflict = (2 * min(cpos, cneg) / (cpos + cneg)) if (cpos + cneg) > 0 else 0.0
    conflict_level = "Cao" if conflict >= 0.6 else ("Trung bình" if conflict >= 0.3 else "Thấp")

    # ── Smart money confidence: đủ dữ liệu × độ tin điểm CHI PHỐI × (1 − phạt mâu thuẫn) ──
    data_suff = _clamp(n_ticks / 500.0, 0, 1)
    dom_conf = acc_conf if accumulation_score >= distribution_score else dist_conf
    smart_money_confidence = round(100 * _clamp((0.40 * data_suff + 0.60 * dom_conf) *
                                                (1.0 - 0.35 * conflict), 0, 1))

    # ── F2 Evidence Engine: ✓/✗ cho kết luận chi phối ──
    evidence = _evidence_engine(lean, phase, acc_led, dist_led, acc_conf, dist_conf,
                                breakout_score, cx, conflict)

    # ── F3 Hypothesis + Decision (rule-based inference, KHÔNG chỉ cộng điểm) ──
    regime = cx.get("regime", "unknown")
    hypotheses = _hypotheses(regime, location, accumulation_score, distribution_score,
                             breakout_score, institution_activity, absorp, supply, diverg,
                             cluster, flow, conflict, memory)
    decision_out = _decision(hypotheses, regime, bull_strength, bear_strength,
                             institution_activity, trend_quality, conflict,
                             smart_money_confidence, cx, evidence)

    # ── F4 Story Engine: kể chuyện dòng tiền + Smart Money Story ──
    from app.services import story as _story
    story = _story.build_story(
        of.get("series") or [], events, cx,
        {"absorp": absorp, "supply": supply, "cluster": cluster, "flow": flow,
         "foreign": foreign, "poc_shift": poc_shift, "diverg": diverg},
        decision_out, hypotheses)

    ledgers = {
        "accumulation": [c.to_dict() for c in acc_led],
        "distribution": [c.to_dict() for c in dist_led],
        "breakout": [c.to_dict() for c in brk_led],
        "institution": [c.to_dict() for c in inst_led],
        "trend": [c.to_dict() for c in trend_led],
    }
    score_confidence = {
        "accumulation": round(100 * acc_conf), "distribution": round(100 * dist_conf),
        "breakout": round(100 * brk_conf), "institution": round(100 * inst_conf),
        "trend": round(100 * trend_conf),
    }
    components = {
        "absorption": round(absorp, 3), "supply": round(supply, 3),
        "cluster": round(cluster, 3), "divergence": round(diverg, 3),
        "flow": round(flow, 3), "foreign_dir": foreign, "dealer_dir": dealer,
        "foreign_dist_gated": round(foreign_dist, 3), "cvd_norm": round(cvd_norm, 3),
        "large_net": round(lo_net, 3), "poc_shift": round(poc_shift, 3),
    }
    report = _report(context=cx, of=of, phase=phase, phase_note=phase_note,
                     accumulation_score=accumulation_score, distribution_score=distribution_score,
                     breakout_score=breakout_score, trend_quality=trend_quality,
                     institution_activity=institution_activity, bull_strength=bull_strength,
                     absorp=absorp, supply=supply, diverg=diverg, delta_recent=delta_recent,
                     cvd_last=cvd.get("last", 0), poc_shift=poc_shift, market_control=market_control)
    evidence_chain = _evidence(events, cx, absorp, supply, diverg)

    return {
        "accumulation_score": accumulation_score,
        "distribution_score": distribution_score,
        "breakout_score": breakout_score,
        "trend_quality": trend_quality,
        "institution_activity": institution_activity,
        "bull_strength": bull_strength,
        "bear_strength": bear_strength,
        "market_control": market_control,
        "smart_money_confidence": smart_money_confidence,
        "conflict": round(100 * conflict),
        "conflict_level": conflict_level,
        "regime": cx.get("regime", "unknown"),
        "wyckoff_phase": phase,
        "phase_note": phase_note,
        "components": components,
        "ledgers": ledgers,                 # F1: sổ cái đóng góp từng điểm số
        "score_confidence": score_confidence,
        "evidence": evidence,               # F2: Evidence Engine (✓/✗ + confidence)
        "hypotheses": hypotheses,           # F3: giả thuyết song song + xác suất
        "decision": decision_out,           # F3: State/Institution/Trend/Risk/Action/Reason
        "story": story,                     # F4: kể chuyện dòng tiền (beats + narrative)
        "evidence_chain": evidence_chain,
        "report": report,
        "n_events": len(events),
    }


def _wyckoff(trend, location, acc, dist, brk, bull, bear, absorp, supply, diverg,
             inst, vol_trend) -> tuple[str, str]:
    """Cây quyết định Wyckoff MỞ RỘNG: Accumulation → Spring → Markup → Buying Climax →
    Distribution → Markdown (+ Trung tính). Minh bạch, dựa trên tổ hợp trend + điểm."""
    # Đỉnh kiệt sức: uptrend + tổ chức rất mạnh + phân kỳ giảm/cung nổi tại đỉnh
    if trend == "uptrend" and inst >= 60 and (diverg <= -0.35 or supply >= 0.5) \
            and location in ("resistance", "breakout"):
        return "Buying Climax", "Uptrend nhưng cung nổi + phân kỳ tại đỉnh → nghi cao trào mua, cảnh giác đảo chiều"
    if trend == "uptrend":
        if dist >= 55 and dist > acc and location in ("resistance", "breakout"):
            return "Distribution", "Uptrend nhưng phân phối rõ tại kháng cự → rủi ro tạo đỉnh"
        return "Markup", "Xu hướng tăng còn hiệu lực" + (" (đang được hấp thụ)" if absorp >= 0.4 else "")
    if trend == "downtrend":
        if acc >= 55 and acc > dist:
            return "Accumulation", "Downtrend nhưng có dấu hiệu gom tạo đáy → theo dõi tích luỹ"
        return "Markdown", "Xu hướng giảm còn hiệu lực"
    # sideway
    if acc >= 55 and acc > dist:
        if location in ("support", "inside_va") and absorp >= 0.45:
            return "Spring", "Nền sideway, hấp thụ lực bán tại hỗ trợ → nghi cú rũ gom (Spring)"
        return "Accumulation", "Nền sideway, dòng tiền gom âm thầm, chưa vào Markup"
    if dist >= 55 and dist > acc:
        return "Distribution", "Nền sideway sau tăng, phân phối chiếm ưu thế"
    return "Trung tính", "Chưa đủ tín hiệu nghiêng về gom hay xả"


def _stars(score: float) -> int:
    return int(_clamp(round(score / 20.0), 0, 5))


def _report(context, of, phase, phase_note, accumulation_score, distribution_score,
            breakout_score, trend_quality, institution_activity, bull_strength,
            absorp, supply, diverg, delta_recent, cvd_last, poc_shift, market_control) -> dict:
    trend = context.get("trend", "unknown")
    trend_vi = {"uptrend": "Tăng", "sideway": "Đi ngang", "downtrend": "Giảm"}.get(trend, "—")
    vwap_side = context.get("vwap_side", "at")
    vwap_flag = {"above": "Chấp nhận (giá trên VWAP)", "below": "Từ chối (giá dưới VWAP)",
                 "at": "Cân bằng quanh VWAP"}.get(vwap_side, "—")
    poc_flag = ("Dịch lên" if poc_shift > 0.001 else ("Dịch xuống" if poc_shift < -0.001 else "Ổn định"))
    stars = {
        "trend": _stars(trend_quality),
        "institution": _stars(institution_activity),
        "accumulation": _stars(accumulation_score),
        "distribution": _stars(distribution_score),
        "breakout": _stars(breakout_score),
    }
    flags = {
        "absorption": "Detected" if absorp >= 0.35 else "Not detected",
        "distribution": "Detected" if distribution_score >= 50 else "Not detected",
        "vwap": vwap_flag,
        "poc": poc_flag,
        "delta": "Dương" if delta_recent > 0 else ("Âm" if delta_recent < 0 else "Trung tính"),
        "cvd": "Dương" if (cvd_last or 0) > 0 else ("Âm" if (cvd_last or 0) < 0 else "Trung tính"),
    }
    conclusion = _conclusion(context, phase, phase_note, trend_vi, accumulation_score,
                             distribution_score, breakout_score, absorp, supply, diverg,
                             market_control)
    return {"trend_label": trend_vi, "phase": phase, "stars": stars, "flags": flags,
            "conclusion": conclusion}


def _conclusion(context, phase, phase_note, trend_vi, acc, dist, brk, absorp, supply,
                diverg, market_control) -> str:
    """Kết luận tiếng Việt, ghép từ trạng thái — giọng như mẫu người dùng đưa."""
    parts: List[str] = []
    loc = context.get("location", "mid")
    loc_vi = {"support": "vùng hỗ trợ", "resistance": "vùng kháng cự", "breakout": "vùng breakout",
              "inside_va": "trong vùng giá trị", "at_poc": "quanh POC", "mid": "vùng trung gian"}.get(loc, loc)
    foreign = context.get("foreign_dir", 0.0)

    parts.append(f"Cổ phiếu đang ở pha **{phase}** — {phase_note.lower()}.")
    if absorp >= 0.35:
        if foreign < -0.15:
            parts.append("Lực bán (gồm khối ngoại) đang được HẤP THỤ — giá giữ vững bất chấp cung ra, dấu hiệu tay to đỡ.")
        else:
            parts.append("Có dấu hiệu hấp thụ lực bán, giá giữ vững.")
    if supply >= 0.4:
        parts.append("Xuất hiện cung chủ động tại vùng cao — theo dõi khả năng phân phối.")
    if diverg <= -0.35:
        parts.append("CVD phân kỳ giảm so với giá — cầu có dấu hiệu suy yếu.")
    elif diverg >= 0.35:
        parts.append("CVD phân kỳ tăng — cung có dấu hiệu cạn.")

    if phase in ("Markup", "Spring", "Accumulation") and dist < 50:
        parts.append(f"Chưa xuất hiện tín hiệu phân phối rõ ràng (điểm phân phối {dist}/100).")
    if brk >= 55:
        parts.append(f"Xác suất breakout khá ({brk}/100) — theo dõi khi vượt {loc_vi} với thanh khoản gia tăng.")
    elif phase in ("Accumulation", "Spring"):
        parts.append("Cần thêm thời gian tích luỹ trước khi kỳ vọng bước vào Markup.")
    parts.append(f"Thế trận: {market_control.lower()}.")
    return " ".join(parts)


def _evidence(events: List[dict], cx: dict, absorp: float, supply: float, diverg: float) -> List[str]:
    """Chuỗi bằng chứng: ngữ cảnh + các event mạnh nhất (để người dùng truy vết kết luận)."""
    out: List[str] = []
    ma = cx.get("ma_state", "")
    out.append(f"Xu hướng {cx.get('trend','?')}"
               + (f" [{ma}]" if ma else "")
               + f", giá tại {cx.get('location','?')}")
    if cx.get("foreign_dir"):
        fd = cx["foreign_dir"]
        out.append(f"Khối ngoại {'mua ròng' if fd > 0 else 'bán ròng'} (dir {fd:+.2f})")
    for e in sorted(events, key=lambda e: abs(e.get("strength") or 0) * (e.get("confidence") or 0),
                    reverse=True)[:4]:
        ts = (e.get("ts") or "")[11:19]
        evd = (e.get("evidence") or [""])[0]
        out.append(f"[{ts}] {e.get('type')} (str {e.get('strength'):+.2f}, conf {e.get('confidence'):.2f}): {evd}")
    return out
