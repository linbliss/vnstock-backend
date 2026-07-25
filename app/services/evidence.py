"""evidence — LAYER 1.5: biến METRIC thô thành EVIDENCE có DIỄN GIẢI THEO CONTEXT.

Nguyên tắc (xem docs/smart-money-design.md · Inference evolution):
  Metric KHÔNG mang hướng cố định. `Price > VWAP` không đồng nghĩa Bullish; `CVD↑` không
  đồng nghĩa Accumulation; `Large Buy` không đồng nghĩa Institution Buying. Mỗi metric chỉ
  trở thành Evidence khi được diễn giải TRONG context (trend/regime/location/foreign…), và
  `direction` do context quyết định — KHÔNG cố định.

`derive_evidence(of, context, events) -> List[Evidence]` gom mọi metric + event thành một
KHO EVIDENCE ĐỒNG NHẤT. Đây là input cho Decision/Hypothesis/Story (các phase sau). P1 chỉ
tạo lớp này + endpoint debug, CHƯA nối vào Decision.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional

# Khoá giả thuyết (canonical) để Evidence khai báo nó ủng hộ giả thuyết nào
ACC, DIST, MARKUP, MARKDOWN, SHAKEOUT, CLIMAX = \
    "accumulation", "distribution", "markup", "markdown", "shakeout", "climax"


@dataclass
class Evidence:
    kind: str                 # loại evidence (price_vs_vwap, cvd_flow, large_order_bias…)
    raw: str                  # metric + giá trị (dữ kiện thô)
    claim: str                # DIỄN GIẢI trong context (ngôn ngữ người)
    direction: float          # -1..1 (dương=bullish) — SUY TỪ CONTEXT
    reliability: float        # 0..1 (đã điều chỉnh theo context)
    supports: List[str] = field(default_factory=list)      # giả thuyết được ủng hộ
    context_used: List[str] = field(default_factory=list)  # context nào đã dùng để suy

    def to_dict(self) -> dict:
        return asdict(self)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _ev_counts(events: List[dict]) -> dict:
    c = {"absorption_buy": 0, "supply": 0, "cluster_buy": 0, "cluster_sell": 0,
         "div_bull": 0, "div_bear": 0}
    for e in events:
        t, s = e.get("type", ""), e.get("strength") or 0
        if t == "absorption" and s > 0:
            c["absorption_buy"] += 1
        elif t == "supply_absorption":
            c["supply"] += 1
        elif t == "institution_cluster":
            c["cluster_buy" if s >= 0 else "cluster_sell"] += 1
        elif t == "cvd_divergence":
            c["div_bull" if s > 0 else "div_bear"] += 1
    return c


def derive_evidence(of: dict, context: dict, events: List[dict]) -> List[dict]:
    """Trả list Evidence (dict). Thuần, phòng thủ với field thiếu."""
    if not of or of.get("empty"):
        return []
    ev: List[Evidence] = []
    trend = context.get("trend", "unknown")
    regime = context.get("regime", "unknown")
    location = context.get("location", "mid")
    vwap_side = context.get("vwap_side", "at")
    foreign = float(context.get("foreign_dir") or 0.0)
    dealer = float(context.get("dealer_dir") or 0.0)
    poc = context.get("poc"); va_low = context.get("va_low"); va_high = context.get("va_high")
    price = context.get("price")
    vwap = of.get("vwap")
    cvd = of.get("cvd") or {}
    lo = of.get("large_orders") or {}
    ec = _ev_counts(events)

    at_support = location in ("support", "inside_va", "at_poc")
    at_resist = location in ("resistance", "breakout")
    sideway = regime == "sideway"
    up = regime == "trending_up"
    down = regime == "trending_down"
    absorbed = ec["absorption_buy"] > 0    # lực bán đang được hấp thụ?

    # ── [1] Giá vs VWAP — hướng phụ thuộc VỊ TRÍ + TREND ──
    if price and vwap:
        raw = f"Giá {price:.2f} {'>' if price > vwap else '<'} VWAP {vwap:.2f}"
        if price > vwap:
            if at_resist:
                d, rel, claim = 0.12, 0.55, "Giá trên VWAP nhưng đang test kháng cự — dễ bị chặn, chưa chắc bullish"
            elif up:
                d, rel, claim = 0.45, 0.70, "Giá giữ trên VWAP trong uptrend — bên mua kiểm soát vùng giá trị"
            else:
                d, rel, claim = 0.30, 0.62, "Bên mua chiếm ưu thế quanh giá trị trung bình"
            sup = [MARKUP]
        else:
            if at_support:
                d, rel, claim = -0.10, 0.58, "Giá dưới VWAP nhưng tại hỗ trợ — có thể là vùng gom giá rẻ"
                sup = [ACC]
            elif down:
                d, rel, claim = -0.40, 0.68, "Giá dưới VWAP trong downtrend — bên bán kiểm soát"
                sup = [MARKDOWN]
            else:
                d, rel, claim = -0.25, 0.60, "Giao dịch dưới giá trị trung bình — phe bán tạm ưu thế"
                sup = [MARKDOWN]
        ev.append(Evidence("price_vs_vwap", raw, claim, d, rel, sup,
                           [f"location={location}", f"regime={regime}"]))

    # ── [2] CVD flow — hướng phụ thuộc REGIME + LOCATION + phân kỳ ──
    peak, trough = abs(cvd.get("peak", 0) or 0), abs(cvd.get("trough", 0) or 0)
    cvd_norm = _clamp((cvd.get("last", 0) or 0) / max(peak, trough, 1), -1, 1)
    if abs(cvd_norm) > 0.05:
        raw = f"CVD {'+' if cvd_norm >= 0 else ''}{cvd.get('last', 0):,} (chuẩn hoá {cvd_norm:+.2f})"
        if cvd_norm > 0:
            if sideway and at_support:
                d, rel, claim, sup = 0.55, 0.72, "Mua chủ động cộng dồn tại nền hỗ trợ — dấu hiệu gom âm thầm", [ACC]
            elif up:
                d, rel, claim, sup = 0.40, 0.68, "Dòng mua chủ động ủng hộ xu hướng tăng", [MARKUP]
            elif at_resist and ec["div_bear"]:
                d, rel, claim, sup = 0.10, 0.55, "CVD dương nhưng phân kỳ tại kháng cự — cầu có dấu hiệu đuối", [CLIMAX]
            else:
                d, rel, claim, sup = 0.30, 0.62, "Mua chủ động nhỉnh hơn bán chủ động", [MARKUP]
        else:
            if down:
                d, rel, claim, sup = -0.45, 0.68, "Bán chủ động cộng dồn trong downtrend", [MARKDOWN]
            elif at_resist:
                d, rel, claim, sup = -0.35, 0.65, "Bán chủ động áp đảo tại kháng cự — nguy cơ phân phối", [DIST]
            else:
                d, rel, claim, sup = -0.28, 0.60, "Bán chủ động nhỉnh hơn mua chủ động", [DIST]
        ev.append(Evidence("cvd_flow", raw, claim, d, rel, sup,
                           [f"regime={regime}", f"location={location}",
                            f"divergence={'bear' if ec['div_bear'] else 'bull' if ec['div_bull'] else 'none'}"]))

    # ── [3] Large order bias — Large Buy ≠ Institution Buying (phụ thuộc VỊ TRÍ + hấp thụ) ──
    n_big = lo.get("count", 0) or 0
    if n_big >= 3:
        blp = lo.get("buy_large_pct", 0); avp = lo.get("above_vwap_pct", 0)
        raw = f"{n_big} lệnh lớn · mua {blp}% · trên VWAP {avp}%"
        if blp >= 60:
            if at_support:
                d, rel, claim, sup = 0.55, 0.80, "Lệnh lớn MUA dồn tại hỗ trợ — tổ chức gom", [ACC, MARKUP]
            elif at_resist and absorbed:
                d, rel, claim, sup = 0.35, 0.78, "Lệnh lớn mua HẤP THỤ cung tại kháng cự — institutional absorption (không phải phân phối)", [MARKUP, ACC]
            else:
                d, rel, claim, sup = 0.35, 0.70, "Tổ chức nghiêng về mua chủ động", [MARKUP]
        elif lo.get("sell_large_pct", 0) >= 60:
            if at_resist:
                d, rel, claim, sup = -0.45, 0.78, "Lệnh lớn BÁN dồn tại kháng cự — dấu hiệu phân phối", [DIST]
            else:
                d, rel, claim, sup = -0.30, 0.68, "Tổ chức nghiêng về bán chủ động", [DIST]
        else:
            d, rel, claim, sup = 0.0, 0.55, "Lệnh lớn hai chiều cân bằng — chưa rõ ý đồ tổ chức", []
        ev.append(Evidence("large_order_bias", raw, claim, d, rel, sup,
                           [f"location={location}", f"absorbed={absorbed}"]))

    # ── [4] POC vị trí — POC↑ ≠ Uptrend ──
    if poc and price:
        raw = f"POC {poc:.2f} · giá {price:.2f}"
        if price > poc:
            d, rel, claim, sup = (0.25, 0.58, "Giá được chấp nhận TRÊN vùng giá trị (POC) — nghiêng tích cực", [MARKUP])
            if at_resist:
                d, claim = 0.10, "Giá trên POC nhưng sát kháng cự — cần vượt để xác nhận"
        else:
            d, rel, claim, sup = (-0.20, 0.56, "Giá dưới vùng giá trị (POC) — đang test lại giá trị", [DIST])
            if at_support:
                d, claim = -0.05, "Giá dưới POC nhưng tại hỗ trợ — vùng cân bằng, chưa nghiêng hẳn"
        ev.append(Evidence("poc_position", raw, claim, d, rel, sup, [f"location={location}"]))

    # ── [5] Value Area ──
    if va_low and va_high and price:
        if price > va_high:
            ev.append(Evidence("value_area", f"Giá {price:.2f} > VA cao {va_high:.2f}",
                               "Giá bứt lên trên Value Area — chấp nhận giá cao hơn", 0.30, 0.58, [MARKUP, "breakout"],
                               [f"va=[{va_low:.2f},{va_high:.2f}]"]))
        elif price < va_low:
            ev.append(Evidence("value_area", f"Giá {price:.2f} < VA thấp {va_low:.2f}",
                               "Giá rơi dưới Value Area — chấp nhận giá thấp hơn", -0.30, 0.58, [MARKDOWN],
                               [f"va=[{va_low:.2f},{va_high:.2f}]"]))

    # ── [6] Absorption (event) — tại hỗ trợ đáng tin hơn ──
    if ec["absorption_buy"] > 0:
        rel = 0.82 if at_support else (0.62 if at_resist else 0.72)
        claim = (f"{ec['absorption_buy']} lần hấp thụ lực bán tại hỗ trợ — tay to đỡ giá (gom)" if at_support
                 else f"{ec['absorption_buy']} lần hấp thụ lực bán — cung bị hấp thụ, giá giữ")
        ev.append(Evidence("absorption", f"{ec['absorption_buy']} vùng hấp thụ mua", claim,
                           _clamp(0.35 + 0.12 * ec["absorption_buy"], 0, 0.9), rel,
                           [ACC, SHAKEOUT] if at_support else [ACC, MARKUP], [f"location={location}"]))
    # ── [7] Supply (event) — tại kháng cự đáng tin hơn ──
    if ec["supply"] > 0:
        rel = 0.82 if at_resist else 0.66
        ev.append(Evidence("supply", f"{ec['supply']} vùng cung chủ động",
                           f"{ec['supply']} lần cung chủ động ép — giá không lên được (phân phối)",
                           -_clamp(0.35 + 0.12 * ec["supply"], 0, 0.9), rel, [DIST], [f"location={location}"]))
    # ── [8] Institution cluster (event) ──
    if ec["cluster_buy"] + ec["cluster_sell"] > 0:
        net = ec["cluster_buy"] - ec["cluster_sell"]
        d = _clamp(net / max(1, ec["cluster_buy"] + ec["cluster_sell"]), -1, 1) * 0.5
        ev.append(Evidence("cluster", f"{ec['cluster_buy']} cụm mua / {ec['cluster_sell']} cụm bán",
                           "Cụm lệnh tổ chức dồn dập — có bàn tay lớn hoạt động", d, 0.85,
                           [MARKUP if net >= 0 else DIST], [f"location={location}"]))
    # ── [9] Divergence (event) ──
    if ec["div_bear"]:
        ev.append(Evidence("divergence", "Phân kỳ giá/CVD giảm",
                           "Giá lên nhưng CVD không theo — cầu suy, cảnh báo đảo chiều", -0.45, 0.65,
                           [CLIMAX, DIST], [f"regime={regime}"]))
    elif ec["div_bull"]:
        ev.append(Evidence("divergence", "Phân kỳ giá/CVD tăng",
                           "Giá xuống nhưng CVD giữ — cung cạn, khả năng đảo chiều lên", 0.45, 0.65,
                           [ACC, SHAKEOUT], [f"regime={regime}"]))

    # ── [10] Foreign flow — GATE bởi hấp thụ (mấu chốt): ngoại bán mà được hấp thụ ≠ bearish ──
    if abs(foreign) > 0.10:
        if foreign < 0:
            if absorbed:
                d, rel, claim, sup = -0.10, 0.70, "Khối ngoại bán ròng NHƯNG đang được hấp thụ — áp lực bị trung hoà", [ACC, MARKUP]
            else:
                d, rel, claim, sup = -0.45, 0.75, "Khối ngoại bán ròng gây áp lực lên giá", [DIST, MARKDOWN]
        else:
            d, rel, claim, sup = 0.45, 0.75, "Khối ngoại mua ròng hỗ trợ xu hướng", [ACC, MARKUP]
        ev.append(Evidence("foreign_flow", f"Khối ngoại dir {foreign:+.2f}", claim, d, rel, sup,
                           [f"absorbed={absorbed}"]))
    # ── [11] Dealer flow ──
    if abs(dealer) > 0.10:
        d = _clamp(dealer, -1, 1) * 0.35
        ev.append(Evidence("dealer_flow", f"Tự doanh dir {dealer:+.2f}",
                           f"Tự doanh {'mua' if dealer > 0 else 'bán'} ròng", d, 0.55,
                           [ACC if dealer > 0 else DIST], []))

    return [e.to_dict() for e in ev]
