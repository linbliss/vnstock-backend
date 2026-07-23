"""story — LAYER 5 Story Engine (xem docs/smart-money-design.md).

Biến chuỗi Event + diễn biến giá thành CÂU CHUYỆN dòng tiền (không phải event-list):
  • beats: dòng thời gian có lời kể (mở cửa → các mốc → cuối phiên → ATC → kết luận).
  • narrative: đoạn "Smart Money Story" tổng hợp hành vi tiền lớn cả phiên, giọng chuyên gia.

Thuần, không gọi API. Nhận series (cvd/vwap/price theo thời gian) + events + context +
signals (đã tính ở decision) + decision/hypotheses.
"""
from __future__ import annotations
from typing import Dict, List, Optional

_LOC_VI = {"support": "hỗ trợ", "resistance": "kháng cự", "breakout": "vùng breakout",
           "inside_va": "vùng giá trị", "at_poc": "quanh POC", "mid": "vùng trung gian"}
_EV_VI = {"absorption": "Hấp thụ lực bán (gom)", "supply_absorption": "Cung chủ động (giá không lên)",
          "cvd_divergence": "Phân kỳ giá/CVD", "institution_cluster": "Cụm lệnh tổ chức"}


def _hhmm(ts: str) -> str:
    return ts[11:16] if ts and len(ts) >= 16 else "—"


def _seg(series: List[dict], lo: float, hi: float):
    """(Δcvd, Δgiá%) trên đoạn [lo,hi] của series (theo tỉ lệ)."""
    n = len(series)
    if n < 2:
        return 0.0, 0.0
    a, b = int(n * lo), max(int(n * lo) + 1, int(n * hi))
    seg = series[a:min(b, n)]
    if len(seg) < 2:
        return 0.0, 0.0
    dcvd = seg[-1]["cvd"] - seg[0]["cvd"]
    p0 = seg[0]["price"]
    dprice = (seg[-1]["price"] - p0) / p0 * 100 if p0 else 0.0
    return dcvd, dprice


def _beat(time: str, text: str, tone: str) -> dict:
    return {"time": time, "text": text, "tone": tone}   # tone: bull|bear|neutral


def build_story(series: List[dict], events: List[dict], cx: dict, signals: dict,
                decision: dict, hypotheses: List[dict]) -> dict:
    beats: List[dict] = []

    # 1) MỞ CỬA
    if series:
        dcvd0, dp0 = _seg(series, 0.0, 0.25)
        _t = str(series[0].get("t") or "")
        t0 = _t[:5] if len(_t) >= 5 else "09:15"     # series "t" = "HH:MM:SS"
        if dcvd0 < 0 and dp0 >= -0.2:
            beats.append(_beat(t0, "Bên bán chủ động chiếm ưu thế, nhưng giá không giảm tương ứng.", "bull"))
        elif dcvd0 > 0 and dp0 <= 0.2:
            beats.append(_beat(t0, "Bên mua chủ động áp đảo, nhưng giá chưa bứt lên.", "bear"))
        elif dcvd0 >= 0:
            beats.append(_beat(t0, "Bên mua chủ động dẫn dắt, giá nhích lên.", "bull"))
        else:
            beats.append(_beat(t0, "Bên bán chủ động ép giá xuống.", "bear"))

    # 2) CÁC MỐC SỰ KIỆN — mạnh nhất, ĐA DẠNG loại (tối đa 2/loại), theo thời gian
    top = sorted(events, key=lambda e: abs(e.get("strength") or 0) * (e.get("confidence") or 0), reverse=True)
    per_type: Dict[str, int] = {}
    picks = []
    for e in top:
        typ = e.get("type", "")
        if per_type.get(typ, 0) >= 2:
            continue
        per_type[typ] = per_type.get(typ, 0) + 1
        picks.append(e)
        if len(picks) >= 5:
            break
    for e in sorted(picks, key=lambda e: e.get("ts", "")):
        typ = e.get("type", "")
        strg = e.get("strength") or 0
        loc = (e.get("context") or {}).get("location", "")
        if typ == "institution_cluster":
            label = "Cụm lệnh tổ chức mua" if strg >= 0 else "Cụm lệnh tổ chức bán"
        else:
            label = _EV_VI.get(typ, typ)
        loc_txt = f" tại {_LOC_VI.get(loc, '')}" if loc in _LOC_VI and loc != "mid" else ""
        beats.append(_beat(_hhmm(e.get("ts", "")), f"{label}{loc_txt}.", "bull" if strg > 0 else "bear"))

    # 3) CUỐI PHIÊN
    if series:
        dcvd1, dp1 = _seg(series, 0.75, 1.0)
        late_supply = any(ev.get("type") == "supply_absorption" and _hhmm(ev.get("ts", "")) >= "13:30" for ev in events)
        if late_supply:
            beats.append(_beat("Cuối phiên", "Xuất hiện cung chủ động — cần theo dõi phân phối.", "bear"))
        elif abs(dcvd1) < (abs(_seg(series, 0.0, 0.25)[0]) * 0.5 + 1):
            beats.append(_beat("Cuối phiên", "Lực cầu/cung yếu dần về cuối phiên, chưa có tín hiệu phân phối rõ.", "neutral"))
        elif dcvd1 > 0:
            beats.append(_beat("Cuối phiên", "Cầu chủ động mạnh lên về cuối phiên.", "bull"))
        else:
            beats.append(_beat("Cuối phiên", "Cung chủ động gia tăng về cuối phiên.", "bear"))

    # 4) ATC
    atc = [ev for ev in events if _hhmm(ev.get("ts", "")) >= "14:30"]
    if not atc:
        beats.append(_beat("ATC", "Phiên ATC không có tín hiệu bất thường.", "neutral"))

    # 5) KẾT LUẬN
    primary = hypotheses[0] if hypotheses else {"name": "Chưa rõ", "probability": 0}
    beats.append(_beat("Kết luận", f"{decision.get('state', '')} — {decision.get('action', '')} "
                       f"(giả thuyết {primary['name']} {primary['probability']}%).",
                       "bull" if decision.get("state") in ("Tích luỹ", "Tăng giá", "Rũ hàng") else
                       "bear" if decision.get("state") in ("Phân phối", "Giảm giá", "Cao trào mua") else "neutral"))

    narrative = _smart_money_story(cx, signals, decision, hypotheses)
    return {"beats": beats, "narrative": narrative}


def _smart_money_story(cx: dict, sig: dict, decision: dict, hypotheses: List[dict]) -> str:
    """Đoạn văn tổng hợp hành vi tiền lớn — giọng chuyên gia."""
    absorp = sig.get("absorp", 0.0); supply = sig.get("supply", 0.0)
    cluster = sig.get("cluster", 0.0); flow = sig.get("flow", 0.0)
    foreign = sig.get("foreign", 0.0); poc_shift = sig.get("poc_shift", 0.0)
    vwap_side = cx.get("vwap_side", "at")
    loc = _LOC_VI.get(cx.get("location", "mid"), "vùng trung gian")
    parts: List[str] = []

    # Hành vi tiền lớn: đẩy giá hay hấp thụ?
    if absorp >= 0.35 and cluster > 0 and flow < 0.5:
        parts.append(f"Dòng tiền lớn hôm nay không mua đẩy giá, mà hấp thụ lượng bán chủ động tại {loc}.")
    elif flow >= 0.5 and cluster > 0:
        parts.append("Dòng tiền lớn chủ động mua đẩy giá lên.")
    elif supply >= 0.4:
        parts.append(f"Xuất hiện cung chủ động lớn tại {loc} — dấu hiệu phân phối cần theo dõi.")
    else:
        parts.append("Dòng tiền lớn chưa thể hiện ý đồ rõ ràng trong phiên.")

    # Khối ngoại vs giá
    if foreign < -0.15:
        if absorp >= 0.35:
            parts.append("Khối ngoại bán ròng mạnh nhưng giá chỉ giảm nhẹ — lực bán đang được hấp thụ.")
        else:
            parts.append("Khối ngoại bán ròng gây áp lực lên giá.")
    elif foreign > 0.15:
        parts.append("Khối ngoại mua ròng hỗ trợ xu hướng.")

    # POC / VWAP
    if poc_shift > 0.01:
        parts.append("POC dịch lên cho thấy vùng giá trị đang cải thiện.")
    elif poc_shift < -0.01:
        parts.append("POC dịch xuống — vùng giá trị chưa cải thiện.")
    else:
        parts.append("POC gần như đi ngang, vùng giá trị chưa cải thiện.")
    if vwap_side == "above":
        parts.append("VWAP vẫn được giữ.")
    elif vwap_side == "below":
        parts.append("Giá đang nằm dưới VWAP.")

    # Kết luận theo giả thuyết
    if len(hypotheses) >= 2:
        p, s = hypotheses[0], hypotheses[1]
        parts.append(f"Do đó xác suất đây là giai đoạn **{p['name']} ({p['probability']}%)** "
                     f"cao hơn {s['name']} ({s['probability']}%).")
    elif hypotheses:
        p = hypotheses[0]
        parts.append(f"Nghiêng về giả thuyết **{p['name']} ({p['probability']}%)**.")
    return " ".join(parts)
