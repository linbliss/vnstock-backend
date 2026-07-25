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


def build_story(series: List[dict], events: List[dict], cx: dict, evidence: List[dict],
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

    narrative = _smart_money_story(cx, evidence, decision, hypotheses)
    chain = _causal_chain(series, events, evidence, hypotheses)
    return {"beats": beats, "narrative": narrative, "chain": chain}


def _causal_chain(series: List[dict], events: List[dict], evidence: List[dict],
                  hypotheses: List[dict]) -> List[dict]:
    """I6 — Chuỗi diễn biến NHÂN-QUẢ (theo thời gian thật, mỗi bước neo vào dữ liệu):
    'bán chủ động mạnh → giá không giảm → hấp thụ xuất hiện → POC giữ → ngoại mua ròng
    → xác suất Markup tăng'. Wording 'sau đó/đi kèm' — trình tự & nhất quán, KHÔNG khẳng
    định nhân quả tuyệt đối. Mỗi bước: {text, tone}."""
    steps: List[dict] = []
    ev_by_kind = {e.get("kind"): e for e in (evidence or [])}

    # 1) Áp lực chủ động đầu phiên (dữ liệu segment thật)
    if series:
        dcvd0, dp0 = _seg(series, 0.0, 0.35)
        if dcvd0 < 0:
            if dp0 >= -0.2:
                steps.append({"text": "Bên bán chủ động chiếm ưu thế đầu phiên", "tone": "bear"})
                steps.append({"text": (f"nhưng giá vẫn TĂNG ({dp0:+.2f}%) — lực bán bị nuốt trọn"
                                       if dp0 > 0.3 else
                                       f"nhưng giá KHÔNG giảm tương ứng ({dp0:+.2f}%)"), "tone": "bull"})
            else:
                steps.append({"text": f"Bên bán chủ động ép giá đầu phiên ({dp0:+.2f}%)", "tone": "bear"})
        elif dcvd0 > 0:
            if dp0 <= 0.2:
                steps.append({"text": "Bên mua chủ động áp đảo đầu phiên", "tone": "bull"})
                steps.append({"text": f"nhưng giá KHÔNG bứt lên tương ứng ({dp0:+.2f}%)", "tone": "bear"})
            else:
                steps.append({"text": f"Bên mua chủ động dẫn dắt, giá nhích lên ({dp0:+.2f}%)", "tone": "bull"})

    # 2) Hành vi hấp thụ / cung (event thật, có giờ)
    abs_evs = [e for e in events if e.get("type") == "absorption" and (e.get("strength") or 0) > 0]
    sup_evs = [e for e in events if e.get("type") == "supply_absorption"]
    if abs_evs:
        steps.append({"text": f"sau đó xuất hiện hấp thụ lực bán ({len(abs_evs)} lần, "
                              f"từ {_hhmm(abs_evs[0].get('ts', ''))})", "tone": "bull"})
    if sup_evs:
        steps.append({"text": f"đi kèm cung chủ động chặn giá ({len(sup_evs)} lần, "
                              f"từ {_hhmm(sup_evs[0].get('ts', ''))})", "tone": "bear"})

    # 3) Chấp nhận giá (POC / Value Area — evidence đã diễn giải)
    poc_e = ev_by_kind.get("poc_position") or ev_by_kind.get("value_area")
    if poc_e:
        d = poc_e.get("direction") or 0.0
        steps.append({"text": ("vùng giá trị vẫn được giữ (giá trên POC)" if d >= 0
                               else "giá bị đẩy xuống dưới vùng giá trị"),
                      "tone": "bull" if d >= 0 else "bear"})

    # 4) Chủ thể lớn (cụm tổ chức, khối ngoại — claim đã context-conditioned)
    cl_e = ev_by_kind.get("cluster")
    if cl_e and abs(cl_e.get("direction") or 0) > 0.05:
        steps.append({"text": cl_e.get("claim", ""),
                      "tone": "bull" if (cl_e.get("direction") or 0) > 0 else "bear"})
    ff = ev_by_kind.get("foreign_flow")
    if ff:
        steps.append({"text": ff.get("claim", ""),
                      "tone": "bull" if (ff.get("direction") or 0) >= 0 else "bear"})

    # 5) Cuối phiên
    if series:
        dcvd1, _ = _seg(series, 0.75, 1.0)
        if abs(dcvd1) > 1:
            steps.append({"text": ("cuối phiên cầu chủ động mạnh lên" if dcvd1 > 0
                                   else "cuối phiên cung chủ động gia tăng"),
                          "tone": "bull" if dcvd1 > 0 else "bear"})

    # 6) Kết luận xác suất
    if hypotheses:
        p = hypotheses[0]
        tone = "bull" if p["name"] in ("Tích luỹ", "Markup", "Rũ hàng (Shakeout)") else \
               "bear" if p["name"] in ("Phân phối", "Markdown", "Cao trào mua") else "neutral"
        steps.append({"text": f"⇒ nghiêng về {p['name']} ({p['probability']}%)", "tone": tone})
    return steps[:8]


def _smart_money_story(cx: dict, evidence: List[dict], decision: dict, hypotheses: List[dict]) -> str:
    """Đoạn văn tổng hợp — KỂ LẠI CHÍNH CÁC EVIDENCE (đã diễn giải theo context), không
    diễn giải lại metric thô. Chọn bằng chứng thuận/nghịch mạnh nhất + kết luận giả thuyết."""
    parts: List[str] = []
    ev = [e for e in (evidence or []) if abs(e.get("direction") or 0) * (e.get("reliability") or 0) > 0.05]
    ev.sort(key=lambda e: abs(e.get("direction") or 0) * (e.get("reliability") or 0), reverse=True)
    bull = [e for e in ev if (e.get("direction") or 0) > 0]
    bear = [e for e in ev if (e.get("direction") or 0) < 0]

    if bull:
        parts.append("Bằng chứng thuận: " + "; ".join(e["claim"] for e in bull[:2]) + ".")
    if bear:
        parts.append("Bằng chứng nghịch: " + "; ".join(e["claim"] for e in bear[:2]) + ".")
    if not bull and not bear:
        parts.append("Chưa có bằng chứng dòng tiền đủ rõ trong phiên.")

    # Foreign gate (nếu có evidence foreign_flow đã diễn giải)
    ff = next((e for e in ev if e.get("kind") == "foreign_flow"), None)
    if ff:
        parts.append(ff["claim"] + ".")

    # Kết luận theo giả thuyết (từ evidence)
    if len(hypotheses) >= 2:
        p, s = hypotheses[0], hypotheses[1]
        parts.append(f"Tổng hợp lại, khả năng cao nhất là **{p['name']} ({p['probability']}%)**, "
                     f"trên {s['name']} ({s['probability']}%).")
    elif hypotheses:
        p = hypotheses[0]
        parts.append(f"Nghiêng về giả thuyết **{p['name']} ({p['probability']}%)**.")
    return " ".join(parts)
