"""smart_money_memory — MEMORY ENGINE (#6): tổng hợp diễn biến NHIỀU PHIÊN.

Chỉ ĐỌC dữ liệu đã persist (không sửa logic hiện có, không gọi API thị trường):
  • smart_money_events (mỗi phiên) → đếm hấp thụ / cụm tổ chức theo phiên → xu hướng.
  • intraday_tape (giữ ~5 phiên) → POC mỗi phiên → xu hướng POC.

Trả lời câu hỏi: "tín hiệu hôm nay là đơn lẻ hay là một QUÁ TRÌNH gom/xả kéo dài?".
Dùng cho: mục ② Market Context (Intraday) và là input `memory` cho Decision Engine (seam).
"""
from __future__ import annotations
from typing import Dict, List, Optional


def _trend(vals: List[Optional[float]]) -> str:
    """So trung bình nửa SAU vs nửa ĐẦU → up|down|flat (bỏ None)."""
    xs = [v for v in vals if v is not None]
    if len(xs) < 2:
        return "flat"
    h = len(xs) // 2
    first = sum(xs[:h]) / max(1, h)
    second = sum(xs[h:]) / max(1, len(xs) - h)
    if first == 0:
        return "up" if second > 0 else "flat"
    if second > first * 1.015:      # ngưỡng 1.5% — hợp cả chuỗi đếm lẫn chuỗi GIÁ (POC)
        return "up"
    if second < first * 0.985:
        return "down"
    return "flat"


def recent_summary(ticker: str, sessions: int = 5) -> Dict:
    """Tổng hợp `sessions` phiên gần nhất từ dữ liệu đã lưu."""
    from app.services import tape_store, order_flow
    tk = ticker.upper()
    # Hợp nhất phiên có event và/hoặc có tape (5 phiên gần nhất)
    dates = set(tape_store.list_event_dates(tk, sessions)) | set(tape_store.list_tape_dates(tk, sessions))
    dates = sorted(dates)[-sessions:]
    if not dates:
        return {"empty": True, "sessions": []}

    per: List[Dict] = []
    for d in dates:
        evs = tape_store.load_events(tk, d)
        buy_abs = sum(1 for e in evs if e.get("type") == "absorption" and (e.get("strength") or 0) > 0)
        sell_abs = sum(1 for e in evs if e.get("type") == "supply_absorption")
        clusters = sum(1 for e in evs if e.get("type") == "institution_cluster")
        poc = None
        try:
            t = tape_store.load(tk, d)
            if t and t.get("ticks"):
                poc = order_flow.volume_profile(t["ticks"]).get("poc")
        except Exception:  # noqa: BLE001
            pass
        per.append({"date": d, "buy_absorption": buy_abs, "sell_absorption": sell_abs,
                    "clusters": clusters, "poc": poc})

    poc_vals = [p["poc"] for p in per]
    return {
        "empty": False,
        "sessions": [p["date"] for p in per],
        "by_session": per,
        "absorption_total_buy": sum(p["buy_absorption"] for p in per),
        "absorption_total_sell": sum(p["sell_absorption"] for p in per),
        "cluster_total": sum(p["clusters"] for p in per),
        "absorption_trend": _trend([p["buy_absorption"] for p in per]),
        "poc_trend": _trend(poc_vals),
        "poc_values": poc_vals,
    }


def decision_bias(summary: Dict) -> Dict:
    """Chuyển summary → nhích giả thuyết cho Decision Engine (seam `memory` của decide()).
    Quá trình gom kéo dài (POC↑ + hấp thụ mua nhiều) → +accum_bias; và ngược lại."""
    if not summary or summary.get("empty"):
        return {}
    acc = 0.0
    if summary.get("poc_trend") == "up":
        acc += 6.0
    elif summary.get("poc_trend") == "down":
        acc -= 6.0
    net_abs = summary.get("absorption_total_buy", 0) - summary.get("absorption_total_sell", 0)
    acc += max(-8.0, min(8.0, net_abs * 1.2))
    return {"accum_bias": acc, "distrib_bias": -acc}
