"""shark_history — đánh giá dấu hiệu cá mập theo KỲ (tuần/tháng/khoảng tùy chọn)
từ dữ liệu giao dịch NGÀY của FireAnt (historical-quotes).

Khác với intraday (tick trong phiên), phần này dùng dòng tiền theo ngày — vốn là
tín hiệu cá mập mạnh & ổn định cho khung thời gian dài:
  • Khối ngoại ròng (buyForeign − sellForeign)
  • Tự doanh ròng (propTradingNetValue)
  • Mua/Bán chủ động (buyQuantity vs sellQuantity)
  • Thoả thuận / block trade (putthroughValue)
"""
from __future__ import annotations
import os
import time
import requests
from datetime import datetime
from typing import List, Optional

_CACHE: dict = {}          # (ticker, start, end) -> (ts, data)
_TTL = 900.0               # 15 phút (dữ liệu ngày đổi chậm)


def _fireant_history(ticker: str, start: str, end: str) -> Optional[list]:
    tok = os.environ.get("FIREANT_TOKEN", "").strip()
    if not tok:
        return None
    headers = {"Authorization": f"Bearer {tok}", "User-Agent": "Mozilla/5.0",
               "Accept": "application/json"}
    url = f"https://restv2.fireant.vn/symbols/{ticker.upper()}/historical-quotes"
    params = {"startDate": start, "endDate": end, "offset": 0, "limit": 500}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=20)
        if r.status_code == 200:
            return r.json()
    except Exception:  # noqa: BLE001
        return None
    return None


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _num(d: dict, k: str) -> float:
    v = d.get(k)
    return float(v) if isinstance(v, (int, float)) else 0.0


def _compute(ticker: str, start: str, end: str, raw: list) -> dict:
    days: List[dict] = []
    for d in raw or []:
        close = _num(d, "priceClose")
        basic = _num(d, "priceBasic") or close
        change = close - basic
        f_buy = _num(d, "buyForeignValue")
        f_sell = _num(d, "sellForeignValue")
        days.append({
            "date": str(d.get("date", ""))[:10],
            "close": close,
            "change_pct": round(change / basic * 100, 2) if basic else 0.0,
            "total_vol": _num(d, "totalVolume"),
            "deal_vol": _num(d, "dealVolume"),
            "active_buy": _num(d, "buyQuantity"),
            "active_sell": _num(d, "sellQuantity"),
            "foreign_buy": f_buy,
            "foreign_sell": f_sell,
            "foreign_net": f_buy - f_sell,
            "prop_net": _num(d, "propTradingNetValue"),
            "putthrough_val": _num(d, "putthroughValue"),
        })
    days.sort(key=lambda x: x["date"])

    if not days:
        return {"ticker": ticker.upper(), "start": start, "end": end, "empty": True,
                "days": [], "score": 0, "label": "Chưa có dữ liệu",
                "updated_at": datetime.now().isoformat()}

    tot_fnet = sum(x["foreign_net"] for x in days)
    tot_prop = sum(x["prop_net"] for x in days)
    tot_buy = sum(x["active_buy"] for x in days)
    tot_sell = sum(x["active_sell"] for x in days)
    tot_pt = sum(x["putthrough_val"] for x in days)

    abs_fnet = sum(abs(x["foreign_net"]) for x in days) or 1.0
    abs_prop = sum(abs(x["prop_net"]) for x in days) or 1.0
    active_imb = (tot_buy - tot_sell) / ((tot_buy + tot_sell) or 1)   # -1..1
    foreign_dir = _clamp(tot_fnet / abs_fnet, -1, 1)                  # -1..1
    prop_dir = _clamp(tot_prop / abs_prop, -1, 1)                     # -1..1

    # Shark Score kỳ: ưu tiên khối ngoại + tự doanh (dòng tiền tổ chức) + chủ động
    score = 100.0 * (0.40 * foreign_dir + 0.25 * prop_dir + 0.35 * active_imb)
    score = round(_clamp(score, -100, 100))
    label = "Gom hàng" if score >= 25 else ("Xả hàng" if score <= -25 else "Trung tính")

    return {
        "ticker": ticker.upper(),
        "start": start,
        "end": end,
        "empty": False,
        "n_days": len(days),
        "score": score,
        "label": label,
        "foreign_net": tot_fnet,        # VND (dương = ngoại mua ròng)
        "prop_net": tot_prop,           # VND (dương = tự doanh mua ròng)
        "active_buy": tot_buy,
        "active_sell": tot_sell,
        "active_imbalance": round(active_imb, 3),
        "putthrough_val": tot_pt,
        "days": days,
        "updated_at": datetime.now().isoformat(),
    }


def get_history(ticker: str, start: str, end: str) -> dict:
    key = (ticker.upper(), start, end)
    now = time.time()
    hit = _CACHE.get(key)
    if hit and now - hit[0] < _TTL:
        return hit[1]
    raw = _fireant_history(ticker, start, end)
    data = _compute(ticker, start, end, raw or [])
    if not data.get("empty"):
        _CACHE[key] = (now, data)
    return data


def get_history_signal(ticker: str, start: str, end: str) -> dict:
    """Bản gọn (không kèm days) cho danh sách nhiều mã."""
    d = get_history(ticker, start, end)
    return {k: v for k, v in d.items() if k != "days"}
