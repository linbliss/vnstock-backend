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


def _period_behavior(days: list) -> dict:
    """Tín hiệu Wyckoff/VSA/OBV/CMF từ nến NGÀY (dòng tiền theo kỳ).
    Trả thành phần [-1..1] (dương = gom) + cờ mô tả.
      - cmf:      Chaikin Money Flow (đóng cửa trong biên × KL) → dòng tiền vào/ra
      - clv_bias: vị trí đóng cửa trong biên (gần High = gom / nến rút đầu = xả)
      - obv_div:  OBV phân kỳ so với giá (OBV lên mà giá đi ngang = gom)
    """
    flags: list = []
    n = len(days)
    if n < 5:
        return {"cmf": 0.0, "clv_bias": 0.0, "obv_div": 0.0, "flags": flags}

    closes = [d["close"] for d in days]
    vols = [d["total_vol"] for d in days]
    volsum = sum(vols) or 1.0

    # CLV & CMF
    mfv = 0.0
    clvs = []
    for d in days:
        rng = d["high"] - d["low"]
        clv = (((d["close"] - d["low"]) - (d["high"] - d["close"])) / rng) if rng > 0 else 0.0
        clvs.append(clv)
        mfv += clv * d["total_vol"]
    cmf = _clamp(mfv / volsum, -1, 1)
    clv_bias = _clamp(sum(clvs) / n, -1, 1)

    # OBV vs giá — phân kỳ
    obv = 0.0
    for i in range(1, n):
        if closes[i] > closes[i - 1]:
            obv += vols[i]
        elif closes[i] < closes[i - 1]:
            obv -= vols[i]
    price_trend = (closes[-1] - closes[0]) / (closes[0] or 1)   # tỉ lệ
    obv_norm = _clamp(obv / volsum, -1, 1)
    price_norm = _clamp(price_trend * 10, -1, 1)
    obv_div = _clamp(obv_norm - price_norm, -1, 1)

    # Xu hướng khối lượng (nửa sau vs nửa đầu)
    half = n // 2
    v1 = sum(vols[:half]) / max(1, half)
    v2 = sum(vols[half:]) / max(1, n - half)
    vol_trend = (v2 - v1) / (v1 or 1)

    # Cờ mô tả
    if cmf > 0.05:
        flags.append("Dòng tiền vào (CMF+)")
    elif cmf < -0.05:
        flags.append("Dòng tiền ra (CMF−)")
    if clv_bias > 0.2:
        flags.append("Đóng cửa gần đỉnh (gom)")
    elif clv_bias < -0.2:
        flags.append("Nến rút đầu (xả)")
    if obv_div > 0.2:
        flags.append("OBV phân kỳ dương (gom)")
    elif obv_div < -0.2:
        flags.append("OBV phân kỳ âm (xả)")
    if vol_trend > 0.25 and abs(price_trend) < 0.03:
        flags.append("KL tăng, giá đi ngang (hấp thụ)")
    elif price_trend > 0.03 and vol_trend < -0.25:
        flags.append("Giá tăng, KL giảm (phân phối)")

    # Khối ngoại gom nhiều phiên liên tiếp (cuối kỳ)
    streak = 0
    for d in reversed(days):
        if d["foreign_net"] > 0:
            streak += 1
        else:
            break
    if streak >= 5:
        flags.append(f"Khối ngoại gom {streak} phiên")

    return {"cmf": round(cmf, 3), "clv_bias": round(clv_bias, 3),
            "obv_div": round(obv_div, 3), "flags": flags}


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
            "open": _num(d, "priceOpen") or close,
            "high": _num(d, "priceHigh") or close,
            "low": _num(d, "priceLow") or close,
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

    beh = _period_behavior(days)   # VSA / OBV / CMF

    # Shark Score kỳ: dòng tiền tổ chức (ngoại/tự doanh/chủ động) + hành vi Wyckoff/VSA
    score = 100.0 * (
        0.22 * foreign_dir +      # khối ngoại
        0.10 * prop_dir +         # tự doanh
        0.13 * active_imb +       # chủ động mua/bán
        0.20 * beh["cmf"] +       # CMF (dòng tiền)
        0.15 * beh["clv_bias"] +  # vị trí đóng cửa (VSA)
        0.20 * beh["obv_div"]     # OBV phân kỳ
    )
    score = round(_clamp(score, -100, 100))
    label = "Gom hàng" if score >= 25 else ("Xả hàng" if score <= -25 else "Trung tính")

    return {
        "cmf": beh["cmf"],
        "clv_bias": beh["clv_bias"],
        "obv_div": beh["obv_div"],
        "patterns": beh["flags"],
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
