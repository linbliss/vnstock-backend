"""shark_monitor — phát hiện dấu hiệu giao dịch "cá mập" từ luồng khớp lệnh
(intraday tick) của vnstock.

Ý tưởng: mỗi lệnh khớp có bên chủ động (Buy/Sell) + khối lượng. Từ đó tính:
  • Mua/Bán chủ động luỹ kế (như FireAnt)
  • Lệnh lớn (giá trị ≥ ngưỡng) → dấu chân cá mập
  • Mất cân bằng chủ động (toàn phiên + cửa sổ trượt gần đây)
  • Shark Score [-100..100]: dương = GOM (accumulation), âm = XẢ (distribution)

Cache tick theo từng mã (dữ liệu công khai, dùng chung mọi user), poll tăng dần
theo `id` để không tải lại. Chỉ gọi API trong giờ giao dịch (ngoài giờ dùng cache).
"""
from __future__ import annotations
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# ── Tham số mặc định (có thể override qua query) ──
BIG_VALUE_VND = 1_000_000_000      # ngưỡng "lệnh lớn": ≥ 1 tỷ đồng/lệnh
WINDOW_MIN = 15                    # cửa sổ trượt (phút) cho tín hiệu "gần đây"
MIN_FETCH_INTERVAL = 18.0          # giây — không refetch 1 mã dày hơn mức này
SEED_PAGE = 1000                   # lần đầu lấy nhiều tick để có bối cảnh
POLL_PAGE = 300                    # các lần sau chỉ lấy tick mới
MAX_TICKS = 20000                  # giới hạn bộ nhớ mỗi mã

_cache: Dict[str, dict] = {}       # ticker -> {ticks, last_id, last_fetch}
_lock = threading.Lock()
_fetch_sema = threading.Semaphore(2)   # tối đa 2 lệnh gọi API song song


def _is_trading_hours() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    return (9 * 60 <= t <= 11 * 60 + 30) or (13 * 60 <= t <= 15 * 60 + 2)


def _fetch_intraday(ticker: str, page_size: int) -> List[dict]:
    from vnstock import Quote
    df = Quote(symbol=ticker.upper(), source="VCI").intraday(page_size=page_size)
    rows: List[dict] = []
    for _, r in df.iterrows():
        mt = str(r.get("match_type", "")).lower()
        side = "B" if mt.startswith("b") else ("S" if mt.startswith("s") else "U")
        try:
            price = float(r["price"])
            vol = int(r["volume"])
            tid = int(r["id"])
        except (TypeError, ValueError):
            continue
        rows.append({
            "id": tid,
            "ts": str(r["time"]),
            "price": price,
            "volume": vol,
            "side": side,
            "value": vol * price * 1000.0,   # VND
        })
    return rows


def _update(ticker: str) -> dict:
    tk = ticker.upper()
    now = time.time()
    with _lock:
        c = _cache.get(tk)
    # Còn tươi → khỏi gọi lại
    if c and now - c["last_fetch"] < MIN_FETCH_INTERVAL:
        return c
    # Ngoài giờ giao dịch mà đã có cache → không gọi API nữa
    if c and not _is_trading_hours():
        return c

    seed = c is None
    try:
        with _fetch_sema:
            rows = _fetch_intraday(tk, SEED_PAGE if seed else POLL_PAGE)
    except Exception as e:  # noqa: BLE001
        if c:
            c["last_fetch"] = now
            return c
        return {"ticks": [], "last_id": 0, "last_fetch": now, "err": str(e)}

    with _lock:
        c = _cache.get(tk)
        if c is None:
            c = {"ticks": [], "last_id": 0, "last_fetch": now}
            _cache[tk] = c
        last_id = c["last_id"]
        new = [r for r in rows if r["id"] > last_id]
        if new:
            new.sort(key=lambda r: r["id"])
            c["ticks"].extend(new)
            c["last_id"] = c["ticks"][-1]["id"]
            if len(c["ticks"]) > MAX_TICKS:
                c["ticks"] = c["ticks"][-MAX_TICKS:]
        c["last_fetch"] = now
        return c


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _parse_ts(ts: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def _metrics(ticker: str, ticks: List[dict], big_value: float, window_min: int) -> dict:
    tk = ticker.upper()
    if not ticks:
        return {
            "ticker": tk, "empty": True, "score": 0, "label": "Chưa có dữ liệu",
            "total_buy": 0, "total_sell": 0, "total_vol": 0,
            "big_count": 0, "big_buy_val": 0, "big_sell_val": 0,
            "last_price": None, "last_ts": None, "updated_at": datetime.now().isoformat(),
        }

    total_buy = sum(t["volume"] for t in ticks if t["side"] == "B")
    total_sell = sum(t["volume"] for t in ticks if t["side"] == "S")
    total_vol = total_buy + total_sell
    total_value = sum(t["value"] for t in ticks)

    big = [t for t in ticks if t["value"] >= big_value]
    big_buy_val = sum(t["value"] for t in big if t["side"] == "B")
    big_sell_val = sum(t["value"] for t in big if t["side"] == "S")
    big_net = big_buy_val - big_sell_val

    # Cửa sổ trượt gần đây theo thời gian tick mới nhất
    last_dt = None
    for t in reversed(ticks):
        last_dt = _parse_ts(t["ts"])
        if last_dt:
            break
    w_buy = w_sell = 0
    if last_dt:
        start = last_dt - timedelta(minutes=window_min)
        for t in ticks:
            dt = _parse_ts(t["ts"])
            if dt and dt >= start:
                if t["side"] == "B":
                    w_buy += t["volume"]
                elif t["side"] == "S":
                    w_sell += t["volume"]

    imbalance = (total_buy - total_sell) / total_vol if total_vol else 0.0          # -1..1
    big_dir = big_net / (big_buy_val + big_sell_val) if (big_buy_val + big_sell_val) else 0.0  # -1..1
    w_total = w_buy + w_sell
    w_imbalance = (w_buy - w_sell) / w_total if w_total else 0.0

    # Shark Score: ưu tiên tiền LỚN (cá mập), có tham chiếu mất cân bằng toàn phiên
    # + động lượng cửa sổ gần đây.
    score = 100.0 * (0.55 * big_dir + 0.30 * imbalance + 0.15 * w_imbalance)
    score = round(_clamp(score, -100, 100))

    if score >= 25:
        label = "Gom hàng"
    elif score <= -25:
        label = "Xả hàng"
    else:
        label = "Trung tính"

    last_price = ticks[-1]["price"]
    big_orders = [
        {"ts": t["ts"], "side": t["side"], "volume": t["volume"],
         "price": t["price"], "value": t["value"]}
        for t in big[-30:]
    ]
    big_orders.reverse()

    return {
        "ticker": tk,
        "empty": False,
        "score": score,
        "label": label,
        "total_buy": total_buy,
        "total_sell": total_sell,
        "total_vol": total_vol,
        "total_value": total_value,
        "imbalance": round(imbalance, 3),
        "active_ratio": round(total_buy / total_vol, 3) if total_vol else 0,
        "window_min": window_min,
        "w_buy": w_buy,
        "w_sell": w_sell,
        "w_imbalance": round(w_imbalance, 3),
        "big_value_threshold": big_value,
        "big_count": len(big),
        "big_buy_val": big_buy_val,
        "big_sell_val": big_sell_val,
        "big_net": big_net,
        "big_orders": big_orders,
        "last_price": last_price,
        "last_ts": ticks[-1]["ts"],
        "n_ticks": len(ticks),
        "updated_at": datetime.now().isoformat(),
    }


def get_signal(ticker: str, big_value: float = BIG_VALUE_VND, window_min: int = WINDOW_MIN) -> dict:
    """Tín hiệu gọn (không kèm tape) — cho danh sách nhiều mã."""
    c = _update(ticker)
    m = _metrics(ticker, c.get("ticks", []), big_value, window_min)
    m.pop("big_orders", None)   # list view không cần chi tiết lệnh lớn
    if "err" in c and m.get("empty"):
        m["error"] = c["err"]
    return m


def get_tape(ticker: str, limit: int = 60, big_value: float = BIG_VALUE_VND,
             window_min: int = WINDOW_MIN) -> dict:
    """Tape (khớp lệnh gần nhất) + đầy đủ metrics — cho màn chi tiết 1 mã."""
    c = _update(ticker)
    ticks = c.get("ticks", [])
    m = _metrics(ticker, ticks, big_value, window_min)
    recent = ticks[-limit:]
    recent = list(reversed(recent))
    m["tape"] = recent
    if "err" in c and m.get("empty"):
        m["error"] = c["err"]
    return m
