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
MIN_FETCH_INTERVAL = 8.0           # giây — không refetch 1 mã dày hơn mức này (có fallback VCI↔KBS)
SEED_PAGE = 1000                   # lần đầu lấy nhiều tick để có bối cảnh
POLL_PAGE = 300                    # các lần sau chỉ lấy tick mới
MAX_TICKS = 20000                  # giới hạn bộ nhớ mỗi mã

# Nguồn intraday theo thứ tự ưu tiên — fallback khi nguồn trước lỗi/throttle.
# Cả VCI và KBS đều trả [time, price, volume, match_type, id] (id VCI là số,
# KBS là chuỗi → chuẩn hoá về chuỗi; time VCI có tz, KBS naive → bỏ tz khi so sánh).
SOURCES = ["VCI", "KBS"]

from app.services import dnse_client   # nguồn tick DNSE (nếu có key), fallback vnstock

_cache: Dict[str, dict] = {}       # ticker -> {ticks, seen(set id), src, last_fetch, err?}
_lock = threading.Lock()
_fetch_sema = threading.Semaphore(3)   # tối đa 3 lệnh gọi API song song


def _is_trading_hours() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    return (9 * 60 <= t <= 11 * 60 + 30) or (13 * 60 <= t <= 15 * 60 + 2)


def _fetch_intraday_src(ticker: str, page_size: int, source: str) -> List[dict]:
    from vnstock import Quote
    df = Quote(symbol=ticker.upper(), source=source.lower()).intraday(page_size=page_size)
    rows: List[dict] = []
    for _, r in df.iterrows():
        mt = str(r.get("match_type", "")).lower()
        side = "B" if mt.startswith("b") else ("S" if mt.startswith("s") else "U")
        try:
            price = float(r["price"])
            vol = int(r["volume"])
        except (TypeError, ValueError):
            continue
        rows.append({
            "id": str(r.get("id", "")),      # chuẩn hoá về chuỗi (VCI:int, KBS:str)
            "ts": str(r["time"]),
            "price": price,
            "volume": vol,
            "side": side,
            "value": vol * price * 1000.0,   # VND
        })
    return rows


def _fetch_with_fallback(ticker: str, page_size: int, prefer: Optional[str] = None):
    """Thử lần lượt các nguồn (ưu tiên nguồn đang chạy tốt) → (rows, source, err)."""
    order = [prefer] if prefer in SOURCES else []
    order += [s for s in SOURCES if s not in order]
    last_err = None
    empty_src = order[0] if order else "VCI"
    for src in order:
        try:
            with _fetch_sema:
                rows = _fetch_intraday_src(ticker, page_size, src)
            if rows:
                return rows, src, None
            empty_src = src            # nguồn OK nhưng rỗng → thử nguồn kế
        except Exception as e:         # noqa: BLE001 — lỗi/throttle → fallback
            last_err = str(e)
            continue
    return [], empty_src, last_err


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
    # Nguồn tick: DNSE REST (nếu có key) → fallback vnstock (VCI/KBS)
    if dnse_client.enabled():
        rows = dnse_client.get_intraday_ticks(tk, max_pages=(8 if seed else 1)) or []
        src = "DNSE"
        err = None if rows else "DNSE: chưa có khớp lệnh"
    else:
        prefer = c.get("src") if c else None
        rows, src, err = _fetch_with_fallback(tk, SEED_PAGE if seed else POLL_PAGE, prefer)

    with _lock:
        c = _cache.get(tk)
        if c is None:
            c = {"ticks": [], "seen": set(), "src": src, "last_fetch": now}
            _cache[tk] = c
        if rows:
            seen = c["seen"]
            new = [r for r in rows if r["id"] not in seen]
            if new:
                for r in new:
                    seen.add(r["id"])
                new.sort(key=lambda r: _parse_ts(r["ts"]) or datetime.min)
                c["ticks"].extend(new)
                if len(c["ticks"]) > MAX_TICKS:
                    c["ticks"] = c["ticks"][-MAX_TICKS:]
                    c["seen"] = {t["id"] for t in c["ticks"]}
            c["src"] = src
            c.pop("err", None)
        elif err and not c["ticks"]:
            c["err"] = err
        c["last_fetch"] = now
        return c


def push_ticks(ticker: str, rows: List[dict], source: str = "DNSE") -> None:
    """dnse_feed đẩy tick realtime vào cache (dedup theo id, sắp theo thời gian)."""
    if not rows:
        return
    tk = ticker.upper()
    now = time.time()
    with _lock:
        c = _cache.get(tk)
        if c is None:
            c = {"ticks": [], "seen": set(), "src": source, "last_fetch": now}
            _cache[tk] = c
        seen = c["seen"]
        new = [r for r in rows if r["id"] not in seen]
        if new:
            for r in new:
                seen.add(r["id"])
            new.sort(key=lambda r: _parse_ts(r["ts"]) or datetime.min)
            c["ticks"].extend(new)
            if len(c["ticks"]) > MAX_TICKS:
                c["ticks"] = c["ticks"][-MAX_TICKS:]
                c["seen"] = {t["id"] for t in c["ticks"]}
        c["src"] = source
        c["last_fetch"] = now


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _parse_ts(ts: str) -> Optional[datetime]:
    try:
        # bỏ tzinfo → naive, để so sánh nhất quán giữa VCI (có tz) và KBS (naive)
        return datetime.fromisoformat(ts).replace(tzinfo=None)
    except (ValueError, TypeError):
        return None


def _behavior(ticks: List[dict], big_value: float) -> dict:
    """Phát hiện HÀNH VI thao túng của cá mập từ chuỗi khớp lệnh (order-flow / tape reading).
    Trả các thành phần [-1..1] (dương = gom, âm = xả) + cờ mô tả để hiển thị.

    - absorption: bán chủ động nhiều mà giá KHÔNG giảm → gom (có tay to đỡ); ngược lại = xả
    - manip:      mánh sau lệnh lớn (rũ hàng gom / kéo giá xả) + động lượng nối tiếp
    - reversal:   spring (phá đáy rồi hồi = gom) / upthrust (vượt đỉnh rồi rớt = xả)
    """
    n = len(ticks)
    flags: List[str] = []
    if n < 8:
        return {"absorption": 0.0, "manip": 0.0, "reversal": 0.0, "flags": flags}

    prices = [t["price"] for t in ticks]
    first_p, last_p = prices[0], prices[-1]
    hi, lo = max(prices), min(prices)
    rng = (hi - lo) or 1e-9
    total_buy = sum(t["volume"] for t in ticks if t["side"] == "B")
    total_sell = sum(t["volume"] for t in ticks if t["side"] == "S")
    total = (total_buy + total_sell) or 1
    net = (total_buy - total_sell) / total                       # -1..1
    price_chg = (last_p - first_p) / (first_p or 1)              # tỉ lệ

    # 1) HẤP THỤ — nghịch pha giữa dòng lệnh chủ động và giá
    absorption = 0.0
    if net < -0.08 and price_chg >= 0:      # bán trội nhưng giá giữ/tăng → GOM
        absorption = min(1.0, (-net) + max(0.0, price_chg) * 8)
        flags.append("Hấp thụ lực bán (gom)")
    elif net > 0.08 and price_chg <= 0:     # mua trội nhưng giá giữ/giảm → XẢ
        absorption = -min(1.0, net + max(0.0, -price_chg) * 8)
        flags.append("Hấp thụ lực mua (xả)")

    # 2) MÁNH quanh LỆNH LỚN — rũ hàng gom / kéo giá xả + động lượng nối tiếp
    big_idx = [i for i, t in enumerate(ticks) if t["value"] >= big_value]
    manip_raw = 0.0
    shakeout = uptrap = 0
    for i in big_idx:
        b = ticks[i]
        wnd = ticks[i + 1:i + 9]            # 8 khớp ngay sau
        if not wnd:
            continue
        follow_same = sum(1 for w in wnd if w["side"] == b["side"])
        if b["side"] == "B":
            # sau MUA lớn có BÁN nhỏ giá THẤP hơn → đạp giá để gom tiếp (rũ hàng)
            if any(w["side"] == "S" and w["price"] < b["price"] and w["volume"] < b["volume"] for w in wnd):
                manip_raw += 1.0; shakeout += 1
            if follow_same >= len(wnd) * 0.6:      # nối tiếp mua → động lượng gom
                manip_raw += 0.4
        elif b["side"] == "S":
            # sau BÁN lớn có MUA nhỏ giá CAO hơn → kéo giá để xả tiếp
            if any(w["side"] == "B" and w["price"] > b["price"] and w["volume"] < b["volume"] for w in wnd):
                manip_raw -= 1.0; uptrap += 1
            if follow_same >= len(wnd) * 0.6:
                manip_raw -= 0.4
    manip = _clamp(manip_raw / max(3.0, len(big_idx)), -1, 1)
    if shakeout:
        flags.append(f"Rũ hàng gom ×{shakeout}")
    if uptrap:
        flags.append(f"Kéo giá xả ×{uptrap}")

    # 3) SPRING / UPTHRUST — đảo chiều ở biên
    reversal = 0.0
    tail = max(1, n // 3)
    low_idx = min(range(n), key=lambda i: prices[i])
    high_idx = max(range(n), key=lambda i: prices[i])
    pos = (last_p - lo) / rng                                    # 0=đáy, 1=đỉnh
    if low_idx >= n - tail and pos >= 0.55:                      # đáy mới gần đây rồi hồi lên
        reversal = min(1.0, pos)
        flags.append("Spring (phá đáy rồi hồi → gom)")
    elif high_idx >= n - tail and pos <= 0.45:                   # đỉnh mới gần đây rồi rớt
        reversal = -min(1.0, 1 - pos)
        flags.append("Upthrust (vượt đỉnh rồi rớt → xả)")

    return {"absorption": round(absorption, 3), "manip": round(manip, 3),
            "reversal": round(reversal, 3), "flags": flags}


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

    # Hành vi thao túng (tape reading): hấp thụ / rũ hàng-kéo giá / spring-upthrust
    beh = _behavior(ticks, big_value)

    # Shark Score: kết hợp tiền LỚN + mất cân bằng chủ động + HÀNH VI cá mập.
    score = 100.0 * (
        0.28 * big_dir +          # hướng tiền lớn
        0.18 * imbalance +        # mất cân bằng toàn phiên
        0.10 * w_imbalance +      # động lượng cửa sổ gần đây
        0.22 * beh["absorption"] +  # hấp thụ
        0.16 * beh["manip"] +       # mánh quanh lệnh lớn (rũ/kéo)
        0.06 * beh["reversal"]      # spring / upthrust
    )
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
        # Thành phần hành vi (tape reading) — để minh bạch & hiển thị
        "absorption": beh["absorption"],
        "manip": beh["manip"],
        "reversal": beh["reversal"],
        "patterns": beh["flags"],
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
