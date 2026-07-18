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
import os
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
# Giới hạn bộ nhớ mỗi mã. Đây là số "lệnh" ĐÃ GỘP (150ms same-side), không phải khớp
# lẻ — mã thanh khoản cao nhất VN sau khi gộp ~30-50k/phiên, nên 100k là dư margin.
# Nếu 1 mã vẫn vượt (cực hiếm) → _cap_ticks cắt phần CŨ và LOG cảnh báo để ta biết.
MAX_TICKS = 100000
SAVE_INTERVAL = 45.0               # giây — ghi tape ra store bền vững thưa (giảm I/O)
# Cột Shark ở LIST chỉ cần điểm tổng phiên → không cần tươi từng giây. Rate limit DNSE
# "Get Trades" = 10.000 req/GIỜ: list poll 12s/mã ⇒ 300 req/giờ/mã ⇒ >34 mã là vượt trần.
# Giãn nhịp làm mới cho list (mặc định 45s ⇒ 80 req/giờ/mã) — màn CHI TIẾT vẫn tươi 3s.
SIGNAL_MAX_AGE = float(os.environ.get("SHARK_SIGNAL_MAX_AGE", "45"))

# Nguồn intraday theo thứ tự ưu tiên — fallback khi nguồn trước lỗi/throttle.
# Cả VCI và KBS đều trả [time, price, volume, match_type, id] (id VCI là số,
# KBS là chuỗi → chuẩn hoá về chuỗi; time VCI có tz, KBS naive → bỏ tz khi so sánh).
SOURCES = ["VCI", "KBS"]

from app.services import dnse_client   # nguồn tick DNSE (nếu có key), fallback vnstock
from app.services import data_source   # cấu hình chọn nguồn theo module (Admin)
from app.services import tape_store    # cache bền vững (SQLite) cho tape trong phiên

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


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _cap_ticks(c: dict, tk: str) -> None:
    """Giữ tape trong giới hạn bộ nhớ. Cắt phần CŨ nếu vượt — và CẢNH BÁO 1 lần để biết
    có mã nào chạm trần (khi đó điểm có thể thiếu đầu phiên → cân nhắc nâng MAX_TICKS)."""
    if len(c["ticks"]) > MAX_TICKS:
        c["ticks"] = c["ticks"][-MAX_TICKS:]
        c["seen"] = {t["id"] for t in c["ticks"]}
        if not c.get("_capped"):
            c["_capped"] = True
            print(f"⚠️  {tk}: tape chạm trần MAX_TICKS={MAX_TICKS} — cắt bớt phần cũ "
                  f"(điểm có thể thiếu đầu phiên). Cân nhắc nâng MAX_TICKS.", flush=True)


def _save_tape(tk: str, trade_date: str, ticks: list, complete: bool) -> None:
    """Ghi tape ra store bền vững (gọi NGOÀI _lock để không chặn cache)."""
    try:
        tape_store.save(tk, trade_date, ticks, complete=complete)
    except Exception as e:  # noqa: BLE001
        print(f"⚠️  tape_store.save {tk}: {type(e).__name__}: {e}", flush=True)


def _ensure_loaded(ticker: str) -> dict:
    """Lấy cache của mã; cold start nạp từ store bền vững (SQLite) — CỤC BỘ, KHÔNG gọi
    API. Dùng ở đường REQUEST (get_signal/get_tape) để đọc-cache-thuần, không đụng mạng."""
    tk = ticker.upper()
    today = _today()
    now = time.time()
    with _lock:
        c = _cache.get(tk)
        if c and c.get("date") != today:      # sang ngày mới → bỏ tape cũ
            c = None
        if c is None:
            c = {"ticks": [], "seen": set(), "src": "STORE", "last_fetch": 0.0,
                 "date": today, "last_saved": now, "complete": False, "deep": False,
                 "seeded": False}
            stored = tape_store.load(tk, today)
            if stored and stored["ticks"]:
                c["ticks"] = stored["ticks"]
                c["seen"] = {t["id"] for t in stored["ticks"]}
                c["complete"] = stored["complete"]
                c["deep"] = stored["complete"]
                c["seeded"] = True
            _cache[tk] = c
        return c


def _update(ticker: str, max_age: Optional[float] = None) -> dict:
    """Nạp/làm mới tape 1 mã — CÓ THỂ gọi API (seed/poll). Chỉ chạy ở BACKGROUND
    (refresh_loop) hoặc batch, KHÔNG gọi trực tiếp từ request handler.
    max_age: tuổi tối đa chấp nhận của cache (giây)."""
    tk = ticker.upper()
    now = time.time()
    today = _today()
    trading = _is_trading_hours()

    # ── Lấy/khởi tạo cache; cold start thì nạp từ store bền vững trước ──
    with _lock:
        c = _cache.get(tk)
        if c and c.get("date") != today:      # sang ngày mới → bỏ tape cũ
            c = None
        if c is None:
            c = {"ticks": [], "seen": set(), "src": "STORE", "last_fetch": 0.0,
                 "date": today, "last_saved": now, "complete": False, "deep": False,
                 "seeded": False}
            stored = tape_store.load(tk, today)
            if stored and stored["ticks"]:
                c["ticks"] = stored["ticks"]
                c["seen"] = {t["id"] for t in stored["ticks"]}
                c["complete"] = stored["complete"]
                c["deep"] = stored["complete"]   # phiên đã đóng = đã đủ cả phiên
                c["seeded"] = True               # store đã có tape phiên → khỏi seed lại
            _cache[tk] = c

    dnse_on = data_source.use_dnse("shark")     # REST DNSE (có breaker)
    fetch_interval = 3.0 if dnse_on else MIN_FETCH_INTERVAL
    if max_age is not None:
        fetch_interval = max(fetch_interval, max_age)

    # WS độc lập với REST: REST (openapi) có thể bị chặn IP trong khi WS (ws-openapi)
    # vẫn chạy. Nên KHÔNG dùng dnse_on ở đây — nếu không, breaker của REST sẽ khiến
    # feed không đăng ký mã nào và WS thành vô dụng.
    if data_source.get_source("shark") == "dnse" and dnse_client.configured():
        try:
            from app.services import dnse_feed   # import trong hàm: tránh vòng lặp
            dnse_feed.register_demand(tk)
            # WS đang đẩy tick cho mã này → khỏi poll REST (kể cả vnstock), vừa đúng
            # thiết kế DNSE vừa tránh ĐẾM TRÙNG (id vnstock ≠ id DNSE nên dedup không
            # bắt được cùng một lệnh khớp từ hai nguồn).
            # BẮT BUỘC seed xong trước: WS chỉ có tick TỪ LÚC KẾT NỐI, tin ngay WS thì
            # tape mất phần đầu phiên → điểm Shark (luỹ kế cả phiên) sai.
            if c.get("seeded") and dnse_feed.streaming(tk):
                return c
        except Exception:  # noqa: BLE001
            pass

    # ── Ngoài giờ giao dịch mà đã có tape → dùng cache, KHÔNG gọi API ──
    if not trading and c["ticks"]:
        if not c["complete"]:                 # finalize 1 lần: đánh dấu phiên đã đóng
            with _lock:
                c["complete"] = True
                snap = list(c["ticks"])
            _save_tape(tk, today, snap, complete=True)
        return c
    # ── Throttle (cả khi lỗi lẫn thành công) — tránh gọi dồn dập ──
    if now - c["last_fetch"] < fetch_interval:
        return c

    # seed = chưa từng nạp REST cho phiên này → nạp sâu; đã có → poll trang gần nhất + dedup
    # (không dựa vào c["ticks"] vì WS có thể đã đẩy tick vào trước khi REST kịp seed)
    seed = not c.get("seeded")

    # ── Fetch: DNSE (nếu bật) → fallback vnstock ──
    # Poll chỉ lấy 1 trang gần nhất (~500 khớp) rồi dedup theo id — không tải lại cả phiên.
    # (Không dùng from_ts theo giờ tick để tránh lệch múi giờ container ↔ giờ sàn.)
    if dnse_on:
        rows = dnse_client.get_intraday_ticks(
            tk, max_pages=(4 if seed else 1)) or []
        src = "DNSE"
        err = None
        if not rows and seed:   # DNSE rỗng lúc seed → thử vnstock (mã có tick ở vnstock)
            prefer = c.get("src") if c.get("src") in SOURCES else None
            rows, vsrc, verr = _fetch_with_fallback(tk, SEED_PAGE, prefer)
            if rows:
                src = vsrc
            else:
                err = verr or "Chưa có khớp lệnh"
    else:
        prefer = c.get("src") if c.get("src") in SOURCES else None
        rows, src, err = _fetch_with_fallback(tk, SEED_PAGE if seed else POLL_PAGE, prefer)

    # ── Merge dưới lock; quyết định persist rồi ghi NGOÀI lock ──
    snapshot = None
    complete_flag = False
    with _lock:
        c = _cache.get(tk) or c
        added = False
        if rows and not _units_consistent(c["ticks"], rows):
            print(f"⛔ {tk}: lệch ĐƠN VỊ giá giữa nguồn cũ ({c['ticks'][-1]['price']}) và "
                  f"{src} ({rows[0]['price']}) — bỏ lô này để không hỏng tape", flush=True)
            rows = []
        if rows:
            seen = c["seen"]
            new = [r for r in rows if r["id"] not in seen]
            if new:
                for r in new:
                    seen.add(r["id"])
                new.sort(key=lambda r: _parse_ts(r["ts"]) or datetime.min)
                c["ticks"].extend(new)
                _cap_ticks(c, tk)
                added = True
            if src != "STORE":
                c["src"] = src
            c["seeded"] = True     # đã nạp được từ REST → lần sau chỉ poll/để WS lo
            c.pop("err", None)
        elif err and not c["ticks"]:
            c["err"] = err
        c["last_fetch"] = now

        # Finalize nếu vừa nạp lần đầu NGOÀI giờ (cold sau khi phiên đóng)
        finalize = (not trading) and bool(c["ticks"]) and not c["complete"]
        need_save = c["ticks"] and (
            finalize or (added and now - c.get("last_saved", 0) >= SAVE_INTERVAL))
        if need_save:
            c["last_saved"] = now
            if finalize:
                c["complete"] = True
            snapshot = list(c["ticks"])
            complete_flag = c["complete"]
        result = c

    if snapshot is not None:
        _save_tape(tk, today, snapshot, complete=complete_flag)
    return result


# ── Đăng ký nhu cầu + worker làm mới NỀN (tách API khỏi đường request) ─────────────
# Mô hình: request handler CHỈ đọc cache; worker nền mới gọi API (seed/poll) + WS đẩy
# tick vào cache. Nhờ vậy bấm vào mã trong Shark Action luôn trả tức thì từ cache,
# không kích hoạt lệnh API nào → giảm tải rate & nhanh hơn nhiều.
_demand: Dict[str, float] = {}     # mã đang xem → thời điểm cuối được hỏi
_deep_want: set = set()            # mã cần nạp SÂU cả phiên (màn chi tiết)
DEMAND_TTL = 300.0                 # 5' không xem → thôi làm mới
REFRESH_INTERVAL = 3.0             # nhịp worker nền quét các mã đang xem


def _touch(ticker: str, deep: bool = False) -> None:
    tk = ticker.upper()
    _demand[tk] = time.time()
    if deep:
        _deep_want.add(tk)
    # WS (nếu dùng DNSE) — subscribe để nhận tick realtime đẩy thẳng vào cache
    if data_source.get_source("shark") == "dnse" and dnse_client.configured():
        try:
            from app.services import dnse_feed
            dnse_feed.register_demand(tk, book=deep)
        except Exception:  # noqa: BLE001
            pass


def _demanded() -> list:
    """Mã đang xem (còn trong TTL), SẮP THEO ƯU TIÊN để worker nền xử lý:
      1. Mã CHƯA seed (cần nạp lần đầu) — lên đầu để bấm vào là có ngay
      2. Mã đang xem CHI TIẾT (deep) trước mã chỉ trong list
      3. Mã được bấm GẦN ĐÂY NHẤT trước
    Nhờ vậy mã vừa bấm luôn ở đầu hàng đợi, seed trong ≤1 vòng (~3s)."""
    now = time.time()
    live = [(t, ts) for t, ts in _demand.items() if now - ts < DEMAND_TTL]

    def _key(item):
        t, ts = item
        seeded = (_cache.get(t) or {}).get("seeded", False)
        return (0 if not seeded else 1, 0 if t in _deep_want else 1, -ts)

    live.sort(key=_key)
    return [t for t, _ in live]


def _ensure_deep(tk: str) -> None:
    """Nạp SÂU CẢ PHIÊN 1 lần (REST) — CHẠY Ở WORKER NỀN, không ở request.
    Phân trang tới khi hết token (đủ từ 9:00) — KHÔNG giới hạn 3000 tick/25 trang như
    trước (mã thanh khoản cao sẽ mất phần sáng, sai điểm Shark)."""
    c = _cache.get(tk)
    if not c or c.get("deep") or not data_source.use_dnse("shark"):
        return
    # max_pages/max_ticks đủ lớn để lấy TRỌN phiên; dừng tự nhiên khi hết nextPageToken.
    deep = dnse_client.get_intraday_ticks(tk, max_pages=250, max_ticks=1_000_000) or []
    if deep:
        push_ticks(tk, deep, "DNSE")
    with _lock:
        c = _cache.get(tk, c)
        c["deep"] = True
        snap = list(c["ticks"])
    if snap:
        _save_tape(tk, _today(), snap, complete=not _is_trading_hours())


def _refresh_one(tk: str) -> None:
    """Làm mới 1 mã ở worker nền: poll/seed REST (nếu cần) + deep-seed nếu đang xem chi tiết.
    Mã đang xem CHI TIẾT → làm mới tươi (throttle 3s); mã chỉ nằm trong LIST → giãn
    (SIGNAL_MAX_AGE=45s) để không tốn rate. Mã đang có WS stream thì _update tự bỏ REST."""
    deep = tk in _deep_want
    _update(tk, max_age=None if deep else SIGNAL_MAX_AGE)
    if deep:
        try:
            _ensure_deep(tk)
        except Exception as e:  # noqa: BLE001
            print(f"⚠️  deep-seed {tk}: {type(e).__name__}: {e}", flush=True)


async def refresh_loop():
    """Worker nền: giữ cache của các mã ĐANG XEM luôn tươi. Đây là NƠI DUY NHẤT gọi API
    theo nhịp — request handler không bao giờ gọi API nữa."""
    import asyncio
    loop = asyncio.get_event_loop()
    while True:
        try:
            tks = _demanded()
            _deep_want.intersection_update(set(tks))   # dọn deep_want theo nhu cầu còn hiệu lực
            if tks and (_is_trading_hours() or any(
                    not (_cache.get(t) or {}).get("seeded") for t in tks)):
                for tk in tks:
                    await loop.run_in_executor(None, _refresh_one, tk)
                    await asyncio.sleep(0.2)   # rải đều, không dội nguồn
        except Exception as e:  # noqa: BLE001
            print(f"⚠️  shark refresh_loop: {type(e).__name__}: {e}", flush=True)
        await asyncio.sleep(REFRESH_INTERVAL if _is_trading_hours() else 30.0)


def _units_consistent(ticks: List[dict], rows: List[dict]) -> bool:
    """Chốt chặn TRỘN NHẦM ĐƠN VỊ giữa 2 nguồn trong cùng 1 tape.

    Tape của Shark có thể vừa nạp đầu phiên bằng vnstock vừa nhận realtime từ DNSE WS.
    Cả hai ĐANG cùng hệ (giá kVND, KL cổ phiếu), nhưng nếu một nguồn đổi đơn vị (hoặc
    ta lùi về nguồn khác) thì tape sẽ hỏng ÂM THẦM: sai giá 1000 lần, sai điểm Shark.
    Giá cổ phiếu VN không bao giờ nhảy 100 lần trong một phiên → lệch cỡ đó chắc chắn
    là sai đơn vị, KHÔNG phải biến động thật → bỏ lô đó và báo động thay vì ghi vào.
    """
    if not ticks or not rows:
        return True
    a = ticks[-1].get("price") or 0
    b = rows[0].get("price") or 0
    if a <= 0 or b <= 0:
        return True
    ratio = max(a, b) / min(a, b)
    return ratio < 100


def _append_agg(ticks: List[dict], r: dict) -> None:
    """Gộp khớp CÙNG CHIỀU cách nhau ≤ AGG_WINDOW_MS vào lệnh cuối (một lệnh quét sổ
    thường khớp thành chuỗi fill) — để nhận diện "lệnh lớn" cho đúng, giống REST."""
    last = ticks[-1] if ticks else None
    if last and last["side"] == r["side"]:
        a, b = _parse_ts(last["ts"]), _parse_ts(r["ts"])
        if a and b:
            dt_ms = (b - a).total_seconds() * 1000.0
            if 0 <= dt_ms <= dnse_client.AGG_WINDOW_MS:
                last["volume"] += r["volume"]
                last["value"] += r["value"]
                last["price"] = (last["value"] / (last["volume"] * 1000.0)
                                 if last["volume"] else r["price"])
                last["ts"] = r["ts"]
                return
    ticks.append(r)


def push_ticks(ticker: str, rows: List[dict], source: str = "DNSE",
               aggregate: bool = False) -> None:
    """dnse_feed đẩy tick realtime vào cache (dedup theo id, sắp theo thời gian).
    aggregate=True khi nguồn đẩy TỪNG khớp lẻ (WS) → gộp sweep như REST."""
    if not rows:
        return
    tk = ticker.upper()
    now = time.time()
    today = _today()
    snapshot = None
    complete_flag = False
    with _lock:
        c = _cache.get(tk)
        # Giữ ĐÚNG shape cache của _update (date/complete/deep/last_saved) — nếu thiếu,
        # _update sẽ tưởng là cache ngày cũ và vứt sạch tick vừa nhận từ WS.
        if c is None or c.get("date") != today:
            c = {"ticks": [], "seen": set(), "src": source, "last_fetch": now,
                 "date": today, "last_saved": now, "complete": False, "deep": False,
                 "seeded": False}   # WS đẩy tick KHÔNG tính là đã seed cả phiên
            _cache[tk] = c
        if not _units_consistent(c["ticks"], rows):
            print(f"⛔ {tk}: lệch ĐƠN VỊ giá giữa tape ({c['ticks'][-1]['price']}) và "
                  f"{source} ({rows[0]['price']}) — bỏ tick này", flush=True)
            return
        seen = c["seen"]
        new = [r for r in rows if r["id"] not in seen]
        if new:
            for r in new:
                seen.add(r["id"])
            new.sort(key=lambda r: _parse_ts(r["ts"]) or datetime.min)
            if aggregate:
                for r in new:
                    _append_agg(c["ticks"], r)
            else:
                c["ticks"].extend(new)
            _cap_ticks(c, tk)
            # WS đẩy tick → _update bị throttle nên sẽ không tự lưu; lưu ở đây (thưa).
            if now - c.get("last_saved", 0) >= SAVE_INTERVAL:
                c["last_saved"] = now
                snapshot = list(c["ticks"])
                complete_flag = c["complete"]
        c["src"] = source
        c["last_fetch"] = now

    if snapshot is not None:
        _save_tape(tk, today, snapshot, complete=complete_flag)


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


def _score_cacheable(big_value: float, window_min: int) -> bool:
    """Chỉ cache điểm khi dùng THAM SỐ MẶC ĐỊNH (tránh cache lẫn nhiều ngưỡng)."""
    return big_value == BIG_VALUE_VND and window_min == WINDOW_MIN


def compute_and_cache_signal(ticker: str, big_value: float = BIG_VALUE_VND,
                             window_min: int = WINDOW_MIN, force: bool = False) -> dict:
    """Tính điểm Shark rồi ghi cache. force=True bỏ qua throttle (dùng cho batch cuối phiên)."""
    tk = ticker.upper()
    c = _update(tk, max_age=(0.0 if force else SIGNAL_MAX_AGE))
    m = _metrics(tk, c.get("ticks", []), big_value, window_min)
    m.pop("big_orders", None)   # list view không cần chi tiết lệnh lớn
    if "err" in c and m.get("empty"):
        m["error"] = c["err"]
    if _score_cacheable(big_value, window_min) and not m.get("empty"):
        try:
            tape_store.save_score(tk, _today(), m, big_value,
                                  complete=not _is_trading_hours())
        except Exception as e:  # noqa: BLE001
            print(f"⚠️  save_score {tk}: {e}", flush=True)
    return m


def get_signal(ticker: str, big_value: float = BIG_VALUE_VND, window_min: int = WINDOW_MIN) -> dict:
    """Tín hiệu gọn (không kèm tape) — CHỈ ĐỌC CACHE, không gọi API.

    Đăng ký nhu cầu → worker nền (refresh_loop) + WS lo việc làm mới. Handler này chỉ:
      • NGOÀI PHIÊN: có cache điểm chốt (complete) → trả NGAY.
      • Còn lại: tính _metrics trên tape đang có trong cache (CPU cục bộ, không đụng mạng)."""
    tk = ticker.upper()
    _touch(tk)
    if not _is_trading_hours() and _score_cacheable(big_value, window_min):
        cached = tape_store.load_score(tk, _today())
        if cached and cached.get("complete") and cached.get("big_value") == big_value:
            return cached["signal"]
    c = _ensure_loaded(tk)          # nạp từ store nếu cần — KHÔNG gọi API
    m = _metrics(tk, c.get("ticks", []), big_value, window_min)
    m.pop("big_orders", None)
    if "err" in c and m.get("empty"):
        m["error"] = c["err"]
    if _score_cacheable(big_value, window_min) and not m.get("empty"):
        try:
            tape_store.save_score(tk, _today(), m, big_value,
                                  complete=not _is_trading_hours())
        except Exception:  # noqa: BLE001
            pass
    return m


def get_tape(ticker: str, limit: int = 2000, big_value: float = BIG_VALUE_VND,
             window_min: int = WINDOW_MIN) -> dict:
    """Tape + metrics + sổ lệnh cho màn chi tiết — CHỈ ĐỌC CACHE, không gọi API.

    Đăng ký nhu cầu (kèm deep + sổ lệnh); worker nền seed sâu + WS đẩy tick/sổ lệnh vào
    cache. Lần bấm đầu cache có thể còn mỏng → trả về phần đang có (frontend hiện 'đang
    tải'), vài giây sau worker nền nạp đủ → poll kế tiếp là đầy."""
    tk = ticker.upper()
    _touch(tk, deep=True)
    c = _ensure_loaded(tk)          # KHÔNG gọi API
    ticks = c.get("ticks", [])
    m = _metrics(tk, ticks, big_value, window_min)
    recent = list(reversed(ticks[-limit:]))
    m["tape"] = recent
    # Sổ lệnh: đọc từ cache WS (worker/WS cập nhật). Không gọi REST ở đây.
    ob = None
    if data_source.get_source("shark") == "dnse" and dnse_client.configured():
        try:
            from app.services import dnse_feed
            ob = dnse_feed.get_orderbook(tk)
        except Exception:  # noqa: BLE001
            ob = None
    m["orderbook"] = ob
    if "err" in c and m.get("empty"):
        m["error"] = c["err"]
    return m


def rebuild_session(ticker: str, date: Optional[str] = None) -> dict:
    """Dựng LẠI TRỌN tape 1 phiên từ DNSE (sửa tape cũ bị thiếu phần sáng do giới hạn
    fetch trước đây). date=None → tự lùi tìm phiên giao dịch gần nhất có khớp lệnh.
    Ghi đè cache + store để hiển thị/điểm đúng ngay. CHẠY Ở THREAD (có REST)."""
    tk = ticker.upper()
    target, rows, src, agg = None, [], None, False

    # 1) DNSE nếu REST dùng được (lấy được cả phiên CŨ theo date)
    if data_source.get_source("shark") == "dnse" and dnse_client.enabled():
        cands = [date] if date else [
            (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(8)]
        for d in cands:
            if not d:
                continue
            r = dnse_client.get_intraday_ticks(tk, max_pages=250, max_ticks=1_000_000, date=d) or []
            if r:
                target, rows, src, agg = d, r, "DNSE", False   # DNSE đã gộp sẵn
                break

    # 2) vnstock (khi DNSE bị chặn IP). LƯU Ý: vnstock intraday CHỈ có phiên HÔM NAY —
    #    không lấy được ngày cũ. Chạy trong/ngay sau phiên giao dịch mới có dữ liệu.
    if not rows and (date is None or date == _today()):
        vrows, vsrc, _err = _fetch_with_fallback(tk, 100000)   # page_size lớn = trọn phiên
        if vrows:
            target, rows, src, agg = _today(), vrows, vsrc, True   # vnstock lẻ → gộp 150ms

    if not rows:
        return {"ok": False, "ticker": tk,
                "message": "không lấy được khớp lệnh (DNSE bị chặn + vnstock không có "
                           "dữ liệu phiên này — chạy trong/sau phiên giao dịch)"}

    # Reset cache mã này rồi nạp trọn phiên
    with _lock:
        _cache.pop(tk, None)
    _ensure_loaded(tk)
    push_ticks(tk, rows, src, aggregate=agg)
    with _lock:
        c = _cache[tk]
        c["deep"] = True
        c["seeded"] = True
        snap = list(c["ticks"])
    _save_tape(tk, _today(), snap, complete=True)
    m = _metrics(tk, snap, BIG_VALUE_VND, WINDOW_MIN)
    if not m.get("empty"):
        m2 = {k: v for k, v in m.items() if k != "big_orders"}
        tape_store.save_score(tk, _today(), m2, BIG_VALUE_VND, complete=True)
    return {"ok": True, "ticker": tk, "session_date": target, "ticks": len(snap),
            "first_ts": snap[0]["ts"] if snap else None,
            "last_ts": snap[-1]["ts"] if snap else None, "score": m.get("score")}


async def rebuild_watchlist(tickers: Optional[List[str]] = None) -> dict:
    """Dựng lại trọn tape phiên cho TẤT CẢ mã trong watchlist (hoặc danh sách cho trước).
    Dùng nguồn hiện có: DNSE nếu REST dùng được, không thì vnstock (phiên hôm nay)."""
    import asyncio
    from app.services import user_store
    tks = tickers or user_store.all_watchlist_tickers()
    loop = asyncio.get_event_loop()
    done, failed, results = 0, [], []
    for tk in tks:
        try:
            r = await loop.run_in_executor(None, rebuild_session, tk)
            if r.get("ok"):
                done += 1
            else:
                failed.append(tk)
            results.append({"ticker": tk, "ok": r.get("ok"), "n": r.get("ticks"),
                            "first_ts": r.get("first_ts"), "score": r.get("score")})
            print(f"🔧 rebuild {tk}: ok={r.get('ok')} n={r.get('ticks')} "
                  f"first={r.get('first_ts')} score={r.get('score')}", flush=True)
        except Exception as e:  # noqa: BLE001
            failed.append(tk)
            print(f"❌ rebuild {tk}: {type(e).__name__}: {e}", flush=True)
        await asyncio.sleep(1.5)   # nhẹ tay với vnstock (tránh throttle)
    msg = f"rebuild_watchlist: {done}/{len(tks)} ok, {len(failed)} lỗi"
    print(f"✅ {msg}", flush=True)
    return {"total": len(tks), "done": done, "failed": failed, "results": results, "message": msg}
