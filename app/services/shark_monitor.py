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
SEED_PAGE = 1000                   # (cũ) — không còn dùng cho seed, giữ để tương thích
POLL_PAGE = 300                    # các lần sau chỉ lấy tick mới
# SEED phải lấy TRỌN PHIÊN (từ 9:00), nếu không mã thanh khoản cao chỉ có phần chiều
# (~13:00 trở lại) → tổng hợp/điểm sai. Chỉ chạy 1 lần/mã/phiên (cờ seeded) + lưu store.
FULL_SEED_PAGE = 40000             # vnstock: đủ phủ cả phiên mã thanh khoản cao
DNSE_SEED_PAGES = 250              # DNSE REST: phân trang tới khi hết token (≈ từ 9:00)
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


def _fetch_paged_src(ticker: str, source: str,
                     kbs_page_size: int = 5000, max_pages: int = 20) -> List[dict]:
    """Lấy TRỌN phiên 1 mã từ 1 nguồn vnstock (khi DNSE REST bị chặn IP).
      • KBS: phân trang theo `page` (1,2,3…) tới trang cuối (đáng tin nhất).
      • VCI: 1 lần với last_time = 09:00 → lấy từ ĐẦU PHIÊN (không phải 'gần nhất')."""
    from vnstock import Quote
    tk = ticker.upper()
    q = Quote(symbol=tk, source=source.lower())
    seen: set = set()
    out: List[dict] = []

    def _add(df) -> int:
        n = 0
        for _, r in df.iterrows():
            idv = str(r.get("id", ""))
            if idv and idv in seen:
                continue
            if idv:
                seen.add(idv)
            mt = str(r.get("match_type", "")).lower()
            side = "B" if mt.startswith("b") else ("S" if mt.startswith("s") else "U")
            try:
                price = float(r["price"]); vol = int(r["volume"])
            except (TypeError, ValueError, KeyError):
                continue
            out.append({"id": idv or f'{r.get("time")}_{price}_{vol}', "ts": str(r["time"]),
                        "price": price, "volume": vol, "side": side,
                        "value": vol * price * 1000.0})
            n += 1
        return n

    if source.upper() == "KBS":
        for p in range(1, max_pages + 1):
            with _fetch_sema:
                df = q.intraday(page=p, page_size=kbs_page_size)
            if df is None or df.empty:
                break
            _add(df)
            if len(df) < kbs_page_size:   # trang cuối → hết phiên
                break
    else:  # VCI — 1 lần, bắt đầu từ 09:00 (last_time)
        open_ts = int(datetime.now().replace(hour=9, minute=0, second=0,
                                             microsecond=0).timestamp())
        with _fetch_sema:
            df = q.intraday(page_size=30000, last_time=open_ts)
        if df is not None and not df.empty:
            _add(df)

    out.sort(key=lambda x: _parse_ts(x["ts"]) or datetime.min)
    return out


def _fetch_full_session(ticker: str, prefer: Optional[str] = None):
    """Lấy TRỌN phiên qua vnstock (DNSE REST chặn IP) → (rows, source, err).
    Ưu tiên KBS (phân trang chuẩn) → VCI (best-effort từ 09:00)."""
    order = ["KBS", "VCI"] if prefer != "VCI" else ["VCI", "KBS"]
    last_err = None
    for src in order:
        try:
            rows = _fetch_paged_src(ticker, src)
            if rows:
                return rows, src, None
        except Exception as e:  # noqa: BLE001
            last_err = str(e)
            continue
    return [], order[0], last_err


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


_sess_memo: Dict[str, tuple] = {}      # tk -> (hết hạn, ngày phiên, ngày hôm nay)


def _session_date(tk: str) -> str:
    """Ngày PHIÊN dùng để hiển thị & tính điểm:
      • Trong giờ giao dịch → hôm nay (phiên đang chạy)
      • Ngoài giờ → hôm nay nếu đã có tape; nếu chưa (ngày nghỉ/cuối tuần/trước giờ mở)
        → PHIÊN GẦN NHẤT có dữ liệu, để vẫn xem/rà soát được phiên cuối.
    Mỗi phiên vẫn nằm ở khoá ngày RIÊNG → KHÔNG trộn tick giữa các phiên."""
    today = _today()
    if _is_trading_hours():
        return today
    now = time.time()
    hit = _sess_memo.get(tk)
    if hit and now < hit[0] and hit[2] == today:
        return hit[1]
    d = today
    try:
        d = tape_store.last_session_date(tk, today) or today
    except Exception:  # noqa: BLE001
        pass
    _sess_memo[tk] = (now + 300.0, d, today)   # nhớ 5' cho nhẹ DB
    return d


def _session_only(rows: list, date: str, tk: str = "") -> list:
    """CHỈ giữ tick thuộc ĐÚNG phiên `date`.

    Điểm Shark trong phiên là luỹ kế CỦA PHIÊN ĐÓ — nếu tick phiên trước lẫn vào (tape
    cũ còn trong store/cache, hoặc rebuild ghi nhầm ngày) thì mua/bán chủ động và lệnh
    lớn bị cộng dồn hai phiên ⇒ điểm SAI. Chặn ngay tại cửa vào.
    Định dạng ts thống nhất 'YYYY-MM-DD HH:MM:SS...' (cả REST lẫn WS)."""
    if not rows:
        return rows
    out = [r for r in rows
           if isinstance(r.get("ts"), str) and r["ts"][:10] == date]
    if tk and len(out) != len(rows):
        print(f"🧹 {tk}: bỏ {len(rows) - len(out)} tick KHÔNG thuộc phiên {date}", flush=True)
    return out


def _covers_open(ticks: list) -> bool:
    """Tape có tick từ ~đầu phiên (≤ 09:16) không → nếu có, đã bắt trọn phiên, khỏi REST
    seed. Quét toàn bộ có early-exit; chỉ được gọi khi CHƯA seeded (vài lần đầu phiên)
    hoặc lúc nạp store (1 lần) nên chi phí không đáng kể."""
    for t in ticks:
        ts = t.get("ts", "")
        if isinstance(ts, str) and len(ts) >= 16 and ts[11:16] <= "09:16":
            return True
    return False


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
    today = _session_date(tk)   # ngày nghỉ → phiên gần nhất; trong phiên → hôm nay
    now = time.time()
    with _lock:
        c = _cache.get(tk)
        if c and c.get("date") != today:      # sang phiên khác → bỏ tape cũ
            c = None
        if c is None:
            c = {"ticks": [], "seen": set(), "src": "STORE", "last_fetch": 0.0,
                 "date": today, "last_saved": now, "complete": False, "deep": False,
                 "seeded": False}
            stored = tape_store.load(tk, today)
            if stored and stored["ticks"]:
                # Lọc theo phiên: blob cũ có thể lẫn tick phiên trước (vd rebuild ghi
                # nhầm ngày) → không lọc thì điểm trong phiên cộng dồn 2 phiên ⇒ SAI.
                stored["ticks"] = _session_only(stored["ticks"], today, tk)
            if stored and stored["ticks"]:
                c["ticks"] = stored["ticks"]
                c["seen"] = {t["id"] for t in stored["ticks"]}
                c["_dirty"] = True        # tape cũ có thể lặp/loạn thứ tự → dọn khi đọc
                c["complete"] = stored["complete"]
                # CHỈ coi đã seed đủ khi tape đã phủ đầu phiên (hoặc phiên đã đóng). Nếu
                # store lưu DỞ (restart giữa phiên) → seeded=False ⇒ _update REST full-seed
                # backfill phần sáng → Shark Point mới chính xác.
                _full = bool(stored["complete"]) or _covers_open(stored["ticks"])
                c["deep"] = _full
                c["seeded"] = _full
            _cache[tk] = c
        return c


def _update(ticker: str, max_age: Optional[float] = None) -> dict:
    """Nạp/làm mới tape 1 mã — CÓ THỂ gọi API (seed/poll). Chỉ chạy ở BACKGROUND
    (refresh_loop) hoặc batch, KHÔNG gọi trực tiếp từ request handler.
    max_age: tuổi tối đa chấp nhận của cache (giây)."""
    tk = ticker.upper()
    now = time.time()
    # Dùng CÙNG khoá phiên với _ensure_loaded, nếu không sẽ reset sạch tape phiên gần
    # nhất mà request vừa nạp (ngày nghỉ).
    today = _session_date(tk)
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
                # Lọc theo phiên: blob cũ có thể lẫn tick phiên trước (vd rebuild ghi
                # nhầm ngày) → không lọc thì điểm trong phiên cộng dồn 2 phiên ⇒ SAI.
                stored["ticks"] = _session_only(stored["ticks"], today, tk)
            if stored and stored["ticks"]:
                c["ticks"] = stored["ticks"]
                c["seen"] = {t["id"] for t in stored["ticks"]}
                c["_dirty"] = True        # tape cũ có thể lặp/loạn thứ tự → dọn khi đọc
                c["complete"] = stored["complete"]
                # Chỉ SEEDED nếu tape phủ đầu phiên (hoặc phiên đã đóng). Restart giữa
                # phiên mà store lưu dở → seeded=False ⇒ REST full-seed backfill sáng.
                _full = bool(stored["complete"]) or _covers_open(stored["ticks"])
                c["deep"] = _full
                c["seeded"] = _full
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
            # WS subscribe TOÀN watchlist từ ĐẦU PHIÊN → nếu tape đã phủ từ ~9:00 thì WS
            # đã có trọn phiên, KHỎI REST seed nặng. Coi như đã seed.
            if not c.get("seeded") and dnse_feed.streaming(tk) and _covers_open(c["ticks"]):
                c["seeded"] = True
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
    # SEED lần đầu = TRỌN PHIÊN (từ 9:00). Các lần sau chỉ lấy 1 trang gần nhất + dedup.
    # (Không dùng from_ts theo giờ tick để tránh lệch múi giờ container ↔ giờ sàn.)
    if dnse_on:
        rows = dnse_client.get_intraday_ticks(
            tk, max_pages=(DNSE_SEED_PAGES if seed else 1),
            max_ticks=(1_000_000 if seed else 3000)) or []
        src = "DNSE"
        err = None
        if not rows and seed:   # DNSE rỗng lúc seed → vnstock TRỌN PHIÊN (phân trang)
            prefer = c.get("src") if c.get("src") in SOURCES else None
            rows, vsrc, verr = _fetch_full_session(tk, prefer)
            if rows:
                src = vsrc
            else:
                err = verr or "Chưa có khớp lệnh"
    else:
        prefer = c.get("src") if c.get("src") in SOURCES else None
        if seed:
            rows, src, err = _fetch_full_session(tk, prefer)   # vnstock TRỌN PHIÊN
        else:
            rows, src, err = _fetch_with_fallback(tk, POLL_PAGE, prefer)  # poll 1 trang

    # ── Merge dưới lock; quyết định persist rồi ghi NGOÀI lock ──
    snapshot = None
    complete_flag = False
    with _lock:
        c = _cache.get(tk) or c
        added = False
        # Chỉ nhận tick thuộc ĐÚNG phiên của cache (chặn lẫn phiên trước)
        rows = _session_only(rows, c.get("date") or today, tk)
        if rows and not _units_consistent(c["ticks"], rows):
            print(f"⛔ {tk}: lệch ĐƠN VỊ giá giữa nguồn cũ ({c['ticks'][-1]['price']}) và "
                  f"{src} ({rows[0]['price']}) — bỏ lô này để không hỏng tape", flush=True)
            rows = []
        if rows:
            seen = c["seen"]
            new = [r for r in rows if r["id"] not in seen]
            if new:
                is_dnse = (src == "DNSE")
                for r in new:
                    seen.add(r["id"])
                    if is_dnse:
                        r["_dnse"] = 1
                new.sort(key=lambda r: _parse_ts(r["ts"]) or datetime.min)
                c["ticks"].extend(new)
                c["_dirty"] = True
                _cap_ticks(c, tk)
                added = True
            if src != "STORE":
                c["src"] = src
            if seed:
                c["deep"] = True   # seed đã lấy TRỌN phiên → _ensure_deep khỏi tải lại
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

# Điểm Shark tính NGẦM ở worker khi tape đổi → request chỉ LOAD, không tính lại.
_score_cache: Dict[str, dict] = {}   # tk -> {signal, sig:(n,last_ts), date, at}
SCORE_RECOMPUTE_MIN = 20.0           # giây — không tính lại điểm dày hơn mức này/mã


def _recompute_score(tk: str) -> None:
    """Tính điểm Shark cho 1 mã Ở NỀN — chỉ khi tape ĐỔI (giao dịch mới) và đã qua
    SCORE_RECOMPUTE_MIN. Lưu vào RAM (_score_cache) + store (shark_score) để request
    chỉ việc load. 'khi tính lại nếu có thay đổi mới cập nhật'."""
    c = _cache.get(tk)
    if not c or not c.get("ticks"):
        return
    now = time.time()
    prev = _score_cache.get(tk)
    date = c.get("date") or _today()
    if prev and prev.get("date") == date and now - prev.get("at", 0) < SCORE_RECOMPUTE_MIN:
        return
    _clean_tape(c)
    ticks = c["ticks"]
    if not ticks:
        return
    sig = (len(ticks), ticks[-1]["ts"])
    if prev and prev.get("date") == date and prev.get("sig") == sig:
        return                       # tape KHÔNG đổi → khỏi tính lại
    m = _metrics(tk, ticks, BIG_VALUE_VND, WINDOW_MIN)
    m.pop("big_orders", None)
    _score_cache[tk] = {"signal": m, "sig": sig, "date": date, "at": now}
    if not m.get("empty"):
        try:
            tape_store.save_score(tk, date, m, BIG_VALUE_VND,
                                  complete=not _is_trading_hours())
        except Exception:  # noqa: BLE001
            pass


_of_cache: Dict[str, dict] = {}   # tk -> {data, sig:(n,last_ts), date}


def get_orderflow(ticker: str) -> dict:
    """Order Flow Analyzer cho 1 mã — CHỈ ĐỌC tape đã cache (không gọi API), tính O(n)
    + cache theo chữ ký tape (n, last_ts) → tape không đổi thì trả lại kết quả cũ."""
    from app.services import order_flow
    tk = ticker.upper()
    _touch(tk, deep=True)          # cần tape đầy đủ → coi như màn chi tiết
    c = _ensure_loaded(tk)
    _clean_tape(c)
    ticks = c.get("ticks", [])
    if not ticks:
        return {"ticker": tk, "empty": True}
    sig = (len(ticks), ticks[-1]["ts"])
    date = c.get("date") or _today()
    prev = _of_cache.get(tk)
    if prev and prev["date"] == date and prev["sig"] == sig:
        return prev["data"]
    data = order_flow.analyze(ticks)
    data["ticker"] = tk
    _of_cache[tk] = {"data": data, "sig": sig, "date": date}
    return data


def get_context(ticker: str, with_foreign: bool = True) -> dict:
    """LAYER 0 — Context Engine cho 1 mã (đọc tape cache + OHLCV ngày + dòng tiền ngày).
    Tái dùng order flow đã cache (get_orderflow) để khỏi tính lại VWAP/Volume Profile."""
    from app.services import market_context
    tk = ticker.upper()
    of = get_orderflow(tk)               # đã cache theo chữ ký tape
    c = _ensure_loaded(tk)
    ticks = c.get("ticks", [])
    ctx = market_context.build_context(tk, ticks, of=of, with_foreign=with_foreign)
    d = ctx.to_dict()
    d["ticker"] = tk
    d["empty"] = not bool(ticks)
    d["date"] = c.get("date") or _today()
    return d


def tape_health(ticks: List[dict]) -> dict:
    """PHASE 0 — đo ĐỘ TIN của trường `side` (nền móng của CVD/imbalance/absorption).

    Trả:
      • side_dist B/S/U + u_pct: %U cao ⇒ nhiều khớp không rõ chiều ⇒ CVD kém tin.
      • tickrule_agree_pct: %khớp mà `side` cùng hướng với tick-rule (uptick→B, downtick→S).
        `side` LÀ aggressor thật thì tương quan mạnh (80–96% thực đo) chứ KHÔNG 100%
        (aggressor ≠ tick-rule hoàn toàn). ~50% ⇒ nghi `side` là nhãn rỗng/ngẫu nhiên.
      • source_mix: tỉ lệ tick từ DNSE (WS) vs vnstock (REST) — biết nguồn đang cấp side.
      • auction: số khớp phiên định kỳ (ATO/ATC = side U, đúng khi bị loại khỏi CVD).
    """
    n = len(ticks)
    if not n:
        return {"empty": True}
    b = sum(1 for t in ticks if t["side"] == "B")
    s = sum(1 for t in ticks if t["side"] == "S")
    u = n - b - s
    dnse = sum(1 for t in ticks if t.get("_dnse"))
    # Tick-rule: bỏ U và giá bằng (zero-tick không kết luận)
    agree = disagree = 0
    prev = None
    for t in ticks:
        p, side = t["price"], t["side"]
        if prev is not None and side in ("B", "S"):
            if p > prev:
                agree += (side == "B"); disagree += (side == "S")
            elif p < prev:
                agree += (side == "S"); disagree += (side == "B")
        prev = p
    tr_tot = agree + disagree
    return {
        "empty": False,
        "n_ticks": n,
        "side_dist": {"B": b, "S": s, "U": u},
        "u_pct": round(u / n * 100, 2),
        "tickrule_agree_pct": round(agree / tr_tot * 100, 1) if tr_tot else None,
        "tickrule_n": tr_tot,
        "source_mix": {"dnse": dnse, "vnstock": n - dnse,
                       "dnse_pct": round(dnse / n * 100, 1)},
        "time_span": [ticks[0]["ts"][11:19], ticks[-1]["ts"][11:19]],
    }


def get_tape_health(ticker: str, cross_check: bool = False) -> dict:
    """Sức khoẻ tape 1 mã (đọc cache). cross_check=True: kéo KBS full-session và đối
    chiếu `side` DNSE-vs-KBS trên các khớp trùng (giây, giá, KL) — CHỈ chạy trên server
    (nơi có tape DNSE) để xác nhận side của DNSE khớp với vendor độc lập."""
    tk = ticker.upper()
    c = _ensure_loaded(tk)
    _clean_tape(c)
    ticks = c.get("ticks", [])
    h = tape_health(ticks)
    h["ticker"] = tk
    h["date"] = c.get("date") or _today()
    if cross_check and ticks:
        try:
            rows, src, _ = _fetch_full_session(tk)
            ref = {}
            for r in rows:
                ref.setdefault((r["ts"][11:19], round(r["price"], 2), r["volume"]), r["side"])
            same = diff = 0
            for t in ticks:
                k = (t["ts"][11:19], round(t["price"], 2), t["volume"])
                rs = ref.get(k)
                if rs and rs in ("B", "S") and t["side"] in ("B", "S"):
                    same += (rs == t["side"]); diff += (rs != t["side"])
            tot = same + diff
            h["cross_check"] = {"ref_source": src, "matched": tot,
                                "side_agree_pct": round(same / tot * 100, 1) if tot else None,
                                "disagree": diff}
        except Exception as e:  # noqa: BLE001
            h["cross_check"] = {"error": str(e)}
    return h


def _cached_score(tk: str) -> Optional[dict]:
    """Điểm đã tính ngầm còn hợp lệ (đúng phiên) trong RAM."""
    sc = _score_cache.get(tk)
    if sc and sc.get("date") == (_cache.get(tk, {}).get("date") or _session_date(tk)):
        return sc["signal"]
    return None
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
        _save_tape(tk, _session_date(tk), snap, complete=not _is_trading_hours())


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
    # Tính NGẦM điểm Shark khi tape đổi → Watchlist/Dashboard chỉ load cache
    try:
        _recompute_score(tk)
    except Exception as e:  # noqa: BLE001
        print(f"⚠️  score {tk}: {type(e).__name__}: {e}", flush=True)


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


def _clean_tape(c: dict) -> None:
    """Chuẩn hoá tape: sắp đúng thời gian + khử TRÙNG bất kể nguồn/id.

    Vì sao dedup theo id không đủ: cùng MỘT khớp lệnh nhưng id KHÁC nhau khi
      • vnstock (id riêng) trộn DNSE WS (id = totalVolumeTraded), hoặc
      • fallback VCI↔KBS (id kiểu khác nhau), hoặc
      • deep-seed REST vs WS gộp lệch mốc.
    ⇒ log lặp 2 lần. Nên khử trùng theo NỘI DUNG, không theo id.

    3 bước (chỉ chạy khi _dirty — ở đường ĐỌC, ~1 lần/đọc):
      1. Sắp theo thời gian đã parse (bỏ tz — nhất quán VCI có tz / KBS / DNSE có ms).
      2. DNSE ưu tiên theo TỪNG GIÂY (không theo cả dải min–max để tránh xoá nhầm khi
         DNSE thưa): giây nào ĐÃ có tick DNSE thì bỏ tick nguồn khác trong giây đó.
      3. Khử trùng theo chữ ký NỘI DUNG (ts, side, volume, price) — hai bản ghi giống
         hệt gần như chắc chắn là một lệnh; xác suất hai lệnh KHÁC nhau trùng cả 4 (kèm
         mili-giây) là không đáng kể.
    """
    if not c.get("_dirty"):
        return
    keyed = [((_parse_ts(t["ts"]) or datetime.min), t) for t in c["ticks"]]
    keyed.sort(key=lambda kt: kt[0])   # chỉ so theo thời gian (tránh so dict khi ts trùng)

    dnse_secs = {k.replace(microsecond=0) for k, t in keyed if t.get("_dnse")}
    out: List[dict] = []
    sig_seen: set = set()
    for k, t in keyed:
        sec = k.replace(microsecond=0)
        # [2] giây có DNSE → bỏ tick nguồn khác trong giây đó
        if dnse_secs and not t.get("_dnse") and sec in dnse_secs:
            continue
        # [3] khử trùng theo GIÂY (bỏ mili-giây): cùng lệnh nhưng hai nguồn/hai lần gộp
        # ghi mili-giây LỆCH nhau (11:00:41.100 vs .638) nên chữ ký có ms không bắt được.
        # Rủi ro: 2 lệnh KHÁC nhau trùng cả (giây, chiều, KL, giá) — hiếm, và 150ms-agg
        # đã gộp các fill sát nhau nên phần lớn đã là 1; chấp nhận đổi lấy hết lặp.
        sig = (sec, t["side"], t["volume"], round(float(t["price"]), 4))
        if sig in sig_seen:
            continue
        sig_seen.add(sig)
        out.append(t)
    c["ticks"] = out
    c["seen"] = {t["id"] for t in out}
    c["_dirty"] = False


def _append_agg(ticks: List[dict], r: dict) -> None:
    """Gộp khớp CÙNG CHIỀU cách nhau ≤ AGG_WINDOW_MS vào lệnh cuối (một lệnh quét sổ
    thường khớp thành chuỗi fill) — để nhận diện "lệnh lớn" cho đúng, giống REST.
    Chỉ gộp khi CÙNG NGUỒN (không trộn tick DNSE vào tick vnstock)."""
    last = ticks[-1] if ticks else None
    if last and last["side"] == r["side"] and last.get("_dnse") == r.get("_dnse"):
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
        # Chỉ nhận tick thuộc ĐÚNG phiên của cache (chặn lẫn phiên trước)
        rows = _session_only(rows, c.get("date") or today, tk)
        if not rows:
            return
        if not _units_consistent(c["ticks"], rows):
            print(f"⛔ {tk}: lệch ĐƠN VỊ giá giữa tape ({c['ticks'][-1]['price']}) và "
                  f"{source} ({rows[0]['price']}) — bỏ tick này", flush=True)
            return
        seen = c["seen"]
        new = [r for r in rows if r["id"] not in seen]
        if new:
            is_dnse = (source == "DNSE")
            for r in new:
                seen.add(r["id"])
                if is_dnse:
                    r["_dnse"] = 1
            new.sort(key=lambda r: _parse_ts(r["ts"]) or datetime.min)
            if aggregate:
                for r in new:
                    _append_agg(c["ticks"], r)
            else:
                c["ticks"].extend(new)
            c["_dirty"] = True
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


# ── Ngưỡng "lệnh lớn" THÍCH ỨNG theo từng mã ─────────────────────────────────────
# Ngưỡng tuyệt đối 1 tỷ đổi nghĩa hoàn toàn theo thanh khoản: với smallcap 300k cp/phiên
# thì 1 lệnh 1 tỷ ≈ 42% KL phiên (không bao giờ có); với bluechip 25M cp thì ≈ 0.14%
# (xảy ra liên tục). Nên "lớn" phải định nghĩa TƯƠNG ĐỐI so với chính mã đó.
BIG_PCTILE = 97.0
BIG_MIN_VND = 200_000_000
BIG_MAX_VND = 5_000_000_000
SCORE_VERSION = 2          # đổi công thức ⇒ cache điểm cũ không còn so sánh được


def _adaptive_big_value(ticks: List[dict]) -> float:
    """Ngưỡng lệnh lớn = percentile giá trị lệnh CỦA CHÍNH MÃ ĐÓ trong phiên (kẹp biên)."""
    if len(ticks) < 200:
        return BIG_VALUE_VND
    vals = sorted(t["value"] for t in ticks)
    k = min(int(len(vals) * BIG_PCTILE / 100.0), len(vals) - 1)
    return float(max(BIG_MIN_VND, min(BIG_MAX_VND, vals[k])))


_exch_memo: Dict[str, str] = {}


def _exchange_of(tk: str) -> str:
    """Sàn của mã (nhớ trong RAM). Mặc định HOSE nếu chưa có trong stock_list."""
    tk = tk.upper()
    if tk in _exch_memo:
        return _exch_memo[tk]
    ex = "HOSE"
    try:
        from app.services import ohlcv_store
        ex = (ohlcv_store.get_exchange(tk) or "HOSE").upper()
    except Exception:  # noqa: BLE001
        pass
    _exch_memo[tk] = ex
    return ex


def _tick_pct(price_k: float, exchange: str = "HOSE") -> float:
    """1 bước giá quy ra % giá (giá tính bằng kVND).

    HOSE chia bậc theo giá (10đ/50đ/100đ), còn HNX & UPCOM dùng 100đ ĐỒNG NHẤT mọi mức
    giá. Dùng công thức HOSE cho mã HNX giá thấp sẽ cho ngưỡng nhỏ hơn thực tế tới 10×
    (mã 8k: 0.125% so với 1.25% thật)."""
    if price_k <= 0:
        return 0.25
    if exchange in ("HNX", "UPCOM"):
        return 0.10 / price_k * 100.0
    step = 0.01 if price_k < 10 else (0.05 if price_k < 50 else 0.10)
    return step / price_k * 100.0


def _divergence(ticks: List[dict], big_value: float) -> dict:
    """PHÂN KỲ lớn/nhỏ — tín hiệu độc lập với 'hướng dòng tiền tổng'.
    >0: lệnh LỚN mua ròng trong khi lệnh NHỎ bán ròng → tổ chức gom từ tay yếu."""
    bb = bs = sb = ss = 0
    for t in ticks:
        big = t["value"] >= big_value
        if t["side"] == "B":
            if big: bb += t["volume"]
            else:   sb += t["volume"]
        elif t["side"] == "S":
            if big: bs += t["volume"]
            else:   ss += t["volume"]
    bt, stt = bb + bs, sb + ss
    big_dir = (bb - bs) / bt if bt else 0.0
    sml_dir = (sb - ss) / stt if stt else 0.0
    valid = bt > 0 and stt > 0 and bt >= 0.02 * (bt + stt)
    return {"big_dir": round(big_dir, 3), "small_dir": round(sml_dir, 3),
            "divergence": round(big_dir - sml_dir, 3) if valid else 0.0,
            "divergence_valid": valid}


def _window_agg(ticks: List[dict], window_min: int):
    """Mua/bán trong cửa sổ cuối — QUÉT NGƯỢC từ cuối nên chỉ parse tick TRONG cửa sổ
    (O(cửa sổ)), thay vì parse timestamp toàn tape mỗi lần gọi (O(n), 149ms/100k tick)."""
    if not ticks:
        return 0, 0
    last = None
    for t in reversed(ticks):
        last = _parse_ts(t["ts"])
        if last:
            break
    if not last:
        return 0, 0
    start = last - timedelta(minutes=window_min)
    wb = ws = 0
    for t in reversed(ticks):
        dt = _parse_ts(t["ts"])
        if dt and dt < start:
            break
        if t["side"] == "B":
            wb += t["volume"]
        elif t["side"] == "S":
            ws += t["volume"]
    return wb, ws


def _detect_manip(ticks: List[dict], big_value: float, follow: int = 30,
                  exchange: str = "HOSE") -> dict:
    """Rũ hàng / kéo giá — bản SIẾT để không còn là tautology.

    Bản cũ chỉ đòi "sau lệnh MUA lớn có một lệnh BÁN giá thấp hơn & KL nhỏ hơn". Vì lệnh
    lớn luôn có KL lớn hơn lệnh thường nên điều kiện gần như LUÔN đúng → đo thực tế:
    bật cờ 100% trên phiên NGẪU NHIÊN thuần (200/200 phiên).

    Bản mới đòi đủ 3 yếu tố — hình dạng THẬT của cú rũ:
      1. Giá bị ép xuống ≥ ngưỡng dip (không phải 1 bước giá lẻ)
      2. Lực ép là BÁN CHỦ ĐỘNG chiếm ưu thế (>55%)
      3. Giá HỒI LẠI trên giá lệnh lớn trước khi hết cửa sổ ← mấu chốt bản cũ thiếu

    Ngưỡng dip THÍCH ỨNG (không cố định): phải vượt ~2 bước giá VÀ tỉ lệ với biên độ
    phiên. Ngưỡng cố định 0.15% là NHỎ HƠN 1 bước giá HOSE ở mức 20k (0.25%) nên bất kỳ
    nhịp giảm 1 bước nào cũng thoả → vẫn 78% dương tính giả (đã đo)."""
    n = len(ticks)
    if n < 20:
        return {"manip": 0.0, "shakeout": 0, "uptrap": 0, "min_dip_pct": 0.0}
    prices = [t["price"] for t in ticks]
    mean_p = sum(prices) / n
    # Chuẩn hoá bằng BIẾN ĐỘNG CỤC BỘ (median |Δ| giữa các tick), KHÔNG dùng biên độ cả
    # phiên: biên độ chỉ tăng theo thời gian ⇒ ngưỡng phình dần (đo được 0.50%→1.04%)
    # ⇒ detector mù dần về cuối phiên, đúng lúc thanh khoản cao nhất.
    diffs = sorted(abs(prices[i] - prices[i - 1]) for i in range(1, n))
    med_move_pct = (diffs[len(diffs) // 2] / mean_p * 100.0) if diffs and mean_p else 0.0
    min_dip = max(2.0 * _tick_pct(mean_p, exchange), 6.0 * med_move_pct, 0.4)

    big_idx = [i for i, t in enumerate(ticks) if t["value"] >= big_value]
    shake = trap = 0
    for i in big_idx:
        b = ticks[i]
        wnd = ticks[i + 1:i + 1 + follow]
        if len(wnd) < 5:
            continue
        px = [w["price"] for w in wnd]
        # Đo lực ép CHỈ TRÊN NHÁNH ÉP (từ lệnh lớn tới đáy/đỉnh), KHÔNG trên toàn cửa sổ.
        # Cửa sổ buộc phải chứa nhánh HỒI để thoả điều kiện "giá hồi lại", mà nhánh hồi
        # toàn lệnh ngược chiều ⇒ pha loãng tỉ lệ xuống dưới 0.55 ⇒ cú rũ càng hồi mạnh
        # càng KHÓ bị phát hiện — ngược hoàn toàn với ý đồ (đo được: nhánh hồi dài thì
        # độ nhạy tụt 88%→72%).
        if b["side"] == "B":
            k = px.index(min(px))                       # vị trí ĐÁY
            dip = (b["price"] - px[k]) / b["price"] * 100.0
            leg = wnd[:k + 1]                           # nhánh GIẢM
            sv = sum(w["volume"] for w in leg if w["side"] == "S")
            tot = sum(w["volume"] for w in leg) or 1
            if dip >= min_dip and sv / tot > 0.55 and px[-1] >= b["price"]:
                shake += 1
        elif b["side"] == "S":
            k = px.index(max(px))                       # vị trí ĐỈNH
            pump = (px[k] - b["price"]) / b["price"] * 100.0
            leg = wnd[:k + 1]                           # nhánh TĂNG
            bv = sum(w["volume"] for w in leg if w["side"] == "B")
            tot = sum(w["volume"] for w in leg) or 1
            if pump >= min_dip and bv / tot > 0.55 and px[-1] <= b["price"]:
                trap += 1
    # Giữ mẫu số có sàn (như bản cũ) để tape mỏng không cho điểm nhiễu
    manip = _clamp((shake - trap) / max(3.0, len(big_idx) or 1), -1, 1)
    return {"manip": round(manip, 3), "shakeout": shake, "uptrap": trap,
            "min_dip_pct": round(min_dip, 3)}


def _behavior(ticks: List[dict], big_value: float, exchange: str = "HOSE") -> dict:
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

    # 2) MÁNH quanh LỆNH LỚN — rũ hàng gom / kéo giá xả (đã siết, xem _detect_manip)
    mp = _detect_manip(ticks, big_value, exchange=exchange)
    manip, shakeout, uptrap = mp["manip"], mp["shakeout"], mp["uptrap"]
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
    # Người dùng để MẶC ĐỊNH → dùng ngưỡng THÍCH ỨNG theo mã; nếu tự đặt ngưỡng ở
    # Shark Action thì tôn trọng giá trị đó.
    if ticks and big_value == BIG_VALUE_VND:
        big_value = _adaptive_big_value(ticks)
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

    # Cửa sổ trượt — quét ngược, chỉ parse tick trong cửa sổ (nhanh hơn nhiều)
    w_buy, w_sell = _window_agg(ticks, window_min)

    imbalance = (total_buy - total_sell) / total_vol if total_vol else 0.0          # -1..1
    big_dir = big_net / (big_buy_val + big_sell_val) if (big_buy_val + big_sell_val) else 0.0  # -1..1
    w_total = w_buy + w_sell
    w_imbalance = (w_buy - w_sell) / w_total if w_total else 0.0

    # Hành vi thao túng (tape reading): hấp thụ / rũ hàng-kéo giá / spring-upthrust
    beh = _behavior(ticks, big_value, exchange=_exchange_of(tk))

    # PHÂN KỲ lớn/nhỏ — thành phần độc lập, trước đây không hề đo
    dv = _divergence(ticks, big_value)

    # Shark Score v2 — bỏ ĐA CỘNG TUYẾN.
    # Bản cũ đặt 0.28 big_dir + 0.18 imbalance + 0.16 manip = 0.62 trọng số lên ba
    # cách viết của CÙNG một đại lượng "hướng dòng tiền" (đo được corr(big_dir,
    # imbalance) = +0.92). Nay gộp nhóm cùng phương thành MỘT "flow", nhường trọng
    # số cho phân kỳ + hấp thụ (hai tín hiệu thực sự độc lập).
    flow = 0.45 * big_dir + 0.35 * imbalance + 0.20 * w_imbalance
    score = 100.0 * (
        0.30 * flow +                                  # hướng dòng tiền (đã gộp)
        0.28 * _clamp(dv["divergence"] / 1.2, -1, 1) +  # phân kỳ lớn/nhỏ (MỚI)
        0.24 * beh["absorption"] +                     # hấp thụ (độc lập)
        0.10 * beh["manip"] +                          # rũ/kéo (đã siết)
        0.08 * beh["reversal"]                         # spring / upthrust
    )
    score = round(_clamp(score, -100, 100))

    if score >= 25:
        label = "Gom hàng"
    elif score <= -25:
        label = "Xả hàng"
    else:
        label = "Trung tính"

    # Cờ phân kỳ — diễn giải trực tiếp "tiền lớn gom / nhỏ lẻ bán"
    patterns = list(beh["flags"])
    if dv["divergence_valid"] and dv["divergence"] >= 0.35:
        patterns.insert(0, f"Tiền lớn gom, nhỏ lẻ bán (phân kỳ {dv['divergence']:+.2f})")
    elif dv["divergence_valid"] and dv["divergence"] <= -0.35:
        patterns.insert(0, f"Tiền lớn xả, nhỏ lẻ mua (phân kỳ {dv['divergence']:+.2f})")

    last_price = ticks[-1]["price"]
    # Danh sách lệnh lớn HIỂN THỊ: trước đây cắt 30 gần nhất → mã nhiều lệnh lớn chỉ thấy
    # buổi chiều (điểm KHÔNG bị ảnh hưởng vì aggregate big_buy_val/big_sell_val quét CẢ
    # phiên). Nay giữ tới 500 lệnh gần nhất → thấy được cả buổi sáng (list cuộn được).
    big_orders = [
        {"ts": t["ts"], "side": t["side"], "volume": t["volume"],
         "price": t["price"], "value": t["value"]}
        for t in big[-500:]
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
        "patterns": patterns,
        # Thành phần MỚI (v2) — hướng dòng tiền đã gộp + phân kỳ lớn/nhỏ
        "flow": round(flow, 3),
        "big_dir": dv["big_dir"],
        "small_dir": dv["small_dir"],
        "divergence": dv["divergence"],
        "last_price": last_price,
        "last_ts": ticks[-1]["ts"],
        "n_ticks": len(ticks),
        "_v": SCORE_VERSION,
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
    _clean_tape(c)
    m = _metrics(tk, c.get("ticks", []), big_value, window_min)
    m.pop("big_orders", None)   # list view không cần chi tiết lệnh lớn
    if "err" in c and m.get("empty"):
        m["error"] = c["err"]
    if _score_cacheable(big_value, window_min) and not m.get("empty"):
        try:
            tape_store.save_score(tk, _session_date(tk), m, big_value,
                                  complete=not _is_trading_hours())
        except Exception as e:  # noqa: BLE001
            print(f"⚠️  save_score {tk}: {e}", flush=True)
    return m


def get_signal(ticker: str, big_value: float = BIG_VALUE_VND, window_min: int = WINDOW_MIN) -> dict:
    """Tín hiệu gọn (không kèm tape) — CHỈ ĐỌC CACHE, không gọi API, KHÔNG tính lại.

    Thứ tự (tham số mặc định): điểm worker tính ngầm (RAM) → điểm chốt trong store →
    tính 1 lần (mã chưa được worker chạm tới, vd chưa vào watchlist). Sau khi tính
    fallback thì lưu ngay để lần sau chỉ load."""
    tk = ticker.upper()
    _touch(tk)
    default = _score_cacheable(big_value, window_min)
    if default:
        # 1) điểm worker tính ngầm khi có giao dịch mới (mới nhất, đúng phiên)
        s = _cached_score(tk)
        if s is not None:
            return s
        # 2) ngoài phiên: điểm chốt trong store (khỏi tải/tính)
        if not _is_trading_hours():
            cached = tape_store.load_score(tk, _session_date(tk))
            if (cached and cached.get("complete") and cached.get("big_value") == big_value
                    and (cached.get("signal") or {}).get("_v") == SCORE_VERSION):
                return cached["signal"]
    # 3) fallback: tính 1 lần trên tape đang có (mã chưa được worker tính ngầm)
    c = _ensure_loaded(tk)          # nạp từ store nếu cần — KHÔNG gọi API
    _clean_tape(c)
    m = _metrics(tk, c.get("ticks", []), big_value, window_min)
    m.pop("big_orders", None)
    if "err" in c and m.get("empty"):
        m["error"] = c["err"]
    if default and not m.get("empty"):
        _score_cache[tk] = {"signal": m, "sig": (len(c["ticks"]), c["ticks"][-1]["ts"]),
                            "date": c.get("date") or _today(), "at": time.time()}
        try:
            tape_store.save_score(tk, _session_date(tk), m, big_value,
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
    _clean_tape(c)                  # khử trùng + sắp tuần tự trước khi đọc
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

    # Chỉ nạp vào cache hiển thị khi đúng phiên HÔM NAY. Phiên CŨ chỉ ghi store theo
    # ĐÚNG ngày phiên — tuyệt đối không ghi dưới khoá hôm nay, nếu không tick phiên cũ
    # sẽ cộng dồn vào phiên mới làm SAI điểm Shark trong phiên.
    if target == _today():
        with _lock:
            _cache.pop(tk, None)
        _ensure_loaded(tk)
        push_ticks(tk, rows, src, aggregate=agg)
        with _lock:
            c = _cache[tk]
            c["deep"] = True
            c["seeded"] = True
            snap = list(c["ticks"])
    else:
        snap = rows   # phiên cũ (chỉ DNSE lấy được) — rows đã gộp sẵn

    _save_tape(tk, target, snap, complete=True)
    m = _metrics(tk, snap, BIG_VALUE_VND, WINDOW_MIN)
    if not m.get("empty"):
        m2 = {k: v for k, v in m.items() if k != "big_orders"}
        tape_store.save_score(tk, target, m2, BIG_VALUE_VND, complete=True)
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
