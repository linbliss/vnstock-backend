"""dnse_feed — Market Data WebSocket của DNSE (theo ĐÚNG tài liệu chính thức).

Vì sao dùng WS: DNSE thiết kế dữ liệu realtime để PUSH qua WebSocket. Poll REST liên
tục cho từng mã là sai thiết kế, tốn quota và dễ bị firewall chặn IP. WS = 1 kết nối,
không tiêu rate-limit REST.

Giao thức (developers.dnse.com.vn/docs/sdk/build_websocket) — đã kiểm chứng thực tế:
  1. Kết nối wss://ws-openapi.dnse.com.vn/v1/stream?encoding={json|msgpack}
     → `encoding` quyết định ĐỊNH DẠNG FRAME (msgpack ⇒ cả frame điều khiển là binary).
  2. Server gửi {"action":"welcome"} → client phải auth trong 30s.
  3. Auth: signature = HMAC-SHA256(secret, "{api_key}:{timestamp}:{nonce}").hexdigest()
     timestamp = giây (±5'), nonce = micro-giây (không lặp trong 10').
  4. {"action":"auth_success"} kèm rate_limit {messages_per_second, subscriptions_max}.
  5. Subscribe {"action":"subscribe","channels":[{"name":"tick_extra.G1.json","symbols":[...]}]}
     tick_extra = khớp lệnh CÓ chiều Mua/Bán (đúng thứ Shark cần).
  6. Server PING mỗi 3' → client PHẢI PONG trong 60s (không PONG ⇒ bị ngắt).
     Vì vậy TẮT ping protocol của thư viện (ping_interval=None) — DNSE ping ở tầng
     ứng dụng (JSON), không phải WS control frame.
  7. Kết nối tối đa 8h → server ngắt (connection_expired) → tự kết nối lại.

Bật bằng DNSE_WS_ENABLED=true (mặc định OFF cho tới khi kiểm chứng frame dữ liệu
TRONG PHIÊN — ngoài giờ không có tick nào để xác nhận schema).
"""
from __future__ import annotations
import os
import json
import time
import asyncio
import hmac
import hashlib
from datetime import datetime, timezone, timedelta

from app.services import dnse_client, shark_monitor

WS_BASE = os.environ.get("DNSE_WS_URL", "wss://ws-openapi.dnse.com.vn/v1/stream")
ENCODING = os.environ.get("DNSE_WS_ENCODING", "json").lower()   # json | msgpack

# Board hợp lệ theo enum chính thức (KHÔNG có G7 — cấu hình cũ sai).
VALID_BOARDS = {"G1", "G3", "G4", "T1", "T3", "T4", "T6"}
# G1 = lô chẵn (giao dịch thường) — đủ cho Shark. G3 = PLO, G4 = lô lẻ.
_boards_env = [b.strip().upper() for b in os.environ.get("DNSE_STOCK_BOARDS", "G1").split(",")]
STOCK_BOARDS = [b for b in _boards_env if b in VALID_BOARDS] or ["G1"]

DEMAND_TTL = 300.0        # giây — mã không được hỏi 5' thì ngừng subscribe
BOOK_CAP = 5              # số mã tối đa lấy SỔ LỆNH (chỉ mã đang xem chi tiết)
CONTROL = {"welcome", "auth_success", "subscribed", "unsubscribed",
           "ping", "pong", "connection_expired", "error"}

_demand: dict[str, float] = {}          # mã cần TICK (cả danh mục)
_book_demand: dict[str, float] = {}     # mã cần SỔ LỆNH (chỉ mã đang xem chi tiết)
_orderbook: dict[str, dict] = {}        # mã → sổ lệnh mới nhất từ WS
_quote: dict[str, dict] = {}            # mã → giá realtime mới nhất (cho bảng giá)
_running = False
_subs_max = 100           # cập nhật từ auth_success.rate_limit.subscriptions_max
_last_tick_at: dict[str, float] = {}   # mã → lần cuối nhận tick qua WS
_tick_count = 0           # tổng tick nhận được (để kiểm chứng qua /api/status)

# ── ĐA KẾT NỐI để vượt trần 100 subscription/kết nối MÀ KHÔNG mất chính xác ──────────
# Round-robin theo KẾT NỐI (không theo thời gian): mỗi mã nằm CỐ ĐỊNH trên 1 kết nối và
# được subscribe LIÊN TỤC → tape không có lỗ hổng → Shark Point vẫn đúng. Watchlist vượt
# 1 kết nối thì tự mở thêm kết nối (mỗi cái ~90 mã). Chia lượt theo giây (bật/tắt luân
# phiên) sẽ tạo lỗ hổng tape → KHÔNG dùng.
SLOT_RESERVE = 8          # chừa mỗi kết nối cho book + margin (không sát trần 100)


class _Conn:
    __slots__ = ("idx", "ws", "authed", "subscribed", "book_subscribed", "task")

    def __init__(self, idx: int):
        self.idx = idx
        self.ws = None
        self.authed = False
        self.subscribed: set[str] = set()
        self.book_subscribed: set[str] = set()
        self.task = None


_conns: dict[int, _Conn] = {}


def _slot() -> int:
    """Số mã TICK tối đa mỗi kết nối (chừa margin + book)."""
    nb = max(1, len(STOCK_BOARDS))
    return max(1, _subs_max // nb - SLOT_RESERVE)


def _shard(idx: int) -> set:
    """Phần mã (tick) mà kết nối idx phụ trách — ổn định theo thứ tự bảng chữ cái."""
    w = sorted(_wanted(_demand))
    s = _slot()
    return set(w[idx * s:(idx + 1) * s])


def _need_conns() -> int:
    import math
    n = len(_wanted(_demand))
    return max(1, math.ceil(n / _slot())) if n else 1


def stats() -> dict:
    """Tóm tắt trạng thái feed — cho /api/status. Chỉ trả SỐ LƯỢNG (đa kết nối)."""
    return {
        "enabled": _running,
        "connected": any(c.ws is not None for c in _conns.values()),
        "authenticated": any(c.authed for c in _conns.values()),
        "connections": sum(1 for c in _conns.values() if c.ws is not None),
        "subscribed": sum(len(c.subscribed) for c in _conns.values()),
        "streaming": sum(1 for t in _last_tick_at if time.time() - _last_tick_at[t] < 60),
        "ticks": _tick_count,
        "book_subscribed": sum(len(c.book_subscribed) for c in _conns.values()),
        "orderbooks": len(_orderbook),
    }


def register_demand(ticker: str, book: bool = False) -> None:
    """shark_monitor gọi khi cần dữ liệu 1 mã → feed sẽ subscribe mã đó.
    book=True: cần thêm SỔ LỆNH (top_price) — chỉ dùng cho mã đang xem chi tiết,
    vì mỗi (kênh, mã) tính 1 subscription mà trần chỉ 100.

    Dùng configured() (chỉ xét key) chứ KHÔNG dùng enabled(): REST bị chặn/breaker
    ngắt không được phép làm WS ngừng đăng ký mã — hai host độc lập nhau."""
    if not dnse_client.configured():
        return
    tk = ticker.upper()
    _demand[tk] = time.time()
    if book:
        _book_demand[tk] = time.time()


def get_orderbook(ticker: str):
    """Sổ lệnh mới nhất nhận qua WS (top_price) — thay REST get_quotes.
    Cùng shape với dnse_client.get_orderbook để frontend không phải đổi."""
    return _orderbook.get(ticker.upper())


def get_quote(ticker: str, max_age: float = 90.0):
    """Giá realtime mới nhất từ tick_extra (cho BẢNG GIÁ).

    KHÔNG có trần/sàn/tham chiếu: các giá đó nằm ở kênh security_definition mà DNSE
    chỉ phát BATCH lúc ~08:00 (BOD) — đã kiểm chứng: subscribe lúc 09:55 không nhận
    được gì. Vì vậy bảng giá vẫn phải lấy trần/sàn/TC từ nguồn khác (KBS), WS chỉ
    phủ phần realtime.
    """
    q = _quote.get(ticker.upper())
    if not q:
        return None
    return q if (time.time() - q["at"]) < max_age else None


def active() -> bool:
    return _running and any(c.ws is not None for c in _conns.values()) and dnse_client.configured()


def streaming(ticker: str) -> bool:
    """True nếu WS đang thực sự đẩy tick cho mã này (trong 60s gần đây)
    → shark_monitor giãn poll REST cho mã đó."""
    return active() and (time.time() - _last_tick_at.get(ticker.upper(), 0.0) < 60.0)


def _wanted(src: dict | None = None) -> set[str]:
    now = time.time()
    d = _demand if src is None else src
    return {t for t, ts in d.items() if now - ts < DEMAND_TTL}


def _auth_message() -> dict:
    ts = int(time.time())
    nonce = str(int(time.time() * 1_000_000))     # micro-giây, duy nhất
    msg = f"{dnse_client._key()}:{ts}:{nonce}"
    sig = hmac.new(dnse_client._secret().encode("utf-8"),
                   msg.encode("utf-8"), hashlib.sha256).hexdigest()
    return {"action": "auth", "api_key": dnse_client._key(),
            "signature": sig, "timestamp": ts, "nonce": nonce}


def _encode(obj: dict):
    """Frame gửi đi phải cùng encoding với kết nối."""
    if ENCODING == "msgpack":
        import msgpack
        return msgpack.packb(obj, use_bin_type=True)
    return json.dumps(obj)


def _decode(raw):
    """Frame nhận: bytes ⇒ msgpack, str ⇒ json (server có thể trả binary dù xin json)."""
    if isinstance(raw, (bytes, bytearray)):
        try:
            import msgpack
            return msgpack.unpackb(raw, raw=False)
        except Exception:  # noqa: BLE001
            return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return None


_VN_TZ = timezone(timedelta(hours=7))   # giờ sàn HOSE/HNX


def _ts_str(t) -> str:
    """WS trả time = {"Seconds": epoch, "Nanos": n} (KHÔNG phải chuỗi như REST).
    Quy về ĐÚNG định dạng REST 'YYYY-MM-DD HH:MM:SS.mmm' theo giờ sàn (VN) để tape
    trộn chung REST+WS vẫn sắp xếp/gộp/tính cửa sổ được.

    Dùng offset +07 tường minh, không phụ thuộc TZ của container.
    """
    if isinstance(t, dict):
        secs = t.get("Seconds", t.get("seconds"))
        nanos = t.get("Nanos", t.get("nanos")) or 0
        if secs is None:
            return ""
        try:
            dt = datetime.fromtimestamp(int(secs), _VN_TZ).replace(tzinfo=None)
        except (TypeError, ValueError, OSError):
            return ""
        return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{int(nanos) // 1_000_000:03d}"
    return str(t or "")


def _extract(d: dict):
    """tick_extra → row chuẩn của shark_monitor. Giá DNSE là kVND (vd HPG 22.2),
    giống hệt REST get_trades nên KHÔNG quy đổi."""
    sym = d.get("symbol")
    price = d.get("matchPrice")
    vol = d.get("matchQtty")
    if not sym or price is None or vol is None:
        return None
    try:
        price = float(price)
        vol = int(vol) * dnse_client.VOL_LOT   # DNSE trả lô 10 CP → quy về cổ phiếu
    except (TypeError, ValueError):
        return None
    if vol <= 0 or price <= 0:
        return None
    ts = _ts_str(d.get("time"))
    if not ts:
        return None
    tvt = d.get("totalVolumeTraded")
    return str(sym).upper(), {
        "id": str(tvt) if tvt is not None else f"{ts}_{price}_{vol}",
        "ts": ts,
        "price": price,
        "volume": vol,
        "side": dnse_client._norm_side(d.get("side")),
        "value": vol * price * 1000.0,
    }


def _on_book(d: dict) -> None:
    """top_price (T='q') → sổ lệnh 10 mức. REST get_quotes chỉ cho 3 mức và tốn
    hạn mức 10k/giờ; WS cho realtime + miễn phí hạn mức."""
    sym = d.get("symbol")
    if not sym:
        return
    def _levels(rows):
        # qtty CŨNG theo lô 10 như matchQtty → quy về cổ phiếu.
        # Chứng minh: HOSE/HNX chỉ nhận lô chẵn 100 CP nên hàng chờ phải là bội số
        # của 100 CP. Quan sát thật: 17.810 / 9.660 / 560 KHÔNG chia hết 100 (⇒ không
        # thể là cổ phiếu) nhưng đều chia hết 10 ⇒ ×10 ra 178.100 / 96.600 / 5.600
        # = đúng bội số 100.
        out = []
        for r in (rows or []):
            p, q = r.get("price"), r.get("qtty")
            if p is None or q is None:
                continue
            out.append({"price": float(p), "quantity": int(q) * dnse_client.VOL_LOT})
        return out
    bid, offer = _levels(d.get("bid")), _levels(d.get("offer"))
    if not bid and not offer:
        return
    _orderbook[str(sym).upper()] = {
        "bid": bid, "offer": offer, "time": _ts_str(d.get("time")),
        "total_bid": d.get("totalBidQtty"), "total_offer": d.get("totalOfferQtty"),
    }


def _on_data(d: dict) -> None:
    global _tick_count
    if d.get("T") == "q":          # sổ lệnh
        _on_book(d)
        return
    got = _extract(d)
    if not got:
        return
    sym, row = got
    _last_tick_at[sym] = time.time()
    _tick_count += 1
    # tick_extra đã kèm sẵn giá/KL luỹ kế/OHLC → dùng luôn cho BẢNG GIÁ,
    # không tốn thêm subscription nào (khỏi cần kênh "tick" riêng).
    #
    # ĐƠN VỊ: _quote quy về ĐÚNG hệ của bảng giá = VND thô + cổ phiếu
    # (khác tape của Shark dùng kVND). Frontend fmtPrice() chia 1000, nên nếu để
    # kVND thì giá hiển thị nhỏ đi 1000 lần và nhấp nháy mỗi khi KBS ghi đè.
    try:
        _quote[sym] = {
            "price": row["price"] * 1000.0,                    # kVND → VND
            "volume": int(d.get("totalVolumeTraded") or 0) * dnse_client.VOL_LOT,
            "open": float(d.get("openPrice") or 0) * 1000.0,
            "high": float(d.get("highestPrice") or 0) * 1000.0,
            "low": float(d.get("lowestPrice") or 0) * 1000.0,
            "at": time.time(),
        }
    except (TypeError, ValueError):
        pass
    if _tick_count == 1:      # xác nhận 1 lần: tick ĐẦU TIÊN thật sự về tới nơi
        print(f"🌊 DNSE WS: tick đầu tiên OK ({sym} {row['side']} "
              f"{row['volume']} @ {row['price']} — {row['ts']})", flush=True)
    # Chỉ nạp vào tape Shark khi Shark THỰC SỰ chọn nguồn DNSE. Trước đây đẩy vô điều
    # kiện ⇒ ép Shark sang vnstock ở Admin nhưng tick DNSE vẫn chui vào tape (trộn 2
    # nguồn ngoài ý muốn). Bảng giá (_quote ở trên) không bị ảnh hưởng vì nó theo
    # cấu hình "priceboard" riêng.
    try:
        from app.services import data_source
        if data_source.get_source("shark") != "dnse":
            return
    except Exception:  # noqa: BLE001
        pass
    # aggregate=True: WS đẩy TỪNG khớp lẻ → gộp cùng chiều trong 150ms thành 1 "lệnh"
    # (giống REST get_intraday_ticks) để nhận diện lệnh lớn cho đúng.
    shark_monitor.push_ticks(sym, [row], source="DNSE", aggregate=True)


async def _pong_loop(ws):
    """Chủ động PONG mỗi 25s (tài liệu cho phép) — giữ kết nối qua NAT."""
    while _running:
        await asyncio.sleep(25)
        try:
            await ws.send(_encode({"action": "pong", "timestamp": int(time.time() * 1000)}))
        except Exception:  # noqa: BLE001
            return


async def _sync(ws, channel: str, wanted: set, current: set) -> set:
    """Gửi subscribe/unsubscribe cho phần chênh lệch của 1 loại kênh."""
    add, rm = wanted - current, current - wanted
    for board in STOCK_BOARDS:
        name = f"{channel}.{board}.{ENCODING}"
        if add:
            await ws.send(_encode({"action": "subscribe",
                                   "channels": [{"name": name, "symbols": sorted(add)}]}))
        if rm:
            await ws.send(_encode({"action": "unsubscribe",
                                   "channels": [{"name": name, "symbols": sorted(rm)}]}))
    return wanted


async def _sub_loop(conn: _Conn, authed: asyncio.Event):
    """Đồng bộ subscription cho MỘT kết nối: tick = shard của kết nối đó; book (sổ lệnh)
    chỉ đặt trên kết nối #0 (≤ BOOK_CAP mã đang xem chi tiết)."""
    await authed.wait()
    while _running and _conns.get(conn.idx) is conn:
        tick = _shard(conn.idx)
        book = set(sorted(_wanted(_book_demand))[:BOOK_CAP]) if conn.idx == 0 else set()
        try:
            conn.subscribed = await _sync(conn.ws, "tick_extra", tick, conn.subscribed)
            conn.book_subscribed = await _sync(conn.ws, "top_price", book, conn.book_subscribed)
        except Exception:  # noqa: BLE001
            return
        await asyncio.sleep(8)


async def _run(conn: _Conn):
    """Vòng đời 1 KẾT NỐI WS (đa kết nối để vượt trần 100/kết nối)."""
    global _subs_max
    import websockets
    url = f"{WS_BASE}?encoding={ENCODING}"
    backoff = 5
    while _running and _conns.get(conn.idx) is conn:
        conn.subscribed = set()
        conn.book_subscribed = set()
        conn.authed = False
        try:
            async with websockets.connect(url, max_size=2 ** 22, ping_interval=None) as ws:
                conn.ws = ws
                backoff = 5
                authed = asyncio.Event()
                sub_task = asyncio.create_task(_sub_loop(conn, authed))
                pong_task = asyncio.create_task(_pong_loop(ws))
                try:
                    async for raw in ws:
                        d = _decode(raw)
                        if not isinstance(d, dict):
                            continue
                        action = d.get("action")
                        if action == "welcome":
                            await ws.send(_encode(_auth_message()))
                        elif action == "auth_success":
                            rl = d.get("rate_limit") or {}
                            _subs_max = int(rl.get("subscriptions_max") or 100)
                            conn.authed = True
                            authed.set()
                            print(f"🌊 DNSE WS #{conn.idx} authenticated (subs_max={_subs_max}, "
                                  f"enc={ENCODING})", flush=True)
                        elif action == "ping":
                            await ws.send(_encode({"action": "pong",
                                                   "timestamp": d.get("timestamp")}))
                        elif action == "connection_expired":
                            print(f"ℹ️  DNSE WS #{conn.idx} hết hạn (8h) — kết nối lại", flush=True)
                            break
                        elif action == "error":
                            print(f"⚠️  DNSE WS #{conn.idx} error: {d}", flush=True)
                            if str(d.get("code")) == "AUTH_FAILED":
                                print("⛔ DNSE WS: sai API key/secret — dừng feed", flush=True)
                                return
                        elif action in CONTROL:
                            pass
                        else:
                            _on_data(d)
                finally:
                    sub_task.cancel()
                    pong_task.cancel()
                    conn.ws = None
        except Exception as e:  # noqa: BLE001
            print(f"⚠️  DNSE WS #{conn.idx} mất kết nối ({type(e).__name__}: {e}) — thử lại "
                  f"sau {backoff}s", flush=True)
        if _running and _conns.get(conn.idx) is conn:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def _supervisor():
    """Giữ ĐỦ số kết nối theo nhu cầu: watchlist phình quá 1 kết nối thì mở thêm; co
    lại thì đóng bớt. Mỗi kết nối phụ trách 1 shard cố định → mã subscribe liên tục."""
    while _running:
        try:
            need = _need_conns()
            for i in range(need):     # mở kết nối còn thiếu
                c = _conns.get(i)
                if c is None or c.task is None or c.task.done():
                    c = _Conn(i)
                    _conns[i] = c
                    c.task = asyncio.create_task(_run(c))
            for i in [k for k in _conns if k >= need]:   # đóng kết nối dư
                c = _conns.pop(i)
                if c.task:
                    c.task.cancel()
                if c.ws:
                    try:
                        await c.ws.close()
                    except Exception:  # noqa: BLE001
                        pass
        except Exception as e:  # noqa: BLE001
            print(f"⚠️  DNSE WS supervisor: {type(e).__name__}: {e}", flush=True)
        await asyncio.sleep(10)


async def start():
    global _running
    if os.environ.get("DNSE_WS_ENABLED", "true").lower() in ("0", "false", "no"):
        print("ℹ️  DNSE feed OFF (DNSE_WS_ENABLED=false)", flush=True)
        return
    # configured() chứ không enabled(): WS là host RIÊNG (ws-openapi), không để sức khoẻ
    # REST (openapi) quyết định có bật feed hay không.
    if not dnse_client.configured():
        print("ℹ️  DNSE feed OFF (chưa có DNSE_API_KEY/SECRET)", flush=True)
        return
    _running = True
    asyncio.create_task(_supervisor())
    print("🌊 DNSE feed: starting… (đa kết nối theo nhu cầu)", flush=True)


async def stop():
    global _running
    _running = False
    for c in list(_conns.values()):
        if c.task:
            c.task.cancel()
        if c.ws:
            try:
                await c.ws.close()
            except Exception:  # noqa: BLE001
                pass
    _conns.clear()
