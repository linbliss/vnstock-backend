"""dnse_feed — luồng market data realtime của DNSE qua WebSocket (streaming push).

Giao thức (theo dnse-tech/openapi-sdk):
  - Kết nối wss://ws-openapi.dnse.com.vn
  - Gửi auth: {action:auth, api_key, signature=HMAC-SHA256("{key}:{ts}:{nonce}"), timestamp, nonce}
    → chờ {action: auth_success}
  - Subscribe: {action:subscribe, channels:[{name:"tick_extra.{board}.json", symbols:[...]}]}
  - Nhận tick khớp lệnh (có side Mua/Bán) → đẩy vào cache shark_monitor.

Bật khi có DNSE_API_KEY/SECRET; thiếu → OFF (giữ vnstock). Chỉ subscribe mã ĐANG CẦN
(shark_monitor.register_demand) để không vượt giới hạn.

CẦN KIỂM CHỨNG khi có key: envelope message dữ liệu, mapping `side`, và ĐƠN VỊ GIÁ.
"""
from __future__ import annotations
import os
import json
import time
import asyncio
import hmac
import hashlib

from app.services import dnse_client, shark_monitor

WS_URL = os.environ.get("DNSE_WS_URL", "wss://ws-openapi.dnse.com.vn")
# Board cổ phiếu — TODO xác nhận mapping sàn khi có key (SDK default gồm G1,G3,G4,G7,T1..T6)
STOCK_BOARDS = [b.strip() for b in os.environ.get("DNSE_STOCK_BOARDS", "G1,G4,G7").split(",") if b.strip()]
DEMAND_TTL = 300.0        # giây — mã không được hỏi 5' thì ngừng quan tâm

_demand: dict[str, float] = {}
_running = False
_ws = None
_subscribed: set[str] = set()


def register_demand(ticker: str) -> None:
    """shark_monitor gọi khi cần dữ liệu 1 mã → feed sẽ subscribe."""
    if dnse_client.enabled():
        _demand[ticker.upper()] = time.time()


def active() -> bool:
    return _running and dnse_client.enabled()


def _wanted() -> set[str]:
    now = time.time()
    return {t for t, ts in _demand.items() if now - ts < DEMAND_TTL}


def _auth_message() -> dict:
    ts = int(time.time())
    nonce = str(int(time.time() * 1_000_000))
    msg = f"{dnse_client._key()}:{ts}:{nonce}"
    sig = hmac.new(dnse_client._secret().encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()
    return {"action": "auth", "api_key": dnse_client._key(), "signature": sig,
            "timestamp": ts, "nonce": nonce}


def _side_char(side) -> str:
    # TODO xác nhận: DNSE side 1 = mua chủ động, 2 = bán chủ động?
    if side in (1, "1", "B", "BUY", "b"):
        return "B"
    if side in (2, "2", "S", "SELL", "s"):
        return "S"
    return "U"


def _extract_tick(data: dict):
    d = data.get("data") or data.get("d") or data
    sym = d.get("symbol") or d.get("s")
    price = d.get("matchPrice", d.get("price", d.get("mp")))
    vol = d.get("matchQtty", d.get("quantity", d.get("mq")))
    if sym is None or price is None or vol is None:
        return None
    return (str(sym).upper(), float(price), int(vol), d.get("side"), str(d.get("time") or d.get("t") or ""))


async def _sub_loop(ws, authed: asyncio.Event):
    """Đồng bộ subscription theo nhu cầu (subscribe mã mới xuất hiện)."""
    global _subscribed
    await authed.wait()
    while _running:
        wanted = _wanted()
        new = wanted - _subscribed
        if new:
            syms = sorted(wanted)
            for board in STOCK_BOARDS:
                msg = {"action": "subscribe",
                       "channels": [{"name": f"tick_extra.{board}.json", "symbols": syms}]}
                try:
                    await ws.send(json.dumps(msg))
                except Exception:  # noqa: BLE001
                    return
            _subscribed = set(wanted)
        await asyncio.sleep(8)


async def _run():
    global _ws, _subscribed
    import websockets
    while _running:
        _subscribed = set()
        try:
            async with websockets.connect(WS_URL, ping_interval=20, max_size=2 ** 22) as ws:
                _ws = ws
                authed = asyncio.Event()
                await ws.send(json.dumps(_auth_message()))
                sub_task = asyncio.create_task(_sub_loop(ws, authed))
                async for raw in ws:
                    try:
                        data = json.loads(raw)
                    except (ValueError, TypeError):
                        continue
                    action = data.get("action") or data.get("a")
                    if action == "auth_success":
                        authed.set()
                        print("🌊 DNSE WS authenticated", flush=True)
                        continue
                    if action in ("auth_error", "error"):
                        print(f"⚠️  DNSE WS error: {data}", flush=True)
                        break
                    if action == "te" or "tick_extra" in str(data.get("channel", "")):
                        tk = _extract_tick(data)
                        if tk:
                            sym, price, vol, side, ts = tk
                            px_k = price / 1000.0 if price > 1000 else price   # TODO đơn vị giá
                            row = {"id": f"{sym}_{ts}_{price}_{vol}", "ts": ts, "price": px_k,
                                   "volume": vol, "side": _side_char(side), "value": vol * px_k * 1000.0}
                            shark_monitor.push_ticks(sym, [row], source="DNSE")
                sub_task.cancel()
        except Exception as e:  # noqa: BLE001
            print(f"⚠️  DNSE WS mất kết nối ({type(e).__name__}: {e}) — thử lại sau 5s", flush=True)
        if _running:
            await asyncio.sleep(5)


async def start():
    global _running
    # WS streaming còn thử nghiệm → chỉ bật khi DNSE_WS_ENABLED=true.
    # Mặc định tắt: shark dùng DNSE REST get_trades (đã kiểm chứng) trong shark_monitor.
    if os.environ.get("DNSE_WS_ENABLED", "").lower() not in ("1", "true", "yes"):
        return
    if not dnse_client.enabled():
        print("ℹ️  DNSE feed OFF (chưa có DNSE_API_KEY/SECRET)", flush=True)
        return
    _running = True
    asyncio.create_task(_run())
    print("🌊 DNSE feed: starting…", flush=True)


async def stop():
    global _running
    _running = False
    if _ws:
        try:
            await _ws.close()
        except Exception:  # noqa: BLE001
            pass
