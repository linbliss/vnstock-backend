"""dnse_client — REST market data của DNSE OpenAPI (tự viết, không cần SDK chưa publish).

Auth: HTTP Signature HMAC-SHA256 (theo dnse-tech/openapi-sdk):
  signature_string = "(request-target): {method} {path}\n date: {date}\n nonce: {nonce}"
  X-Signature: Signature keyId="{api_key}",algorithm="hmac-sha256",headers="(request-target) date",signature="{b64url}",nonce="{nonce}"

Bật khi có DNSE_API_KEY + DNSE_API_SECRET; thiếu → enabled()=False (giữ vnstock/FireAnt).
LƯU Ý cần kiểm chứng khi có key: tham số get_ohlc (bar_type/resolution), ĐƠN VỊ GIÁ
(VND thô hay nghìn), và schema trả về của từng endpoint.
"""
from __future__ import annotations
import os
import time
import hmac
import hashlib
import base64
from datetime import datetime, timezone
from urllib.parse import quote, urlparse, urlencode
from uuid import uuid4
from typing import Optional

import requests

REST_BASE = os.environ.get("DNSE_REST_URL", "https://openapi.dnse.com.vn").rstrip("/")
API_VERSION = os.environ.get("DNSE_API_VERSION", "2026-05-07")
AGG_WINDOW_MS = 150   # gộp khớp cùng chiều trong cửa sổ này thành 1 "lệnh" (sweep)

# ĐƠN VỊ KHỐI LƯỢNG: DNSE trả matchQtty/totalVolumeTraded theo LÔ 10 CỔ PHIẾU,
# trong khi OHLC (v) trả theo CỔ PHIẾU. Đã kiểm chứng 2 cách độc lập (17/07/2026):
#   1) HPG 16/07: OHLC volume = 22.880.600 / totalVolumeTraded cuối phiên 2.288.060
#      = ĐÚNG 10.00
#   2) grossTradeAmount (tỷ đồng) / (totalVolumeTraded × avgPrice × 1000) = 10.0
#      với cả HPG lẫn SHS
# Không nhân 10 ⇒ giá trị lệnh nhỏ đi 10 lần ⇒ ngưỡng "lệnh lớn" của Shark bỏ sót.
VOL_LOT = 10


def _key() -> str:
    return os.environ.get("DNSE_API_KEY", "").strip()


def _secret() -> str:
    return os.environ.get("DNSE_API_SECRET", "").strip()


# ── Circuit breaker: nếu DNSE unreachable (timeout) nhiều lần → tạm ngắt, dùng fallback ──
_fail_count = 0
_disabled_until = 0.0
_BREAKER_THRESHOLD = 3
_BREAKER_COOLDOWN = 300.0   # giây


def configured() -> bool:
    """Chỉ xét ĐÃ CÓ KEY hay chưa — KHÔNG xét circuit breaker.

    REST (openapi.dnse.com.vn) và WS (ws-openapi.dnse.com.vn) là HAI host/IP khác
    nhau: REST có thể bị chặn từ IP nước ngoài trong khi WS vẫn chạy tốt. Vì vậy WS
    phải dùng hàm này, không dùng enabled() — nếu không, breaker của REST sẽ kéo WS
    chết theo (không đăng ký được mã nào).
    """
    return bool(_key() and _secret())


def enabled() -> bool:
    """REST có dùng được không (có key + breaker không ngắt)."""
    if not configured():
        return False
    if time.time() < _disabled_until:   # đang trong thời gian ngắt tạm
        return False
    return True


def _record_fail():
    global _fail_count, _disabled_until
    _fail_count += 1
    if _fail_count >= _BREAKER_THRESHOLD:
        _disabled_until = time.time() + _BREAKER_COOLDOWN
        _fail_count = 0
        print(f"⚠️  DNSE tạm NGẮT {int(_BREAKER_COOLDOWN)}s (nhiều timeout) — chuyển vnstock/FireAnt", flush=True)


def _record_ok():
    global _fail_count
    _fail_count = 0


def _backoff_429(headers) -> None:
    """429 = vượt rate limit (tài liệu: Rate/giờ + Quota/ngày, theo APIKey & endpoint).
    Nghỉ đúng theo Retry-After / X-RateLimit-Reset thay vì thử lại ngay."""
    global _disabled_until
    wait = 0.0
    ra = headers.get("Retry-After")
    if ra:
        try:
            wait = float(ra)
        except (TypeError, ValueError):
            wait = 0.0
    if wait <= 0:
        reset = headers.get("X-RateLimit-Reset") or headers.get("X-Ratelimit-Reset")
        try:
            wait = max(0.0, float(reset) - time.time()) if reset else 0.0
        except (TypeError, ValueError):
            wait = 0.0
    wait = min(max(wait, 60.0), 3600.0)      # kẹp 1' … 1h
    _disabled_until = max(_disabled_until, time.time() + wait)
    print(f"⚠️  DNSE rate limit — nghỉ {int(wait)}s (dùng vnstock trong lúc đó)", flush=True)


def _note_quota(path: str, headers) -> None:
    """Cảnh báo sớm khi quota sắp cạn (tài liệu khuyên chủ động điều tiết)."""
    rem = headers.get("X-RateLimit-Remaining") or headers.get("X-Ratelimit-Remaining")
    if rem is None:
        return
    try:
        rem_i = int(rem)
    except (TypeError, ValueError):
        return
    if rem_i and rem_i < 500:
        print(f"⚠️  DNSE quota sắp hết: còn {rem_i} request ({path})", flush=True)


def _sign(method: str, path: str) -> dict:
    date_value = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")
    nonce = uuid4().hex
    sig_string = (
        f"(request-target): {method.lower()} {path}\n"
        f"date: {date_value}\n"
        f"nonce: {nonce}"
    )
    mac = hmac.new(_secret().encode("utf-8"), sig_string.encode("utf-8"), hashlib.sha256)
    sig = quote(base64.b64encode(mac.digest()).decode("utf-8"), safe="")
    xsig = (
        f'Signature keyId="{_key()}",algorithm="hmac-sha256",'
        f'headers="(request-target) date",signature="{sig}",nonce="{nonce}"'
    )
    return {"Date": date_value, "X-Signature": xsig, "x-api-key": _key(),
            "version": API_VERSION, "Accept": "application/json"}


def health_check() -> dict:
    """Dò TRỰC TIẾP xem DNSE có gọi được không & PHÂN BIỆT nguyên nhân.
    Cố ý KHÔNG qua enabled() để vẫn dò được khi circuit breaker đang ngắt.

    state:
      ok           – gọi được (kèm latency + quota còn lại)
      no_key       – chưa cấu hình key
      blocked      – TCP không thiết lập được (chữ ký của IP bị firewall chặn)
      unreachable  – lỗi mạng/DNS
      timeout      – kết nối được nhưng không trả lời kịp
      rate_limited – 429 (vượt hạn mức)
      auth_failed  – 401/403 (sai key/chữ ký/lệch giờ)
      http_error   – mã lỗi khác
    """
    if not (_key() and _secret()):
        return {"ok": False, "state": "no_key",
                "message": "Chưa cấu hình DNSE_API_KEY / DNSE_API_SECRET"}

    now = int(time.time())
    path = "/price/ohlc"      # hạn mức cao nhất (50k/giờ) → dò cho rẻ
    params = {"type": "STOCK", "symbol": "HPG", "resolution": "1D",
              "from": now - 5 * 86400, "to": now}
    full = f"{REST_BASE}{path}?{urlencode(params)}"
    t0 = time.time()
    try:
        r = requests.get(full, headers=_sign("GET", path), timeout=(4, 8), proxies=_proxies())
        ms = int((time.time() - t0) * 1000)
        rem = r.headers.get("X-RateLimit-Remaining") or r.headers.get("X-Ratelimit-Remaining")
        if r.status_code == 200:
            return {"ok": True, "state": "ok", "latency_ms": ms,
                    "quota_remaining": int(rem) if rem and rem.isdigit() else None,
                    "message": f"DNSE phản hồi bình thường ({ms} ms)"}
        if r.status_code == 429:
            return {"ok": False, "state": "rate_limited", "latency_ms": ms,
                    "message": "Vượt hạn mức (429) — cần giãn nhịp gọi"}
        if r.status_code in (401, 403):
            return {"ok": False, "state": "auth_failed", "latency_ms": ms,
                    "message": f"Xác thực thất bại ({r.status_code}) — sai key/chữ ký "
                               f"hoặc đồng hồ server lệch >1 phút"}
        return {"ok": False, "state": "http_error", "latency_ms": ms,
                "message": f"HTTP {r.status_code}: {r.text[:100]}"}
    except requests.exceptions.ConnectTimeout:
        # DNS ra IP nhưng TCP handshake bị nuốt → chữ ký của việc bị chặn ở tầng mạng.
        # LƯU Ý: chỉ nói về HOST REST. WS là host khác (ws-openapi) và có thể vẫn chạy tốt.
        return {"ok": False, "state": "blocked",
                "message": f"Không thiết lập được kết nối tới {urlparse(REST_BASE).hostname} "
                           f"(ConnectTimeout) — host REST đang chặn IP server. "
                           f"WS là host khác nên có thể vẫn hoạt động bình thường."}
    except requests.exceptions.ReadTimeout:
        return {"ok": False, "state": "timeout",
                "message": "Kết nối được nhưng DNSE không trả lời kịp"}
    except requests.exceptions.RequestException as e:
        return {"ok": False, "state": "unreachable",
                "message": f"Lỗi mạng: {type(e).__name__}"}


def breaker_state() -> dict:
    """Circuit breaker đang ngắt hay không (ngắt ⇒ tạm dùng vnstock)."""
    left = max(0.0, _disabled_until - time.time())
    return {"open": left > 0, "seconds_left": int(left)}


def _proxies():
    # Định tuyến DNSE qua proxy VN nếu server bị chặn IP (đặt DNSE_PROXY trong .env,
    # vd http://user:pass@ip:port hoặc socks5://ip:port — socks cần cài PySocks).
    p = os.environ.get("DNSE_PROXY", "").strip()
    return {"http": p, "https": p} if p else None


def _get(path: str, params: Optional[dict] = None):
    """GET có ký. Ký theo request-target = PATH (không gồm query) — đúng như SDK."""
    if not enabled():
        return None
    qs = urlencode({k: v for k, v in (params or {}).items() if v is not None})
    full = f"{REST_BASE}{path}" + (f"?{qs}" if qs else "")
    sign_path = urlparse(full).path
    try:
        r = requests.get(full, headers=_sign("GET", sign_path), timeout=(5, 15), proxies=_proxies())
        if r.status_code == 200:
            _record_ok()
            _note_quota(path, r.headers)
            return r.json()
        if r.status_code == 429:
            # Đúng theo tài liệu: vượt rate limit → 429. TÔN TRỌNG Retry-After thay vì
            # cứ thử lại (thử lại dồn dập chính là thứ khiến IP bị firewall chặn).
            _backoff_429(r.headers)
            print(f"⚠️  DNSE 429 {path}: {r.text[:120]}", flush=True)
            return None
        if r.status_code >= 500:
            _record_fail()
        print(f"⚠️  DNSE GET {path} → {r.status_code}: {r.text[:140]}", flush=True)
    except requests.exceptions.RequestException as e:
        _record_fail()       # timeout / không kết nối được → tính vào breaker
        print(f"⚠️  DNSE GET {path} err: {type(e).__name__}: {e}", flush=True)
    return None


# ── Các endpoint market data (kiểm chứng tham số/đơn vị khi có key) ──
def get_ohlc(symbol: str, resolution: str = "1D", from_ts: int = 0, to_ts: int = 0,
             bar_type: str = "STOCK"):
    """Nến lịch sử. from_ts/to_ts = unix seconds. resolution: '1','5','15','60','1D','1W'."""
    return _get("/price/ohlc", {
        "type": bar_type, "symbol": symbol.upper(),
        "resolution": resolution, "from": int(from_ts), "to": int(to_ts),
    })


def get_ohlc_history(symbol: str, start_date: str, end_date: str, resolution: str = "1D"):
    """Nến ngày lịch sử → list {date, open, high, low, close, volume} (giá kVND).
    start_date/end_date = 'YYYY-MM-DD'. Thay vnstock Quote.history."""
    if not enabled():
        return None
    from datetime import datetime
    try:
        frm = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        to = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp()) + 86400
    except (ValueError, TypeError):
        return None
    r = get_ohlc(symbol, resolution=resolution, from_ts=frm, to_ts=to)
    if not r or not r.get("t"):
        return None
    t, o, h, low, c, v = (r.get(k) or [] for k in ("t", "o", "h", "l", "c", "v"))
    out = []
    for i in range(len(t)):
        if t[i] is None:
            continue
        out.append({
            "date": datetime.fromtimestamp(t[i]).strftime("%Y-%m-%d"),
            "open": o[i], "high": h[i], "low": low[i], "close": c[i], "volume": v[i],
        })
    return out or None


def get_trades(symbol: str, board_id: Optional[str] = None, from_date: int = 0,
               to_date: int = 0, limit: int = 500, order: str = "DESC",
               next_page_token: Optional[str] = None):
    """Khớp lệnh (tick) trong khoảng thời gian. Range tick tối đa ~1 phiên."""
    return _get(f"/price/{symbol.upper()}/trades", {
        "boardId": board_id, "from": from_date or None, "to": to_date or None,
        "limit": limit, "order": order, "nextPageToken": next_page_token,
    })


def _norm_side(s) -> str:
    s = str(s).upper()
    return "B" if s.startswith("B") else ("S" if s.startswith("S") else "U")


def get_intraday_ticks(symbol: str, max_ticks: int = 3000, max_pages: int = 8,
                       date: Optional[str] = None):
    """Lấy tick khớp lệnh của 1 phiên, chuẩn hoá cho shark_monitor.
    Trả list {id, ts, price(kVND), volume, side('B'/'S'), value(VND)} tăng dần theo thời gian.

    date=None → phiên hôm nay; date='YYYY-MM-DD' → phiên ngày đó (để dựng lại tape cũ).
    LƯU Ý: get_trades trả DESC (mới→cũ) và phân trang bằng nextPageToken. Muốn ĐỦ CẢ
    PHIÊN phải phân trang tới khi hết token — đừng đặt max_ticks/max_pages quá nhỏ, nếu
    không mã thanh khoản cao (STB…) sẽ chỉ còn nửa phiên cuối (mất phần sáng).
    """
    if not enabled():
        return None
    import time as _t
    from datetime import datetime, timedelta
    if date:
        d0 = datetime.strptime(date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
        start = int(d0.timestamp())
        now = int((d0 + timedelta(days=1)).timestamp())
    else:
        now = int(_t.time())
        start = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    rows: list = []
    token = None
    for _ in range(max_pages):
        r = get_trades(symbol, from_date=start, to_date=now, limit=500, order="DESC",
                       next_page_token=token)
        if not r:
            break
        trades = r.get("trades") or []
        for t in trades:
            try:
                price = float(t["matchPrice"])
                vol = int(t["matchQtty"]) * VOL_LOT   # lô 10 → cổ phiếu
            except (TypeError, KeyError, ValueError):
                continue
            tvt = t.get("totalVolumeTraded")
            rows.append({
                "id": str(tvt) if tvt is not None else f"{t.get('time')}_{price}_{vol}",
                "ts": str(t.get("time", "")),
                "price": price,                 # kVND (giống vnstock)
                "volume": vol,
                "side": _norm_side(t.get("side")),
                "value": vol * price * 1000.0,  # VND
            })
        token = r.get("nextPageToken")
        if not token or not trades or len(rows) >= max_ticks:
            break
    rows.sort(key=lambda x: x["ts"])

    # DNSE trả TỪNG khớp lẻ → gộp các khớp CÙNG chiều trong cửa sổ ngắn (một lệnh chủ
    # động quét sổ thường khớp thành chuỗi fill cách nhau <150ms) thành 1 "lệnh" để
    # nhận diện lệnh lớn cho đúng (giống tape FireAnt).
    from datetime import datetime as _dt

    def _p(ts):
        try:
            return _dt.fromisoformat(ts)
        except (ValueError, TypeError):
            return None

    agg: list = []
    last_dt = None
    for r in rows:
        rdt = _p(r["ts"])
        if (agg and agg[-1]["side"] == r["side"] and last_dt and rdt
                and 0 <= (rdt - last_dt).total_seconds() * 1000 <= AGG_WINDOW_MS):
            a = agg[-1]
            a["volume"] += r["volume"]
            a["value"] += r["value"]
            a["price"] = a["value"] / (a["volume"] * 1000.0) if a["volume"] else r["price"]
            a["ts"] = r["ts"]
        else:
            agg.append(dict(r))
        last_dt = rdt
    return agg


def get_foreign_trading(symbol: str, board_id: Optional[str] = None, from_date: int = 0,
                        to_date: int = 0, limit: int = 100, order: str = "DESC"):
    """Giao dịch khối ngoại theo ngày (thay FireAnt cho phần foreign nếu muốn)."""
    return _get(f"/price/{symbol.upper()}/foreign-trading", {
        "boardId": board_id, "from": from_date or None, "to": to_date or None,
        "limit": limit, "order": order,
    })


def get_quotes(symbol: str, board_id: Optional[str] = None, from_date: int = 0,
               to_date: int = 0, limit: int = 1, order: str = "DESC"):
    """Sổ lệnh (bid/ask top price) — snapshot gần nhất."""
    return _get(f"/price/{symbol.upper()}/quotes", {
        "boardId": board_id, "from": from_date or None, "to": to_date or None,
        "limit": limit, "order": order,
    })


def get_instruments(symbol: str = "", market_id: str = "", security_group_id: str = "",
                    limit: int = 1000, page: int = 1):
    """Danh sách mã (thay vnstock Listing)."""
    return _get("/instruments", {
        "symbol": symbol or None, "marketId": market_id or None,
        "securityGroupId": security_group_id or None, "limit": limit, "page": page,
    })


# marketId DNSE → sàn
_EXCH_MAP = {"STO": "HOSE", "STX": "HNX", "UPX": "UPCOM"}


_stocklist_cache = None
_stocklist_ts = 0.0
_STOCKLIST_TTL = 86400.0   # danh sách mã đổi rất chậm → cache 1 ngày


def get_stock_list():
    """Danh sách cổ phiếu HOSE/HNX/UPCOM → [{ticker, name, exchange}] (thay vnstock Listing)."""
    global _stocklist_cache, _stocklist_ts
    if not enabled():
        return None
    if _stocklist_cache and time.time() - _stocklist_ts < _STOCKLIST_TTL:
        return _stocklist_cache
    out: list = []
    seen: set = set()
    for pg in range(1, 8):
        r = get_instruments(limit=1000, page=pg)
        data = r.get("data", []) if r else []
        if not data:
            break
        for x in data:
            if x.get("securityGroupId") != "ST":
                continue
            exch = _EXCH_MAP.get(x.get("marketId"))
            tk = str(x.get("symbol", "")).strip().upper()
            if not exch or not tk or tk in seen:
                continue
            seen.add(tk)
            out.append({"ticker": tk, "exchange": exch,
                        "name": str(x.get("name") or x.get("shortName") or "").strip()})
        if len(data) < 1000:
            break
    if out:
        _stocklist_cache = out
        _stocklist_ts = time.time()
    return out or None


def get_tickers_by_exchange(exchange: str):
    """Danh sách mã của 1 sàn (HOSE/HNX/UPCOM) → [ticker]. Thay Listing().symbols_by_exchange().

    Ưu tiên bảng stock_list đã lưu SQLite: danh sách mã đổi rất chậm (vài lần/năm) nên
    không đáng tốn quota "Get Instruments" mỗi lần khởi động lại container.
    Cập nhật lại qua Admin → "Danh sách mã CK".
    """
    ex = exchange.upper()
    try:
        from app.services import ohlcv_store
        saved = ohlcv_store.get_stock_list()
        tks = [x["ticker"] for x in saved if str(x.get("exchange", "")).upper() == ex]
        if tks:
            return tks
    except Exception:  # noqa: BLE001
        pass          # chưa có bảng/dữ liệu → lấy từ API

    lst = get_stock_list()
    if not lst:
        return None
    return [x["ticker"] for x in lst if x["exchange"] == ex]


_ob_cache: dict = {}   # ticker -> (ts, orderbook)
_OB_TTL = 4.0


def get_orderbook(symbol: str):
    """Sổ lệnh mới nhất: {bid:[{price,quantity}], offer:[...], time}. Cache ~4s."""
    if not enabled():
        return None
    tk = symbol.upper()
    now = time.time()
    hit = _ob_cache.get(tk)
    if hit and now - hit[0] < _OB_TTL:
        return hit[1]
    n = int(now)
    r = get_quotes(symbol, from_date=n - 20 * 3600, to_date=n, limit=1, order="DESC")
    ob = None
    if r and r.get("quotes"):
        q = r["quotes"][0]

        def _lv(rows):
            # quantity cũng theo LÔ 10 như matchQtty (quan sát: 10.590/37.160/17.720
            # không chia hết 100 — bất khả thi nếu là cổ phiếu vì sàn chỉ nhận lô 100
            # — nhưng đều chia hết 10) → quy về cổ phiếu.
            out = []
            for l in (rows or []):
                p, v = l.get("price"), l.get("quantity", l.get("qtty"))
                if p is None or v is None:
                    continue
                out.append({"price": float(p), "quantity": int(v) * VOL_LOT})
            return out

        ob = {"bid": _lv(q.get("bid")), "offer": _lv(q.get("offer")), "time": q.get("time")}
        _ob_cache[tk] = (now, ob)
    return ob
