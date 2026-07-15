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


def _key() -> str:
    return os.environ.get("DNSE_API_KEY", "").strip()


def _secret() -> str:
    return os.environ.get("DNSE_API_SECRET", "").strip()


# ── Circuit breaker: nếu DNSE unreachable (timeout) nhiều lần → tạm ngắt, dùng fallback ──
_fail_count = 0
_disabled_until = 0.0
_BREAKER_THRESHOLD = 3
_BREAKER_COOLDOWN = 300.0   # giây


def enabled() -> bool:
    if not (_key() and _secret()):
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
            return r.json()
        if r.status_code == 429 or r.status_code >= 500:
            _record_fail()   # rate-limit / lỗi server → tính vào breaker
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


def get_intraday_ticks(symbol: str, max_ticks: int = 3000, max_pages: int = 8):
    """Lấy tick khớp lệnh phiên hôm nay, chuẩn hoá cho shark_monitor.
    Trả list {id, ts, price(kVND), volume, side('B'/'S'), value(VND)} tăng dần theo thời gian.
    """
    if not enabled():
        return None
    import time as _t
    from datetime import datetime
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
                vol = int(t["matchQtty"])
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
    """Danh sách mã của 1 sàn (HOSE/HNX/UPCOM) → [ticker]. Thay Listing().symbols_by_exchange()."""
    lst = get_stock_list()
    if not lst:
        return None
    ex = exchange.upper()
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
        ob = {"bid": q.get("bid") or [], "offer": q.get("offer") or [], "time": q.get("time")}
        _ob_cache[tk] = (now, ob)
    return ob
