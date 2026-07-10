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


def _key() -> str:
    return os.environ.get("DNSE_API_KEY", "").strip()


def _secret() -> str:
    return os.environ.get("DNSE_API_SECRET", "").strip()


def enabled() -> bool:
    return bool(_key() and _secret())


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
    return {"Date": date_value, "X-Signature": xsig, "version": API_VERSION,
            "Accept": "application/json"}


def _get(path: str, params: Optional[dict] = None):
    """GET có ký. Ký theo request-target = PATH (không gồm query) — đúng như SDK."""
    if not enabled():
        return None
    qs = urlencode({k: v for k, v in (params or {}).items() if v is not None})
    full = f"{REST_BASE}{path}" + (f"?{qs}" if qs else "")
    sign_path = urlparse(full).path
    try:
        r = requests.get(full, headers=_sign("GET", sign_path), timeout=20)
        if r.status_code == 200:
            return r.json()
        print(f"⚠️  DNSE GET {path} → {r.status_code}: {r.text[:140]}", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"⚠️  DNSE GET {path} err: {type(e).__name__}: {e}", flush=True)
    return None


# ── Các endpoint market data (kiểm chứng tham số/đơn vị khi có key) ──
def get_ohlc(symbol: str, resolution: str = "1D", from_ts: int = 0, to_ts: int = 0,
             bar_type: str = "STOCK"):
    """Nến lịch sử. from_ts/to_ts = unix seconds. resolution: '1','5','15','60','1D','1W'."""
    return _get("/price/ohlc", {
        "bar_type": bar_type, "symbol": symbol.upper(),
        "resolution": resolution, "from": int(from_ts), "to": int(to_ts),
    })


def get_trades(symbol: str, board_id: Optional[str] = None, from_date: int = 0,
               to_date: int = 0, limit: int = 500, order: str = "DESC"):
    """Khớp lệnh lịch sử (seed đầu phiên cho shark)."""
    return _get(f"/price/{symbol.upper()}/trades", {
        "boardId": board_id, "from": from_date or None, "to": to_date or None,
        "limit": limit, "order": order,
    })


def get_foreign_trading(symbol: str, board_id: Optional[str] = None, from_date: int = 0,
                        to_date: int = 0, limit: int = 100, order: str = "DESC"):
    """Giao dịch khối ngoại theo ngày (thay FireAnt cho phần foreign nếu muốn)."""
    return _get(f"/price/{symbol.upper()}/foreign-trading", {
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
