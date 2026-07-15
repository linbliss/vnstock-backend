"""data_source — cấu hình chọn NGUỒN dữ liệu theo từng module (DNSE / vnstock).
Cho phép chủ động ép nguồn khi DNSE bị chặn IP, không phụ thuộc fallback tự động.
Lưu JSON ở data dir (persist), có cache; sửa qua Admin.

Giá trị mỗi module:
  - "dnse"    : ưu tiên DNSE (kèm fallback vnstock + circuit breaker) — mặc định
  - "vnstock" : ÉP dùng vnstock, KHÔNG đụng DNSE
"""
from __future__ import annotations
import os
import json
import threading

from app.services import dnse_client

# module → nhãn hiển thị (chỉ các module CÓ đường DNSE)
MODULES = {
    "shark": "Shark Action (tape + sổ lệnh trong phiên)",
    "ohlcv": "Lịch sử OHLCV (screener / chart / backfill)",
    "ticker_list": "Danh sách mã CK",
}
VALUES = ("dnse", "vnstock")
DEFAULT = "dnse"

_DIR = os.environ.get("OHLCV_DB_DIR", "/app/data")
_PATH = os.path.join(_DIR, "data_source.json")
_lock = threading.Lock()
_cache: dict | None = None


def _load() -> dict:
    global _cache
    if _cache is None:
        try:
            with open(_PATH) as f:
                _cache = json.load(f)
        except Exception:  # noqa: BLE001
            _cache = {}
    return _cache


def get_source(module: str) -> str:
    return _load().get(module, DEFAULT)


def get_all() -> dict:
    d = _load()
    return {m: d.get(m, DEFAULT) for m in MODULES}


def set_source(module: str, value: str) -> bool:
    global _cache
    if module not in MODULES or value not in VALUES:
        return False
    with _lock:
        d = dict(_load())
        d[module] = value
        os.makedirs(_DIR, exist_ok=True)
        with open(_PATH, "w") as f:
            json.dump(d, f)
        _cache = d
    return True


def use_dnse(module: str) -> bool:
    """True nếu module được phép dùng DNSE (không bị ép vnstock, có key, không bị breaker)."""
    if get_source(module) == "vnstock":
        return False
    return dnse_client.enabled()
