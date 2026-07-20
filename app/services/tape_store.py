"""tape_store — cache BỀN VỮNG (SQLite) cho tape khớp lệnh trong phiên.

Mục đích: giảm tải truy cập vnstock/DNSE.
  • Lưu tape đã nạp (đã dedup, đã gộp lệnh) theo khóa (ticker, trade_date).
  • Khởi động lại / deploy KHÔNG mất cache → khỏi nạp lại cả phiên.
  • Phiên đã đóng (complete=1) → ngoài giờ chỉ đọc cache, không gọi API.

Blob = JSON list các tick {id, ts, price, volume, side, value} (giống shark_monitor).
Tách file riêng (intraday_tape.db) để không tranh chấp WAL với OHLCV DB.
"""
from __future__ import annotations
import os
import json
import sqlite3
import threading
from datetime import datetime, timedelta

_DIR = os.environ.get("OHLCV_DB_DIR", "/app/data")
DB_PATH = os.path.join(_DIR, "intraday_tape.db")
_lock = threading.Lock()
_inited = False


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=15)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


def init_db() -> None:
    global _inited
    if _inited:
        return
    os.makedirs(_DIR, exist_ok=True)
    with _lock, _conn() as c:
        c.execute(
            """CREATE TABLE IF NOT EXISTS intraday_tape(
                 ticker      TEXT NOT NULL,
                 trade_date  TEXT NOT NULL,
                 ticks_json  TEXT NOT NULL,
                 last_ts     TEXT,
                 n           INTEGER,
                 complete    INTEGER DEFAULT 0,
                 updated_at  TEXT,
                 PRIMARY KEY(ticker, trade_date)
               )"""
        )
        # Cache ĐIỂM Shark đã tính (không phải cả tape) — để list watchlist/Shark Action
        # khỏi tính lại _metrics trên hàng chục nghìn tick mỗi lần mở. complete=1 = chốt
        # cuối phiên (ngoài giờ đọc thẳng, không tính lại). big_value để biết cache có
        # đúng ngưỡng "lệnh lớn" đang yêu cầu không.
        c.execute(
            """CREATE TABLE IF NOT EXISTS shark_score(
                 ticker      TEXT NOT NULL,
                 trade_date  TEXT NOT NULL,
                 signal_json TEXT NOT NULL,
                 big_value   REAL,
                 complete    INTEGER DEFAULT 0,
                 updated_at  TEXT,
                 PRIMARY KEY(ticker, trade_date)
               )"""
        )
    _inited = True


def save_score(ticker: str, trade_date: str, signal: dict,
               big_value: float, complete: bool = False) -> None:
    """Lưu điểm Shark đã tính cho (mã, ngày)."""
    init_db()
    payload = json.dumps(signal, separators=(",", ":"), ensure_ascii=False)
    with _lock, _conn() as c:
        c.execute(
            """INSERT INTO shark_score(ticker, trade_date, signal_json, big_value, complete, updated_at)
               VALUES(?,?,?,?,?,?)
               ON CONFLICT(ticker, trade_date) DO UPDATE SET
                 signal_json=excluded.signal_json, big_value=excluded.big_value,
                 complete=excluded.complete, updated_at=excluded.updated_at""",
            (ticker.upper(), trade_date, payload, float(big_value),
             1 if complete else 0, datetime.now().isoformat()),
        )


def load_score(ticker: str, trade_date: str) -> dict | None:
    """Trả {signal, big_value, complete} hoặc None."""
    init_db()
    with _lock, _conn() as c:
        row = c.execute(
            "SELECT signal_json, big_value, complete FROM shark_score "
            "WHERE ticker=? AND trade_date=?",
            (ticker.upper(), trade_date),
        ).fetchone()
    if not row:
        return None
    try:
        signal = json.loads(row[0])
    except (ValueError, TypeError):
        return None
    return {"signal": signal, "big_value": row[1], "complete": bool(row[2])}


def load(ticker: str, trade_date: str) -> dict | None:
    """Trả {ticks, last_ts, complete} hoặc None nếu chưa có."""
    init_db()
    with _lock, _conn() as c:
        row = c.execute(
            "SELECT ticks_json, last_ts, complete FROM intraday_tape "
            "WHERE ticker=? AND trade_date=?",
            (ticker.upper(), trade_date),
        ).fetchone()
    if not row:
        return None
    try:
        ticks = json.loads(row[0])
    except (ValueError, TypeError):
        return None
    return {"ticks": ticks, "last_ts": row[1], "complete": bool(row[2])}


def save(ticker: str, trade_date: str, ticks: list, complete: bool = False) -> None:
    """Ghi/ghi đè blob tape cho (mã, ngày)."""
    init_db()
    last_ts = ticks[-1]["ts"] if ticks else ""
    payload = json.dumps(ticks, separators=(",", ":"), ensure_ascii=False)
    with _lock, _conn() as c:
        c.execute(
            """INSERT INTO intraday_tape(ticker, trade_date, ticks_json, last_ts, n, complete, updated_at)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(ticker, trade_date) DO UPDATE SET
                 ticks_json=excluded.ticks_json, last_ts=excluded.last_ts,
                 n=excluded.n, complete=excluded.complete, updated_at=excluded.updated_at""",
            (ticker.upper(), trade_date, payload, last_ts, len(ticks),
             1 if complete else 0, datetime.now().isoformat()),
        )


def last_session_date(ticker: str, max_date: str) -> str | None:
    """Ngày phiên GẦN NHẤT (≤ max_date) có tape của mã — để ngày nghỉ vẫn xem được
    phiên cuối cùng. Mỗi phiên vẫn nằm ở khoá ngày RIÊNG nên không lẫn dữ liệu."""
    init_db()
    with _lock, _conn() as c:
        row = c.execute(
            "SELECT MAX(trade_date) FROM intraday_tape WHERE ticker=? AND trade_date<=?",
            (ticker.upper(), max_date),
        ).fetchone()
    return row[0] if row and row[0] else None


def cleanup(keep_days: int = 5) -> int:
    """Xoá tape cũ hơn keep_days (giữ ổ đĩa gọn). Trả số bản ghi đã xoá."""
    init_db()
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    with _lock, _conn() as c:
        cur = c.execute("DELETE FROM intraday_tape WHERE trade_date < ?", (cutoff,))
        c.execute("DELETE FROM shark_score WHERE trade_date < ?", (cutoff,))
        return cur.rowcount
