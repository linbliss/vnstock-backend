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
        # Smart Money EVENTS (Phase A schema) — đơn vị để backtest/benchmark từng pattern
        # theo NGỮ CẢNH. Nhiều dòng/mã/ngày; algo_version để so phiên bản thuật toán.
        c.execute(
            """CREATE TABLE IF NOT EXISTS smart_money_events(
                 ticker       TEXT NOT NULL,
                 trade_date   TEXT NOT NULL,
                 ts           TEXT NOT NULL,
                 type         TEXT NOT NULL,
                 strength     REAL,
                 confidence   REAL,
                 context_json TEXT,
                 evidence_json TEXT,
                 algo_version INTEGER DEFAULT 1,
                 updated_at   TEXT
               )"""
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_sme_tk_date "
            "ON smart_money_events(ticker, trade_date, algo_version)"
        )
    _inited = True


def save_events(ticker: str, trade_date: str, events: list, algo_version: int = 1) -> int:
    """Ghi ĐÈ toàn bộ event của (mã, ngày, algo_version) — idempotent khi tính lại phiên.
    events = list dict {type, ts, strength, confidence, context, evidence}."""
    init_db()
    tk = ticker.upper()
    now = datetime.now().isoformat()
    with _lock, _conn() as c:
        c.execute("DELETE FROM smart_money_events WHERE ticker=? AND trade_date=? AND algo_version=?",
                  (tk, trade_date, algo_version))
        c.executemany(
            """INSERT INTO smart_money_events(ticker, trade_date, ts, type, strength,
                 confidence, context_json, evidence_json, algo_version, updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            [(tk, trade_date, e.get("ts", ""), e.get("type", ""),
              e.get("strength"), e.get("confidence"),
              json.dumps(e.get("context") or {}, separators=(",", ":"), ensure_ascii=False),
              json.dumps(e.get("evidence") or [], separators=(",", ":"), ensure_ascii=False),
              algo_version, now)
             for e in events],
        )
    return len(events)


def load_events(ticker: str, trade_date: str, algo_version: int = 1) -> list:
    """Đọc event đã lưu cho (mã, ngày, algo_version)."""
    init_db()
    with _lock, _conn() as c:
        rows = c.execute(
            "SELECT ts, type, strength, confidence, context_json, evidence_json "
            "FROM smart_money_events WHERE ticker=? AND trade_date=? AND algo_version=? "
            "ORDER BY ts ASC",
            (ticker.upper(), trade_date, algo_version),
        ).fetchall()
    out = []
    for ts, typ, stg, conf, cj, ej in rows:
        try:
            ctx = json.loads(cj) if cj else {}
            ev = json.loads(ej) if ej else []
        except (ValueError, TypeError):
            ctx, ev = {}, []
        out.append({"ts": ts, "type": typ, "strength": stg, "confidence": conf,
                    "context": ctx, "evidence": ev})
    return out


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


def cleanup(keep_days: int = 5, keep_score_days: int = 400) -> int:
    """Dọn dữ liệu cũ. TÁCH BẠCH hai loại:
      • intraday_tape: NẶNG (vài MB/mã/phiên) → chỉ giữ vài ngày.
      • shark_score:   NHẸ (vài KB) và là DỮ LIỆU GỐC ĐỂ BACKTEST → giữ rất lâu.
    Trước đây xoá cả hai sau 5 ngày ⇒ không bao giờ tích được mẫu để đo hiệu quả."""
    init_db()
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    s_cutoff = (datetime.now() - timedelta(days=keep_score_days)).strftime("%Y-%m-%d")
    with _lock, _conn() as c:
        cur = c.execute("DELETE FROM intraday_tape WHERE trade_date < ?", (cutoff,))
        c.execute("DELETE FROM shark_score WHERE trade_date < ?", (s_cutoff,))
        return cur.rowcount


def all_scores(min_date: str | None = None) -> list:
    """Toàn bộ điểm Shark đã lưu → [{ticker, date, score, label, v}] (cho backtest)."""
    init_db()
    q = "SELECT ticker, trade_date, signal_json FROM shark_score"
    args: list = []
    if min_date:
        q += " WHERE trade_date >= ?"
        args.append(min_date)
    q += " ORDER BY trade_date, ticker"
    with _lock, _conn() as c:
        rows = c.execute(q, args).fetchall()
    out = []
    for tk, d, js in rows:
        try:
            sig = json.loads(js)
        except (ValueError, TypeError):
            continue
        if sig.get("empty"):
            continue
        out.append({"ticker": tk, "date": d, "score": sig.get("score", 0),
                    "label": sig.get("label", ""), "v": sig.get("_v")})
    return out
