"""SQLite-based OHLCV store.

Lưu dữ liệu lịch sử OHLCV theo ticker+date để các thuật toán phân tích
(screener, alert_engine) không phải gọi lại vnstock mỗi lần.

- WAL mode: concurrent read while write
- UPSERT: idempotent với dữ liệu cũ (daily update gọi lại không sinh trùng)
- File path: /app/data/ohlcv.db (Docker volume)
"""
import json
import os
import sqlite3
import threading
from datetime import datetime
from typing import List, Optional, Dict, Any

import pandas as pd

DB_DIR  = os.environ.get("OHLCV_DB_DIR", "/app/data")
DB_PATH = os.path.join(DB_DIR, "ohlcv.db")

_lock = threading.Lock()


def _connect() -> sqlite3.Connection:
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Khởi tạo schema lần đầu (idempotent)."""
    with _lock, _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS ohlcv (
                ticker TEXT NOT NULL,
                date   TEXT NOT NULL,
                open   REAL,
                high   REAL,
                low    REAL,
                close  REAL,
                volume INTEGER,
                PRIMARY KEY (ticker, date)
            );
            CREATE INDEX IF NOT EXISTS idx_ohlcv_date ON ohlcv(date);

            CREATE TABLE IF NOT EXISTS backfill_status (
                ticker     TEXT PRIMARY KEY,
                first_date TEXT,
                last_date  TEXT,
                row_count  INTEGER,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS backfill_job (
                id          TEXT PRIMARY KEY,
                scope       TEXT,
                start_date  TEXT,
                end_date    TEXT,
                total       INTEGER,
                completed   INTEGER,
                failed      INTEGER,
                status      TEXT,    -- pending/running/done/cancelled/error
                message     TEXT,
                started_at  TEXT,
                finished_at TEXT
            );

            CREATE TABLE IF NOT EXISTS fundamentals (
                ticker      TEXT PRIMARY KEY,
                data_json   TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS rs_ratings (
                ticker      TEXT PRIMARY KEY,
                rs_score    REAL NOT NULL,   -- raw weighted return score
                rs_rating   REAL NOT NULL,   -- percentile rank (1-99)
                rank        INTEGER NOT NULL,
                total       INTEGER NOT NULL,
                updated_at  TEXT NOT NULL
            );
            """
        )
    print(f"✅ OHLCV DB ready at {DB_PATH}")


# ── OHLCV CRUD ────────────────────────────────────────────────────────────
def upsert_ohlcv(ticker: str, df: pd.DataFrame) -> int:
    """Upsert toàn bộ rows từ df. df phải có: date, open, high, low, close, volume.
    date có thể là pd.Timestamp hoặc str. Trả về số rows đã ghi.
    """
    if df is None or df.empty:
        return 0
    t = ticker.upper()
    # Chuẩn hoá date → 'YYYY-MM-DD' str
    d = df.copy()
    if "date" not in d.columns:
        # vnstock đôi khi dùng 'time'
        if "time" in d.columns:
            d = d.rename(columns={"time": "date"})
        else:
            d = d.reset_index().rename(columns={"index": "date"})
    d["date"] = pd.to_datetime(d["date"]).dt.strftime("%Y-%m-%d")
    for col in ("open", "high", "low", "close"):
        if col not in d.columns:
            return 0
    if "volume" not in d.columns:
        d["volume"] = 0

    rows = [
        (
            t, r["date"],
            float(r["open"]) if pd.notna(r["open"]) else None,
            float(r["high"]) if pd.notna(r["high"]) else None,
            float(r["low"])  if pd.notna(r["low"])  else None,
            float(r["close"]) if pd.notna(r["close"]) else None,
            int(r["volume"])  if pd.notna(r["volume"]) else 0,
        )
        for _, r in d.iterrows()
    ]
    with _lock, _connect() as conn:
        conn.executemany(
            """INSERT INTO ohlcv(ticker,date,open,high,low,close,volume)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(ticker,date) DO UPDATE SET
                 open=excluded.open, high=excluded.high, low=excluded.low,
                 close=excluded.close, volume=excluded.volume""",
            rows,
        )
        # Cập nhật status
        cur = conn.execute(
            "SELECT MIN(date), MAX(date), COUNT(*) FROM ohlcv WHERE ticker=?",
            (t,),
        )
        first, last, cnt = cur.fetchone()
        conn.execute(
            """INSERT INTO backfill_status(ticker,first_date,last_date,row_count,updated_at)
               VALUES (?,?,?,?,?)
               ON CONFLICT(ticker) DO UPDATE SET
                 first_date=excluded.first_date,
                 last_date=excluded.last_date,
                 row_count=excluded.row_count,
                 updated_at=excluded.updated_at""",
            (t, first, last, cnt, datetime.now().isoformat()),
        )
    return len(rows)


def get_ohlcv(
    ticker: str, start: Optional[str] = None, end: Optional[str] = None
) -> Optional[pd.DataFrame]:
    """Đọc OHLCV [start,end] cho 1 ticker. Trả về DataFrame cột
    date/open/high/low/close/volume, sort tăng dần theo date. None nếu rỗng.
    """
    t = ticker.upper()
    q = "SELECT date,open,high,low,close,volume FROM ohlcv WHERE ticker=?"
    args: List[Any] = [t]
    if start:
        q += " AND date>=?"; args.append(start)
    if end:
        q += " AND date<=?"; args.append(end)
    q += " ORDER BY date ASC"
    with _connect() as conn:
        df = pd.read_sql_query(q, conn, params=args)
    if df.empty:
        return None
    return df


def get_last_date(ticker: str) -> Optional[str]:
    with _connect() as conn:
        row = conn.execute(
            "SELECT last_date FROM backfill_status WHERE ticker=?",
            (ticker.upper(),),
        ).fetchone()
    return row["last_date"] if row else None


def get_last_close(ticker: str) -> Optional[float]:
    """Lấy giá close của phiên giao dịch cuối cùng đang lưu trong SQLite."""
    t = ticker.upper()
    with _connect() as conn:
        row = conn.execute(
            "SELECT close FROM ohlcv WHERE ticker=? ORDER BY date DESC LIMIT 1",
            (t,),
        ).fetchone()
    return float(row["close"]) if row and row["close"] else None


def delete_ohlcv(ticker: str) -> int:
    """Xoá toàn bộ OHLCV rows + backfill_status của 1 ticker (để re-fetch)."""
    t = ticker.upper()
    with _lock, _connect() as conn:
        n = conn.execute("DELETE FROM ohlcv WHERE ticker=?", (t,)).rowcount
        conn.execute("DELETE FROM backfill_status WHERE ticker=?", (t,))
    return n


def list_tickers() -> List[str]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT ticker FROM backfill_status ORDER BY ticker"
        ).fetchall()
    return [r["ticker"] for r in rows]


def get_stats() -> Dict[str, Any]:
    with _connect() as conn:
        t_cnt = conn.execute("SELECT COUNT(*) c FROM backfill_status").fetchone()["c"]
        r_cnt = conn.execute("SELECT COUNT(*) c FROM ohlcv").fetchone()["c"]
        rng   = conn.execute(
            "SELECT MIN(first_date) a, MAX(last_date) b FROM backfill_status"
        ).fetchone()
    size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
    return {
        "db_path":      DB_PATH,
        "db_size_mb":   round(size / 1024 / 1024, 2),
        "ticker_count": t_cnt,
        "row_count":    r_cnt,
        "date_min":     rng["a"],
        "date_max":     rng["b"],
    }


# ── Job tracking ──────────────────────────────────────────────────────────
def create_job(job_id: str, scope: str, start: str, end: str, total: int) -> None:
    with _lock, _connect() as conn:
        conn.execute(
            """INSERT INTO backfill_job(id,scope,start_date,end_date,total,
                 completed,failed,status,started_at)
               VALUES (?,?,?,?,?,0,0,'running',?)""",
            (job_id, scope, start, end, total, datetime.now().isoformat()),
        )


def update_job(
    job_id: str,
    completed: Optional[int] = None,
    failed: Optional[int] = None,
    status: Optional[str] = None,
    message: Optional[str] = None,
) -> None:
    sets, args = [], []
    if completed is not None: sets.append("completed=?"); args.append(completed)
    if failed    is not None: sets.append("failed=?");    args.append(failed)
    if status    is not None:
        sets.append("status=?"); args.append(status)
        if status in ("done", "cancelled", "error"):
            sets.append("finished_at=?"); args.append(datetime.now().isoformat())
    if message   is not None: sets.append("message=?");   args.append(message)
    if not sets:
        return
    args.append(job_id)
    with _lock, _connect() as conn:
        conn.execute(f"UPDATE backfill_job SET {','.join(sets)} WHERE id=?", args)


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM backfill_job WHERE id=?", (job_id,)).fetchone()
    return dict(row) if row else None


def list_jobs(limit: int = 20) -> List[Dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM backfill_job ORDER BY started_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


# ── Fundamentals (EPS/ROE) ────────────────────────────────────────────────
FUND_EXPIRY_DAYS = 7  # Dữ liệu fundamental được coi là "stale" sau 7 ngày


def upsert_fundamental(ticker: str, data: Dict[str, Any]) -> None:
    """Lưu fundamental data (EPS/ROE) vào SQLite. data là dict trả về từ _fetch_fundamental_via_api."""
    t = ticker.upper()
    now = datetime.now().isoformat()
    with _lock, _connect() as conn:
        conn.execute(
            """INSERT INTO fundamentals(ticker, data_json, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(ticker) DO UPDATE SET
                 data_json=excluded.data_json,
                 updated_at=excluded.updated_at""",
            (t, json.dumps(data, ensure_ascii=False), now),
        )


def get_fundamental(ticker: str) -> Optional[Dict[str, Any]]:
    """Đọc fundamental data từ SQLite. Trả về dict hoặc None nếu chưa có."""
    t = ticker.upper()
    with _connect() as conn:
        row = conn.execute(
            "SELECT data_json, updated_at FROM fundamentals WHERE ticker=?", (t,)
        ).fetchone()
    if not row:
        return None
    data = json.loads(row["data_json"])
    data["_updated_at"] = row["updated_at"]
    return data


def is_fundamental_stale(ticker: str) -> bool:
    """Kiểm tra fundamental data có cần cập nhật không (>FUND_EXPIRY_DAYS ngày)."""
    t = ticker.upper()
    with _connect() as conn:
        row = conn.execute(
            "SELECT updated_at FROM fundamentals WHERE ticker=?", (t,)
        ).fetchone()
    if not row:
        return True
    days = (datetime.now() - datetime.fromisoformat(row["updated_at"])).total_seconds() / 86400
    return days > FUND_EXPIRY_DAYS


def list_stale_fundamentals(tickers: Optional[List[str]] = None) -> List[str]:
    """Trả về danh sách ticker cần cập nhật fundamental.
    Nếu tickers=None, kiểm tra tất cả ticker trong OHLCV store."""
    if tickers is None:
        tickers = list_tickers()
    return [t for t in tickers if is_fundamental_stale(t)]


def get_fundamental_stats() -> Dict[str, Any]:
    """Thống kê fundamentals table."""
    with _connect() as conn:
        total = conn.execute("SELECT COUNT(*) c FROM fundamentals").fetchone()["c"]
        oldest = conn.execute("SELECT MIN(updated_at) m FROM fundamentals").fetchone()["m"]
        newest = conn.execute("SELECT MAX(updated_at) m FROM fundamentals").fetchone()["m"]
    return {"total": total, "oldest_update": oldest, "newest_update": newest}


# ── RS RATINGS ────────────────────────────────────────────────────────────────

def upsert_rs_ratings(ratings: List[Dict[str, Any]]) -> int:
    """Bulk upsert RS ratings (toàn bộ thị trường, chạy mỗi đêm)."""
    if not ratings:
        return 0
    now = datetime.now().isoformat()
    rows = [(r["ticker"], r["rs_score"], r["rs_rating"], r["rank"], r["total"], now)
            for r in ratings]
    with _lock, _connect() as conn:
        conn.executemany(
            """INSERT OR REPLACE INTO rs_ratings (ticker, rs_score, rs_rating, rank, total, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            rows
        )
    return len(rows)


def get_rs_rating(ticker: str) -> Optional[Dict[str, Any]]:
    """Lấy RS Rating cho 1 mã."""
    with _connect() as conn:
        row = conn.execute(
            "SELECT rs_score, rs_rating, rank, total, updated_at FROM rs_ratings WHERE ticker = ?",
            (ticker.upper(),)
        ).fetchone()
    if not row:
        return None
    return {
        "rs_score": row["rs_score"],
        "rs_rating": row["rs_rating"],
        "rank": row["rank"],
        "total": row["total"],
        "updated_at": row["updated_at"],
    }


def get_all_rs_ratings() -> Dict[str, float]:
    """Trả về dict {ticker: rs_rating} cho toàn bộ thị trường."""
    with _connect() as conn:
        rows = conn.execute("SELECT ticker, rs_rating FROM rs_ratings").fetchall()
    return {row["ticker"]: row["rs_rating"] for row in rows}


def is_rs_ratings_stale() -> bool:
    """Kiểm tra xem RS Ratings đã cũ chưa (>24h hoặc chưa có)."""
    with _connect() as conn:
        row = conn.execute("SELECT MAX(updated_at) m FROM rs_ratings").fetchone()
    if not row or not row["m"]:
        return True
    from datetime import datetime as dt
    last_update = dt.fromisoformat(row["m"])
    return (dt.now() - last_update).total_seconds() > 86400  # >24h
