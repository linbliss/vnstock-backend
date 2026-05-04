"""SQLite-based user data store.

Lưu dữ liệu người dùng: users, broker_accounts, trades, watchlists, watchlist_items, user_settings.
Thay thế Supabase REST API trong alert_engine và các routers.

- WAL mode: concurrent read while write
- Thread-safe: dùng threading.Lock + context manager
- File path: /app/data/user_data.db (Docker volume, cùng thư mục ohlcv.db)
"""
import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any


def _now() -> str:
    """ISO-8601 với milliseconds + Z — JavaScript new Date() parse được."""
    t = datetime.now(timezone.utc)
    return t.strftime('%Y-%m-%dT%H:%M:%S.') + f"{t.microsecond // 1000:03d}Z"

DB_DIR  = os.environ.get("USER_DB_DIR", "/app/data")
DB_PATH = os.path.join(DB_DIR, "user_data.db")

_lock = threading.Lock()


def _connect() -> sqlite3.Connection:
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Khởi tạo schema lần đầu (idempotent)."""
    with _lock, _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id           TEXT PRIMARY KEY,
                email        TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS broker_accounts (
                id             TEXT PRIMARY KEY,
                user_id        TEXT NOT NULL,
                name           TEXT NOT NULL DEFAULT '',
                account_name   TEXT NOT NULL DEFAULT '',
                account_number TEXT NOT NULL DEFAULT '',
                broker         TEXT NOT NULL DEFAULT '',
                is_active      INTEGER NOT NULL DEFAULT 1,
                created_at     TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS trades (
                id                TEXT PRIMARY KEY,
                user_id           TEXT NOT NULL,
                broker_account_id TEXT,
                ticker            TEXT NOT NULL,
                exchange          TEXT NOT NULL DEFAULT 'HOSE',
                side              TEXT NOT NULL CHECK(side IN ('BUY','SELL')),
                quantity          REAL NOT NULL,
                price             REAL NOT NULL,
                fee               REAL NOT NULL DEFAULT 0,
                trade_date        TEXT NOT NULL,
                notes             TEXT NOT NULL DEFAULT '',
                source            TEXT NOT NULL DEFAULT 'MANUAL',
                created_at        TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS watchlists (
                id         TEXT PRIMARY KEY,
                user_id    TEXT NOT NULL,
                name       TEXT NOT NULL,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS watchlist_items (
                id           TEXT PRIMARY KEY,
                watchlist_id TEXT NOT NULL,
                ticker       TEXT NOT NULL,
                note         TEXT NOT NULL DEFAULT '',
                alert_price  REAL,
                created_at   TEXT NOT NULL,
                UNIQUE (watchlist_id, ticker),
                FOREIGN KEY (watchlist_id) REFERENCES watchlists(id)
            );

            CREATE TABLE IF NOT EXISTS user_settings (
                user_id    TEXT PRIMARY KEY,
                settings   TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE INDEX IF NOT EXISTS idx_trades_user     ON trades(user_id);
            CREATE INDEX IF NOT EXISTS idx_accounts_user   ON broker_accounts(user_id);
            CREATE INDEX IF NOT EXISTS idx_watchlists_user ON watchlists(user_id);
            CREATE INDEX IF NOT EXISTS idx_wl_items_wl     ON watchlist_items(watchlist_id);
            """
        )

        # Migrations for existing DBs
        try: conn.execute("ALTER TABLE broker_accounts ADD COLUMN account_name TEXT NOT NULL DEFAULT ''")
        except Exception: pass
        try: conn.execute("ALTER TABLE broker_accounts ADD COLUMN account_number TEXT NOT NULL DEFAULT ''")
        except Exception: pass
        try: conn.execute("ALTER TABLE trades ADD COLUMN exchange TEXT NOT NULL DEFAULT 'HOSE'")
        except Exception: pass
        try: conn.execute("ALTER TABLE trades ADD COLUMN fee REAL NOT NULL DEFAULT 0")
        except Exception: pass

    print(f"✅ User DB ready at {DB_PATH}")


# ── Users ─────────────────────────────────────────────────────────────────────

def create_user(email: str, password_hash: str) -> dict:
    uid = uuid.uuid4().hex
    now = _now()
    with _lock, _connect() as conn:
        conn.execute(
            "INSERT INTO users(id, email, password_hash, created_at) VALUES (?,?,?,?)",
            (uid, email.lower().strip(), password_hash, now),
        )
        # Khởi tạo settings mặc định
        conn.execute(
            "INSERT INTO user_settings(user_id, settings, updated_at) VALUES (?,?,?)",
            (uid, "{}", now),
        )
    return {"id": uid, "email": email.lower().strip(), "created_at": now}


def get_user_by_email(email: str) -> Optional[dict]:
    with _connect() as conn:
        row = conn.execute(
            "SELECT id, email, password_hash, created_at FROM users WHERE email=?",
            (email.lower().strip(),),
        ).fetchone()
    return dict(row) if row else None


def get_user_by_id(user_id: str) -> Optional[dict]:
    with _connect() as conn:
        row = conn.execute(
            "SELECT id, email, created_at FROM users WHERE id=?",
            (user_id,),
        ).fetchone()
    return dict(row) if row else None


def get_all_user_ids() -> List[str]:
    with _connect() as conn:
        rows = conn.execute("SELECT user_id FROM user_settings").fetchall()
    return [r["user_id"] for r in rows]


def count_users() -> int:
    with _connect() as conn:
        return conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]


# ── Trades ────────────────────────────────────────────────────────────────────

def get_trades(user_id: str) -> List[dict]:
    with _connect() as conn:
        rows = conn.execute(
            """SELECT id, user_id, broker_account_id, ticker,
                      COALESCE(exchange, 'HOSE') as exchange,
                      side, quantity, price,
                      COALESCE(fee, 0) as fee,
                      trade_date, notes, source, created_at
               FROM trades WHERE user_id=? ORDER BY trade_date DESC, created_at DESC""",
            (user_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def add_trade(user_id: str, data: dict) -> dict:
    tid  = uuid.uuid4().hex
    now  = _now()
    row = (
        tid,
        user_id,
        data.get("broker_account_id") or None,
        data["ticker"].upper(),
        data.get("exchange", "HOSE"),
        data["side"].upper(),
        float(data["quantity"]),
        float(data["price"]),
        float(data.get("fee", 0)),
        data["trade_date"],
        data.get("notes", ""),
        data.get("source", "MANUAL"),
        now,
    )
    with _lock, _connect() as conn:
        conn.execute(
            """INSERT INTO trades(id, user_id, broker_account_id, ticker, exchange, side,
                quantity, price, fee, trade_date, notes, source, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            row,
        )
    return {
        "id": tid, "user_id": user_id,
        "broker_account_id": data.get("broker_account_id"),
        "ticker": data["ticker"].upper(),
        "exchange": data.get("exchange", "HOSE"),
        "side": data["side"].upper(),
        "quantity": float(data["quantity"]),
        "price": float(data["price"]),
        "fee": float(data.get("fee", 0)),
        "trade_date": data["trade_date"],
        "notes": data.get("notes", ""),
        "source": data.get("source", "MANUAL"),
        "created_at": now,
    }


def update_trade(trade_id: str, user_id: str, patch: dict) -> Optional[dict]:
    allowed = {"broker_account_id", "ticker", "exchange", "side", "quantity", "price",
               "fee", "trade_date", "notes", "source"}
    sets, args = [], []
    for k, v in patch.items():
        if k not in allowed:
            continue
        if k == "ticker" and v:
            v = v.upper()
        if k == "side" and v:
            v = v.upper()
        sets.append(f"{k}=?")
        args.append(v)
    if not sets:
        return get_trade_by_id(trade_id, user_id)
    args.extend([trade_id, user_id])
    with _lock, _connect() as conn:
        conn.execute(
            f"UPDATE trades SET {','.join(sets)} WHERE id=? AND user_id=?", args
        )
    return get_trade_by_id(trade_id, user_id)


def delete_trade(trade_id: str, user_id: str) -> bool:
    with _lock, _connect() as conn:
        cur = conn.execute(
            "DELETE FROM trades WHERE id=? AND user_id=?", (trade_id, user_id)
        )
    return cur.rowcount > 0


def delete_all_trades(user_id: str) -> int:
    with _lock, _connect() as conn:
        cur = conn.execute("DELETE FROM trades WHERE user_id=?", (user_id,))
    return cur.rowcount


def get_trade_by_id(trade_id: str, user_id: str) -> Optional[dict]:
    with _connect() as conn:
        row = conn.execute(
            """SELECT id, user_id, broker_account_id, ticker,
                      COALESCE(exchange, 'HOSE') as exchange,
                      side, quantity, price,
                      COALESCE(fee, 0) as fee,
                      trade_date, notes, source, created_at
               FROM trades WHERE id=? AND user_id=?""",
            (trade_id, user_id)
        ).fetchone()
    return dict(row) if row else None


# ── Broker Accounts ───────────────────────────────────────────────────────────

def get_accounts(user_id: str) -> List[dict]:
    with _connect() as conn:
        rows = conn.execute(
            """SELECT id, user_id,
                      CASE WHEN account_name != '' THEN account_name ELSE name END as account_name,
                      account_number, broker, is_active, created_at
               FROM broker_accounts WHERE user_id=? ORDER BY created_at""",
            (user_id,),
        ).fetchall()
    return [{**dict(r), 'is_active': bool(r['is_active'])} for r in rows]


def add_account(user_id: str, data: dict) -> dict:
    aid = uuid.uuid4().hex
    now = _now()
    account_name = data.get("account_name") or data.get("name", "")
    account_number = data.get("account_number", "")
    with _lock, _connect() as conn:
        conn.execute(
            """INSERT INTO broker_accounts(id, user_id, name, account_name, account_number, broker, is_active, created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (aid, user_id, account_name, account_name, account_number,
             data.get("broker", ""), int(data.get("is_active", 1) or 1), now),
        )
    return {
        "id": aid, "user_id": user_id,
        "account_name": account_name, "account_number": account_number,
        "broker": data.get("broker", ""),
        "is_active": bool(data.get("is_active", 1)),
        "created_at": now,
    }


def update_account(acct_id: str, user_id: str, patch: dict) -> Optional[dict]:
    allowed = {"name", "account_name", "account_number", "broker", "is_active"}
    sets, args = [], []
    for k, v in patch.items():
        if k not in allowed:
            continue
        if k == "is_active":
            v = int(v) if v is not None else 1
        sets.append(f"{k}=?")
        args.append(v)
    if not sets:
        return _get_account_by_id(acct_id, user_id)
    args.extend([acct_id, user_id])
    with _lock, _connect() as conn:
        conn.execute(
            f"UPDATE broker_accounts SET {','.join(sets)} WHERE id=? AND user_id=?", args
        )
    return _get_account_by_id(acct_id, user_id)


def delete_account(acct_id: str, user_id: str) -> bool:
    with _lock, _connect() as conn:
        cur = conn.execute(
            "DELETE FROM broker_accounts WHERE id=? AND user_id=?", (acct_id, user_id)
        )
    return cur.rowcount > 0


def _get_account_by_id(acct_id: str, user_id: str) -> Optional[dict]:
    with _connect() as conn:
        row = conn.execute(
            """SELECT id, user_id,
                      CASE WHEN account_name != '' THEN account_name ELSE name END as account_name,
                      account_number, broker, is_active, created_at
               FROM broker_accounts WHERE id=? AND user_id=?""",
            (acct_id, user_id)
        ).fetchone()
    if not row:
        return None
    return {**dict(row), 'is_active': bool(row['is_active'])}


# ── Watchlists ────────────────────────────────────────────────────────────────

def get_watchlists(user_id: str) -> List[dict]:
    """Trả về watchlists kèm items."""
    with _connect() as conn:
        wls = conn.execute(
            """SELECT id, user_id, name, sort_order, created_at
               FROM watchlists WHERE user_id=? ORDER BY sort_order, created_at""",
            (user_id,),
        ).fetchall()
        result = []
        for wl in wls:
            items = conn.execute(
                """SELECT id, watchlist_id, ticker, note, alert_price,
                          created_at AS added_at
                   FROM watchlist_items WHERE watchlist_id=? ORDER BY created_at""",
                (wl["id"],),
            ).fetchall()
            d = dict(wl)
            d["items"] = [dict(i) for i in items]
            result.append(d)
    return result


def add_watchlist(user_id: str, name: str) -> dict:
    wid = uuid.uuid4().hex
    now = _now()
    with _lock, _connect() as conn:
        conn.execute(
            "INSERT INTO watchlists(id, user_id, name, sort_order, created_at) VALUES (?,?,?,0,?)",
            (wid, user_id, name, now),
        )
    return {"id": wid, "user_id": user_id, "name": name, "sort_order": 0,
            "created_at": now, "items": []}


def delete_watchlist(wl_id: str, user_id: str) -> bool:
    with _lock, _connect() as conn:
        # Xóa items trước (foreign key)
        conn.execute("DELETE FROM watchlist_items WHERE watchlist_id=?", (wl_id,))
        cur = conn.execute(
            "DELETE FROM watchlists WHERE id=? AND user_id=?", (wl_id, user_id)
        )
    return cur.rowcount > 0


def rename_watchlist(wl_id: str, user_id: str, name: str) -> bool:
    with _lock, _connect() as conn:
        cur = conn.execute(
            "UPDATE watchlists SET name=? WHERE id=? AND user_id=?",
            (name.strip(), wl_id, user_id)
        )
    return cur.rowcount > 0


# ── Watchlist Items ───────────────────────────────────────────────────────────

def add_watchlist_item(wl_id: str, ticker: str, note: str = "", alert_price: Optional[float] = None) -> dict:
    iid = uuid.uuid4().hex
    now = _now()
    with _lock, _connect() as conn:
        conn.execute(
            """INSERT INTO watchlist_items(id, watchlist_id, ticker, note, alert_price, created_at)
               VALUES (?,?,?,?,?,?)""",
            (iid, wl_id, ticker.upper(), note, alert_price, now),
        )
    return {
        "id": iid, "watchlist_id": wl_id, "ticker": ticker.upper(),
        "note": note, "alert_price": alert_price, "added_at": now,
    }


def update_watchlist_item(item_id: str, patch: dict) -> Optional[dict]:
    allowed = {"note", "alert_price", "ticker"}
    sets, args = [], []
    for k, v in patch.items():
        if k not in allowed:
            continue
        if k == "ticker" and v:
            v = v.upper()
        sets.append(f"{k}=?")
        args.append(v)
    if not sets:
        return _get_wl_item_by_id(item_id)
    args.append(item_id)
    with _lock, _connect() as conn:
        conn.execute(
            f"UPDATE watchlist_items SET {','.join(sets)} WHERE id=?", args
        )
    return _get_wl_item_by_id(item_id)


def delete_watchlist_item(item_id: str) -> bool:
    with _lock, _connect() as conn:
        cur = conn.execute("DELETE FROM watchlist_items WHERE id=?", (item_id,))
    return cur.rowcount > 0


def _get_wl_item_by_id(item_id: str) -> Optional[dict]:
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM watchlist_items WHERE id=?", (item_id,)
        ).fetchone()
    return dict(row) if row else None


def get_all_watchlist_items(user_id: str) -> List[dict]:
    """Lấy tất cả watchlist items của user (qua join watchlists).
    Trả về list[dict] với keys: ticker, alert_price (+ id, watchlist_id, note, created_at).
    Dùng bởi alert_engine._load_watchlist_items().
    """
    with _connect() as conn:
        rows = conn.execute(
            """SELECT wi.id, wi.watchlist_id, wi.ticker, wi.note, wi.alert_price, wi.created_at
               FROM watchlist_items wi
               JOIN watchlists wl ON wl.id = wi.watchlist_id
               WHERE wl.user_id=?
               ORDER BY wi.created_at""",
            (user_id,),
        ).fetchall()
    return [dict(r) for r in rows]


# ── User Settings ─────────────────────────────────────────────────────────────

def get_user_settings(user_id: str) -> dict:
    """Trả về settings dict. Nếu chưa có row, trả về {}.
    Dùng bởi alert_engine._load_settings().
    """
    with _connect() as conn:
        row = conn.execute(
            "SELECT settings FROM user_settings WHERE user_id=?", (user_id,)
        ).fetchone()
    if not row:
        return {}
    try:
        return json.loads(row["settings"])
    except (json.JSONDecodeError, TypeError):
        return {}


def save_user_settings(user_id: str, settings_dict: dict) -> None:
    """Upsert settings cho user.
    Dùng bởi alert_engine._save_settings().
    """
    now = _now()
    with _lock, _connect() as conn:
        conn.execute(
            """INSERT INTO user_settings(user_id, settings, updated_at) VALUES (?,?,?)
               ON CONFLICT(user_id) DO UPDATE SET
                 settings=excluded.settings, updated_at=excluded.updated_at""",
            (user_id, json.dumps(settings_dict, ensure_ascii=False), now),
        )
