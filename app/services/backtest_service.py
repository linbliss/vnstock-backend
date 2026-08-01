"""Backtest service — mô phỏng chiến lược VCP breakout + trailing stop.

Kiến trúc 2 pha để chạy TƯƠNG TÁC được (detect_vcp toàn universe ~30 phút):
  1. build_cache(): quét 1 LẦN, lưu mọi tín hiệu breakout (ticker, ngày vào, giá,
     KLGD-TB 50 phiên trước, đáy nền) vào bảng `breakout_signals` trong ohlcv.db.
  2. run_backtest(params): đọc cache + OHLCV, mô phỏng NHANH (chỉ trailing, không
     detect lại VCP) với tham số chỉnh được → trả summary/yearly/top/equity.

Luật thoát lệnh (mặc định, chỉnh qua params):
  - Trailing stop = max( entry×(1-init_stop),  peak_high×(1-trail) )
    → chưa có lãi: stop = entry×(1-init_stop) (vd -7%);
      đã có lãi (đỉnh vượt entry đủ): trailing peak×(1-trail) (vd -8% dưới đỉnh).
  - intraday=True: gap-down mở dưới stop → khớp tại giá mở cửa; else khớp tại stop.
  - Giữ đến khi vi phạm; lệnh còn mở cuối kỳ → đóng theo close cuối (flag open).
  - 1 vị thế / mã tại 1 thời điểm (bỏ qua tín hiệu mới khi đang mở lệnh mã đó).
"""
from __future__ import annotations

import gzip
import json
import os
import threading
from datetime import datetime
from typing import Callable, Dict, List, Optional, Any

import numpy as np
import pandas as pd

from app.services.screener import detect_vcp
from app.services import ohlcv_store

MIN_HIST = 220
VOL_WINDOW = 50          # số phiên TRƯỚC breakout để tính KLGD trung bình

# ── trạng thái job build cache (giống backfill_job) ───────────────────────
_build_lock = threading.Lock()
_build_state: Dict[str, Any] = {"status": "idle", "done": 0, "total": 0,
                                "signals": 0, "message": "", "at": None}


def _get_df_from_store(ticker: str) -> Optional[pd.DataFrame]:
    df = ohlcv_store.get_ohlcv(ticker)            # DataFrame date/o/h/l/c/v, sort ASC | None
    if df is None or df.empty or "close" not in df:
        return None
    return df.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════
# PHA 1 — quét & cache tín hiệu breakout
# ══════════════════════════════════════════════════════════════════════════
def scan_signals(get_df: Callable[[str], Optional[pd.DataFrame]],
                 tickers: List[str],
                 progress: Optional[Callable[[int, str, int], None]] = None
                 ) -> List[Dict[str, Any]]:
    """Quét breakout transitions. get_df(ticker) -> DataFrame đã sort theo date."""
    sigs: List[Dict[str, Any]] = []
    for i, tk in enumerate(tickers):
        df = get_df(tk)
        if df is None or len(df) < MIN_HIST + 5:
            if progress:
                progress(i, tk, len(sigs))
            continue
        close = df["close"].astype(float).values
        high = df["high"].astype(float).values
        vol = df["volume"].astype(float).values
        dates = df["date"].astype(str).values
        n = len(df)
        prev = None
        for t in range(MIN_HIST, n):
            sub = df.iloc[:t + 1]
            cur = float(close[t])
            try:
                v = detect_vcp(sub, current_price=cur)
                st = v.get("stage_loose", "?")
            except Exception:
                prev = None
                continue
            if st == "breakout" and prev != "breakout":
                v0 = vol[max(0, t - VOL_WINDOW):t]           # KLGD TRƯỚC breakout
                avg_vol = float(np.mean(v0)) if len(v0) else 0.0
                sl = float(v.get("stop_loss", 0) or 0)
                sigs.append({
                    "ticker": tk, "date": str(dates[t])[:10],
                    "entry_close": cur, "entry_high": float(high[t]),
                    "avg_vol50": avg_vol, "base_low": sl,
                })
            prev = st
        if progress:
            progress(i, tk, len(sigs))
    return sigs


def _ensure_table():
    with ohlcv_store._lock, ohlcv_store._connect() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS breakout_signals (
                 ticker      TEXT NOT NULL,
                 date        TEXT NOT NULL,
                 entry_close REAL,
                 entry_high  REAL,
                 avg_vol50   REAL,
                 base_low    REAL,
                 PRIMARY KEY (ticker, date)
               )""")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_sig_date ON breakout_signals(date)")


# Tín hiệu breakout quá khứ KHÔNG đổi → seed sẵn (đã scan local) để server khỏi
# quét lại 30 phút. File nằm trong image (app/seeds/), ngoài volume /app/data.
SEED_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                         "seeds", "breakout_signals_seed.json.gz")
_seed_checked = False


def ensure_seeded() -> None:
    """Nạp seed vào bảng nếu bảng đang RỖNG và có file seed. Chạy 1 lần/process."""
    global _seed_checked
    if _seed_checked:
        return
    _seed_checked = True
    _ensure_table()
    try:
        with ohlcv_store._lock, ohlcv_store._connect() as conn:
            n = conn.execute("SELECT COUNT(*) FROM breakout_signals").fetchone()[0]
            if n > 0 or not os.path.exists(SEED_PATH):
                return
            with gzip.open(SEED_PATH, "rt", encoding="utf-8") as f:
                data = json.load(f)
            conn.executemany(
                """INSERT OR REPLACE INTO breakout_signals
                   (ticker,date,entry_close,entry_high,avg_vol50,base_low)
                   VALUES (?,?,?,?,?,?)""",
                [(s["ticker"], s["date"], s["entry_close"], s["entry_high"],
                  s["avg_vol50"], s["base_low"]) for s in data])
        print(f"✅ Backtest: seeded {len(data)} tín hiệu breakout từ {SEED_PATH}")
    except Exception as e:  # noqa
        print(f"⚠️ Backtest seed lỗi: {e}")


def build_cache(tickers: Optional[List[str]] = None) -> Dict[str, Any]:
    """Quét toàn bộ (hoặc danh sách) mã → ghi bảng breakout_signals. Chạy trong thread."""
    if not _build_lock.acquire(blocking=False):
        return {"ok": False, "message": "Đang chạy build khác"}
    try:
        _ensure_table()
        if tickers is None:
            tickers = ohlcv_store.list_tickers()
        _build_state.update(status="running", done=0, total=len(tickers),
                            signals=0, message="", at=datetime.now().isoformat())

        def prog(i, tk, nsig):
            _build_state.update(done=i + 1, signals=nsig)

        sigs = scan_signals(_get_df_from_store, tickers, progress=prog)
        with ohlcv_store._lock, ohlcv_store._connect() as conn:
            conn.execute("DELETE FROM breakout_signals")
            conn.executemany(
                """INSERT OR REPLACE INTO breakout_signals
                   (ticker,date,entry_close,entry_high,avg_vol50,base_low)
                   VALUES (?,?,?,?,?,?)""",
                [(s["ticker"], s["date"], s["entry_close"], s["entry_high"],
                  s["avg_vol50"], s["base_low"]) for s in sigs])
        _build_state.update(status="done", signals=len(sigs),
                            message=f"{len(sigs)} tín hiệu", at=datetime.now().isoformat())
        return {"ok": True, "signals": len(sigs)}
    except Exception as e:  # noqa
        _build_state.update(status="error", message=str(e))
        return {"ok": False, "message": str(e)}
    finally:
        _build_lock.release()


def start_build_async(tickers: Optional[List[str]] = None) -> Dict[str, Any]:
    if _build_state.get("status") == "running":
        return {"ok": False, "message": "Đang chạy", "state": dict(_build_state)}
    threading.Thread(target=build_cache, args=(tickers,), daemon=True).start()
    return {"ok": True, "message": "Đã bắt đầu build cache"}


def build_status() -> Dict[str, Any]:
    ensure_seeded()
    with ohlcv_store._lock, ohlcv_store._connect() as conn:
        row = conn.execute(
            "SELECT COUNT(*), MIN(date), MAX(date) FROM breakout_signals").fetchone()
    n, mn, mx = (row[0], row[1], row[2]) if row else (0, None, None)
    return {"n_signals": n, "min_date": mn, "max_date": mx, "job": dict(_build_state)}


def get_cached_signals() -> List[Dict[str, Any]]:
    ensure_seeded()
    with ohlcv_store._lock, ohlcv_store._connect() as conn:
        rows = conn.execute(
            "SELECT ticker,date,entry_close,entry_high,avg_vol50,base_low "
            "FROM breakout_signals").fetchall()
    return [dict(r) for r in rows]


# ── LƯU kết quả backtest gần nhất (singleton, xem lại đa thiết bị) ──────────
import json as _json


def _ensure_result_table():
    with ohlcv_store._lock, ohlcv_store._connect() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS backtest_last (
                 id          INTEGER PRIMARY KEY CHECK (id = 1),
                 params_json TEXT NOT NULL,
                 result_json TEXT NOT NULL,
                 run_at      TEXT NOT NULL
               )""")


def save_last_result(params: Dict[str, Any], result: Dict[str, Any]) -> None:
    _ensure_result_table()
    with ohlcv_store._lock, ohlcv_store._connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO backtest_last (id, params_json, result_json, run_at) "
            "VALUES (1, ?, ?, ?)",
            (_json.dumps(params), _json.dumps(result), datetime.now().isoformat()))


def get_last_result() -> Optional[Dict[str, Any]]:
    _ensure_result_table()
    with ohlcv_store._lock, ohlcv_store._connect() as conn:
        row = conn.execute(
            "SELECT params_json, result_json, run_at FROM backtest_last WHERE id = 1").fetchone()
    if not row:
        return None
    return {"params": _json.loads(row["params_json"]),
            "result": _json.loads(row["result_json"]), "run_at": row["run_at"]}


# ══════════════════════════════════════════════════════════════════════════
# PHA 2 — mô phỏng trailing stop (nhanh, tham số hoá)
# ══════════════════════════════════════════════════════════════════════════
DEFAULTS = {
    "min_avg_vol": 300_000,     # KLGD TB 50 phiên trước breakout (cổ phiếu)
    "trail_pct": 0.08,          # trailing 8% dưới đỉnh (khi đã có lãi)
    "init_stop_pct": 0.07,      # stop cứng 7% khi chưa có lãi
    "position_vnd": 10_000_000,  # 10tr / lệnh
    "fee_roundtrip": 0.003,     # 0.3% khứ hồi
    "intraday": True,           # khớp trailing trong phiên (gap-down → giá mở cửa)
    "exchanges": None,          # None = tất cả; hoặc ["HOSE","HNX"]
    "date_from": None,
    "date_to": None,
}


def _simulate_one(df: pd.DataFrame, i0: int, p: Dict[str, Any]) -> Dict[str, Any]:
    """Mô phỏng 1 lệnh từ index i0 (phiên breakout, vào tại close). Trả kết quả."""
    close = df["close"].astype(float).values
    high = df["high"].astype(float).values
    low = df["low"].astype(float).values
    op = df["open"].astype(float).values
    dates = df["date"].astype(str).values
    n = len(df)
    entry = float(close[i0])
    trail, init, intraday = p["trail_pct"], p["init_stop_pct"], p["intraday"]
    # Đỉnh khởi tạo = giá VÀO (mua tại close breakout); high sớm hơn trong phiên đã
    # xảy ra TRƯỚC khi mua nên không tính. → chưa có lãi thì stop = entry×(1-init) = -7%.
    peak = entry
    exit_price, exit_idx, is_open = None, None, False
    for t in range(i0 + 1, n):
        stop = max(entry * (1 - init), peak * (1 - trail))
        if intraday:
            if float(op[t]) <= stop:              # gap-down xuyên stop → khớp giá mở
                exit_price, exit_idx = float(op[t]), t
                break
            if float(low[t]) <= stop:             # chạm stop trong phiên
                exit_price, exit_idx = stop, t
                break
        else:
            if float(close[t]) <= stop:
                exit_price, exit_idx = float(close[t]), t
                break
        peak = max(peak, float(high[t]))          # cập nhật đỉnh SAU khi kiểm tra
    if exit_price is None:                          # chưa thoát → đóng cuối kỳ
        exit_price, exit_idx, is_open = float(close[n - 1]), n - 1, True

    gross = exit_price / entry - 1.0
    fee = p["fee_roundtrip"]
    size = p["position_vnd"]
    sell = size * (exit_price / entry)
    net_pnl = sell - size - size * fee / 2 - sell * fee / 2    # phí 2 chiều
    net_ret = net_pnl / size
    hold = int(exit_idx - i0)
    return {"entry_date": str(dates[i0])[:10], "exit_date": str(dates[exit_idx])[:10],
            "entry": entry, "exit": exit_price, "gross_ret": gross, "net_ret": net_ret,
            "net_pnl": net_pnl, "hold_days": hold, "open": is_open}


def run_backtest(signals: List[Dict[str, Any]],
                 get_df: Callable[[str], Optional[pd.DataFrame]],
                 exch_map: Optional[Dict[str, str]] = None,
                 params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    p = {**DEFAULTS, **(params or {})}
    exch_set = set(p["exchanges"]) if p.get("exchanges") else None
    exch_map = exch_map or {}
    size = p["position_vnd"]

    # lọc + sắp theo ngày để enforce 1-vị-thế/mã
    sig = [s for s in signals if s.get("avg_vol50", 0) >= p["min_avg_vol"]]
    if p.get("date_from"):
        sig = [s for s in sig if s["date"] >= p["date_from"]]
    if p.get("date_to"):
        sig = [s for s in sig if s["date"] <= p["date_to"]]
    if exch_set:
        sig = [s for s in sig if exch_map.get(s["ticker"]) in exch_set]
    sig.sort(key=lambda x: x["date"])

    df_cache: Dict[str, pd.DataFrame] = {}
    open_until: Dict[str, str] = {}
    trades: List[Dict[str, Any]] = []
    skipped_overlap = 0
    for s in sig:
        tk = s["ticker"]
        if tk in open_until and s["date"] <= open_until[tk]:
            skipped_overlap += 1
            continue
        if tk not in df_cache:
            d = get_df(tk)
            df_cache[tk] = d if d is not None else pd.DataFrame()
        df = df_cache[tk]
        if df.empty:
            continue
        locs = np.where(df["date"].astype(str).values == s["date"])[0]
        if len(locs) == 0:
            continue
        r = _simulate_one(df, int(locs[0]), p)
        r["ticker"] = tk
        r["year"] = r["entry_date"][:4]
        trades.append(r)
        open_until[tk] = r["exit_date"]

    return _aggregate(trades, size, p, skipped_overlap)


def _aggregate(trades, size, p, skipped_overlap):
    n = len(trades)
    if n == 0:
        return {"params": p, "summary": {"n_trades": 0}, "pnl": {}, "yearly": [],
                "top": [], "equity": [], "message": "Không có lệnh nào khớp bộ lọc"}
    rets = np.array([t["net_ret"] for t in trades])
    pnls = np.array([t["net_pnl"] for t in trades])
    holds = np.array([t["hold_days"] for t in trades])
    wins = rets > 0
    aw = rets[wins].mean() if wins.any() else 0.0
    al = rets[~wins].mean() if (~wins).any() else 0.0
    payoff = (aw / -al) if al < 0 else float("nan")
    total_inv = n * size
    total_pnl = float(pnls.sum())

    # equity curve theo NGÀY THOÁT (P&L thực hiện luỹ kế) + max drawdown
    ex = sorted(trades, key=lambda t: t["exit_date"])
    cum, eq, peak_eq, maxdd = 0.0, [], 0.0, 0.0
    for t in ex:
        cum += t["net_pnl"]
        peak_eq = max(peak_eq, cum)
        maxdd = min(maxdd, cum - peak_eq)
        eq.append({"date": t["exit_date"], "cum_pnl": round(cum)})

    summary = {
        "n_trades": n, "n_open": int(sum(t["open"] for t in trades)),
        "win_rate": round(float(wins.mean()) * 100, 1),
        "payoff": round(payoff, 2) if payoff == payoff else None,
        "expectancy_pct": round(float(rets.mean()) * 100, 2),
        "avg_win_pct": round(float(aw) * 100, 2), "avg_loss_pct": round(float(al) * 100, 2),
        "avg_hold_days": round(float(holds.mean()), 1),
        "median_hold_days": int(np.median(holds)), "skipped_overlap": skipped_overlap,
    }
    pnl = {
        "total_invested": round(total_inv), "total_net_pnl": round(total_pnl),
        "roi_pct": round(total_pnl / total_inv * 100, 2) if total_inv else 0,
        "n_win": int(wins.sum()), "n_loss": int((~wins).sum()),
        "gross_profit": round(float(pnls[pnls > 0].sum())),
        "gross_loss": round(float(pnls[pnls <= 0].sum())),
        "best_trade": round(float(pnls.max())), "worst_trade": round(float(pnls.min())),
        "max_drawdown": round(float(maxdd)),
    }
    # theo năm (theo năm VÀO lệnh)
    yy: Dict[str, list] = {}
    for t in trades:
        yy.setdefault(t["year"], []).append(t)
    yearly = []
    for y in sorted(yy):
        ts = yy[y]
        r = np.array([x["net_ret"] for x in ts]); pv = np.array([x["net_pnl"] for x in ts])
        yearly.append({
            "year": y, "n": len(ts), "win_rate": round(float((r > 0).mean()) * 100, 1),
            "net_pnl": round(float(pv.sum())), "expectancy_pct": round(float(r.mean()) * 100, 2),
            "roi_pct": round(float(pv.sum()) / (len(ts) * size) * 100, 2),
        })
    # top mã theo tổng lãi ròng
    tk: Dict[str, list] = {}
    for t in trades:
        tk.setdefault(t["ticker"], []).append(t)
    per_ticker = []
    for k, ts in tk.items():
        pv = np.array([x["net_pnl"] for x in ts]); r = np.array([x["net_ret"] for x in ts])
        per_ticker.append({"ticker": k, "n": len(ts), "net_pnl": round(float(pv.sum())),
                           "win_rate": round(float((r > 0).mean()) * 100, 1),
                           "avg_ret_pct": round(float(r.mean()) * 100, 2),
                           "best_ret_pct": round(float(r.max()) * 100, 2),
                           "worst_ret_pct": round(float(r.min()) * 100, 2)})
    top = sorted(per_ticker, key=lambda x: x["net_pnl"], reverse=True)[:20]
    bottom = sorted(per_ticker, key=lambda x: x["net_pnl"])[:20]  # lỗ nhiều nhất

    return {"params": p, "summary": summary, "pnl": pnl, "yearly": yearly,
            "top": top, "bottom": bottom, "equity": eq}
