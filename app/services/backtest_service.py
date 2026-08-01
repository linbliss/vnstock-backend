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


def _regime_up(get_df: Callable[[str], Optional[pd.DataFrame]], ma: int = 50) -> Dict[str, bool]:
    """Dict {ngày -> VNINDEX > MA50} — thị trường uptrend (yếu tố 'M' của Minervini)."""
    idx = get_df("VNINDEX")
    if idx is None or idx.empty:
        return {}
    df = idx.sort_values("date").reset_index(drop=True)
    m = df["close"].astype(float).rolling(ma).mean()
    return {str(df["date"].iloc[i])[:10]: bool(df["close"].iloc[i] > m.iloc[i])
            for i in range(len(df)) if not np.isnan(m.iloc[i])}


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
    # ── Chế độ DANH MỤC có giới hạn (mô phỏng thực tế) ──
    "portfolio_mode": False,    # True = danh mục có trần vốn/vị thế, chạy theo ngày
    "initial_capital": 100_000_000,   # vốn ban đầu X
    "max_positions": 10,        # Y: số mã tối đa BAN ĐẦU (0 = không giới hạn)
    "max_positions_grown": 0,   # Z: số mã tối đa KHI CÓ LÃI (0/≤Y = không tăng)
    "sizing_mode": "equal",     # fixed | equal (equity/số vị thế) | percent | unit (X/Y lô cố định)
    "position_pct": 0.10,       # dùng khi sizing_mode=percent
    "compound": True,           # lãi kép: tái đầu tư lợi nhuận đã thực hiện
    "rotation": "weakest",      # off | weakest (cắt mã yếu) | take_profit (chốt mã lãi cao)
    "pyramid": False,           # gia tăng vị thế mã đang nắm khi có breakout mới & còn tiền
    "pyramid_max": 3,           # trần số 'lô' mỗi mã khi gia tăng (vd 3 = tối đa 3×X/Y)
    # ── LỌC THỊ TRƯỜNG (VNINDEX vs MA50 — yếu tố 'M' Minervini) ──
    "regime_filter": "off",     # off | reduce (giảm giải ngân khi yếu) | block (chặn mở mới khi yếu)
    "regime_weak_exposure": 0.3,  # reduce: tỷ lệ vị thế cho phép khi thị trường yếu (0.3 = 30%)
    "regime_ma": 50,            # MA của VNINDEX để xác định uptrend
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
            "net_pnl": net_pnl, "alloc": size, "hold_days": hold, "open": is_open}


def run_backtest(signals: List[Dict[str, Any]],
                 get_df: Callable[[str], Optional[pd.DataFrame]],
                 exch_map: Optional[Dict[str, str]] = None,
                 params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    p = {**DEFAULTS, **(params or {})}
    exch_set = set(p["exchanges"]) if p.get("exchanges") else None
    exch_map = exch_map or {}
    size = p["position_vnd"]

    # lọc chung: thanh khoản + thời gian + sàn
    sig = [s for s in signals if s.get("avg_vol50", 0) >= p["min_avg_vol"]]
    if p.get("date_from"):
        sig = [s for s in sig if s["date"] >= p["date_from"]]
    if p.get("date_to"):
        sig = [s for s in sig if s["date"] <= p["date_to"]]
    if exch_set:
        sig = [s for s in sig if exch_map.get(s["ticker"]) in exch_set]
    sig.sort(key=lambda x: x["date"])

    if p.get("portfolio_mode"):
        return run_portfolio(sig, get_df, p)

    # lọc thị trường yếu (per-signal: bỏ tín hiệu vào lúc VNINDEX < MA50)
    if p.get("regime_filter", "off") != "off":
        regime = _regime_up(get_df, int(p.get("regime_ma", 50)))
        sig = [s for s in sig if regime.get(s["date"], True)]

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


# ── Helper dùng chung cho cả 2 chế độ ──────────────────────────────────────
def _trade_summary(trades) -> Dict[str, Any]:
    rets = np.array([t["net_ret"] for t in trades])
    holds = np.array([t["hold_days"] for t in trades])
    wins = rets > 0
    aw = rets[wins].mean() if wins.any() else 0.0
    al = rets[~wins].mean() if (~wins).any() else 0.0
    payoff = (aw / -al) if al < 0 else float("nan")
    return {
        "n_trades": len(trades), "n_open": int(sum(t["open"] for t in trades)),
        "win_rate": round(float(wins.mean()) * 100, 1),
        "payoff": round(payoff, 2) if payoff == payoff else None,
        "expectancy_pct": round(float(rets.mean()) * 100, 2),
        "avg_win_pct": round(float(aw) * 100, 2), "avg_loss_pct": round(float(al) * 100, 2),
        "avg_hold_days": round(float(holds.mean()), 1), "median_hold_days": int(np.median(holds)),
    }


def _yearly_of(trades) -> List[Dict[str, Any]]:
    yy: Dict[str, list] = {}
    for t in trades:
        yy.setdefault(t["year"], []).append(t)
    out = []
    for y in sorted(yy):
        ts = yy[y]
        r = np.array([x["net_ret"] for x in ts]); pv = np.array([x["net_pnl"] for x in ts])
        inv = float(sum(x["alloc"] for x in ts))
        out.append({"year": y, "n": len(ts), "win_rate": round(float((r > 0).mean()) * 100, 1),
                    "net_pnl": round(float(pv.sum())), "expectancy_pct": round(float(r.mean()) * 100, 2),
                    "roi_pct": round(float(pv.sum()) / inv * 100, 2) if inv else 0})
    return out


def _top_bottom_of(trades):
    tk: Dict[str, list] = {}
    for t in trades:
        tk.setdefault(t["ticker"], []).append(t)
    per = []
    for k, ts in tk.items():
        pv = np.array([x["net_pnl"] for x in ts]); r = np.array([x["net_ret"] for x in ts])
        per.append({"ticker": k, "n": len(ts), "net_pnl": round(float(pv.sum())),
                    "win_rate": round(float((r > 0).mean()) * 100, 1),
                    "avg_ret_pct": round(float(r.mean()) * 100, 2),
                    "best_ret_pct": round(float(r.max()) * 100, 2),
                    "worst_ret_pct": round(float(r.min()) * 100, 2)})
    return (sorted(per, key=lambda x: x["net_pnl"], reverse=True)[:20],
            sorted(per, key=lambda x: x["net_pnl"])[:20])


def _aggregate(trades, size, p, skipped_overlap):
    if not trades:
        return {"params": p, "summary": {"n_trades": 0}, "pnl": {}, "yearly": [],
                "top": [], "bottom": [], "equity": [], "message": "Không có lệnh nào khớp bộ lọc"}
    pnls = np.array([t["net_pnl"] for t in trades])
    total_inv = len(trades) * size
    total_pnl = float(pnls.sum())

    # VỐN TỐI ĐA cùng lúc (interval overlap): mở (+1) trước đóng (-1) cùng ngày.
    ev = [(t["entry_date"], 1) for t in trades] + [(t["exit_date"], -1) for t in trades]
    ev.sort(key=lambda x: (x[0], -x[1]))
    cur = peak_pos = 0
    for _d, delta in ev:
        cur += delta
        peak_pos = max(peak_pos, cur)
    peak_capital = peak_pos * size

    # equity theo NGÀY THOÁT (P&L thực hiện luỹ kế) + max drawdown
    cum, eq, peak_eq, maxdd = 0.0, [], 0.0, 0.0
    for t in sorted(trades, key=lambda t: t["exit_date"]):
        cum += t["net_pnl"]; peak_eq = max(peak_eq, cum); maxdd = min(maxdd, cum - peak_eq)
        eq.append({"date": t["exit_date"], "cum_pnl": round(cum)})

    summary = {**_trade_summary(trades), "skipped_overlap": skipped_overlap}
    wins = pnls > 0
    pnl = {
        "total_invested": round(total_inv), "total_net_pnl": round(total_pnl),
        "roi_pct": round(total_pnl / total_inv * 100, 2) if total_inv else 0,
        "n_win": int(wins.sum()), "n_loss": int((~wins).sum()),
        "gross_profit": round(float(pnls[pnls > 0].sum())),
        "gross_loss": round(float(pnls[pnls <= 0].sum())),
        "best_trade": round(float(pnls.max())), "worst_trade": round(float(pnls.min())),
        "max_drawdown": round(float(maxdd)),
        "peak_positions": peak_pos, "peak_capital": round(peak_capital),
        "roi_on_peak_pct": round(total_pnl / peak_capital * 100, 2) if peak_capital else 0,
    }
    top, bottom = _top_bottom_of(trades)
    return {"params": p, "summary": summary, "pnl": pnl, "yearly": _yearly_of(trades),
            "top": top, "bottom": bottom, "equity": eq}


# ══════════════════════════════════════════════════════════════════════════
# CHẾ ĐỘ DANH MỤC — mô phỏng theo NGÀY, có trần vốn/vị thế, xoay vòng, lãi kép
# ══════════════════════════════════════════════════════════════════════════
def _pick_victim(positions, d, pget, mode, min_hold=10):
    """Chọn mã để bán lấy chỗ khi danh mục đầy. Trả None nếu không nên xoay.
    Chỉ xét vị thế đã giữ ≥ min_hold phiên (tránh cắt lệnh non đang chờ chạy)."""
    cand = []
    for tk, ps in positions.items():
        row = pget(tk).get(d)
        c = row[3] if row else ps["last_close"]
        cand.append((tk, c / ps["entry"] - 1.0, ps["days"]))
    if mode == "take_profit":                       # chốt mã LÃI ≥+20% (giữ đủ lâu) lấy vốn
        elig = [x for x in cand if x[1] >= 0.20 and x[2] >= min_hold]
        return max(elig, key=lambda x: x[1])[0] if elig else None
    # 'weakest': cắt mã THUA (ret<0) đã giữ ≥min_hold — laggard thực sự, không cắt winner
    elig = [x for x in cand if x[1] < 0 and x[2] >= min_hold]
    return min(elig, key=lambda x: x[1])[0] if elig else None


def run_portfolio(sig, get_df, p) -> Dict[str, Any]:
    from collections import defaultdict
    fee, trail, init, intraday = p["fee_roundtrip"], p["trail_pct"], p["init_stop_pct"], p["intraday"]
    X = float(p["initial_capital"])
    Y = int(p["max_positions"]) or 10 ** 9                 # số mã tối đa BAN ĐẦU
    Z = int(p.get("max_positions_grown") or 0)             # số mã tối đa KHI CÓ LÃI
    Z = Z if (Y < 10 ** 9 and Z > Y) else Y                # Z<=Y hoặc Y vô hạn → không tăng
    mode, compound, pct = p["sizing_mode"], p["compound"], p["position_pct"]
    unit = (X / Y) if (mode == "unit" and Y and Y < 10 ** 9) else None   # lô cố định X/Y
    pyramid, pyr_max = bool(p.get("pyramid")), float(p.get("pyramid_max", 3))
    rf = p.get("regime_filter", "off")                    # off | reduce | block
    wexp = float(p.get("regime_weak_exposure", 0.3))
    regime = _regime_up(get_df, int(p.get("regime_ma", 50))) if rf != "off" else {}
    MIN_BUY = 1_000_000

    by_date: Dict[str, list] = defaultdict(list)
    for s in sig:
        by_date[s["date"]].append(s)
    for d in by_date:                                # trong ngày ưu tiên mã thanh khoản cao
        by_date[d].sort(key=lambda x: -x.get("avg_vol50", 0))

    idx = get_df("VNINDEX")
    if idx is not None and not idx.empty:
        cal = [str(x)[:10] for x in idx["date"].tolist()]
    else:
        cal = sorted(by_date.keys())
    lo = p.get("date_from") or (min(by_date) if by_date else (cal[0] if cal else None))
    hi = p.get("date_to") or (cal[-1] if cal else None)
    cal = [d for d in cal if (lo is None or d >= lo) and (hi is None or d <= hi)]
    if not cal:
        return {"params": p, "summary": {"n_trades": 0}, "pnl": {}, "yearly": [],
                "top": [], "bottom": [], "equity": [], "message": "Không có phiên nào trong khoảng chọn"}

    price: Dict[str, dict] = {}

    def pget(tk):
        if tk not in price:
            df = get_df(tk)
            price[tk] = ({} if df is None else
                         {str(r[0])[:10]: (float(r[1]), float(r[2]), float(r[3]), float(r[4]))
                          for r in df[["date", "open", "high", "low", "close"]].itertuples(index=False, name=None)})
        return price[tk]

    cash = float(p["initial_capital"])
    positions: Dict[str, dict] = {}
    trades: List[Dict[str, Any]] = []
    equity: List[Dict[str, Any]] = []
    peak_pos = n_rot = 0
    peak_deployed = 0.0

    def sell(tk, d, px, is_open=False):
        nonlocal cash
        ps = positions.pop(tk)
        proceeds = ps["shares"] * px * (1 - fee / 2)
        cost = ps["alloc"] * (1 + fee / 2)
        net = proceeds - cost
        cash += proceeds
        trades.append({"ticker": tk, "entry_date": ps["entry_date"], "exit_date": d,
                       "entry": ps["entry"], "exit": px, "net_ret": net / ps["alloc"],
                       "net_pnl": net, "alloc": ps["alloc"], "hold_days": ps["days"],
                       "adds": ps.get("adds", 0), "year": ps["entry_date"][:4], "open": is_open})

    for d in cal:
        # 1) cập nhật & thoát lệnh
        for tk in list(positions.keys()):
            ps = positions[tk]; row = pget(tk).get(d)
            if not row:
                continue
            o, h, l, c = row; ps["last_close"] = c; ps["days"] += 1
            stop = max(ps["entry"] * (1 - init), ps["peak"] * (1 - trail))
            ex = None
            if intraday:
                if o <= stop: ex = o
                elif l <= stop: ex = stop
            elif c <= stop:
                ex = c
            if ex is not None:
                sell(tk, d, ex)
            elif h > ps["peak"]:
                ps["peak"] = h
        # 2) vào lệnh theo tín hiệu breakout hôm nay
        eq_start = cash + sum(ps["shares"] * ps["last_close"] for ps in positions.values())
        # trần vị thế ĐỘNG: bắt đầu Y, tăng theo vốn tới Z (chỉ khi sizing 'unit')
        if unit:
            cap_now = min(Z, max(Y, int(eq_start // unit)))
        else:
            cap_now = Y
        # LỌC THỊ TRƯỜNG: yếu → giảm trần (reduce) hoặc chặn mở mới (block) + tắt pyramiding
        weak = (rf != "off") and (not regime.get(d, True))
        if weak:
            eff_cap = len(positions) if rf == "block" else max(0, int(round(cap_now * wexp)))
        else:
            eff_cap = cap_now
        allow_pyr = pyramid and not weak
        for s in by_date.get(d, []):
            tk = s["ticker"]; entry = s["entry_close"]
            # kích thước lô mục tiêu
            if unit is not None:
                target = unit
            elif mode == "percent":
                target = (eq_start if compound else X) * pct
            elif mode == "equal":
                base_n = cap_now if cap_now < 10 ** 9 else Y
                target = (eq_start if compound else X) / base_n
            else:  # fixed
                target = p["position_vnd"]

            if tk in positions:                              # ĐÃ NẮM → cân nhắc gia tăng vị thế
                if allow_pyr and cash >= MIN_BUY:
                    ps = positions[tk]
                    room = pyr_max * ps["base"] - ps["alloc"]     # trần pyr_max lô/mã
                    add = min(target, cash, room)
                    if add >= MIN_BUY:
                        ash = add / entry
                        ps["entry"] = (ps["entry"] * ps["shares"] + entry * ash) / (ps["shares"] + ash)
                        ps["shares"] += ash; ps["alloc"] += add; ps["adds"] += 1
                        cash -= add * (1 + fee / 2)
                continue

            if len(positions) >= eff_cap:                    # đầy (hoặc bị lọc regime) → xoay vòng
                if p["rotation"] == "off" or weak:           # thị trường yếu: không xoay để mở mã mới
                    continue
                victim = _pick_victim(positions, d, pget, p["rotation"])
                if victim is None:
                    continue
                vrow = pget(victim).get(d)
                sell(victim, d, vrow[3] if vrow else positions[victim]["last_close"])
                n_rot += 1
            alloc = min(target, cash)
            if alloc < MIN_BUY:
                continue
            positions[tk] = {"entry_date": d, "entry": entry, "alloc": alloc, "base": alloc,
                             "shares": alloc / entry, "peak": entry, "last_close": entry,
                             "days": 0, "adds": 0}
            cash -= alloc * (1 + fee / 2)
        # 3) chốt equity ngày
        deployed = sum(ps["shares"] * ps["last_close"] for ps in positions.values())
        peak_pos = max(peak_pos, len(positions))
        peak_deployed = max(peak_deployed, deployed)
        equity.append({"date": d, "equity": round(cash + deployed)})

    for tk in list(positions.keys()):                        # đóng phần còn mở cuối kỳ
        sell(tk, cal[-1], positions[tk]["last_close"], is_open=True)

    return _aggregate_portfolio(trades, equity, p, peak_pos, peak_deployed, n_rot, cal)


def _aggregate_portfolio(trades, equity, p, peak_pos, peak_deployed, n_rot, cal):
    init_cap = float(p["initial_capital"])
    if not trades:
        return {"params": p, "summary": {"n_trades": 0}, "pnl": {}, "yearly": [],
                "top": [], "bottom": [], "equity": [], "message": "Không có lệnh nào khớp bộ lọc"}
    final_eq = init_cap + sum(t["net_pnl"] for t in trades)
    total_pnl = final_eq - init_cap
    # drawdown trên đường EQUITY thật
    peak_e, maxdd, maxdd_pct = init_cap, 0.0, 0.0
    for e in equity:
        peak_e = max(peak_e, e["equity"])
        dd = e["equity"] - peak_e
        if dd < maxdd:
            maxdd, maxdd_pct = dd, dd / peak_e * 100
    yrs = (len(cal) / 252.0) if cal else 1.0
    cagr = ((final_eq / init_cap) ** (1 / yrs) - 1) * 100 if init_cap > 0 and yrs > 0 else 0.0
    pnls = np.array([t["net_pnl"] for t in trades]); wins = pnls > 0

    n_pyr = int(sum(t.get("adds", 0) for t in trades))
    summary = {**_trade_summary(trades), "skipped_overlap": 0,
               "n_rotations": n_rot, "n_pyramids": n_pyr}
    pnl = {
        "initial_capital": round(init_cap), "final_equity": round(final_eq),
        "total_net_pnl": round(total_pnl),
        "total_return_pct": round(total_pnl / init_cap * 100, 2) if init_cap else 0,
        "cagr_pct": round(cagr, 2),
        "total_invested": round(float(sum(t["alloc"] for t in trades))),
        "roi_pct": round(total_pnl / float(sum(t["alloc"] for t in trades)) * 100, 2) if trades else 0,
        "n_win": int(wins.sum()), "n_loss": int((~wins).sum()),
        "gross_profit": round(float(pnls[pnls > 0].sum())),
        "gross_loss": round(float(pnls[pnls <= 0].sum())),
        "best_trade": round(float(pnls.max())), "worst_trade": round(float(pnls.min())),
        "max_drawdown": round(float(maxdd)), "max_drawdown_pct": round(float(maxdd_pct), 2),
        "peak_positions": peak_pos, "peak_capital": round(peak_deployed),
        "roi_on_peak_pct": round(total_pnl / peak_deployed * 100, 2) if peak_deployed else 0,
    }
    top, bottom = _top_bottom_of(trades)
    eq_curve = [{"date": e["date"], "cum_pnl": round(e["equity"] - init_cap)} for e in equity]

    # ROI THEO NĂM trên EQUITY (lãi kép): equity cuối năm / cuối năm trước − 1
    last_eq: Dict[str, float] = {}
    for e in equity:
        last_eq[e["date"][:4]] = e["equity"]
    yret: Dict[str, float] = {}
    prev = init_cap
    for y in sorted(last_eq):
        yret[y] = last_eq[y] / prev - 1 if prev else 0.0
        prev = last_eq[y]
    # gộp với thống kê lệnh VÀO trong năm (n, win, kỳ vọng)
    by_year: Dict[str, list] = {}
    for t in trades:
        by_year.setdefault(t["year"], []).append(t)
    yearly = []
    for y in sorted(set(list(last_eq) + list(by_year))):
        ts = by_year.get(y, [])
        r = np.array([x["net_ret"] for x in ts]) if ts else np.array([0.0])
        pv = float(sum(x["net_pnl"] for x in ts))
        yearly.append({
            "year": y, "n": len(ts),
            "win_rate": round(float((r > 0).mean()) * 100, 1) if ts else 0,
            "net_pnl": round(pv), "expectancy_pct": round(float(r.mean()) * 100, 2) if ts else 0,
            "roi_pct": round(yret.get(y, 0.0) * 100, 2),      # ROI danh mục năm đó (equity)
            "end_equity": round(last_eq.get(y, init_cap)),    # tổng tài sản cuối năm
        })
    return {"params": p, "summary": summary, "pnl": pnl, "yearly": yearly,
            "top": top, "bottom": bottom, "equity": eq_curve}
