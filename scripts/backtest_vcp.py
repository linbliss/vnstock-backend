#!/usr/bin/env python3
"""Backtest harness cho VCP detector — kiểm chứng threshold có sinh alpha thật.

Ý tưởng (Minervini): tại mỗi phiên trong lịch sử, chạy detect_vcp trên dữ liệu
TỚI phiên đó (không nhìn tương lai). Khi detector phát tín hiệu (breakout /
near-pivot / VDU / pocket pivot), ghi lại FORWARD RETURN sau 5/10/20 phiên.
So sánh win-rate & mean return của tín hiệu vs baseline (mọi phiên) → biết
threshold đang sinh alpha hay chỉ fit vào vài mã quan sát bằng mắt.

Dữ liệu lấy từ SQLite ohlcv_store (cần đã backfill).

Cách chạy:
    python scripts/backtest_vcp.py                 # mặc định: VN30-ish, tín hiệu breakout
    python scripts/backtest_vcp.py --signal near_pivot --tickers FPT,HPG,GMD
    python scripts/backtest_vcp.py --signal vdu --max-tickers 50 --step 3
    python scripts/backtest_vcp.py --atr           # bật ATR normalization để so sánh

Lưu ý: backtest đầy đủ rất nặng (detect_vcp ~vài ms × số phiên × số mã).
Dùng --step để sample (vd mỗi 3 phiên) và --max-tickers để giới hạn.
"""
import argparse
import os
import sys
from dataclasses import replace
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services import ohlcv_store                       # noqa: E402
from app.services.screener import (                        # noqa: E402
    detect_vcp, compute_rs_score, DEFAULT_VCP_CONFIG,
)

SIGNALS = {
    # name → predicate(vcp_dict) -> bool
    #
    # ⚠️ 'breakout' (state) fire MỌI phiên giá còn trên pivot → gồm nhiều entry
    #    trễ/extended đã mean-revert. Dùng để so sánh, KHÔNG phải tín hiệu entry.
    "breakout":    lambda v: v.get("stage") == "breakout" or (v.get("is_vcp") and v.get("above_pivot")),
    # 'breakout_fresh' = ĐÚNG vùng vừa vượt pivot (0–3%) + volume xác nhận →
    #    sát "ngày breakout" thực tế hơn, đây mới là tín hiệu entry Minervini.
    "breakout_fresh": lambda v: (v.get("is_vcp") and v.get("above_pivot")
                                 and 0 <= v.get("diff_pivot_pct", 99) <= 3
                                 and v.get("vol_confirmed")),
    # 'breakout_vol' = breakout (mọi mức) nhưng BẮT BUỘC volume xác nhận.
    "breakout_vol": lambda v: (v.get("is_vcp") and v.get("above_pivot")
                               and v.get("vol_confirmed")),
    # 'breakout_strict' = VCP strict + breakout + volume mạnh (≥1.5× MA50).
    "breakout_strict": lambda v: (v.get("is_vcp_strict") and v.get("above_pivot")
                                  and v.get("vol_confirmed_strict")),
    "near_pivot":  lambda v: v.get("is_vcp") and v.get("near_pivot"),
    "vcp":         lambda v: bool(v.get("is_vcp")),
    "vdu":         lambda v: bool(v.get("vdu_today")) and bool(v.get("contracting")),
    "pocket":      lambda v: bool(v.get("pocket_pivot")) and bool(v.get("contracting")),
}

HORIZONS = (5, 10, 20)
MIN_HISTORY = 120          # cần tối thiểu để detect_vcp chạy ổn


# Clip forward return để chặn outlier (giá chưa điều chỉnh corporate action,
# penny pump...) làm hỏng MEAN. 0 = tắt. Set qua --clip.
CLIP_PCT = 0.0


def _clip(r: float) -> float:
    return max(-CLIP_PCT, min(CLIP_PCT, r)) if CLIP_PCT > 0 else r


def _forward_returns(close: np.ndarray, i: int) -> dict:
    """% thay đổi giá từ phiên i đến i+h cho mỗi horizon. None nếu thiếu data."""
    out = {}
    base = close[i]
    for h in HORIZONS:
        j = i + h
        out[h] = _clip((close[j] / base - 1) * 100) if (j < len(close) and base > 0) else None
    return out


def _stop_aware_returns(close: np.ndarray, low: np.ndarray, i: int,
                        stop_price: float) -> dict:
    """Forward return CÓ stop-loss: vào lệnh tại close[i], thoát khi low chạm
    stop_price (khớp tại stop) hoặc giữ tới hết horizon. Phản ánh cách trade
    thật — short-term drawdown bị chặn, winner dài hạn vẫn chạy.
    """
    base = close[i]
    out = {h: None for h in HORIZONS}
    if base <= 0:
        return out
    maxh = max(HORIZONS)
    end = min(i + maxh, len(close) - 1)
    # Phiên ĐẦU TIÊN stop bị chạm trong khoảng [i+1, end]
    stop_j = None
    if stop_price and stop_price > 0:
        for j in range(i + 1, end + 1):
            if low[j] <= stop_price:
                stop_j = j
                break
    stop_ret = (stop_price / base - 1) * 100 if stop_price > 0 else None
    for h in HORIZONS:
        j = i + h
        if j >= len(close):
            continue
        if stop_j is not None and stop_j <= j:
            out[h] = _clip(stop_ret)              # đã thoát ở stop trước/đúng horizon
        else:
            out[h] = _clip((close[j] / base - 1) * 100)
    return out


def _regime_ok(cur_date, idx_close_dt: pd.Series, idx_ma50: pd.Series) -> bool:
    """Market regime: VNINDEX > MA50 tại (hoặc trước) cur_date — no-lookahead."""
    ic = idx_close_dt.asof(cur_date)
    im = idx_ma50.asof(cur_date)
    if pd.isna(ic) or pd.isna(im):
        return False
    return bool(ic > im)


def backtest_ticker(ticker: str, signal_fn, config, step: int,
                    min_rs: Optional[float] = None,
                    idx_close_dt: Optional[pd.Series] = None,
                    use_regime: bool = False,
                    idx_ma50: Optional[pd.Series] = None,
                    use_stop: bool = False,
                    stop_pct: float = 8.0) -> list:
    """Trả về list forward-return dict cho mỗi phiên có tín hiệu.

    RS gate (min_rs): tính RS score TẠI từng phiên từ window TỚI phiên đó
    (cắt cả index theo ngày) → KHÔNG lookahead, khác hẳn việc đọc rs_ratings
    table (chỉ có RS hôm nay → dùng cho quá khứ = lookahead bias).

    Regime filter (use_regime): chỉ tính tín hiệu khi VNINDEX > MA50 tại phiên đó.

    Stop-loss (use_stop): thoát tại vcp.stop_loss (fallback -stop_pct% nếu thiếu).
    """
    df = ohlcv_store.get_ohlcv(ticker)
    if df is None or len(df) < MIN_HISTORY + max(HORIZONS) + 1:
        return []
    df = df.reset_index(drop=True)
    close = df["close"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float) if "low" in df.columns else close
    n = len(close)

    use_rs = min_rs is not None and idx_close_dt is not None
    need_dates = use_rs or use_regime
    dates = pd.to_datetime(df["date"]) if need_dates else None
    stk_close_dt = pd.Series(close, index=dates) if use_rs else None

    hits = []
    # Chạy từ MIN_HISTORY đến n - max_horizon (cần forward data để đo return)
    for i in range(MIN_HISTORY, n - max(HORIZONS), step):
        cur_date = dates.iloc[i] if need_dates else None
        # ── Regime filter (no-lookahead) — rẻ, check trước detect_vcp ──
        if use_regime and not _regime_ok(cur_date, idx_close_dt, idx_ma50):
            continue
        window = df.iloc[: i + 1]            # chỉ dữ liệu TỚI phiên i (no lookahead)
        try:
            vcp = detect_vcp(window, current_price=float(close[i]), config=config)
        except Exception:
            continue
        if not signal_fn(vcp):
            continue
        # ── RS gate (no-lookahead) ──
        if use_rs:
            stk_w = stk_close_dt.iloc[: i + 1]
            idx_w = idx_close_dt[idx_close_dt.index <= cur_date]   # cắt index TỚI ngày i
            try:
                rs = compute_rs_score(stk_w, idx_w)
            except Exception:
                rs = 0.0
            if rs < min_rs:
                continue
        if use_stop:
            # vcp.stop_loss cùng đơn vị close (đều từ df). Fallback -stop_pct%.
            sl = float(vcp.get("stop_loss") or 0)
            if not (0 < sl < close[i]):
                sl = close[i] * (1 - stop_pct / 100)
            fr = _stop_aware_returns(close, low, i, sl)
        else:
            fr = _forward_returns(close, i)
        if all(fr[h] is not None for h in HORIZONS):
            hits.append(fr)
    return hits


def baseline_ticker(ticker: str, step: int,
                    use_regime: bool = False,
                    idx_close_dt: Optional[pd.Series] = None,
                    idx_ma50: Optional[pd.Series] = None,
                    use_stop: bool = False,
                    stop_pct: float = 8.0) -> list:
    """Baseline: forward return của MỌI phiên (không lọc tín hiệu).

    Nếu use_regime: baseline cũng chỉ lấy phiên VNINDEX > MA50 → so sánh CÔNG
    BẰNG (cô lập edge của tín hiệu VCP trong cùng điều kiện up-regime, tránh
    nhầm cải thiện do regime với cải thiện do detector).

    Nếu use_stop: baseline dùng stop cố định -stop_pct% (không có pivot) → so
    sánh signal-có-stop vs baseline-có-stop cùng quy tắc thoát.
    """
    df = ohlcv_store.get_ohlcv(ticker)
    if df is None or len(df) < MIN_HISTORY + max(HORIZONS) + 1:
        return []
    close = df["close"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float) if "low" in df.columns else close
    n = len(close)
    dates = pd.to_datetime(df["date"]) if use_regime else None
    out = []
    for i in range(MIN_HISTORY, n - max(HORIZONS), step):
        if use_regime and not _regime_ok(dates.iloc[i], idx_close_dt, idx_ma50):
            continue
        if use_stop:
            fr = _stop_aware_returns(close, low, i, close[i] * (1 - stop_pct / 100))
        else:
            fr = _forward_returns(close, i)
        if all(fr[h] is not None for h in HORIZONS):
            out.append(fr)
    return out


def _avg_volume(ticker: str, window: int = 60) -> float:
    """Volume trung bình `window` phiên gần nhất — dùng lọc thanh khoản."""
    df = ohlcv_store.get_ohlcv(ticker)
    if df is None or len(df) < window:
        return 0.0
    return float(df["volume"].tail(window).mean())


def _select_tickers(args) -> list:
    """Chọn mã: ưu tiên --tickers; nếu không, lọc theo thanh khoản rồi lấy top N.

    Mặc định KHÔNG còn lấy 50 mã đầu bảng chữ cái (toàn penny A*) — thay vào
    đó xếp theo volume TB giảm dần để mẫu phản ánh leader thanh khoản tốt.
    """
    if args.tickers.strip():
        return [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    allt = ohlcv_store.list_tickers()
    if args.min_avg_vol > 0 or args.sort_volume:
        ranked = [(t, _avg_volume(t)) for t in allt]
        ranked = [(t, v) for t, v in ranked if v >= args.min_avg_vol]
        ranked.sort(key=lambda x: x[1], reverse=True)
        return [t for t, _ in ranked[: args.max_tickers]]
    return allt[: args.max_tickers]


def _summarize(label: str, samples: list):
    if not samples:
        print(f"  {label:<14} (0 mẫu)")
        return
    print(f"  {label:<14} n={len(samples)}")
    for h in HORIZONS:
        vals = np.array([s[h] for s in samples], dtype=float)
        win = float((vals > 0).mean() * 100)
        print(f"      +{h:>2}p:  mean {vals.mean():+6.2f}%   median {np.median(vals):+6.2f}%"
              f"   win {win:4.1f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--signal", choices=list(SIGNALS), default="breakout")
    ap.add_argument("--tickers", default="", help="CSV; mặc định = tất cả trong store")
    ap.add_argument("--max-tickers", type=int, default=30)
    ap.add_argument("--step", type=int, default=2, help="sample mỗi N phiên (giảm tải)")
    ap.add_argument("--atr", action="store_true", help="bật ATR normalization")
    ap.add_argument("--min-avg-vol", type=float, default=0.0,
                    help="lọc mã có volume TB 60 phiên >= ngưỡng (vd 500000)")
    ap.add_argument("--sort-volume", action="store_true",
                    help="xếp mã theo thanh khoản giảm dần (tránh mẫu penny A*)")
    ap.add_argument("--min-rs", type=float, default=None,
                    help="RS gate: chỉ tính tín hiệu khi RS score (relative return "
                         "vs VNINDEX) >= ngưỡng tại phiên đó. >0 = outperform index. "
                         "Tính per-bar, KHÔNG lookahead.")
    ap.add_argument("--index", default="VNINDEX", help="mã index cho RS gate")
    ap.add_argument("--clip", type=float, default=0.0,
                    help="winsorize forward return ở ±N%% (chặn outlier corporate "
                         "action/penny pump). Vd --clip 50. 0 = tắt.")
    ap.add_argument("--regime", action="store_true",
                    help="market-regime filter: chỉ tính phiên VNINDEX > MA50 "
                         "(áp cho CẢ signal lẫn baseline). No-lookahead.")
    ap.add_argument("--horizons", default="5,10,20,40,60",
                    help="CSV số phiên forward, vd '5,10,20,40,60'")
    ap.add_argument("--stop", action="store_true",
                    help="mô phỏng stop-loss: signal thoát tại vcp.stop_loss, "
                         "baseline thoát tại -stop_pct%%. Phản ánh trade thật.")
    ap.add_argument("--stop-pct", type=float, default=8.0,
                    help="%% stop cho baseline (và fallback signal). Mặc định 8.")
    args = ap.parse_args()

    global CLIP_PCT, HORIZONS
    CLIP_PCT = max(0.0, args.clip)
    HORIZONS = tuple(int(h) for h in args.horizons.split(",") if h.strip())

    config = replace(DEFAULT_VCP_CONFIG, use_atr_depth=True) if args.atr else DEFAULT_VCP_CONFIG
    signal_fn = SIGNALS[args.signal]
    tickers = _select_tickers(args)

    # Index series (DatetimeIndex) — cần cho RS gate VÀ/HOẶC regime filter
    idx_close_dt = idx_ma50 = None
    if args.min_rs is not None or args.regime:
        idx_df = ohlcv_store.get_ohlcv(args.index)
        if idx_df is None or len(idx_df) < 63:
            print(f"⚠️  Không có dữ liệu index '{args.index}' → tắt RS gate & regime")
            args.min_rs = None
            args.regime = False
        else:
            idx_close_dt = pd.Series(
                idx_df["close"].to_numpy(dtype=float),
                index=pd.to_datetime(idx_df["date"]),
            ).sort_index()
            idx_ma50 = idx_close_dt.rolling(50).mean()

    print(f"Backtest VCP — signal='{args.signal}', ATR={'on' if args.atr else 'off'}, "
          f"{len(tickers)} mã, step={args.step}, horizons={HORIZONS}")
    if args.min_avg_vol > 0 or args.sort_volume:
        print(f"  (lọc thanh khoản: min_avg_vol={args.min_avg_vol:,.0f}, xếp theo volume)")
    if args.min_rs is not None:
        print(f"  (RS gate: RS score >= {args.min_rs} vs {args.index}, no-lookahead)")
    if args.regime:
        print(f"  (regime filter: {args.index} > MA50, áp cho signal + baseline)")
    if args.stop:
        print(f"  (stop-loss: signal=vcp.stop_loss, baseline=-{args.stop_pct:.0f}%)")
    print("-" * 60)

    sig_all, base_all = [], []
    for t in tickers:
        hits = backtest_ticker(t, signal_fn, config, args.step,
                               min_rs=args.min_rs, idx_close_dt=idx_close_dt,
                               use_regime=args.regime, idx_ma50=idx_ma50,
                               use_stop=args.stop, stop_pct=args.stop_pct)
        base = baseline_ticker(t, args.step, use_regime=args.regime,
                               idx_close_dt=idx_close_dt, idx_ma50=idx_ma50,
                               use_stop=args.stop, stop_pct=args.stop_pct)
        sig_all.extend(hits)
        base_all.extend(base)
        if hits:
            print(f"{t}: {len(hits)} tín hiệu / {len(base)} phiên")

    print("-" * 60)
    print("KẾT QUẢ TỔNG HỢP")
    _summarize(f"[{args.signal}]", sig_all)
    _summarize("[baseline]", base_all)

    # ALPHA — báo cáo cả MEAN và các thống kê KHÁNG OUTLIER (median, win-rate).
    # MEAN dễ bị nhiễu bởi vài bar cực đoan (giá chưa điều chỉnh corporate action,
    # penny pump) → median & win-rate phản ánh "edge" thực tế đáng tin hơn.
    if sig_all and base_all:
        print("\n  ALPHA (signal − baseline):")
        print(f"      {'horizon':>8}  {'Δmean':>8}  {'Δmedian':>8}  {'Δwin%':>7}")
        for h in HORIZONS:
            sv = np.array([s[h] for s in sig_all], dtype=float)
            bv = np.array([s[h] for s in base_all], dtype=float)
            d_mean = sv.mean() - bv.mean()
            d_med  = np.median(sv) - np.median(bv)
            d_win  = (sv > 0).mean() * 100 - (bv > 0).mean() * 100
            print(f"      +{h:>2}p:    {d_mean:+7.2f}%  {d_med:+7.2f}%  {d_win:+6.1f}")
        if CLIP_PCT > 0:
            print(f"  (forward return winsorized ở ±{CLIP_PCT:.0f}%)")
        else:
            print("  Lưu ý: Δmean dễ bị outlier; ưu tiên Δmedian & Δwin%. "
                  "Dùng --clip 50 để winsorize.")


if __name__ == "__main__":
    main()
