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

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services import ohlcv_store                       # noqa: E402
from app.services.screener import detect_vcp, DEFAULT_VCP_CONFIG  # noqa: E402

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


def _forward_returns(close: np.ndarray, i: int) -> dict:
    """% thay đổi giá từ phiên i đến i+h cho mỗi horizon. None nếu thiếu data."""
    out = {}
    base = close[i]
    for h in HORIZONS:
        j = i + h
        out[h] = (close[j] / base - 1) * 100 if (j < len(close) and base > 0) else None
    return out


def backtest_ticker(ticker: str, signal_fn, config, step: int) -> list:
    """Trả về list forward-return dict cho mỗi phiên có tín hiệu."""
    df = ohlcv_store.get_ohlcv(ticker)
    if df is None or len(df) < MIN_HISTORY + max(HORIZONS) + 1:
        return []
    df = df.reset_index(drop=True)
    close = df["close"].to_numpy(dtype=float)
    n = len(close)
    hits = []
    # Chạy từ MIN_HISTORY đến n - max_horizon (cần forward data để đo return)
    for i in range(MIN_HISTORY, n - max(HORIZONS), step):
        window = df.iloc[: i + 1]            # chỉ dữ liệu TỚI phiên i (no lookahead)
        try:
            vcp = detect_vcp(window, current_price=float(close[i]), config=config)
        except Exception:
            continue
        if signal_fn(vcp):
            fr = _forward_returns(close, i)
            if all(fr[h] is not None for h in HORIZONS):
                hits.append(fr)
    return hits


def baseline_ticker(ticker: str, step: int) -> list:
    """Baseline: forward return của MỌI phiên (không lọc tín hiệu)."""
    df = ohlcv_store.get_ohlcv(ticker)
    if df is None or len(df) < MIN_HISTORY + max(HORIZONS) + 1:
        return []
    close = df["close"].to_numpy(dtype=float)
    n = len(close)
    out = []
    for i in range(MIN_HISTORY, n - max(HORIZONS), step):
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
    args = ap.parse_args()

    config = replace(DEFAULT_VCP_CONFIG, use_atr_depth=True) if args.atr else DEFAULT_VCP_CONFIG
    signal_fn = SIGNALS[args.signal]
    tickers = _select_tickers(args)

    print(f"Backtest VCP — signal='{args.signal}', ATR={'on' if args.atr else 'off'}, "
          f"{len(tickers)} mã, step={args.step}, horizons={HORIZONS}")
    if args.min_avg_vol > 0 or args.sort_volume:
        print(f"  (lọc thanh khoản: min_avg_vol={args.min_avg_vol:,.0f}, xếp theo volume)")
    print("-" * 60)

    sig_all, base_all = [], []
    for t in tickers:
        hits = backtest_ticker(t, signal_fn, config, args.step)
        base = baseline_ticker(t, args.step)
        sig_all.extend(hits)
        base_all.extend(base)
        if hits:
            print(f"{t}: {len(hits)} tín hiệu / {len(base)} phiên")

    print("-" * 60)
    print("KẾT QUẢ TỔNG HỢP")
    _summarize(f"[{args.signal}]", sig_all)
    _summarize("[baseline]", base_all)

    # Alpha = chênh lệch mean forward return tín hiệu vs baseline
    if sig_all and base_all:
        print("\n  ALPHA (signal − baseline):")
        for h in HORIZONS:
            sa = np.mean([s[h] for s in sig_all])
            ba = np.mean([s[h] for s in base_all])
            print(f"      +{h:>2}p:  {sa - ba:+6.2f}%")


if __name__ == "__main__":
    main()
