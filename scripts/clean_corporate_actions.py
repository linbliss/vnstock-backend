#!/usr/bin/env python3
"""Quét OHLCV tìm dấu hiệu corporate action (giá lịch sử CHƯA điều chỉnh) và
re-fetch lại lịch sử đã điều chỉnh từ vnstock.

Vì sao cần: khi mã thưởng CP / tách-gộp / trả cổ tức bằng CP, nhà cung cấp dữ
liệu điều chỉnh HỒI TỐ toàn bộ giá lịch sử. Bản incremental (daily_update) chỉ
thêm phiên mới nên lịch sử cũ trong SQLite vẫn là giá CHƯA điều chỉnh → tạo
"gap" nhảy giá làm méo chart, VCP depth, và MEAN của backtest.

Cách nhận biết: |%thay đổi close| giữa 2 phiên liên tiếp > ngưỡng (mặc định
18%). Biên độ trần/sàn VN là ±7% (HOSE), ±10% (HNX), ±15% (UPCOM) nên 18% lọc
được hầu hết limit-move bình thường, chỉ còn lại corporate action / lỗi dữ liệu.

Cách chạy (trong container):
    # 1. Liệt kê ứng viên trước (KHÔNG sửa gì)
    docker exec vnstock-backend python scripts/clean_corporate_actions.py --dry-run

    # 2. Refetch thật (mọi mã có gap), 5 năm lịch sử
    docker exec vnstock-backend python scripts/clean_corporate_actions.py --years 5

    # 3. Chỉ một số mã
    docker exec vnstock-backend python scripts/clean_corporate_actions.py --tickers GEE,VND --years 5
"""
import argparse
import asyncio
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services import ohlcv_store         # noqa: E402
from app.services import backfill            # noqa: E402


def find_gaps(ticker: str, threshold: float, since: str = None) -> list:
    """Trả về list (date, pct) các phiên có |%thay đổi close| > threshold.

    since: chỉ xét gap từ ngày này trở đi (YYYY-MM-DD) — corporate action cũ
    nhiều năm trước ít ảnh hưởng. None = toàn bộ lịch sử.
    """
    df = ohlcv_store.get_ohlcv(ticker)
    if df is None or len(df) < 2:
        return []
    df = df.reset_index(drop=True)
    close = df["close"].to_numpy(dtype=float)
    dates = df["date"].astype(str).tolist()
    gaps = []
    for i in range(1, len(close)):
        if close[i - 1] <= 0:
            continue
        pct = (close[i] / close[i - 1] - 1) * 100
        if abs(pct) > threshold:
            if since is None or dates[i] >= since:
                gaps.append((dates[i], round(pct, 1)))
    return gaps


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threshold", type=float, default=18.0,
                    help="ngưỡng %% gap close-to-close coi là corporate action (mặc định 18)")
    ap.add_argument("--since", default=None,
                    help="chỉ xét gap từ ngày YYYY-MM-DD (mặc định: toàn lịch sử)")
    ap.add_argument("--tickers", default="", help="CSV; mặc định = tất cả trong store")
    ap.add_argument("--years", type=int, default=5, help="số năm lịch sử refetch")
    ap.add_argument("--dry-run", action="store_true",
                    help="chỉ liệt kê ứng viên, KHÔNG refetch")
    ap.add_argument("--limit", type=int, default=0,
                    help="giới hạn số mã refetch (0 = không giới hạn)")
    args = ap.parse_args()

    if args.tickers.strip():
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = ohlcv_store.list_tickers()

    print(f"Quét corporate action — {len(tickers)} mã, threshold={args.threshold}%"
          + (f", since={args.since}" if args.since else "")
          + (", DRY-RUN" if args.dry_run else f", refetch {args.years} năm"))
    print("-" * 64)

    candidates = []
    for t in tickers:
        if t.upper() in ("VNINDEX", "VN30", "HNXINDEX"):
            continue
        gaps = find_gaps(t, args.threshold, args.since)
        if gaps:
            candidates.append((t, gaps))
            sample = ", ".join(f"{d}:{p:+.0f}%" for d, p in gaps[:3])
            more = f" (+{len(gaps) - 3})" if len(gaps) > 3 else ""
            print(f"  {t:<8} {len(gaps)} gap  [{sample}{more}]")

    print("-" * 64)
    print(f"Tìm thấy {len(candidates)} mã nghi corporate action chưa điều chỉnh")

    if args.dry_run or not candidates:
        if args.dry_run:
            print("DRY-RUN: không refetch. Bỏ --dry-run để chạy thật.")
        return

    targets = candidates[: args.limit] if args.limit > 0 else candidates
    print(f"\nBắt đầu refetch {len(targets)} mã ({args.years} năm)...")
    ok = fail = 0
    for t, _ in targets:
        try:
            n = await backfill.refetch_ticker(t, years=args.years)
            if n > 0:
                ok += 1
                print(f"  ✅ {t}: {n} rows")
            else:
                fail += 1
                print(f"  ⚠️  {t}: 0 rows")
        except Exception as e:
            fail += 1
            print(f"  ❌ {t}: {type(e).__name__}: {e}")
    print("-" * 64)
    print(f"Hoàn tất: {ok} OK, {fail} lỗi / {len(targets)} mã")


if __name__ == "__main__":
    asyncio.run(main())
