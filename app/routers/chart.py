"""
Chart router — cung cấp dữ liệu OHLCV + moving averages cho trang Charts.

GET /api/chart/history?ticker=VCB&period=6m
  → { candles: [...], ma20: [...], ma50: [...], ma150: [...], ma200: [...], pivot: float|null }

period: 1m | 3m | 6m | 1y | 2y | all  (default 6m)
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query
import pandas as pd

from app.services import ohlcv_store

router = APIRouter()


def _period_start(period: str) -> Optional[str]:
    today = date.today()
    mapping = {
        "1m":  30,
        "3m":  90,
        "6m":  180,
        "1y":  365,
        "2y":  730,
    }
    if period == "all":
        return None
    days = mapping.get(period, 180)
    return (today - timedelta(days=days)).isoformat()


def _ma(series: pd.Series, n: int) -> List[Optional[float]]:
    """Rolling mean, NaN → None (JSON null)."""
    rolled = series.rolling(n).mean()
    return [None if pd.isna(v) else round(float(v), 2) for v in rolled]


def _ema(series: pd.Series, n: int) -> List[Optional[float]]:
    """EMA, NaN → None."""
    rolled = series.ewm(span=n, adjust=False).mean()
    return [None if pd.isna(v) else round(float(v), 2) for v in rolled]


@router.get("/history")
async def chart_history(
    ticker: str = Query(..., description="Mã CK, e.g. VCB"),
    period: str = Query("6m", description="1m|3m|6m|1y|2y|all"),
):
    """Trả về OHLCV candles + MA lines cho biểu đồ kỹ thuật."""
    ticker = ticker.upper().strip()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker is required")

    start = _period_start(period)

    # Nếu period ngắn, cần thêm data để tính MA200 đủ độ
    # → load thêm 200 ngày extra để đảm bảo MA200 không toàn None
    if start is not None:
        extra_start_date = date.fromisoformat(start) - timedelta(days=220)
        load_start = extra_start_date.isoformat()
    else:
        load_start = None

    df = ohlcv_store.get_ohlcv(ticker, start=load_start, end=None)
    if df is None or df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Không có dữ liệu OHLCV cho {ticker}. "
                   "Vui lòng chạy backfill trước.",
        )

    close = df["close"].astype(float)

    # Tính các MA / EMA trên toàn bộ data đã load (kể cả vùng extra)
    ma20_all  = _ma(close, 20)
    ma50_all  = _ma(close, 50)
    ma150_all = _ma(close, 150)
    ma200_all = _ma(close, 200)
    ema20_all = _ema(close, 20)

    # Cắt về window hiển thị (từ start trở đi)
    if start is not None:
        mask = df["date"] >= start
        df_view    = df[mask].reset_index(drop=True)
        start_idx  = df[mask].index[0] if mask.any() else len(df)
    else:
        df_view   = df
        start_idx = 0

    def _slice(lst: list) -> list:
        return lst[start_idx:]

    # Build candles list
    candles = []
    for _, row in df_view.iterrows():
        candles.append({
            "date":   row["date"],
            "open":   float(row["open"]),
            "high":   float(row["high"]),
            "low":    float(row["low"]),
            "close":  float(row["close"]),
            "volume": int(row["volume"]),
        })

    # Xác định pivot gần nhất (swing-high 20 ngày cuối)
    pivot: Optional[float] = None
    if len(df_view) >= 5:
        last20 = df_view["high"].tail(20).astype(float)
        pivot  = round(float(last20.max()), 2)

    return {
        "ticker":  ticker,
        "period":  period,
        "candles": candles,
        "ma20":    _slice(ema20_all),   # dùng EMA20 (responsive hơn)
        "ma50":    _slice(ma50_all),
        "ma150":   _slice(ma150_all),
        "ma200":   _slice(ma200_all),
        "pivot":   pivot,
    }
