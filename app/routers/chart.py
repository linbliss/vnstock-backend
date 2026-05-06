"""
Chart router — cung cấp dữ liệu OHLCV + moving averages cho trang Charts.

GET /api/chart/history?ticker=VCB&period=6m
  → { candles: [...], ma20: [...], ma50: [...], ma150: [...], ma200: [...], pivot: float|null }

period: 1m | 3m | 6m | 1y | 2y | all  (default 6m)

Realtime: nếu trong/sau giờ giao dịch và market_service.quotes có data của ticker,
candle hôm nay được append/update trước khi tính MA → MA & chart luôn có ngày current.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query
import pandas as pd

from app.services import ohlcv_store
from app.services.market_data import market_service

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


def _is_market_open_or_post() -> bool:
    """Trading day, từ 9:00 đến 16:00 (bao gồm post-close 15:00-16:00 lúc ATO/data settle)."""
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    return 9 * 60 <= t <= 16 * 60


def _merge_intraday_candle(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Append/update candle hôm nay từ market_service.quotes nếu có data.
    Đảm bảo MA tính trên data có ngày hiện tại → chart không bị thiếu cây nến hôm nay.

    CRITICAL UNIT FIX: market_service.quotes trả giá ở REAL VND (28600),
    nhưng ohlcv_store lưu ở NGHÌN VND (28.6). Phải quy đổi trước khi merge,
    nếu không sẽ tạo 1 cây nến với giá gấp 1000x → chart squeeze hoàn toàn.
    """
    quote = market_service.quotes.get(ticker, {})
    if not quote:
        return df

    raw_price = float(quote.get('price', 0) or 0)
    if raw_price <= 0:
        return df

    # Detect unit của df (nghìn VND nếu giá trung bình < 1000 — chuẩn VN)
    sample_close = float(df['close'].iloc[-1]) if len(df) > 0 else raw_price
    df_in_kvnd   = sample_close < 1000

    # Quy đổi quote về cùng unit với df
    def _to_df_unit(v: float) -> float:
        if df_in_kvnd and v > 1000:
            return v / 1000.0
        return v

    price  = _to_df_unit(raw_price)
    open_p = _to_df_unit(float(quote.get('open',  raw_price) or raw_price))
    high_p = _to_df_unit(float(quote.get('high',  raw_price) or raw_price))
    low_p  = _to_df_unit(float(quote.get('low',   raw_price) or raw_price))
    volume = int(quote.get('volume', 0) or 0)

    today_str = date.today().isoformat()
    last_date = str(df['date'].iloc[-1]) if len(df) > 0 else None

    if last_date != today_str:
        # Chưa có candle hôm nay trong store → append
        if not _is_market_open_or_post():
            # Ngoài giờ + chưa có T row → có thể đang sáng sớm, đừng append (data có thể stale)
            return df
        new_row = pd.DataFrame([{
            'date':   today_str,
            'open':   open_p,
            'high':   high_p,
            'low':    low_p,
            'close':  price,
            'volume': volume,
        }])
        df = pd.concat([df, new_row], ignore_index=True)
    else:
        # Đã có candle hôm nay → update với intraday data realtime
        if _is_market_open_or_post():
            idx = df.index[-1]
            df.at[idx, 'high']   = max(float(df.at[idx, 'high']), high_p)
            df.at[idx, 'low']    = min(float(df.at[idx, 'low']),  low_p)
            df.at[idx, 'close']  = price
            df.at[idx, 'volume'] = volume
            # Open giữ nguyên — open của phiên là từ ATO đầu phiên
    return df


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

    # Merge realtime candle hôm nay từ market_service.quotes (nếu trong giờ GD)
    # → MA & chart luôn có ngày current, không phải đợi EOD sync
    df = _merge_intraday_candle(df, ticker)

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
