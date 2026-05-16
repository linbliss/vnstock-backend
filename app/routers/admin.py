"""Admin endpoints: backfill, daily update, OHLCV stats.

Auth: mỗi request phải có header `X-Admin-Token` khớp env `ADMIN_TOKEN`.
Nếu `ADMIN_TOKEN` không set → endpoint trả 503 để tránh mở cửa toàn bộ ra public.
"""
import os
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Header, Depends

from app.services import ohlcv_store, backfill


def _require_admin(x_admin_token: Optional[str] = Header(default=None)) -> None:
    expected = os.environ.get("ADMIN_TOKEN", "").strip()
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="ADMIN_TOKEN chưa được cấu hình trên server",
        )
    if not x_admin_token or x_admin_token.strip() != expected:
        raise HTTPException(status_code=401, detail="invalid admin token")


router = APIRouter(dependencies=[Depends(_require_admin)])


@router.post("/backfill")
async def start_backfill(
    scope: str = Query(..., description="VN30 | HOSE | HNX | UPCOM | HOSE_HNX | ALL | csv list"),
    years: int = Query(default=10, ge=1, le=30),
    start_date: Optional[str] = Query(default=None, description="YYYY-MM-DD (override years)"),
    end_date:   Optional[str] = Query(default=None, description="YYYY-MM-DD (default today)"),
):
    """Khởi tạo backfill job (background). Trả về job_id để poll tiến độ."""
    try:
        info = await backfill.start_backfill(scope, years, start_date, end_date)
        return {"ok": True, **info}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except BaseException as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")


@router.get("/backfill/status/{job_id}")
async def backfill_status(job_id: str):
    job = ohlcv_store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@router.get("/backfill/jobs")
async def backfill_jobs(limit: int = Query(default=20, ge=1, le=100)):
    return {"jobs": ohlcv_store.list_jobs(limit)}


@router.post("/backfill/cancel/{job_id}")
async def backfill_cancel(job_id: str):
    ok = backfill.cancel_backfill(job_id)
    if not ok:
        raise HTTPException(status_code=404, detail="job not active")
    return {"ok": True, "job_id": job_id, "message": "cancel requested"}


@router.post("/backfill/mark-stale")
async def backfill_mark_stale():
    """Đánh dấu mọi job đang 'running' thành 'cancelled'.
    Dùng sau khi restart container — task in-memory đã mất, DB còn lưu status cũ."""
    import sqlite3
    from datetime import datetime
    with sqlite3.connect(ohlcv_store.DB_PATH) as conn:
        cur = conn.execute(
            "UPDATE backfill_job SET status='cancelled', finished_at=?, "
            "message=COALESCE(message,'')||' [auto-marked stale]' "
            "WHERE status='running'",
            (datetime.now().isoformat(),),
        )
        n = cur.rowcount
    return {"ok": True, "marked": n}


@router.post("/ohlcv/daily-update")
async def trigger_daily_update():
    """Chạy ngay daily_update (không chờ 16:00)."""
    result = await backfill.daily_update()
    return result


@router.post("/fundamentals/refresh")
async def trigger_fundamental_refresh():
    """Chạy ngay fundamental refresh (không chờ thứ Hai)."""
    result = await backfill.refresh_fundamentals()
    return result


@router.get("/fundamentals/stats")
async def fundamental_stats():
    """Thống kê fundamentals cache."""
    return ohlcv_store.get_fundamental_stats()


@router.get("/ohlcv/stats")
async def ohlcv_stats():
    stats = ohlcv_store.get_stats()
    stats["fundamentals"] = ohlcv_store.get_fundamental_stats()
    return stats


@router.get("/ohlcv/tickers")
async def ohlcv_tickers():
    tickers = ohlcv_store.list_tickers()
    return {"count": len(tickers), "tickers": tickers}


@router.post("/ohlcv/refetch/{ticker}")
async def refetch_ticker_ohlcv(
    ticker: str,
    years: int = Query(default=3, ge=1, le=15, description="Số năm lịch sử cần fetch lại"),
):
    """Xoá OHLCV cũ và fetch lại toàn bộ lịch sử cho 1 ticker.
    Dùng khi corporate action (thưởng CP, tách/gộp cổ phiếu) làm giá lịch sử bị sai."""
    n = await backfill.refetch_ticker(ticker.upper(), years=years)
    return {"ticker": ticker.upper(), "rows_fetched": n, "years": years, "ok": n > 0}


@router.get("/ohlcv/{ticker}")
async def ohlcv_detail(
    ticker: str,
    start: Optional[str] = None,
    end:   Optional[str] = None,
    limit: int = Query(default=30, ge=1, le=5000),
):
    df = ohlcv_store.get_ohlcv(ticker.upper(), start, end)
    if df is None:
        return {"ticker": ticker.upper(), "rows": 0, "data": []}
    tail = df.tail(limit)
    return {
        "ticker": ticker.upper(),
        "rows":   len(df),
        "first":  df["date"].iloc[0],
        "last":   df["date"].iloc[-1],
        "data":   tail.to_dict("records"),
    }
