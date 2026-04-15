"""Admin endpoints: backfill, daily update, OHLCV stats."""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query

from app.services import ohlcv_store, backfill

router = APIRouter()


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


@router.post("/ohlcv/daily-update")
async def trigger_daily_update():
    """Chạy ngay daily_update (không chờ 16:00)."""
    result = await backfill.daily_update()
    return result


@router.get("/ohlcv/stats")
async def ohlcv_stats():
    return ohlcv_store.get_stats()


@router.get("/ohlcv/tickers")
async def ohlcv_tickers():
    tickers = ohlcv_store.list_tickers()
    return {"count": len(tickers), "tickers": tickers}


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
