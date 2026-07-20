"""Admin endpoints: backfill, daily update, OHLCV stats.

Auth: mỗi request phải có header `X-Admin-Token` khớp env `ADMIN_TOKEN`.
Nếu `ADMIN_TOKEN` không set → endpoint trả 503 để tránh mở cửa toàn bộ ra public.
"""
import asyncio
import os
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Header, Depends

from app.services import ohlcv_store, backfill, data_source, dnse_client


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


@router.get("/data-source")
async def get_data_source():
    """Cấu hình nguồn dữ liệu theo module + trạng thái DNSE hiện tại."""
    return {
        "modules": data_source.MODULES,          # key → nhãn
        "values": list(data_source.VALUES),      # ["dnse","vnstock"]
        "current": data_source.get_all(),        # key → giá trị đang chọn
        "dnse_configured": bool(dnse_client._key() and dnse_client._secret()),
        "dnse_available": dnse_client.enabled(),  # False nếu chưa key hoặc đang bị breaker ngắt
    }


@router.get("/dnse/health")
async def dnse_health():
    """Chẩn đoán DNSE: REST có bị chặn không + WS có nhận tick không + vì sao.
    Dò thật (1 request tới endpoint hạn mức cao) nên chỉ gọi khi người dùng bấm."""
    from app.services import dnse_feed
    loop = asyncio.get_event_loop()
    rest = await loop.run_in_executor(None, dnse_client.health_check)
    ws = dnse_feed.stats()
    shark_src = data_source.get_source("shark")

    # Vì sao WS chưa có tick? Đây mới là thứ hay bị hiểu nhầm là "bị chặn".
    hint = None
    if not rest["ok"] and rest["state"] == "blocked" and ws.get("connected"):
        hint = ("REST (openapi.dnse.com.vn) bị chặn nhưng WS (ws-openapi.dnse.com.vn) là "
                "HOST KHÁC và vẫn chạy → Shark vẫn có realtime qua WS, chỉ phần nạp đầu "
                "phiên lùi về vnstock. Không cần đổi gì.")
    elif shark_src != "dnse":
        hint = (f"Shark đang đặt nguồn '{shark_src}' → không đăng ký mã nào với DNSE. "
                f"Đổi Shark sang 'dnse' ở mục Nguồn dữ liệu để dùng WS.")
    elif not ws.get("enabled"):
        hint = "WS feed đang tắt (DNSE_WS_ENABLED=false)."
    elif not ws.get("connected"):
        hint = "WS chưa kết nối được — xem REST ở trên để biết có bị chặn không."
    elif not ws.get("subscribed"):
        hint = ("WS đã kết nối nhưng chưa mã nào được đăng ký — hãy MỞ trang Shark Action "
                "(feed chỉ theo dõi mã đang xem, hết 5 phút không xem thì bỏ).")
    elif not ws.get("ticks"):
        hint = ("Đã đăng ký nhưng chưa có tick — ngoài giờ khớp lệnh, đang ATO/nghỉ trưa, "
                "hoặc mã thanh khoản thấp chưa khớp.")

    from urllib.parse import urlparse as _up
    return {"rest": rest, "ws": ws, "breaker": dnse_client.breaker_state(),
            "shark_source": shark_src, "hint": hint,
            "hosts": {"rest": _up(dnse_client.REST_BASE).hostname,
                      "ws": _up(dnse_feed.WS_BASE).hostname}}


@router.post("/data-source")
async def set_data_source(module: str = Query(...), value: str = Query(...)):
    """Đặt nguồn cho 1 module. value: 'dnse' | 'vnstock'."""
    if not data_source.set_source(module, value):
        raise HTTPException(status_code=400,
                            detail=f"module/value không hợp lệ (module∈{list(data_source.MODULES)}, value∈{list(data_source.VALUES)})")
    return {"ok": True, "current": data_source.get_all()}


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


def _merge_live(job: dict) -> dict:
    """Ghép tiến độ realtime (bộ nhớ) vào job đọc từ DB — cập nhật tức thì, không lag."""
    if not job:
        return job
    live = backfill.get_live(job.get("job_id") or job.get("id"))
    if live:
        job = {**job, "completed": live.get("completed", job.get("completed")),
               "failed": live.get("failed", job.get("failed"))}
        if live.get("status"):
            job["status"] = live["status"]
    return job


@router.get("/backfill/status/{job_id}")
async def backfill_status(job_id: str):
    job = ohlcv_store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return _merge_live(job)


@router.get("/backfill/jobs")
async def backfill_jobs(limit: int = Query(default=20, ge=1, le=100)):
    return {"jobs": [_merge_live(j) for j in ohlcv_store.list_jobs(limit)]}


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


_readjust_running = {"on": False, "result": None}


@router.post("/ohlcv/verify-adjust")
async def verify_adjust(scope: Optional[str] = Query(
        default=None, description="VN30 | HOSE | HNX | UPCOM | ALL | csv; bỏ trống = toàn store")):
    """Rà điều chỉnh giá ngược (thưởng CP / cổ tức CP / tách-gộp) cho toàn bộ (hoặc 1
    nhóm) mã và refetch những mã bị lệch. Chạy NỀN — trả về ngay, xem tiến độ ở log.
    Dùng để sửa dữ liệu tồn đọng mà không phải refetch từng mã thủ công."""
    if _readjust_running["on"]:
        return {"ok": False, "message": "đang chạy rồi — chờ hoàn tất"}

    tickers = None
    if scope:
        try:
            tickers = await backfill.get_tickers_for_scope(scope)
        except Exception as e:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(e))

    async def _run():
        _readjust_running["on"] = True
        try:
            _readjust_running["result"] = await backfill.verify_and_readjust(tickers)
        finally:
            _readjust_running["on"] = False

    asyncio.create_task(_run())
    n = len(tickers) if tickers else "toàn bộ"
    return {"ok": True, "message": f"Đã bắt đầu rà điều chỉnh giá ({n} mã) — xem log/tiến độ."}


@router.get("/ohlcv/verify-adjust/status")
async def verify_adjust_status():
    """Trạng thái lần rà điều chỉnh gần nhất."""
    return {"running": _readjust_running["on"], "result": _readjust_running["result"]}


_shark_rebuild_running = {"on": False, "result": None}


@router.post("/shark/rebuild-watchlist")
async def shark_rebuild_watchlist():
    """Dựng lại trọn tape phiên cho TẤT CẢ mã watchlist (DNSE nếu được, không thì vnstock).
    Chạy NỀN — trả về ngay, xem tiến độ ở log / status."""
    if _shark_rebuild_running["on"]:
        return {"ok": False, "message": "đang chạy rồi — chờ hoàn tất"}
    from app.services import shark_monitor

    async def _run():
        _shark_rebuild_running["on"] = True
        try:
            _shark_rebuild_running["result"] = await shark_monitor.rebuild_watchlist()
        finally:
            _shark_rebuild_running["on"] = False

    asyncio.create_task(_run())
    return {"ok": True, "message": "Đã bắt đầu dựng lại tape watchlist — xem log/status."}


@router.get("/shark/rebuild-watchlist/status")
async def shark_rebuild_watchlist_status():
    return {"running": _shark_rebuild_running["on"], "result": _shark_rebuild_running["result"]}


@router.get("/shark/backtest")
async def shark_backtest(horizons: str = Query(default="1,3,5", description="vd 1,3,5,10"),
                         min_date: Optional[str] = Query(default=None, description="YYYY-MM-DD")):
    """Đo điểm Shark có DỰ BÁO được giá không: lợi suất T+h sau mỗi tín hiệu, so mức nền."""
    from app.services import shark_backtest as bt
    try:
        hs = [int(x) for x in horizons.split(",") if x.strip()]
    except ValueError:
        raise HTTPException(status_code=400, detail="horizons phải là số, vd '1,3,5'")
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, bt.run_backtest, hs, min_date)


@router.post("/shark/rebuild/{ticker}")
async def shark_rebuild(ticker: str, date: Optional[str] = Query(
        default=None, description="YYYY-MM-DD; bỏ trống = tự tìm phiên gần nhất")):
    """Dựng lại TRỌN tape 1 phiên cho 1 mã (sửa tape thiếu phần sáng)."""
    from app.services import shark_monitor
    loop = asyncio.get_event_loop()
    res = await loop.run_in_executor(None, shark_monitor.rebuild_session, ticker.upper(), date)
    return res


@router.post("/stock-list/refresh")
async def refresh_stock_list():
    """Fetch toàn bộ danh sách mã CK từ vnstock Listing và lưu vào SQLite.
    Chạy 1 lần sau khi deploy, hoặc khi cần cập nhật mã mới lên/xuống sàn."""
    loop = asyncio.get_event_loop()

    # DNSE trước (nếu có key)
    try:
        from app.services import dnse_client, data_source
        if data_source.use_dnse("ticker_list"):
            stocks_dnse = await loop.run_in_executor(None, dnse_client.get_stock_list)
            if stocks_dnse:
                n = ohlcv_store.upsert_stock_list(stocks_dnse)
                return {"ok": True, "count": n, "source": "DNSE",
                        "stats": ohlcv_store.get_stock_list_stats()}
    except Exception as e:  # noqa: BLE001
        print(f"⚠️  stock-list DNSE: {type(e).__name__}: {e}", flush=True)

    def _fetch() -> list:
        from vnstock import Listing
        df = Listing().symbols_by_exchange()
        if df is None or df.empty:
            return []
        mask = (df["type"] == "stock") & (df["exchange"].isin(["HOSE", "HNX", "UPCOM"]))
        filtered = df[mask][["symbol", "organ_name", "exchange"]].drop_duplicates("symbol")
        result = []
        for _, r in filtered.iterrows():
            ticker = str(r["symbol"]).strip().upper()
            name   = str(r["organ_name"]).strip() if r["organ_name"] else ""
            exch   = str(r["exchange"]).strip().upper()
            if ticker and exch in ("HOSE", "HNX", "UPCOM"):
                result.append({"ticker": ticker, "name": name, "exchange": exch})
        return result

    try:
        stocks = await loop.run_in_executor(None, _fetch)
    except BaseException as e:
        raise HTTPException(status_code=500, detail=f"vnstock error: {type(e).__name__}: {e}")

    if not stocks:
        raise HTTPException(status_code=500, detail="Không lấy được dữ liệu từ vnstock")

    n = ohlcv_store.upsert_stock_list(stocks)
    stats = ohlcv_store.get_stock_list_stats()
    return {"ok": True, "count": n, "stats": stats}


@router.get("/stock-list/stats")
async def stock_list_stats():
    """Thống kê bảng stock_list."""
    return ohlcv_store.get_stock_list_stats()


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
