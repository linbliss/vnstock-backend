"""Router Shark Action — tín hiệu giao dịch "cá mập" từ luồng khớp lệnh intraday.
Dữ liệu thị trường công khai (read-only) nên không yêu cầu auth, giống snapshot."""
from fastapi import APIRouter, Query
from app.services import shark_monitor, shark_history

router = APIRouter(prefix="/api/shark", tags=["shark"])


@router.get("/signals")
def shark_signals(
    tickers: str = Query(..., description="Danh sách mã, phân tách bằng dấu phẩy"),
    big_value: float = Query(shark_monitor.BIG_VALUE_VND, ge=0),
    window_min: int = Query(shark_monitor.WINDOW_MIN, ge=1, le=120),
):
    """Tín hiệu gọn cho nhiều mã (danh sách theo dõi)."""
    ts = [t.strip().upper() for t in tickers.split(",") if t.strip()][:15]
    return {"signals": [shark_monitor.get_signal(t, big_value, window_min) for t in ts]}


@router.get("/tape/{ticker}")
def shark_tape(
    ticker: str,
    limit: int = Query(2000, ge=1, le=5000),
    big_value: float = Query(shark_monitor.BIG_VALUE_VND, ge=0),
    window_min: int = Query(shark_monitor.WINDOW_MIN, ge=1, le=120),
):
    """Tape (khớp lệnh gần nhất) + đầy đủ metrics cho 1 mã."""
    return shark_monitor.get_tape(ticker, limit, big_value, window_min)


@router.get("/orderflow/{ticker}")
def shark_orderflow(ticker: str):
    """Order Flow Analyzer: CVD, Volume Profile, VWAP, lệnh lớn (ngưỡng thích ứng),
    absorption, iceberg (thử nghiệm) — tính trên tape đã cache của phiên."""
    return shark_monitor.get_orderflow(ticker)


@router.get("/context/{ticker}")
def shark_context(ticker: str, with_foreign: bool = Query(True)):
    """Layer 0 — Context Engine: trend/MA, vị trí (S/R, POC, VA, VWAP), pha phiên,
    hướng khối ngoại/tự doanh. Ngữ cảnh để Layer 2/3 diễn giải event dòng tiền."""
    return shark_monitor.get_context(ticker, with_foreign=with_foreign)


@router.get("/health/{ticker}")
def shark_tape_health(ticker: str, cross_check: bool = Query(False)):
    """Phase 0 — độ tin của trường `side` (nền của CVD/imbalance/absorption):
    phân bố B/S/U, %U, đồng thuận tick-rule, tỉ lệ nguồn DNSE/vnstock.
    cross_check=true: đối chiếu side DNSE với KBS (chạy trên server)."""
    return shark_monitor.get_tape_health(ticker, cross_check=cross_check)


@router.get("/history/{ticker}")
def shark_history_one(
    ticker: str,
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
):
    """Đánh giá cá mập theo KỲ (dòng tiền ngày: khối ngoại/tự doanh/chủ động/thoả thuận)."""
    return shark_history.get_history(ticker, start, end)


@router.get("/history-signals")
def shark_history_signals(
    tickers: str = Query(..., description="Danh sách mã, phân tách bằng dấu phẩy"),
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
):
    """Tín hiệu theo KỲ (gọn) cho nhiều mã."""
    ts = [t.strip().upper() for t in tickers.split(",") if t.strip()][:15]
    return {"signals": [shark_history.get_history_signal(t, start, end) for t in ts]}
