"""shark_backtest — đo điểm Shark có DỰ BÁO được giá không.

Câu hỏi duy nhất đáng trả lời: sau tín hiệu "Gom hàng", giá 1/3/5 phiên sau có tốt hơn
mức nền không? Mọi thứ khác (tương quan, tautology…) chỉ là tính chất công thức.

Cách đo:
  • Mẫu   = mỗi (mã, ngày) có điểm Shark đã lưu (bảng shark_score).
  • Lợi suất kỳ vọng = close[T+h] / close[T] − 1, lấy từ OHLCV đã lưu (nhiều năm).
  • So NHÓM tín hiệu với MỨC NỀN (trung bình toàn bộ mẫu cùng kỳ) — bắt buộc, vì lợi
    suất bị chi phối bởi xu hướng chung của thị trường; không trừ nền thì "Gom hàng"
    trông có lãi chỉ vì hôm đó cả thị trường tăng.
  • t-stat để biết chênh lệch có phải nhiễu không (|t| < 2 ⇒ chưa kết luận được).

GIỚI HẠN THÀNH THẬT: kết quả chỉ đáng tin khi đã tích đủ mẫu qua nhiều phiên. Điểm chỉ
bắt đầu được lưu từ khi bật tính năng, nên những ngày đầu n rất nhỏ → đọc là "chưa đủ
dữ liệu", KHÔNG phải "không có tác dụng".
"""
from __future__ import annotations
from typing import Dict, List, Optional

from app.services import ohlcv_store, tape_store

GOM = 25        # ngưỡng nhãn (khớp shark_monitor)
XA = -25


def _mean(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _median(xs: List[float]) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def _std(xs: List[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    m = _mean(xs)
    return (sum((x - m) ** 2 for x in xs) / (n - 1)) ** 0.5


def _fwd_returns(ticker: str, horizons: List[int]) -> Dict[str, Dict[int, float]]:
    """{date: {h: lợi suất %}} cho 1 mã, tính từ OHLCV đã lưu."""
    df = ohlcv_store.get_ohlcv(ticker)
    if df is None or df.empty:
        return {}
    dates = df["date"].tolist()
    closes = df["close"].tolist()
    pos = {d: i for i, d in enumerate(dates)}
    out: Dict[str, Dict[int, float]] = {}
    for d, i in pos.items():
        r: Dict[int, float] = {}
        base = closes[i]
        if not base or base <= 0:
            continue
        for h in horizons:
            j = i + h
            if j < len(closes) and closes[j] is not None:
                r[h] = (closes[j] - base) / base * 100.0
        if r:
            out[d] = r
    return out


def run_backtest(horizons: Optional[List[int]] = None,
                 min_date: Optional[str] = None) -> dict:
    """Chạy backtest trên toàn bộ điểm Shark đã lưu."""
    horizons = horizons or [1, 3, 5]
    samples = tape_store.all_scores(min_date)
    if not samples:
        return {"ok": False, "n_samples": 0,
                "message": "Chưa có điểm Shark nào được lưu — cần chạy qua vài phiên đã."}

    # Gom mẫu theo mã để chỉ đọc OHLCV 1 lần/mã
    by_tk: Dict[str, List[dict]] = {}
    for s in samples:
        by_tk.setdefault(s["ticker"], []).append(s)

    rows: List[dict] = []
    for tk, ss in by_tk.items():
        fwd = _fwd_returns(tk, horizons)
        if not fwd:
            continue
        for s in ss:
            r = fwd.get(s["date"])
            if r:
                rows.append({**s, "fwd": r})

    if not rows:
        return {"ok": False, "n_samples": len(samples), "n_matched": 0,
                "message": "Có điểm nhưng chưa khớp được OHLCV (chưa đủ phiên SAU tín hiệu "
                           "để tính lợi suất, hoặc thiếu dữ liệu giá)."}

    def bucket(score: float) -> str:
        return "Gom hàng" if score >= GOM else ("Xả hàng" if score <= XA else "Trung tính")

    result: Dict[str, dict] = {}
    for h in horizons:
        allr = [r["fwd"][h] for r in rows if h in r["fwd"]]
        if not allr:
            continue
        base_mean = _mean(allr)          # MỨC NỀN: trung bình toàn mẫu
        per: Dict[str, dict] = {}
        for name in ("Gom hàng", "Trung tính", "Xả hàng"):
            xs = [r["fwd"][h] for r in rows if h in r["fwd"] and bucket(r["score"]) == name]
            n = len(xs)
            if n == 0:
                per[name] = {"n": 0}
                continue
            m, sd = _mean(xs), _std(xs)
            edge = m - base_mean
            t = (edge / (sd / (n ** 0.5))) if (sd > 0 and n > 1) else 0.0
            per[name] = {
                "n": n,
                "mean_pct": round(m, 3),
                "median_pct": round(_median(xs), 3),
                "win_rate_pct": round(sum(1 for x in xs if x > 0) / n * 100, 1),
                "edge_vs_base_pct": round(edge, 3),   # ← con số đáng nhìn nhất
                "t_stat": round(t, 2),
                "significant": bool(abs(t) >= 2 and n >= 30),
            }
        result[f"T+{h}"] = {"baseline_mean_pct": round(base_mean, 3),
                            "n_all": len(allr), "buckets": per}

    dates = sorted({r["date"] for r in rows})
    enough = len(rows) >= 100
    return {
        "ok": True,
        "n_scores_stored": len(samples),
        "n_matched": len(rows),
        "n_tickers": len(by_tk),
        "date_from": dates[0] if dates else None,
        "date_to": dates[-1] if dates else None,
        "horizons": result,
        "enough_data": enough,
        "note": ("Đủ mẫu để bắt đầu đọc kết quả." if enough else
                 "MẪU CÒN QUÁ ÍT (<100) — số liệu chỉ mang tính tham khảo, chưa kết luận "
                 "được. Cứ để hệ thống chạy tích thêm phiên rồi chạy lại."),
        "how_to_read": "edge_vs_base_pct > 0 và significant=true ở nhóm 'Gom hàng' "
                       "⇒ tín hiệu có giá trị dự báo. |t_stat| < 2 ⇒ chưa phân biệt được với nhiễu.",
    }
