"""Backtest router — mô phỏng chiến lược VCP breakout + trailing stop (tham số hoá).

  GET  /api/backtest/cache-status         → trạng thái cache tín hiệu
  POST /api/backtest/run                  → chạy mô phỏng với tham số (nhanh, đọc cache)
  POST /api/backtest/build-cache  (admin) → quét & dựng cache tín hiệu (nền, ~30 phút)
"""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.services import backtest_service as bt
from app.services import ohlcv_store
from app.routers.admin import _require_admin

router = APIRouter()


class BacktestParams(BaseModel):
    min_avg_vol: float = Field(300_000, ge=0)        # KLGD TB 50 phiên trước breakout
    trail_pct: float = Field(0.08, gt=0, le=0.5)     # trailing khi đã có lãi
    init_stop_pct: float = Field(0.07, gt=0, le=0.5)  # stop cứng khi chưa có lãi
    position_vnd: float = Field(10_000_000, gt=0)
    fee_roundtrip: float = Field(0.003, ge=0, le=0.05)
    intraday: bool = True
    exchanges: Optional[List[str]] = None            # ["HOSE","HNX","UPCOM"] | None=tất cả
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    # ── danh mục có giới hạn ──
    portfolio_mode: bool = False
    initial_capital: float = Field(100_000_000, gt=0)
    max_positions: int = Field(10, ge=0, le=200)              # Y
    max_positions_grown: int = Field(0, ge=0, le=500)         # Z
    sizing_mode: str = "equal"                       # fixed | equal | percent | unit
    position_pct: float = Field(0.10, gt=0, le=1)
    compound: bool = True
    rotation: str = "weakest"                        # off | weakest | take_profit
    pyramid: bool = False
    pyramid_max: float = Field(3, ge=1, le=10)


def _exch_map():
    try:
        return {s["ticker"]: s.get("exchange") for s in ohlcv_store.get_stock_list()}
    except Exception:
        return {}


@router.get("/cache-status")
def cache_status():
    return bt.build_status()


@router.get("/latest")
def latest():
    """Kết quả backtest gần nhất đã lưu (xem lại đa thiết bị, không cần chạy lại)."""
    return bt.get_last_result() or {}


@router.post("/run")
def run(params: BacktestParams):
    sigs = bt.get_cached_signals()
    if not sigs:
        raise HTTPException(
            status_code=409,
            detail="Cache tín hiệu chưa được dựng. Gọi POST /api/backtest/build-cache (admin) trước.")
    p = params.model_dump()
    result = bt.run_backtest(sigs, bt._get_df_from_store, exch_map=_exch_map(), params=p)
    bt.save_last_result(p, result)          # lưu để xem lại sau / máy khác
    return result


@router.post("/build-cache", dependencies=[Depends(_require_admin)])
def build_cache():
    return bt.start_build_async()
