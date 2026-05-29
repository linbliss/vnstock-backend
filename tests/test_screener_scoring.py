"""Regression tests cho scoring + VCP gating logic (app/services/screener.py).

Chạy được CẢ 2 cách:
  - pytest tests/test_screener_scoring.py
  - python tests/test_screener_scoring.py   (fallback không cần pytest)

Bao phủ các fix:
  1. compute_score — sửa scoring inversion + breakout bonus
  3. compute_score — gate tightness/rs_line bonus sau cấu trúc VCP
  2. compute_rs_line_breakout — date-align (không tail-align)
  4. evaluate_contractions — loose pairwise/vol scaling + gateway
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.screener import (  # noqa: E402
    compute_score,
    evaluate_contractions,
    compute_rs_line_breakout,
    _align_by_date,
)


# ── Helpers ────────────────────────────────────────────────────────────────────
def _vcp(**kw):
    """VCP dict tối thiểu cho compute_score; override field qua kwargs."""
    base = {
        "is_vcp": True, "contracting": True, "t_count": 3,
        "tightness": 4.0, "pivot_buy": 100.0, "diff_pivot_pct": 1.0,
        "above_pivot": False, "vol_confirmed": False, "vol_confirmed_strict": False,
    }
    base.update(kw)
    return base


def _contraction(depth, trough_avg_vol=100.0, is_volume_dry=False):
    return {"depth": depth, "trough_avg_vol": trough_avg_vol, "is_volume_dry": is_volume_dry}


# ── ITEM 1: Scoring inversion + breakout bonus ──────────────────────────────────
def test_breakout_with_volume_beats_pre_breakout():
    """Mã breakout có volume PHẢI điểm cao hơn mã chỉ đứng sát pivot (chưa BO)."""
    trend = {"score": 6}
    rs = 70.0
    pre_breakout = _vcp(diff_pivot_pct=1.0, above_pivot=False,
                        vol_confirmed=False, vol_confirmed_strict=False)
    breakout = _vcp(diff_pivot_pct=4.0, above_pivot=True,
                    vol_confirmed=True, vol_confirmed_strict=True)
    s_pre = compute_score(trend, rs, pre_breakout)["total_score"]
    s_bo  = compute_score(trend, rs, breakout)["total_score"]
    assert s_bo > s_pre, f"breakout {s_bo} phải > pre-breakout {s_pre}"
    # Chênh lệch đúng bằng breakout_bonus full (15)
    assert compute_score(trend, rs, breakout)["score_breakdown"]["breakout_bonus"] == 15


def test_strong_breakout_not_penalized_for_leaving_buy_zone():
    """Mã đã vượt pivot xa (diff=+8, ngoài buy zone) vẫn nhận breakout_bonus.

    Trước fix: near_pts=0 (|diff|>3) VÀ không có bonus → bị phạt nặng.
    """
    trend = {"score": 7}
    bd = compute_score(trend, 80.0, _vcp(diff_pivot_pct=8.0, above_pivot=True,
                                         vol_confirmed=True, vol_confirmed_strict=False)
                       )["score_breakdown"]
    assert bd["near_pivot"] == 0          # ngoài buy zone (diff=8)
    assert bd["breakout_bonus"] == 8      # vẫn được thưởng (vol loose ≥1.3)


def test_buy_zone_covers_just_above_pivot():
    """diff trong (-3, 5) = buy zone → near_pts=10 (gồm cả vừa vượt pivot)."""
    trend = {"score": 5}
    for diff in (-2.5, 0.0, 2.0, 4.9):
        bd = compute_score(trend, 60.0, _vcp(diff_pivot_pct=diff))["score_breakdown"]
        assert bd["near_pivot"] == 10, f"diff={diff} phải trong buy zone"
    for diff in (-5.0, 5.0, 9.0):
        bd = compute_score(trend, 60.0, _vcp(diff_pivot_pct=diff))["score_breakdown"]
        assert bd["near_pivot"] == 0, f"diff={diff} phải ngoài buy zone"


# ── ITEM 3: Gate bonuses sau cấu trúc VCP ───────────────────────────────────────
def test_tightness_bonus_gated_by_structure():
    """tightness_bonus chỉ tính khi t_count >= 2."""
    trend = {"score": 5}
    no_struct = compute_score(trend, 50.0, _vcp(t_count=1, tightness=2.0,
                                                is_vcp=False, contracting=False))
    assert no_struct["score_breakdown"]["tightness_bonus"] == 0
    struct = compute_score(trend, 50.0, _vcp(t_count=3, tightness=2.0))
    assert struct["score_breakdown"]["tightness_bonus"] == 10


def test_rs_line_bonus_requires_contracting():
    """rs_line_bonus chỉ ý nghĩa khi base đang thắt lại (contracting=True)."""
    trend = {"score": 5}
    off = compute_score(trend, 50.0, _vcp(contracting=False, is_vcp=False),
                        rs_line_breakout=True, rs_line_breakout_pct=1.5)
    assert off["score_breakdown"]["rs_line_bonus"] == 0
    on = compute_score(trend, 50.0, _vcp(contracting=True),
                       rs_line_breakout=True, rs_line_breakout_pct=1.5)
    assert on["score_breakdown"]["rs_line_bonus"] == 15


# ── ITEM 4: Loose pairwise/vol scaling + gateway ────────────────────────────────
def test_loose_t2_single_pair_no_violation_allowed():
    """t_count=2 (1 pair): 1 violation = 100% vi phạm → KHÔNG được pass (đã sửa)."""
    # depth tăng (10 → 12): reduction âm = 1 violation, và T_cuối > T_đầu
    cq = evaluate_contractions([_contraction(10), _contraction(12)], t_count=2)
    assert cq["loose_violations"] == 1
    assert cq["loose_pairwise_ok"] is False
    assert cq["contracting_loose"] is False


def test_loose_t2_valid_narrowing_passes():
    """t_count=2 thắt lại đúng (12 → 5): pass cả loose lẫn strict."""
    cq = evaluate_contractions([_contraction(12), _contraction(5)], t_count=2)
    assert cq["loose_violations"] == 0
    assert cq["loose_pairwise_ok"] is True
    assert cq["contracting_loose"] is True
    assert cq["strict_pairwise_ok"] is True   # 58% ≥ 50


def test_gateway_blocks_non_narrowing_sequence():
    """Gateway: T_cuối phải nông hơn T_đầu, nếu không → contracting_loose=False."""
    # [8, 6, 10]: pair ok-ish nhưng T_cuối(10) > T_đầu(8) → gateway chặn
    cq = evaluate_contractions(
        [_contraction(8), _contraction(6), _contraction(10)], t_count=3)
    assert cq["contracting_loose"] is False


def test_vol_decreasing_loose_scales_with_t_count():
    """vol_decreasing_loose = violations <= max(0, t_count-2)."""
    # t_count=2, trough vol tăng (100 → 200) = 1 violation → KHÔNG pass
    cq2 = evaluate_contractions(
        [_contraction(12, trough_avg_vol=100), _contraction(5, trough_avg_vol=200)],
        t_count=2)
    assert cq2["vol_decline_violations"] == 1
    assert cq2["vol_decreasing_loose"] is False
    # t_count=3, 1 violation → pass (max(0,1)=1)
    cq3 = evaluate_contractions(
        [_contraction(20, trough_avg_vol=100),
         _contraction(10, trough_avg_vol=80),
         _contraction(4,  trough_avg_vol=200)],
        t_count=3)
    assert cq3["vol_decline_violations"] == 1
    assert cq3["vol_decreasing_loose"] is True


# ── ITEM 2: compute_rs_line_breakout date-align ─────────────────────────────────
def _bdays(n):
    return pd.bdate_range("2024-01-01", periods=n)


def test_align_by_date_uses_common_dates():
    """_align_by_date lấy GIAO ngày, không tail-align theo vị trí."""
    dates_a = _bdays(80)
    dates_b = _bdays(85)[5:]   # lệch: bắt đầu trễ 5 phiên → giao = 75 ngày
    a = pd.Series(np.arange(80, dtype=float), index=dates_a)
    b = pd.Series(np.arange(85, dtype=float)[5:], index=dates_b)
    sa, sb = _align_by_date(a, b)
    assert len(sa) == len(sb)
    assert list(sa.index) == list(sb.index)       # cùng tập ngày
    assert (sa.index == sb.index).all()


def test_rs_line_breakout_detects_new_high():
    """RS line phiên cuối vượt đỉnh 20 phiên → breakout True, strength đúng."""
    n = 80
    idx = pd.Series(np.full(n, 100.0), index=_bdays(n))
    stock_vals = np.full(n, 200.0)
    stock_vals[-1] = 260.0                          # ratio 2.0 → 2.6
    stock = pd.Series(stock_vals, index=_bdays(n))
    is_break, strength = compute_rs_line_breakout(stock, idx, lookback=20)
    assert is_break is True
    assert abs(strength - 30.0) < 0.5               # (2.6/2.0 - 1)*100


def test_rs_line_breakout_no_signal_when_weak():
    """Phiên cuối thấp nhất → KHÔNG breakout."""
    n = 80
    idx = pd.Series(np.full(n, 100.0), index=_bdays(n))
    stock_vals = np.full(n, 200.0)
    stock_vals[-1] = 150.0                          # ratio rớt
    stock = pd.Series(stock_vals, index=_bdays(n))
    is_break, strength = compute_rs_line_breakout(stock, idx, lookback=20)
    assert is_break is False


def test_rs_line_breakout_handles_suspension_gap():
    """Mã nghỉ giao dịch (thiếu ngày) vẫn align đúng theo ngày, không crash."""
    n = 90
    all_dates = _bdays(n)
    idx = pd.Series(np.linspace(100, 189, n), index=all_dates)
    # stock bỏ 8 phiên giữa (suspension)
    keep = list(range(0, 40)) + list(range(48, n))
    s_dates = all_dates[keep]
    s_vals = (np.linspace(100, 189, n)[keep]) * 2.0
    s_vals[-1] *= 1.15                              # phiên cuối tăng tương đối
    stock = pd.Series(s_vals, index=s_dates)
    is_break, strength = compute_rs_line_breakout(stock, idx, lookback=20)
    assert is_break is True
    assert strength > 0


# ── Standalone runner (không cần pytest) ────────────────────────────────────────
if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  ✓ {t.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  ✗ {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ✗ {t.__name__}: {type(e).__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed (tổng {len(tests)})")
    sys.exit(1 if failed else 0)
