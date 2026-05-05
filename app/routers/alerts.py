import os
import httpx
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from app.routers.auth import get_current_user
from app.services import user_store

router = APIRouter()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

async def send_telegram(text: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        return False
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
            )
            return resp.status_code == 200
    except Exception as e:
        print(f"Telegram error: {e}")
        return False

@router.get("/log")
def get_alert_log(
    limit: int = 30,
    current_user: dict = Depends(get_current_user),
):
    """Lấy lịch sử cảnh báo đã gửi gần nhất."""
    return user_store.get_recent_alerts(current_user["id"], limit=min(limit, 100))


@router.get("/test")
async def test_alert():
    ok = await send_telegram(
        "✅ <b>VN Stock Manager</b>\nAlerts đang hoạt động!\nBạn sẽ nhận cảnh báo giá tại đây."
    )
    return {"sent": ok, "token_set": bool(BOT_TOKEN), "chat_set": bool(CHAT_ID)}

@router.get("/status")
async def alert_status():
    """Trả về trạng thái alert engine — alerts được quản lý qua SQLite watchlist_items."""
    from app.services.alert_engine import _states
    return {
        "engine": "running",
        "users": len(_states),
        "note": "Alerts được quản lý qua Watchlist (alert_price) và VCP engine trên backend."
    }


@router.get("/debug/buy/{ticker}")
async def debug_buy_alert(ticker: str):
    """
    Debug: tại sao 1 mã không (chưa) được cảnh báo 3B.
    Trả về toàn bộ điều kiện đã/chưa đạt cho từng user state.
    """
    from datetime import datetime
    from app.services.alert_engine import _states, _is_trading, _sepa, VCP_MAX_ALERTS
    from app.services.market_data import market_service
    from app.services.screener import screener_service

    sym = ticker.upper()
    now = datetime.now()
    quote = market_service.quotes.get(sym, {})

    # Phân tích VCP/SEPA mới (không qua cache)
    try:
        r = await screener_service._analyze_ticker(sym)
    except Exception as e:
        return {"error": f"analyze failed: {type(e).__name__}: {e}"}

    if not r:
        return {"ticker": sym, "error": "Không đủ dữ liệu OHLCV"}

    vcp = r.get("vcp", {})
    is_vcp    = bool(vcp.get("is_vcp"))
    pivot_buy = float(vcp.get("pivot_buy") or 0)
    vol_ratio = float(vcp.get("vol_ratio") or 0)

    price = float(quote.get("price") or r.get("price") or 0)
    # Quy đổi sang nghìn VND cho đồng bộ với pivot_buy
    price_kvnd = price / 1000.0 if price > 1000 else price
    diff_pct   = (price_kvnd - pivot_buy) / pivot_buy * 100 if pivot_buy else 999
    price_pct  = abs(diff_pct)
    BREAKOUT_MAX_PCT = 5.0

    # Per-user diagnostics
    per_user = []
    for uid, state in _states.items():
        in_wl = any(it["ticker"] == sym for it in state.watchlist_items)
        sepa_min  = state.sepa_min()
        pivot_pct = state.vcp_pivot_pct()
        vol_mult  = state.vcp_vol_mult()
        vcp_max   = state.vcp_max()
        interval  = state.vcp_interval()
        vs        = state.vcp_state.get(sym, {"count": 0, "last_sent": None})

        # Tính SEPA
        try:
            score, criteria = await _sepa(state, sym)
        except Exception as e:
            score, criteria = -1, {"error": str(e)}

        # Đánh giá từng điều kiện
        # Biên 2 phía: dưới pivot ≤ pivot_pct, trên pivot ≤ 5% (breakout)
        if diff_pct >= 0:
            zone_pass = diff_pct <= BREAKOUT_MAX_PCT
            zone_label = f"breakout_<= {BREAKOUT_MAX_PCT:.1f}%"
        else:
            zone_pass = abs(diff_pct) <= pivot_pct
            zone_label = f"near_pivot_<= {pivot_pct:.1f}%"
        checks = {
            "user_buy_enabled":     state.buy_enabled(),
            "trading_hours":        _is_trading(),
            "in_watchlist":         in_wl,
            "is_vcp":               is_vcp,
            "has_pivot_buy":        pivot_buy > 0,
            zone_label:             zone_pass,
            f"vol_ratio_>= {vol_mult:.2f}x": vol_ratio >= vol_mult,
            f"sepa_>= {sepa_min}":  score >= sepa_min,
            f"count_< {vcp_max}":   vs["count"] < vcp_max,
            "cooldown_passed":      not vs["last_sent"] or (now - vs["last_sent"]).total_seconds() >= interval * 60,
        }
        all_passed = all(checks.values())

        per_user.append({
            "user_id": uid,
            "settings": {
                "buy_enabled": state.buy_enabled(),
                "sepa_min": sepa_min,
                "pivot_pct": pivot_pct,
                "vol_mult": vol_mult,
                "vcp_max": vcp_max,
                "interval_min": interval,
            },
            "watchlist_size": len(state.watchlist_items),
            "in_watchlist": in_wl,
            "vcp_state": {
                "count": vs["count"],
                "last_sent": vs["last_sent"].isoformat() if vs["last_sent"] else None,
            },
            "sepa_score": score,
            "checks": checks,
            "all_passed": all_passed,
        })

    return {
        "ticker": sym,
        "now": now.isoformat(),
        "trading_hours": _is_trading(),
        "price": price,
        "vcp": {
            "is_vcp": is_vcp,
            "pivot_buy_kvnd": pivot_buy,
            "pivot_buy_vnd": int(pivot_buy * 1000),
            "diff_pct": round(diff_pct, 2),  # signed: âm=dưới pivot, dương=trên pivot
            "distance_pct": round(price_pct, 2),
            "vol_ratio": vol_ratio,
            "vol_confirmed": vcp.get("vol_confirmed"),
            "stage": vcp.get("stage"),
            "t_count": vcp.get("t_count"),
        },
        "trend_score": r.get("trend_score"),
        "users_count": len(_states),
        "per_user": per_user,
    }
