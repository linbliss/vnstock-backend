import os
import httpx
from fastapi import APIRouter
from pydantic import BaseModel

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

@router.get("/test")
async def test_alert():
    ok = await send_telegram(
        "✅ <b>VN Stock Manager</b>\nAlerts đang hoạt động!\nBạn sẽ nhận cảnh báo giá tại đây."
    )
    return {"sent": ok, "token_set": bool(BOT_TOKEN), "chat_set": bool(CHAT_ID)}

@router.get("/status")
async def alert_status():
    """Trả về trạng thái alert engine — alerts được quản lý qua Supabase watchlist_items."""
    from app.services.alert_engine import _states
    return {
        "engine": "running",
        "users": len(_states),
        "note": "Alerts được quản lý qua Watchlist (alert_price) và VCP engine trên backend."
    }
