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

class SetAlertRequest(BaseModel):
    ticker: str
    target_price: float
    direction: str  # "above" | "below"

@router.post("/set")
async def set_alert(req: SetAlertRequest):
    from app.services.alert_engine import set_price_alert
    set_price_alert(req.ticker.upper(), req.target_price, req.direction)
    direction_text = "tăng lên trên" if req.direction == "above" else "giảm xuống dưới"
    await send_telegram(
        f"🔔 <b>Đã đặt cảnh báo</b>\n"
        f"Mã: <b>{req.ticker.upper()}</b>\n"
        f"Khi giá {direction_text} <b>{req.target_price:,.0f}</b>"
    )
    return {"set": True, "ticker": req.ticker.upper(), "target": req.target_price}

@router.delete("/remove/{ticker}")
async def remove_alert(ticker: str):
    from app.services.alert_engine import remove_alert
    remove_alert(ticker.upper())
    return {"removed": ticker.upper()}

@router.get("/list")
async def list_alerts():
    from app.services.alert_engine import active_alerts
    return list(active_alerts.values())
