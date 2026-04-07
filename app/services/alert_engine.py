import asyncio
import os
from datetime import datetime
from typing import Dict
from app.services.market_data import market_service
from app.routers.alerts import send_telegram

# Lưu các cảnh báo đang active: {ticker: {"price": float, "direction": "above"|"below", "triggered": bool}}
active_alerts: Dict[str, dict] = {}
# Cooldown tránh spam: {alert_key: last_triggered_time}
cooldowns: Dict[str, datetime] = {}
COOLDOWN_MINUTES = 30

def set_price_alert(ticker: str, target_price: float, direction: str):
    """Đặt cảnh báo giá cho một mã"""
    key = f"{ticker}_{direction}_{target_price}"
    active_alerts[key] = {
        "ticker": ticker,
        "target": target_price,
        "direction": direction,  # "above" hoặc "below"
        "created": datetime.now().isoformat(),
    }
    print(f"✅ Alert set: {ticker} {direction} {target_price:,.0f}")

def remove_alert(ticker: str):
    """Xoá tất cả cảnh báo của một mã"""
    keys = [k for k in active_alerts if k.startswith(ticker)]
    for k in keys:
        del active_alerts[k]

async def check_alerts():
    """Kiểm tra tất cả cảnh báo với giá hiện tại"""
    if not active_alerts:
        return
    triggered = []
    now = datetime.now()

    for key, alert in list(active_alerts.items()):
        ticker    = alert["ticker"]
        target    = alert["target"]
        direction = alert["direction"]

        # Lấy giá từ cache
        quote = market_service.quotes.get(ticker)
        if not quote:
            continue

        price = quote["price"]
        hit = (direction == "above" and price >= target) or \
              (direction == "below" and price <= target)

        if not hit:
            continue

        # Kiểm tra cooldown
        last = cooldowns.get(key)
        if last and (now - last).seconds < COOLDOWN_MINUTES * 60:
            continue

        cooldowns[key] = now
        triggered.append((key, alert, quote))

    # Gửi thông báo
    for key, alert, quote in triggered:
        ticker    = alert["ticker"]
        target    = alert["target"]
        direction = alert["direction"]
        price     = quote["price"]
        change_pct = quote["change_pct"]

        emoji = "🟢" if direction == "above" else "🔴"
        direction_text = "vượt lên trên" if direction == "above" else "xuống dưới"

        msg = (
            f"{emoji} <b>Cảnh báo giá – {ticker}</b>\n"
            f"Giá đã {direction_text} ngưỡng <b>{target:,.0f}</b>\n"
            f"Giá hiện tại: <b>{price:,.0f}</b>\n"
            f"Thay đổi: {change_pct:+.2f}%\n"
            f"⏰ {now.strftime('%H:%M:%S %d/%m/%Y')}"
        )
        await send_telegram(msg)
        print(f"🔔 Alert triggered: {ticker} {direction} {target:,.0f} → {price:,.0f}")

async def run_alert_engine():
    """Vòng lặp kiểm tra cảnh báo mỗi 5 giây"""
    print("✅ Alert engine started")
    while True:
        try:
            await check_alerts()
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Alert engine error: {e}")
            await asyncio.sleep(10)
