import asyncio
import os
from datetime import datetime
from typing import Dict
from app.services.market_data import market_service
from app.routers.alerts import send_telegram

# Lưu các cảnh báo đang active: {key: alert_dict}
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


async def load_alerts_from_supabase():
    """
    Load tất cả active alerts từ Supabase khi khởi động.
    Tránh mất alerts khi Railway restart.
    """
    url = os.getenv("SUPABASE_URL", "").rstrip("/")
    key = os.getenv("SUPABASE_SERVICE_KEY", "") or os.getenv("SUPABASE_ANON_KEY", "")
    if not url or not key:
        print("⚠️  Supabase chưa cấu hình – bỏ qua load alerts")
        return

    try:
        import httpx
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{url}/rest/v1/alerts",
                params={"is_active": "eq.true", "select": "*"},
                headers={
                    "apikey": key,
                    "Authorization": f"Bearer {key}",
                },
            )
            if resp.status_code != 200:
                print(f"⚠️  Load alerts from Supabase failed: {resp.status_code}")
                return

            rows = resp.json()
            loaded = 0
            for row in rows:
                ticker     = str(row.get("ticker", "")).upper()
                alert_type = str(row.get("alert_type", ""))
                condition  = row.get("condition", {})

                # Chỉ xử lý PRICE và STOP_LOSS alerts (có target cụ thể)
                if alert_type in ("PRICE", "STOP_LOSS") and isinstance(condition, dict):
                    target = condition.get("price")
                    op     = condition.get("operator", "gt")
                    if target is None:
                        continue
                    direction = "above" if op in ("gt", "gte") else "below"
                    key = f"{ticker}_{direction}_{target}"
                    active_alerts[key] = {
                        "ticker":    ticker,
                        "target":    float(target),
                        "direction": direction,
                        "created":   row.get("created_at", datetime.now().isoformat()),
                        "supabase_id": row.get("id"),
                    }
                    loaded += 1

            print(f"✅ Loaded {loaded} alerts from Supabase (total rows: {len(rows)})")
    except Exception as e:
        print(f"⚠️  load_alerts_from_supabase error: {e}")


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

    for key, alert, quote in triggered:
        ticker     = alert["ticker"]
        target     = alert["target"]
        direction  = alert["direction"]
        price      = quote["price"]
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
    # Load alerts từ Supabase khi khởi động
    await load_alerts_from_supabase()
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
