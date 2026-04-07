import os
import httpx
from datetime import datetime
from app.models.market import StockQuote, AlertConfig

TELEGRAM_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
RESEND_API_KEY  = os.getenv("RESEND_API_KEY", "")
ALERT_FROM      = os.getenv("ALERT_FROM_EMAIL", "alerts@vnstock.app")

class AlertService:
    def __init__(self):
        self.triggered_alerts: dict[str, datetime] = {}  # alert_id -> last triggered

    # ── Kiểm tra điều kiện cảnh báo ──

    def check_alert(self, alert: AlertConfig, quote: StockQuote) -> bool:
        if not alert.is_active:
            return False

        # Cooldown 5 phút tránh spam
        last = self.triggered_alerts.get(alert.id)
        if last and (datetime.now() - last).seconds < 300:
            return False

        cond = alert.condition
        alert_type = alert.alert_type

        if alert_type == "PRICE":
            price    = float(cond.get("price", 0))
            operator = cond.get("operator", "gt")
            triggered = {
                "gt":  quote.price >  price,
                "lt":  quote.price <  price,
                "gte": quote.price >= price,
                "lte": quote.price <= price,
            }.get(operator, False)
            return triggered

        if alert_type == "STOP_LOSS":
            stop_price = float(cond.get("price", 0))
            return quote.price <= stop_price

        if alert_type == "VOLUME":
            volume_ratio = float(cond.get("volume_ratio", 1.3))
            # Cần MA30 volume để so sánh – sẽ implement ở Phase 4
            return False

        return False

    # ── Gửi thông báo ──

    async def send_alert(
        self,
        alert: AlertConfig,
        quote: StockQuote,
        message: str
    ):
        self.triggered_alerts[alert.id] = datetime.now()
        channels = alert.channels

        tasks = []
        if "telegram" in channels and TELEGRAM_TOKEN:
            tasks.append(self._send_telegram(
                chat_id=alert.condition.get("telegram_chat_id", ""),
                text=message
            ))
        if "email" in channels and RESEND_API_KEY:
            tasks.append(self._send_email(
                to=alert.condition.get("email", ""),
                subject=f"[VN Stock] Cảnh báo {alert.ticker}",
                body=message
            ))

        import asyncio
        await asyncio.gather(*tasks, return_exceptions=True)

    def format_message(self, alert: AlertConfig, quote: StockQuote) -> str:
        emoji_map = {
            "PRICE":     "📊",
            "STOP_LOSS": "🛑",
            "VOLUME":    "📈",
            "PIVOT":     "🎯",
        }
        emoji = emoji_map.get(alert.alert_type, "🔔")

        lines = [
            f"{emoji} *{alert.ticker}* – Cảnh báo {alert.alert_type}",
            f"💰 Giá hiện tại: *{quote.price:,.0f}*",
            f"📉 Thay đổi: {quote.change_pct:+.2f}%",
            f"📊 Khối lượng: {quote.volume:,}",
            f"⏰ {quote.timestamp[:19].replace('T', ' ')}",
        ]

        cond = alert.condition
        if alert.alert_type == "PRICE":
            lines.insert(2, f"🎯 Điều kiện: giá {cond.get('operator','')} {float(cond.get('price',0)):,.0f}")
        elif alert.alert_type == "STOP_LOSS":
            lines.insert(2, f"🛑 Stop Loss: {float(cond.get('price',0)):,.0f}")

        return "\n".join(lines)

    async def _send_telegram(self, chat_id: str, text: str):
        if not chat_id or not TELEGRAM_TOKEN:
            return
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(url, json={
                    "chat_id":    chat_id,
                    "text":       text,
                    "parse_mode": "Markdown",
                })
                if resp.status_code != 200:
                    print(f"Telegram error: {resp.text}")
                else:
                    print(f"✅ Telegram sent to {chat_id}")
        except Exception as e:
            print(f"Telegram send error: {e}")

    async def _send_email(self, to: str, subject: str, body: str):
        if not to or not RESEND_API_KEY:
            return
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    "https://api.resend.com/emails",
                    headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
                    json={
                        "from":    ALERT_FROM,
                        "to":      [to],
                        "subject": subject,
                        "text":    body,
                    }
                )
                if resp.status_code not in (200, 201):
                    print(f"Email error: {resp.text}")
                else:
                    print(f"✅ Email sent to {to}")
        except Exception as e:
            print(f"Email send error: {e}")

    # ── Gửi thông báo test ──

    async def test_telegram(self, chat_id: str, bot_token: str) -> bool:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(url, json={
                    "chat_id": chat_id,
                    "text":    "✅ VN Stock Manager – Kết nối Telegram thành công!\nBạn sẽ nhận được cảnh báo tại đây.",
                })
                return resp.status_code == 200
        except:
            return False


alert_service = AlertService()
