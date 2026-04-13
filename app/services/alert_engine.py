import asyncio
import os
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
from app.services.market_data import market_service
from app.routers.alerts import send_telegram

# Lưu các cảnh báo đang active: {key: alert_dict}
active_alerts: Dict[str, dict] = {}
# Cooldown tránh spam: {alert_key: last_triggered_time}
cooldowns: Dict[str, datetime] = {}
COOLDOWN_MINUTES = 30

# ── Cache SEPA score riêng cho alert engine ──────────────────────────────────
# Mỗi ticker lưu: {"score": int, "criteria": dict, "checked_at": datetime}
# Refresh mỗi 60 phút — tránh gọi vnstock API quá nhiều
_sepa_cache: Dict[str, dict] = {}
SEPA_CACHE_MINUTES  = 60
SEPA_MIN_SCORE      = 6   # Tối thiểu 6/8 tiêu chí mới gửi cảnh báo


def set_price_alert(ticker: str, target_price: float, direction: str):
    """Đặt cảnh báo giá cho một mã"""
    key = f"{ticker}_{direction}_{target_price}"
    active_alerts[key] = {
        "ticker":    ticker,
        "target":    target_price,
        "direction": direction,  # "above" hoặc "below"
        "created":   datetime.now().isoformat(),
    }
    print(f"✅ Alert set: {ticker} {direction} {target_price:,.0f}")


def remove_alert(ticker: str):
    """Xoá tất cả cảnh báo của một mã"""
    keys = [k for k in active_alerts if k.startswith(ticker)]
    for k in keys:
        del active_alerts[k]


# ── SEPA score check ─────────────────────────────────────────────────────────

async def _get_sepa_score(ticker: str) -> Tuple[int, dict]:
    """
    Lấy SEPA (Trend Template) score của ticker.
    Dùng cache 60 phút — chỉ gọi vnstock khi hết hạn.
    Trả về (score, criteria_dict).
    """
    now = datetime.now()

    # Kiểm tra cache còn hiệu lực không
    cached = _sepa_cache.get(ticker)
    if cached:
        age = (now - cached["checked_at"]).total_seconds() / 60
        if age < SEPA_CACHE_MINUTES:
            return cached["score"], cached["criteria"]

    # Cache hết hạn → fetch mới qua screener_service
    try:
        from app.services.screener import screener_service
        result = await screener_service._analyze_ticker(ticker)
        if result:
            score    = result.get("trend_score", 0)
            criteria = result.get("criteria", {})
        else:
            score, criteria = 0, {}
    except Exception as e:
        print(f"⚠️  SEPA check error {ticker}: {e}")
        # Nếu lỗi fetch → dùng điểm cũ nếu có, không chặn alert
        if cached:
            return cached["score"], cached["criteria"]
        return 0, {}

    _sepa_cache[ticker] = {
        "score":      score,
        "criteria":   criteria,
        "checked_at": now,
    }
    print(f"📊 SEPA cache updated: {ticker} → {score}/8 tiêu chí")
    return score, criteria


def _format_criteria_summary(criteria: dict) -> str:
    """Tóm tắt tiêu chí SEPA để hiển thị trong Telegram"""
    labels = {
        "c1_price_above_ma200": "Giá > MA200",
        "c2_ma200_trending_up": "MA200 tăng",
        "c3_price_above_ma150": "Giá > MA150",
        "c4_ma_stack":          "MA50>MA150>MA200",
        "c5_price_above_ma50":  "Giá > MA50",
        "c6_above_52w_low_30":  "+30% vs đáy 52w",
        "c7_near_52w_high_25":  "75% đỉnh 52w",
        "c8_volume_sufficient": "Volume đủ lớn",
    }
    lines = []
    for key, label in labels.items():
        passed = criteria.get(key, False)
        lines.append(f"  {'✅' if passed else '❌'} {label}")
    return "\n".join(lines)


# ── Load alerts từ Supabase ──────────────────────────────────────────────────

async def load_alerts_from_supabase():
    """
    Load tất cả active alerts từ Supabase khi khởi động.
    Tránh mất alerts khi server restart.
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

                if alert_type in ("PRICE", "STOP_LOSS") and isinstance(condition, dict):
                    target = condition.get("price")
                    op     = condition.get("operator", "gt")
                    if target is None:
                        continue
                    direction = "above" if op in ("gt", "gte") else "below"
                    key = f"{ticker}_{direction}_{target}"
                    active_alerts[key] = {
                        "ticker":      ticker,
                        "target":      float(target),
                        "direction":   direction,
                        "created":     row.get("created_at", datetime.now().isoformat()),
                        "supabase_id": row.get("id"),
                    }
                    loaded += 1

            print(f"✅ Loaded {loaded} alerts from Supabase (total rows: {len(rows)})")
    except Exception as e:
        print(f"⚠️  load_alerts_from_supabase error: {e}")


# ── Vòng lặp kiểm tra cảnh báo ───────────────────────────────────────────────

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
        if last and (now - last).total_seconds() < COOLDOWN_MINUTES * 60:
            continue

        cooldowns[key] = now
        triggered.append((key, alert, quote))

    for key, alert, quote in triggered:
        ticker     = alert["ticker"]
        target     = alert["target"]
        direction  = alert["direction"]
        price      = quote["price"]
        change_pct = quote["change_pct"]

        # ── Kiểm tra SEPA (chỉ áp dụng cho cảnh báo "above" — tín hiệu mua) ──
        if direction == "above":
            sepa_score, sepa_criteria = await _get_sepa_score(ticker)

            if sepa_score < SEPA_MIN_SCORE:
                # Không đủ tiêu chí SEPA → không gửi alert, chỉ log
                print(
                    f"⛔ Alert {ticker} bị chặn: giá đã vượt {target:,.0f} "
                    f"nhưng SEPA chỉ {sepa_score}/8 (cần ≥ {SEPA_MIN_SCORE})"
                )
                continue

            # Đủ tiêu chí → gửi alert kèm thông tin SEPA
            criteria_text = _format_criteria_summary(sepa_criteria)
            msg = (
                f"🟢 <b>Cảnh báo mua – {ticker}</b>\n"
                f"Giá đã vượt lên trên ngưỡng <b>{target:,.0f}</b>\n"
                f"Giá hiện tại: <b>{price:,.0f}</b>\n"
                f"Thay đổi: {change_pct:+.2f}%\n"
                f"\n"
                f"📊 <b>SEPA Score: {sepa_score}/8</b>\n"
                f"{criteria_text}\n"
                f"\n"
                f"⏰ {now.strftime('%H:%M:%S %d/%m/%Y')}"
            )
        else:
            # Cảnh báo "below" (stop loss) → gửi ngay, không cần check SEPA
            msg = (
                f"🔴 <b>Cảnh báo giá – {ticker}</b>\n"
                f"Giá đã xuống dưới ngưỡng <b>{target:,.0f}</b>\n"
                f"Giá hiện tại: <b>{price:,.0f}</b>\n"
                f"Thay đổi: {change_pct:+.2f}%\n"
                f"⏰ {now.strftime('%H:%M:%S %d/%m/%Y')}"
            )

        await send_telegram(msg)
        print(f"🔔 Alert triggered: {ticker} {direction} {target:,.0f} → {price:,.0f}")


async def run_alert_engine():
    """Vòng lặp kiểm tra cảnh báo mỗi 5 giây"""
    await load_alerts_from_supabase()
    print(f"✅ Alert engine started (SEPA filter: cần ≥ {SEPA_MIN_SCORE}/8 tiêu chí cho lệnh mua)")
    while True:
        try:
            await check_alerts()
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Alert engine error: {e}")
            await asyncio.sleep(10)
