"""
Alert Engine — Hệ thống cảnh báo tập trung trên backend VPS

Chức năng:
  3A. Cảnh báo mua theo giá thủ công (alert_price trong watchlist, ±2%)
  3B. Cảnh báo mua VCP breakout + SEPA score ≥ sepaMinScore
  CL. Cảnh báo Cutloss: giá ≤ anchor × (1 − threshold%)
  TR. Trailing anchor: cuối phiên (15:01–15:30) cập nhật anchor lên nếu giá đóng cửa cao hơn

Dữ liệu đọc từ Supabase:
  • user_settings.settings.alert         — cutloss enabled, threshold, buyPoint, sepaMinScore
  • user_settings.settings.anchorPrices  — anchor prices (auto trailing)
  • user_settings.settings.holdingSettings — per-ticker anchor_price, cutloss_pct
  • watchlist_items                       — ticker, alert_price
  • trades                                — tính FIFO holdings

Backend ghi lại Supabase sau trailing:
  • user_settings.settings.anchorPrices  — giá neo mới
  • user_settings.settings.holdingSettings — anchor thủ công đã trailing
"""

import asyncio
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import httpx

from app.services.market_data import market_service
from app.routers.alerts import send_telegram

# ── Config mặc định ──────────────────────────────────────────────────────────
DEFAULT_SEPA_MIN        = 6
DEFAULT_CUTLOSS_PCT     = 8      # % toàn cục
DEFAULT_CL_PCT_PER      = 7      # % per-ticker
COOLDOWN_CUTLOSS_MIN    = 15     # phút
COOLDOWN_BUY_MIN        = 30     # phút (3A)
VCP_INTERVAL_MIN        = 5      # phút giữa 2 lần cảnh báo VCP cùng mã
VCP_MAX_ALERTS          = 5      # tối đa lần cảnh báo VCP / phiên
SEPA_CACHE_MIN          = 60     # phút cache SEPA score
VCP_CACHE_MIN           = 5      # phút cache VCP result
DATA_RELOAD_MIN         = 5      # phút tải lại watchlist/trades

# ── Supabase client ───────────────────────────────────────────────────────────
_SUPA_URL  = os.getenv("SUPABASE_URL", "").rstrip("/")
_SUPA_KEY  = os.getenv("SUPABASE_SERVICE_KEY", "") or os.getenv("SUPABASE_ANON_KEY", "")
_SUPA_HDR  = {
    "apikey":        _SUPA_KEY,
    "Authorization": f"Bearer {_SUPA_KEY}",
    "Content-Type":  "application/json",
}


async def _supa_get(table: str, params: dict = None) -> list:
    if not _SUPA_URL or not _SUPA_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{_SUPA_URL}/rest/v1/{table}",
                            params=params or {}, headers=_SUPA_HDR)
            if r.status_code != 200:
                print(f"⚠️  Supabase GET {table} {r.status_code}: {r.text[:120]}")
                return []
            return r.json()
    except Exception as e:
        print(f"⚠️  Supabase GET error: {e}")
        return []


async def _supa_patch(table: str, data: dict, params: dict) -> bool:
    if not _SUPA_URL or not _SUPA_KEY:
        return False
    try:
        hdrs = {**_SUPA_HDR, "Prefer": "return=minimal"}
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.patch(f"{_SUPA_URL}/rest/v1/{table}",
                              params=params, json=data, headers=hdrs)
            return r.status_code in (200, 204)
    except Exception as e:
        print(f"⚠️  Supabase PATCH error: {e}")
        return False

# ── Giờ giao dịch ────────────────────────────────────────────────────────────

def _is_trading() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    return (9 * 60 <= t <= 11 * 60 + 30) or (13 * 60 <= t <= 15 * 60 + 1)


def _is_post_close() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    return 15 * 60 + 1 <= t <= 15 * 60 + 30


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")

# ── FIFO Holdings ─────────────────────────────────────────────────────────────

def _fifo_holdings(trades: List[dict]) -> Dict[str, dict]:
    """Tính holding từ trades theo thuật toán FIFO."""
    raw: Dict[str, dict] = {}
    for t in sorted(trades, key=lambda x: x.get("trade_date", "")):
        tk   = (t.get("ticker") or "").upper()
        qty  = float(t.get("quantity") or 0)
        px   = float(t.get("price") or 0)
        fee  = float(t.get("fee") or 0)
        side = t.get("side", "")
        if not tk or qty <= 0:
            continue
        if tk not in raw:
            if side == "BUY":
                raw[tk] = {"ticker": tk, "qty": qty, "total_cost": px * qty + fee}
            continue
        h = raw[tk]
        if side == "BUY":
            h["qty"] += qty
            h["total_cost"] += px * qty + fee
        elif side == "SELL":
            avg = h["total_cost"] / h["qty"] if h["qty"] > 0 else 0
            h["qty"] -= qty
            h["total_cost"] = max(0.0, h["total_cost"] - avg * qty)
    return {
        tk: {**h, "avg_cost": h["total_cost"] / h["qty"]}
        for tk, h in raw.items() if h["qty"] > 0
    }

# ── Format helpers ────────────────────────────────────────────────────────────

def _fp(p: float) -> str:
    return f"{p:,.0f}"


def _fmt_criteria(criteria: dict) -> str:
    labels = {
        "c1_price_above_ma200": "Giá > MA200",
        "c2_ma200_trending_up": "MA200 tăng",
        "c3_price_above_ma150": "Giá > MA150",
        "c4_ma_stack":          "MA50>MA150>MA200",
        "c5_price_above_ma50":  "Giá > MA50",
        "c6_above_52w_low_30":  "+30% vs đáy 52w",
        "c7_near_52w_high_25":  "75% đỉnh 52w",
        "c8_rs_rating_strong":  "RS Rating ≥ 55",
    }
    return "\n".join(
        f"  {'✅' if criteria.get(k) else '❌'} {v}"
        for k, v in labels.items()
    )

# ── Per-user runtime state ────────────────────────────────────────────────────

class _UserState:
    def __init__(self, uid: str):
        self.uid = uid
        self.settings:        dict = {}
        self.watchlist_items: List[dict] = []
        self.holdings:        Dict[str, dict] = {}
        self.last_load:       Optional[datetime] = None
        # Cooldowns: "kind:ticker" → datetime
        self.cooldowns:       Dict[str, datetime] = {}
        # VCP alerts: ticker → {count, last_sent}
        self.vcp_state:       Dict[str, dict] = {}
        # SEPA cache: ticker → {score, criteria, at}
        self.sepa_cache:      Dict[str, dict] = {}
        # VCP result cache: ticker → {pivot_buy, vol_ratio, is_vcp, at}
        self.vcp_cache:       Dict[str, dict] = {}
        # Trailing anchor: chỉ chạy 1 lần / ngày
        self.anchor_date:     str = ""

    def cooldown_ok(self, kind: str, ticker: str, minutes: int) -> bool:
        last = self.cooldowns.get(f"{kind}:{ticker}")
        if not last:
            return True
        return (datetime.now() - last).total_seconds() >= minutes * 60

    def set_cooldown(self, kind: str, ticker: str):
        self.cooldowns[f"{kind}:{ticker}"] = datetime.now()

    # ── Settings helpers ──────────────────────────────────────────────────────
    def _alert(self) -> dict:
        return self.settings.get("alert", {})

    def sepa_min(self) -> int:
        return self._alert().get("sepaMinScore", DEFAULT_SEPA_MIN)

    def cutloss_enabled(self) -> bool:
        return self._alert().get("cutloss", {}).get("enabled", False)

    def cutloss_threshold(self) -> float:
        return self._alert().get("cutloss", {}).get("thresholdPct", DEFAULT_CUTLOSS_PCT)

    def cutloss_repeat(self) -> int:
        return self._alert().get("cutloss", {}).get("repeatMinutes", COOLDOWN_CUTLOSS_MIN)

    def buy_enabled(self) -> bool:
        return self._alert().get("buyPoint", {}).get("enabled", False)

    def vcp_max(self) -> int:
        return self._alert().get("buyPoint", {}).get("maxAlerts", VCP_MAX_ALERTS)

    def vcp_pivot_pct(self) -> float:
        return self._alert().get("buyPoint", {}).get("pivotRangePct", 3.0)

    def vcp_vol_mult(self) -> float:
        return self._alert().get("buyPoint", {}).get("volumeMultiplier", 1.5)

    def vcp_interval(self) -> int:
        return self._alert().get("buyPoint", {}).get("intervalMinutes", VCP_INTERVAL_MIN)

    def anchor_prices(self) -> Dict[str, float]:
        return self.settings.get("anchorPrices", {})

    def holding_settings(self) -> Dict[str, dict]:
        return self.settings.get("holdingSettings", {})


_states: Dict[str, _UserState] = {}

# ── Data loading ──────────────────────────────────────────────────────────────

async def _load_user_ids() -> List[str]:
    rows = await _supa_get("user_settings", {"select": "user_id"})
    return [r["user_id"] for r in rows if r.get("user_id")]


async def _load_settings(uid: str) -> dict:
    rows = await _supa_get("user_settings",
                           {"user_id": f"eq.{uid}", "select": "settings"})
    return rows[0].get("settings", {}) if rows else {}


async def _load_watchlist_items(uid: str) -> List[dict]:
    """Lấy tất cả watchlist items của user."""
    rows = await _supa_get("watchlists", {
        "user_id": f"eq.{uid}",
        "select":  "id,watchlist_items(ticker,alert_price)",
    })
    items = []
    for wl in rows:
        for item in (wl.get("watchlist_items") or []):
            if item.get("ticker"):
                items.append(item)
    return items


async def _load_trades(uid: str) -> List[dict]:
    return await _supa_get("trades", {
        "user_id": f"eq.{uid}",
        "select":  "ticker,side,quantity,price,fee,trade_date",
    })


async def _save_settings(uid: str, settings: dict) -> bool:
    return await _supa_patch(
        "user_settings",
        {"settings": settings, "updated_at": datetime.now().isoformat()},
        {"user_id": f"eq.{uid}"},
    )


async def _refresh(state: _UserState):
    """Tải lại dữ liệu nếu đã quá DATA_RELOAD_MIN phút."""
    now = datetime.now()
    if state.last_load and (now - state.last_load).total_seconds() < DATA_RELOAD_MIN * 60:
        return

    state.settings        = await _load_settings(state.uid)
    state.watchlist_items = await _load_watchlist_items(state.uid)
    trades                = await _load_trades(state.uid)
    state.holdings        = _fifo_holdings(trades)
    state.last_load       = now

    # Đảm bảo market_service đang poll các mã cần thiết
    tickers = (set(state.holdings.keys())
               | {i["ticker"] for i in state.watchlist_items if i.get("ticker")})
    if tickers:
        market_service.subscribe(list(tickers))

    print(f"🔄 [{state.uid[:8]}] {len(state.holdings)} holdings, "
          f"{len(state.watchlist_items)} watchlist items")

# ── SEPA helper ───────────────────────────────────────────────────────────────

async def _sepa(state: _UserState, ticker: str) -> Tuple[int, dict]:
    now = datetime.now()
    c = state.sepa_cache.get(ticker)
    if c and (now - c["at"]).total_seconds() < SEPA_CACHE_MIN * 60:
        return c["score"], c["criteria"]
    try:
        from app.services.screener import screener_service
        r = await screener_service._analyze_ticker(ticker)
        score    = r.get("trend_score", 0) if r else 0
        criteria = r.get("criteria", {})   if r else {}
    except Exception as e:
        print(f"⚠️  SEPA {ticker}: {e}")
        return (c["score"], c["criteria"]) if c else (0, {})
    state.sepa_cache[ticker] = {"score": score, "criteria": criteria, "at": now}
    return score, criteria

# ── 3A: Alert price (watchlist alert_price) ───────────────────────────────────

async def _check_3a(state: _UserState):
    """Cảnh báo khi giá chạm ±2% của alert_price trong watchlist. Không cần SEPA."""
    now_ts = datetime.now()
    for item in state.watchlist_items:
        ap = item.get("alert_price")
        if not ap:
            continue
        ticker = item["ticker"]
        q = market_service.quotes.get(ticker)
        if not q or q.get("price", 0) <= 0:
            continue

        price      = q["price"]
        alert_p    = float(ap)
        diff_pct   = abs(price - alert_p) / alert_p * 100
        if diff_pct > 2:
            continue
        if not state.cooldown_ok("3a", ticker, COOLDOWN_BUY_MIN):
            continue

        direction = "📈 Giá đã chạm / vượt ngưỡng" if price >= alert_p else "📉 Giá tiệm cận ngưỡng"
        msg = (
            f"🎯 <b>Cảnh báo mua – {ticker}</b>\n"
            f"{direction} <b>{_fp(alert_p)}</b>\n"
            f"Giá hiện tại: <b>{_fp(price)}</b> ({q.get('change_pct', 0):+.2f}%)\n"
            f"Cách ngưỡng: {diff_pct:.1f}%\n"
            f"⏰ {now_ts.strftime('%H:%M:%S %d/%m/%Y')}"
        )
        await send_telegram(msg)
        state.set_cooldown("3a", ticker)
        print(f"🎯 3A alert {ticker}: {_fp(price)} ~ {_fp(alert_p)}")

# ── 3B: VCP breakout + SEPA ───────────────────────────────────────────────────

async def _check_3b(state: _UserState):
    """VCP breakout + SEPA score ≥ sepaMinScore. Thay thế hệ thống cảnh báo 'above' cũ."""
    if not state.buy_enabled():
        return

    sepa_min   = state.sepa_min()
    pivot_pct  = state.vcp_pivot_pct()
    vol_mult   = state.vcp_vol_mult()
    vcp_max    = state.vcp_max()
    interval   = state.vcp_interval()
    now        = datetime.now()

    for item in state.watchlist_items:
        ticker = item["ticker"]
        q = market_service.quotes.get(ticker)
        if not q or q.get("price", 0) <= 0:
            continue

        # Kiểm tra giới hạn số lần cảnh báo
        vs = state.vcp_state.get(ticker, {"count": 0, "last_sent": None})
        if vs["count"] >= vcp_max:
            continue
        if vs["last_sent"] and (now - vs["last_sent"]).total_seconds() < interval * 60:
            continue

        # Lấy VCP từ cache hoặc tính mới
        vc = state.vcp_cache.get(ticker)
        if vc and (now - vc["at"]).total_seconds() < VCP_CACHE_MIN * 60:
            is_vcp    = vc["is_vcp"]
            pivot_buy = vc["pivot_buy"]
            vol_ratio = vc["vol_ratio"]
        else:
            try:
                from app.services.screener import screener_service
                r = await screener_service._analyze_ticker(ticker)
                if not r:
                    continue
                vcp_data  = r.get("vcp", {})
                is_vcp    = vcp_data.get("is_vcp", False)
                pivot_buy = float(vcp_data.get("pivot_buy") or 0)
                vol_ratio = float(vcp_data.get("vol_ratio") or 0)
                state.vcp_cache[ticker] = {
                    "is_vcp": is_vcp, "pivot_buy": pivot_buy,
                    "vol_ratio": vol_ratio, "at": now,
                }
            except Exception as e:
                print(f"⚠️  VCP {ticker}: {e}")
                continue

        if not is_vcp or not pivot_buy:
            continue

        price     = q["price"]
        price_pct = abs(price - pivot_buy) / pivot_buy * 100
        if price_pct > pivot_pct:
            continue
        if vol_ratio < vol_mult:
            continue

        # Kiểm tra SEPA
        score, criteria = await _sepa(state, ticker)
        if score < sepa_min:
            print(f"⛔ 3B {ticker}: SEPA {score}/{sepa_min} — bị chặn")
            continue

        count = vs["count"] + 1
        crit_text = _fmt_criteria(criteria)
        msg = (
            f"🟢 <b>Điểm mua VCP – {ticker}</b>\n"
            f"Giá: <b>{_fp(price)}</b> ({q.get('change_pct', 0):+.2f}%)\n"
            f"Pivot buy: {_fp(pivot_buy)} | Cách: {price_pct:.1f}%\n"
            f"Volume: <b>{vol_ratio:.1f}x</b> MA30\n"
            f"Lần cảnh báo: {count}/{vcp_max}\n"
            f"\n"
            f"📊 <b>SEPA Score: {score}/8</b>\n"
            f"{crit_text}\n"
            f"\n"
            f"⏰ {now.strftime('%H:%M:%S %d/%m/%Y')}"
        )
        await send_telegram(msg)
        state.vcp_state[ticker] = {"count": count, "last_sent": now}
        print(f"🟢 3B VCP {ticker}: {_fp(price)} pivot={_fp(pivot_buy)} sepa={score}/8")

# ── Cutloss ───────────────────────────────────────────────────────────────────

async def _check_cutloss(state: _UserState):
    if not state.cutloss_enabled():
        return

    anchors    = state.anchor_prices()
    hs         = state.holding_settings()
    global_cl  = state.cutloss_threshold()
    repeat_min = state.cutloss_repeat()
    now        = datetime.now()

    for ticker, holding in state.holdings.items():
        q = market_service.quotes.get(ticker)
        if not q or q.get("price", 0) <= 0:
            continue

        price    = q["price"]
        qty      = holding.get("qty", 0)
        avg_cost = holding.get("avg_cost", 0)
        if qty <= 0:
            continue

        per = hs.get(ticker, {})
        # Anchor: manual > auto trailing > avgCost
        anchor = float(per.get("anchor_price") or anchors.get(ticker) or avg_cost or 0)
        cl_pct = float(per.get("cutloss_pct") or global_cl)
        if anchor <= 0:
            continue

        cl_price = anchor * (1 - cl_pct / 100)
        if price > cl_price:
            continue
        if not state.cooldown_ok("cutloss", ticker, repeat_min):
            continue

        gap_pct = (price - cl_price) / cl_price * 100
        msg = (
            f"⚠️ <b>CẢNH BÁO CUTLOSS – {ticker}</b>\n"
            f"Giá hiện tại: <b>{_fp(price)}</b> ({q.get('change_pct', 0):+.2f}%)\n"
            f"Giá neo: {_fp(anchor)} | Ngưỡng: -{cl_pct:.0f}%\n"
            f"Giá Cutloss: {_fp(cl_price)} | Thủng: {gap_pct:.1f}%\n"
            f"Số lượng đang giữ: {qty:,.0f}\n"
            f"⏰ {now.strftime('%H:%M:%S %d/%m/%Y')}"
        )
        await send_telegram(msg)
        state.set_cooldown("cutloss", ticker)
        print(f"⚠️ Cutloss {ticker}: {_fp(price)} ≤ {_fp(cl_price)}")

# ── Trailing anchor (15:01–15:30, 1 lần/ngày) ────────────────────────────────

async def _trail_anchors(state: _UserState):
    today = _today()
    if state.anchor_date == today:
        return

    anchors  = dict(state.anchor_prices())
    hs       = dict(state.holding_settings())
    updated  = False

    for ticker, holding in state.holdings.items():
        q        = market_service.quotes.get(ticker)
        close    = float(q["price"]) if q and q.get("price", 0) > 0 else 0
        avg_cost = holding.get("avg_cost", 0)

        cur_anchor = float(anchors.get(ticker) or 0)

        # Khởi tạo anchor nếu chưa có
        if cur_anchor == 0 and avg_cost > 0:
            anchors[ticker] = avg_cost
            cur_anchor = avg_cost
            updated = True
            print(f"🔖 Init anchor {ticker}: {_fp(avg_cost)}")

        # Trailing auto anchor (chỉ tăng)
        if close > cur_anchor > 0:
            anchors[ticker] = close
            updated = True
            print(f"🔖 Trail auto {ticker}: {_fp(cur_anchor)} → {_fp(close)}")

        # Trailing manual anchor (holdingSettings.anchor_price) — cũng chỉ tăng
        per = hs.get(ticker, {})
        manual_anch = float(per.get("anchor_price") or 0)
        if manual_anch > 0 and close > manual_anch:
            hs[ticker] = {**per, "anchor_price": close}
            updated = True
            print(f"🔖 Trail manual {ticker}: {_fp(manual_anch)} → {_fp(close)}")

    # Reset anchor cho mã đã bán hết
    for ticker in list(anchors.keys()):
        if ticker not in state.holdings:
            del anchors[ticker]
            updated = True
            print(f"🗑 Reset anchor {ticker}")

    state.anchor_date = today

    if not updated:
        return

    # Ghi lại Supabase
    current = await _load_settings(state.uid)
    current["anchorPrices"]    = anchors
    current["holdingSettings"] = hs
    ok = await _save_settings(state.uid, current)
    if ok:
        # Force reload data on next cycle
        state.last_load = None
        print(f"✅ Anchor prices saved for user {state.uid[:8]}...")
    else:
        print(f"⚠️  Failed to save anchors for user {state.uid[:8]}...")

# ── Main loop ─────────────────────────────────────────────────────────────────

async def run_alert_engine():
    print("✅ Alert engine started (buy: 3A alert_price + 3B VCP+SEPA | cutloss: backend)")

    while True:
        try:
            user_ids = await _load_user_ids()

            for uid in user_ids:
                if uid not in _states:
                    _states[uid] = _UserState(uid)
                state = _states[uid]

                # Tải lại dữ liệu nếu cần
                await _refresh(state)

                # Post-close: trailing anchor
                if _is_post_close():
                    await _trail_anchors(state)

                # Giờ giao dịch: kiểm tra cảnh báo
                if _is_trading():
                    await _check_3a(state)       # 3A: alert_price
                    await _check_3b(state)       # 3B: VCP + SEPA
                    await _check_cutloss(state)  # CL: cutloss

            await asyncio.sleep(5)

        except asyncio.CancelledError:
            break
        except Exception as e:
            import traceback
            print(f"Alert engine error: {e}")
            traceback.print_exc()
            await asyncio.sleep(10)
