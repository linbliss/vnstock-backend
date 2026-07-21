import asyncio
import os
import time
from typing import Dict, List, Callable
from datetime import datetime
from collections import deque

class RateLimiter:
    def __init__(self, max_calls: int = 35, period: float = 60.0):
        self.max_calls = max_calls
        self.period    = period
        self._calls: deque = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            while self._calls and now - self._calls[0] >= self.period:
                self._calls.popleft()
            if len(self._calls) >= self.max_calls:
                wait = self.period - (now - self._calls[0])
                if wait > 0:
                    print(f"Rate limit: chờ {wait:.1f}s")
                    await asyncio.sleep(wait)
                    now = time.monotonic()
                    while self._calls and now - self._calls[0] >= self.period:
                        self._calls.popleft()
            self._calls.append(time.monotonic())


class MarketDataService:
    def __init__(self):
        self.quotes: Dict[str, dict] = {}
        self.subscribed: set         = set()
        self._screener_subs: set     = set()   # mã do screener đăng ký (tự thay thế)
        self.listeners: List[Callable] = []
        self._task    = None
        self._running = False
        # 35/60 để có buffer rộng — vnai Community hard-limit là 60/min,
        # nhiều entry point song song (polling, historical, screener) + sliding window
        # có thể tràn qua vnai nếu sát ngưỡng. Nếu dùng gói Sponsor → có thể tăng.
        self._limiter = RateLimiter(max_calls=35, period=60.0)

    async def start(self):
        self._running = True
        self._set_api_key()
        await self._preload()
        self._task = asyncio.create_task(self._polling_loop())
        print("✅ Market service started")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()

    def _set_api_key(self):
        # Đọc trực tiếp từ environment (Railway inject vào đây)
        key = os.environ.get("VNSTOCK_API_KEY") or os.getenv("VNSTOCK_API_KEY", "")
        debug_env = os.getenv("DEBUG_VNSTOCK", "").strip().lower() in {"1", "true", "yes", "y"}
        if debug_env:
            has_key = bool(key)
            print(f"DEBUG: VNSTOCK_API_KEY present = {has_key}")
            print(f"DEBUG: All env keys with VNSTOCK: {[k for k in os.environ if 'VNSTOCK' in k]}")

        if not key:
            print("⚠️  VNSTOCK_API_KEY chưa được cấu hình – dùng guest mode")
            return
        try:
            import vnstock
            vnstock.change_api_key(key)
            print("✅ vnstock API key đã thiết lập")
        except Exception as e:
            print(f"⚠️  Lỗi set API key: {e}")

    def subscribe(self, tickers: List[str]):
        for t in tickers:
            self.subscribed.add(t.upper())
        self._register_ws(tickers)

    def subscribe_screener(self, tickers: List[str], cap: int = 40):
        """Đăng ký giá cho các mã LỌT KẾT QUẢ screener — CHỈ KBS (vnstock), KHÔNG WS.

        Screener chỉ cần price + volume để tính vol_ratio/near_pivot; KBS price_board đủ.
        KHÔNG đăng ký WS: (1) tránh lẫn nguồn, (2) chừa trần 100 subscription của DNSE WS
        cho Shark Action + danh mục. Bộ này TỰ THAY THẾ mỗi lần quét + chặn trần để
        subscribed không phình vô hạn (subscribe() chỉ thêm không gỡ)."""
        new = {t.upper() for t in tickers[:cap] if t}
        for t in getattr(self, "_screener_subs", set()) - new:
            self.subscribed.discard(t)
        self._screener_subs = new
        for t in new:
            self.subscribed.add(t)
        # KHÔNG gọi _register_ws — screener độc lập với WS.

    @staticmethod
    def _register_ws(tickers: List[str]) -> None:
        """Báo DNSE WS subscribe các mã của bảng giá → có giá realtime dạng push."""
        try:
            from app.services import dnse_feed, data_source
            if data_source.get_source("priceboard") != "dnse":
                return
            for t in tickers:
                dnse_feed.register_demand(t.upper())
        except Exception:  # noqa: BLE001
            pass

    def _apply_ws(self, q: dict) -> dict:
        """Phủ giá realtime từ DNSE WS lên quote KBS.

        KBS vẫn là nguồn DUY NHẤT của trần/sàn/tham chiếu: các giá này nằm ở kênh
        security_definition mà DNSE chỉ phát BATCH lúc ~08:00 (BOD) — kết nối sau 8h
        sẽ không bao giờ nhận được (đã kiểm chứng thật). WS chỉ phủ phần realtime.
        """
        try:
            from app.services import dnse_feed, data_source
            if data_source.get_source("priceboard") != "dnse":
                return q
            w = dnse_feed.get_quote(q["ticker"])
        except Exception:  # noqa: BLE001
            return q
        if not w or not w.get("price"):
            return q
        price = w["price"]
        ref = float(q.get("reference_price") or 0)
        out = {**q, "price": price, "source": "DNSE_WS",
               "timestamp": datetime.now().isoformat()}
        # KL luỹ kế/OHLC: lấy giá trị "tiến" hơn giữa KBS và WS (tránh lùi số khi
        # một trong hai nguồn trễ nhịp).
        if w.get("volume"):
            out["volume"] = max(int(q.get("volume") or 0), int(w["volume"]))
        if w.get("high"):
            out["high"] = max(float(q.get("high") or 0), float(w["high"]))
        if w.get("low"):
            lo = float(q.get("low") or 0)
            out["low"] = min(lo, float(w["low"])) if lo > 0 else float(w["low"])
        if w.get("open") and not q.get("open"):
            out["open"] = float(w["open"])
        if ref > 0:
            out["change"] = round(price - ref, 2)
            out["change_pct"] = round((price - ref) / ref * 100, 2)
        return out

    def add_listener(self, fn: Callable):
        self.listeners.append(fn)

    def remove_listener(self, fn: Callable):
        if fn in self.listeners:
            self.listeners.remove(fn)

    async def fetch_quotes(self, tickers: List[str]) -> List[dict]:
        if not tickers:
            return []
        await self._limiter.acquire()
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._fetch_sync, tickers)
        except Exception as e:
            print(f"fetch_quotes error: {e}")
            return []

    def _fetch_sync(self, tickers: List[str]) -> List[dict]:
        try:
            from vnstock import Trading
            df = Trading(source='KBS').price_board([t.upper() for t in tickers])
            if df is None or df.empty:
                return []
            quotes = []
            for _, row in df.iterrows():
                try:
                    d      = row.to_dict()
                    ticker = str(d.get('symbol', '')).upper()
                    price  = float(d.get('close_price')     or 0)
                    ref    = float(d.get('reference_price') or price)
                    change = round(price - ref, 2)
                    pct    = round((change / ref * 100), 2) if ref > 0 else 0.0
                    if not ticker or price <= 0:
                        continue
                    q = {
                        'ticker':          ticker,
                        'price':           price,
                        'reference_price': ref,
                        'change':          change,
                        'change_pct':      pct,
                        'volume':    int(d.get('volume_accumulated') or 0),
                        'high':    float(d.get('high_price')    or price),
                        'low':     float(d.get('low_price')     or price),
                        'open':    float(d.get('open_price')    or price),
                        'ceiling': float(d.get('ceiling_price') or 0),
                        'floor':   float(d.get('floor_price')   or 0),
                        'exchange':  str(d.get('exchange') or ''),
                        'timestamp': datetime.now().isoformat(),
                    }
                    self.quotes[ticker] = q
                    quotes.append(q)
                except:
                    continue
            print(f"✅ Fetched {len(quotes)} quotes")
            return quotes
        # BaseException để nuốt SystemExit từ vnai.beam.quota (rate limit)
        except BaseException as e:
            print(f"_fetch_sync error: {type(e).__name__}: {e}")
            return []

    async def fetch_historical(self, ticker: str, from_date: str, to_date: str) -> List[dict]:
        await self._limiter.acquire()
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._hist_sync, ticker, from_date, to_date)
        except Exception as e:
            print(f"fetch_historical error: {e}")
            return []

    def _hist_sync(self, ticker: str, from_date: str, to_date: str) -> List[dict]:
        try:
            from vnstock import Quote
            df = Quote(symbol=ticker.upper(), source='KBS').history(
                start=from_date, end=to_date, interval='1D'
            )
            if df is None or df.empty:
                return []
            return df.reset_index().to_dict('records')
        # BaseException để nuốt SystemExit từ vnai.beam.quota (rate limit)
        except BaseException as e:
            print(f"_hist_sync error: {type(e).__name__}: {e}")
            return []

    def _ws_covers(self, tickers: List[str]) -> bool:
        """DNSE WS có đang đẩy tick cho phần lớn mã đang xem không?"""
        try:
            from app.services import dnse_feed, data_source
            if data_source.get_source("priceboard") != "dnse" or not dnse_feed.active():
                return False
            live = sum(1 for t in tickers if dnse_feed.get_quote(t))
            return live >= max(1, len(tickers) // 2)
        except Exception:  # noqa: BLE001
            return False

    async def _polling_loop(self):
        BATCH_SIZE = 20
        last_kbs = 0.0
        while self._running:
            try:
                if not self.is_trading_hours():
                    # Ngoài giờ giao dịch — không query vnstock, sleep 60s
                    await asyncio.sleep(60)
                    continue

                tickers = list(self.subscribed)
                if tickers:
                    self._register_ws(tickers)
                    # WS đẩy realtime rồi → KBS chỉ còn nhiệm vụ cấp trần/sàn/TC
                    # (tĩnh cả ngày) nên gọi thưa hẳn: 30s thay vì 5s.
                    ws_on = self._ws_covers(tickers)
                    kbs_every = 30.0 if ws_on else 5.0
                    now = time.time()

                    updated: List[dict] = []
                    if now - last_kbs >= kbs_every or not self.quotes:
                        last_kbs = now
                        batches = [tickers[i:i+BATCH_SIZE]
                                   for i in range(0, len(tickers), BATCH_SIZE)]
                        for batch in batches:
                            updated.extend(await self.fetch_quotes(batch))
                            if len(batches) > 1:
                                await asyncio.sleep(1)
                    elif ws_on:
                        # Giữa 2 nhịp KBS: phát bản đã phủ WS → bảng giá vẫn realtime
                        updated = [q for q in (self.quotes.get(t) for t in tickers) if q]

                    if updated:
                        payload = [self._apply_ws(q) for q in updated]
                        for q in payload:            # cache bản mới nhất đã phủ WS
                            self.quotes[q["ticker"]] = q
                        for fn in self.listeners:
                            try:
                                await fn(payload)
                            except Exception as e:
                                print(f"Listener error: {e}")
                await asyncio.sleep(2 if self.subscribed else 5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Polling error: {e}")
                await asyncio.sleep(10)

    async def _preload(self):
        TOP = ["VIC","VHM","HPG","TCB","VCB","ACB","MWG","VNM","FPT","SSI","MBB","VPB"]
        self.subscribe(TOP)
        result = await self.fetch_quotes(TOP)
        print(f"✅ Preloaded {len(result)} quotes")

    @staticmethod
    def is_trading_hours() -> bool:
        """Giờ giao dịch HOSE/HNX: 9:00-11:30 và 13:00-15:01, ngày làm việc"""
        now = datetime.now()
        if now.weekday() >= 5:  # Thứ 7, Chủ nhật
            return False
        total = now.hour * 60 + now.minute
        return (9*60 <= total <= 11*60+30) or (13*60 <= total <= 15*60+1)


market_service = MarketDataService()
