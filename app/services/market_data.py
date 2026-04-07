import asyncio
import os
import time
from typing import Dict, List, Callable
from datetime import datetime
from collections import deque

class RateLimiter:
    def __init__(self, max_calls: int = 55, period: float = 60.0):
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
        self.listeners: List[Callable] = []
        self._task    = None
        self._running = False
        self._limiter = RateLimiter(max_calls=55, period=60.0)

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
        key = os.getenv("VNSTOCK_API_KEY", "")
        if not key:
            print("⚠️  VNSTOCK_API_KEY chưa được cấu hình – dùng guest mode")
            return
        try:
            import vnstock
            vnstock.change_api_key(key)
            print(f"✅ vnstock API key đã thiết lập: {key[:20]}...")
        except Exception as e:
            print(f"⚠️  Lỗi set API key: {e}")

    def subscribe(self, tickers: List[str]):
        for t in tickers:
            self.subscribed.add(t.upper())

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
        except Exception as e:
            print(f"_fetch_sync error: {e}")
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
        except Exception as e:
            print(f"_hist_sync error: {e}")
            return []

    async def _polling_loop(self):
        BATCH_SIZE = 20
        while self._running:
            try:
                tickers = list(self.subscribed)
                if tickers:
                    batches = [tickers[i:i+BATCH_SIZE]
                               for i in range(0, len(tickers), BATCH_SIZE)]
                    all_updated = []
                    for batch in batches:
                        updated = await self.fetch_quotes(batch)
                        all_updated.extend(updated)
                        if len(batches) > 1:
                            await asyncio.sleep(1)
                    if all_updated:
                        for fn in self.listeners:
                            try:
                                await fn(all_updated)
                            except Exception as e:
                                print(f"Listener error: {e}")
                await asyncio.sleep(5)
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
        now = datetime.now()
        if now.weekday() >= 5:
            return False
        total = now.hour * 60 + now.minute
        return (9*60 <= total <= 11*60+30) or (13*60 <= total <= 14*60+45)


market_service = MarketDataService()
