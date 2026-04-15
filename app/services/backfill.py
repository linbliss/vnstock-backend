"""Backfill & daily update engine cho OHLCV store.

Scopes hỗ trợ:
  VN30, HOSE, HNX, UPCOM, HOSE_HNX, ALL, <CSV_LIST>

API:
  start_backfill(scope, years=10, start_date=None, end_date=None) -> job_id (async)
  daily_update() -> cập nhật tất cả ticker trong store từ last_date → hôm nay
  get_tickers_for_scope(scope) -> List[str]
"""
import asyncio
import uuid
from datetime import datetime, timedelta
from typing import List, Optional, Dict

import pandas as pd

from app.services import ohlcv_store
from app.services.market_data import market_service

VN30 = [
    "VIC","VHM","HPG","TCB","VCB","ACB","MWG","VNM","FPT","SSI",
    "MBB","VPB","HDB","BCM","MSN","STB","CTG","BID","GAS","SAB",
    "VJC","PLX","POW","VRE","GVR",
]

# Job cancellation flags
_cancel_flags: Dict[str, bool] = {}


def _listing_for(exchange: str) -> List[str]:
    """Lấy ticker list từ vnstock Listing. Không rate-limit ở đây —
    caller nên acquire limiter trước."""
    try:
        from vnstock import Listing
        df = Listing().symbols_by_exchange()
        if df is None or df.empty:
            return []
        filtered = df[
            (df["exchange"].astype(str).str.upper() == exchange.upper())
            & (df["type"] == "stock")
        ]
        return filtered["symbol"].dropna().astype(str).str.upper().tolist()
    except BaseException as e:
        print(f"⚠️  listing {exchange}: {type(e).__name__}: {e}")
        return []


async def get_tickers_for_scope(scope: str) -> List[str]:
    """Scope → list ticker. Scope không match → thử parse CSV."""
    s = (scope or "").strip().upper()
    loop = asyncio.get_event_loop()

    async def listing(ex: str) -> List[str]:
        await market_service._limiter.acquire()
        return await loop.run_in_executor(None, _listing_for, ex)

    if s == "VN30":
        return list(VN30)
    if s == "HOSE":
        return await listing("HOSE")
    if s == "HNX":
        return await listing("HNX")
    if s == "UPCOM":
        return await listing("UPCOM")
    if s in ("HOSE_HNX", "HOSEHNX", "HOSE+HNX"):
        a = await listing("HOSE")
        b = await listing("HNX")
        seen, out = set(), []
        for t in a + b:
            if t not in seen:
                seen.add(t); out.append(t)
        return out
    if s == "ALL":
        a = await listing("HOSE")
        b = await listing("HNX")
        c = await listing("UPCOM")
        seen, out = set(), []
        for t in a + b + c:
            if t not in seen:
                seen.add(t); out.append(t)
        return out
    # CSV fallback
    return [t.strip().upper() for t in scope.split(",") if t.strip()]


def _fetch_history_sync(ticker: str, start: str, end: str) -> Optional[pd.DataFrame]:
    """Gọi vnstock Quote.history cho 1 ticker. Thử kbs → vci. Trả df normalized
    với cột date/open/high/low/close/volume hoặc None."""
    from vnstock import Quote
    for source in ("kbs", "vci"):
        try:
            raw = Quote(symbol=ticker.upper(), source=source).history(
                start=start, end=end, interval="1D"
            )
            if raw is None or raw.empty:
                continue
            df = raw.reset_index()
            col_map = {}
            for c in df.columns:
                cl = str(c).lower()
                if   "time"   in cl or cl == "date": col_map[c] = "date"
                elif "close"  in cl: col_map[c] = "close"
                elif "open"   in cl: col_map[c] = "open"
                elif "high"   in cl: col_map[c] = "high"
                elif "low"    in cl: col_map[c] = "low"
                elif "volume" in cl: col_map[c] = "volume"
            df = df.rename(columns=col_map)
            if "date" not in df.columns:
                # vnstock fallback — dùng index
                df["date"] = pd.to_datetime(df.index)
            keep = [c for c in ("date","open","high","low","close","volume") if c in df.columns]
            return df[keep]
        except BaseException as e:
            print(f"⚠️  {ticker} via {source}: {type(e).__name__}: {e}")
            continue
    return None


async def _backfill_one(ticker: str, start: str, end: str) -> int:
    """Fetch + upsert 1 ticker. Trả số rows."""
    await market_service._limiter.acquire()
    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, _fetch_history_sync, ticker, start, end)
    if df is None or df.empty:
        return 0
    return ohlcv_store.upsert_ohlcv(ticker, df)


async def _run_job(job_id: str, tickers: List[str], start: str, end: str) -> None:
    """Background task chạy backfill tuần tự. Mỗi ticker = 1 acquire."""
    completed, failed = 0, 0
    try:
        for i, t in enumerate(tickers):
            if _cancel_flags.get(job_id):
                ohlcv_store.update_job(job_id, completed=completed, failed=failed,
                                       status="cancelled",
                                       message=f"cancelled at {i}/{len(tickers)}")
                return
            try:
                n = await _backfill_one(t, start, end)
                if n > 0:
                    completed += 1
                else:
                    failed += 1
            except BaseException as e:
                failed += 1
                print(f"❌ backfill {t}: {type(e).__name__}: {e}")
            # Update progress mỗi 5 tickers
            if (i + 1) % 5 == 0 or i + 1 == len(tickers):
                ohlcv_store.update_job(
                    job_id, completed=completed, failed=failed,
                    message=f"{i+1}/{len(tickers)} — last={t}",
                )
        ohlcv_store.update_job(
            job_id, completed=completed, failed=failed,
            status="done", message=f"done {completed}/{len(tickers)} ok, {failed} failed",
        )
        print(f"✅ Backfill job {job_id} done: {completed} ok, {failed} failed")
    except BaseException as e:
        ohlcv_store.update_job(
            job_id, completed=completed, failed=failed,
            status="error", message=f"{type(e).__name__}: {e}",
        )
    finally:
        _cancel_flags.pop(job_id, None)


async def start_backfill(
    scope: str,
    years: int = 10,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict:
    """Khởi tạo backfill job, trả về {job_id, total, tickers, start, end}."""
    now = datetime.now()
    end   = end_date   or now.strftime("%Y-%m-%d")
    start = start_date or (now - timedelta(days=int(years * 365.25))).strftime("%Y-%m-%d")

    tickers = await get_tickers_for_scope(scope)
    if not tickers:
        raise ValueError(f"Scope '{scope}' không trả về ticker nào")

    job_id = uuid.uuid4().hex[:12]
    ohlcv_store.create_job(job_id, scope, start, end, len(tickers))
    _cancel_flags[job_id] = False
    asyncio.create_task(_run_job(job_id, tickers, start, end))
    return {
        "job_id":  job_id,
        "scope":   scope,
        "total":   len(tickers),
        "start":   start,
        "end":     end,
    }


def cancel_backfill(job_id: str) -> bool:
    if job_id not in _cancel_flags:
        return False
    _cancel_flags[job_id] = True
    return True


async def daily_update() -> Dict:
    """Cập nhật tất cả ticker trong store từ last_date (+1) → hôm nay.
    Chạy tự động lúc 16:00 Việt Nam."""
    tickers = ohlcv_store.list_tickers()
    if not tickers:
        return {"updated": 0, "total": 0, "message": "store empty"}
    today = datetime.now().strftime("%Y-%m-%d")
    updated, failed = 0, 0
    for t in tickers:
        last = ohlcv_store.get_last_date(t)
        if last and last >= today:
            continue
        # start = last+1 nếu có, ngược lại 7 ngày trước (safety)
        if last:
            nxt = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            nxt = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        try:
            n = await _backfill_one(t, nxt, today)
            if n > 0:
                updated += 1
        except BaseException as e:
            failed += 1
            print(f"❌ daily_update {t}: {type(e).__name__}: {e}")
    msg = f"daily_update {datetime.now().isoformat()}: {updated}/{len(tickers)} updated, {failed} failed"
    print("✅", msg)
    return {"updated": updated, "failed": failed, "total": len(tickers), "message": msg}


async def daily_update_scheduler():
    """Chạy daily_update mỗi ngày lúc 16:00 giờ Việt Nam.
    Container không có TZ → dùng datetime.now() (VPS đã set TZ Asia/Ho_Chi_Minh
    hoặc lifespan sẽ log để user biết). Nếu TZ khác, đặt env TZ=Asia/Ho_Chi_Minh.
    """
    while True:
        now = datetime.now()
        target = now.replace(hour=16, minute=0, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        wait = (target - now).total_seconds()
        print(f"⏰ Daily update scheduled at {target.isoformat()} (in {wait/3600:.1f}h)")
        try:
            await asyncio.sleep(wait)
            # Chỉ chạy ngày làm việc
            if datetime.now().weekday() < 5:
                await daily_update()
            else:
                print("⏭️  Weekend — skip daily_update")
        except asyncio.CancelledError:
            break
        except BaseException as e:
            print(f"daily_update_scheduler error: {type(e).__name__}: {e}")
            await asyncio.sleep(300)
