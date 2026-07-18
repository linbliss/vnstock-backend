"""Backfill & daily update engine cho OHLCV store.

Scopes hỗ trợ:
  VN30, HOSE, HNX, UPCOM, HOSE_HNX, ALL, <CSV_LIST>

API:
  start_backfill(scope, years=10, start_date=None, end_date=None) -> job_id (async)
  daily_update() -> cập nhật tất cả ticker trong store từ last_date → hôm nay
  get_tickers_for_scope(scope) -> List[str]
"""
import asyncio
import os
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
_live: Dict[str, dict] = {}   # tiến độ realtime trong bộ nhớ (không phụ thuộc ghi DB)


def get_live(job_id: str) -> Optional[dict]:
    return _live.get(job_id)


def _listing_for(exchange: str) -> List[str]:
    """Lấy ticker list 1 sàn: SQLite (cục bộ) → DNSE → vnstock Listing."""
    # stock_list là dữ liệu CỤC BỘ → thử trước, KHÔNG đặt sau cổng use_dnse()
    # (DNSE bị chặn ⇒ breaker ngắt ⇒ rơi xuống vnstock chậm/lỗi ⇒ backfill 0 mã).
    saved = ohlcv_store.get_tickers_by_exchange(exchange)
    if saved:
        return saved
    try:
        from app.services import dnse_client, data_source
        if data_source.use_dnse("ticker_list"):
            tks = dnse_client.get_tickers_by_exchange(exchange)
            if tks:
                return tks
    except Exception as e:  # noqa: BLE001
        print(f"⚠️  listing {exchange} DNSE: {type(e).__name__}: {e}")
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
        from app.services import data_source
        if not data_source.use_dnse("ticker_list"):   # DNSE có danh sách mã sẵn, không cần phanh
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
    """Lịch sử OHLCV 1 ticker → df date/open/high/low/close/volume.
    Ưu tiên DNSE (nếu có key) → fallback vnstock (kbs → vci)."""
    # DNSE trước
    try:
        from app.services import dnse_client, data_source
        if data_source.use_dnse("ohlcv"):
            rows = dnse_client.get_ohlc_history(ticker, start, end)
            if rows:
                return pd.DataFrame(rows)
    except Exception as e:  # noqa: BLE001
        print(f"⚠️  {ticker} DNSE ohlc: {type(e).__name__}: {e}", flush=True)

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


async def _backfill_one(ticker: str, start: str, end: str, timeout: float = 45.0) -> int:
    """Fetch + upsert 1 ticker. Trả số rows."""
    from app.services import data_source
    if not data_source.use_dnse("ohlcv"):   # DNSE không cần phanh 35/60 của vnstock
        await market_service._limiter.acquire()
    loop = asyncio.get_event_loop()
    print(f"→ backfill {ticker} [{start}..{end}]", flush=True)
    try:
        df = await asyncio.wait_for(
            loop.run_in_executor(None, _fetch_history_sync, ticker, start, end),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        print(f"⏱️  {ticker} TIMEOUT sau {timeout}s — skip", flush=True)
        return 0
    if df is None or df.empty:
        print(f"∅ {ticker}: no data", flush=True)
        return 0
    # Upsert chạy TRONG executor để KHÔNG block event loop (iterrows + ghi DB nặng,
    # nhất là 15 năm ~3500 dòng/mã) → progress cập nhật + fetch song song thật sự.
    n = await loop.run_in_executor(None, ohlcv_store.upsert_ohlcv, ticker, df)
    print(f"✅ {ticker}: {n} rows saved", flush=True)
    return n


def _fetch_and_store(ticker: str, start: str, end: str, cutoff: str) -> str:
    """Chạy HOÀN TOÀN trong thread (không đụng event loop): check-skip → fetch → upsert.
    Trả 'skipped' | 'ok' | 'empty' | 'fail'."""
    try:
        last = ohlcv_store.get_last_date(ticker)
        if last and last >= cutoff:
            return "skipped"
        df = _fetch_history_sync(ticker, start, end)
        if df is None or df.empty:
            return "empty"
        ohlcv_store.upsert_ohlcv(ticker, df)
        return "ok"
    except BaseException as e:  # noqa: BLE001
        print(f"❌ backfill {ticker}: {type(e).__name__}: {e}", flush=True)
        return "fail"


async def _run_job(job_id: str, tickers: List[str], start: str, end: str) -> None:
    """Background task chạy backfill tuần tự. Mỗi ticker = 1 acquire.

    Resume-friendly: nếu ticker đã có last_date >= end-3 ngày (lịch giao dịch) →
    skip, không tốn API call. Để re-download đầy đủ, xoá row trong backfill_status
    trước hoặc gọi force endpoint (chưa có).
    """
    completed, failed, skipped = 0, 0, 0
    # Ticker được coi là "đủ" nếu last_date >= end - 3 ngày (bù cuối tuần/lễ)
    from datetime import datetime, timedelta
    try:
        cutoff = (datetime.strptime(end, "%Y-%m-%d") - timedelta(days=3)).strftime("%Y-%m-%d")
    except Exception:
        cutoff = end
    try:
        from app.services import data_source
        total = len(tickers)

        # ── DNSE: chạy SONG SONG (OHLC 50k/giờ nên thoải mái) ──
        # QUAN TRỌNG: MỌI thao tác DB (get_last_date/upsert/update_job) chạy trong
        # THREAD (executor), KHÔNG trên event loop — vì DB dùng threading _lock; nếu
        # gọi trên loop khi thread đang giữ _lock để ghi → cả loop đứng (job/progress/cancel treo).
        if data_source.use_dnse("ohlcv"):
            import functools
            from concurrent.futures import ThreadPoolExecutor
            conc = int(os.environ.get("DNSE_BACKFILL_CONCURRENCY", "6"))
            executor = ThreadPoolExecutor(max_workers=conc + 2)
            sem = asyncio.Semaphore(conc)
            cnt = {"completed": 0, "failed": 0, "skipped": 0, "done": 0, "total": total}
            _live[job_id] = cnt        # tiến độ realtime (endpoint đọc thẳng, không cần ghi DB)
            loop = asyncio.get_event_loop()

            async def _update(**kw):
                await loop.run_in_executor(executor, functools.partial(ohlcv_store.update_job, job_id, **kw))

            async def _worker(t: str):
                if _cancel_flags.get(job_id):
                    return
                async with sem:
                    if _cancel_flags.get(job_id):
                        return
                    r = await loop.run_in_executor(executor, _fetch_and_store, t, start, end, cutoff)
                if r == "skipped":
                    cnt["skipped"] += 1; cnt["completed"] += 1
                elif r == "ok":
                    cnt["completed"] += 1
                else:
                    cnt["failed"] += 1
                cnt["done"] += 1
                # Ghi DB thưa (100 mã/lần) để đỡ tranh write-lock; tiến độ realtime đã có trong _live
                if cnt["done"] % 100 == 0:
                    await _update(completed=cnt["completed"], failed=cnt["failed"],
                                  message=f"{cnt['done']}/{total} (song song {conc})")

            try:
                await asyncio.gather(*[_worker(t) for t in tickers])
            finally:
                executor.shutdown(wait=False)
            status = "cancelled" if _cancel_flags.get(job_id) else "done"
            cnt["status"] = status
            await _update(completed=cnt["completed"], failed=cnt["failed"], status=status,
                          message=f"{status} {cnt['completed']}/{total} ok ({cnt['skipped']} skipped), {cnt['failed']} failed")
            print(f"✅ Backfill job {job_id} {status}: {cnt['completed']}/{total} ok ({cnt['skipped']} skipped), {cnt['failed']} failed", flush=True)
            return

        # ── vnstock: tuần tự (giữ phanh limiter) ──
        for i, t in enumerate(tickers):
            if _cancel_flags.get(job_id):
                ohlcv_store.update_job(job_id, completed=completed, failed=failed,
                                       status="cancelled",
                                       message=f"cancelled at {i}/{len(tickers)} (skipped={skipped})")
                return
            # Skip nếu đã có data tới end
            last = ohlcv_store.get_last_date(t)
            if last and last >= cutoff:
                skipped += 1
                completed += 1
                if (i + 1) % 10 == 0:
                    print(f"⏭  {t} đã có tới {last} — skip ({skipped} skipped)", flush=True)
                continue
            try:
                n = await _backfill_one(t, start, end)
                if n > 0:
                    completed += 1
                else:
                    failed += 1
            except BaseException as e:
                failed += 1
                print(f"❌ backfill {t}: {type(e).__name__}: {e}", flush=True)
            # Update progress mỗi 5 tickers
            if (i + 1) % 5 == 0 or i + 1 == len(tickers):
                ohlcv_store.update_job(
                    job_id, completed=completed, failed=failed,
                    message=f"{i+1}/{len(tickers)} — last={t}",
                )
        ohlcv_store.update_job(
            job_id, completed=completed, failed=failed,
            status="done",
            message=f"done {completed}/{len(tickers)} ok ({skipped} skipped), {failed} failed",
        )
        print(f"✅ Backfill job {job_id} done: {completed} ok ({skipped} skipped), {failed} failed", flush=True)
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
    Chạy tự động lúc 16:00 Việt Nam.

    Tự động phát hiện ĐIỀU CHỈNH GIÁ NGƯỢC (thưởng CP / cổ tức CP / tách-gộp) bằng cách
    fetch cửa sổ CHỒNG LẤN rồi so khớp giá cùng ngày store↔nguồn: lệch >1% ⇒ đã điều
    chỉnh ⇒ refetch toàn bộ. Bắt được cả cổ tức CP nhỏ (5-15%) mà ngưỡng 15% cũ bỏ sót.
    """
    tickers = ohlcv_store.list_tickers()
    if not tickers:
        return {"updated": 0, "total": 0, "message": "store empty"}
    today = datetime.now().strftime("%Y-%m-%d")
    updated, failed, readjusted = 0, 0, []

    for t in tickers:
        last = ohlcv_store.get_last_date(t)
        if last and last >= today:
            continue
        try:
            # Fetch 1 lần: cửa sổ CHỒNG LẤN (so khớp điều chỉnh) + ngày mới.
            if last:
                ov_start = (datetime.strptime(last, "%Y-%m-%d")
                            - timedelta(days=OVERLAP_DAYS)).strftime("%Y-%m-%d")
            else:
                ov_start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

            df = await _fetch_only(t, ov_start, today)
            if df is None or df.empty:
                continue

            # Điều chỉnh ngược? So giá cùng ngày trên phần chồng lấn.
            if last:
                stored = ohlcv_store.get_ohlcv(t, ov_start, last)
                if _price_mismatch(stored, df):
                    print(f"🔔 {t}: phát hiện điều chỉnh giá ngược → refetch toàn bộ", flush=True)
                    await refetch_ticker(t)
                    readjusted.append(t)
                    updated += 1
                    continue

            n = await asyncio.get_event_loop().run_in_executor(
                None, ohlcv_store.upsert_ohlcv, t, df)
            if n > 0:
                updated += 1
        except BaseException as e:
            failed += 1
            print(f"❌ daily_update {t}: {type(e).__name__}: {e}")

    msg = (
        f"daily_update {datetime.now().isoformat()}: "
        f"{updated}/{len(tickers)} updated, {failed} failed"
    )
    if readjusted:
        msg += f", {len(readjusted)} readjusted: {readjusted}"
    print("✅", msg)
    return {"updated": updated, "failed": failed, "total": len(tickers),
            "readjusted": readjusted, "message": msg}


async def refetch_ticker(ticker: str, years: int = 3) -> int:
    """Xoá OHLCV cũ và fetch lại toàn bộ lịch sử cho 1 ticker.
    Dùng khi phát hiện corporate action (giá điều chỉnh ngược) hoặc gọi thủ công.
    GIỮ NGUYÊN bề dày lịch sử: refetch từ first_date đã có (không co lại còn `years`).
    Trả về số rows đã ghi."""
    t = ticker.upper()
    first = ohlcv_store.get_ohlcv_first_date(t)   # giữ span cũ nếu đã backfill dài
    default_start = (datetime.now() - timedelta(days=365 * years)).strftime("%Y-%m-%d")
    start = min(first, default_start) if first else default_start
    end   = datetime.now().strftime("%Y-%m-%d")
    print(f"🔄 refetch_ticker {t}: xoá cache cũ, fetch lại [{start}..{end}]", flush=True)
    ohlcv_store.delete_ohlcv(t)
    n = await _backfill_one(t, start, end, timeout=120.0)
    print(f"✅ refetch_ticker {t}: {n} rows", flush=True)
    return n


# ── Phát hiện ĐIỀU CHỈNH GIÁ NGƯỢC (thưởng CP / cổ tức CP / tách-gộp) ──────────────
# Cách cũ (so close cuối trước/sau khi thêm ngày mới, ngưỡng 15%) bỏ SÓT cổ tức CP
# nhỏ (5-15%) và phụ thuộc daily_update chạy đúng ex-date. Cách mới: SO KHỚP GIÁ CÙNG
# NGÀY giữa store và nguồn trên một cửa sổ chồng lấn — nguồn luôn trả giá đã điều chỉnh
# ngược, nên nếu giá cùng 1 ngày lệch >1% ⇒ đã có điều chỉnh ⇒ refetch toàn bộ.
OVERLAP_DAYS = 12          # ~8 phiên chồng lấn để so khớp
ADJ_EPS = 0.01             # lệch >1% ở ngày chung = đã điều chỉnh (bỏ nhiễu làm tròn)


async def _fetch_only(ticker: str, start: str, end: str, timeout: float = 45.0):
    """Fetch OHLCV nhưng KHÔNG upsert — để so khớp trước khi quyết định."""
    from app.services import data_source
    if not data_source.use_dnse("ohlcv"):
        await market_service._limiter.acquire()
    loop = asyncio.get_event_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, _fetch_history_sync, ticker, start, end),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        return None


def _price_mismatch(stored, fresh, eps: float = ADJ_EPS) -> bool:
    """True nếu giá đóng cửa CÙNG NGÀY lệch > eps giữa store và nguồn (đã điều chỉnh)."""
    if stored is None or fresh is None or stored.empty or fresh.empty:
        return False
    s = stored.set_index("date")["close"]
    f = fresh.set_index("date")["close"]
    common = [d for d in s.index if d in f.index]
    if len(common) < 3:
        return False
    for d in common[-6:]:
        sv, fv = float(s.loc[d]), float(f.loc[d])
        if sv > 0 and abs(fv - sv) / sv > eps:
            return True
    return False


async def verify_and_readjust(tickers: Optional[List[str]] = None) -> Dict:
    """Rà tất cả (hoặc 1 nhóm) ticker: nếu giá lịch sử đã bị điều chỉnh ngược ở nguồn
    mà store chưa cập nhật → refetch toàn bộ. Sửa dữ liệu tồn đọng mà không phải
    refetch từng mã thủ công."""
    tickers = tickers or ohlcv_store.list_tickers()
    checked, readjusted, failed = 0, [], 0
    for t in tickers:
        last = ohlcv_store.get_last_date(t)
        if not last:
            continue
        try:
            ov_start = (datetime.strptime(last, "%Y-%m-%d")
                        - timedelta(days=OVERLAP_DAYS)).strftime("%Y-%m-%d")
            fresh = await _fetch_only(t, ov_start, last)
            stored = ohlcv_store.get_ohlcv(t, ov_start, last)
            if _price_mismatch(stored, fresh):
                await refetch_ticker(t)
                readjusted.append(t)
            checked += 1
        except BaseException as e:  # noqa: BLE001
            failed += 1
            print(f"❌ verify {t}: {type(e).__name__}: {e}", flush=True)
        await asyncio.sleep(0.05)
    msg = f"verify_and_readjust: {checked} checked, {len(readjusted)} readjusted, {failed} failed"
    print(f"✅ {msg}: {readjusted}", flush=True)
    return {"checked": checked, "readjusted": readjusted, "failed": failed, "message": msg}


async def refresh_fundamentals() -> Dict:
    """Cập nhật EPS/ROE cho tất cả ticker có trong OHLCV store mà data đã quá 7 ngày.
    Chạy tự động sau daily_update (mỗi ngày thứ Hai) hoặc gọi thủ công từ admin.
    """
    stale = ohlcv_store.list_stale_fundamentals()
    if not stale:
        print("✅ Fundamental: tất cả đã cập nhật, không cần refresh")
        return {"refreshed": 0, "total": 0, "message": "all up to date"}

    print(f"📊 Fundamental refresh: {len(stale)} ticker cần cập nhật", flush=True)

    # Import fetch function từ screener router
    from app.routers.screener import _fetch_fundamental_via_api

    refreshed, failed = 0, 0
    loop = asyncio.get_event_loop()
    for i, t in enumerate(stale):
        try:
            await market_service._limiter.acquire()
            result = await asyncio.wait_for(
                loop.run_in_executor(None, _fetch_fundamental_via_api, t),
                timeout=30.0,
            )
            if result.get("eps"):
                ohlcv_store.upsert_fundamental(t, result)
                refreshed += 1
            else:
                failed += 1
        except asyncio.TimeoutError:
            failed += 1
            print(f"⏱️  fundamental {t} TIMEOUT", flush=True)
        except BaseException as e:
            failed += 1
            print(f"❌ fundamental {t}: {type(e).__name__}: {e}", flush=True)
        if (i + 1) % 20 == 0:
            print(f"📊 Fundamental progress: {i+1}/{len(stale)} ({refreshed} ok, {failed} fail)", flush=True)

    msg = f"fundamental refresh: {refreshed}/{len(stale)} ok, {failed} failed"
    print(f"✅ {msg}", flush=True)
    return {"refreshed": refreshed, "failed": failed, "total": len(stale), "message": msg}


async def daily_update_scheduler():
    """Chạy daily_update mỗi ngày lúc 16:00 giờ Việt Nam.
    Fundamental refresh chạy thêm vào thứ Hai (sau daily_update).
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
            weekday = datetime.now().weekday()
            # Chỉ chạy ngày làm việc
            if weekday < 5:
                await daily_update()
                # Thứ Hai: refresh fundamental cho tất cả ticker stale
                if weekday == 0:
                    print("📊 Thứ Hai — chạy fundamental refresh", flush=True)
                    await refresh_fundamentals()
                # Rebuild snapshot screener sau khi data EOD đã cập nhật
                # → sáng hôm sau app mở lên có kết quả mới nhất ngay (instant)
                try:
                    from app.services.screener import screener_service
                    for ex in ("hose", "hnx", "upcom"):
                        await screener_service.build_snapshot(ex)
                except BaseException as e:
                    print(f"snapshot rebuild error: {type(e).__name__}: {e}")
            else:
                print("⏭️  Weekend — skip daily_update")
        except asyncio.CancelledError:
            break
        except BaseException as e:
            print(f"daily_update_scheduler error: {type(e).__name__}: {e}")
            await asyncio.sleep(300)
