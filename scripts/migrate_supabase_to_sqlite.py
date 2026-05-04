#!/usr/bin/env python3
"""
migrate_supabase_to_sqlite.py  (v3 — gộp migration + cleanup duplicate watchlists)
────────────────────────────────────────────────────────────────────────────────────
1. Đọc toàn bộ dữ liệu từ Supabase
2. Xóa TOÀN BỘ dữ liệu cũ trên backend (trades, watchlists, accounts) để migrate sạch
3. Import lại từ Supabase → SQLite backend
4. Tự cleanup nếu còn watchlist trùng tên sau khi import

Chạy:
    cd /Users/NamLT/Desktop/vnstock-backend
    source venv/bin/activate
    python scripts/migrate_supabase_to_sqlite.py

Yêu cầu: pip install requests
"""

import sys
import time
import getpass

try:
    import requests
except ImportError:
    print("❌ Thiếu thư viện requests. Chạy: pip install requests")
    sys.exit(1)

# ── Cấu hình ─────────────────────────────────────────────────────────────────

SUPABASE_URL     = "https://uwfmkdfvymteysxkmnzd.supabase.co"
SUPABASE_SVC_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InV3Zm1rZGZ2eW10ZXlzeGttbnpkIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTQ4MzI2NSwiZXhwIjoyMDkxMDU5MjY1fQ.M0mRBSjCs0xkSstvKND6Q8rJ8xpYZ7amR42329BhEy8"
BACKEND_URL      = "http://67.215.255.242"

SUPA_HDR = {
    "apikey":        SUPABASE_SVC_KEY,
    "Authorization": f"Bearer {SUPABASE_SVC_KEY}",
    "Content-Type":  "application/json",
}


# ── Supabase helpers ──────────────────────────────────────────────────────────

def supa_get(table: str, params: dict = None) -> list:
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}",
                     headers=SUPA_HDR,
                     params={"limit": "10000", "offset": "0", **(params or {})},
                     timeout=30)
    r.raise_for_status()
    return r.json()


def supa_auth_users() -> list:
    r = requests.get(f"{SUPABASE_URL}/auth/v1/admin/users",
                     headers=SUPA_HDR, params={"per_page": 1000}, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("users", []) if isinstance(data, dict) else data


# ── Backend client ────────────────────────────────────────────────────────────

class Client:
    def __init__(self, base: str, token: str):
        self.base = base.rstrip("/")
        self.h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def get(self, path):
        r = requests.get(f"{self.base}{path}", headers=self.h, timeout=30)
        r.raise_for_status(); return r.json()

    def post(self, path, body, ok_status=None):
        r = requests.post(f"{self.base}{path}", json=body, headers=self.h, timeout=30)
        if ok_status and r.status_code != ok_status:
            raise Exception(f"POST {path} → {r.status_code}: {r.text[:200]}")
        if not r.ok:
            raise Exception(f"POST {path} → {r.status_code}: {r.text[:200]}")
        return r.json() if r.text else {}

    def put(self, path, body):
        r = requests.put(f"{self.base}{path}", json=body, headers=self.h, timeout=30)
        if not r.ok: raise Exception(f"PUT {path} → {r.status_code}: {r.text[:200]}")
        return r.json() if r.text else {}

    def delete(self, path):
        r = requests.delete(f"{self.base}{path}", headers=self.h, timeout=30)
        if not r.ok: raise Exception(f"DELETE {path} → {r.status_code}: {r.text[:200]}")
        return r.json() if r.text else {}


# ── Bước phụ: cleanup watchlist trùng ───────────────────────────────────────

def cleanup_duplicate_watchlists(client: Client):
    """Xóa watchlist trùng tên — giữ 1 bản có nhiều items nhất."""
    wls = client.get("/api/watchlists/")
    from collections import defaultdict
    by_name = defaultdict(list)
    for w in wls:
        by_name[w["name"]].append(w)

    found_dup = False
    for name, group in by_name.items():
        if len(group) <= 1:
            continue
        found_dup = True
        # Giữ bản có nhiều items nhất; nếu bằng nhau → giữ bản cũ nhất
        group.sort(key=lambda w: (-len(w.get("items") or []), w.get("created_at", "")))
        keep   = group[0]
        remove = group[1:]
        n = len(keep.get("items") or [])
        print(f"   🔁 '{name}': giữ lại bản có {n} mã, xóa {len(remove)} bản trùng")
        for w in remove:
            try:
                client.delete(f"/api/watchlists/{w['id']}")
                print(f"      🗑️  Đã xóa id={w['id'][:8]}...")
            except Exception as e:
                print(f"      ⚠️  Không xóa được id={w['id'][:8]}...: {e}")

    if not found_dup:
        print("   ✅ Không có watchlist trùng tên")


# ── Main migration ────────────────────────────────────────────────────────────

def migrate():
    print("=" * 62)
    print("  VNStock — Migrate Supabase → SQLite (v3)")
    print("=" * 62)

    # 1. Kết nối Supabase + chọn user
    print("\n📡 Đang kết nối Supabase...")
    try:
        supa_users = supa_auth_users()
    except Exception as e:
        print(f"❌ Không đọc được users từ Supabase: {e}"); sys.exit(1)

    if not supa_users:
        print("❌ Không tìm thấy user nào trong Supabase Auth"); sys.exit(1)

    print(f"✅ Tìm thấy {len(supa_users)} user trong Supabase")
    for i, u in enumerate(supa_users):
        print(f"   [{i+1}] {u.get('email')} (id: {u.get('id')})")

    if len(supa_users) == 1:
        chosen = supa_users[0]
        print(f"\n→ Tự động chọn: {chosen['email']}")
    else:
        try:
            idx = int(input(f"\nChọn user cần migrate [1-{len(supa_users)}]: ")) - 1
            chosen = supa_users[idx]
        except (ValueError, IndexError):
            print("❌ Lựa chọn không hợp lệ"); sys.exit(1)

    supa_uid = chosen["id"]
    email    = chosen["email"]
    print(f"\n👤 Migrate dữ liệu của: {email}")

    # 2. Mật khẩu
    print("\n🔑 Mật khẩu tài khoản backend (tối thiểu 6 ký tự):")
    while True:
        pw1 = getpass.getpass("   Mật khẩu: ")
        pw2 = getpass.getpass("   Nhập lại:  ")
        if pw1 != pw2:   print("   ❌ Không khớp"); continue
        if len(pw1) < 6: print("   ❌ Quá ngắn"); continue
        break

    # 3. Kiểm tra backend
    print(f"\n📡 Kiểm tra backend {BACKEND_URL}...")
    try:
        requests.get(f"{BACKEND_URL}/health", timeout=10).raise_for_status()
        print("✅ Backend đang hoạt động")
    except Exception as e:
        print(f"❌ Không kết nối được backend: {e}"); sys.exit(1)

    # 4. Đăng nhập / đăng ký
    print(f"\n📝 Xác thực tài khoản '{email}'...")
    r = requests.post(f"{BACKEND_URL}/api/auth/register",
                      json={"email": email, "password": pw1}, timeout=30)
    if r.status_code == 201:
        token = r.json()["token"]
        print("✅ Đã tạo tài khoản mới")
    elif r.status_code == 400 and "already registered" in r.text.lower():
        r2 = requests.post(f"{BACKEND_URL}/api/auth/login",
                           json={"email": email, "password": pw1}, timeout=30)
        if not r2.ok:
            print(f"❌ Đăng nhập thất bại: {r2.text}"); sys.exit(1)
        token = r2.json()["token"]
        print("✅ Đăng nhập thành công")
    else:
        print(f"❌ Xác thực thất bại: {r.text}"); sys.exit(1)

    client = Client(BACKEND_URL, token)

    # 5. Đọc dữ liệu từ Supabase
    print("\n📥 Đọc dữ liệu từ Supabase...")

    print("   • Đọc trades...")
    trades = supa_get("trades", {"user_id": f"eq.{supa_uid}",
                                  "order": "trade_date.asc,created_at.asc"})
    print(f"     → {len(trades)} giao dịch")

    print("   • Đọc broker_accounts...")
    accounts = supa_get("broker_accounts", {"user_id": f"eq.{supa_uid}"})
    print(f"     → {len(accounts)} tài khoản")

    print("   • Đọc watchlists + items (nested select)...")
    r = requests.get(f"{SUPABASE_URL}/rest/v1/watchlists", headers=SUPA_HDR, params={
        "select": "*,watchlist_items(*)",
        "user_id": f"eq.{supa_uid}",
        "order": "sort_order.asc,created_at.asc",
        "limit": "1000",
    }, timeout=30)
    r.raise_for_status()
    watchlists_raw = r.json()

    watchlists, items = [], []
    for wl in watchlists_raw:
        wl_items = wl.pop("watchlist_items", []) or []
        watchlists.append(wl)
        for it in wl_items:
            it["watchlist_id"] = wl["id"]
            items.append(it)
    print(f"     → {len(watchlists)} watchlist, {len(items)} items")

    print("   • Đọc user_settings...")
    settings_rows = supa_get("user_settings", {"user_id": f"eq.{supa_uid}"})
    settings_data = settings_rows[0]["settings"] if settings_rows else {}
    print(f"     → {'Có' if settings_data else 'Không có'} dữ liệu cài đặt")

    # 6. XÓA SẠCH dữ liệu cũ trên backend (tránh trùng lặp)
    print("\n🗑️  Xóa dữ liệu cũ trên backend để import sạch...")

    # Xóa tất cả trades
    try:
        client.delete("/api/portfolio/trades")
        print("   ✅ Đã xóa trades cũ")
    except Exception as e:
        print(f"   ⚠️  Xóa trades: {e}")

    # Xóa tất cả watchlists (kéo theo items do CASCADE)
    try:
        existing_wls = client.get("/api/watchlists/")
        for wl in existing_wls:
            try:
                client.delete(f"/api/watchlists/{wl['id']}")
            except Exception:
                pass
        print(f"   ✅ Đã xóa {len(existing_wls)} watchlist cũ")
    except Exception as e:
        print(f"   ⚠️  Xóa watchlists: {e}")

    # Xóa tất cả broker accounts
    try:
        existing_accs = client.get("/api/portfolio/accounts")
        for acc in existing_accs:
            try:
                client.delete(f"/api/portfolio/accounts/{acc['id']}")
            except Exception:
                pass
        print(f"   ✅ Đã xóa {len(existing_accs)} tài khoản cũ")
    except Exception as e:
        print(f"   ⚠️  Xóa accounts: {e}")

    # 7. Import broker accounts
    acct_id_map: dict = {}
    if accounts:
        print(f"\n💼 Import {len(accounts)} tài khoản môi giới...")
        for acc in accounts:
            try:
                new_acc = client.post("/api/portfolio/accounts", {
                    "account_name":   acc.get("account_name") or acc.get("name", ""),
                    "account_number": acc.get("account_number", ""),
                    "broker":         acc.get("broker", "MANUAL"),
                    "is_active":      bool(acc.get("is_active", True)),
                })
                acct_id_map[acc["id"]] = new_acc["id"]
                print(f"   ✅ {acc.get('account_name') or acc.get('name', 'N/A')}")
            except Exception as e:
                print(f"   ⚠️  Bỏ qua '{acc.get('account_name','?')}': {e}")

    # 8. Import trades
    if trades:
        print(f"\n📊 Import {len(trades)} giao dịch...")

        def map_trade(t):
            old_acc = t.get("broker_account_id")
            return {
                "ticker":            t.get("ticker", "").upper(),
                "exchange":          t.get("exchange", "HOSE") or "HOSE",
                "side":              (t.get("side") or "BUY").upper(),
                "quantity":          float(t.get("quantity") or 0),
                "price":             max(float(t.get("price") or 0), 0),
                "fee":               float(t.get("fee") or 0),
                "trade_date":        (t.get("trade_date") or "")[:10],
                "notes":             t.get("notes") or "",
                "source":            t.get("source") or "MANUAL",
                "broker_account_id": acct_id_map.get(old_acc) if old_acc else None,
            }

        BATCH, ok = 100, 0
        for i in range(0, len(trades), BATCH):
            batch = [map_trade(t) for t in trades[i:i + BATCH]]
            try:
                client.post("/api/portfolio/trades/batch", batch)
                ok += len(batch)
                print(f"   ✅ {ok}/{len(trades)} giao dịch")
            except Exception as e:
                print(f"   ❌ Lỗi batch {i}–{i+BATCH}: {e}")
                for td in batch:
                    try:
                        client.post("/api/portfolio/trades", td, ok_status=201)
                        ok += 1
                    except Exception as e2:
                        print(f"      ⚠️  Bỏ qua {td['ticker']} {td['trade_date']}: {e2}")
            time.sleep(0.05)

        print(f"   → Tổng: {ok}/{len(trades)} giao dịch import thành công")

    # 9. Import watchlists + items
    if watchlists:
        print(f"\n📋 Import {len(watchlists)} watchlist...")
        for wl in watchlists:
            try:
                new_wl = client.post("/api/watchlists/", {"name": wl.get("name", "Watchlist")})
                wl_items = [it for it in items if it["watchlist_id"] == wl["id"]]
                ok_items = 0
                for item in wl_items:
                    try:
                        client.post(f"/api/watchlists/{new_wl['id']}/items", {
                            "ticker":      (item.get("ticker") or "").upper(),
                            "note":        item.get("note") or "",
                            "alert_price": item.get("alert_price"),
                        })
                        ok_items += 1
                    except Exception as e:
                        if "409" not in str(e):
                            print(f"      ⚠️  {item.get('ticker','?')}: {e}")
                print(f"   ✅ '{wl['name']}' — {ok_items}/{len(wl_items)} mã")
            except Exception as e:
                print(f"   ⚠️  Bỏ qua watchlist '{wl.get('name','?')}': {e}")

    # 10. Import settings
    if settings_data:
        print("\n⚙️  Import cài đặt người dùng...")
        try:
            client.put("/api/user/settings",
                       settings_data if isinstance(settings_data, dict) else {})
            print("   ✅ Đã import settings")
        except Exception as e:
            print(f"   ⚠️  Bỏ qua settings: {e}")

    # 11. Cleanup watchlists trùng (phòng ngừa)
    print("\n🔍 Kiểm tra watchlist trùng tên...")
    try:
        cleanup_duplicate_watchlists(client)
    except Exception as e:
        print(f"   ⚠️  Cleanup error: {e}")

    # 12. Hoàn tất
    print("\n" + "=" * 62)
    print("  ✅ MIGRATION HOÀN TẤT!")
    print("=" * 62)
    print(f"\n📌 Thông tin đăng nhập:")
    print(f"   Email:    {email}")
    print(f"   Mật khẩu: (mật khẩu vừa nhập)")
    print(f"   Backend:  {BACKEND_URL}")
    print()


if __name__ == "__main__":
    migrate()
