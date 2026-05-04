#!/usr/bin/env python3
"""
migrate_supabase_to_sqlite.py
─────────────────────────────
Đọc toàn bộ dữ liệu từ Supabase (trades, watchlists, broker_accounts, settings)
rồi import vào backend SQLite mới qua REST API.

Chạy:
    python scripts/migrate_supabase_to_sqlite.py

Yêu cầu:
    pip install requests
"""

import json
import sys
import time
import getpass

try:
    import requests
except ImportError:
    print("❌ Thiếu thư viện requests. Chạy: pip install requests")
    sys.exit(1)

# ── Cấu hình (lấy từ .env) ────────────────────────────────────────────────────

SUPABASE_URL     = "https://uwfmkdfvymteysxkmnzd.supabase.co"
SUPABASE_SVC_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InV3Zm1rZGZ2eW10ZXlzeGttbnpkIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTQ4MzI2NSwiZXhwIjoyMDkxMDU5MjY1fQ.M0mRBSjCs0xkSstvKND6Q8rJ8xpYZ7amR42329BhEy8"
BACKEND_URL      = "http://67.215.255.242"

SUPA_HEADERS = {
    "apikey":        SUPABASE_SVC_KEY,
    "Authorization": f"Bearer {SUPABASE_SVC_KEY}",
    "Content-Type":  "application/json",
}


# ── Supabase helpers ──────────────────────────────────────────────────────────

def supa_get(table: str, params: dict = None) -> list:
    """Đọc dữ liệu từ Supabase REST API (có phân trang, lấy tối đa 10000 rows)."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    p = {"limit": "10000", "offset": "0", **(params or {})}
    r = requests.get(url, headers=SUPA_HEADERS, params=p, timeout=30)
    r.raise_for_status()
    return r.json()


def supa_auth_users() -> list:
    """Lấy danh sách users từ Supabase Auth Admin API."""
    url = f"{SUPABASE_URL}/auth/v1/admin/users"
    r = requests.get(url, headers=SUPA_HEADERS, params={"per_page": 1000}, timeout=30)
    r.raise_for_status()
    data = r.json()
    # Supabase trả về {"users": [...]} hoặc list trực tiếp
    if isinstance(data, dict):
        return data.get("users", [])
    return data


# ── Backend helpers ───────────────────────────────────────────────────────────

class BackendClient:
    def __init__(self, base_url: str, token: str):
        self.base = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        }

    def post(self, path: str, body: dict, expected_status: int = None) -> dict:
        r = requests.post(f"{self.base}{path}", json=body, headers=self.headers, timeout=30)
        if expected_status and r.status_code != expected_status:
            raise Exception(f"POST {path} → {r.status_code}: {r.text}")
        if not r.ok:
            raise Exception(f"POST {path} → {r.status_code}: {r.text}")
        return r.json() if r.text else {}

    def put(self, path: str, body: dict) -> dict:
        r = requests.put(f"{self.base}{path}", json=body, headers=self.headers, timeout=30)
        if not r.ok:
            raise Exception(f"PUT {path} → {r.status_code}: {r.text}")
        return r.json() if r.text else {}

    def delete(self, path: str) -> dict:
        r = requests.delete(f"{self.base}{path}", headers=self.headers, timeout=30)
        if not r.ok:
            raise Exception(f"DELETE {path} → {r.status_code}: {r.text}")
        return r.json() if r.text else {}


# ── Migration logic ───────────────────────────────────────────────────────────

def migrate():
    print("=" * 60)
    print("  VNStock — Migrate Supabase → SQLite Backend")
    print("=" * 60)

    # 1. Đọc danh sách users từ Supabase Auth
    print("\n📡 Đang kết nối Supabase...")
    try:
        supa_users = supa_auth_users()
    except Exception as e:
        print(f"❌ Không đọc được users từ Supabase: {e}")
        sys.exit(1)

    if not supa_users:
        print("❌ Không tìm thấy user nào trong Supabase Auth")
        sys.exit(1)

    print(f"✅ Tìm thấy {len(supa_users)} user trong Supabase")
    for i, u in enumerate(supa_users):
        print(f"   [{i+1}] {u.get('email')} (id: {u.get('id')})")

    # 2. Chọn user cần migrate
    if len(supa_users) == 1:
        chosen = supa_users[0]
        print(f"\n→ Tự động chọn: {chosen['email']}")
    else:
        try:
            idx = int(input(f"\nChọn user cần migrate [1-{len(supa_users)}]: ")) - 1
            chosen = supa_users[idx]
        except (ValueError, IndexError):
            print("❌ Lựa chọn không hợp lệ")
            sys.exit(1)

    supa_user_id = chosen["id"]
    email        = chosen["email"]
    print(f"\n👤 Sẽ migrate dữ liệu của: {email}")

    # 3. Đặt mật khẩu mới cho tài khoản SQLite
    print("\n🔑 Đặt mật khẩu cho tài khoản backend mới (tối thiểu 6 ký tự):")
    while True:
        pw1 = getpass.getpass("   Mật khẩu mới: ")
        pw2 = getpass.getpass("   Nhập lại:     ")
        if pw1 != pw2:
            print("   ❌ Mật khẩu không khớp, thử lại")
            continue
        if len(pw1) < 6:
            print("   ❌ Mật khẩu quá ngắn (tối thiểu 6 ký tự)")
            continue
        break

    # 4. Kiểm tra backend có hoạt động không
    print(f"\n📡 Kiểm tra backend {BACKEND_URL}...")
    try:
        r = requests.get(f"{BACKEND_URL}/health", timeout=10)
        r.raise_for_status()
        print("✅ Backend đang hoạt động")
    except Exception as e:
        print(f"❌ Không kết nối được backend: {e}")
        print(f"   Kiểm tra lại BACKEND_URL: {BACKEND_URL}")
        sys.exit(1)

    # 5. Đăng ký / đăng nhập tài khoản trên backend mới
    print(f"\n📝 Tạo tài khoản '{email}' trên backend mới...")
    token = None
    # Thử register trước
    r = requests.post(
        f"{BACKEND_URL}/api/auth/register",
        json={"email": email, "password": pw1},
        timeout=30,
    )
    if r.status_code == 201:
        token = r.json()["token"]
        print("✅ Đã tạo tài khoản mới")
    elif r.status_code == 400 and "already registered" in r.text.lower():
        print("   ℹ️  Tài khoản đã tồn tại, đang đăng nhập...")
        r2 = requests.post(
            f"{BACKEND_URL}/api/auth/login",
            json={"email": email, "password": pw1},
            timeout=30,
        )
        if r2.ok:
            token = r2.json()["token"]
            print("✅ Đăng nhập thành công")
        else:
            print(f"❌ Đăng nhập thất bại: {r2.text}")
            sys.exit(1)
    else:
        print(f"❌ Tạo tài khoản thất bại: {r.text}")
        sys.exit(1)

    client = BackendClient(BACKEND_URL, token)

    # 6. Đọc dữ liệu từ Supabase
    print("\n📥 Đọc dữ liệu từ Supabase...")

    # Trades
    print("   • Đọc trades...")
    trades = supa_get("trades", {"user_id": f"eq.{supa_user_id}",
                                  "order": "trade_date.asc,created_at.asc"})
    print(f"     → {len(trades)} giao dịch")

    # Broker accounts
    print("   • Đọc broker_accounts...")
    accounts = supa_get("broker_accounts", {"user_id": f"eq.{supa_user_id}"})
    print(f"     → {len(accounts)} tài khoản")

    # Watchlists + items — dùng nested select như frontend
    print("   • Đọc watchlists + items...")
    url = f"{SUPABASE_URL}/rest/v1/watchlists"
    r = requests.get(url, headers=SUPA_HEADERS, params={
        "select":   "*,watchlist_items(*)",
        "user_id":  f"eq.{supa_user_id}",
        "order":    "sort_order.asc,created_at.asc",
        "limit":    "1000",
    }, timeout=30)
    r.raise_for_status()
    watchlists_raw = r.json()

    # Tách watchlists và items
    watchlists = []
    items = []
    for wl in watchlists_raw:
        wl_items = wl.pop("watchlist_items", []) or []
        watchlists.append(wl)
        for it in wl_items:
            it["watchlist_id"] = wl["id"]   # đảm bảo luôn có watchlist_id
            items.append(it)

    print(f"     → {len(watchlists)} watchlist, {len(items)} items")

    # User settings
    print("   • Đọc user_settings...")
    settings_rows = supa_get("user_settings", {"user_id": f"eq.{supa_user_id}"})
    settings_data = settings_rows[0]["settings"] if settings_rows else {}
    print(f"     → {'Có' if settings_data else 'Không có'} dữ liệu cài đặt")

    # 7. Import broker accounts
    acct_id_map: dict[str, str] = {}  # old Supabase ID → new backend ID
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
                print(f"   ⚠️  Bỏ qua tài khoản {acc.get('account_name', '?')}: {e}")

    # 8. Import trades (batch theo 100)
    if trades:
        print(f"\n📊 Import {len(trades)} giao dịch...")

        # Xóa trades cũ nếu có (tài khoản đã tồn tại trước đó)
        try:
            client.delete("/api/portfolio/trades")
            print("   🗑️  Đã xóa giao dịch cũ (nếu có)")
        except Exception:
            pass

        def map_trade(t: dict) -> dict:
            old_acc_id = t.get("broker_account_id")
            new_acc_id = acct_id_map.get(old_acc_id) if old_acc_id else None
            price = float(t.get("price", 0))
            # Workaround: price=0 (cổ phần thưởng) → lưu 1 để backend chấp nhận
            # Backend mới cho phép price=0, nhưng giữ nguyên nếu đã là 1 từ workaround cũ
            return {
                "ticker":            t.get("ticker", "").upper(),
                "exchange":          t.get("exchange", "HOSE"),
                "side":              t.get("side", "BUY").upper(),
                "quantity":          float(t.get("quantity", 0)),
                "price":             max(price, 0),
                "fee":               float(t.get("fee", 0)),
                "trade_date":        t.get("trade_date", "")[:10],
                "notes":             t.get("notes", "") or "",
                "source":            t.get("source", "MANUAL"),
                "broker_account_id": new_acc_id,
            }

        BATCH = 100
        ok_count = 0
        for i in range(0, len(trades), BATCH):
            batch = [map_trade(t) for t in trades[i:i + BATCH]]
            try:
                client.post("/api/portfolio/trades/batch", batch)
                ok_count += len(batch)
                print(f"   ✅ {ok_count}/{len(trades)} giao dịch")
            except Exception as e:
                print(f"   ❌ Lỗi batch {i}-{i+BATCH}: {e}")
                # Thử từng cái một để xác định cái lỗi
                for t_data in batch:
                    try:
                        client.post("/api/portfolio/trades", t_data, expected_status=201)
                        ok_count += 1
                    except Exception as e2:
                        print(f"      ⚠️  Bỏ qua {t_data['ticker']} {t_data['trade_date']}: {e2}")
            time.sleep(0.1)  # rate limit nhẹ

        print(f"   → Tổng: {ok_count}/{len(trades)} giao dịch import thành công")

    # 9. Import watchlists + items
    if watchlists:
        print(f"\n📋 Import {len(watchlists)} watchlist...")
        wl_id_map: dict[str, str] = {}  # old ID → new ID

        for wl in watchlists:
            try:
                new_wl = client.post("/api/watchlists/", {
                    "name": wl.get("name", "Watchlist"),
                })
                wl_id_map[wl["id"]] = new_wl["id"]
                wl_items = [it for it in items if it["watchlist_id"] == wl["id"]]
                print(f"   ✅ '{wl['name']}' ({len(wl_items)} mã)")

                # Import items
                for item in wl_items:
                    try:
                        client.post(f"/api/watchlists/{new_wl['id']}/items", {
                            "ticker":      item.get("ticker", "").upper(),
                            "note":        item.get("note", "") or "",
                            "alert_price": item.get("alert_price"),
                        })
                    except Exception as e:
                        # 409 = ticker đã có, bỏ qua
                        if "409" not in str(e):
                            print(f"      ⚠️  Bỏ qua {item.get('ticker', '?')}: {e}")

            except Exception as e:
                print(f"   ⚠️  Bỏ qua watchlist '{wl.get('name', '?')}': {e}")

    # 10. Import user settings
    if settings_data:
        print("\n⚙️  Import cài đặt người dùng...")
        try:
            client.put("/api/user/settings", settings_data if isinstance(settings_data, dict) else {})
            print("   ✅ Đã import settings")
        except Exception as e:
            print(f"   ⚠️  Bỏ qua settings: {e}")

    # 11. Hoàn tất
    print("\n" + "=" * 60)
    print("  ✅ MIGRATION HOÀN TẤT!")
    print("=" * 60)
    print(f"\n📌 Thông tin đăng nhập vào App:")
    print(f"   Email:    {email}")
    print(f"   Mật khẩu: (mật khẩu bạn vừa đặt)")
    print(f"   Backend:  {BACKEND_URL}")
    print(f"\n💡 Tip: Đặt JWT_SECRET_KEY trên VPS để bảo mật hơn")
    print(f"   Xem hướng dẫn trong SETUP.md\n")


if __name__ == "__main__":
    migrate()
