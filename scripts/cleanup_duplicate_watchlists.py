#!/usr/bin/env python3
"""
cleanup_duplicate_watchlists.py
─────────────────────────────────
Xóa watchlist trùng tên, chỉ giữ lại 1 bản có nhiều items nhất.

Chạy:
    python scripts/cleanup_duplicate_watchlists.py
"""
import sys
import getpass
from collections import defaultdict

try:
    import requests
except ImportError:
    print("❌ Thiếu requests. Chạy: pip install requests")
    sys.exit(1)

BACKEND_URL = "http://67.215.255.242"


def main():
    print("=" * 55)
    print("  Cleanup duplicate watchlists")
    print("=" * 55)

    email    = input("\nEmail: ").strip()
    password = getpass.getpass("Mật khẩu: ")

    # Login
    r = requests.post(f"{BACKEND_URL}/api/auth/login",
                      json={"email": email, "password": password}, timeout=15)
    if not r.ok:
        print(f"❌ Đăng nhập thất bại: {r.text}")
        sys.exit(1)
    token = r.json()["token"]
    hdrs  = {"Authorization": f"Bearer {token}"}
    print("✅ Đăng nhập thành công\n")

    # Lấy toàn bộ watchlists
    resp = requests.get(f"{BACKEND_URL}/api/watchlists/", headers=hdrs, timeout=15)
    if not resp.ok:
        print(f"❌ Không lấy được watchlists: {resp.text}")
        sys.exit(1)
    wls = resp.json()

    print(f"📋 Tổng số watchlists: {len(wls)}")
    for w in wls:
        n = len(w.get("items") or [])
        print(f"   '{w['name']}'  id={w['id'][:8]}...  items={n}")

    # Nhóm theo tên
    by_name = defaultdict(list)
    for w in wls:
        by_name[w["name"]].append(w)

    found_dup = False
    for name, group in by_name.items():
        if len(group) <= 1:
            continue
        found_dup = True

        # Sắp xếp: nhiều items nhất trước; bằng nhau → cũ nhất trước
        group.sort(key=lambda w: (-len(w.get("items") or []), w.get("created_at", "")))
        keep   = group[0]
        remove = group[1:]

        n_keep = len(keep.get("items") or [])
        print(f"\n🔁 '{name}': {len(group)} bản trùng")
        print(f"   ✅ Giữ lại: id={keep['id'][:8]}... ({n_keep} mã)")

        for w in remove:
            n_del = len(w.get("items") or [])
            r2 = requests.delete(f"{BACKEND_URL}/api/watchlists/{w['id']}",
                                 headers=hdrs, timeout=15)
            status = "✅" if r2.ok else f"❌ {r2.text}"
            print(f"   🗑️  {status} id={w['id'][:8]}... ({n_del} mã)")

    if not found_dup:
        print("\n✅ Không có watchlist trùng tên.")
    else:
        print("\n✅ Hoàn tất cleanup!")

    # Hiển thị trạng thái sau cleanup
    wls2 = requests.get(f"{BACKEND_URL}/api/watchlists/", headers=hdrs, timeout=15).json()
    print(f"\n📋 Danh sách sau cleanup ({len(wls2)} watchlists):")
    for w in wls2:
        n = len(w.get("items") or [])
        print(f"   '{w['name']}'  ({n} mã)")


if __name__ == "__main__":
    main()
