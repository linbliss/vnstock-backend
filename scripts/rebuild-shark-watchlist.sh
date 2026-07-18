#!/usr/bin/env bash
# Dựng lại TRỌN tape phiên cho tất cả mã trong Watchlist (sửa tape thiếu phần sáng).
# Nguồn: DNSE nếu REST dùng được, không thì vnstock (chỉ có phiên HÔM NAY).
#
# Chạy TRÊN SERVER (nơi có container backend). Nên chạy TRONG hoặc NGAY SAU phiên
# giao dịch — vì DNSE đang bị chặn IP nên dùng vnstock, mà vnstock intraday chỉ có
# dữ liệu phiên hôm nay.
#
#   bash scripts/rebuild-shark-watchlist.sh
#
# Tuỳ chọn: BASE=http://localhost:8000 (mặc định)

set -e
BASE="${BASE:-http://localhost:8000}"

cd "$(dirname "$0")/.."

# Lấy admin token từ container (không cần biết giá trị)
TOKEN="$(docker compose exec -T backend printenv ADMIN_TOKEN | tr -d '\r\n')"
if [ -z "$TOKEN" ]; then
  echo "❌ Không đọc được ADMIN_TOKEN từ container. Kiểm tra .env / container đang chạy?"
  exit 1
fi

echo "▶ Bắt đầu dựng lại tape watchlist…"
curl -s -X POST -H "X-Admin-Token: $TOKEN" \
  "$BASE/api/admin/shark/rebuild-watchlist" | python3 -m json.tool

echo "▶ Theo dõi tiến độ (Ctrl+C để dừng theo dõi — job vẫn chạy nền):"
while true; do
  sleep 8
  OUT="$(curl -s -H "X-Admin-Token: $TOKEN" "$BASE/api/admin/shark/rebuild-watchlist/status")"
  RUNNING="$(echo "$OUT" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("running"))' 2>/dev/null || echo "?")"
  echo "  … running=$RUNNING"
  if [ "$RUNNING" = "False" ]; then
    echo "✅ Xong. Kết quả:"
    echo "$OUT" | python3 -m json.tool
    break
  fi
done
