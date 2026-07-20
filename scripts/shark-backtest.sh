#!/usr/bin/env bash
# Đo điểm Shark có DỰ BÁO được giá không.
#
# Mẫu = mỗi (mã, ngày) đã lưu điểm Shark; lợi suất T+h lấy từ OHLCV đã lưu.
# So nhóm "Gom hàng" với MỨC NỀN (trung bình toàn mẫu) — trừ nền là bắt buộc, vì
# lợi suất bị chi phối bởi xu hướng chung của thị trường.
#
#   bash scripts/shark-backtest.sh            # T+1, T+3, T+5
#   bash scripts/shark-backtest.sh 1,3,5,10   # tuỳ chọn kỳ hạn
#   HORIZONS=1,5 MIN_DATE=2026-06-01 bash scripts/shark-backtest.sh
set -e
BASE="${BASE:-http://localhost:8000}"
HORIZONS="${1:-${HORIZONS:-1,3,5}}"

cd "$(dirname "$0")/.."
TOKEN="$(docker compose exec -T backend printenv ADMIN_TOKEN | tr -d '\r\n')"
if [ -z "$TOKEN" ]; then
  echo "❌ Không đọc được ADMIN_TOKEN từ container."; exit 1
fi

URL="$BASE/api/admin/shark/backtest?horizons=$HORIZONS"
[ -n "$MIN_DATE" ] && URL="$URL&min_date=$MIN_DATE"

curl -s -H "X-Admin-Token: $TOKEN" "$URL" | python3 -c '
import sys, json
d = json.load(sys.stdin)
if not d.get("ok"):
    print("⚠️ ", d.get("message", d)); sys.exit(0)

print(f"Mẫu: {d[\"n_matched\"]} tín hiệu / {d[\"n_tickers\"]} mã  ({d[\"date_from\"]} → {d[\"date_to\"]})")
print(d["note"]); print()
for h, blk in d["horizons"].items():
    print(f"── {h}  (nền = {blk[\"baseline_mean_pct\"]:+.2f}%) " + "─"*30)
    print(f'   {"nhóm":<12}{"n":>6}{"TB %":>9}{"thắng %":>9}{"EDGE %":>9}{"t":>7}  kết luận')
    for name in ("Gom hàng", "Trung tính", "Xả hàng"):
        b = blk["buckets"].get(name, {})
        if not b.get("n"):
            print(f'   {name:<12}{0:>6}   (không có mẫu)'); continue
        verdict = "CÓ tín hiệu" if b["significant"] else "chưa phân biệt được với nhiễu"
        print(f'   {name:<12}{b["n"]:>6}{b["mean_pct"]:>9.2f}{b["win_rate_pct"]:>9.1f}'
              f'{b["edge_vs_base_pct"]:>9.2f}{b["t_stat"]:>7.1f}  {verdict}')
    print()
print("Đọc kết quả:", d["how_to_read"])
'
