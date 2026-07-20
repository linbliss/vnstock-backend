#!/usr/bin/env bash
# Đo điểm Shark có DỰ BÁO được giá không.
#
# Mẫu = mỗi (mã, ngày) đã lưu điểm Shark; lợi suất T+h lấy từ OHLCV đã lưu.
# So nhóm "Gom hàng" với MỨC NỀN (trung bình toàn mẫu) — trừ nền là bắt buộc, vì
# lợi suất bị chi phối bởi xu hướng chung của thị trường.
#
#   bash scripts/shark-backtest.sh            # T+1, T+3, T+5
#   bash scripts/shark-backtest.sh 1,3,5,10   # tuỳ chọn kỳ hạn
#   MIN_DATE=2026-06-01 bash scripts/shark-backtest.sh
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

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT
curl -s -H "X-Admin-Token: $TOKEN" "$URL" >"$TMP"

python3 - "$TMP" <<'PY'
import sys, json
d = json.load(open(sys.argv[1]))
if not d.get("ok"):
    print("⚠️ ", d.get("message", d))
    sys.exit(0)

print("Mẫu: {} tín hiệu / {} mã  ({} → {})".format(
    d["n_matched"], d["n_tickers"], d["date_from"], d["date_to"]))
print(d["note"])
print()
for h, blk in d["horizons"].items():
    print("── {}  (nền = {:+.2f}%) ".format(h, blk["baseline_mean_pct"]) + "─" * 28)
    print("   {:<12}{:>6}{:>9}{:>9}{:>9}{:>7}  kết luận".format(
        "nhóm", "n", "TB %", "thắng %", "EDGE %", "t"))
    for name in ("Gom hàng", "Trung tính", "Xả hàng"):
        b = blk["buckets"].get(name) or {}
        if not b.get("n"):
            print("   {:<12}{:>6}   (không có mẫu)".format(name, 0))
            continue
        verdict = "CÓ tín hiệu" if b["significant"] else "chưa phân biệt được với nhiễu"
        print("   {:<12}{:>6}{:>9.2f}{:>9.1f}{:>9.2f}{:>7.1f}  {}".format(
            name, b["n"], b["mean_pct"], b["win_rate_pct"],
            b["edge_vs_base_pct"], b["t_stat"], verdict))
    print()
print("Đọc kết quả:", d["how_to_read"])
PY
