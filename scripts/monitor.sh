#!/bin/bash
# =============================================================================
# monitor.sh — Xem trạng thái server nhanh
# Cách dùng: bash /opt/vnstock-backend/scripts/monitor.sh
# =============================================================================

APP_DIR="${APP_DIR:-/opt/vnstock-backend}"
cd "$APP_DIR"

echo "╔═══════════════════════════════════════════════════╗"
echo "║         VNStock Backend — Monitor Dashboard       ║"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# Health check
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ]; then
    echo "🟢 App Status    : RUNNING (HTTP 200)"
else
    echo "🔴 App Status    : DOWN (HTTP $HTTP_CODE)"
fi

# Container info
echo ""
echo "── Docker Containers ──────────────────────────────"
docker compose ps 2>/dev/null || echo "(docker compose không khả dụng)"

# Resource usage
echo ""
echo "── Tài nguyên hệ thống ────────────────────────────"
echo "RAM: $(free -h | awk '/Mem:/ {printf "Dùng %s / %s (còn %s)", $3, $2, $7}')"
echo "CPU: $(top -bn1 | grep 'Cpu(s)' | awk '{print $2}')% đang dùng"
echo "Disk: $(df -h /opt 2>/dev/null | awk 'NR==2 {printf "Dùng %s / %s (%s)", $3, $2, $5}')"

# Nginx status
echo ""
echo "── Nginx ──────────────────────────────────────────"
systemctl is-active nginx && echo "🟢 Nginx: Running" || echo "🔴 Nginx: Stopped"

# Commit info
echo ""
echo "── Phiên bản đang chạy ────────────────────────────"
echo "Commit: [$(git -C $APP_DIR rev-parse --short HEAD)] $(git -C $APP_DIR log -1 --pretty=format:'%s')"
echo "Deploy: $(git -C $APP_DIR log -1 --pretty=format:'%ai')"

# Recent logs
echo ""
echo "── 10 dòng log gần nhất ───────────────────────────"
docker compose logs --tail=10 2>/dev/null || echo "(không có log)"
echo ""
