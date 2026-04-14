#!/bin/bash
# =============================================================================
# deploy.sh — Script deploy thủ công HOẶC được gọi bởi GitHub Actions
# =============================================================================
# Cách dùng thủ công trên VPS:
#   bash /opt/vnstock-backend/scripts/deploy.sh
#
# GitHub Actions gọi tự động khi push lên branch main
# =============================================================================

set -euo pipefail

# ── Màu sắc ─────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}[$(date '+%H:%M:%S')] INFO${NC}  $*"; }
success() { echo -e "${GREEN}[$(date '+%H:%M:%S')] OK${NC}    $*"; }
warn()    { echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARN${NC}  $*"; }
error()   { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR${NC} $*"; exit 1; }

APP_DIR="${APP_DIR:-/opt/vnstock-backend}"

info "=== Bắt đầu deploy VNStock Backend ==="
info "Directory: $APP_DIR"
info "Thời gian: $(date)"

# ── Kiểm tra thư mục ────────────────────────────────────────────────────────
[ -d "$APP_DIR" ] || error "Thư mục $APP_DIR không tồn tại. Chạy setup-vps.sh trước."
[ -f "$APP_DIR/.env" ] || error "File .env chưa có. Tạo từ .env.example và điền giá trị."
cd "$APP_DIR"

# ── Bước 1: Pull code mới nhất ──────────────────────────────────────────────
info "Bước 1/5: Pull code từ GitHub..."
git fetch --all --prune

LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

if [ "$LOCAL" = "$REMOTE" ]; then
    warn "Code đã là mới nhất ($LOCAL) — vẫn tiếp tục deploy để cập nhật image"
else
    info "Có code mới: $LOCAL → $REMOTE"
    git reset --hard origin/main
    success "Pull code thành công"
fi

COMMIT_SHA=$(git rev-parse --short HEAD)
COMMIT_MSG=$(git log -1 --pretty=format:"%s")
info "Commit: [$COMMIT_SHA] $COMMIT_MSG"

# ── Bước 2: Kiểm tra requirements có thay đổi không (chỉ để log) ───────────
info "Bước 2/5: Kiểm tra dependencies..."
REQUIREMENTS_CHANGED=$(git diff HEAD@{1} HEAD -- requirements.txt 2>/dev/null | wc -l || echo "0")
if [ "$REQUIREMENTS_CHANGED" -gt 0 ]; then
    info "requirements.txt đã thay đổi — rebuild đầy đủ"
else
    info "requirements.txt không đổi — Docker dùng layer cache (pip install bỏ qua)"
fi

# ── Bước 3: Luôn rebuild image với code mới ─────────────────────────────────
# Phải có --build để copy source code mới vào image
# Docker tự cache layer pip install nếu requirements.txt không đổi → vẫn nhanh
info "Bước 3/5: Rebuild & restart container..."
docker compose down --remove-orphans 2>/dev/null || true
docker compose up -d --build
success "Container đã khởi động với code mới"

# ── Bước 4: Health check ────────────────────────────────────────────────────
info "Bước 4/5: Kiểm tra health check..."
MAX_RETRIES=15
RETRY=0
sleep 3   # Chờ container start

while [ $RETRY -lt $MAX_RETRIES ]; do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health 2>/dev/null || echo "000")
    if [ "$HTTP_CODE" = "200" ]; then
        success "Health check OK (HTTP 200) sau $((RETRY * 2))s"
        break
    fi
    RETRY=$((RETRY + 1))
    info "Đang chờ... ($RETRY/$MAX_RETRIES) — HTTP $HTTP_CODE"
    sleep 2
done

if [ $RETRY -eq $MAX_RETRIES ]; then
    error "Health check THẤT BẠI sau $((MAX_RETRIES * 2))s — xem logs: docker compose logs"
fi

# ── Bước 5: Dọn dẹp Docker images cũ ──────────────────────────────────────
info "Bước 5/5: Dọn dẹp..."
docker image prune -f --filter "until=24h" 2>/dev/null || true
success "Dọn xong images cũ"

# ── Tóm tắt ─────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  Deploy thành công!${NC}"
echo -e "${GREEN}============================================${NC}"
echo "  Commit : [$COMMIT_SHA] $COMMIT_MSG"
echo "  Thời gian: $(date)"
echo "  Status : $(docker compose ps --format 'table {{.Name}}\t{{.Status}}')"
echo ""
