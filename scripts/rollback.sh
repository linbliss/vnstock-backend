#!/bin/bash
# =============================================================================
# rollback.sh — Rollback về commit trước khi có vấn đề
# =============================================================================
# Cách dùng:
#   bash /opt/vnstock-backend/scripts/rollback.sh           # Quay về commit trước
#   bash /opt/vnstock-backend/scripts/rollback.sh abc1234   # Quay về commit cụ thể
# =============================================================================

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

APP_DIR="${APP_DIR:-/opt/vnstock-backend}"
TARGET_COMMIT="${1:-HEAD~1}"   # Default: commit trước đó

cd "$APP_DIR" || error "Thư mục $APP_DIR không tồn tại"

CURRENT=$(git rev-parse --short HEAD)
info "Commit hiện tại: [$CURRENT] $(git log -1 --pretty=format:'%s')"

# Xác nhận target commit
TARGET_SHA=$(git rev-parse --short "$TARGET_COMMIT" 2>/dev/null || error "Commit '$TARGET_COMMIT' không tồn tại")
TARGET_MSG=$(git log -1 --pretty=format:"%s" "$TARGET_COMMIT")
warn "Sẽ rollback về: [$TARGET_SHA] $TARGET_MSG"

read -r -p "Xác nhận rollback? [y/N]: " REPLY
[[ "$REPLY" =~ ^[Yy]$ ]] || { info "Đã hủy rollback."; exit 0; }

# Rollback
info "Đang rollback..."
git reset --hard "$TARGET_COMMIT"
success "Code đã rollback về [$TARGET_SHA]"

# Restart
info "Đang restart container..."
docker compose down
docker compose up -d --build

# Health check
sleep 5
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ]; then
    success "Rollback thành công! App đang chạy bình thường."
else
    error "Rollback xong nhưng health check thất bại (HTTP $HTTP_CODE). Kiểm tra: docker compose logs"
fi
