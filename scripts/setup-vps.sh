#!/bin/bash
# =============================================================================
# setup-vps.sh — Chạy MỘT LẦN DUY NHẤT trên VPS mới để cài đặt môi trường
# =============================================================================
# Cách dùng:
#   curl -sSL https://raw.githubusercontent.com/linbliss/vnstock-backend/main/scripts/setup-vps.sh | bash
# Hoặc:
#   bash scripts/setup-vps.sh
# =============================================================================

set -euo pipefail

# ── Màu sắc output ──────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Config ──────────────────────────────────────────────────────────────────
APP_DIR="/opt/vnstock-backend"
APP_USER="vnstock"
REPO_URL="https://github.com/linbliss/vnstock-backend.git"
DOMAIN=""   # Để trống nếu chỉ dùng IP, điền domain nếu muốn SSL

info "=== VNStock Backend — VPS Setup Script ==="
info "App directory : $APP_DIR"
info "App user      : $APP_USER"
info "Repo          : $REPO_URL"
echo ""

# ── Kiểm tra quyền root ─────────────────────────────────────────────────────
[[ $EUID -ne 0 ]] && error "Cần chạy với quyền root: sudo bash setup-vps.sh"

# ── Bước 1: Update hệ thống ─────────────────────────────────────────────────
info "Bước 1/9: Update hệ thống..."
apt-get update -qq && apt-get upgrade -y -qq
success "Hệ thống đã được update"

# ── Bước 2: Cài packages cần thiết ─────────────────────────────────────────
info "Bước 2/9: Cài packages..."
apt-get install -y -qq \
    git curl wget \
    python3.11 python3.11-venv python3-pip \
    nginx \
    certbot python3-certbot-nginx \
    ufw \
    fail2ban \
    htop \
    jq
success "Packages đã cài xong"

# ── Bước 3: Cài Docker + Docker Compose ────────────────────────────────────
info "Bước 3/9: Cài Docker..."
if ! command -v docker &>/dev/null; then
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
    success "Docker đã cài"
else
    success "Docker đã có sẵn ($(docker --version))"
fi

# Cài docker-compose plugin
if ! docker compose version &>/dev/null 2>&1; then
    apt-get install -y docker-compose-plugin
fi
success "Docker Compose: $(docker compose version)"

# ── Bước 4: Tạo user ứng dụng ───────────────────────────────────────────────
info "Bước 4/9: Tạo user '$APP_USER'..."
if ! id "$APP_USER" &>/dev/null; then
    useradd -m -s /bin/bash "$APP_USER"
    usermod -aG docker "$APP_USER"
    success "User '$APP_USER' đã tạo"
else
    usermod -aG docker "$APP_USER"
    success "User '$APP_USER' đã tồn tại"
fi

# ── Bước 5: Clone repo ──────────────────────────────────────────────────────
info "Bước 5/9: Clone repository..."
if [ -d "$APP_DIR" ]; then
    warn "Thư mục $APP_DIR đã tồn tại — pull latest thay vì clone"
    cd "$APP_DIR" && sudo -u "$APP_USER" git pull
else
    git clone "$REPO_URL" "$APP_DIR"
    chown -R "$APP_USER:$APP_USER" "$APP_DIR"
    success "Repo đã clone vào $APP_DIR"
fi

# ── Bước 6: Tạo file .env ───────────────────────────────────────────────────
info "Bước 6/9: Cấu hình environment..."
if [ ! -f "$APP_DIR/.env" ]; then
    cp "$APP_DIR/.env.example" "$APP_DIR/.env"
    chown "$APP_USER:$APP_USER" "$APP_DIR/.env"
    chmod 600 "$APP_DIR/.env"
    echo ""
    warn "======================================================"
    warn "  File .env đã được tạo tại: $APP_DIR/.env"
    warn "  BẮT BUỘC phải điền các giá trị sau trước khi deploy:"
    warn "    - FIREANT_TOKEN"
    warn "    - SUPABASE_URL"
    warn "    - SUPABASE_SERVICE_KEY"
    warn "    - TELEGRAM_BOT_TOKEN"
    warn "    - TELEGRAM_CHAT_ID"
    warn "  Lệnh: nano $APP_DIR/.env"
    warn "======================================================"
    echo ""
else
    success ".env đã tồn tại — giữ nguyên"
fi

# ── Bước 7: Cài Nginx config ────────────────────────────────────────────────
info "Bước 7/9: Cài Nginx..."
cat > /etc/nginx/sites-available/vnstock << 'NGINX_EOF'
# VNStock Backend — Nginx reverse proxy
# Tự động sinh bởi setup-vps.sh

upstream vnstock_backend {
    server 127.0.0.1:8000;
    keepalive 32;
}

server {
    listen 80;
    listen [::]:80;
    server_name _;   # Sẽ được thay bằng domain khi cài SSL

    # Security headers
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;

    # Gzip
    gzip on;
    gzip_types application/json text/plain;

    # Health check — không log để tránh spam
    location = /health {
        proxy_pass http://vnstock_backend;
        access_log off;
    }

    # WebSocket — cần header đặc biệt
    location /api/quotes/ws {
        proxy_pass http://vnstock_backend;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 86400s;   # 24h — WebSocket cần persistent
        proxy_send_timeout 86400s;
    }

    # API endpoints thường
    location / {
        proxy_pass http://vnstock_backend;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Timeout
        proxy_connect_timeout 10s;
        proxy_read_timeout 120s;    # Screener cần thời gian dài hơn
        proxy_send_timeout 30s;
    }
}
NGINX_EOF

ln -sf /etc/nginx/sites-available/vnstock /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default   # Bỏ trang default của Nginx

nginx -t && systemctl reload nginx
success "Nginx đã cấu hình"

# ── Bước 8: Cài systemd service để Nginx tự restart ────────────────────────
info "Bước 8/9: Cài systemd service cho app..."
cat > /etc/systemd/system/vnstock-backend.service << SERVICE_EOF
[Unit]
Description=VNStock Backend (Docker Compose)
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
User=$APP_USER
WorkingDirectory=$APP_DIR
ExecStartPre=docker compose pull --quiet
ExecStart=docker compose up -d --build
ExecStop=docker compose down
ExecReload=docker compose up -d --build --force-recreate
TimeoutStartSec=120

[Install]
WantedBy=multi-user.target
SERVICE_EOF

systemctl daemon-reload
systemctl enable vnstock-backend
success "Systemd service đã cài"

# ── Bước 9: Cấu hình Firewall ───────────────────────────────────────────────
info "Bước 9/9: Cấu hình UFW firewall..."
ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow ssh          # Port 22
ufw allow 80/tcp       # HTTP
ufw allow 443/tcp      # HTTPS
ufw --force enable
success "Firewall đã bật (cho phép: SSH, HTTP, HTTPS)"

# ── Hoàn tất ────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  Setup hoàn tất!${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo "Bước tiếp theo:"
echo "  1. Điền .env:   nano $APP_DIR/.env"
echo "  2. Deploy app:  bash $APP_DIR/scripts/deploy.sh"
echo ""
echo "Nếu có domain, thêm SSL:"
echo "  certbot --nginx -d your-domain.com"
echo ""
