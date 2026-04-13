#!/bin/bash
# =============================================================================
# generate-ssh-key.sh — Tạo SSH key pair cho GitHub Actions
# Chạy TRÊN MÁY CỦA BẠN (macOS/Linux), không phải trên VPS
# =============================================================================
# Cách dùng: bash scripts/generate-ssh-key.sh
# =============================================================================

set -euo pipefail

KEY_NAME="vnstock-deploy"
KEY_PATH="$HOME/.ssh/$KEY_NAME"

echo "=== Tạo SSH key cho GitHub Actions CI/CD ==="
echo ""

# ── Tạo key pair ────────────────────────────────────────────────────────────
if [ -f "$KEY_PATH" ]; then
    echo "⚠️  Key $KEY_PATH đã tồn tại. Dùng key này hay tạo mới?"
    read -r -p "Tạo mới? [y/N]: " REPLY
    if [[ ! "$REPLY" =~ ^[Yy]$ ]]; then
        echo "Dùng key hiện tại."
    else
        ssh-keygen -t ed25519 -f "$KEY_PATH" -N "" -C "github-actions-vnstock-deploy"
    fi
else
    ssh-keygen -t ed25519 -f "$KEY_PATH" -N "" -C "github-actions-vnstock-deploy"
fi

PUBLIC_KEY=$(cat "${KEY_PATH}.pub")
PRIVATE_KEY=$(cat "$KEY_PATH")

echo ""
echo "============================================================"
echo " BƯỚC 1: Thêm PUBLIC KEY lên VPS"
echo " Chạy lệnh này (thay <VPS_IP> bằng IP thật):"
echo "============================================================"
echo ""
echo "   ssh ubuntu@<VPS_IP> 'mkdir -p ~/.ssh && echo \"$PUBLIC_KEY\" >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys'"
echo ""

echo "============================================================"
echo " BƯỚC 2: Thêm PRIVATE KEY vào GitHub Secrets"
echo " Vào: GitHub Repo → Settings → Secrets and variables → Actions"
echo " Tạo các secrets sau:"
echo "============================================================"
echo ""
echo " Secret name     : VPS_HOST"
echo " Secret value    : <IP hoặc domain của VPS>"
echo ""
echo " Secret name     : VPS_USER"
echo " Secret value    : ubuntu   (hoặc vnstock nếu dùng user riêng)"
echo ""
echo " Secret name     : VPS_PORT"
echo " Secret value    : 22"
echo ""
echo " Secret name     : VPS_SSH_KEY"
echo " Secret value    : (copy toàn bộ nội dung bên dưới)"
echo ""
echo "──────── PRIVATE KEY (copy toàn bộ, kể cả dòng BEGIN/END) ────────"
echo "$PRIVATE_KEY"
echo "───────────────────────────────────────────────────────────────────"
echo ""
echo "✅ Sau khi thêm secrets, mọi push lên branch 'main' sẽ tự động deploy!"
