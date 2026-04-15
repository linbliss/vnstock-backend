# ── Stage 1: Builder ─────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# Cài build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Cài Python dependencies vào /install để copy sang stage sau
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --prefix=/install --no-cache-dir -r requirements.txt

# ── Stage 2: Runtime ─────────────────────────────────────────
FROM python:3.11-slim AS runtime

WORKDIR /app

# Copy installed packages từ builder
COPY --from=builder /install /usr/local

# Copy source code
COPY . .

# Tạo user non-root để chạy app (bảo mật)
# Tạo sẵn /app/data + /app/logs để named volume kế thừa ownership appuser
# (nếu không, volume mount mặc định root-owned → sqlite không ghi được)
RUN useradd -m -u 1000 appuser \
    && mkdir -p /app/data /app/logs \
    && chown -R appuser:appuser /app
USER appuser

# Port mà uvicorn lắng nghe
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

# Chạy app
# workers=1: alert engine chạy asyncio trong 1 process duy nhất
# Multi-worker sẽ gây duplicate Telegram alerts
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
