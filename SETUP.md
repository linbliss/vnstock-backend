# 🚀 VN Stock Backend – Hướng dẫn cài đặt

## Chạy local (development)

### Bước 1 – Tạo môi trường Python
```bash
cd ~/Desktop/vnstock-backend
python3 -m venv venv
source venv/bin/activate
```

### Bước 2 – Cài dependencies
```bash
pip install -r requirements.txt
```

### Bước 3 – Tạo file .env
```bash
cp .env.example .env
```

Mở file `.env` và điền:
```
FIREANT_TOKEN=eyJ0eXAiOiJKV1QiLCJhbGci...  ← token FireAnt của bạn
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_SERVICE_KEY=eyJ...  ← Service Role key (khác với anon key)
```

### Bước 4 – Chạy backend
```bash
uvicorn main:app --reload --port 8000
```

Mở trình duyệt vào http://localhost:8000 để kiểm tra.
API docs: http://localhost:8000/docs

---

## Deploy lên Railway (production)

### Bước 1 – Cài Railway CLI
```bash
brew install railway
```

### Bước 2 – Đăng nhập Railway
```bash
railway login
```

### Bước 3 – Tạo project và deploy
```bash
railway init
railway up
```

### Bước 4 – Thêm biến môi trường trên Railway
Vào dashboard.railway.app → project → Variables → thêm tất cả biến từ .env

### Bước 5 – Lấy URL
Railway sẽ cấp URL dạng: https://vnstock-backend-xxx.railway.app

---

## Kiểm tra API

Test lấy giá:
```bash
curl "http://localhost:8000/api/quotes?symbols=VIC,HPG,TCB"
```

Test WebSocket (dùng wscat):
```bash
npm install -g wscat
wscat -c ws://localhost:8000/api/quotes/ws
# Gửi: {"action":"subscribe","tickers":["VIC","HPG"]}
```
