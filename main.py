import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from app.services.market_data import market_service
from app.services.alert_engine import run_alert_engine
from app.routers import quotes, alerts

alert_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global alert_task
    await market_service.start()
    alert_task = asyncio.create_task(run_alert_engine())
    yield
    if alert_task:
        alert_task.cancel()
    await market_service.stop()

app = FastAPI(title="VN Stock API", version="0.2.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(quotes.router, prefix="/api/quotes", tags=["quotes"])
app.include_router(alerts.router, prefix="/api/alerts", tags=["alerts"])

@app.get("/")
async def root():
    return {
        "status": "running",
        "quotes_cached": len(market_service.quotes),
        "active_alerts": len(__import__('app.services.alert_engine', fromlist=['active_alerts']).active_alerts)
    }

@app.get("/health")
async def health():
    return {"status": "ok"}
