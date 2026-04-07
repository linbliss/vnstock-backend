from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def screener_status():
    return {
        "status": "coming_soon",
        "phase": 3,
        "description": "SEPA Screener – Giai đoạn 3"
    }
