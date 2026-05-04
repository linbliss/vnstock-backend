"""User settings router (JWT protected).

GET /   → settings dict
PUT / body: arbitrary settings dict → {ok: true}
"""
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import Any, Dict

from app.routers.auth import get_current_user
from app.services import user_store

router = APIRouter()


class SettingsBody(BaseModel):
    # Chấp nhận bất kỳ dict JSON nào
    model_config = {"extra": "allow"}

    def to_dict(self) -> dict:
        return self.model_dump()


@router.get("/")
def get_settings(current_user: dict = Depends(get_current_user)):
    return user_store.get_user_settings(current_user["id"])


@router.put("/")
def save_settings(
    body: Dict[str, Any],
    current_user: dict = Depends(get_current_user),
):
    user_store.save_user_settings(current_user["id"], body)
    return {"ok": True}
