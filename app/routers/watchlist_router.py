"""Watchlist router (JWT protected).

GET    /                           → Watchlist[] with items
POST   /         {name}            → new watchlist
PUT    /{wl_id}  {name}            → rename watchlist
DELETE /{wl_id}                    → ok
POST   /{wl_id}/items  {ticker}    → new item
PUT    /items/{item_id} {note, alert_price} → updated item
DELETE /items/{item_id}            → ok
"""
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.routers.auth import get_current_user
from app.services import user_store

router = APIRouter()


# ── Schemas ───────────────────────────────────────────────────────────────────

class WatchlistCreate(BaseModel):
    name: str


class WatchlistPatch(BaseModel):
    name: str


class WatchlistItemCreate(BaseModel):
    ticker: str
    note: Optional[str] = ""
    alert_price: Optional[float] = None


class WatchlistItemPatch(BaseModel):
    note: Optional[str] = None
    alert_price: Optional[float] = None


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/")
def list_watchlists(current_user: dict = Depends(get_current_user)):
    return user_store.get_watchlists(current_user["id"])


@router.post("/", status_code=201)
def create_watchlist(body: WatchlistCreate, current_user: dict = Depends(get_current_user)):
    if not body.name.strip():
        raise HTTPException(status_code=400, detail="name is required")
    return user_store.add_watchlist(current_user["id"], body.name.strip())


@router.put("/{wl_id}")
def rename_watchlist(
    wl_id: str,
    body: WatchlistPatch,
    current_user: dict = Depends(get_current_user),
):
    if not body.name.strip():
        raise HTTPException(status_code=400, detail="name is required")
    ok = user_store.rename_watchlist(wl_id, current_user["id"], body.name.strip())
    if not ok:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    return {"ok": True, "name": body.name.strip()}


@router.delete("/{wl_id}")
def delete_watchlist(wl_id: str, current_user: dict = Depends(get_current_user)):
    ok = user_store.delete_watchlist(wl_id, current_user["id"])
    if not ok:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    return {"ok": True}


@router.post("/{wl_id}/items", status_code=201)
def add_item(
    wl_id: str,
    body: WatchlistItemCreate,
    current_user: dict = Depends(get_current_user),
):
    if not body.ticker.strip():
        raise HTTPException(status_code=400, detail="ticker is required")

    # Xác nhận watchlist thuộc về user
    wls = user_store.get_watchlists(current_user["id"])
    wl_ids = {wl["id"] for wl in wls}
    if wl_id not in wl_ids:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    try:
        return user_store.add_watchlist_item(
            wl_id,
            body.ticker.strip().upper(),
            body.note or "",
            body.alert_price,
        )
    except Exception as e:
        # UNIQUE constraint violation → ticker đã tồn tại trong watchlist
        if "UNIQUE" in str(e).upper():
            raise HTTPException(status_code=409, detail="Ticker already in watchlist")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/items/{item_id}")
def update_item(
    item_id: str,
    body: WatchlistItemPatch,
    current_user: dict = Depends(get_current_user),
):
    patch = {k: v for k, v in body.model_dump().items() if v is not None}
    updated = user_store.update_watchlist_item(item_id, patch)
    if not updated:
        raise HTTPException(status_code=404, detail="Item not found")
    return updated


@router.delete("/items/{item_id}")
def delete_item(item_id: str, current_user: dict = Depends(get_current_user)):
    ok = user_store.delete_watchlist_item(item_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"ok": True}
