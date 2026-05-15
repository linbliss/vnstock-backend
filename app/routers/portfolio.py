"""Portfolio router — trades & broker accounts (JWT protected).

GET    /trades
POST   /trades
POST   /trades/batch
PUT    /trades/{trade_id}
DELETE /trades
DELETE /trades/{trade_id}

GET    /accounts
POST   /accounts
PUT    /accounts/{acct_id}
DELETE /accounts/{acct_id}
"""
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.routers.auth import get_current_user
from app.services import user_store

router = APIRouter()


# ── Schemas ───────────────────────────────────────────────────────────────────

class TradeCreate(BaseModel):
    ticker: str
    exchange: Optional[str] = "HOSE"
    side: str          # BUY | SELL
    quantity: float
    price: float
    fee: Optional[float] = 0
    trade_date: str    # YYYY-MM-DD
    broker_account_id: Optional[str] = None
    notes: Optional[str] = ""
    source: Optional[str] = "MANUAL"


class TradePatch(BaseModel):
    ticker: Optional[str] = None
    exchange: Optional[str] = None
    side: Optional[str] = None
    quantity: Optional[float] = None
    price: Optional[float] = None
    fee: Optional[float] = None
    trade_date: Optional[str] = None
    broker_account_id: Optional[str] = None
    notes: Optional[str] = None
    source: Optional[str] = None


class AccCreate(BaseModel):
    account_name: str
    account_number: Optional[str] = ""
    broker: Optional[str] = ""
    is_active: Optional[bool] = True


class AccPatch(BaseModel):
    account_name: Optional[str] = None
    account_number: Optional[str] = None
    broker: Optional[str] = None
    is_active: Optional[bool] = None


class DividendCreate(BaseModel):
    ticker: str
    dividend_per_share: float    # VND/cổ phiếu
    quantity: float              # số CP nắm tại ngày ex-div
    ex_date: str                 # YYYY-MM-DD
    broker_account_id: Optional[str] = None
    notes: Optional[str] = ""


class DividendPatch(BaseModel):
    ticker: Optional[str] = None
    dividend_per_share: Optional[float] = None
    quantity: Optional[float] = None
    ex_date: Optional[str] = None
    broker_account_id: Optional[str] = None
    notes: Optional[str] = None


# ── Trades ────────────────────────────────────────────────────────────────────

@router.get("/trades")
def list_trades(current_user: dict = Depends(get_current_user)):
    return user_store.get_trades(current_user["id"])


@router.post("/trades", status_code=201)
def create_trade(body: TradeCreate, current_user: dict = Depends(get_current_user)):
    side = body.side.upper()
    if side not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="side must be BUY or SELL")
    if body.quantity <= 0:
        raise HTTPException(status_code=400, detail="quantity must be positive")
    if body.price < 0:
        raise HTTPException(status_code=400, detail="price cannot be negative")

    return user_store.add_trade(current_user["id"], body.model_dump())


@router.post("/trades/batch", status_code=201)
def batch_create_trades(body: List[TradeCreate], current_user: dict = Depends(get_current_user)):
    results = []
    for trade in body:
        data = trade.model_dump()
        data['side'] = data['side'].upper()
        results.append(user_store.add_trade(current_user["id"], data))
    return results


@router.put("/trades/{trade_id}")
def update_trade(
    trade_id: str,
    body: TradePatch,
    current_user: dict = Depends(get_current_user),
):
    patch = {k: v for k, v in body.model_dump().items() if v is not None}
    if "side" in patch and patch["side"].upper() not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="side must be BUY or SELL")

    updated = user_store.update_trade(trade_id, current_user["id"], patch)
    if not updated:
        raise HTTPException(status_code=404, detail="Trade not found")
    return updated


@router.delete("/trades")
def delete_all_trades(current_user: dict = Depends(get_current_user)):
    count = user_store.delete_all_trades(current_user["id"])
    return {"deleted": count}


@router.delete("/trades/{trade_id}")
def delete_trade(trade_id: str, current_user: dict = Depends(get_current_user)):
    ok = user_store.delete_trade(trade_id, current_user["id"])
    if not ok:
        raise HTTPException(status_code=404, detail="Trade not found")
    return {"ok": True}


# ── Accounts ──────────────────────────────────────────────────────────────────

@router.get("/accounts")
def list_accounts(current_user: dict = Depends(get_current_user)):
    return user_store.get_accounts(current_user["id"])


@router.post("/accounts", status_code=201)
def create_account(body: AccCreate, current_user: dict = Depends(get_current_user)):
    if not body.account_name.strip():
        raise HTTPException(status_code=400, detail="account_name is required")
    return user_store.add_account(current_user["id"], body.model_dump())


@router.put("/accounts/{acct_id}")
def update_account(
    acct_id: str,
    body: AccPatch,
    current_user: dict = Depends(get_current_user),
):
    patch = {k: v for k, v in body.model_dump().items() if v is not None}
    updated = user_store.update_account(acct_id, current_user["id"], patch)
    if not updated:
        raise HTTPException(status_code=404, detail="Account not found")
    return updated


@router.delete("/accounts/{acct_id}")
def delete_account(acct_id: str, current_user: dict = Depends(get_current_user)):
    ok = user_store.delete_account(acct_id, current_user["id"])
    if not ok:
        raise HTTPException(status_code=404, detail="Account not found")
    return {"ok": True}


# ── Dividends (cổ tức tiền mặt) ───────────────────────────────────────────────

@router.get("/dividends")
def list_dividends(current_user: dict = Depends(get_current_user)):
    return user_store.get_dividends(current_user["id"])


@router.post("/dividends", status_code=201)
def create_dividend(body: DividendCreate, current_user: dict = Depends(get_current_user)):
    if body.dividend_per_share <= 0:
        raise HTTPException(status_code=400, detail="dividend_per_share phải > 0")
    if body.quantity <= 0:
        raise HTTPException(status_code=400, detail="quantity phải > 0")
    return user_store.add_dividend(
        user_id=current_user["id"],
        ticker=body.ticker,
        dividend_per_share=body.dividend_per_share,
        quantity=body.quantity,
        ex_date=body.ex_date,
        broker_account_id=body.broker_account_id,
        notes=body.notes or "",
    )


@router.put("/dividends/{div_id}")
def update_div(div_id: str, body: DividendPatch, current_user: dict = Depends(get_current_user)):
    patch = body.model_dump(exclude_unset=True)
    if "dividend_per_share" in patch and patch["dividend_per_share"] is not None and patch["dividend_per_share"] <= 0:
        raise HTTPException(status_code=400, detail="dividend_per_share phải > 0")
    if "quantity" in patch and patch["quantity"] is not None and patch["quantity"] <= 0:
        raise HTTPException(status_code=400, detail="quantity phải > 0")
    updated = user_store.update_dividend(div_id, current_user["id"], patch)
    if not updated:
        raise HTTPException(status_code=404, detail="Dividend not found")
    return updated


@router.delete("/dividends/{div_id}")
def delete_div(div_id: str, current_user: dict = Depends(get_current_user)):
    ok = user_store.delete_dividend(div_id, current_user["id"])
    if not ok:
        raise HTTPException(status_code=404, detail="Dividend not found")
    return {"ok": True}
