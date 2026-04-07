from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
import asyncio, json
from app.services.market_data import market_service

router = APIRouter()
connections: list = []

async def broadcast(quotes):
    if not connections:
        return
    data = json.dumps(quotes)
    dead = []
    for ws in connections:
        try:
            await ws.send_text(data)
        except:
            dead.append(ws)
    for ws in dead:
        connections.remove(ws)

market_service.add_listener(broadcast)

@router.get("")
@router.get("/")
async def get_quotes(symbols: str = Query(...)):
    tickers = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    market_service.subscribe(tickers)
    return await market_service.fetch_quotes(tickers)

@router.get("/cached")
async def get_cached(symbols: str = Query(...)):
    tickers = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    return [market_service.quotes[t] for t in tickers if t in market_service.quotes]

@router.get("/all")
async def get_all():
    return list(market_service.quotes.values())

@router.get("/historical/{ticker}")
async def get_historical(ticker: str, from_date: str = Query(...), to_date: str = Query(...)):
    return await market_service.fetch_historical(ticker, from_date, to_date)

@router.post("/subscribe")
async def subscribe(tickers: list[str]):
    market_service.subscribe(tickers)
    return {"subscribed": list(market_service.subscribed)}

@router.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    connections.append(websocket)
    try:
        if market_service.quotes:
            await websocket.send_text(json.dumps(list(market_service.quotes.values())))
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
                if msg.get("action") == "subscribe":
                    tickers = msg.get("tickers", [])
                    market_service.subscribe(tickers)
                    quotes = await market_service.fetch_quotes(tickers)
                    if quotes:
                        await websocket.send_text(json.dumps(quotes))
                elif msg.get("action") == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
            except:
                pass
    except WebSocketDisconnect:
        if websocket in connections:
            connections.remove(websocket)
