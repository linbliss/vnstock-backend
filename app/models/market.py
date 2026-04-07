from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class StockQuote(BaseModel):
    ticker: str
    price: float
    reference_price: float   # Giá tham chiếu (đóng cửa hôm qua)
    change: float            # Thay đổi tuyệt đối
    change_pct: float        # Thay đổi %
    volume: int
    high: float
    low: float
    open: float
    ceiling: float           # Giá trần
    floor: float             # Giá sàn
    total_value: float       # Tổng giá trị khớp
    timestamp: str

class AlertConfig(BaseModel):
    id: str
    user_id: str
    ticker: str
    alert_type: str          # PRICE | STOP_LOSS | VOLUME | PIVOT
    condition: dict
    channels: list[str]      # ['telegram', 'email', 'sound']
    is_active: bool

class PriceAlertCondition(BaseModel):
    price: float
    operator: str            # gt | lt | gte | lte
