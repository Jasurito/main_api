from datetime import datetime
from pydantic import BaseModel


class CreateFlashSaleRequest(BaseModel):
    product_id: int
    discount_price: float
    stock: int
    starts_at: datetime
    ends_at: datetime


class FlashSaleResponse(BaseModel):
    flash_sale_id: int
    product_id: int
    discount_price: float
    stock: int
    starts_at: datetime
    ends_at: datetime
