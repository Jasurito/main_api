from datetime import date
from pydantic import BaseModel


class CreateOrderRequest(BaseModel):
    address_id: int | None = None


class OrderItemResponse(BaseModel):
    product_id: int
    quantity: int
    unit_price: float
    subtotal: float


class OrderResponse(BaseModel):
    order_id: int
    status: str | None = None
    total_amount: float
    created_at: date
    items: list[OrderItemResponse] = []
