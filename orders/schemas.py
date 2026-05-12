from datetime import date
from pydantic import BaseModel


VALID_TRANSITIONS = {
    "pending": "confirmed",
    "confirmed": "processing",
    "processing": "shipped",
    "shipped": "delivered",
}


class CreateOrderRequest(BaseModel):
    address_id: int


class AdvanceOrderStatusRequest(BaseModel):
    status: str


class OrderItemResponse(BaseModel):
    product_id: int
    quantity: int
    unit_price: float
    subtotal: float


class OrderAddressResponse(BaseModel):
    address_id: int
    street: str
    city: str
    apartment: str | None = None
    extra_info: str | None = None


class OrderResponse(BaseModel):
    order_id: int
    status: str | None = None
    total_amount: float
    created_at: date
    address: OrderAddressResponse | None = None
    items: list[OrderItemResponse] = []
