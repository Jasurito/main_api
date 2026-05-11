from pydantic import BaseModel


class AddToCartRequest(BaseModel):
    product_id: int
    quantity: int = 1


class CartItemResponse(BaseModel):
    cart_id: int
    product_id: int
    quantity: int
    unit_price: float
    subtotal: float


class CartResponse(BaseModel):
    items: list[CartItemResponse]
    total: float
