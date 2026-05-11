import json
import os

from fastapi import APIRouter, Depends, status
from fastapi.security import HTTPAuthorizationCredentials

from auth.security import bearer_scheme, decode_access_token
from cart.schemas import AddToCartRequest, CartItemResponse, CartResponse
from config import kafka, postgres


router = APIRouter(prefix="/cart", tags=["cart"])


@router.post("/items", status_code=status.HTTP_202_ACCEPTED)
async def add_to_cart(
    data: AddToCartRequest,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    event = {
        "type": "cart.item_added",
        "data": {
            "user_id": user_id,
            "product_id": data.product_id,
            "quantity": data.quantity,
        },
    }

    producer = await kafka.get_producer()
    topic = os.environ.get("KAFKA_CART_EVENTS_TOPIC", "cart-events")
    await producer.send_and_wait(topic, json.dumps(event).encode("utf-8"))

    return {"status": "accepted", "message": "Item added to cart"}


@router.get("", response_model=CartResponse)
async def get_cart(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT cart_id, product_id, quantity, unit_price FROM cart_items WHERE user_id = $1",
            user_id,
        )

    items = [
        CartItemResponse(
            cart_id=r["cart_id"],
            product_id=r["product_id"],
            quantity=r["quantity"],
            unit_price=float(r["unit_price"]),
            subtotal=float(r["unit_price"]) * r["quantity"],
        )
        for r in rows
    ]

    return CartResponse(items=items, total=sum(i.subtotal for i in items))


@router.delete("/items/{product_id}", status_code=status.HTTP_202_ACCEPTED)
async def remove_from_cart(
    product_id: int,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    event = {
        "type": "cart.item_removed",
        "data": {
            "user_id": user_id,
            "product_id": product_id,
        },
    }

    producer = await kafka.get_producer()
    topic = os.environ.get("KAFKA_CART_EVENTS_TOPIC", "cart-events")
    await producer.send_and_wait(topic, json.dumps(event).encode("utf-8"))

    return {"status": "accepted", "message": "Item removed from cart"}
