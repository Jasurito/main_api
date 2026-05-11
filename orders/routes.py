import json
import os

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials

from auth.security import bearer_scheme, decode_access_token
from orders.schemas import CreateOrderRequest, OrderItemResponse, OrderResponse
from config import kafka, postgres


router = APIRouter(prefix="/orders", tags=["orders"])


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def create_order(
    data: CreateOrderRequest,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    event = {
        "type": "order.created",
        "data": {
            "user_id": user_id,
            "address_id": data.address_id,
        },
    }

    producer = await kafka.get_producer()
    topic = os.environ.get("KAFKA_ORDER_EVENTS_TOPIC", "order-events")
    await producer.send_and_wait(topic, json.dumps(event).encode("utf-8"))

    return {"status": "accepted", "message": "Order creation request sent to worker"}


@router.get("", response_model=list[OrderResponse])
async def list_orders(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT order_id, status, total_amount, created_at FROM orders WHERE user_id = $1 ORDER BY created_at DESC",
            user_id,
        )

    return [
        OrderResponse(
            order_id=r["order_id"],
            status=r["status"],
            total_amount=float(r["total_amount"] or 0),
            created_at=r["created_at"],
        )
        for r in rows
    ]


@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(
    order_id: int,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        order = await conn.fetchrow(
            "SELECT order_id, status, total_amount, created_at FROM orders WHERE order_id = $1 AND user_id = $2",
            order_id,
            user_id,
        )

        if not order:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")

        items = await conn.fetch(
            "SELECT product_id, quantity, unit_price FROM order_items WHERE order_id = $1",
            order_id,
        )

    return OrderResponse(
        order_id=order["order_id"],
        status=order["status"],
        total_amount=float(order["total_amount"] or 0),
        created_at=order["created_at"],
        items=[
            OrderItemResponse(
                product_id=i["product_id"],
                quantity=i["quantity"],
                unit_price=float(i["unit_price"]),
                subtotal=float(i["unit_price"]) * i["quantity"],
            )
            for i in items
        ],
    )
