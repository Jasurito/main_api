import json
import os

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials

from auth.security import bearer_scheme, decode_access_token
from orders.schemas import AdvanceOrderStatusRequest, CreateOrderRequest, OrderAddressResponse, OrderItemResponse, OrderResponse, VALID_TRANSITIONS
from config import kafka, postgres


router = APIRouter(prefix="/orders", tags=["orders"])


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def create_order(
    data: CreateOrderRequest,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        role = await conn.fetchval("SELECT role FROM users WHERE user_id = $1", user_id)
        if role == "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admins cannot place orders",
            )

        address = await conn.fetchrow(
            "SELECT address_id FROM addresses WHERE address_id = $1 AND user_id = $2",
            data.address_id,
            user_id,
        )

    if not address:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Address not found",
        )

    event = {
        "type": "order.created",
        "data": {"user_id": user_id, "address_id": data.address_id},
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
            """
            SELECT o.order_id, o.status, o.total_amount, o.created_at,
                   a.address_id, a.street, a.city, a.apartment, a.extra_info
            FROM orders o
            LEFT JOIN addresses a ON a.address_id = o.address_id
            WHERE o.user_id = $1
            ORDER BY o.created_at DESC
            """,
            user_id,
        )

    return [
        OrderResponse(
            order_id=r["order_id"],
            status=r["status"],
            total_amount=float(r["total_amount"] or 0),
            created_at=r["created_at"],
            address=OrderAddressResponse(
                address_id=r["address_id"],
                street=r["street"],
                city=r["city"],
                apartment=r["apartment"],
                extra_info=r["extra_info"],
            ) if r["address_id"] else None,
        )
        for r in rows
    ]


@router.patch("/{order_id}/status")
async def advance_order_status(
    order_id: int,
    data: AdvanceOrderStatusRequest,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        role = await conn.fetchval("SELECT role FROM users WHERE user_id = $1", user_id)
        if role != "admin":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")

        order = await conn.fetchrow("SELECT status FROM orders WHERE order_id = $1", order_id)
        if not order:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")

        current = order["status"]
        expected_next = VALID_TRANSITIONS.get(current)

        if expected_next is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Order with status '{current}' cannot be advanced",
            )

        if data.status != expected_next:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid transition: '{current}' → '{data.status}'. Expected '{expected_next}'",
            )

        await conn.execute("UPDATE orders SET status = $1 WHERE order_id = $2", data.status, order_id)

    return {"order_id": order_id, "status": data.status}


@router.post("/{order_id}/cancel", status_code=status.HTTP_202_ACCEPTED)
async def cancel_order(
    order_id: int,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        order = await conn.fetchrow(
            "SELECT status FROM orders WHERE order_id = $1 AND user_id = $2",
            order_id,
            user_id,
        )

    if not order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")

    if order["status"] == "cancelled":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Order is already cancelled")

    event = {
        "type": "order.cancelled",
        "data": {"order_id": order_id, "user_id": user_id},
    }

    producer = await kafka.get_producer()
    topic = os.environ.get("KAFKA_ORDER_EVENTS_TOPIC", "order-events")
    await producer.send_and_wait(topic, json.dumps(event).encode("utf-8"))

    return {"status": "accepted", "message": "Order cancellation request sent to worker"}


@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(
    order_id: int,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        order = await conn.fetchrow(
            """
            SELECT o.order_id, o.status, o.total_amount, o.created_at,
                   a.address_id, a.street, a.city, a.apartment, a.extra_info
            FROM orders o
            LEFT JOIN addresses a ON a.address_id = o.address_id
            WHERE o.order_id = $1 AND o.user_id = $2
            """,
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
        address=OrderAddressResponse(
            address_id=order["address_id"],
            street=order["street"],
            city=order["city"],
            apartment=order["apartment"],
            extra_info=order["extra_info"],
        ) if order["address_id"] else None,
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
