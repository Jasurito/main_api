import asyncio
import json
import os

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.security import HTTPAuthorizationCredentials

from auth.security import bearer_scheme, decode_access_token
from flash_sales.schemas import CreateFlashSaleRequest, FlashSaleResponse
from config import kafka, postgres


router = APIRouter(prefix="/flash-sales", tags=["flash-sales"])


async def require_admin(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)
    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        role = await conn.fetchval("SELECT role FROM users WHERE user_id = $1", user_id)
    if role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return user_id


@router.post("", response_model=FlashSaleResponse, status_code=status.HTTP_201_CREATED)
async def create_flash_sale(
    data: CreateFlashSaleRequest,
    _: int = Depends(require_admin),
):
    if data.ends_at <= data.starts_at:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ends_at must be after starts_at")

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        product_exists = await conn.fetchval(
            "SELECT 1 FROM products WHERE product_id = $1", data.product_id
        )
        if not product_exists:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")

        row = await conn.fetchrow(
            """
            INSERT INTO flash_sales (product_id, discount_price, stock, starts_at, ends_at)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING flash_sale_id, product_id, discount_price, stock, starts_at, ends_at
            """,
            data.product_id,
            data.discount_price,
            data.stock,
            data.starts_at,
            data.ends_at,
        )

    return FlashSaleResponse(**dict(row))


@router.get("/active", response_model=list[FlashSaleResponse])
async def get_active_flash_sales():
    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT flash_sale_id, product_id, discount_price, stock, starts_at, ends_at
            FROM flash_sales
            WHERE NOW() BETWEEN starts_at AND ends_at AND stock > 0
            ORDER BY ends_at ASC
            """,
        )
    return [FlashSaleResponse(**dict(r)) for r in rows]


@router.post("/{flash_sale_id}/buy", status_code=status.HTTP_202_ACCEPTED)
async def buy_flash_sale(
    flash_sale_id: int,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        sale = await conn.fetchrow(
            """
            SELECT flash_sale_id, product_id, discount_price, stock, starts_at, ends_at
            FROM flash_sales
            WHERE flash_sale_id = $1
            """,
            flash_sale_id,
        )

    if not sale:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Flash sale not found")

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    if not (sale["starts_at"] <= now <= sale["ends_at"]):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Flash sale is not active")

    if sale["stock"] <= 0:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Flash sale is sold out")

    event = {
        "type": "flash_sale.purchased",
        "data": {
            "user_id": user_id,
            "flash_sale_id": flash_sale_id,
            "product_id": sale["product_id"],
            "discount_price": float(sale["discount_price"]),
        },
    }

    producer = await kafka.get_producer()
    topic = os.environ.get("KAFKA_FLASH_SALE_EVENTS_TOPIC", "flash-sale-events")
    await producer.send_and_wait(topic, json.dumps(event).encode("utf-8"))

    return {"status": "accepted", "message": "Purchase request sent to worker"}


@router.websocket("/{flash_sale_id}/stock")
async def flash_sale_stock_ws(flash_sale_id: int, websocket: WebSocket):
    await websocket.accept()

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT stock FROM flash_sales WHERE flash_sale_id = $1", flash_sale_id
        )

    if not row:
        await websocket.send_json({"error": "Flash sale not found"})
        await websocket.close()
        return

    await websocket.send_json({"flash_sale_id": flash_sale_id, "stock": row["stock"]})

    queue: asyncio.Queue = asyncio.Queue()

    def on_notify(connection, pid, channel, payload):
        queue.put_nowait(payload)

    channel = f"flash_stock_{flash_sale_id}"

    async with pool.acquire() as conn:
        await conn.add_listener(channel, on_notify)
        try:
            while True:
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=30.0)
                    data = json.loads(payload)
                    await websocket.send_json({"flash_sale_id": flash_sale_id, "stock": data["stock"]})
                except asyncio.TimeoutError:
                    await websocket.send_json({"type": "keepalive"})
        except WebSocketDisconnect:
            pass
        finally:
            await conn.remove_listener(channel, on_notify)
