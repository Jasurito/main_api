import asyncio
import io
import json
import os
import uuid

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, WebSocket, WebSocketDisconnect, status
from fastapi.security import HTTPAuthorizationCredentials

from auth.security import bearer_scheme, decode_access_token
from products.schemas import CreateProductResponse, ProductResponse, ProductSearchResponse
from config import elasticsearch, kafka, mongo, postgres, storage


router = APIRouter(prefix="/products", tags=["products"])

ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}


async def require_admin(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        role = await conn.fetchval(
            "SELECT role FROM users WHERE user_id = $1",
            user_id,
        )

    if role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )

    return user_id


def _build_image_urls(paths: list[str]) -> list[str]:
    base = os.environ.get("STORAGE_BASE_URL", "").rstrip("/")
    if not base:
        return paths
    return [f"{base}/{p.lstrip('/')}" for p in paths]


@router.post("", response_model=CreateProductResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_product(
    name: str = Form(...),
    price: float = Form(...),
    quantity: int = Form(0),
    description: str | None = Form(None),
    category: str | None = Form(None),
    images: list[UploadFile] = File(default=[]),
    _: int = Depends(require_admin),
):
    for image in images:
        ext = os.path.splitext(image.filename or "")[1].lower()
        if ext not in ALLOWED_EXTENSIONS:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File '{image.filename}' is not allowed. Use jpg, png, or webp.",
            )

    client = storage.get_client()
    bucket = storage.get_bucket()
    temp_paths = []

    batch = uuid.uuid4().hex
    for image in images:
        content = await image.read()
        temp_path = f"tmp/{batch}/{image.filename}"
        await asyncio.to_thread(
            client.put_object,
            bucket,
            temp_path,
            io.BytesIO(content),
            length=len(content),
            content_type=image.content_type or "application/octet-stream",
        )
        temp_paths.append(temp_path)

    event = {
        "type": "product.created",
        "data": {
            "name": name,
            "description": description,
            "price": price,
            "quantity": quantity,
            "category": category,
            "temp_images": temp_paths,
        },
    }

    producer = await kafka.get_producer()
    topic = os.environ.get("KAFKA_PRODUCT_EVENTS_TOPIC", "product-events")
    await producer.send_and_wait(topic, json.dumps(event).encode("utf-8"))

    return CreateProductResponse(
        status="accepted",
        message="Product creation request sent to worker",
    )


@router.get("/search", response_model=ProductSearchResponse)
async def search_products(
    q: str | None = None,
    category: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    in_stock: bool | None = None,
    size: int = 10,
):
    must_clause = (
        {
            "multi_match": {
                "query": q,
                "fields": ["name^3", "description", "category^2"],
                "type": "best_fields",
                "fuzziness": "AUTO",
            }
        }
        if q
        else {"match_all": {}}
    )

    filters = []
    if category:
        filters.append({"term": {"category": category}})
    if min_price is not None or max_price is not None:
        price_range: dict = {}
        if min_price is not None:
            price_range["gte"] = min_price
        if max_price is not None:
            price_range["lte"] = max_price
        filters.append({"range": {"price": price_range}})

    body: dict = {
        "size": size,
        "query": {"bool": {"must": must_clause, "filter": filters}},
    }

    es = elasticsearch.get_client()
    resp = await es.search(index="products", body=body, ignore_unavailable=True)
    hits = resp["hits"]["hits"]
    total = resp["hits"]["total"]["value"]

    product_ids = [h["_source"]["postgres_id"] for h in hits]
    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        qty_rows = await conn.fetch(
            "SELECT product_id, quantity FROM products WHERE product_id = ANY($1)",
            product_ids,
        )
    qty_map = {r["product_id"]: r["quantity"] for r in qty_rows}

    results = [
        ProductResponse(
            product_id=h["_source"]["postgres_id"],
            name=h["_source"]["name"],
            description=h["_source"].get("description"),
            price=h["_source"]["price"],
            quantity=qty_map.get(h["_source"]["postgres_id"], 0),
            category=h["_source"].get("category"),
            images=_build_image_urls(h["_source"].get("images", [])),
        )
        for h in hits
        if in_stock is None or (qty_map.get(h["_source"]["postgres_id"], 0) > 0) == in_stock
    ]

    return ProductSearchResponse(total=len(results), results=results)


@router.websocket("/{product_id}/quantity")
async def product_quantity_ws(product_id: int, websocket: WebSocket):
    await websocket.accept()
    pool = await postgres.get_pool()
    last_quantity = None

    try:
        while True:
            async with pool.acquire() as conn:
                quantity = await conn.fetchval(
                    "SELECT quantity FROM products WHERE product_id = $1",
                    product_id,
                )

            if quantity is None:
                await websocket.send_json({"error": "Product not found", "product_id": product_id})
                break

            if quantity != last_quantity:
                await websocket.send_json({"product_id": product_id, "quantity": quantity})
                last_quantity = quantity

            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass


@router.get("/{product_id}", response_model=ProductResponse)
async def get_product(product_id: int):
    doc, pool = await asyncio.gather(
        mongo.get_db().products.find_one({"postgres_id": product_id}),
        postgres.get_pool(),
    )

    if not doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")

    async with pool.acquire() as conn:
        quantity = await conn.fetchval(
            "SELECT quantity FROM products WHERE product_id = $1", product_id
        )

    return ProductResponse(
        product_id=doc["postgres_id"],
        name=doc["name"],
        description=doc.get("description"),
        price=doc["price"],
        quantity=quantity or 0,
        category=doc.get("category"),
        images=_build_image_urls(doc.get("images", [])),
    )
