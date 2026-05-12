from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials

from auth.security import bearer_scheme, decode_access_token
from reviews.schemas import CreateReviewRequest, ReviewResponse
from config import postgres


router = APIRouter(tags=["reviews"])


@router.post("/reviews", response_model=ReviewResponse, status_code=status.HTTP_201_CREATED)
async def create_review(
    data: CreateReviewRequest,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        order = await conn.fetchrow(
            "SELECT status FROM orders WHERE order_id = $1 AND user_id = $2",
            data.order_id,
            user_id,
        )

        if not order:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")

        if order["status"] != "delivered":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You can only review products from delivered orders",
            )

        ordered = await conn.fetchval(
            "SELECT 1 FROM order_items WHERE order_id = $1 AND product_id = $2",
            data.order_id,
            data.product_id,
        )

        if not ordered:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Product was not part of this order",
            )

        existing = await conn.fetchval(
            "SELECT 1 FROM reviews WHERE user_id = $1 AND product_id = $2 AND order_id = $3",
            user_id,
            data.product_id,
            data.order_id,
        )

        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="You have already reviewed this product for this order",
            )

        row = await conn.fetchrow(
            """
            INSERT INTO reviews (user_id, product_id, order_id, rating, comment)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING review_id, user_id, product_id, rating, comment, created_at
            """,
            user_id,
            data.product_id,
            data.order_id,
            data.rating,
            data.comment,
        )

    return ReviewResponse(**dict(row))


@router.get("/products/{product_id}/reviews", response_model=list[ReviewResponse])
async def get_product_reviews(product_id: int):
    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT review_id, user_id, product_id, rating, comment, created_at
            FROM reviews
            WHERE product_id = $1
            ORDER BY created_at DESC
            """,
            product_id,
        )
    return [ReviewResponse(**dict(r)) for r in rows]
