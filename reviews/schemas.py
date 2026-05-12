from datetime import date
from pydantic import BaseModel, field_validator


class CreateReviewRequest(BaseModel):
    product_id: int
    order_id: int
    rating: int
    comment: str | None = None

    @field_validator("rating")
    @classmethod
    def rating_range(cls, v: int) -> int:
        if not 1 <= v <= 5:
            raise ValueError("Rating must be between 1 and 5")
        return v


class ReviewResponse(BaseModel):
    review_id: int
    user_id: int
    product_id: int
    rating: int
    comment: str | None = None
    created_at: date
