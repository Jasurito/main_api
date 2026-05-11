from pydantic import BaseModel


class CreateProductResponse(BaseModel):
    status: str
    message: str


class ProductResponse(BaseModel):
    product_id: int
    name: str
    description: str | None = None
    price: float
    quantity: int
    category: str | None = None
    images: list[str] = []


class ProductSearchResponse(BaseModel):
    total: int
    results: list[ProductResponse]
