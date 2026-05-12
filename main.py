from contextlib import asynccontextmanager

from fastapi import FastAPI

from addresses.routes import router as addresses_router
from auth.routes import router as auth_router
from cart.routes import router as cart_router
from flash_sales.routes import router as flash_sales_router
from orders.routes import router as orders_router
from products.routes import router as products_router
from reviews.routes import router as reviews_router
from users.routes import router as users_router

from config import elasticsearch, kafka, mongo, postgres, storage


@asynccontextmanager
async def lifespan(app: FastAPI):
    await postgres.get_pool()
    await kafka.get_producer()
    storage.ensure_bucket()
    yield
    await postgres.close_pool()
    await kafka.close_producer()
    await elasticsearch.close_client()
    await mongo.close_client()


app = FastAPI(lifespan=lifespan)

app.include_router(auth_router)
app.include_router(addresses_router)
app.include_router(products_router)
app.include_router(cart_router)
app.include_router(orders_router)
app.include_router(flash_sales_router)
app.include_router(reviews_router)
app.include_router(users_router)


@app.get("/health")
async def health():
    return {
        "postgres": await postgres.ping(),
        "mongo": await mongo.ping(),
        "elasticsearch": await elasticsearch.ping(),
        "kafka": await kafka.ping(),
    }
