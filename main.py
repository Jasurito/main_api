from contextlib import asynccontextmanager

from fastapi import FastAPI

from config import elasticsearch, kafka, mongo, postgres


@asynccontextmanager
async def lifespan(app: FastAPI):
    await postgres.get_pool()
    await kafka.get_producer()
    yield
    await postgres.close_pool()
    await kafka.close_producer()
    await elasticsearch.close_client()
    await mongo.close_client()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "postgres": await postgres.ping(),
        "mongo": await mongo.ping(),
        "elasticsearch": await elasticsearch.ping(),
        "kafka": await kafka.ping(),
    }
