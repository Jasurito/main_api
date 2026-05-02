import os
import motor.motor_asyncio

_client: motor.motor_asyncio.AsyncIOMotorClient | None = None


def get_client() -> motor.motor_asyncio.AsyncIOMotorClient:
    global _client
    if _client is None:
        uri = os.environ.get("MONGO_URI")
        if not uri:
            host = os.environ["MONGO_HOST"]
            port = os.environ.get("MONGO_PORT", "27017")
            user = os.environ.get("MONGO_USER", "")
            password = os.environ.get("MONGO_PASSWORD", "")
            if user and password:
                uri = f"mongodb://{user}:{password}@{host}:{port}"
            else:
                uri = f"mongodb://{host}:{port}"
        _client = motor.motor_asyncio.AsyncIOMotorClient(uri)
    return _client


def get_db(name: str | None = None) -> motor.motor_asyncio.AsyncIOMotorDatabase:
    db_name = name or os.environ["MONGO_DB"]
    return get_client()[db_name]


def close_client() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None


async def ping() -> bool:
    await get_client().admin.command("ping")
    return True
