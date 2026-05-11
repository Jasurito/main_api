import os
from minio import Minio

_client: Minio | None = None


def get_client() -> Minio:
    global _client
    if _client is None:
        endpoint = os.environ.get("STORAGE_ENDPOINT", "http://rustfs:9000")
        # Minio expects host without scheme
        endpoint = endpoint.removeprefix("http://").removeprefix("https://")
        secure = os.environ.get("STORAGE_ENDPOINT", "").startswith("https://")
        _client = Minio(
            endpoint,
            access_key=os.environ["STORAGE_ACCESS_KEY"],
            secret_key=os.environ["STORAGE_SECRET_KEY"],
            secure=secure,
        )
    return _client


def get_bucket() -> str:
    return os.environ.get("STORAGE_BUCKET", "dad-storage")


def ensure_bucket() -> None:
    client = get_client()
    bucket = get_bucket()
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
