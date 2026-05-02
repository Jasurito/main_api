import os
from elasticsearch import AsyncElasticsearch

_client: AsyncElasticsearch | None = None


def get_client() -> AsyncElasticsearch:
    global _client
    if _client is None:
        hosts = os.environ.get("ELASTICSEARCH_HOSTS", "http://localhost:9200").split(",")
        kwargs: dict = {"hosts": [h.strip() for h in hosts]}

        user = os.environ.get("ELASTICSEARCH_USER")
        password = os.environ.get("ELASTICSEARCH_PASSWORD")
        if user and password:
            kwargs["basic_auth"] = (user, password)

        api_key = os.environ.get("ELASTICSEARCH_API_KEY")
        if api_key:
            kwargs["api_key"] = api_key

        verify_certs = os.environ.get("ELASTICSEARCH_VERIFY_CERTS", "false").lower() != "false"
        kwargs["verify_certs"] = verify_certs

        _client = AsyncElasticsearch(**kwargs)
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None


async def ping() -> bool:
    return await get_client().ping()
