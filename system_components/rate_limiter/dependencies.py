import os

from fastapi import HTTPException, Request, status

from config import redis
from system_components.rate_limiter import RedisTokenBucketRateLimiter, TokenBucketRateLimiter


def _get_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return int(raw)


def _get_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return float(raw)


flash_sale_purchase_limiter = TokenBucketRateLimiter(
    capacity=_get_int_env("FLASH_SALE_RATE_LIMIT_CAPACITY", 3),
    refill_rate_per_second=_get_float_env("FLASH_SALE_RATE_LIMIT_REFILL_PER_SECOND", 0.3),
)


async def enforce_flash_sale_purchase_rate_limit(user_id: int, flash_sale_id: int) -> None:
    key = f"flash_sale:{flash_sale_id}:user:{user_id}"

    allowed = await flash_sale_purchase_limiter.allow(key)

    if allowed:
        return

    retry_after = await flash_sale_purchase_limiter.get_retry_after_seconds(key)

    raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail={
            "message": "Too many flash sale purchase attempts. Please try again later.",
            "retry_after_seconds": round(retry_after, 2),
        },
        headers={"Retry-After": str(max(1, round(retry_after)))},
    )


main_page_limiter = RedisTokenBucketRateLimiter(
    redis_client_factory=redis.get_client,
    namespace="rate_limit:main_page",
    capacity=_get_int_env("MAIN_PAGE_RATE_LIMIT_CAPACITY", 10),
    refill_rate_per_second=_get_float_env("MAIN_PAGE_RATE_LIMIT_REFILL_PER_SECOND", 2.0),
    state_ttl_seconds=_get_int_env("MAIN_PAGE_RATE_LIMIT_STATE_TTL_SECONDS", 60),
)


async def enforce_main_page_rate_limit(request: Request) -> None:
    forwarded_for = request.headers.get("x-forwarded-for")

    if forwarded_for:
        client_ip = forwarded_for.split(",")[0].strip()
    elif request.client:
        client_ip = request.client.host
    else:
        client_ip = "unknown"

    key = f"ip:{client_ip}:products_search"

    decision = await main_page_limiter.consume(key)

    if decision.allowed:
        return

    retry_after = max(1, round(decision.retry_after_seconds))

    raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail={
            "message": "Too many product search requests. Please slow down.",
            "retry_after_seconds": round(decision.retry_after_seconds, 2),
        },
        headers={"Retry-After": str(retry_after)},
    )