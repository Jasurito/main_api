import asyncio
import time
from dataclasses import dataclass
from typing import Awaitable, Callable


@dataclass
class BucketState:
    tokens: float
    last_refill_at: float


@dataclass
class RateLimitDecision:
    allowed: bool
    retry_after_seconds: float


class TokenBucketRateLimiter:
    def __init__(self, capacity: int, refill_rate_per_second: float):
        if capacity <= 0:
            raise ValueError("capacity must be greater than zero")
        if refill_rate_per_second <= 0:
            raise ValueError("refill_rate_per_second must be greater than zero")

        self.capacity = capacity
        self.refill_rate_per_second = refill_rate_per_second
        self._buckets: dict[str, BucketState] = {}
        self._lock = asyncio.Lock()

    async def allow(self, key: str, tokens: int = 1) -> bool:
        if tokens <= 0:
            raise ValueError("tokens must be greater than zero")

        now = time.monotonic()

        async with self._lock:
            bucket = self._buckets.get(key)

            if bucket is None:
                bucket = BucketState(
                    tokens=float(self.capacity),
                    last_refill_at=now,
                )
                self._buckets[key] = bucket

            self._refill(bucket, now)

            if bucket.tokens < tokens:
                return False

            bucket.tokens -= tokens
            return True

    async def get_retry_after_seconds(self, key: str, tokens: int = 1) -> float:
        now = time.monotonic()

        async with self._lock:
            bucket = self._buckets.get(key)

            if bucket is None:
                return 0.0

            self._refill(bucket, now)

            missing_tokens = max(0.0, tokens - bucket.tokens)
            return missing_tokens / self.refill_rate_per_second

    def _refill(self, bucket: BucketState, now: float) -> None:
        elapsed_seconds = now - bucket.last_refill_at

        if elapsed_seconds <= 0:
            return

        new_tokens = elapsed_seconds * self.refill_rate_per_second
        bucket.tokens = min(self.capacity, bucket.tokens + new_tokens)
        bucket.last_refill_at = now


class RedisTokenBucketRateLimiter:
    _LUA_SCRIPT = """
local bucket_key = KEYS[1]
local block_key = KEYS[2]

local capacity = tonumber(ARGV[1])
local refill_rate_per_second = tonumber(ARGV[2])
local requested_tokens = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])
local block_ttl_ms = tonumber(ARGV[5])

local blocked = redis.call("PTTL", block_key)

if blocked > 0 then
    return { 0, blocked / 1000.0, 1 }
end

local redis_time = redis.call("TIME")
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)

local bucket = redis.call("HMGET", bucket_key, "tokens", "last_refill_ms")
local tokens = tonumber(bucket[1])
local last_refill_ms = tonumber(bucket[2])

if tokens == nil or last_refill_ms == nil then
    tokens = capacity
    last_refill_ms = now_ms
end

local elapsed_ms = now_ms - last_refill_ms
if elapsed_ms < 0 then
    elapsed_ms = 0
end

local refill_tokens = (elapsed_ms / 1000.0) * refill_rate_per_second
tokens = math.min(capacity, tokens + refill_tokens)
last_refill_ms = now_ms

local allowed = 0
local retry_after_seconds = 0
local is_blocked = 0

if tokens >= requested_tokens then
    tokens = tokens - requested_tokens
    allowed = 1
else
    redis.call("SET", block_key, "1", "PX", block_ttl_ms)
    retry_after_seconds = block_ttl_ms / 1000.0
    is_blocked = 1
end

redis.call("HSET", bucket_key, "tokens", tostring(tokens), "last_refill_ms", tostring(last_refill_ms))
redis.call("PEXPIRE", bucket_key, ttl_ms)

return { allowed, retry_after_seconds, is_blocked }
"""

    def __init__(
        self,
        redis_client_factory: Callable[[], Awaitable[object]],
        namespace: str,
        capacity: int,
        refill_rate_per_second: float,
        state_ttl_seconds: int | None = None,
        block_ttl_seconds: int = 10,
    ):
        if capacity <= 0:
            raise ValueError("capacity must be greater than zero")
        if refill_rate_per_second <= 0:
            raise ValueError("refill_rate_per_second must be greater than zero")
        if block_ttl_seconds <= 0:
            raise ValueError("block_ttl_seconds must be greater than zero")

        self.redis_client_factory = redis_client_factory
        self.namespace = namespace.strip(":")
        self.capacity = capacity
        self.refill_rate_per_second = refill_rate_per_second

        if state_ttl_seconds is None:
            state_ttl_seconds = max(60, int((capacity / refill_rate_per_second) * 2))

        self.state_ttl_seconds = state_ttl_seconds
        self.block_ttl_seconds = block_ttl_seconds

    async def consume(self, key: str, tokens: int = 1) -> RateLimitDecision:
        if tokens <= 0:
            raise ValueError("tokens must be greater than zero")

        bucket_key = f"{self.namespace}:bucket:{key}"
        block_key = f"{self.namespace}:blocked:{key}"

        ttl_ms = self.state_ttl_seconds * 1000
        block_ttl_ms = self.block_ttl_seconds * 1000

        client = await self.redis_client_factory()

        allowed, retry_after, _ = await client.eval(
            self._LUA_SCRIPT,
            2,
            bucket_key,
            block_key,
            self.capacity,
            self.refill_rate_per_second,
            tokens,
            ttl_ms,
            block_ttl_ms,
        )

        return RateLimitDecision(
            allowed=bool(int(allowed)),
            retry_after_seconds=float(retry_after),
        )