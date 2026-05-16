import asyncio
import time
from dataclasses import dataclass


@dataclass
class BucketState:
    tokens: float
    last_refill_at: float


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
