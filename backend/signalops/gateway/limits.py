"""Provider rate limits, atomic in Redis (ADR-026).

A window holds a request count and a token count. The script takes one request and
its tokens together or nothing. Limits protect the provider's quota, not the budget,
which PostgreSQL owns, so losing the counters at worst lets one extra window through.

With Redis down, calls pause rather than run unmetered, and a pause that lasts too
long retries the job. Tokens are an estimate made before the call.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Final, Protocol

from redis.asyncio import Redis
from redis.exceptions import RedisError

from signalops.agents.jobs import TransientJobError

logger = logging.getLogger(__name__)

WINDOW_SECONDS: Final = 60
# Takes the window or reports the milliseconds until it ends. A call larger than the
# whole token limit still gets an empty window, or it could never run.
ACQUIRE: Final = """
local requests = tonumber(redis.call('GET', KEYS[1]) or '0')
local tokens = tonumber(redis.call('GET', KEYS[2]) or '0')
local wanted = tonumber(ARGV[3])
if requests + 1 > tonumber(ARGV[1]) or (tokens > 0 and tokens + wanted > tonumber(ARGV[2])) then
    return tonumber(ARGV[4])
end
redis.call('SET', KEYS[1], requests + 1, 'EX', ARGV[5])
redis.call('SET', KEYS[2], tokens + wanted, 'EX', ARGV[5])
return 0
"""
UNAVAILABLE_RETRY_SECONDS: Final = 1.0


class RateLimiter(Protocol):
    async def acquire(self, provider: str, tokens: int) -> None:
        """Wait for room for one request of about this many tokens."""
        ...


class NoRateLimit:
    async def acquire(self, provider: str, tokens: int) -> None:
        return None


@dataclass(frozen=True)
class RateLimits:
    requests_per_minute: int
    tokens_per_minute: int
    # The longest a call pauses for a full window or an unreachable Redis.
    max_wait_seconds: float


class RedisRateLimiter:
    def __init__(self, client: "Redis", limits: RateLimits) -> None:
        self._client = client
        self._limits = limits

    async def acquire(self, provider: str, tokens: int) -> None:
        loop = asyncio.get_running_loop()
        give_up = loop.time() + self._limits.max_wait_seconds
        while True:
            try:
                wait = await self._try(provider, tokens)
            except (RedisError, OSError) as error:
                logger.warning("rate limiter unavailable", extra={"error": type(error).__name__})
                wait = UNAVAILABLE_RETRY_SECONDS
            if wait == 0:
                return
            if loop.time() + wait > give_up:
                raise TransientJobError("no provider capacity within the wait limit")
            await asyncio.sleep(wait)

    async def _try(self, provider: str, tokens: int) -> float:
        now = time.time()
        window = int(now // WINDOW_SECONDS)
        remaining_ms = max(1, round((WINDOW_SECONDS - now % WINDOW_SECONDS) * 1000))
        wait_ms = await self._client.eval(
            ACQUIRE,
            2,
            f"limit:provider:{provider}:requests:{window}",
            f"limit:provider:{provider}:tokens:{window}",
            self._limits.requests_per_minute,
            self._limits.tokens_per_minute,
            tokens,
            remaining_ms,
            WINDOW_SECONDS * 2,
        )
        return int(wait_ms) / 1000
