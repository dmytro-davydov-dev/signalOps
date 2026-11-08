"""Redis pub/sub wake-ups for investigation progress streams.

A wake-up only tells a stream to read PostgreSQL now rather than at its next
catch-up. It carries no data, may be lost, and may arrive for a commit the
stream has already read; `progress_events` stays the only record.
"""

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Protocol

from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

# A publish slower than this is dropped; the stream's catch-up covers it.
PUBLISH_TIMEOUT_SECONDS = 0.5


def channel_for(investigation_id: uuid.UUID) -> str:
    return f"ui:investigation:{investigation_id}"


class RedisProgressNotifier:
    """A `Database.on_progress` hook publishing one wake-up per investigation."""

    def __init__(self, client: Redis) -> None:
        self._client = client

    async def __call__(self, investigation_ids: frozenset[uuid.UUID]) -> None:
        try:
            async with asyncio.timeout(PUBLISH_TIMEOUT_SECONDS):
                for investigation_id in investigation_ids:
                    await self._client.publish(channel_for(investigation_id), "1")
        except (RedisError, OSError, TimeoutError) as error:
            logger.debug("progress wake-up not published", extra={"error": type(error).__name__})


class Wakeups(Protocol):
    async def wait(self, seconds: float) -> bool:
        """True when woken within `seconds`; False when they pass."""
        ...


class SleepOnly:
    """No wake-ups: every wait lasts its timeout, so readers fall back to catch-up."""

    async def wait(self, seconds: float) -> bool:
        await asyncio.sleep(seconds)
        return False


class RedisWakeups:
    def __init__(self, pubsub: PubSub) -> None:
        self._pubsub = pubsub
        self._failed = False

    async def wait(self, seconds: float) -> bool:
        if self._failed:
            await asyncio.sleep(seconds)
            return False
        loop = asyncio.get_running_loop()
        deadline = loop.time() + seconds
        try:
            while (left := deadline - loop.time()) > 0:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=left
                )
                if message is not None:
                    return True
        except (RedisError, OSError) as error:
            # Redis went away mid-stream: keep serving from catch-up alone.
            logger.info("progress wake-ups lost", extra={"error": type(error).__name__})
            self._failed = True
            await asyncio.sleep(max(0.0, deadline - loop.time()))
        return False


@asynccontextmanager
async def subscribe(client: Redis | None, investigation_id: uuid.UUID) -> AsyncIterator[Wakeups]:
    """Wake-ups for one investigation, or catch-up alone when Redis is unavailable."""
    if client is None:
        yield SleepOnly()
        return
    pubsub = client.pubsub()
    try:
        try:
            await pubsub.subscribe(channel_for(investigation_id))
        except (RedisError, OSError) as error:
            logger.info("progress wake-ups unavailable", extra={"error": type(error).__name__})
            yield SleepOnly()
            return
        yield RedisWakeups(pubsub)
    finally:
        with suppress(RedisError, OSError):
            await pubsub.aclose()  # type: ignore[no-untyped-call]
