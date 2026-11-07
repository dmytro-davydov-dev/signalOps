"""Read-only tool results in Redis (ADR-029).

Redis is an accelerator here, never an authority. The key holds everything that
decides a result: workspace, replay scope, cutoff, data version, tool, and arguments.
The value repeats the identifying parts, and a read compares them and revalidates the
result as a contract, including each entry against the cutoff. So a stale or foreign
value is a miss. An unreachable Redis is a miss as well.
"""

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, Protocol

from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from signalops.contracts.tools import TOOL_RESULT_ADAPTER, ToolResult
from signalops.contracts.workflow import ToolName
from signalops.persistence.repositories.scope import EvidenceScope

logger = logging.getLogger(__name__)

# Bump when a tool's output changes, so no older shape is read back.
CACHE_FORMAT: Final = 1


@dataclass(frozen=True)
class ToolScope:
    """What a call may see, set from the investigation and never from the model."""

    evidence: EvidenceScope
    cutoff: datetime


@dataclass(frozen=True)
class ReadKey:
    scope: ToolScope
    data_version: str
    tool: ToolName
    # Canonical JSON of the validated arguments.
    arguments: str

    def redis_key(self) -> str:
        digest = hashlib.sha256(self.arguments.encode()).hexdigest()
        evidence = self.scope.evidence
        return (
            f"cache:query:{CACHE_FORMAT}:{evidence.workspace_id}:{evidence.replay_scope}:"
            f"{self.scope.cutoff.isoformat()}:{self.data_version}:{self.tool}:{digest}"
        )

    def identity(self) -> dict[str, str]:
        evidence = self.scope.evidence
        return {
            "workspace_id": str(evidence.workspace_id),
            "replay_scope": evidence.replay_scope,
            "evidence_cutoff_at": self.scope.cutoff.isoformat(),
            "data_version": self.data_version,
            "tool": self.tool.value,
            "arguments": self.arguments,
        }


class ReadCache(Protocol):
    async def get(self, key: ReadKey) -> ToolResult | None: ...

    async def put(self, key: ReadKey, result: ToolResult) -> None: ...


class RedisReadCache:
    def __init__(self, client: "Redis", ttl_seconds: int) -> None:
        self._client = client
        self._ttl_seconds = ttl_seconds

    async def get(self, key: ReadKey) -> ToolResult | None:
        try:
            stored = await self._client.get(key.redis_key())
        except (RedisError, OSError) as error:
            logger.warning("read cache unavailable", extra={"error": type(error).__name__})
            return None
        return None if stored is None else self._decode(key, stored)

    async def put(self, key: ReadKey, result: ToolResult) -> None:
        value = json.dumps({"identity": key.identity(), "result": result.model_dump(mode="json")})
        try:
            await self._client.set(key.redis_key(), value, ex=self._ttl_seconds)
        except (RedisError, OSError) as error:
            logger.warning("read cache unavailable", extra={"error": type(error).__name__})

    @staticmethod
    def _decode(key: ReadKey, stored: bytes | str) -> ToolResult | None:
        try:
            document: dict[str, Any] = json.loads(stored)
            if document["identity"] != key.identity():
                raise ValueError("the value belongs to another read")
            result = TOOL_RESULT_ADAPTER.validate_python(document["result"])
            if result.tool != key.tool or result.evidence_cutoff_at != key.scope.cutoff:
                raise ValueError("the result belongs to another read")
        except (ValueError, KeyError, TypeError, ValidationError):
            logger.warning("read cache value rejected", extra={"tool": str(key.tool)})
            return None
        return result
