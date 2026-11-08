"""Operator and source credentials, rotatable without a restart (ADR-038).

Two sources of credentials, used together:

- `SIGNALOPS_CREDENTIALS_FILE`: a JSON file of named credentials, each stored
  as the SHA-256 digest of its token (`signalops hash-token` prints one), with
  an optional `not_after`. The API re-reads the file when its modification
  time or size changes, at most every `reload_seconds`, so adding a new
  credential and then deleting the old one rotates it with no restart. A file
  that becomes unreadable or invalid keeps the last valid set and logs an
  error; one that is invalid at startup stops the API.
- `SIGNALOPS_OPERATOR_TOKENS` and `SIGNALOPS_INGEST_TOKENS`: plaintext tokens in
  the environment, for development. Production settings refuse them.

A credential is either an operator's (opens operator routes for one
workspace) or a source's (opens ingestion for one source); a digest present in
both sets is refused, so a source credential never opens an operator route.
Every comparison checks all candidates in constant time, so timing reveals
neither which credential matched nor whether an expired one did.
"""

import hashlib
import hmac
import logging
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, SecretStr, StringConstraints, model_validator

from signalops.contracts.common import UtcDatetime

logger = logging.getLogger(__name__)

CredentialId = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9._:-]{1,64}$")]
Digest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


def token_digest(token: str) -> str:
    """The stored form of a token: lowercase hex SHA-256."""
    return hashlib.sha256(token.encode()).hexdigest()


class _Entry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: CredentialId
    sha256: Digest
    not_after: UtcDatetime | None = None


class OperatorEntry(_Entry):
    workspace_id: uuid.UUID


class SourceEntry(_Entry):
    source_id: uuid.UUID


class CredentialFile(BaseModel):
    """`SIGNALOPS_CREDENTIALS_FILE` contents."""

    model_config = ConfigDict(extra="forbid")

    operators: list[OperatorEntry] = Field(default_factory=list, max_length=1000)
    sources: list[SourceEntry] = Field(default_factory=list, max_length=1000)

    @model_validator(mode="after")
    def _check(self) -> Self:
        ids = [entry.id for entry in (*self.operators, *self.sources)]
        if len(ids) != len(set(ids)):
            raise ValueError("credential ids must be unique")
        operator_digests = {entry.sha256 for entry in self.operators}
        if any(entry.sha256 in operator_digests for entry in self.sources):
            raise ValueError("a source credential must not equal an operator credential")
        return self


@dataclass(frozen=True)
class Credential:
    id: str
    # The workspace an operator credential opens, or the source a source credential opens.
    principal: uuid.UUID
    digest: bytes
    not_after: datetime | None


@dataclass(frozen=True)
class CredentialSet:
    operators: tuple[Credential, ...]
    sources: tuple[Credential, ...]


def _credential(
    entry_id: str, principal: uuid.UUID, digest: str, not_after: datetime | None
) -> Credential:
    return Credential(entry_id, principal, bytes.fromhex(digest), not_after)


def credential_set(
    file: CredentialFile | None,
    operator_tokens: Mapping[uuid.UUID, SecretStr],
    source_tokens: Mapping[uuid.UUID, SecretStr],
) -> CredentialSet:
    """File credentials plus environment tokens, checked to be disjoint."""
    operators = [
        _credential(
            f"env:operator:{workspace}", workspace, token_digest(t.get_secret_value()), None
        )
        for workspace, t in operator_tokens.items()
    ]
    sources = [
        _credential(f"env:source:{source}", source, token_digest(t.get_secret_value()), None)
        for source, t in source_tokens.items()
    ]
    if file is not None:
        operators += [
            _credential(e.id, e.workspace_id, e.sha256, e.not_after) for e in file.operators
        ]
        sources += [_credential(e.id, e.source_id, e.sha256, e.not_after) for e in file.sources]
    operator_digests = {c.digest for c in operators}
    if any(c.digest in operator_digests for c in sources):
        raise ValueError("a source credential must not equal an operator credential")
    return CredentialSet(tuple(operators), tuple(sources))


@dataclass(frozen=True)
class Match:
    credential_id: str
    principal: uuid.UUID


def _match(candidates: Iterable[Credential], presented: str, now: datetime) -> Match | None:
    digest = hashlib.sha256(presented.encode()).digest()
    found: Match | None = None
    # Compare against every candidate, so timing does not reveal which one matched.
    for credential in candidates:
        same = hmac.compare_digest(digest, credential.digest)
        live = credential.not_after is None or now < credential.not_after
        if same and live:
            found = Match(credential.id, credential.principal)
    return found


class CredentialStore:
    def __init__(
        self,
        operator_tokens: Mapping[uuid.UUID, SecretStr],
        source_tokens: Mapping[uuid.UUID, SecretStr],
        path: Path | None = None,
        *,
        reload_seconds: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._operator_tokens = dict(operator_tokens)
        self._source_tokens = dict(source_tokens)
        self._path = path
        self._reload_seconds = reload_seconds
        self._clock = clock
        self._now = now
        self._stamp: tuple[int, int] | None = None
        self._checked = clock()
        # Raises at startup for a missing or invalid file: fail closed.
        self._set = credential_set(self._read(), self._operator_tokens, self._source_tokens)

    def operator(self, presented: str) -> Match | None:
        return _match(self._current().operators, presented, self._now())

    def source(self, source_id: uuid.UUID, presented: str) -> Match | None:
        # Every source credential is compared, so timing does not reveal which sources have one.
        found = _match(self._current().sources, presented, self._now())
        return found if found is not None and found.principal == source_id else None

    def _read(self) -> CredentialFile | None:
        if self._path is None:
            return None
        stat = self._path.stat()
        self._stamp = (stat.st_mtime_ns, stat.st_size)
        return CredentialFile.model_validate_json(self._path.read_bytes())

    def _current(self) -> CredentialSet:
        if self._path is None or self._clock() - self._checked < self._reload_seconds:
            return self._set
        self._checked = self._clock()
        try:
            stat = self._path.stat()
            if (stat.st_mtime_ns, stat.st_size) == self._stamp:
                return self._set
            self._set = credential_set(self._read(), self._operator_tokens, self._source_tokens)
            logger.info(
                "credentials reloaded",
                extra={"operators": len(self._set.operators), "sources": len(self._set.sources)},
            )
        except (OSError, ValueError) as error:
            # Keeping the last valid set means a bad edit cannot lock everyone out
            # or reopen a removed credential.
            logger.error("credentials file rejected", extra={"error": type(error).__name__})
        return self._set
