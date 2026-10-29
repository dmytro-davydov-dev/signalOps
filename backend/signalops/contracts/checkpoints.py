"""Workflow step checkpoints stored in `agent_steps.checkpoint`.

A resumed worker decodes the checkpoint before reusing it. An unknown schema
version is an error rather than a best-effort read, and a checkpoint written by
another workflow version is never reused (cross-cutting rule 5).
"""

from collections.abc import Mapping
from typing import Any, Literal, Self

from pydantic import Field, ValidationError, model_validator

from signalops.contracts.common import (
    Attributes,
    ContractError,
    ContractErrorCategory,
    ContractModel,
    Key,
    Version,
    describe_validation_error,
    require_unique,
)
from signalops.contracts.workflow import AgentRole

CHECKPOINT_SCHEMA_VERSION: Literal[1] = 1
MAX_TOOL_CALLS_PER_STEP = 64


class StepCheckpoint(ContractModel):
    schema_version: Literal[1] = CHECKPOINT_SCHEMA_VERSION
    workflow_version: Version
    step_key: Key
    agent_role: AgentRole
    attempt: int = Field(ge=1)
    # Generation of the lease that wrote this checkpoint.
    lease_generation: int = Field(ge=0)
    # Tool calls whose results are persisted and can be reused on resume.
    completed_tool_calls: tuple[Key, ...] = Field(default=(), max_length=MAX_TOOL_CALLS_PER_STEP)
    # Node-specific structured state; never unrestricted model reasoning.
    state: Attributes = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check_calls(self) -> Self:
        require_unique(self.completed_tool_calls, "completed_tool_calls")
        return self

    def reusable_for(self, workflow_version: str) -> bool:
        return self.workflow_version == workflow_version


def decode_checkpoint(data: Mapping[str, Any]) -> StepCheckpoint:
    version = data.get("schema_version")
    if version != CHECKPOINT_SCHEMA_VERSION or type(version) is not int:
        raise ContractError(
            ContractErrorCategory.UNSUPPORTED_VERSION,
            f"checkpoint schema_version {version!r} is not supported",
        )
    try:
        return StepCheckpoint.model_validate(data)
    except ValidationError as error:
        raise ContractError(
            ContractErrorCategory.INVALID_PAYLOAD, describe_validation_error(error)
        ) from None
