"""A record of each tool call an agent made.

Rows hold the arguments and a hash of the result, never the result, so evidence stays
in its own tables. A repeated call in a step reuses its row: `call_key` derives from
the arguments, and tools are read-only, so the outcome is the same.
"""

import uuid

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from signalops.contracts.workflow import ToolCallStatus, ToolName
from signalops.persistence.models import ToolCall
from signalops.persistence.repositories.llm_calls import LlmCallRepository


class ToolCallRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def start(
        self,
        investigation_id: uuid.UUID,
        step_key: str,
        call_key: str,
        tool: ToolName,
        arguments: dict[str, object],
    ) -> uuid.UUID:
        step_id = await LlmCallRepository(self._session).step_id(investigation_id, step_key)
        statement = insert(ToolCall).values(
            agent_step_id=step_id,
            call_key=call_key,
            tool_name=tool,
            arguments=arguments,
            status=ToolCallStatus.RUNNING,
        )
        saved = await self._session.execute(
            statement.on_conflict_do_update(
                index_elements=[ToolCall.agent_step_id, ToolCall.call_key],
                set_={"status": ToolCallStatus.RUNNING},
            ).returning(ToolCall.id)
        )
        return saved.scalar_one()

    async def finish(
        self, call_id: uuid.UUID, status: ToolCallStatus, result_ref: str | None, duration_ms: int
    ) -> None:
        await self._session.execute(
            update(ToolCall)
            .where(ToolCall.id == call_id)
            .values(status=status, result_ref=result_ref, duration_ms=duration_ms)
        )
