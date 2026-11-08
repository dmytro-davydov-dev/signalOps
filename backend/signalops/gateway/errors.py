"""What can go wrong in a provider call, in terms the gateway acts on.

Messages hold no provider text: an error body can quote the request, which carries
evidence. The fields are a category, an HTTP status, and a request ID.

**Ambiguous.** A timeout, a lost connection, a server error, or an unreadable
response may follow a completed, billed request, so its cost stays unknown and its
reservation stays held. A retry is a new, separately reserved, possibly billable
call. A throttle or a rejection means the provider did no work.
"""

from enum import StrEnum
from typing import Final


class ProviderErrorCategory(StrEnum):
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    UNAVAILABLE = "unavailable"
    INVALID_RESPONSE = "invalid_response"
    THROTTLED = "throttled"
    REJECTED = "rejected"
    AUTHENTICATION = "authentication"


AMBIGUOUS_CATEGORIES: Final = frozenset(
    {
        ProviderErrorCategory.TIMEOUT,
        ProviderErrorCategory.CONNECTION,
        ProviderErrorCategory.UNAVAILABLE,
        ProviderErrorCategory.INVALID_RESPONSE,
    }
)
PERMANENT_CATEGORIES: Final = frozenset(
    {ProviderErrorCategory.REJECTED, ProviderErrorCategory.AUTHENTICATION}
)


class ProviderError(Exception):
    def __init__(
        self,
        category: ProviderErrorCategory,
        *,
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
        provider_request_id: str | None = None,
    ) -> None:
        super().__init__(f"provider call failed: {category} (status {status_code})")
        self.category = category
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        self.provider_request_id = provider_request_id

    @property
    def ambiguous(self) -> bool:
        return self.category in AMBIGUOUS_CATEGORIES

    @property
    def retryable(self) -> bool:
        return self.category not in PERMANENT_CATEGORIES


class BudgetExceededError(Exception):
    """The next call's maximum cost does not fit the budget; the investigation ends on it."""


class BudgetShareExhaustedError(Exception):
    """The call does not fit its step's share or the money not held back for later roles.

    Not a `BudgetExceededError`: the branch ends and reports it, the investigation goes on.
    """
