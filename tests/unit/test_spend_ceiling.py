"""Preflight of the run-level spend ceiling (P13-T06), with no network and no database."""

import pytest
from pydantic import SecretStr

from signalops.config import Settings
from signalops.contracts.evaluation import ScenarioSplit, SystemVariant
from signalops.evaluation.runner import PaidRunRefusedError, RunRequest, estimate, preflight

FAKE = Settings(database_url="postgresql+psycopg://unused@localhost/unused")  # type: ignore[arg-type]
# A non-fake provider; nothing here sends a request.
PAID = FAKE.model_copy(update={"model_provider": "anthropic", "model_api_key": SecretStr("unused")})
BUDGET = 3_000_000
AGENTS = (SystemVariant.SINGLE_AGENT, SystemVariant.MULTI_AGENT)


def request(**fields: object) -> RunRequest:
    base: dict[str, object] = {
        "label": "ceiling",
        "split": ScenarioSplit.DEVELOPMENT,
        "trials": 1,
        "budget_microusd": BUDGET,
    }
    return RunRequest.model_validate(base | fields)


def test_the_worst_case_counts_agent_cases_only() -> None:
    spend = estimate(request(), FAKE)
    # 14 development scenarios; the deterministic baseline costs nothing.
    assert (spend.scenarios, spend.agent_variants, spend.agent_cases) == (14, 2, 28)
    assert spend.worst_case_microusd == 28 * BUDGET
    assert spend.paid is False
    held_out = estimate(request(split=ScenarioSplit.HELD_OUT, trials=3, variants=AGENTS), FAKE)
    assert held_out.worst_case_microusd == 37 * 2 * 3 * BUDGET


def test_a_paid_run_needs_a_ceiling_permission_and_an_approval() -> None:
    worst = 28 * BUDGET
    for missing in (
        {},
        {"max_spend_microusd": worst, "approval_ref": "Q12"},
        {"allow_paid": True, "approval_ref": "Q12"},
        {"allow_paid": True, "max_spend_microusd": worst},
    ):
        with pytest.raises(PaidRunRefusedError, match="allow_paid"):
            preflight(request(**missing), PAID)
    allowed = request(allow_paid=True, max_spend_microusd=worst, approval_ref="Q12")
    assert preflight(allowed, PAID).worst_case_microusd == worst


def test_a_worst_case_one_microusd_over_the_ceiling_is_refused_with_both_numbers() -> None:
    worst = 28 * BUDGET
    over = request(allow_paid=True, max_spend_microusd=worst - 1, approval_ref="Q12")
    with pytest.raises(PaidRunRefusedError, match=f"worst case {worst} .* ceiling of {worst - 1}"):
        preflight(over, PAID)
    # The same bound holds for a fake run that states a ceiling.
    with pytest.raises(PaidRunRefusedError):
        preflight(request(max_spend_microusd=worst - 1), FAKE)


def test_the_fake_provider_needs_no_ceiling() -> None:
    assert preflight(request(), FAKE).max_spend_microusd is None
