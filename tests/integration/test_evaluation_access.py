"""Evaluation persistence and database access roles (P10-T01).

Login roles with test-only names are provisioned the way `signalops migrate` does,
then each test connects as them. The runtime login runs a whole `v3` investigation,
so its grants are enough for the investigation runtime, and is refused every read
of ground truth. The evaluator login stores a scenario and scores a case.
"""

import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from pydantic import SecretStr
from sqlalchemy import Engine, delete, make_url, text
from sqlalchemy.exc import DBAPIError, ProgrammingError

from signalops.config import Settings
from signalops.contracts.diagnosis import Diagnosis, DiagnosisStatus
from signalops.contracts.evaluation import (
    CaseScores,
    CaseStatus,
    ExpectedOutcome,
    FaultWindow,
    ObservedOutcome,
    ScenarioDefinition,
    ScenarioFamily,
    ScenarioSplit,
    SystemVariant,
)
from signalops.contracts.workflow import InvestigationStatus
from signalops.evaluation.scoring import score_case
from signalops.gateway.fake import FakeProvider
from signalops.persistence import database as db
from signalops.persistence.models import EvaluationCase, Job
from signalops.persistence.repositories.evaluation import (
    EvaluationRepository,
    NewRun,
    ScenarioConflictError,
)
from signalops.persistence.roles import EVALUATOR_GROUP, RUNTIME_GROUP, provision_login_roles
from signalops.persistence.seed import DEMO_WORKSPACE_ID
from tests.integration.multi_agent_support import queued_v3, roles_with, timeout_incident, v3_engine
from tests.integration.test_investigator import row_of, run_once

pytestmark = pytest.mark.integration

EVALUATION_TABLES = ("scenarios", "runs", "replays", "cases", "audits")
HASH = "0" * 64


@dataclass(frozen=True)
class Logins:
    runtime: Settings
    evaluator: Settings


@pytest.fixture(scope="module")
def logins(seeded: tuple[Settings, Engine]) -> Iterator[Logins]:
    owner, engine = seeded
    suffix = uuid.uuid4().hex[:8]
    runtime_user, evaluator_user = f"so_test_app_{suffix}", f"so_test_eval_{suffix}"
    password = uuid.uuid4().hex
    provisioning = owner.model_copy(
        update={
            "db_runtime_user": runtime_user,
            "db_runtime_password": SecretStr(password),
            "db_evaluator_user": evaluator_user,
            "db_evaluator_password": SecretStr(password),
        }
    )
    assert provision_login_roles(provisioning) == [runtime_user, evaluator_user]
    # Provisioning twice updates the roles in place.
    assert provision_login_roles(provisioning) == [runtime_user, evaluator_user]
    url = make_url(owner.validated_database_url())

    def as_user(user: str) -> Settings:
        login = url.set(username=user, password=password).render_as_string(hide_password=False)
        return owner.model_copy(update={"database_url": SecretStr(login)})

    try:
        yield Logins(as_user(runtime_user), as_user(evaluator_user))
    finally:
        with engine.connect() as connection:
            for user in (runtime_user, evaluator_user):
                connection.execute(text(f'DROP ROLE IF EXISTS "{user}"'))
            connection.commit()


async def _database(settings: Settings) -> AsyncIterator[db.Database]:
    engine = db.create_engine(settings)
    try:
        yield db.Database.from_engine(engine)
    finally:
        await engine.dispose()


@pytest.fixture
async def runtime(logins: Logins) -> AsyncIterator[db.Database]:
    async for database in _database(logins.runtime):
        yield database


@pytest.fixture
async def evaluator(logins: Logins) -> AsyncIterator[db.Database]:
    async for database in _database(logins.evaluator):
        yield database


def test_only_the_evaluator_group_has_privileges_on_ground_truth(engine: Engine) -> None:
    with engine.connect() as connection:
        for table in EVALUATION_TABLES:
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCES", "TRIGGER"):
                runtime = connection.execute(
                    text("SELECT has_table_privilege(:role, :table, :privilege)"),
                    {"role": RUNTIME_GROUP, "table": f"evaluation.{table}", "privilege": privilege},
                ).scalar()
                assert runtime is False, (table, privilege)
            evaluator = connection.execute(
                text("SELECT has_table_privilege(:role, :table, 'SELECT')"),
                {"role": EVALUATOR_GROUP, "table": f"evaluation.{table}"},
            ).scalar()
            assert evaluator is True, table
        usage = connection.execute(
            text("SELECT has_schema_privilege(:role, 'evaluation', 'USAGE')"),
            {"role": RUNTIME_GROUP},
        ).scalar()
        assert usage is False
        # PUBLIC, which every role belongs to, has nothing either.
        public = connection.execute(
            text(
                "SELECT count(*) FROM pg_namespace, aclexplode(nspacl) AS acl "
                "WHERE nspname = 'evaluation' AND acl.grantee = 0"
            )
        ).scalar()
        assert public == 0


async def test_the_runtime_login_cannot_read_ground_truth(runtime: db.Database) -> None:
    queries = [f"SELECT * FROM evaluation.{table} LIMIT 1" for table in EVALUATION_TABLES]  # noqa: S608
    queries += [
        "SELECT expected_outcome, injection_spec FROM evaluation.scenarios",
        "SET ROLE signalops_evaluator",
        "UPDATE alembic_version SET version_num = version_num",
        "CREATE TABLE public.sneaky (id int)",
    ]
    for query in queries:
        with pytest.raises((ProgrammingError, DBAPIError)) as caught:
            async with runtime.transaction() as session:
                await session.execute(text(query))
        assert "permission denied" in str(caught.value.orig), query


async def test_the_runtime_login_runs_a_whole_investigation(
    database: db.Database, runtime: db.Database
) -> None:
    async with database.transaction() as session:
        await session.execute(delete(Job))
    scope, incident_id = await timeout_incident(database, 9701)
    investigation_id = await queued_v3(database, scope, incident_id)

    # Claims, steps, tool reads, budget holds, model calls, and the ending, all as runtime.
    await run_once(runtime, v3_engine(runtime, FakeProvider(responder=roles_with())))

    row = await row_of(database, investigation_id)
    assert row.status == InvestigationStatus.COMPLETED
    diagnosis = Diagnosis.model_validate(row.diagnosis)
    assert diagnosis.root_cause is not None
    assert diagnosis.root_cause.service_name == "bank-gateway"


def _definition(version: str = "1", root_cause: str = "bank-gateway") -> ScenarioDefinition:
    start = datetime(2026, 10, 16, 10, 34, tzinfo=UTC)
    return ScenarioDefinition(
        name=f"access-test-{uuid.uuid4().hex[:6]}",
        version=version,
        family=ScenarioFamily.DOWNSTREAM_TIMEOUT,
        split=ScenarioSplit.HELD_OUT,
        dataset_ref="generator:v1",
        dataset_hash=HASH,
        record_count=10,
        injection_spec={"fault": "latency", "service": root_cause},
        expected_outcome=ExpectedOutcome(
            incident_expected=True,
            diagnosable=True,
            root_cause_service=root_cause,
            failure_type="downstream_timeout",
            affected_services=(root_cause, "payment"),
            fault_window=FaultWindow(start=start, end=start.replace(minute=38)),
            supporting_event_ids=("bank-gateway-span-1",),
        ),
    )


async def test_the_evaluator_stores_ground_truth_and_scores_cases(
    evaluator: db.Database,
) -> None:
    definition = _definition()
    async with evaluator.transaction() as session:
        repository = EvaluationRepository(session)
        scenario = await repository.register_scenario(definition)
        # Registering the same version again is a no-op; changing it is refused.
        assert (await repository.register_scenario(definition)).id == scenario.id
    changed = definition.model_copy(update={"injection_spec": {"fault": "other"}})
    with pytest.raises(ScenarioConflictError, match="injection_spec"):
        async with evaluator.transaction() as session:
            await EvaluationRepository(session).register_scenario(changed)

    expected = definition.expected_outcome
    async with evaluator.transaction() as session:
        repository = EvaluationRepository(session)
        run = await repository.create_run(
            NewRun(
                workspace_id=DEMO_WORKSPACE_ID,
                label="access test",
                split=ScenarioSplit.HELD_OUT,
                variants=(SystemVariant.DETERMINISTIC,),
                trials=1,
                seed=7,
                dataset_hash=HASH,
                workflow_versions={"deterministic": "d1"},
                prompt_versions={},
                model_config={"provider": "fake"},
                pricing_config={"version": "test"},
                limits={"budget_microusd": 1},
            )
        )
        replay = await repository.ensure_replay(run.id, scenario.id)
        assert replay.replay_scope == f"replay:{replay.id}"
        assert (await repository.ensure_replay(run.id, scenario.id)).id == replay.id
        case = await repository.ensure_case(
            replay,
            case_key="case/deterministic/1",
            variant=SystemVariant.DETERMINISTIC,
            trial=1,
            expected_outcome=expected.summary().model_dump(mode="json"),
        )

    wrong = ObservedOutcome(
        diagnosis_status=DiagnosisStatus.DIAGNOSED,
        root_cause_service="payment",
        failure_type="downstream_timeout",
    )
    scores = score_case(expected, wrong)
    assert scores == CaseScores(
        outcome_correct=False,
        root_cause_correct=False,
        abstained=False,
        false_diagnosis=True,
        unsupported_diagnosis=True,
    )
    async with evaluator.transaction() as session:
        await EvaluationRepository(session).finish_case(
            case.id,
            status=CaseStatus.SCORED,
            observed=wrong.model_dump(mode="json"),
            scores=scores.model_dump(mode="json"),
            now=datetime.now(UTC),
        )
    async with evaluator.transaction() as session:
        stored = await session.get_one(EvaluationCase, case.id)
    assert stored.status == CaseStatus.SCORED
    assert stored.scores == scores.model_dump(mode="json")
    # The case keeps the outcome but not the evidence labels.
    assert stored.expected_outcome["supporting_event_ids"] == []
    assert stored.expected_outcome["root_cause_service"] == "bank-gateway"
