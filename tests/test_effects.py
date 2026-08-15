from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, cast

import pytest

from maida.workflows import (
    AccessContractError,
    AccessPolicy,
    ApprovalEvidence,
    ApproveCommand,
    CancelCommand,
    Capability,
    CapabilityGrant,
    ConnectorRegistry,
    Effect,
    EffectAdapter,
    EffectSpec,
    ExecutionContext,
    Idempotency,
    InteractionKind,
    InteractionRequest,
    Module,
    PauseCommand,
    PolicyDecision,
    RejectCommand,
    ResumeCommand,
    RuntimeValue,
    TaskWorker,
    Workflow,
    WorkflowClient,
    WorkflowRun,
    WorkflowRunner,
    WorkflowScheduler,
)
from maida.workflows._canonical import digest_data
from maida.workflows.models import EffectKind, EffectRecord, RunStatus, TaskStatus
from maida.workflows.persistence import PersistenceError, PostgresStore, StaleLeaseError
from maida.workflows.replay import build_module_registry

SEND_MESSAGE = EffectSpec(
    "messaging.send",
    connector="messaging",
    operation="send",
    input_type=str,
    output_type=str,
    connector_version="v1",
    idempotency=Idempotency.REQUIRED,
)


@dataclass(frozen=True)
class Receipt:
    receipt_id: str


SEND_RECEIPTS = EffectSpec(
    "receipts.send",
    connector="receipts",
    operation="send",
    input_type=str,
    output_type=dict[str, Receipt],
    connector_version="v1",
    idempotency=Idempotency.REQUIRED,
)


class MessagingAdapter:
    connector = "messaging"
    connector_version = "v1"
    operations: frozenset[str] = frozenset()
    effect_operations = frozenset({"send"})

    def __init__(
        self,
        *,
        idempotent: bool = True,
        fail_first: bool = False,
        invalid_response: bool = False,
        on_effect: Callable[[], None] | None = None,
    ) -> None:
        self.idempotent_effects = frozenset({"send"}) if idempotent else frozenset()
        self.fail_first = fail_first
        self.invalid_response = invalid_response
        self.on_effect = on_effect
        self.calls: list[tuple[str, str, str]] = []
        self.secret = "provider-credential-must-never-be-recorded"

    async def effect(
        self,
        operation: str,
        request: Any,
        *,
        idempotency_key: str,
    ) -> Any:
        self.calls.append((operation, str(request), idempotency_key))
        if self.on_effect is not None:
            self.on_effect()
        if self.fail_first and len(self.calls) == 1:
            raise RuntimeError(f"provider failed with {self.secret}")
        if self.invalid_response:
            return {"credential": self.secret}
        return "receipt-" + "x" * 96

    async def read(self, operation: str, request: Any) -> Any:
        raise AssertionError("effect adapter must not be invoked through read")


class ReceiptAdapter:
    connector = "receipts"
    connector_version = "v1"
    operations: frozenset[str] = frozenset()
    effect_operations = frozenset({"send"})
    idempotent_effects = effect_operations

    def __init__(self) -> None:
        self.calls = 0

    async def effect(
        self,
        operation: str,
        request: Any,
        *,
        idempotency_key: str,
    ) -> dict[str, Receipt]:
        assert operation == "send"
        assert request and idempotency_key
        self.calls += 1
        return {"primary": Receipt("receipt-1")}


class EffectWorkflow(Workflow[str, str]):
    workflow_id = "effect-workflow"
    input_type = str
    output_type = str

    def __init__(self, effect: EffectSpec[str, str] = SEND_MESSAGE) -> None:
        self.send = Effect(effect)

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.send(value)


class CommitThenFail(Module[str, str]):
    input_type = str
    output_type = str
    effectful = True
    effects = (SEND_MESSAGE,)

    def __init__(self) -> None:
        self.executions = 0

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        result = await ctx.broker.effect(
            SEND_MESSAGE.connector,
            SEND_MESSAGE.operation,
            value,
            connector_version=SEND_MESSAGE.connector_version,
        )
        self.executions += 1
        if self.executions == 1:
            raise RuntimeError("simulated worker failure after durable effect commit")
        return str(result)


class CommitThenFailWorkflow(Workflow[str, str]):
    workflow_id = "commit-then-fail"
    input_type = str
    output_type = str

    def __init__(self) -> None:
        self.send = CommitThenFail()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.send(value)


class NestedCommitThenFail(Module[str, dict[str, Receipt]]):
    input_type = str
    output_type = dict[str, Receipt]
    effectful = True
    effects = (SEND_RECEIPTS,)

    def __init__(self) -> None:
        self.executions = 0

    async def execute(self, value: str, ctx: ExecutionContext) -> dict[str, Receipt]:
        result = cast(
            dict[str, Receipt],
            await ctx.broker.effect(
                SEND_RECEIPTS.connector,
                SEND_RECEIPTS.operation,
                value,
                connector_version=SEND_RECEIPTS.connector_version,
            ),
        )
        assert isinstance(result["primary"], Receipt)
        self.executions += 1
        if self.executions == 1:
            raise RuntimeError("simulated crash after nested effect commit")
        return result


class NestedCommitThenFailWorkflow(Workflow[str, dict[str, Receipt]]):
    workflow_id = "nested-commit-then-fail"
    input_type = str
    output_type = dict[str, Receipt]

    def __init__(self) -> None:
        self.send = NestedCommitThenFail()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[dict[str, Receipt]]:
        return self.send(value)


class ConflictingRetry(Module[str, str]):
    input_type = str
    output_type = str
    effectful = True
    effects = (SEND_MESSAGE,)

    def __init__(self) -> None:
        self.executions = 0

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        self.executions += 1
        request = value if self.executions == 1 else "different-sensitive-request"
        result = await ctx.broker.effect(
            SEND_MESSAGE.connector,
            SEND_MESSAGE.operation,
            request,
            connector_version=SEND_MESSAGE.connector_version,
        )
        if self.executions == 1:
            raise RuntimeError("force retry after commit")
        return str(result)


class ConflictingRetryWorkflow(Workflow[str, str]):
    workflow_id = "conflicting-effect-retry"
    input_type = str
    output_type = str

    def __init__(self) -> None:
        self.send = ConflictingRetry()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.send(value)


class StaticEffectPolicy:
    def __init__(self, decision: PolicyDecision) -> None:
        self.decision = decision
        self.calls = 0

    async def authorize(
        self,
        access: Capability[Any, Any] | EffectSpec[Any, Any],
        request: Any,
        *,
        grant: CapabilityGrant,
        run_id: str,
        task_id: str,
        attempt_id: str,
    ) -> PolicyDecision:
        self.calls += 1
        assert access.name in (*grant.capabilities, *grant.effects)
        assert request
        assert run_id and task_id and attempt_id
        return self.decision


def effect_rows(store: PostgresStore, task_id: str) -> list[dict[str, Any]]:
    with store.connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT * FROM workflow_effect_operations WHERE task_id = %s ORDER BY ordinal",
            (task_id,),
        )
        return list(cursor.fetchall())


def test_effect_registry_resolves_exact_versions_and_validates_idempotency_claims() -> None:
    adapter = MessagingAdapter()
    adapter_contract: EffectAdapter = adapter
    registry = ConnectorRegistry([adapter])

    assert registry.resolve_effect("messaging", "send", connector_version="v1") is adapter_contract
    assert registry.effect_is_idempotent(adapter, "send") is True
    with pytest.raises(AccessContractError, match="not registered"):
        registry.resolve_effect("messaging", "send", connector_version="v2")

    invalid = MessagingAdapter()
    invalid.idempotent_effects = frozenset({"undeclared"})
    with pytest.raises(ValueError, match="declared effect operations"):
        ConnectorRegistry([invalid])


def test_effect_public_contracts_reject_malformed_evidence_and_adapters() -> None:
    with pytest.raises(ValueError, match="request_id"):
        ApprovalEvidence(request_id="", command_id="command")
    with pytest.raises(ValueError, match="command_id"):
        ApprovalEvidence(request_id="request", command_id=" ")
    with pytest.raises(ValueError, match="allowed"):
        PolicyDecision(cast(bool, 1), "policy", "reason")
    with pytest.raises(ValueError, match="ApprovalEvidence"):
        PolicyDecision(True, "policy", "reason", cast(ApprovalEvidence, object()))

    malformed: Any = MessagingAdapter()
    malformed.connector = 7
    with pytest.raises(ValueError, match="connector"):
        ConnectorRegistry([malformed])

    malformed = MessagingAdapter()
    malformed.connector_version = ""
    with pytest.raises(ValueError, match="connector_version"):
        ConnectorRegistry([malformed])

    malformed = MessagingAdapter()
    malformed.effect_operations = ["send"]
    with pytest.raises(ValueError, match="frozensets"):
        ConnectorRegistry([malformed])

    malformed = MessagingAdapter()
    malformed.effect_operations = frozenset()
    with pytest.raises(ValueError, match="at least one"):
        ConnectorRegistry([malformed])

    malformed = MessagingAdapter()
    malformed.idempotent_effects = ["send"]
    with pytest.raises(ValueError, match="idempotent_effects"):
        ConnectorRegistry([malformed])

    malformed = MessagingAdapter()
    malformed.effect_operations = frozenset({cast(str, 7)})
    malformed.idempotent_effects = frozenset()
    with pytest.raises(ValueError, match="stable names"):
        ConnectorRegistry([malformed])

    class MissingEffectHandler:
        connector = "messaging"
        connector_version = "v1"
        operations: frozenset[str] = frozenset()
        effect_operations = frozenset({"send"})
        idempotent_effects = frozenset({"send"})

    with pytest.raises(ValueError, match="provide async effect"):
        ConnectorRegistry([cast(EffectAdapter, MissingEffectHandler())])

    registry = ConnectorRegistry([MessagingAdapter()])
    with pytest.raises(ValueError, match="effect key"):
        registry.register(MessagingAdapter())


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_effect_commits_one_logical_result_with_safe_durable_evidence(
    postgres_store: PostgresStore,
) -> None:
    adapter = MessagingAdapter()
    result = await WorkflowRunner(
        postgres_store,
        connectors=ConnectorRegistry([adapter]),
    ).run(EffectWorkflow(), "private-message", tenant_id="tenant-a")

    history = postgres_store.load_run_history(result.run_id, tenant_id="tenant-a")
    task = history.tasks[0]
    boundary = history.accepted_boundaries[0]
    rows = effect_rows(postgres_store, task.task_id)
    events = [event for event in history.events if event.event_type.startswith("EFFECT_")]

    assert result.output == "receipt-" + "x" * 96
    assert len(adapter.calls) == 1
    assert adapter.calls[0][2].startswith("mwf_")
    assert len(rows) == 1
    assert rows[0]["status"] == "COMMITTED"
    assert rows[0]["attempt_count"] == 1
    assert rows[0]["result_ref"]["storage"] == "artifact"
    assert [event.event_type for event in events] == [
        "EFFECT_ATTEMPTED",
        "EFFECT_COMMITTED",
    ]
    assert "latency_ms" not in events[0].payload
    assert isinstance(events[1].payload["latency_ms"], float)
    assert events[1].payload["latency_ms"] >= 0
    assert [effect.kind for effect in boundary.effects] == [
        EffectKind.ATTEMPTED,
        EffectKind.COMMITTED,
    ]
    assert all(effect.effect_name == SEND_MESSAGE.name for effect in boundary.effects)
    assert all(effect.ordinal == 0 for effect in boundary.effects)
    assert all(effect.idempotency_key == adapter.calls[0][2] for effect in boundary.effects)
    serialized = repr((events, boundary.effects, rows[0]["request_digest"]))
    assert "private-message" not in serialized
    assert adapter.secret not in serialized


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_retry_after_commit_reuses_result_without_second_adapter_call(
    postgres_store: PostgresStore,
) -> None:
    adapter = MessagingAdapter()
    workflow = CommitThenFailWorkflow()

    result = await WorkflowRunner(
        postgres_store,
        connectors=ConnectorRegistry([adapter]),
        max_attempts=2,
    ).run(workflow, "private-message", tenant_id="tenant-a")

    history = postgres_store.load_run_history(result.run_id, tenant_id="tenant-a")
    rows = effect_rows(postgres_store, history.tasks[0].task_id)
    effect_events = [event for event in history.events if event.event_type.startswith("EFFECT_")]
    assert result.output == "receipt-" + "x" * 96
    assert workflow.send.executions == 2
    assert len(adapter.calls) == 1
    assert len(history.attempts) == 2
    assert rows[0]["attempt_count"] == 1
    assert [event.event_type for event in effect_events] == [
        "EFFECT_ATTEMPTED",
        "EFFECT_COMMITTED",
    ]
    assert len(history.accepted_boundaries[0].effects) == 2


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_committed_effect_rehydrates_nested_typed_mappings_after_a_crash(
    postgres_store: PostgresStore,
) -> None:
    adapter = ReceiptAdapter()
    workflow = NestedCommitThenFailWorkflow()

    result = await WorkflowRunner(
        postgres_store,
        connectors=ConnectorRegistry([adapter]),
        max_attempts=2,
    ).run(workflow, "private-message", tenant_id="tenant-a")

    assert result.output == {"primary": Receipt("receipt-1")}
    assert isinstance(result.output["primary"], Receipt)
    assert workflow.send.executions == 2
    assert adapter.calls == 1


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_committed_effect_recovery_needs_no_live_adapter_or_policy(
    postgres_store: PostgresStore,
) -> None:
    adapter = MessagingAdapter()
    workflow = CommitThenFailWorkflow()
    run = WorkflowClient(postgres_store).start(
        workflow,
        "private-message",
        tenant_id="tenant-a",
    )
    scheduler = WorkflowScheduler.resume(
        postgres_store,
        workflow,
        run.run_id,
        tenant_id="tenant-a",
    )
    modules = build_module_registry(workflow, scheduler.plan, output=scheduler.output)
    first_worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=modules,
        worker_id="worker-1",
        connectors=ConnectorRegistry([adapter]),
        max_attempts=2,
    )

    with pytest.raises(RuntimeError, match="after durable effect commit"):
        await first_worker.run_once()

    class ExplodingPolicy:
        async def authorize(self, *_args: Any, **_kwargs: Any) -> PolicyDecision:
            raise AssertionError("committed-result recovery must not invoke policy")

    first_worker.connectors = ConnectorRegistry()
    first_worker.access_policy = ExplodingPolicy()

    boundary = await first_worker.run_once()

    assert boundary is not None
    assert len(adapter.calls) == 1
    assert [effect.kind for effect in boundary.effects] == [
        EffectKind.ATTEMPTED,
        EffectKind.COMMITTED,
    ]
    assert scheduler.advance().status is RunStatus.SUCCEEDED


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_retry_before_commit_reuses_the_same_adapter_idempotency_key(
    postgres_store: PostgresStore,
) -> None:
    adapter = MessagingAdapter(fail_first=True)
    result = await WorkflowRunner(
        postgres_store,
        connectors=ConnectorRegistry([adapter]),
        max_attempts=2,
    ).run(EffectWorkflow(), "private-message", tenant_id="tenant-a")

    history = postgres_store.load_run_history(result.run_id, tenant_id="tenant-a")
    rows = effect_rows(postgres_store, history.tasks[0].task_id)
    assert result.output == "receipt-" + "x" * 96
    assert len(adapter.calls) == 2
    assert adapter.calls[0][2] == adapter.calls[1][2]
    assert rows[0]["status"] == "COMMITTED"
    assert rows[0]["attempt_count"] == 2
    assert [
        event.event_type for event in history.events if event.event_type.startswith("EFFECT_")
    ] == [
        "EFFECT_ATTEMPTED",
        "EFFECT_FAILED",
        "EFFECT_ATTEMPTED",
        "EFFECT_COMMITTED",
    ]


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_effect_adapter_failure_is_retryable_and_sanitized(
    postgres_store: PostgresStore,
) -> None:
    adapter = MessagingAdapter(fail_first=True)

    with pytest.raises(AccessContractError, match="adapter call failed") as raised:
        await WorkflowRunner(
            postgres_store,
            connectors=ConnectorRegistry([adapter]),
            max_attempts=1,
        ).run(EffectWorkflow(), "private-message", tenant_id="tenant-a")

    assert raised.value.retryable is True
    assert raised.value.__cause__ is None
    with postgres_store.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT diagnostic FROM workflow_attempts")
        diagnostic = cursor.fetchone()
        cursor.execute("SELECT payload FROM workflow_events WHERE event_type LIKE 'EFFECT_%'")
        events = cursor.fetchall()
    assert adapter.secret not in repr((raised.value, diagnostic, events))
    assert "private-message" not in repr((diagnostic, events))


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_effect_retry_with_conflicting_request_fails_without_second_call(
    postgres_store: PostgresStore,
) -> None:
    adapter = MessagingAdapter()

    with pytest.raises(AccessContractError, match="different request"):
        await WorkflowRunner(
            postgres_store,
            connectors=ConnectorRegistry([adapter]),
            max_attempts=2,
        ).run(ConflictingRetryWorkflow(), "private-message", tenant_id="tenant-a")

    assert len(adapter.calls) == 1
    with postgres_store.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT status, request_digest FROM workflow_effect_operations")
        row = cursor.fetchone()
    assert row is not None and row["status"] == "COMMITTED"
    assert "private-message" not in repr(row)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_caught_retry_conflict_keeps_committed_ledger_effect_in_boundary(
    postgres_store: PostgresStore,
) -> None:
    class CatchConflictingRetry(Module[str, str]):
        input_type = str
        output_type = str
        effectful = True
        effects = (SEND_MESSAGE,)

        def __init__(self) -> None:
            self.executions = 0

        async def execute(self, value: str, ctx: ExecutionContext) -> str:
            self.executions += 1
            request = value if self.executions == 1 else "different-sensitive-request"
            try:
                await ctx.broker.effect(
                    SEND_MESSAGE.connector,
                    SEND_MESSAGE.operation,
                    request,
                    connector_version=SEND_MESSAGE.connector_version,
                )
            except AccessContractError:
                return "fallback-after-conflict"
            if self.executions == 1:
                raise RuntimeError("retry after committed effect")
            raise AssertionError("conflicting retry was not rejected")

    class CatchConflictWorkflow(Workflow[str, str]):
        workflow_id = "caught-conflicting-effect-retry"
        input_type = str
        output_type = str
        step = CatchConflictingRetry()

        def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
            return self.step(value)

    adapter = MessagingAdapter()
    result = await WorkflowRunner(
        postgres_store,
        connectors=ConnectorRegistry([adapter]),
        max_attempts=2,
    ).run(CatchConflictWorkflow(), "private-message", tenant_id="tenant-a")
    history = postgres_store.load_run_history(result.run_id, tenant_id="tenant-a")

    assert result.output == "fallback-after-conflict"
    assert len(adapter.calls) == 1
    assert [effect.kind for effect in history.accepted_boundaries[0].effects] == [
        EffectKind.ATTEMPTED,
        EffectKind.COMMITTED,
    ]
    assert history.accepted_boundaries[0].effects[1].result_digest is not None


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_non_idempotent_adapter_is_never_called_on_unsafe_retry(
    postgres_store: PostgresStore,
) -> None:
    optional = EffectSpec(
        "messaging.send",
        connector="messaging",
        operation="send",
        input_type=str,
        output_type=str,
        connector_version="v1",
        idempotency=Idempotency.OPTIONAL,
    )
    adapter = MessagingAdapter(idempotent=False, fail_first=True)

    with pytest.raises(AccessContractError, match="unsafe retry"):
        await WorkflowRunner(
            postgres_store,
            connectors=ConnectorRegistry([adapter]),
            max_attempts=2,
        ).run(EffectWorkflow(optional), "private-message", tenant_id="tenant-a")

    assert len(adapter.calls) == 1
    with postgres_store.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT status, attempt_count FROM workflow_effect_operations")
        row = cursor.fetchone()
    assert row is not None
    assert (row["status"], row["attempt_count"]) == ("ATTEMPTED", 1)


@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("adapter", "effect", "policy", "message"),
    [
        (
            MessagingAdapter(idempotent=False),
            SEND_MESSAGE,
            None,
            "requires adapter idempotency",
        ),
        (
            MessagingAdapter(),
            SEND_MESSAGE,
            StaticEffectPolicy(PolicyDecision.deny("deployment", "blocked")),
            "policy denied",
        ),
        (
            MessagingAdapter(),
            EffectSpec(
                "messaging.send",
                connector="messaging",
                operation="send",
                input_type=str,
                output_type=str,
                connector_version="v1",
                approval_required=True,
            ),
            StaticEffectPolicy(PolicyDecision.allow("approval-policy")),
            "approval",
        ),
    ],
)
async def test_effect_idempotency_policy_and_approval_fail_closed_before_adapter(
    postgres_store: PostgresStore,
    adapter: MessagingAdapter,
    effect: EffectSpec[str, str],
    policy: AccessPolicy | None,
    message: str,
) -> None:
    with pytest.raises(AccessContractError, match=message):
        await WorkflowRunner(
            postgres_store,
            connectors=ConnectorRegistry([adapter]),
            access_policy=policy,
            max_attempts=1,
        ).run(EffectWorkflow(effect), "private-message", tenant_id="tenant-a")

    assert adapter.calls == []
    with postgres_store.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) AS count FROM workflow_effect_operations")
        row = cursor.fetchone()
    assert row is not None and row["count"] == 0


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_caught_broker_denial_does_not_create_legacy_synthetic_effects(
    postgres_store: PostgresStore,
) -> None:
    class CatchDeniedEffect(Module[str, str]):
        input_type = str
        output_type = str
        effectful = True
        effects = (SEND_MESSAGE,)

        async def execute(self, value: str, ctx: ExecutionContext) -> str:
            try:
                await ctx.broker.effect(
                    SEND_MESSAGE.connector,
                    SEND_MESSAGE.operation,
                    value,
                    connector_version=SEND_MESSAGE.connector_version,
                )
            except AccessContractError:
                return "denied-safely"
            raise AssertionError("policy denial was not enforced")

    class CatchDeniedWorkflow(Workflow[str, str]):
        workflow_id = "catch-denied-effect"
        input_type = str
        output_type = str
        effect = CatchDeniedEffect()

        def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
            return self.effect(value)

    adapter = MessagingAdapter()
    result = await WorkflowRunner(
        postgres_store,
        connectors=ConnectorRegistry([adapter]),
        access_policy=StaticEffectPolicy(PolicyDecision.deny("deployment", "blocked")),
    ).run(CatchDeniedWorkflow(), "private-message", tenant_id="tenant-a")
    history = postgres_store.load_run_history(result.run_id, tenant_id="tenant-a")

    assert result.output == "denied-safely"
    assert history.accepted_boundaries[0].effects == ()
    assert adapter.calls == []
    assert [
        event.event_type for event in history.events if event.event_type.startswith("EFFECT_")
    ] == ["EFFECT_DENIED"]


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_declared_effect_module_that_skips_access_has_no_synthetic_effect(
    postgres_store: PostgresStore,
) -> None:
    class SkipDeclaredEffect(Module[str, str]):
        input_type = str
        output_type = str
        effectful = True
        effects = (SEND_MESSAGE,)

        async def execute(self, value: str, ctx: ExecutionContext) -> str:
            assert ctx.broker is not None
            return value

    class SkipWorkflow(Workflow[str, str]):
        workflow_id = "skip-declared-effect"
        input_type = str
        output_type = str
        step = SkipDeclaredEffect()

        def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
            return self.step(value)

    result = await WorkflowRunner(postgres_store).run(
        SkipWorkflow(),
        "no-effect",
        tenant_id="tenant-a",
    )
    history = postgres_store.load_run_history(result.run_id, tenant_id="tenant-a")

    assert history.accepted_boundaries[0].effects == ()
    with postgres_store.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) AS count FROM workflow_effect_operations")
        assert cursor.fetchone() == {"count": 0}


@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize("narrow_grant", [False, True], ids=("compiled-grant", "empty-grant"))
async def test_store_discards_forged_managed_effects_without_a_ledger_row(
    postgres_store: PostgresStore,
    monkeypatch: pytest.MonkeyPatch,
    narrow_grant: bool,
) -> None:
    class SkipEffect(Module[str, str]):
        input_type = str
        output_type = str
        effectful = True
        effects = (SEND_MESSAGE,)

        async def execute(self, value: str, ctx: ExecutionContext) -> str:
            assert ctx.broker is not None
            return value

    class SkipEffectWorkflow(Workflow[str, str]):
        workflow_id = "forged-effect-boundary"
        input_type = str
        output_type = str
        step = SkipEffect()

        def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
            return self.step(value)

    original_complete = postgres_store.complete_task
    original_enqueue = postgres_store.enqueue_task

    if narrow_grant:

        def enqueue_with_empty_grant(*args: Any, **kwargs: Any) -> Any:
            kwargs["capability_grant"] = CapabilityGrant()
            return original_enqueue(*args, **kwargs)

        monkeypatch.setattr(postgres_store, "enqueue_task", enqueue_with_empty_grant)

    def complete_with_forged_effect(claim: Any, boundary: Any) -> Any:
        forged = (
            EffectRecord(
                kind=EffectKind.ATTEMPTED,
                adapter="forged",
                operation="send",
                request_digest="forged-request",
                effect_name=SEND_MESSAGE.name,
                ordinal=0,
                idempotency_key="forged-key",
                connector_version="v1",
            ),
            EffectRecord(
                kind=EffectKind.COMMITTED,
                adapter="forged",
                operation="send",
                request_digest="forged-request",
                result_digest="forged-result",
                effect_name=SEND_MESSAGE.name,
                ordinal=0,
                idempotency_key="forged-key",
                connector_version="v1",
            ),
        )
        return original_complete(claim, replace(boundary, effects=forged))

    monkeypatch.setattr(postgres_store, "complete_task", complete_with_forged_effect)
    result = await WorkflowRunner(postgres_store).run(
        SkipEffectWorkflow(),
        "no-effect",
        tenant_id="tenant-a",
    )
    history = postgres_store.load_run_history(result.run_id, tenant_id="tenant-a")

    assert history.accepted_boundaries[0].effects == ()
    with postgres_store.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) AS count FROM workflow_effect_operations")
        assert cursor.fetchone() == {"count": 0}


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_caught_undeclared_broker_effect_has_no_legacy_synthetic_commit(
    postgres_store: PostgresStore,
) -> None:
    class LegacyModuleCatchingDenial(Module[str, str]):
        input_type = str
        output_type = str
        effectful = True

        async def execute(self, value: str, ctx: ExecutionContext) -> str:
            try:
                await ctx.broker.effect("messaging", "send", value, connector_version="v1")
            except AccessContractError:
                return "denied-without-effect"
            raise AssertionError("undeclared broker effect was not denied")

    class LegacyDenialWorkflow(Workflow[str, str]):
        workflow_id = "legacy-effect-denial"
        input_type = str
        output_type = str
        step = LegacyModuleCatchingDenial()

        def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
            return self.step(value)

    result = await WorkflowRunner(postgres_store).run(
        LegacyDenialWorkflow(),
        "private-message",
        tenant_id="tenant-a",
    )
    history = postgres_store.load_run_history(result.run_id, tenant_id="tenant-a")

    assert result.output == "denied-without-effect"
    assert history.accepted_boundaries[0].effects == ()
    assert [
        event.event_type for event in history.events if event.event_type.startswith("EFFECT_")
    ] == ["EFFECT_DENIED"]


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_caught_attempt_failure_cannot_accept_a_task_boundary(
    postgres_store: PostgresStore,
) -> None:
    class CatchAttemptFailure(Module[str, str]):
        input_type = str
        output_type = str
        effectful = True
        effects = (SEND_MESSAGE,)

        async def execute(self, value: str, ctx: ExecutionContext) -> str:
            try:
                await ctx.broker.effect(
                    SEND_MESSAGE.connector,
                    SEND_MESSAGE.operation,
                    value,
                    connector_version=SEND_MESSAGE.connector_version,
                )
            except AccessContractError:
                return "suppressed-provider-failure"
            raise AssertionError("adapter failure was not raised")

    class CatchAttemptFailureWorkflow(Workflow[str, str]):
        workflow_id = "catch-attempt-failure"
        input_type = str
        output_type = str
        effect = CatchAttemptFailure()

        def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
            return self.effect(value)

    adapter = MessagingAdapter(fail_first=True)
    with pytest.raises(PersistenceError, match="uncommitted effect"):
        await WorkflowRunner(
            postgres_store,
            connectors=ConnectorRegistry([adapter]),
            max_attempts=1,
        ).run(CatchAttemptFailureWorkflow(), "private-message", tenant_id="tenant-a")

    with postgres_store.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT task_id, status, accepted_boundary FROM workflow_tasks")
        task = cursor.fetchone()
        cursor.execute("SELECT status, diagnostic FROM workflow_attempts")
        attempt = cursor.fetchone()
        cursor.execute("SELECT status FROM workflow_runs")
        run = cursor.fetchone()
    assert task is not None
    assert task["status"] == "FAILED"
    assert task["accepted_boundary"] is None
    assert attempt is not None
    assert attempt["status"] == "FAILED"
    assert "uncommitted broker-managed effect" in attempt["diagnostic"]["reason"]
    assert run == {"status": "FAILED"}
    rows = effect_rows(postgres_store, str(task["task_id"]))
    assert [row["status"] for row in rows] == ["ATTEMPTED"]


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_boundary_effect_history_is_not_mutable_module_metadata(
    postgres_store: PostgresStore,
) -> None:
    class TamperWithMetadata(Module[str, str]):
        input_type = str
        output_type = str
        effectful = True
        effects = (SEND_MESSAGE,)

        async def execute(self, value: str, ctx: ExecutionContext) -> str:
            result = await ctx.broker.effect(
                SEND_MESSAGE.connector,
                SEND_MESSAGE.operation,
                value,
                connector_version=SEND_MESSAGE.connector_version,
            )
            ctx.metadata["effects"] = [
                {
                    "kind": "EFFECT_COMMITTED",
                    "adapter": "forged",
                    "operation": "forged",
                    "request_digest": "forged",
                    "result_digest": "forged",
                }
            ]
            ctx.metadata["broker_managed_effects"] = False
            return str(result)

    class MetadataWorkflow(Workflow[str, str]):
        workflow_id = "effect-metadata-authority"
        input_type = str
        output_type = str
        effect = TamperWithMetadata()

        def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
            return self.effect(value)

    adapter = MessagingAdapter()
    result = await WorkflowRunner(
        postgres_store,
        connectors=ConnectorRegistry([adapter]),
    ).run(MetadataWorkflow(), "private-message", tenant_id="tenant-a")
    history = postgres_store.load_run_history(result.run_id, tenant_id="tenant-a")
    effects = history.accepted_boundaries[0].effects

    assert [effect.kind for effect in effects] == [
        EffectKind.ATTEMPTED,
        EffectKind.COMMITTED,
    ]
    assert all(effect.adapter == "messaging" for effect in effects)
    assert all(effect.operation == "send" for effect in effects)
    assert all(effect.effect_name == SEND_MESSAGE.name for effect in effects)
    assert all(effect.ordinal == 0 for effect in effects)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_boundary_preserves_cross_effect_reservation_order(
    postgres_store: PostgresStore,
) -> None:
    first = EffectSpec(
        "z.first",
        connector="multi",
        operation="z_first",
        input_type=str,
        output_type=str,
        idempotency=Idempotency.REQUIRED,
    )
    second = EffectSpec(
        "a.second",
        connector="multi",
        operation="a_second",
        input_type=str,
        output_type=str,
        idempotency=Idempotency.REQUIRED,
    )

    class MultiAdapter:
        connector = "multi"
        connector_version = None
        operations: frozenset[str] = frozenset()
        effect_operations = frozenset({"z_first", "a_second"})
        idempotent_effects = effect_operations

        def __init__(self) -> None:
            self.calls: list[str] = []

        async def effect(
            self,
            operation: str,
            request: Any,
            *,
            idempotency_key: str,
        ) -> Any:
            assert request and idempotency_key
            self.calls.append(operation)
            return f"{operation}-receipt"

    class OrderedEffects(Module[str, str]):
        input_type = str
        output_type = str
        effectful = True
        effects = (first, second)

        async def execute(self, value: str, ctx: ExecutionContext) -> str:
            await ctx.broker.effect("multi", "z_first", value)
            await ctx.broker.effect("multi", "a_second", value)
            return "done"

    class OrderedWorkflow(Workflow[str, str]):
        workflow_id = "ordered-effects"
        input_type = str
        output_type = str
        step = OrderedEffects()

        def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
            return self.step(value)

    adapter = MultiAdapter()
    result = await WorkflowRunner(
        postgres_store,
        connectors=ConnectorRegistry([adapter]),
    ).run(OrderedWorkflow(), "private-message", tenant_id="tenant-a")
    history = postgres_store.load_run_history(result.run_id, tenant_id="tenant-a")

    assert adapter.calls == ["z_first", "a_second"]
    assert [effect.effect_name for effect in history.accepted_boundaries[0].effects] == [
        "z.first",
        "z.first",
        "a.second",
        "a.second",
    ]
    with postgres_store.connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT effect_name, reservation_order FROM workflow_effect_operations "
            "ORDER BY reservation_order"
        )
        assert list(cursor.fetchall()) == [
            {"effect_name": "z.first", "reservation_order": 0},
            {"effect_name": "a.second", "reservation_order": 1},
        ]


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_approved_effect_executes_but_response_schema_failure_never_commits(
    postgres_store: PostgresStore,
) -> None:
    approved_effect = EffectSpec(
        "messaging.send",
        connector="messaging",
        operation="send",
        input_type=str,
        output_type=str,
        connector_version="v1",
        approval_required=True,
    )
    request_id = "approve-messaging-send-0"
    command_id = "approve-effect-command"
    approved = StaticEffectPolicy(
        PolicyDecision.allow(
            "approval-policy",
            "approved",
            approval=ApprovalEvidence(request_id=request_id, command_id=command_id),
        )
    )
    adapter = MessagingAdapter(invalid_response=True)
    workflow = EffectWorkflow(approved_effect)
    run = WorkflowClient(postgres_store).start(
        workflow,
        "private-message",
        tenant_id="tenant-a",
    )
    scheduler = WorkflowScheduler.resume(
        postgres_store,
        workflow,
        run.run_id,
        tenant_id="tenant-a",
    )
    modules = build_module_registry(workflow, scheduler.plan, output=scheduler.output)
    parking_worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=modules,
        worker_id="approval-request-worker",
    )
    envelope = parking_worker.claim()
    assert envelope is not None
    envelope = parking_worker.start(envelope)
    parking_worker.park(
        envelope,
        InteractionRequest(
            request_id=request_id,
            kind=InteractionKind.APPROVAL,
            prompt="Send the message?",
            metadata={
                "effect_name": approved_effect.name,
                "ordinal": 0,
                "request_digest": digest_data("private-message"),
            },
        ),
    )
    run.send(ApproveCommand(command_id=command_id, request_id=request_id))
    effect_worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=modules,
        worker_id="effect-worker",
        connectors=ConnectorRegistry([adapter]),
        access_policy=approved,
        max_attempts=2,
    )
    with pytest.raises(AccessContractError, match="response contract"):
        await effect_worker.run_once()

    assert approved.calls == 1
    assert len(adapter.calls) == 1
    with postgres_store.connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT status, result_ref, approval_request_id,
                   approval_command_id, approval_event_id
            FROM workflow_effect_operations
            """
        )
        row = cursor.fetchone()
        cursor.execute("SELECT diagnostic FROM workflow_attempts")
        diagnostic = cursor.fetchone()
    assert row is not None
    assert row["status"] == "ATTEMPTED"
    assert row["result_ref"] is None
    assert row["approval_request_id"] == request_id
    assert row["approval_command_id"] == command_id
    assert row["approval_event_id"] is not None
    assert adapter.secret not in repr(diagnostic)


@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("latest_metadata", "latest_command", "evidence_command_id"),
    [
        (
            {},
            RejectCommand(command_id="new-rejection", request_id="reused-effect-approval"),
            "old-approval",
        ),
        (
            {"ordinal": 1},
            ApproveCommand(
                command_id="new-wrong-effect-approval",
                request_id="reused-effect-approval",
            ),
            "new-wrong-effect-approval",
        ),
        (
            {"effect_name": "different.effect"},
            ApproveCommand(
                command_id="new-wrong-name-approval",
                request_id="reused-effect-approval",
            ),
            "new-wrong-name-approval",
        ),
        (
            {"request_digest": digest_data("different-private-message")},
            ApproveCommand(
                command_id="new-wrong-digest-approval",
                request_id="reused-effect-approval",
            ),
            "new-wrong-digest-approval",
        ),
    ],
)
async def test_latest_approval_cycle_must_match_the_exact_effect_request(
    postgres_store: PostgresStore,
    latest_metadata: dict[str, Any],
    latest_command: RejectCommand | ApproveCommand,
    evidence_command_id: str,
) -> None:
    effect = EffectSpec(
        "messaging.send",
        connector="messaging",
        operation="send",
        input_type=str,
        output_type=str,
        connector_version="v1",
        approval_required=True,
    )
    workflow = EffectWorkflow(effect)
    run = WorkflowClient(postgres_store).start(
        workflow,
        "private-message",
        tenant_id="tenant-a",
    )
    scheduler = WorkflowScheduler.resume(
        postgres_store,
        workflow,
        run.run_id,
        tenant_id="tenant-a",
    )
    modules = build_module_registry(workflow, scheduler.plan, output=scheduler.output)
    parking_worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=modules,
        worker_id="approval-request-worker",
    )
    request_id = "reused-effect-approval"
    metadata = {
        "effect_name": effect.name,
        "ordinal": 0,
        "request_digest": digest_data("private-message"),
    }
    first = parking_worker.claim()
    assert first is not None
    parking_worker.park(
        parking_worker.start(first),
        InteractionRequest(
            request_id=request_id,
            kind=InteractionKind.APPROVAL,
            prompt="Send this message?",
            metadata=metadata,
        ),
    )
    run.send(ApproveCommand(command_id="old-approval", request_id=request_id))
    second = parking_worker.claim()
    assert second is not None
    parking_worker.park(
        parking_worker.start(second),
        InteractionRequest(
            request_id=request_id,
            kind=InteractionKind.APPROVAL,
            prompt="Send this message after review?",
            metadata={**metadata, **latest_metadata},
        ),
    )
    run.send(latest_command)

    adapter = MessagingAdapter()
    effect_worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=modules,
        worker_id="effect-worker",
        connectors=ConnectorRegistry([adapter]),
        access_policy=StaticEffectPolicy(
            PolicyDecision.allow(
                "approval-policy",
                "approved",
                approval=ApprovalEvidence(
                    request_id=request_id,
                    command_id=evidence_command_id,
                ),
            )
        ),
        max_attempts=3,
    )

    with pytest.raises(AccessContractError, match="approval evidence"):
        await effect_worker.run_once()

    assert adapter.calls == []
    with postgres_store.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT status FROM workflow_effect_operations")
        assert cursor.fetchone() == {"status": "RESERVED"}


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_approval_evidence_cannot_cross_run_task_or_tenant_scope(
    postgres_store: PostgresStore,
) -> None:
    effect = EffectSpec(
        "messaging.send",
        connector="messaging",
        operation="send",
        input_type=str,
        output_type=str,
        connector_version="v1",
        approval_required=True,
    )
    source_workflow = EffectWorkflow(effect)
    source_run = WorkflowClient(postgres_store).start(
        source_workflow,
        "private-message",
        tenant_id="tenant-a",
    )
    source_scheduler = WorkflowScheduler.resume(
        postgres_store,
        source_workflow,
        source_run.run_id,
        tenant_id="tenant-a",
    )
    source_worker = TaskWorker(
        postgres_store,
        workflow_id=source_scheduler.plan.workflow_id,
        definition_digest=source_scheduler.plan.digest,
        modules=build_module_registry(
            source_workflow,
            source_scheduler.plan,
            output=source_scheduler.output,
        ),
        worker_id="source-approval-worker",
    )
    request_id = "tenant-scoped-approval"
    command_id = "tenant-a-approval"
    envelope = source_worker.claim()
    assert envelope is not None
    source_worker.park(
        source_worker.start(envelope),
        InteractionRequest(
            request_id=request_id,
            kind=InteractionKind.APPROVAL,
            prompt="Approve only this run?",
            metadata={
                "effect_name": effect.name,
                "ordinal": 0,
                "request_digest": digest_data("private-message"),
            },
        ),
    )
    source_run.send(ApproveCommand(command_id=command_id, request_id=request_id))
    source_run.send(PauseCommand(command_id="pause-source-after-approval"))

    adapter = MessagingAdapter()
    with pytest.raises(AccessContractError, match="approval evidence"):
        await WorkflowRunner(
            postgres_store,
            connectors=ConnectorRegistry([adapter]),
            access_policy=StaticEffectPolicy(
                PolicyDecision.allow(
                    "approval-policy",
                    "approved",
                    approval=ApprovalEvidence(
                        request_id=request_id,
                        command_id=command_id,
                    ),
                )
            ),
            max_attempts=1,
        ).run(EffectWorkflow(effect), "private-message", tenant_id="tenant-b")

    assert adapter.calls == []


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_broker_audit_sink_cannot_forge_approval_control_events(
    postgres_store: PostgresStore,
) -> None:
    effect = EffectSpec(
        "messaging.send",
        connector="messaging",
        operation="send",
        input_type=str,
        output_type=str,
        connector_version="v1",
        approval_required=True,
    )

    class AttemptApprovalForgery(Module[str, str]):
        input_type = str
        output_type = str
        effectful = True
        effects = (effect,)

        async def execute(self, value: str, ctx: ExecutionContext) -> str:
            assert not hasattr(ctx.broker, "audit")
            callback = ctx.broker._audit
            assert callback is not None
            with pytest.raises(PersistenceError, match="rejects non-broker"):
                callback(
                    "APPROVAL_REQUIRED",
                    {
                        "request_id": "forged-approval",
                        "metadata": {
                            "effect_name": effect.name,
                            "ordinal": 0,
                            "request_digest": digest_data(value),
                        },
                    },
                )
            return str(
                await ctx.broker.effect(
                    effect.connector,
                    effect.operation,
                    value,
                    connector_version=effect.connector_version,
                )
            )

    class ForgeryWorkflow(Workflow[str, str]):
        workflow_id = "broker-approval-forgery"
        input_type = str
        output_type = str
        step = AttemptApprovalForgery()

        def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
            return self.step(value)

    adapter = MessagingAdapter()
    with pytest.raises(AccessContractError, match="approval evidence"):
        await WorkflowRunner(
            postgres_store,
            connectors=ConnectorRegistry([adapter]),
            access_policy=StaticEffectPolicy(
                PolicyDecision.allow(
                    "approval-policy",
                    "approved",
                    approval=ApprovalEvidence(
                        request_id="forged-approval",
                        command_id="forged-command",
                    ),
                )
            ),
            max_attempts=1,
        ).run(ForgeryWorkflow(), "private-message", tenant_id="tenant-a")

    assert adapter.calls == []
    with postgres_store.connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT count(*) AS count FROM workflow_events
            WHERE event_type IN (
                'APPROVAL_REQUIRED', 'APPROVAL_RESOLVED', 'COMMAND_RECEIVED'
            )
            """
        )
        assert cursor.fetchone() == {"count": 0}


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_missing_effect_adapter_and_request_schema_fail_before_reservation(
    postgres_store: PostgresStore,
) -> None:
    with pytest.raises(AccessContractError, match="not registered"):
        await WorkflowRunner(
            postgres_store,
            connectors=ConnectorRegistry(),
            max_attempts=1,
        ).run(EffectWorkflow(), "private-message", tenant_id="tenant-a")

    class WrongRequest(Module[int, str]):
        input_type = int
        output_type = str
        effectful = True
        effects = (SEND_MESSAGE,)

        async def execute(self, value: int, ctx: ExecutionContext) -> str:
            return str(
                await ctx.broker.effect(
                    SEND_MESSAGE.connector,
                    SEND_MESSAGE.operation,
                    value,
                    connector_version=SEND_MESSAGE.connector_version,
                )
            )

    class WrongRequestWorkflow(Workflow[int, str]):
        workflow_id = "wrong-effect-request"
        input_type = int
        output_type = str
        send = WrongRequest()

        def build(self, value: RuntimeValue[int]) -> RuntimeValue[str]:
            return self.send(value)

    adapter = MessagingAdapter()
    with pytest.raises(AccessContractError, match="request contract"):
        await WorkflowRunner(
            postgres_store,
            connectors=ConnectorRegistry([adapter]),
            max_attempts=1,
        ).run(WrongRequestWorkflow(), 7, tenant_id="tenant-b")
    assert adapter.calls == []


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_cancellation_invalidates_effect_commit_lease(
    postgres_store: PostgresStore,
) -> None:
    run: WorkflowRun | None = None

    def cancel_during_effect() -> None:
        assert run is not None
        run.send(CancelCommand(command_id="cancel-during-effect", reason="stop"))

    adapter = MessagingAdapter(on_effect=cancel_during_effect)
    workflow = EffectWorkflow()
    run = WorkflowClient(postgres_store).start(workflow, "private-message", tenant_id="tenant-a")
    scheduler = WorkflowScheduler.resume(
        postgres_store,
        workflow,
        run.run_id,
        tenant_id="tenant-a",
    )
    worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=build_module_registry(workflow, scheduler.plan, output=scheduler.output),
        worker_id="worker-1",
        connectors=ConnectorRegistry([adapter]),
        max_attempts=1,
    )

    with pytest.raises(StaleLeaseError):
        await worker.run_once()

    history = postgres_store.load_run_history(run.run_id, tenant_id="tenant-a")
    rows = effect_rows(postgres_store, history.tasks[0].task_id)
    assert history.run.status is RunStatus.CANCELLED
    assert history.tasks[0].status is TaskStatus.CANCELLED
    assert rows[0]["status"] == "ATTEMPTED"
    assert not any(event.event_type == "EFFECT_COMMITTED" for event in history.events)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_pause_during_effect_keeps_the_running_attempt_committable(
    postgres_store: PostgresStore,
) -> None:
    run: WorkflowRun | None = None

    def pause_during_effect() -> None:
        assert run is not None
        run.send(PauseCommand(command_id="pause-during-effect", reason="inspect"))

    adapter = MessagingAdapter(on_effect=pause_during_effect)
    workflow = EffectWorkflow()
    run = WorkflowClient(postgres_store).start(workflow, "private-message", tenant_id="tenant-a")
    scheduler = WorkflowScheduler.resume(
        postgres_store,
        workflow,
        run.run_id,
        tenant_id="tenant-a",
    )
    worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=build_module_registry(workflow, scheduler.plan, output=scheduler.output),
        worker_id="worker-1",
        connectors=ConnectorRegistry([adapter]),
        max_attempts=1,
    )

    boundary = await worker.run_once()

    assert boundary is not None
    assert len(adapter.calls) == 1
    paused = postgres_store.load_run_history(run.run_id, tenant_id="tenant-a")
    assert paused.run.status is RunStatus.PAUSED
    assert paused.tasks[0].status is TaskStatus.SUCCEEDED
    assert effect_rows(postgres_store, paused.tasks[0].task_id)[0]["status"] == "COMMITTED"

    run.send(ResumeCommand(command_id="resume-after-effect"))
    assert scheduler.advance().status is RunStatus.SUCCEEDED
