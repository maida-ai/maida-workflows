from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from maida.plan_contract import PlanValidationIssue  # type: ignore[import-untyped]
from maida.policy import load_policy  # type: ignore[import-untyped]

from maida.workflows import (
    Budget,
    Capability,
    CapabilityGrant,
    Connector,
    ConnectorRegistry,
    Effect,
    EffectSpec,
    ExecutionContext,
    Idempotency,
    Module,
    ModuleRegistry,
    PlanBoundary,
    PlanFragmentIR,
    PlanLimits,
    PlanNode,
    PlanSignature,
    PlanValidationError,
    PlanValidator,
    RunStatus,
    WorkflowRunner,
)
from maida.workflows._canonical import schema_digest
from maida.workflows.dynamic import _plan_from_signature
from maida.workflows.fixture import ReplayFixtureExporter, load_fixture
from maida.workflows.guardrail import PlanGuardrailError
from maida.workflows.persistence import (
    InvalidRunStateError,
    PersistenceError,
    PostgresStore,
    blank_boundary,
)
from maida.workflows.runtime import RuntimeContractError, _bootstrap_plan

NODE_BUDGET = Budget(
    wall_time=timedelta(seconds=1),
    model_tokens=0,
    tool_calls=0,
    cost_usd=0.0,
)
TOOL_BUDGET = Budget(
    wall_time=timedelta(seconds=1),
    model_tokens=0,
    tool_calls=1,
    cost_usd=0.0,
)
CONTEXT = Capability(
    "records.context.read",
    "local-records",
    "context",
    str,
    str,
    connector_version="1",
)
DELIVER = EffectSpec(
    "messages.deliver",
    "local-records",
    "deliver",
    str,
    str,
    connector_version="1",
    idempotency=Idempotency.REQUIRED,
)


class Normalize(Module[str, str]):
    module_id = "demo.normalize"
    input_type = str
    output_type = str
    budget = NODE_BUDGET

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value.upper()


class Draft(Module[str, str]):
    module_id = "demo.draft"
    input_type = str
    output_type = str
    budget = NODE_BUDGET

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return f"draft:{value}"


class Audit(Module[tuple[str, str], str]):
    module_id = "demo.audit"
    input_type = tuple[str, str]
    output_type = str
    budget = NODE_BUDGET

    async def execute(self, value: tuple[str, str], ctx: ExecutionContext) -> str:
        return " | ".join(value)


def context_module() -> Module[Any, Any]:
    module = Connector(CONTEXT)
    module.budget = TOOL_BUDGET
    return module


def deliver_module() -> Module[Any, Any]:
    module = Effect(DELIVER)
    module.budget = TOOL_BUDGET
    return module


REGISTRY = ModuleRegistry(
    modules={
        "text.normalize": Normalize,
        "text.draft": Draft,
        "text.audit": Audit,
        "records.context": context_module,
        "messages.deliver": deliver_module,
    }
)
BOUNDARY = PlanBoundary(
    REGISTRY,
    PlanLimits(
        max_nodes=8,
        max_depth=5,
        max_fanout=3,
        budget=Budget(
            wall_time=timedelta(seconds=5),
            model_tokens=0,
            tool_calls=2,
            cost_usd=0.0,
        ),
    ),
    region_id="request-plan",
    output_type=str,
    region_grant=CapabilityGrant(
        capabilities=(CONTEXT.name,),
        effects=(DELIVER.name,),
    ),
)


def brief_plan() -> PlanFragmentIR:
    return PlanFragmentIR(
        "brief-plan",
        (
            PlanNode("normalize", "text.normalize", ("$input",)),
            PlanNode("draft", "text.draft", ("normalize",)),
        ),
        ("draft",),
    )


def thorough_plan() -> PlanFragmentIR:
    return PlanFragmentIR(
        "thorough-plan",
        (
            PlanNode("normalize", "text.normalize", ("$input",)),
            PlanNode("context", "records.context", ("$input",)),
            PlanNode("draft", "text.draft", ("normalize",)),
            PlanNode("audit", "text.audit", ("draft", "context")),
            PlanNode("deliver", "messages.deliver", ("audit",)),
        ),
        ("deliver",),
    )


class Planner(Module[str, dict[str, Any]]):
    module_id = "demo.planner"
    input_type = str
    output_type = dict[str, Any]
    plan_boundary = BOUNDARY

    async def execute(self, value: str, ctx: ExecutionContext) -> dict[str, Any]:
        selected = thorough_plan() if "thorough" in value else brief_plan()
        return selected.to_dict()


class RejectedPlanner(Planner):
    module_id = "demo.rejected-planner"

    async def execute(self, value: str, ctx: ExecutionContext) -> dict[str, Any]:
        return PlanFragmentIR(
            "rejected-plan",
            (PlanNode("unsafe", "untrusted.shell", ("$input",)),),
            ("unsafe",),
        ).to_dict()


class ExplodingPlanner(Planner):
    module_id = "demo.exploding-planner"

    async def execute(self, value: str, ctx: ExecutionContext) -> dict[str, Any]:
        raise ValueError("planner exploded")


class Fail(Module[str, str]):
    module_id = "demo.fail"
    input_type = str
    output_type = str
    budget = NODE_BUDGET

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        raise ValueError("generated child exploded")


FAIL_REGISTRY = ModuleRegistry(modules={"fail": Fail})
FAIL_BOUNDARY = PlanBoundary(
    FAIL_REGISTRY,
    PlanLimits(max_nodes=1, max_depth=1, max_fanout=1, budget=NODE_BUDGET),
    region_id="failing-plan",
    output_type=str,
)


class FailingChildPlanner(Planner):
    module_id = "demo.failing-child-planner"
    plan_boundary = FAIL_BOUNDARY

    async def execute(self, value: str, ctx: ExecutionContext) -> dict[str, Any]:
        return PlanFragmentIR(
            "failing-child-plan",
            (PlanNode("fail", "fail", ("$input",)),),
            ("fail",),
        ).to_dict()


class MissingBoundaryPlanner(Module[str, dict[str, Any]]):
    module_id = "demo.missing-boundary-planner"
    input_type = str
    output_type = dict[str, Any]

    async def execute(self, value: str, ctx: ExecutionContext) -> dict[str, Any]:
        return brief_plan().to_dict()


class BlankIdPlanner(Planner):
    module_id = ""


class LocalAdapter:
    connector = "local-records"
    connector_version = "1"
    operations = frozenset({"context"})
    effect_operations = frozenset({"deliver"})
    idempotent_effects = frozenset({"deliver"})

    async def read(self, operation: str, request: Any) -> Any:
        assert operation == "context"
        return f"context:{request}"

    async def effect(self, operation: str, request: Any, *, idempotency_key: str) -> Any:
        assert operation == "deliver"
        assert idempotency_key
        return f"delivered:{request}"


def signature(history: Any) -> PlanSignature:
    event = next(event for event in history.events if event.event_type == "PLAN_MATERIALIZED")
    return PlanSignature.from_dict(event.payload["signature"])


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_standalone_modules_execute_from_a_generated_plan(
    postgres_store: PostgresStore,
) -> None:
    result = await WorkflowRunner(postgres_store).run_generated(Planner(), "brief request")
    history = postgres_store.load_run_history(result.run_id, tenant_id="local")
    generated_ids = {task.module_id for task in history.tasks if task.plan_provenance is not None}

    assert result.output == "draft:BRIEF REQUEST"
    assert generated_ids == {Normalize.module_id, Draft.module_id}


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_generated_root_plan_varies_executes_and_exports_verifiable_history(
    postgres_store: PostgresStore,
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(
        postgres_store,
        connectors=ConnectorRegistry((LocalAdapter(),)),
    )

    brief = await runner.run_generated(Planner(), "brief request")
    thorough = await runner.run_generated(Planner(), "thorough request")

    assert brief.output == "draft:BRIEF REQUEST"
    assert thorough.output == ("delivered:draft:THOROUGH REQUEST | context:thorough request")
    brief_history = postgres_store.load_run_history(brief.run_id, tenant_id="local")
    thorough_history = postgres_store.load_run_history(thorough.run_id, tenant_id="local")
    brief_signature = signature(brief_history)
    thorough_signature = signature(thorough_history)
    assert (brief_signature.node_count, brief_signature.max_depth, brief_signature.max_fanout) == (
        2,
        2,
        1,
    )
    assert (
        thorough_signature.node_count,
        thorough_signature.max_depth,
        thorough_signature.max_fanout,
    ) == (5, 4, 2)
    assert brief_signature.source_fragment_digest != thorough_signature.source_fragment_digest
    assert brief_signature.required_grant == CapabilityGrant()
    assert thorough_signature.required_grant == CapabilityGrant(
        capabilities=(CONTEXT.name,),
        effects=(DELIVER.name,),
    )

    source = next(task for task in thorough_history.tasks if task.plan_provenance is None)
    generated = tuple(task for task in thorough_history.tasks if task.plan_provenance is not None)
    assert source.module_id == Planner.module_id
    assert all(
        cast(Any, task.plan_provenance).parent_task_id == source.task_id for task in generated
    )
    assert thorough_history.definition.workflow_id == "dynamic:request-plan"
    assert thorough_history.definition.digest == thorough.definition_digest
    assert thorough_history.run.definition_digest == thorough.definition_digest
    assert thorough_history.run.status is RunStatus.SUCCEEDED
    approved = next(
        event for event in thorough_history.events if event.event_type == "PLAN_APPROVED"
    )
    assert approved.payload["checked_before_execution"] is True
    assert approved.payload["artifact"]["artifact_id"]
    proved = next(
        event for event in thorough_history.events if event.event_type == "PLAN_EXECUTION_VERIFIED"
    )
    assert proved.payload == {
        "artifact_id": approved.payload["artifact"]["artifact_id"],
        "region_instance_id": "request-plan-root",
    }

    fixture = ReplayFixtureExporter(postgres_store.values).export(
        thorough_history,
        tmp_path / "thorough-fixture",
    )
    restored = load_fixture(tmp_path / "thorough-fixture")
    assert restored.digest == fixture.digest
    assert restored.workflow_ir.digest == thorough.definition_digest
    assert restored.generated_plans[0].signature == thorough_signature


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_generated_root_plan_rejects_unknown_alias_before_child_insertion(
    postgres_store: PostgresStore,
) -> None:
    runner = WorkflowRunner(postgres_store)

    with pytest.raises(PlanValidationError) as captured:
        await runner.run_generated(RejectedPlanner(), "request")

    assert captured.value.code == "PLAN_MODULE_NOT_ALLOWED"
    with postgres_store.connect() as connection:
        row = connection.execute(
            "SELECT run_id FROM workflow_runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    assert row is not None
    history = postgres_store.load_run_history(str(row["run_id"]), tenant_id="local")
    assert history.run.status is RunStatus.FAILED
    assert not any(task.plan_provenance is not None for task in history.tasks)
    failed = next(event for event in history.events if event.event_type == "RUN_FAILED")
    assert failed.payload["code"] == "PLAN_MODULE_NOT_ALLOWED"


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_generated_root_plan_is_refused_by_core_policy_before_child_insertion(
    postgres_store: PostgresStore,
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        "version: 2.1\nmetrics:\n  plan_fanout: {kind: measured, direction: upper, limit: 1}\n",
        encoding="utf-8",
    )
    runner = WorkflowRunner(postgres_store)

    with pytest.raises(PlanGuardrailError) as captured:
        await runner.run_generated(
            Planner(),
            "thorough request",
            policy=load_policy(policy_path),
        )

    assert captured.value.code == "PLAN_FANOUT_EXCEEDED"
    assert str(captured.value) == (
        "PLAN REFUSED: PLAN_FANOUT_EXCEEDED\n"
        "Plan fan-out is 2; policy allows at most 1 (plan_fanout)."
    )
    with postgres_store.connect() as connection:
        row = connection.execute(
            "SELECT run_id FROM workflow_runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    assert row is not None
    history = postgres_store.load_run_history(str(row["run_id"]), tenant_id="local")
    assert history.run.status is RunStatus.FAILED
    assert not any(task.plan_provenance is not None for task in history.tasks)
    refused = next(event for event in history.events if event.event_type == "PLAN_REJECTED")
    assert refused.payload["valid"] is False
    assert refused.payload["issues"][0]["code"] == "PLAN_FANOUT_EXCEEDED"
    failed = next(event for event in history.events if event.event_type == "RUN_FAILED")
    assert failed.payload["code"] == "PLAN_FANOUT_EXCEEDED"


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_generated_root_plan_fails_closed_when_execution_diverges(
    postgres_store: PostgresStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issue = PlanValidationIssue(
        code="PLAN_EXECUTION_DIVERGENCE",
        message="Executed module did not match the approved plan.",
        location="execution.nodes.normalize",
    )
    monkeypatch.setattr(
        "maida.workflows.guardrail.PlanGuardrail.verify_execution",
        lambda *_args, **_kwargs: (issue,),
    )

    with pytest.raises(RuntimeError, match="did not match the approved plan") as captured:
        await WorkflowRunner(postgres_store).run_generated(Planner(), "brief request")

    assert cast(Any, captured.value).code == "PLAN_EXECUTION_DIVERGENCE"
    with postgres_store.connect() as connection:
        row = connection.execute(
            "SELECT run_id FROM workflow_runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    assert row is not None
    history = postgres_store.load_run_history(str(row["run_id"]), tenant_id="local")
    assert history.run.status is RunStatus.FAILED
    divergence = next(
        event for event in history.events if event.event_type == "PLAN_EXECUTION_DIVERGED"
    )
    assert divergence.payload["issues"] == [issue.to_dict()]
    failed = next(event for event in history.events if event.event_type == "RUN_FAILED")
    assert failed.payload["code"] == "PLAN_EXECUTION_DIVERGENCE"


def test_plan_boundary_rejects_untrusted_marker_configuration() -> None:
    with pytest.raises(TypeError, match="registry"):
        PlanBoundary(cast(Any, object()), BOUNDARY.limits, "region", str)
    with pytest.raises(TypeError, match="limits"):
        PlanBoundary(REGISTRY, cast(Any, object()), "region", str)
    with pytest.raises(ValueError, match="region_id"):
        PlanBoundary(REGISTRY, BOUNDARY.limits, "not a stable region", str)
    with pytest.raises(TypeError, match="region_grant"):
        PlanBoundary(
            REGISTRY,
            BOUNDARY.limits,
            "region",
            str,
            region_grant=cast(Any, object()),
        )
    with pytest.raises(TypeError, match="registry"):
        PlanValidator(
            cast(Any, object()),
            BOUNDARY.limits,
            region_id="region",
            region_grant=CapabilityGrant(),
        )
    with pytest.raises(TypeError, match="limits"):
        PlanValidator(
            REGISTRY,
            cast(Any, object()),
            region_id="region",
            region_grant=CapabilityGrant(),
        )
    with pytest.raises(TypeError, match="approval_check"):
        PlanValidator(
            REGISTRY,
            BOUNDARY.limits,
            region_id="region",
            region_grant=CapabilityGrant(),
            approval_check=cast(Any, object()),
        )

    validator = PlanValidator(
        REGISTRY,
        BOUNDARY.limits,
        region_id="region",
        region_grant=CapabilityGrant(),
    )
    with pytest.raises(PlanValidationError, match="fragment must be PlanFragmentIR"):
        validator.validate(
            cast(Any, {}),
            region_input_schema_digest=schema_digest(str),
            expected_output_schema_digests=(schema_digest(str),),
        )


@pytest.mark.asyncio
async def test_generated_runner_requires_trusted_marker_input_and_identity() -> None:
    runner = WorkflowRunner(cast(Any, None))

    with pytest.raises(RuntimeContractError, match="trusted PlanBoundary"):
        await runner.run_generated(MissingBoundaryPlanner(), "request")
    with pytest.raises(RuntimeContractError, match="input contract"):
        await runner.run_generated(Planner(), cast(Any, 1))
    with pytest.raises(RuntimeContractError, match="declare a non-empty module_id"):
        await runner.run_generated(BlankIdPlanner(), "request")


@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("planner", "message"),
    [
        (ExplodingPlanner(), "planner exploded"),
        (FailingChildPlanner(), "generated child exploded"),
    ],
)
async def test_generated_runner_records_execution_failures(
    postgres_store: PostgresStore,
    planner: Planner,
    message: str,
) -> None:
    runner = WorkflowRunner(postgres_store, max_attempts=1)

    with pytest.raises(ValueError, match=message):
        await runner.run_generated(planner, "request")

    with postgres_store.connect() as connection:
        row = connection.execute(
            "SELECT run_id FROM workflow_runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    assert row is not None
    history = postgres_store.load_run_history(str(row["run_id"]), tenant_id="local")
    assert history.run.status is RunStatus.FAILED
    failed = next(event for event in history.events if event.event_type == "RUN_FAILED")
    assert failed.payload == {"exception_type": "ValueError", "reason": message}


@pytest.mark.postgres
def test_generated_definition_adoption_requires_the_accepted_bootstrap(
    postgres_store: PostgresStore,
) -> None:
    planner = Planner()
    bootstrap = _bootstrap_plan(planner)
    fragment = brief_plan()
    validator = PlanValidator(
        BOUNDARY.registry,
        BOUNDARY.limits,
        region_id=BOUNDARY.region_id,
        region_grant=BOUNDARY.region_grant,
    )
    plan = _plan_from_signature(
        validator.validate(
            fragment,
            region_input_schema_digest=schema_digest(str),
            expected_output_schema_digests=(schema_digest(str),),
        )
    )

    with pytest.raises(PersistenceError, match="was not found"):
        postgres_store.adopt_run_definition(
            "00000000-0000-0000-0000-000000000001",
            plan,
            expected_definition_digest=bootstrap.digest,
            source_task_id="00000000-0000-0000-0000-000000000002",
        )

    encoded_input = postgres_store.values.encode("request", schema_digest=schema_digest(str))
    run = postgres_store.create_run(bootstrap, tenant_id="local", root_input=encoded_input)
    with pytest.raises(InvalidRunStateError, match="accepted bootstrap"):
        postgres_store.adopt_run_definition(
            run.run_id,
            plan,
            expected_definition_digest=bootstrap.digest,
            source_task_id="00000000-0000-0000-0000-000000000002",
        )

    step = bootstrap.executable_steps[0]
    task = postgres_store.enqueue_task(
        run.run_id,
        step,
        step_instance_id="planner",
        input_value=encoded_input,
    )
    claim = postgres_store.claim_task(worker_id="test-worker", task_id=task.task_id)
    assert claim is not None
    claim = postgres_store.start_task(claim)
    encoded_fragment = postgres_store.values.encode(
        fragment.to_dict(),
        schema_digest=schema_digest(planner.output_type),
    )
    boundary = blank_boundary(
        workflow_id=bootstrap.workflow_id,
        definition_digest=bootstrap.digest,
        claim=claim,
        input_value=encoded_input,
        output_value=encoded_fragment,
    )
    postgres_store.complete_task(claim, boundary)

    with pytest.raises(InvalidRunStateError, match="changed before"):
        postgres_store.adopt_run_definition(
            run.run_id,
            plan,
            expected_definition_digest="wrong-bootstrap",
            source_task_id=task.task_id,
        )
    definition = postgres_store.adopt_run_definition(
        run.run_id,
        plan,
        expected_definition_digest=bootstrap.digest,
        source_task_id=task.task_id,
    )
    assert (
        postgres_store.adopt_run_definition(
            run.run_id,
            plan,
            expected_definition_digest=bootstrap.digest,
            source_task_id=task.task_id,
        )
        == definition
    )

    encoded_output = postgres_store.values.encode("done", schema_digest=schema_digest(str))
    postgres_store.complete_run(run.run_id, encoded_output)
    with pytest.raises(InvalidRunStateError, match="must be running"):
        postgres_store.adopt_run_definition(
            run.run_id,
            plan,
            expected_definition_digest=bootstrap.digest,
            source_task_id=task.task_id,
        )
