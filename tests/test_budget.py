from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
from typing import Any

import pytest

from maida.workflows import (
    Budget,
    ExecutionContext,
    Module,
    RuntimeValue,
    TaskEnvelope,
    Workflow,
    compile_workflow,
)
from maida.workflows._canonical import digest_data, schema_digest
from maida.workflows.alignment import DiffKind, GraphAligner
from maida.workflows.ir import PlanIR
from maida.workflows.persistence import InvalidRunStateError, PostgresStore


class BudgetedIdentity(Module[int, int]):
    input_type = int
    output_type = int

    def __init__(self, budget: Budget) -> None:
        self.budget = budget

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return value


class BudgetedWorkflow(Workflow[int, int]):
    workflow_id = "budgeted-workflow"
    input_type = int
    output_type = int

    def __init__(self, budget: Budget) -> None:
        self.identity = BudgetedIdentity(budget)

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.identity(value)


@pytest.mark.parametrize(
    ("arguments", "message"),
    (
        ({"wall_time": -timedelta(milliseconds=1)}, "wall_time"),
        ({"wall_time": timedelta(microseconds=1)}, "millisecond precision"),
        ({"model_tokens": -1}, "model_tokens"),
        ({"tool_calls": -1}, "tool_calls"),
        ({"cost_usd": -0.01}, "cost_usd"),
        ({"cost_usd": float("inf")}, "finite"),
        ({"cost_usd": float("nan")}, "finite"),
    ),
)
def test_budget_rejects_negative_lossy_or_nonfinite_values(
    arguments: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        Budget(**arguments)


@pytest.mark.parametrize(
    "arguments",
    (
        {"wall_time": 1},
        {"model_tokens": 1.5},
        {"model_tokens": True},
        {"tool_calls": 1.5},
        {"tool_calls": False},
        {"cost_usd": "1.00"},
        {"cost_usd": True},
    ),
)
def test_budget_rejects_values_with_ambiguous_types(arguments: dict[str, Any]) -> None:
    with pytest.raises(TypeError):
        Budget(**arguments)


def test_budget_is_immutable_and_round_trips_through_canonical_wire_data() -> None:
    budget = Budget(
        wall_time=timedelta(seconds=12, milliseconds=345),
        model_tokens=10_000,
        tool_calls=12,
        cost_usd=0.75,
    )

    assert budget.to_data() == {
        "cost_usd": 0.75,
        "model_tokens": 10_000,
        "tool_calls": 12,
        "wall_time_ms": 12_345,
    }
    assert Budget.from_data(budget.to_data()) == budget
    assert Budget().to_data() == {
        "cost_usd": None,
        "model_tokens": None,
        "tool_calls": None,
        "wall_time_ms": None,
    }
    with pytest.raises(FrozenInstanceError):
        budget.model_tokens = 20_000  # type: ignore[misc]
    with pytest.raises(ValueError, match="fields"):
        Budget.from_data({"model_tokens": 10})
    with pytest.raises(ValueError, match="fields"):
        Budget.from_data({**budget.to_data(), "usage": 1})
    with pytest.raises(TypeError, match="wall_time_ms"):
        Budget.from_data({**Budget().to_data(), "wall_time_ms": 1.5})
    with pytest.raises(ValueError, match="wall_time_ms"):
        Budget.from_data({**Budget().to_data(), "wall_time_ms": -1})
    with pytest.raises(ValueError, match="timedelta range"):
        Budget.from_data({**Budget().to_data(), "wall_time_ms": 10**30})


def test_compiler_rejects_a_non_budget_module_declaration() -> None:
    workflow = BudgetedWorkflow(Budget())
    workflow.identity.budget = {"model_tokens": 10}  # type: ignore[assignment]

    with pytest.raises(ValueError, match="module budget must be a Budget"):
        compile_workflow(workflow)


def test_budget_changes_content_identity_and_has_a_specific_structural_diff() -> None:
    source = compile_workflow(BudgetedWorkflow(Budget(model_tokens=1_000)))
    current = compile_workflow(BudgetedWorkflow(Budget(model_tokens=2_000)))
    source_step = source.executable_steps[0]
    current_step = current.executable_steps[0]

    assert source.version == "0.3.0"
    assert source_step.replay_key == current_step.replay_key
    assert source_step.budget == Budget(model_tokens=1_000).to_data()
    assert source_step.module_digest != current_step.module_digest
    assert source_step.definition_digest != current_step.definition_digest
    assert [change.kind for change in GraphAligner().align(source, current).diff.changes] == [
        DiffKind.MODULE_DIGEST_CHANGED,
        DiffKind.BUDGET_CHANGED,
    ]


def test_current_budget_ir_round_trips_and_rejects_missing_or_extra_usage_fields() -> None:
    plan = compile_workflow(BudgetedWorkflow(Budget(tool_calls=4)))
    data = plan.to_dict()

    assert PlanIR.from_dict(data).to_dict() == data

    missing = plan.to_dict()
    missing["steps"][0].pop("budget")
    with pytest.raises(ValueError, match="require a budget field"):
        PlanIR.from_dict(missing)

    usage_in_declaration = plan.to_dict()
    usage_in_declaration["steps"][0]["budget"]["usage"] = 1
    with pytest.raises(ValueError, match=r"budget is invalid.*fields"):
        PlanIR.from_dict(usage_in_declaration)


def test_legacy_missing_budget_aligns_with_the_current_unbounded_default() -> None:
    current = compile_workflow(BudgetedWorkflow(Budget()))
    legacy = replace(
        current,
        version="0.2.0",
        steps=tuple(replace(step, budget=None) for step in current.steps),
    )

    assert GraphAligner().align(legacy, current).diff.changes == ()


LEGACY_STEP = {
    "control": None,
    "definition_digest": "definition",
    "dependencies": ["input"],
    "execution": None,
    "input_binding": {"schema_digest": "input-schema", "source": "input"},
    "kind": "module",
    "logical_step": "root",
    "module_digest": "module",
    "module_id": "legacy.step",
    "node_id": "root",
    "output_schema_digest": "output-schema",
}


@pytest.mark.parametrize(
    ("version", "step_fields", "expected_json", "expected_digest"),
    (
        (
            "0.1.0",
            {},
            '{"input_schema":{"type":"integer"},"output_node":"root",'
            '"output_schema":{"type":"integer"},"steps":[{"control":null,'
            '"definition_digest":"definition","dependencies":["input"],'
            '"execution":null,"input_binding":{"schema_digest":"input-schema",'
            '"source":"input"},"kind":"module","logical_step":"root",'
            '"module_digest":"module","module_id":"legacy.step","node_id":"root",'
            '"output_schema_digest":"output-schema"}],"version":"0.1.0",'
            '"workflow_id":"legacy"}',
            "515ac63d7b3088397ce0bbd0b07f85bebd1703aa3d2928f0f6d7478ba07cf155",
        ),
        (
            "0.2.0",
            {"capabilities": [], "effects": []},
            '{"input_schema":{"type":"integer"},"output_node":"root",'
            '"output_schema":{"type":"integer"},"steps":[{"capabilities":[],'
            '"control":null,"definition_digest":"definition","dependencies":["input"],'
            '"effects":[],"execution":null,"input_binding":{'
            '"schema_digest":"input-schema","source":"input"},"kind":"module",'
            '"logical_step":"root","module_digest":"module",'
            '"module_id":"legacy.step","node_id":"root",'
            '"output_schema_digest":"output-schema"}],"version":"0.2.0",'
            '"workflow_id":"legacy"}',
            "d1bd918214343fa05536269bbd3bdf5d992b7f9ed6b40100d450256a38120c55",
        ),
    ),
)
def test_legacy_workflow_ir_keeps_exact_bytes_and_digests(
    version: str,
    step_fields: dict[str, Any],
    expected_json: str,
    expected_digest: str,
) -> None:
    data = {
        "version": version,
        "workflow_id": "legacy",
        "input_schema": {"type": "integer"},
        "output_schema": {"type": "integer"},
        "steps": [{**LEGACY_STEP, **step_fields}],
        "output_node": "root",
    }

    loaded = PlanIR.from_dict(data)

    assert loaded.to_dict() == data
    assert loaded.canonical_json() == expected_json
    assert loaded.digest == expected_digest == digest_data(data)
    assert loaded.executable_steps[0].budget is None


@pytest.mark.parametrize("version", ("0.1.0", "0.2.0"))
def test_legacy_workflow_ir_rejects_budget_fields(version: str) -> None:
    step: dict[str, Any] = dict(LEGACY_STEP)
    if version == "0.2.0":
        step.update(capabilities=[], effects=[])
    step["budget"] = Budget().to_data()
    data = {
        "version": version,
        "workflow_id": "legacy",
        "input_schema": {"type": "integer"},
        "output_schema": {"type": "integer"},
        "steps": [step],
        "output_node": "root",
    }

    with pytest.raises(ValueError, match="does not define budget"):
        PlanIR.from_dict(data)


@pytest.mark.postgres
def test_task_and_worker_envelope_preserve_the_budget_declaration(
    postgres_store: PostgresStore,
) -> None:
    budget = Budget(
        wall_time=timedelta(seconds=30),
        model_tokens=2_000,
        tool_calls=8,
        cost_usd=0.5,
    )
    plan = compile_workflow(BudgetedWorkflow(budget))
    value = postgres_store.values.encode(1, schema_digest=schema_digest(int))
    run = postgres_store.create_run(plan, tenant_id="tenant-a", root_input=value)
    task = postgres_store.enqueue_task(
        run.run_id,
        plan.executable_steps[0],
        step_instance_id="singleton",
        input_value=value,
    )
    changed_step = replace(
        plan.executable_steps[0],
        budget=Budget(model_tokens=4_000).to_data(),
    )
    with pytest.raises(InvalidRunStateError, match="different budget declaration"):
        postgres_store.enqueue_task(
            run.run_id,
            changed_step,
            step_instance_id="singleton",
            input_value=value,
        )
    claim = postgres_store.claim_task(worker_id="worker-a", task_id=task.task_id)

    assert claim is not None
    assert task.budget == budget
    assert claim.task.budget == budget
    assert TaskEnvelope.from_claim(claim).to_data()["budget"] == budget.to_data()
    history = postgres_store.load_run_history(run.run_id, tenant_id="tenant-a")
    assert history.tasks[0].budget == budget


def test_replacing_a_step_budget_does_not_misclassify_usage_as_a_declaration() -> None:
    source = compile_workflow(BudgetedWorkflow(Budget(cost_usd=0.25)))
    step = source.executable_steps[0]
    changed = replace(step, budget=Budget(cost_usd=0.5).to_data())
    current = replace(
        source,
        steps=tuple(changed if candidate is step else candidate for candidate in source.steps),
    )

    changes = GraphAligner().align(source, current).diff.changes

    assert [change.kind for change in changes] == [DiffKind.BUDGET_CHANGED]
    assert changes[0].location.endswith(".budget")
