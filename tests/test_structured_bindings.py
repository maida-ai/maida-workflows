from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import pytest

from maida.workflows import (
    ExecutionContext,
    Module,
    PlanIR,
    RuntimeValue,
    Workflow,
    WorkflowRunner,
    compile_workflow,
    when,
)
from maida.workflows.alignment import DiffKind, GraphAligner
from maida.workflows.fixture import ReplayFixtureExporter
from maida.workflows.ir import IR_VERSION
from maida.workflows.persistence import PostgresStore
from maida.workflows.replay import ReplayCase, ReplayEngine, ReplayMode, ReplayStatus


class NoTraceBridge:
    async def trace[OutputT](
        self, name: str, callback: Callable[[], Awaitable[OutputT]]
    ) -> tuple[OutputT, None]:
        return await callback(), None


@dataclass(frozen=True)
class Request:
    name: str
    count: int


@dataclass(frozen=True)
class GreetingInput:
    name: str
    normalized_count: int
    punctuation: str


class Normalize(Module[int, int]):
    module_id = "number.normalize"
    input_type = int
    output_type = int

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return max(value, 1)


class Greet(Module[GreetingInput, str]):
    module_id = "text.greet"
    input_type = GreetingInput
    output_type = str

    async def execute(self, value: GreetingInput, ctx: ExecutionContext) -> str:
        return f"Hello, {value.name} x{value.normalized_count}{value.punctuation}"


class StructuredWorkflow(Workflow[Request, str]):
    workflow_id = "structured-bindings"
    input_type = Request
    output_type = str

    def __init__(self) -> None:
        self.normalize = Normalize()
        self.greet = Greet()

    def build(self, value: RuntimeValue[Request]) -> RuntimeValue[str]:
        count = self.normalize(value.count)
        return self.greet(
            name=value.field("name"),
            normalized_count=count,
            punctuation="!",
        )


@dataclass(frozen=True)
class RoutedRequest:
    approved: bool
    urgent: bool
    message: str


class CopyMessage(Module[str, str]):
    module_id = "text.copy"
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value


class ProjectedConditionWorkflow(Workflow[RoutedRequest, str]):
    workflow_id = "projected-condition"
    input_type = RoutedRequest
    output_type = str
    condition_field = "approved"

    def __init__(self) -> None:
        self.approved = CopyMessage()
        self.rejected = CopyMessage()

    def build(self, value: RuntimeValue[RoutedRequest]) -> RuntimeValue[str]:
        return when(
            value.field(self.condition_field),
            self.approved.at("approved")(value.message),
            self.rejected.at("rejected")(value.message),
        )


class ChangedProjectedConditionWorkflow(ProjectedConditionWorkflow):
    condition_field = "urgent"


def test_keyword_bindings_compile_to_canonical_reconstructable_ir() -> None:
    plan = compile_workflow(StructuredWorkflow())
    restored = PlanIR.from_dict(plan.to_dict())
    greet = next(
        step
        for step in plan.executable_steps
        if step.module_id is not None and step.module_id.endswith("greet")
    )
    normalize = next(
        step
        for step in plan.executable_steps
        if step.module_id is not None and step.module_id.endswith("normalize")
    )

    assert plan.version == IR_VERSION
    assert restored.canonical_json() == plan.canonical_json()
    assert greet.input_binding is not None
    assert greet.input_binding.kind == "object"
    assert [name for name, _ in greet.input_binding.fields] == [
        "name",
        "normalized_count",
        "punctuation",
    ]
    assert set(greet.dependencies) == {"input", normalize.node_id}


def test_runtime_value_field_projection_is_typed_and_attribute_sugar_matches() -> None:
    value = RuntimeValue.input(Request)

    assert value.field("name").value_type is str
    assert value.count.value_type is int
    with pytest.raises(ValueError, match="has no field"):
        value.field("missing")


def test_field_projection_can_drive_a_when_condition() -> None:
    plan = compile_workflow(ProjectedConditionWorkflow())
    branch = next(step for step in plan.steps if step.kind == "when")

    assert branch.input_binding is not None
    assert branch.input_binding.kind == "field"
    assert branch.input_binding.path == ("approved",)
    assert PlanIR.from_dict(plan.to_dict()).canonical_json() == plan.canonical_json()


def test_changing_a_projected_when_condition_is_control_flow_divergence() -> None:
    source = compile_workflow(ProjectedConditionWorkflow())
    current = compile_workflow(ChangedProjectedConditionWorkflow())

    divergence = GraphAligner().align(source, current).diff.first_divergence

    assert divergence is not None
    assert divergence.kind is DiffKind.CONTROL_FLOW_CHANGED


def test_keyword_binding_rejects_missing_unknown_and_mismatched_fields() -> None:
    module = Greet()
    root = RuntimeValue.input(Request)

    with pytest.raises(TypeError, match="missing required fields"):
        module(name=root.name, normalized_count=root.count)
    with pytest.raises(TypeError, match="unknown fields"):
        module(
            name=root.name,
            normalized_count=root.count,
            punctuation="!",
            extra="no",
        )
    with pytest.raises(TypeError, match="punctuation"):
        module(name=root.name, normalized_count=root.count, punctuation=3)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_structured_binding_executes_across_durable_task_handoffs(
    postgres_store: PostgresStore,
) -> None:
    result = await WorkflowRunner(postgres_store).run(
        StructuredWorkflow(), Request(name="Ada", count=0)
    )

    assert result.output == "Hello, Ada x1!"


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_projected_condition_routes_across_durable_task_handoffs(
    postgres_store: PostgresStore,
) -> None:
    result = await WorkflowRunner(postgres_store).run(
        ProjectedConditionWorkflow(),
        RoutedRequest(approved=False, urgent=True, message="change"),
    )

    plan = compile_workflow(ProjectedConditionWorkflow())
    history = postgres_store.load_run_history(result.run_id, tenant_id="local")
    rejected_step = next(step for step in plan.executable_steps if step.logical_step == "rejected")
    task = next(task for task in history.tasks if task.logical_step == rejected_step.logical_step)
    assert task.status.value == "SUCCEEDED"


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_structured_binding_full_stub_replay_reconstructs_exact_handoff(
    postgres_store: PostgresStore,
) -> None:
    workflow = StructuredWorkflow()
    result = await WorkflowRunner(postgres_store).run(workflow, Request(name="Grace", count=2))
    history = postgres_store.load_run_history(result.run_id, tenant_id="local")
    fixture = ReplayFixtureExporter(postgres_store.values).project(history)

    replayed = await ReplayEngine(trace_bridge=NoTraceBridge()).replay(
        workflow, ReplayCase(fixture, ReplayMode.FULL_STUB)
    )

    assert replayed.status is ReplayStatus.PASS
    assert replayed.output == "Hello, Grace x2!"
