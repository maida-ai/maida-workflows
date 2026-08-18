from __future__ import annotations

from dataclasses import dataclass

import pytest

from maida.workflows import (
    BindingSpec,
    ExecutionContext,
    Module,
    ModuleRegistry,
    NodeSpec,
    PlanIR,
    WorkflowRunner,
    WorkflowSpec,
    compile_workflow_spec,
)
from maida.workflows._canonical import type_schema
from maida.workflows.ir import IR_VERSION
from maida.workflows.persistence import PostgresStore


class Normalize(Module[int, int]):
    module_id = "math.normalize"
    input_type = int
    output_type = int

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return max(1, value)


@dataclass(frozen=True)
class MessageInput:
    name: str
    count: int


class Message(Module[MessageInput, str]):
    module_id = "text.message"
    input_type = MessageInput
    output_type = str

    async def execute(self, value: MessageInput, ctx: ExecutionContext) -> str:
        return f"Hello, {value.name} x{value.count}"


class IsUrgent(Module[dict[str, object], bool]):
    module_id = "routing.is-urgent"
    input_type = dict[str, object]
    output_type = bool

    async def execute(self, value: dict[str, object], ctx: ExecutionContext) -> bool:
        return bool(value["urgent"])


class Urgent(Module[dict[str, object], str]):
    module_id = "routing.urgent"
    input_type = dict[str, object]
    output_type = str

    async def execute(self, value: dict[str, object], ctx: ExecutionContext) -> str:
        return "urgent"


class Normal(Urgent):
    async def execute(self, value: dict[str, object], ctx: ExecutionContext) -> str:
        return "normal"


class ReadItem(Module[dict[str, object], str]):
    module_id = "items.read"
    input_type = dict[str, object]
    output_type = str

    async def execute(self, value: dict[str, object], ctx: ExecutionContext) -> str:
        return str(value["id"])


def registry() -> ModuleRegistry:
    return ModuleRegistry(
        modules={
            "number.normalize": Normalize,
            "text.message": Message,
            "ticket.is_urgent": IsUrgent,
            "ticket.urgent": Urgent,
            "ticket.normal": Normal,
            "item.read": ReadItem,
        }
    )


def greeting_spec() -> WorkflowSpec:
    return WorkflowSpec(
        workflow_id="spec-greeting",
        input_schema={
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "count": {"type": "integer"},
            },
            "required": ["count", "name"],
            "additionalProperties": False,
        },
        output_schema=type_schema(str),
        nodes=(
            NodeSpec.task(
                "normalize",
                "number.normalize",
                BindingSpec.root("count"),
            ),
            NodeSpec.task(
                "message",
                "text.message",
                BindingSpec.object(
                    name=BindingSpec.root("name"),
                    count=BindingSpec.node("normalize"),
                ),
            ),
        ),
        output=BindingSpec.node("message"),
    )


def test_workflow_spec_round_trip_schema_and_explanation_are_deterministic() -> None:
    spec = greeting_spec()
    restored = WorkflowSpec.from_dict(spec.to_dict())
    compilation = compile_workflow_spec(restored, registry())

    assert restored.canonical_json() == spec.canonical_json()
    assert WorkflowSpec.json_schema()["properties"]["nodes"]["type"] == "array"
    assert compilation.ok
    assert compilation.plan is not None
    assert compilation.plan.version == IR_VERSION
    assert PlanIR.from_dict(compilation.plan.to_dict()) == compilation.plan
    assert compilation.explanation.node_count == 2
    assert compilation.explanation.edges == (
        ("input", "message"),
        ("input", "normalize"),
        ("normalize", "message"),
    )
    assert "number.normalize" in compilation.explanation.render_text()


def test_declaration_order_does_not_change_spec_or_plan_identity() -> None:
    first = greeting_spec()
    second = WorkflowSpec(
        workflow_id=first.workflow_id,
        input_schema=first.input_schema,
        output_schema=first.output_schema,
        nodes=tuple(reversed(first.nodes)),
        output=first.output,
    )

    compiled_first = compile_workflow_spec(first, registry()).raise_for_errors()
    compiled_second = compile_workflow_spec(second, registry()).raise_for_errors()

    assert first.digest == second.digest
    assert compiled_first.plan.digest == compiled_second.plan.digest


def test_validation_reports_unknown_modules_cycles_schema_errors_and_secrets() -> None:
    unknown = WorkflowSpec(
        workflow_id="unknown",
        input_schema=type_schema(str),
        output_schema=type_schema(str),
        nodes=(NodeSpec.task("step", "missing", BindingSpec.root()),),
        output=BindingSpec.node("step"),
    )
    cycle = WorkflowSpec(
        workflow_id="cycle",
        input_schema=type_schema(str),
        output_schema=type_schema(str),
        nodes=(
            NodeSpec.task("a", "text.message", BindingSpec.node("b")),
            NodeSpec.task("b", "text.message", BindingSpec.node("a")),
        ),
        output=BindingSpec.node("a"),
    )
    mismatch = WorkflowSpec(
        workflow_id="mismatch",
        input_schema=type_schema(str),
        output_schema=type_schema(int),
        nodes=(NodeSpec.task("step", "number.normalize", BindingSpec.root()),),
        output=BindingSpec.node("step"),
    )
    secret = WorkflowSpec(
        workflow_id="secret",
        input_schema=type_schema(str),
        output_schema=type_schema(str),
        nodes=(
            NodeSpec.task("step", "text.message", BindingSpec.root(), config={"api_key": "no"}),
        ),
        output=BindingSpec.node("step"),
    )

    assert {issue.code for issue in compile_workflow_spec(unknown, registry()).issues} == {
        "UNKNOWN_MODULE"
    }
    assert "CYCLE" in {issue.code for issue in compile_workflow_spec(cycle, registry()).issues}
    assert "SCHEMA_MISMATCH" in {
        issue.code for issue in compile_workflow_spec(mismatch, registry()).issues
    }
    assert "SECRET_LITERAL" in {
        issue.code for issue in compile_workflow_spec(secret, registry()).issues
    }


def test_branch_spec_compiles_to_lazy_control_region() -> None:
    spec = WorkflowSpec(
        workflow_id="spec-branch",
        input_schema={
            "type": "object",
            "properties": {"urgent": {"type": "boolean"}},
            "required": ["urgent"],
            "additionalProperties": False,
        },
        output_schema=type_schema(str),
        nodes=(
            NodeSpec.task("check", "ticket.is_urgent", BindingSpec.root()),
            NodeSpec.task("urgent", "ticket.urgent", BindingSpec.root()),
            NodeSpec.task("normal", "ticket.normal", BindingSpec.root()),
            NodeSpec.branch("choice", "check", "urgent", "normal"),
        ),
        output=BindingSpec.node("choice"),
    )

    compilation = compile_workflow_spec(spec, registry())

    assert compilation.ok
    assert compilation.plan is not None
    choice = next(step for step in compilation.plan.steps if step.node_id == "nodes/choice")
    assert choice.kind == "when"
    assert choice.dependencies == ("nodes/check", "nodes/urgent", "nodes/normal")


def test_nested_spec_is_flattened_with_hierarchical_replay_identity() -> None:
    child = greeting_spec()
    parent = WorkflowSpec(
        workflow_id="spec-parent",
        input_schema={
            "type": "object",
            "properties": {"request": dict(child.input_schema)},
            "required": ["request"],
            "additionalProperties": False,
        },
        output_schema=type_schema(str),
        nodes=(NodeSpec.nested("greeting", child, BindingSpec.root("request")),),
        output=BindingSpec.node("greeting"),
    )

    restored = WorkflowSpec.from_dict(parent.to_dict())
    compilation = compile_workflow_spec(restored, registry())

    assert compilation.ok
    assert compilation.plan is not None
    assert {step.logical_step for step in compilation.plan.executable_steps} == {
        "nodes/greeting/nodes/normalize",
        "nodes/greeting/nodes/message",
    }
    assert all(step.node_id.startswith("nodes/greeting/") for step in compilation.plan.steps)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_spec_authored_workflow_executes_on_the_same_durable_runtime(
    postgres_store: PostgresStore,
) -> None:
    bound = compile_workflow_spec(greeting_spec(), registry()).raise_for_errors()

    result = await WorkflowRunner(postgres_store).run(bound, {"name": "Ada", "count": 0})

    assert result.output == "Hello, Ada x1"


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_nested_spec_executes_without_a_parent_worker_call_stack(
    postgres_store: PostgresStore,
) -> None:
    child = greeting_spec()
    parent = WorkflowSpec(
        workflow_id="spec-parent-runtime",
        input_schema={
            "type": "object",
            "properties": {"request": dict(child.input_schema)},
            "required": ["request"],
            "additionalProperties": False,
        },
        output_schema=type_schema(str),
        nodes=(NodeSpec.nested("greeting", child, BindingSpec.root("request")),),
        output=BindingSpec.node("greeting"),
    )
    bound = compile_workflow_spec(parent, registry()).raise_for_errors()

    result = await WorkflowRunner(postgres_store).run(
        bound, {"request": {"name": "Lin", "count": 3}}
    )

    assert result.output == "Hello, Lin x3"


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_spec_map_uses_stable_item_keys_for_distributed_instances(
    postgres_store: PostgresStore,
) -> None:
    item_schema = {
        "type": "object",
        "properties": {"id": {"type": "string"}, "value": {"type": "integer"}},
        "required": ["id", "value"],
        "additionalProperties": False,
    }
    spec = WorkflowSpec(
        workflow_id="spec-map",
        input_schema={"type": "array", "items": item_schema},
        output_schema={"type": "array", "items": {"type": "string"}},
        nodes=(NodeSpec.map("read", "item.read", BindingSpec.root(), item_key="id"),),
        output=BindingSpec.node("read"),
    )
    bound = compile_workflow_spec(spec, registry()).raise_for_errors()

    result = await WorkflowRunner(postgres_store).run(
        bound, [{"id": "b", "value": 2}, {"id": "a", "value": 1}]
    )

    assert result.output == ["b", "a"]


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_spec_branch_executes_only_the_selected_distributed_path(
    postgres_store: PostgresStore,
) -> None:
    spec = WorkflowSpec(
        workflow_id="spec-branch-runtime",
        input_schema={
            "type": "object",
            "properties": {"urgent": {"type": "boolean"}},
            "required": ["urgent"],
            "additionalProperties": False,
        },
        output_schema=type_schema(str),
        nodes=(
            NodeSpec.task("check", "ticket.is_urgent", BindingSpec.root()),
            NodeSpec.task("urgent", "ticket.urgent", BindingSpec.root()),
            NodeSpec.task("normal", "ticket.normal", BindingSpec.root()),
            NodeSpec.branch("choice", "check", "urgent", "normal"),
        ),
        output=BindingSpec.node("choice"),
    )
    bound = compile_workflow_spec(spec, registry()).raise_for_errors()

    result = await WorkflowRunner(postgres_store).run(bound, {"urgent": True})
    history = postgres_store.load_run_history(result.run_id, tenant_id="local")

    assert result.output == "urgent"
    assert {boundary.logical_step for boundary in history.accepted_boundaries} == {
        "nodes/check",
        "nodes/urgent",
    }
