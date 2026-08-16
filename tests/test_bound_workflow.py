from __future__ import annotations

from dataclasses import replace
from typing import Any, ClassVar, cast

import pytest

from maida.workflows import (
    BindingSpec,
    BoundWorkflow,
    ExecutionContext,
    Module,
    ModuleRegistry,
    NodeSpec,
    RuntimeValue,
    Workflow,
    WorkflowRunner,
    WorkflowSpec,
    bind_workflow,
    compile_workflow_spec,
    when,
)
from maida.workflows._canonical import type_schema
from maida.workflows.ir import ReplayKey
from maida.workflows.persistence import PostgresStore


class IsPositive(Module[int, bool]):
    input_type = int
    output_type = bool

    async def execute(self, value: int, ctx: ExecutionContext) -> bool:
        return value > 0


class Offset(Module[int, int]):
    input_type = int
    output_type = int

    def __init__(self, offset: int) -> None:
        self.offset = offset

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return value + self.offset


class CountingWorkflow(Workflow[int, int]):
    workflow_id = "bound-counting"
    input_type = int
    output_type = int
    builds: ClassVar[int] = 0

    def __init__(self) -> None:
        self.check = IsPositive()
        self.positive = Offset(10)
        self.negative = Offset(-10)

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        type(self).builds += 1
        return when(self.check(value), self.positive(value), self.negative(value))


def test_bind_workflow_compiles_once_and_freezes_exact_modules() -> None:
    CountingWorkflow.builds = 0

    bound = bind_workflow(CountingWorkflow())

    assert isinstance(bound, BoundWorkflow)
    assert CountingWorkflow.builds == 1
    assert bound.plan.workflow_id == "bound-counting"
    assert len(bound.modules) == 3
    assert tuple(bound.modules) == tuple(step.replay_key for step in bound.plan.executable_steps)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_bound_workflow_executes_compiled_graph_without_rebuilding(
    postgres_store: PostgresStore,
) -> None:
    CountingWorkflow.builds = 0
    bound = bind_workflow(CountingWorkflow())

    positive = await WorkflowRunner(postgres_store).run(bound, 2)
    negative = await WorkflowRunner(postgres_store).run(bound, -2)

    assert positive.output == 12
    assert negative.output == -12
    assert CountingWorkflow.builds == 1


def test_bound_workflow_rejects_a_module_that_no_longer_matches_the_plan() -> None:
    bound = bind_workflow(CountingWorkflow())
    modules = dict(bound.modules)
    key = next(key for key in modules if key.module_id.endswith("positive"))
    modules[key] = Offset(11)

    with pytest.raises(ValueError, match="module digest"):
        BoundWorkflow(
            plan=bound.plan,
            input_type=bound.input_type,
            output_type=bound.output_type,
            modules=modules,
        )


def test_bound_workflow_validates_root_schemas_and_exact_module_keys() -> None:
    bound = bind_workflow(CountingWorkflow())
    modules = dict(bound.modules)

    with pytest.raises(ValueError, match="input type"):
        replace(bound, input_type=str)
    with pytest.raises(ValueError, match="output type"):
        replace(bound, output_type=str)
    missing = dict(modules)
    missing.pop(next(iter(missing)))
    with pytest.raises(ValueError, match="missing"):
        replace(bound, modules=missing)
    extra = {
        **modules,
        ReplayKey("extra.module", "extra-step"): Offset(1),
    }
    with pytest.raises(ValueError, match="extra"):
        replace(bound, modules=extra)
    invalid = dict(modules)
    invalid[next(iter(invalid))] = cast(Any, object())
    with pytest.raises(TypeError, match="Module instance"):
        replace(bound, modules=invalid)


def test_bound_workflow_validates_every_pinned_step_contract() -> None:
    bound = bind_workflow(CountingWorkflow())
    target = bound.plan.executable_steps[0]
    target_key = cast(ReplayKey, target.replay_key)

    def changed_plan(**changes: Any) -> Any:
        step = replace(target, **changes)
        return replace(
            bound.plan,
            steps=tuple(
                step if item.node_id == target.node_id else item for item in bound.plan.steps
            ),
        )

    with pytest.raises(ValueError, match="no input binding"):
        replace(bound, plan=changed_plan(input_binding=None))
    assert target.input_binding is not None
    wrong_binding = replace(target.input_binding, schema_digest="0" * 64)
    with pytest.raises(ValueError, match="input schema"):
        replace(bound, plan=changed_plan(input_binding=wrong_binding))
    with pytest.raises(ValueError, match="output schema"):
        replace(bound, plan=changed_plan(output_schema_digest="0" * 64))
    with pytest.raises(ValueError, match="model declarations"):
        replace(bound, plan=changed_plan(models=({"name": "changed"},)))

    assert bound.modules[target_key] is not None
    with pytest.raises(ValueError, match="map item-key"):
        replace(bound, map_item_keys={"missing-map": "id"})


def test_schema_bound_workflow_validates_values_without_python_root_types() -> None:
    spec = WorkflowSpec(
        "schema-bound",
        type_schema(str),
        type_schema(str),
        (NodeSpec.task("echo", "echo", BindingSpec.root()),),
        BindingSpec.node("echo"),
    )
    bound = compile_workflow_spec(spec, ModuleRegistry(modules={"echo": lambda: Offset(1)})).bound
    assert bound is None

    class Echo(Module[str, str]):
        input_type = str
        output_type = str

        async def execute(self, value: str, ctx: ExecutionContext) -> str:
            return value

    compiled = compile_workflow_spec(spec, ModuleRegistry(modules={"echo": Echo}))
    schema_bound = compiled.raise_for_errors()

    assert schema_bound.input_type is Any
    assert schema_bound.accepts_input("value")
    assert not schema_bound.accepts_input(1)
    assert schema_bound.accepts_output("value")
    assert not schema_bound.accepts_output(1)
