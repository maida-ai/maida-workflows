from __future__ import annotations

from typing import ClassVar

import pytest

from maida.workflows import (
    BoundWorkflow,
    ExecutionContext,
    Module,
    RuntimeValue,
    Workflow,
    WorkflowRunner,
    bind_workflow,
    when,
)
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
