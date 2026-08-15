from __future__ import annotations

from dataclasses import dataclass

import pytest

from maida_workflows import (
    ExecutionContext,
    Module,
    RuntimeValue,
    Workflow,
    compile_workflow,
    map_over,
    when,
)
from maida_workflows._canonical import schema_digest
from maida_workflows.models import AttemptStatus, RunHistory, RunStatus
from maida_workflows.persistence import PostgresStore
from maida_workflows.runtime import TaskWorker, WorkflowRunner


class FlakyIncrement(Module[int, int]):
    input_type = int
    output_type = int

    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("transient failure")
        ctx.metadata["usage"] = {"input_tokens": 2, "output_tokens": 1, "cost_usd": 0.01}
        return value + 1


class FlakyWorkflow(Workflow[int, int]):
    workflow_id = "flaky"
    input_type = int
    output_type = int
    increment = FlakyIncrement()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.increment(value)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_runtime_retries_and_accepts_only_one_logical_result(
    postgres_store: PostgresStore,
) -> None:
    workflow = FlakyWorkflow()
    result = await WorkflowRunner(postgres_store, max_attempts=2).run(workflow, 4)
    history = postgres_store.load_run_history(result.run_id, tenant_id="local")

    assert result.output == 5
    assert workflow.increment.calls == 2
    assert history.run.status is RunStatus.SUCCEEDED
    assert [attempt.status for attempt in history.attempts] == [
        AttemptStatus.FAILED,
        AttemptStatus.SUCCEEDED,
    ]
    assert len(history.accepted_boundaries) == 1
    boundary = history.accepted_boundaries[0]
    assert boundary.accepted_attempt.attempt_number == 2
    assert boundary.usage.input_tokens == 2


@dataclass(frozen=True)
class Item:
    item_id: str
    value: int


class ReadItem(Module[Item, int]):
    input_type = Item
    output_type = int

    async def execute(self, value: Item, ctx: ExecutionContext) -> int:
        return value.value


class MappedWorkflow(Workflow[list[Item], list[int]]):
    workflow_id = "mapped-runtime"
    input_type = list[Item]
    output_type = list[int]
    read = ReadItem()

    def build(self, value: RuntimeValue[list[Item]]) -> RuntimeValue[list[int]]:
        return map_over(value, self.read, item_key="item_id")


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_mapped_step_instances_follow_item_keys_not_positions(
    postgres_store: PostgresStore,
) -> None:
    runner = WorkflowRunner(postgres_store)
    first = await runner.run(MappedWorkflow(), [Item("a", 1), Item("b", 2)])
    second = await runner.run(MappedWorkflow(), [Item("b", 2), Item("a", 1)])
    first_history = postgres_store.load_run_history(first.run_id, tenant_id="local")
    second_history = postgres_store.load_run_history(second.run_id, tenant_id="local")

    def instances_by_value(history: RunHistory) -> dict[str, str]:
        return {
            postgres_store.values.decode(boundary.input_value)["item_id"]: boundary.step_instance_id
            for boundary in history.accepted_boundaries
        }

    assert first.output == [1, 2]
    assert second.output == [2, 1]
    assert instances_by_value(first_history) == instances_by_value(second_history)
    assert any(event.event_type == "MAP_DECISION" for event in first_history.events)


class IsPositive(Module[int, bool]):
    input_type = int
    output_type = bool

    async def execute(self, value: int, ctx: ExecutionContext) -> bool:
        return value > 0


class CountingBranch(Module[int, int]):
    input_type = int
    output_type = int

    def __init__(self, offset: int) -> None:
        self.offset = offset
        self.calls = 0

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        self.calls += 1
        return value + self.offset


class BranchWorkflow(Workflow[int, int]):
    workflow_id = "branch-runtime"
    input_type = int
    output_type = int
    check = IsPositive()
    positive = CountingBranch(10)
    negative = CountingBranch(-10)

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return when(self.check(value), self.positive(value), self.negative(value))


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_runtime_records_selected_branch_and_never_runs_the_other(
    postgres_store: PostgresStore,
) -> None:
    workflow = BranchWorkflow()
    result = await WorkflowRunner(postgres_store).run(workflow, 2)
    history = postgres_store.load_run_history(result.run_id, tenant_id="local")

    assert result.output == 12
    assert workflow.positive.calls == 1
    assert workflow.negative.calls == 0
    assert len(history.accepted_boundaries) == 2
    chosen = next(
        boundary
        for boundary in history.accepted_boundaries
        if boundary.module_id.endswith("positive")
    )
    assert chosen.branch_decisions == ({"control_node": "root", "selected": "true"},)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_recovery_worker_uses_persisted_input_without_replay_resolution(
    postgres_store: PostgresStore,
) -> None:
    workflow = BranchWorkflow()
    plan = compile_workflow(workflow)
    value = postgres_store.values.encode(1, schema_digest=schema_digest(int))
    run = postgres_store.create_run(plan, tenant_id="local", root_input=value)
    step = next(
        item for item in plan.executable_steps if item.module_id == "branch-runtime.positive"
    )
    task = postgres_store.enqueue_task(
        run.run_id,
        step,
        step_instance_id="recovered",
        input_value=value,
    )
    assert step.replay_key is not None
    worker = TaskWorker(
        postgres_store,
        workflow_id=plan.workflow_id,
        definition_digest=plan.digest,
        modules={step.replay_key: workflow.positive},
        worker_id="recovery-only",
    )
    boundary = await worker.run_once(task_id=task.task_id)

    assert boundary is not None
    assert postgres_store.values.decode(boundary.output_value) == 11
    postgres_store.complete_run(run.run_id, boundary.output_value)
