from __future__ import annotations

import time
from datetime import timedelta

import pytest

from maida.workflows import (
    ExecutionContext,
    ExecutionSpec,
    ExecutorCapabilities,
    LocalExecutor,
    Module,
    RuntimeValue,
    TaskStatus,
    Workflow,
    WorkflowRunner,
    WorkflowScheduler,
    compile_workflow,
    map_over,
    parallel,
)
from maida.workflows.models import AttemptStatus, RunStatus
from maida.workflows.persistence import PostgresStore, StaleLeaseError
from maida.workflows.replay import build_module_registry
from maida.workflows.runtime import RuntimeContractError, RuntimeExecutionError, TaskWorker

IMAGE = "maida/test-worker@sha256:" + "a" * 64
LOCK = "sha256:" + "b" * 64
VM_EXECUTION = ExecutionSpec(
    isolation="vm",
    image=IMAGE,
    dependency_lock=LOCK,
    cpu=2,
    memory="1GiB",
)
VM_CAPABILITIES = ExecutorCapabilities(
    isolations=frozenset({"vm"}),
    images=frozenset({IMAGE}),
    cpu=4,
    memory="8GiB",
)


class AddOne(Module[int, int]):
    module_id = "distributed.add-one"
    input_type = int
    output_type = int
    execution = VM_EXECUTION

    def __init__(self) -> None:
        self._calls = 0

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        self._calls += 1
        return value + 1


class AddTwo(Module[int, int]):
    module_id = "distributed.add-two"
    input_type = int
    output_type = int
    execution = VM_EXECUTION

    def __init__(self) -> None:
        self._calls = 0

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        self._calls += 1
        return value + 2


class Sum(Module[tuple[int, int], int]):
    module_id = "distributed.sum"
    input_type = tuple[int, int]
    output_type = int
    execution = VM_EXECUTION

    def __init__(self) -> None:
        self._calls = 0

    async def execute(self, value: tuple[int, int], ctx: ExecutionContext) -> int:
        self._calls += 1
        return sum(value)


class FanOutFanIn(Workflow[int, int]):
    workflow_id = "distributed-fanout"
    input_type = int
    output_type = int

    def __init__(self) -> None:
        self.left = AddOne()
        self.right = AddTwo()
        self.join = Sum()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.join(parallel(self.left(value), self.right(value)))


class ReadValue(Module[dict[str, int | str], int]):
    module_id = "distributed.read-value"
    input_type = dict[str, int | str]
    output_type = int

    async def execute(self, value: dict[str, int | str], ctx: ExecutionContext) -> int:
        return int(value["value"])


class MappedFanOut(Workflow[list[dict[str, int | str]], list[int]]):
    workflow_id = "distributed-map"
    input_type = list[dict[str, int | str]]
    output_type = list[int]
    read = ReadValue()

    def build(self, value: RuntimeValue[list[dict[str, int | str]]]) -> RuntimeValue[list[int]]:
        return map_over(value, self.read, item_key="id")


class OtherVmWorkflow(Workflow[int, int]):
    workflow_id = "other-vm-definition"
    input_type = int
    output_type = int
    step = AddOne()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.step(value)


def test_execution_environment_is_behavior_bearing_but_placement_is_not() -> None:
    class Configured(AddOne):
        execution = ExecutionSpec(
            isolation="vm",
            image="maida/worker@sha256:" + "1" * 64,
            dependency_lock="sha256:" + "2" * 64,
        )

    class ConfiguredWorkflow(Workflow[int, int]):
        workflow_id = "execution-digest"
        input_type = int
        output_type = int
        step = Configured()

        def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
            return self.step(value)

    original = compile_workflow(ConfiguredWorkflow()).executable_steps[0]
    Configured.execution = ExecutionSpec(
        isolation="vm",
        image="maida/worker@sha256:" + "3" * 64,
        dependency_lock="sha256:" + "2" * 64,
    )
    changed_image = compile_workflow(ConfiguredWorkflow()).executable_steps[0]
    Configured.execution = ExecutionSpec(
        isolation="vm",
        image="maida/worker@sha256:" + "3" * 64,
        dependency_lock="sha256:" + "4" * 64,
    )
    changed_lock = compile_workflow(ConfiguredWorkflow()).executable_steps[0]

    assert original.module_digest != changed_image.module_digest
    assert changed_image.module_digest != changed_lock.module_digest
    assert changed_lock.execution is not None
    assert changed_lock.execution["isolation"] == "vm"
    assert "worker_id" not in changed_lock.execution


def test_execution_spec_rejects_mutable_images_and_invalid_resources() -> None:
    with pytest.raises(ValueError, match="immutable sha256 digest"):
        ExecutionSpec(isolation="vm", image="maida/worker:latest")
    with pytest.raises(ValueError, match="cpu"):
        ExecutionSpec(cpu=0)
    with pytest.raises(ValueError, match="memory"):
        ExecutionSpec(memory="lots")


def test_execution_specs_round_trip_and_executor_matching_fails_closed() -> None:
    restored = ExecutionSpec.from_data(VM_EXECUTION.to_data())

    assert restored == VM_EXECUTION
    assert restored.memory_bytes == 1024**3
    assert VM_CAPABILITIES.memory_bytes == 8 * 1024**3
    assert VM_CAPABILITIES.supports(restored)
    assert ExecutorCapabilities.local_process().supports(ExecutionSpec())
    assert not ExecutorCapabilities(isolations=frozenset({"vm"})).supports(restored)
    assert not ExecutorCapabilities(
        isolations=frozenset({"vm"}), images=frozenset({IMAGE}), cpu=1, memory="8GiB"
    ).supports(restored)
    assert not ExecutorCapabilities(
        isolations=frozenset({"vm"}), images=frozenset({IMAGE}), cpu=4, memory="512MiB"
    ).supports(restored)
    assert not ExecutorCapabilities(
        isolations=frozenset({"vm"}),
        images=frozenset({IMAGE}),
        cpu=4,
        memory="8GiB",
        capabilities=frozenset(),
    ).supports(ExecutionSpec(isolation="vm", image=IMAGE, capabilities=("network",)))

    with pytest.raises(ValueError, match="isolation"):
        ExecutionSpec(isolation="host")
    with pytest.raises(ValueError, match="dependency_lock"):
        ExecutionSpec(dependency_lock="requirements.txt")
    with pytest.raises(ValueError, match="non-empty"):
        ExecutionSpec(capabilities=("",))
    with pytest.raises(ValueError, match="unique"):
        ExecutionSpec(capabilities=("network", "network"))
    with pytest.raises(ValueError, match="unknown executor"):
        ExecutorCapabilities(isolations=frozenset({"host"}))
    with pytest.raises(ValueError, match="cpu"):
        ExecutorCapabilities(cpu=0)
    with pytest.raises(ValueError, match="memory"):
        ExecutorCapabilities(memory="8GB")


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_durable_dag_runs_across_executors_and_recovers_a_dead_vm(
    postgres_store: PostgresStore,
) -> None:
    workflow = FanOutFanIn()
    scheduler = WorkflowScheduler.submit(postgres_store, workflow, 10, tenant_id="tenant-a")
    initial = postgres_store.load_run_history(scheduler.run_id, tenant_id="tenant-a")

    assert len(initial.tasks) == 3
    assert {task.status for task in initial.tasks} == {TaskStatus.BLOCKED}
    existing = initial.tasks[0]
    existing_step = next(step for step in scheduler.plan.steps if step.node_id == existing.node_id)
    assert (
        postgres_store.enqueue_task(
            scheduler.run_id,
            existing_step,
            step_instance_id=existing.step_instance_id,
            input_value=None,
        ).task_id
        == existing.task_id
    )

    resumed_workflow = FanOutFanIn()
    scheduler = WorkflowScheduler.resume(
        postgres_store,
        resumed_workflow,
        scheduler.run_id,
        tenant_id="tenant-a",
    )
    progress = scheduler.advance()
    scheduled = postgres_store.load_run_history(scheduler.run_id, tenant_id="tenant-a")
    assert progress.ready_tasks == 2
    assert [task.status for task in scheduled.tasks].count(TaskStatus.READY) == 2
    assert [task.status for task in scheduled.tasks].count(TaskStatus.BLOCKED) == 1
    assert resumed_workflow.left._calls == 0
    assert resumed_workflow.right._calls == 0
    assert resumed_workflow.join._calls == 0
    already_ready = next(task for task in scheduled.tasks if task.status is TaskStatus.READY)
    assert already_ready.input_value is not None
    assert (
        postgres_store.ready_task(
            already_ready.task_id,
            input_value=already_ready.input_value,
            dependency_instance_keys=already_ready.dependency_instance_keys,
        ).status
        is TaskStatus.READY
    )

    modules = build_module_registry(resumed_workflow, scheduler.plan, output=scheduler.output)

    dead_worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=modules,
        worker_id="vm-17",
        capabilities=VM_CAPABILITIES,
    )
    recovery_lease = timedelta(milliseconds=500)
    dead_envelope = dead_worker.claim(lease_for=recovery_lease)
    assert dead_envelope is not None
    envelope_data = dead_envelope.to_data()
    assert envelope_data["task_id"] == dead_envelope.task_id
    assert envelope_data["attempt_id"] == dead_envelope.attempt_id
    assert envelope_data["lease_token"] == dead_envelope.lease_token
    assert envelope_data["execution_requirements"] == VM_EXECUTION.to_data()
    dead_envelope = dead_worker.start(dead_envelope)
    checkpoint = postgres_store.values.encode({"cursor": 1}, schema_digest="checkpoint-schema")
    dead_worker.checkpoint(dead_envelope, checkpoint)
    deadline = dead_worker.heartbeat(dead_envelope, lease_for=recovery_lease)
    assert deadline > dead_envelope.lease_expires_at

    live_worker = LocalExecutor(
        TaskWorker(
            postgres_store,
            workflow_id=scheduler.plan.workflow_id,
            definition_digest=scheduler.plan.digest,
            modules=modules,
            worker_id="vm-42",
            capabilities=VM_CAPABILITIES,
        )
    )
    assert live_worker.worker_id == "vm-42"
    assert live_worker.capabilities == VM_CAPABILITIES
    assert await live_worker.run_once() is not None
    assert scheduler.advance().ready_tasks == 0

    time.sleep(recovery_lease.total_seconds() + 0.05)
    recovery_worker = LocalExecutor(
        TaskWorker(
            postgres_store,
            workflow_id=scheduler.plan.workflow_id,
            definition_digest=scheduler.plan.digest,
            modules=modules,
            worker_id="vm-99",
            capabilities=VM_CAPABILITIES,
        )
    )
    assert await recovery_worker.run_once() is not None

    assert scheduler.advance().ready_tasks == 1
    join_worker = LocalExecutor(
        TaskWorker(
            postgres_store,
            workflow_id=scheduler.plan.workflow_id,
            definition_digest=scheduler.plan.digest,
            modules=modules,
            worker_id="vm-8",
            capabilities=VM_CAPABILITIES,
        )
    )
    assert await join_worker.run_once() is not None
    terminal = scheduler.advance()

    assert terminal.status is RunStatus.SUCCEEDED
    assert terminal.output == 23
    assert scheduler.advance() == terminal
    assert resumed_workflow.left._calls == 1
    assert resumed_workflow.right._calls == 1
    assert resumed_workflow.join._calls == 1

    history = postgres_store.load_run_history(scheduler.run_id, tenant_id="tenant-a")
    assert len(history.accepted_boundaries) == 3
    assert {boundary.accepted_attempt.worker_id for boundary in history.accepted_boundaries} == {
        "vm-42",
        "vm-99",
        "vm-8",
    }
    abandoned_task = next(task for task in history.tasks if task.task_id == dead_envelope.task_id)
    assert abandoned_task.accepted_boundary is not None
    assert abandoned_task.accepted_boundary.accepted_attempt.worker_id == "vm-99"
    assert [
        attempt.status for attempt in history.attempts if attempt.task_id == abandoned_task.task_id
    ] == [AttemptStatus.ABANDONED, AttemptStatus.SUCCEEDED]
    abandoned_attempt = next(
        attempt
        for attempt in history.attempts
        if attempt.task_id == abandoned_task.task_id and attempt.status is AttemptStatus.ABANDONED
    )
    assert abandoned_attempt.checkpoint == checkpoint
    with pytest.raises(StaleLeaseError, match="stale"):
        dead_worker.heartbeat(dead_envelope)


@pytest.mark.postgres
def test_executor_claims_only_tasks_it_can_satisfy(postgres_store: PostgresStore) -> None:
    scheduler = WorkflowScheduler.submit(postgres_store, FanOutFanIn(), 1)
    scheduler.advance()
    plan = scheduler.plan
    modules = build_module_registry(FanOutFanIn(), plan)

    process_worker = TaskWorker(
        postgres_store,
        workflow_id=plan.workflow_id,
        definition_digest=plan.digest,
        modules=modules,
        worker_id="local-process",
        capabilities=ExecutorCapabilities.local_process(),
    )

    assert process_worker.claim() is None


@pytest.mark.postgres
def test_definition_worker_does_not_claim_an_unrelated_ready_workflow(
    postgres_store: PostgresStore,
) -> None:
    unrelated = WorkflowScheduler.submit(postgres_store, OtherVmWorkflow(), 1)
    unrelated.advance()
    scheduler = WorkflowScheduler.submit(postgres_store, FanOutFanIn(), 1)
    scheduler.advance()
    modules = build_module_registry(FanOutFanIn(), scheduler.plan)
    worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=modules,
        worker_id="definition-worker",
        capabilities=VM_CAPABILITIES,
    )

    envelope = worker.claim()
    assert envelope is not None
    assert envelope.task.run_id == scheduler.run_id
    assert envelope.task.run_id != unrelated.run_id
    with pytest.raises(RuntimeContractError, match="definition does not match"):
        WorkflowScheduler.resume(postgres_store, OtherVmWorkflow(), scheduler.run_id)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_local_runner_does_not_bypass_vm_requirements(
    postgres_store: PostgresStore,
) -> None:
    with pytest.raises(RuntimeExecutionError, match="matching their execution requirements"):
        await WorkflowRunner(postgres_store).run(FanOutFanIn(), 1)


@pytest.mark.postgres
def test_one_scheduler_pass_exposes_the_entire_stable_map_fanout(
    postgres_store: PostgresStore,
) -> None:
    items: list[dict[str, int | str]] = [
        {"id": "a", "value": 1},
        {"id": "b", "value": 2},
        {"id": "c", "value": 3},
    ]
    scheduler = WorkflowScheduler.submit(postgres_store, MappedFanOut(), items)

    assert scheduler.advance().ready_tasks == 3
    history = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    assert len(history.tasks) == 3
    assert {task.status for task in history.tasks} == {TaskStatus.READY}
