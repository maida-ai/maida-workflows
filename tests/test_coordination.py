from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from maida.workflows import (
    ExecutionContext,
    Module,
    RuntimeValue,
    Workflow,
    WorkflowCatalog,
    WorkflowClient,
    WorkflowCoordinator,
)
from maida.workflows.models import RunStatus, TaskStatus
from maida.workflows.persistence import PostgresStore
from maida.workflows.replay import build_module_registry
from maida.workflows.runtime import TaskWorker, WorkflowScheduler

ADD_CALLS: list[int] = []


class AddOne(Module[int, int]):
    input_type = int
    output_type = int

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        ADD_CALLS.append(value)
        return value + 1


class TwoSteps(Workflow[int, int]):
    workflow_id = "coordinated-two-steps"
    input_type = int
    output_type = int

    def __init__(self) -> None:
        self.first = AddOne()
        self.second = AddOne()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.second(self.first(value))


class SlowAdd(Module[int, int]):
    input_type = int
    output_type = int

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        await asyncio.sleep(0.12)
        return value + 1


class SlowWorkflow(Workflow[int, int]):
    workflow_id = "heartbeat-slow"
    input_type = int
    output_type = int
    slow = SlowAdd()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.slow(value)


def test_catalog_pins_factories_by_definition_digest() -> None:
    catalog = WorkflowCatalog([TwoSteps])
    first = catalog.definitions[0]

    resolved = catalog.resolve(first.definition_digest)

    assert isinstance(resolved, TwoSteps)
    assert resolved is not catalog.resolve(first.definition_digest)
    assert first.workflow_id == TwoSteps.workflow_id
    assert catalog.register(TwoSteps) == first
    with pytest.raises(ValueError, match="registered"):
        WorkflowCatalog().resolve(first.definition_digest)
    with pytest.raises(ValueError, match="not registered"):
        catalog.resolve_workflow("missing")
    with pytest.raises(TypeError, match="Workflow"):
        catalog.register(lambda: object())  # type: ignore[arg-type,return-value]


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_coordinator_advances_runs_without_executing_handlers_and_restarts(
    postgres_store: PostgresStore,
) -> None:
    ADD_CALLS.clear()
    workflow = TwoSteps()
    run = WorkflowClient(postgres_store).start(workflow, 1, tenant_id="tenant-a")
    catalog = WorkflowCatalog([TwoSteps])
    coordinator = WorkflowCoordinator(postgres_store, catalog)

    first_tick = coordinator.run_once()

    assert first_tick.scanned_runs == 1
    assert first_tick.advanced_runs == 1
    assert first_tick.unavailable_runs == 0
    assert ADD_CALLS == []
    scheduler = WorkflowScheduler.resume(
        postgres_store, TwoSteps(), run.run_id, tenant_id="tenant-a"
    )
    worker_workflow = TwoSteps()
    worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=build_module_registry(worker_workflow, scheduler.plan),
        worker_id="worker-a",
    )
    assert await worker.run_once() is not None

    restarted = WorkflowCoordinator(postgres_store, WorkflowCatalog([TwoSteps]))
    assert restarted.run_once().ready_tasks == 1
    assert ADD_CALLS == [1]
    assert await worker.run_once() is not None
    terminal = restarted.run_once()

    assert terminal.completed_runs == 1
    assert run.snapshot().status is RunStatus.SUCCEEDED
    assert run.snapshot().output == 3
    assert ADD_CALLS == [1, 2]
    assert restarted.run_once().scanned_runs == 0


@pytest.mark.postgres
def test_coordinator_reports_unavailable_pinned_definitions(
    postgres_store: PostgresStore,
) -> None:
    run = WorkflowClient(postgres_store).start(TwoSteps(), 1)

    progress = WorkflowCoordinator(postgres_store, WorkflowCatalog()).run_once()

    assert progress.scanned_runs == 1
    assert progress.advanced_runs == 0
    assert progress.unavailable_runs == 1
    assert run.snapshot().status is RunStatus.RUNNING


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_worker_heartbeats_long_attempt_and_persistent_polling(
    postgres_store: PostgresStore,
) -> None:
    workflow = SlowWorkflow()
    run = WorkflowClient(postgres_store).start(workflow, 1)
    scheduler = WorkflowScheduler.resume(postgres_store, workflow, run.run_id)
    modules = build_module_registry(workflow, scheduler.plan)
    worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=modules,
        worker_id="slow-worker",
    )
    rival = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=modules,
        worker_id="rival-worker",
    )

    work = asyncio.create_task(
        worker.run_once(
            lease_for=timedelta(milliseconds=60),
            heartbeat_every=timedelta(milliseconds=15),
        )
    )
    await asyncio.sleep(0.09)
    assert rival.claim() is None
    assert await work is not None
    history = postgres_store.load_run_history(run.run_id, tenant_id="local")
    assert history.tasks[0].status is TaskStatus.SUCCEEDED
    assert any(event.event_type == "ATTEMPT_HEARTBEAT" for event in history.events)

    # Persistent polling exits through a caller-owned stop predicate and does
    # not create another attempt when no work remains.
    processed = await worker.serve(stop=lambda: True, poll_interval=0.001)
    assert processed == 0


@pytest.mark.asyncio
async def test_worker_polling_validates_intervals_without_storage() -> None:
    worker = TaskWorker(
        object(),  # type: ignore[arg-type]
        workflow_id="test",
        definition_digest="digest",
        modules={},
        worker_id="worker",
    )
    with pytest.raises(ValueError, match="poll_interval"):
        await worker.serve(stop=lambda: False, poll_interval=0)
    with pytest.raises(ValueError, match="heartbeat_every"):
        await worker.run_once(heartbeat_every=timedelta(0))


@pytest.mark.asyncio
async def test_coordinator_polling_handles_idle_stores_and_validates_limits() -> None:
    class EmptyStore:
        def list_active_runs(self, *, limit: int) -> tuple[tuple[str, str, str], ...]:
            return ()

    coordinator = WorkflowCoordinator(EmptyStore(), WorkflowCatalog())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="limit"):
        coordinator.run_once(limit=0)
    with pytest.raises(ValueError, match="poll_interval"):
        await coordinator.serve(stop=lambda: False, poll_interval=0)
    checks = 0

    def stop() -> bool:
        nonlocal checks
        checks += 1
        return checks > 1

    assert await coordinator.serve(stop=stop, poll_interval=0.001) == 0
