from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping
from typing import Any, cast

import pytest

from examples.workflow_creation import generated_plan
from maida.workflows import (
    Approval,
    ApprovalDecision,
    ApproveCommand,
    BoundaryHarness,
    CeleryBackend,
    ExecutionContext,
    ExecutionRequest,
    LocalExecutor,
    Module,
    RuntimeValue,
    TaskStatus,
    Workflow,
    WorkflowRun,
    WorkflowRunner,
    WorkflowScheduler,
    bind_workflow,
)
from maida.workflows.ir import ReplayKey
from maida.workflows.models import BoundaryRecord, RunHistory
from maida.workflows.persistence import PostgresStore
from maida.workflows.runtime import RuntimeContractError, TaskWorker


class _FakeResult:
    def __init__(
        self,
        handler: Callable[[Mapping[str, Any]], Mapping[str, Any]],
        payload: Mapping[str, Any],
    ) -> None:
        self.handler = handler
        self.payload = payload

    def get(self, *, timeout: float) -> Mapping[str, Any]:
        assert timeout > 0
        return self.handler(self.payload)


class _FakeCeleryTask:
    def __init__(self, handler: Callable[[Mapping[str, Any]], Mapping[str, Any]]) -> None:
        self.handler = handler
        self.dispatched: list[tuple[dict[str, Any], str]] = []

    def apply_async(
        self,
        *,
        args: tuple[Mapping[str, Any]],
        task_id: str,
    ) -> _FakeResult:
        payload = json.loads(json.dumps(args[0]))
        self.dispatched.append((payload, task_id))
        return _FakeResult(self.handler, payload)


class _LyingBackend:
    async def execute(self, request: ExecutionRequest) -> bool:
        return True


class _ExternalFlaky(Module[str, str]):
    module_id = "external.flaky"
    input_type = str
    output_type = str

    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("external delivery failed")
        return value.upper()


class _ExternalFlakyWorkflow(Workflow[str, str]):
    workflow_id = "external-flaky"
    input_type = str
    output_type = str
    flaky = _ExternalFlaky()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.flaky(value)


class _ExternalApprovalWorkflow(Workflow[str, ApprovalDecision]):
    workflow_id = "external-approval"
    input_type = str
    output_type = ApprovalDecision
    approval = Approval(str, prompt="Deploy this change?")

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[ApprovalDecision]:
        return self.approval(value)


class _FanoutBackend:
    def __init__(self, store: PostgresStore, backend: CeleryBackend) -> None:
        self.store = store
        self.backend = backend
        self.ready_sets: list[tuple[str, ...]] = []

    async def execute(self, request: ExecutionRequest) -> bool:
        history = self.store.load_run_history(request.run_id, tenant_id=request.tenant_id)
        ready = tuple(
            task.task_id
            for task in sorted(history.tasks, key=lambda item: item.task_id, reverse=True)
            if task.status is TaskStatus.READY
        )
        if len(ready) < 2:
            return await self.backend.execute(request)
        self.ready_sets.append(ready)
        requests = tuple(
            ExecutionRequest(
                run_id=request.run_id,
                tenant_id=request.tenant_id,
                workflow_id=request.workflow_id,
                definition_digest=request.definition_digest,
                task_id=task_id,
            )
            for task_id in ready
        )
        results = await asyncio.gather(*(self.backend.execute(item) for item in requests))
        return results[ready.index(request.task_id)]


def _harness_for(
    store: PostgresStore,
    planner: Any,
    request: ExecutionRequest,
) -> BoundaryHarness:
    history = store.load_run_history(request.run_id, tenant_id=request.tenant_id)
    task = next(task for task in history.tasks if task.task_id == request.task_id)
    module = (
        planner
        if task.module_id == planner.module_id
        else generated_plan.registry.resolve_exact(task.module_id, task.module_digest)
    )
    return BoundaryHarness(
        store,
        workflow_id=request.workflow_id,
        definition_digest=request.definition_digest,
        modules={ReplayKey(task.module_id, task.logical_step): module},
        worker_id="celery-worker",
        connectors=generated_plan.connectors,
    )


def _portable_boundary(boundary: BoundaryRecord) -> dict[str, Any]:
    data = boundary.to_data()
    data.pop("accepted_attempt")
    cast(dict[str, Any], data["usage"]).pop("latency_ms")
    for effect in cast(list[dict[str, Any]], data["effects"]):
        effect.pop("idempotency_key")
    return data


def _portable_history(history: RunHistory) -> dict[str, Any]:
    tasks = []
    for task in history.tasks:
        provenance = task.plan_provenance
        tasks.append(
            {
                "budget": task.budget.to_data(),
                "capability_grant": task.capability_grant.to_data(),
                "dependency_node_ids": task.dependency_node_ids,
                "input_digest": task.input_value.digest if task.input_value else None,
                "logical_step": task.logical_step,
                "module_digest": task.module_digest,
                "module_id": task.module_id,
                "output_digest": (
                    task.accepted_boundary.output_value.digest
                    if task.accepted_boundary is not None
                    else None
                ),
                "plan_provenance": (
                    {
                        "node_key": provenance.node_key,
                        "plan_digest": provenance.plan_digest,
                        "region_id": provenance.region_id,
                        "region_instance_id": provenance.region_instance_id,
                    }
                    if provenance is not None
                    else None
                ),
                "status": task.status,
            }
        )
    evidence: dict[str, Any] = {}
    for event in history.events:
        if event.event_type in {"PLAN_APPROVED", "PLAN_EXECUTION_VERIFIED"}:
            evidence[event.event_type] = event.payload
        elif event.event_type == "PLAN_MATERIALIZED":
            evidence[event.event_type] = {
                "fragment_id": event.payload["fragment_id"],
                "outputs": event.payload["outputs"],
                "plan_digest": event.payload["plan_digest"],
                "region_id": event.payload["region_id"],
                "region_instance_id": event.payload["region_instance_id"],
                "signature": event.payload["signature"],
                "signature_digest": event.payload["signature_digest"],
            }
    return {
        "boundaries": sorted(
            (_portable_boundary(boundary) for boundary in history.accepted_boundaries),
            key=lambda item: (item["module_id"], item["logical_step"]),
        ),
        "definition": history.definition.canonical_ir,
        "evidence": evidence,
        "root_input": history.run.root_input.digest,
        "root_output": history.run.root_output.digest if history.run.root_output else None,
        "status": history.run.status,
        "tasks": sorted(tasks, key=lambda item: (item["module_id"], item["logical_step"])),
    }


@pytest.mark.asyncio
async def test_execution_request_and_celery_receipt_fail_closed() -> None:
    request = ExecutionRequest(
        run_id="run-1",
        tenant_id="tenant-1",
        workflow_id="workflow-1",
        definition_digest="a" * 64,
        task_id="task-1",
    )
    assert ExecutionRequest.from_data(request.to_data()) == request
    with pytest.raises(ValueError, match="fields"):
        ExecutionRequest.from_data({**request.to_data(), "queue": "model-selected"})
    with pytest.raises(ValueError, match="strings"):
        ExecutionRequest.from_data({**request.to_data(), "task_id": 1})
    with pytest.raises(ValueError, match="sha256"):
        ExecutionRequest(
            run_id="run-1",
            tenant_id="tenant-1",
            workflow_id="workflow-1",
            definition_digest="latest",
            task_id="task-1",
        )
    with pytest.raises(TypeError, match="apply_async"):
        CeleryBackend(cast(Any, object()))
    with pytest.raises(ValueError, match="timeout"):
        CeleryBackend(_FakeCeleryTask(lambda payload: payload), timeout=0)
    with pytest.raises(TypeError, match="factory"):
        CeleryBackend.task_handler(cast(Any, None))
    invalid_harness = CeleryBackend.task_handler(lambda _request: cast(Any, object()))
    with pytest.raises(TypeError, match="BoundaryHarness"):
        invalid_harness(request.to_data())

    wrong_workflow = CeleryBackend.task_handler(
        lambda _request: BoundaryHarness(
            cast(Any, object()),
            workflow_id="different",
            definition_digest=request.definition_digest,
            modules={},
            worker_id="worker",
        )
    )
    with pytest.raises(RuntimeContractError, match="workflow"):
        wrong_workflow(request.to_data())

    wrong_definition = CeleryBackend.task_handler(
        lambda _request: BoundaryHarness(
            cast(Any, object()),
            workflow_id=request.workflow_id,
            definition_digest="b" * 64,
            modules={},
            worker_id="worker",
        )
    )
    with pytest.raises(RuntimeContractError, match="definition"):
        wrong_definition(request.to_data())

    local = LocalExecutor(
        TaskWorker(
            cast(Any, object()),
            workflow_id="different",
            definition_digest=request.definition_digest,
            modules={},
            worker_id="worker",
        )
    )
    with pytest.raises(RuntimeContractError, match="workflow"):
        await local.execute(request)

    direct_harness = BoundaryHarness(
        cast(Any, object()),
        workflow_id=request.workflow_id,
        definition_digest=request.definition_digest,
        modules={},
        worker_id="worker",
    )
    with pytest.raises(RuntimeContractError, match="definition"):
        direct_harness.validate_request(
            ExecutionRequest(
                run_id=request.run_id,
                tenant_id=request.tenant_id,
                workflow_id=request.workflow_id,
                definition_digest="b" * 64,
                task_id=request.task_id,
            )
        )

    invalid_fields = _FakeCeleryTask(lambda _payload: {"accepted": True})
    with pytest.raises(RuntimeContractError, match="fields"):
        await CeleryBackend(invalid_fields).execute(request)

    task = _FakeCeleryTask(
        lambda payload: {
            "accepted": True,
            "execution_id": "wrong",
            "task_id": payload["task_id"],
        }
    )
    with pytest.raises(RuntimeContractError, match="receipt"):
        await CeleryBackend(task).execute(request)


@pytest.mark.postgres
def test_external_failure_leaves_retry_policy_to_celery(
    postgres_store: PostgresStore,
) -> None:
    workflow = _ExternalFlakyWorkflow()
    bound = bind_workflow(workflow)
    scheduler = WorkflowScheduler.submit(postgres_store, bound, "retry me")
    scheduler.advance()
    history = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    task = history.tasks[0]
    request = ExecutionRequest(
        run_id=scheduler.run_id,
        tenant_id="local",
        workflow_id=bound.plan.workflow_id,
        definition_digest=bound.plan.digest,
        task_id=task.task_id,
    )
    harness = BoundaryHarness(
        postgres_store,
        workflow_id=request.workflow_id,
        definition_digest=request.definition_digest,
        modules=bound.modules,
        worker_id="celery-worker",
    )
    handler = CeleryBackend.task_handler(lambda _request: harness)

    with pytest.raises(RuntimeError, match="external delivery failed"):
        handler(request.to_data())
    failed_delivery = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    assert failed_delivery.tasks[0].status is TaskStatus.READY
    assert len(failed_delivery.attempts) == 1
    assert failed_delivery.attempts[0].lease_token is None
    assert any(event.event_type == "BOUNDARY_EXECUTION_FAILED" for event in failed_delivery.events)
    assert not any(event.event_type == "TASK_RETRY" for event in failed_delivery.events)
    with pytest.raises(ValueError, match="execution_id"):
        postgres_store.start_external_execution(
            failed_delivery.tasks[0], execution_id="invalid", worker_id="celery-worker"
        )
    with pytest.raises(ValueError, match="worker_id"):
        postgres_store.start_external_execution(
            failed_delivery.tasks[0], execution_id="f" * 64, worker_id=" "
        )

    assert handler(request.to_data())["accepted"] is True
    completed = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    assert len(completed.attempts) == 1
    assert completed.attempts[0].diagnostic is None
    assert not any(event.event_type == "TASK_RETRY" for event in completed.events)
    assert (
        postgres_store.start_external_execution(
            completed.tasks[0],
            execution_id=request.execution_id,
            worker_id="another-celery-worker",
        )
        is None
    )
    assert scheduler.advance().output == "RETRY ME"


@pytest.mark.postgres
def test_external_interaction_reuses_its_non_expiring_reservation(
    postgres_store: PostgresStore,
) -> None:
    workflow = _ExternalApprovalWorkflow()
    bound = bind_workflow(workflow)
    scheduler = WorkflowScheduler.submit(postgres_store, bound, "release")
    scheduler.advance()
    task = postgres_store.load_run_history(scheduler.run_id, tenant_id="local").tasks[0]
    request = ExecutionRequest(
        run_id=scheduler.run_id,
        tenant_id="local",
        workflow_id=bound.plan.workflow_id,
        definition_digest=bound.plan.digest,
        task_id=task.task_id,
    )
    harness = BoundaryHarness(
        postgres_store,
        workflow_id=request.workflow_id,
        definition_digest=request.definition_digest,
        modules=bound.modules,
        worker_id="celery-worker",
    )
    handler = CeleryBackend.task_handler(lambda _request: harness)

    assert handler(request.to_data())["accepted"] is False
    parked = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    required = next(event for event in parked.events if event.event_type == "APPROVAL_REQUIRED")
    assert parked.tasks[0].status is TaskStatus.NEEDS_APPROVAL
    assert len(parked.attempts) == 1
    assert parked.attempts[0].lease_token is None

    WorkflowRun(postgres_store, scheduler.run_id).send(
        ApproveCommand(request_id=required.payload["request_id"], command_id="approve-1")
    )
    assert handler(request.to_data())["accepted"] is True
    completed = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    assert len(completed.attempts) == 1
    assert [
        event.event_type
        for event in completed.events
        if event.event_type.startswith("BOUNDARY_EXECUTION_")
    ] == ["BOUNDARY_EXECUTION_STARTED", "BOUNDARY_EXECUTION_RESUMED"]
    assert scheduler.advance().output.approved is True


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_generated_plan_has_equivalent_verifiable_history_on_local_and_celery(
    postgres_store: PostgresStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_planner = type(generated_plan.planner)()
    local = await WorkflowRunner(postgres_store).run_generated(
        local_planner,
        generated_plan.BRIEF_INPUT,
    )

    remote_planner = type(generated_plan.planner)()

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("external execution entered the local claim lifecycle")

    for name in ("claim_task", "start_task", "heartbeat_task", "fail_task"):
        monkeypatch.setattr(postgres_store, name, forbidden)

    handler = CeleryBackend.task_handler(
        lambda request: _harness_for(postgres_store, remote_planner, request)
    )
    celery_task = _FakeCeleryTask(handler)
    external = await WorkflowRunner(
        postgres_store,
        backend=CeleryBackend(celery_task),
    ).run_generated(remote_planner, generated_plan.BRIEF_INPUT)

    local_history = postgres_store.load_run_history(local.run_id, tenant_id="local")
    external_history = postgres_store.load_run_history(external.run_id, tenant_id="local")

    assert external.output == local.output == generated_plan.BRIEF_EXPECTED_OUTPUT
    assert external.definition_digest == local.definition_digest
    assert _portable_history(external_history) == _portable_history(local_history)
    assert len(celery_task.dispatched) == 3
    assert all(payload["task_id"] for payload, _task_id in celery_task.dispatched)
    assert all(
        task_id == ExecutionRequest.from_data(payload).execution_id
        for payload, task_id in celery_task.dispatched
    )
    assert external_history.attempts
    assert all(attempt.lease_token is None for attempt in external_history.attempts)
    assert not any(event.event_type == "ATTEMPT_CLAIMED" for event in external_history.events)
    attempts_before = len(external_history.attempts)
    completions_before = sum(
        event.event_type == "TASK_COMPLETED" for event in external_history.events
    )
    replayed_receipt = await asyncio.to_thread(handler, celery_task.dispatched[0][0])
    assert replayed_receipt["accepted"] is True
    replayed_history = postgres_store.load_run_history(external.run_id, tenant_id="local")
    assert len(replayed_history.attempts) == attempts_before
    assert (
        sum(event.event_type == "TASK_COMPLETED" for event in replayed_history.events)
        == completions_before
    )

    external_task = external_history.tasks[-1]
    forged = ExecutionRequest(
        run_id=local.run_id,
        tenant_id="local",
        workflow_id=external_history.definition.workflow_id,
        definition_digest=external_history.definition.digest,
        task_id=external_task.task_id,
    )
    forged_harness = BoundaryHarness(
        postgres_store,
        workflow_id=forged.workflow_id,
        definition_digest=forged.definition_digest,
        modules={},
        worker_id="celery-worker",
    )
    with pytest.raises(RuntimeContractError, match="does not belong"):
        await asyncio.to_thread(
            CeleryBackend.task_handler(lambda _request: forged_harness), forged.to_data()
        )


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_external_boundary_history_still_fails_plan_execution_proof(
    postgres_store: PostgresStore,
) -> None:
    planner = type(generated_plan.planner)()
    trusted_handler = CeleryBackend.task_handler(
        lambda request: _harness_for(postgres_store, planner, request)
    )

    def divergent_handler(data: Mapping[str, Any]) -> Mapping[str, Any]:
        receipt = trusted_handler(data)
        request = ExecutionRequest.from_data(data)
        history = postgres_store.load_run_history(request.run_id, tenant_id=request.tenant_id)
        task = next(item for item in history.tasks if item.task_id == request.task_id)
        if task.plan_provenance is not None:
            with postgres_store.connect() as connection:
                connection.execute(
                    """
                    UPDATE workflow_tasks
                    SET accepted_boundary = jsonb_set(
                        accepted_boundary,
                        '{output_schema_digest}',
                        to_jsonb(%s::text)
                    )
                    WHERE task_id = %s
                    """,
                    ("f" * 64, request.task_id),
                )
        return receipt

    with pytest.raises(RuntimeError, match="output contract diverged"):
        await WorkflowRunner(
            postgres_store,
            backend=CeleryBackend(_FakeCeleryTask(divergent_handler)),
        ).run_generated(planner, generated_plan.BRIEF_INPUT)

    with postgres_store.connect() as connection:
        row = connection.execute(
            "SELECT run_id FROM workflow_runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    assert row is not None
    history = postgres_store.load_run_history(str(row["run_id"]), tenant_id="local")
    divergence = next(
        event for event in history.events if event.event_type == "PLAN_EXECUTION_DIVERGED"
    )
    assert divergence.payload["issues"][0]["code"] == "PLAN_EXECUTION_DIVERGENCE"


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_fanout_plan_has_equivalent_history_when_external_substrate_schedules_ready_tasks(
    postgres_store: PostgresStore,
) -> None:
    local_planner = type(generated_plan.planner)()
    local = await WorkflowRunner(
        postgres_store,
        connectors=generated_plan.connectors,
    ).run_generated(local_planner, generated_plan.EXAMPLE_INPUT)

    remote_planner = type(generated_plan.planner)()
    handler = CeleryBackend.task_handler(
        lambda request: _harness_for(postgres_store, remote_planner, request)
    )
    celery_task = _FakeCeleryTask(handler)
    fanout = _FanoutBackend(postgres_store, CeleryBackend(celery_task))
    external = await WorkflowRunner(postgres_store, backend=fanout).run_generated(
        remote_planner,
        generated_plan.EXAMPLE_INPUT,
    )

    local_history = postgres_store.load_run_history(local.run_id, tenant_id="local")
    external_history = postgres_store.load_run_history(external.run_id, tenant_id="local")
    assert fanout.ready_sets
    assert any(len(ready) == 2 for ready in fanout.ready_sets)
    assert external.output == local.output == generated_plan.EXPECTED_OUTPUT
    assert _portable_history(external_history) == _portable_history(local_history)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_runner_rejects_backend_receipt_without_a_durable_boundary(
    postgres_store: PostgresStore,
) -> None:
    planner = type(generated_plan.planner)()

    with pytest.raises(RuntimeContractError, match="without a durable boundary"):
        await WorkflowRunner(postgres_store, backend=_LyingBackend()).run_generated(
            planner,
            generated_plan.BRIEF_INPUT,
        )
