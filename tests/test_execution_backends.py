from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping
from typing import Any, cast

import pytest

from examples.workflow_creation import generated_plan
from maida.workflows import CeleryBackend, ExecutionRequest, LocalExecutor, WorkflowRunner
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


def _portable_boundary(boundary: BoundaryRecord) -> dict[str, Any]:
    data = boundary.to_data()
    data.pop("accepted_attempt")
    cast(dict[str, Any], data["usage"]).pop("latency_ms")
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
    invalid_worker = CeleryBackend.task_handler(lambda _request: cast(Any, object()))
    with pytest.raises(TypeError, match="TaskWorker"):
        invalid_worker(request.to_data())

    wrong_workflow = CeleryBackend.task_handler(
        lambda _request: TaskWorker(
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
        lambda _request: TaskWorker(
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
@pytest.mark.asyncio
async def test_generated_plan_has_equivalent_verifiable_history_on_local_and_celery(
    postgres_store: PostgresStore,
) -> None:
    local_planner = type(generated_plan.planner)()
    local = await WorkflowRunner(postgres_store).run_generated(
        local_planner,
        generated_plan.BRIEF_INPUT,
    )

    remote_planner = type(generated_plan.planner)()

    def worker_for(request: ExecutionRequest) -> TaskWorker:
        history = postgres_store.load_run_history(request.run_id, tenant_id=request.tenant_id)
        task = next(task for task in history.tasks if task.task_id == request.task_id)
        module = (
            remote_planner
            if task.module_id == remote_planner.module_id
            else generated_plan.registry.resolve_exact(task.module_id, task.module_digest)
        )
        return TaskWorker(
            postgres_store,
            workflow_id=request.workflow_id,
            definition_digest=request.definition_digest,
            modules={ReplayKey(task.module_id, task.logical_step): module},
            worker_id="celery-worker",
        )

    handler = CeleryBackend.task_handler(worker_for)
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
    replayed_receipt = await asyncio.to_thread(handler, celery_task.dispatched[0][0])
    assert replayed_receipt["accepted"] is True

    external_task = external_history.tasks[-1]
    forged = ExecutionRequest(
        run_id=local.run_id,
        tenant_id="local",
        workflow_id=external_history.definition.workflow_id,
        definition_digest=external_history.definition.digest,
        task_id=external_task.task_id,
    )
    forged_worker = TaskWorker(
        postgres_store,
        workflow_id=forged.workflow_id,
        definition_digest=forged.definition_digest,
        modules={},
        worker_id="celery-worker",
    )
    with pytest.raises(RuntimeContractError, match="does not belong"):
        await asyncio.to_thread(
            CeleryBackend.task_handler(lambda _request: forged_worker), forged.to_data()
        )


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
