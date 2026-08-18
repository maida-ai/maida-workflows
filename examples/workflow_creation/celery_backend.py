"""Run the generated-plan example through the real Celery adapter offline.

The tiny eager task below stands in only for Celery's broker and worker pool.
It JSON-round-trips the exact payload and invokes the same boundary handler
an application registers as a Celery task; Maida's plan, trust checks, module
execution, and durable history are all real.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any

from maida.workflows import (
    BoundaryHarness,
    CeleryBackend,
    ExecutionRequest,
    RunResult,
    WorkflowRunner,
)
from maida.workflows.ir import ReplayKey
from maida.workflows.persistence import PostgresStore

from . import generated_plan

EXAMPLE_INPUT = generated_plan.BRIEF_INPUT
EXPECTED_OUTPUT = generated_plan.BRIEF_EXPECTED_OUTPUT


class _EagerResult:
    def __init__(
        self,
        handler: Callable[[Mapping[str, Any]], Mapping[str, Any]],
        payload: Mapping[str, Any],
    ) -> None:
        self.handler = handler
        self.payload = payload

    def get(self, *, timeout: float) -> Mapping[str, Any]:
        if timeout <= 0:  # pragma: no cover - CeleryBackend validates this
            raise TimeoutError("result timeout expired")
        return self.handler(self.payload)


class _EagerTask:
    def __init__(self, handler: Callable[[Mapping[str, Any]], Mapping[str, Any]]) -> None:
        self.handler = handler

    def apply_async(
        self,
        *,
        args: tuple[Mapping[str, Any]],
        task_id: str,
    ) -> _EagerResult:
        if not task_id:
            raise ValueError("Celery task id must carry Maida's execution id")
        payload = json.loads(json.dumps(args[0]))
        return _EagerResult(self.handler, payload)


async def run_example(
    store: PostgresStore,
    value: str = EXAMPLE_INPUT,
) -> RunResult:
    """Execute one accepted generated plan through the Celery transport seam."""
    planner = type(generated_plan.planner)()

    def harness_for(request: ExecutionRequest) -> BoundaryHarness:
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
            worker_id="celery-example-worker",
            connectors=generated_plan.connectors,
        )

    task = _EagerTask(CeleryBackend.task_handler(harness_for))
    return await WorkflowRunner(store, backend=CeleryBackend(task)).run_generated(
        planner,
        value,
    )
