"""Run Maida task boundaries on an application-owned Celery deployment.

The adapter sends only a strict execution request. Celery remains responsible
for queues, routing, retry timing, worker pools, and compute placement; the
worker-side handler resolves trusted application code and commits results to
Maida's authoritative boundary history.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from .runtime import (
    BoundaryHarness,
    ExecutionRequest,
    RuntimeContractError,
)


class _CeleryResult(Protocol):
    def get(self, *, timeout: float) -> Any: ...


class _CeleryTask(Protocol):
    def apply_async(
        self,
        *,
        args: tuple[Mapping[str, Any]],
        task_id: str,
    ) -> _CeleryResult: ...


HarnessFactory = Callable[[ExecutionRequest], BoundaryHarness]


class CeleryBackend:
    """Dispatch exact Maida tasks through an existing Celery task.

    Parameters
    ----------
    task
        Registered Celery task (or signature-compatible object) wrapping the
        callable returned by :meth:`task_handler`.
    timeout
        Maximum seconds to wait for the Celery result. Queue selection and
        retry policy remain Celery configuration, not Maida plan fields.
    """

    def __init__(self, task: _CeleryTask, *, timeout: float = 30.0) -> None:
        if not callable(getattr(task, "apply_async", None)):
            raise TypeError("Celery task must provide apply_async()")
        if timeout <= 0:
            raise ValueError("Celery result timeout must be positive")
        self.task = task
        self.timeout = timeout

    async def execute(self, request: ExecutionRequest) -> bool:
        """Dispatch one request and validate the Celery worker's receipt."""
        result = self.task.apply_async(
            args=(request.to_data(),),
            task_id=request.execution_id,
        )
        receipt = await asyncio.to_thread(result.get, timeout=self.timeout)
        expected = {"accepted", "execution_id", "task_id"}
        if not isinstance(receipt, Mapping) or set(receipt) != expected:
            raise RuntimeContractError("Celery execution receipt fields are invalid")
        if (
            receipt["execution_id"] != request.execution_id
            or receipt["task_id"] != request.task_id
            or type(receipt["accepted"]) is not bool
        ):
            raise RuntimeContractError("Celery execution receipt does not match its request")
        return receipt["accepted"]

    @staticmethod
    def task_handler(
        harness_for: HarnessFactory,
    ) -> Callable[[Mapping[str, Any]], dict[str, Any]]:
        """Build the synchronous function an application registers with Celery.

        ``harness_for`` is application-owned trusted code. It resolves an exact
        definition and module set from the request identities; no import path,
        credential, queue, or capability grant arrives from generated data.
        """
        if not callable(harness_for):
            raise TypeError("Celery harness factory must be callable")

        def handle(data: Mapping[str, Any]) -> dict[str, Any]:
            request = ExecutionRequest.from_data(data)
            harness = harness_for(request)
            if not isinstance(harness, BoundaryHarness):
                raise TypeError("Celery harness factory must return a BoundaryHarness")
            if harness.workflow_id != request.workflow_id:
                raise RuntimeContractError("Celery harness resolved a different workflow")
            if harness.definition_digest != request.definition_digest:
                raise RuntimeContractError("Celery harness resolved a different definition")
            accepted = asyncio.run(harness.run_request(request))
            return {
                "accepted": accepted,
                "execution_id": request.execution_id,
                "task_id": request.task_id,
            }

        return handle
