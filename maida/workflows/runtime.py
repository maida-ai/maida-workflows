"""Execute compiled workflows as durable typed module boundaries.

The runtime records runs, tasks, attempts, accepted boundary values, usage, and
control decisions through a :class:`DurableRuntimeStore`. Control-plane
services use :class:`WorkflowScheduler`; executor services use
:class:`TaskWorker`. :class:`WorkflowRunner` hosts both roles only as a local
development convenience.
"""

from __future__ import annotations

import asyncio
import dataclasses
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

from ._canonical import (
    _rehydrate_value,
    digest_data,
    schema_digest,
    value_matches_type,
)
from .access import (
    AccessBroker,
    AccessContractError,
    AccessPolicy,
    Capability,
    ConnectorRegistry,
    EffectSpec,
)
from .authoring import (
    ExecutionContext,
    Module,
    Workflow,
)
from .definitions import BoundWorkflow, bind_workflow
from .ir import BindingIR, PlanIR, ReplayKey, StepIR, module_digest
from .models import (
    AcceptedAttemptProvenance,
    Attempt,
    BoundaryRecord,
    CapabilityGrant,
    EffectKind,
    EffectRecord,
    ExecutionMode,
    ExecutorCapabilities,
    RunStatus,
    StoredValue,
    Task,
    TaskStatus,
    TrajectoryRecord,
    Usage,
    _EffectOperation,
)
from .persistence import ClaimedTask

_rehydrate = _rehydrate_value
_NESTED_SCOPE_PATTERN = re.compile(r"\.nested\[([^\]]+)\]")


class RuntimeExecutionError(RuntimeError):
    """Base error raised when durable workflow execution cannot continue."""


class RuntimeContractError(RuntimeExecutionError):
    """Raised when runtime data or registered code violates a pinned contract."""


class DurableRuntimeStore(Protocol):
    """Persistence operations required by the workflow runner and workers."""

    values: Any

    def create_run(
        self,
        plan: PlanIR,
        *,
        tenant_id: str,
        root_input: StoredValue,
        execution_mode: ExecutionMode = ExecutionMode.LIVE,
        run_id: str | None = None,
    ) -> Any:
        """Persist a new run pinned to a compiled workflow definition."""
        ...

    def enqueue_task(
        self,
        run_id: str,
        step: StepIR,
        *,
        step_instance_id: str,
        input_value: StoredValue | None,
        dependency_instance_keys: tuple[str, ...] = (),
        dependency_node_ids: tuple[str, ...] | None = None,
        capability_grant: CapabilityGrant | None = None,
        branch_decisions: tuple[dict[str, Any], ...] = (),
        map_decisions: tuple[dict[str, Any], ...] = (),
        task_id: str | None = None,
    ) -> Any:
        """Create or return one durable logical task for an execution instance."""
        ...

    def claim_task(
        self,
        *,
        worker_id: str,
        capabilities: ExecutorCapabilities | None = None,
        definition_digest: str | None = None,
        lease_for: timedelta = timedelta(minutes=5),
        task_id: str | None = None,
    ) -> ClaimedTask | None:
        """Lease one pending task, optionally restricted to a task ID."""
        ...

    def start_task(self, claim: ClaimedTask) -> ClaimedTask:
        """Transition a claimed attempt to running."""
        ...

    def heartbeat_task(
        self,
        claim: ClaimedTask,
        *,
        lease_for: timedelta = timedelta(minutes=5),
    ) -> datetime:
        """Extend an active attempt lease."""
        ...

    def checkpoint_task(self, claim: ClaimedTask, checkpoint: StoredValue) -> None:
        """Persist an immutable checkpoint reference."""
        ...

    def _reserve_effect(
        self,
        claim: ClaimedTask,
        *,
        effect_name: str,
        ordinal: int,
        connector: str,
        operation: str,
        connector_version: str | None,
        idempotency_requirement: str,
        adapter_idempotent: bool,
        request_digest: str,
        result_schema_digest: str,
    ) -> _EffectOperation: ...

    def _lookup_effect(
        self,
        claim: ClaimedTask,
        *,
        effect_name: str,
        ordinal: int,
        connector: str,
        operation: str,
        connector_version: str | None,
        idempotency_requirement: str,
        request_digest: str,
        result_schema_digest: str,
    ) -> _EffectOperation | None: ...

    def _mark_effect_attempted(
        self,
        claim: ClaimedTask,
        operation: _EffectOperation,
        *,
        policy_id: str,
        reason_code: str,
        approval_required: bool,
        approval_request_id: str | None,
        approval_command_id: str | None,
    ) -> _EffectOperation: ...

    def _commit_effect(
        self,
        claim: ClaimedTask,
        operation: _EffectOperation,
        result: StoredValue,
        *,
        latency_ms: float,
    ) -> _EffectOperation: ...

    def complete_task(self, claim: ClaimedTask, boundary: BoundaryRecord) -> BoundaryRecord:
        """Accept and return the store-authoritative logical boundary."""
        ...

    def fail_task(
        self,
        claim: ClaimedTask,
        diagnostic: dict[str, Any],
        *,
        retry: bool,
    ) -> None:
        """Record a failed attempt and optionally make its task retryable."""
        ...

    def park_task(self, claim: ClaimedTask, request: dict[str, Any]) -> None:
        """Relinquish a running attempt while awaiting a durable command."""
        ...

    def complete_run(self, run_id: str, root_output: StoredValue) -> None:
        """Mark a run successful with its immutable root output."""
        ...

    def fail_run(self, run_id: str, diagnostic: dict[str, Any]) -> None:
        """Mark a run failed and persist a diagnostic event."""
        ...

    def append_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        task_id: str | None = None,
        attempt_id: str | None = None,
    ) -> None:
        """Append an ordered run, task, or attempt event."""
        ...

    def append_access_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        task_id: str,
        attempt_id: str,
    ) -> None:
        """Append one allowlisted broker diagnostic event."""
        ...

    def ready_task(
        self,
        task_id: str,
        *,
        input_value: StoredValue,
        dependency_instance_keys: tuple[str, ...],
        branch_decisions: tuple[dict[str, Any], ...] = (),
        map_decisions: tuple[dict[str, Any], ...] = (),
    ) -> Task:
        """Make a blocked task claimable after materializing its input."""
        ...

    def load_run_history(self, run_id: str, *, tenant_id: str) -> Any:
        """Load durable scheduler state and accepted results."""
        ...


@dataclass(frozen=True)
class RunResult:
    """Successful terminal output returned by :class:`WorkflowRunner`.

    Attributes
    ----------
    run_id
        Durable identifier of the completed run.
    output
        Rehydrated workflow output matching its root contract.
    definition_digest
        Digest of the compiled workflow definition used for execution.
    """

    run_id: str
    output: Any
    definition_digest: str


@dataclass(frozen=True)
class TaskEnvelope:
    """Small transport-safe control envelope returned by a worker claim.

    Payload bytes stay inline only when the configured value codec chose that
    representation; large values remain content-addressed references. The
    envelope preserves immutable execution and budget declarations while
    containing no workflow object, Python call stack, usage counter, or VM
    placement.
    """

    task: Task
    attempt: Attempt
    worker_id: str
    lease_expires_at: datetime

    @classmethod
    def from_claim(cls, claim: ClaimedTask) -> TaskEnvelope:
        """Project an internal store claim into the public worker envelope."""
        return cls(claim.task, claim.attempt, claim.worker_id, claim.lease_expires_at)

    @property
    def task_id(self) -> str:
        """Return the durable logical task identifier."""
        return self.task.task_id

    @property
    def attempt_id(self) -> str:
        """Return the physical attempt identifier."""
        return str(self.attempt.attempt_id)

    @property
    def lease_token(self) -> str:
        """Return the compare-and-swap token required by worker updates."""
        return str(self.attempt.lease_token)

    @property
    def input_ref(self) -> StoredValue:
        """Return the immutable task input reference.

        Raises
        ------
        RuntimeContractError
            If a blocked task was incorrectly exposed to an executor.
        """
        if self.task.input_value is None:
            raise RuntimeContractError("a blocked task cannot be placed in a worker envelope")
        return self.task.input_value

    def to_data(self) -> dict[str, Any]:
        """Return remote-worker transport data with the declared budget.

        The returned ``budget`` is the immutable module declaration. Attempt
        usage is recorded separately and is never folded into this envelope.
        """
        return {
            "task_id": self.task.task_id,
            "run_id": self.task.run_id,
            "module_id": self.task.module_id,
            "logical_step": self.task.logical_step,
            "step_instance_id": self.task.step_instance_id,
            "module_digest": self.task.module_digest,
            "input_ref": self.input_ref.to_data(),
            "execution_requirements": self.task.execution.to_data(),
            "budget": self.task.budget.to_data(),
            "capability_grant": self.task.capability_grant.to_data(),
            "attempt_id": self.attempt_id,
            "attempt_number": self.attempt.attempt_number,
            "lease_token": self.lease_token,
            "lease_expires_at": self.lease_expires_at.isoformat(),
        }

    def _claim(self) -> ClaimedTask:
        return ClaimedTask(self.task, self.attempt, self.worker_id, self.lease_expires_at)


@dataclass(frozen=True)
class ScheduleProgress:
    """Observable result of one non-blocking scheduler pass."""

    run_id: str
    status: RunStatus
    ready_tasks: int
    output: Any = None


@dataclass(frozen=True)
class _Evaluation:
    value: Any
    dependency_instance_keys: tuple[str, ...] = ()
    branch_decisions: tuple[dict[str, Any], ...] = ()
    map_decisions: tuple[dict[str, Any], ...] = ()


def _stable_instance_id(step: StepIR, scope: tuple[str, ...]) -> str:
    if step.replay_key is None:
        raise RuntimeContractError("control nodes cannot have task instances")
    return digest_data({"replay_key": step.replay_key.as_string(), "scope": scope})[:24]


def _coerce_trajectory(value: dict[str, Any]) -> TrajectoryRecord:
    return TrajectoryRecord(
        kind=str(value["kind"]),
        name=str(value["name"]),
        request_digest=str(value["request_digest"]),
        response_digest=str(value["response_digest"]),
        metadata=dict(value.get("metadata", {})),
    )


def _coerce_effect(value: dict[str, Any]) -> EffectRecord:
    return EffectRecord(
        kind=EffectKind(value["kind"]),
        adapter=str(value["adapter"]),
        operation=str(value["operation"]),
        request_digest=str(value["request_digest"]),
        result_digest=value.get("result_digest"),
        effect_name=value.get("effect_name"),
        ordinal=value.get("ordinal"),
        idempotency_key=value.get("idempotency_key"),
        connector_version=value.get("connector_version"),
    )


class TaskWorker:
    """Claim and execute durable module tasks without replay substitution.

    Parameters
    ----------
    store
        Durable runtime store containing tasks and typed values.
    workflow_id
        Workflow identity recorded on accepted boundaries.
    definition_digest
        Compiled definition digest that registered modules must match.
    modules
        Current module instances keyed by exact replay address.
    worker_id
        Stable diagnostic identity of this worker process.
    max_attempts
        Maximum attempts allowed for a logical task.
    capabilities
        Isolation modes, images, resources, and executor labels this worker can
        satisfy for placement. These are never connector access grants.
    connectors
        Provider-neutral read and effect adapters available in this worker
        deployment. Effect adapters receive stable logical idempotency keys;
        adapter instances and credentials never enter task envelopes.
    access_policy
        Optional policy that may narrow each task's compiled access grant and
        reference same-task durable approval evidence for protected effects.
    """

    def __init__(
        self,
        store: DurableRuntimeStore,
        *,
        workflow_id: str,
        definition_digest: str,
        modules: Mapping[ReplayKey, Module[Any, Any]],
        worker_id: str,
        max_attempts: int = 3,
        capabilities: ExecutorCapabilities | None = None,
        connectors: ConnectorRegistry | None = None,
        access_policy: AccessPolicy | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self.store = store
        self.workflow_id = workflow_id
        self.definition_digest = definition_digest
        self.modules = modules
        self.module_digests = {key: module_digest(module) for key, module in modules.items()}
        self.worker_id = worker_id
        self.max_attempts = max_attempts
        self.capabilities = capabilities or ExecutorCapabilities.local_process()
        self.connectors = connectors or ConnectorRegistry()
        self.access_policy = access_policy

    def claim(
        self,
        *,
        lease_for: timedelta = timedelta(minutes=5),
        task_id: str | None = None,
    ) -> TaskEnvelope | None:
        """Claim one compatible ready task without starting its handler."""
        claim = self.store.claim_task(
            worker_id=self.worker_id,
            capabilities=self.capabilities,
            definition_digest=self.definition_digest,
            lease_for=lease_for,
            task_id=task_id,
        )
        return TaskEnvelope.from_claim(claim) if claim is not None else None

    def start(self, envelope: TaskEnvelope) -> TaskEnvelope:
        """Start a claimed attempt using its current lease token."""
        return TaskEnvelope.from_claim(self.store.start_task(envelope._claim()))

    def heartbeat(
        self,
        envelope: TaskEnvelope,
        *,
        lease_for: timedelta = timedelta(minutes=5),
    ) -> datetime:
        """Extend a claimed or running attempt lease."""
        return self.store.heartbeat_task(envelope._claim(), lease_for=lease_for)

    def checkpoint(self, envelope: TaskEnvelope, checkpoint: StoredValue) -> None:
        """Persist a content-addressed checkpoint for a running attempt."""
        self.store.checkpoint_task(envelope._claim(), checkpoint)

    def complete(self, envelope: TaskEnvelope, boundary: BoundaryRecord) -> BoundaryRecord:
        """Accept and return the store-authoritative logical result."""
        return self.store.complete_task(envelope._claim(), boundary)

    def fail(
        self,
        envelope: TaskEnvelope,
        diagnostic: dict[str, Any],
        *,
        retry: bool,
    ) -> None:
        """Fail an attempt and optionally return its logical task to ready."""
        self.store.fail_task(envelope._claim(), diagnostic, retry=retry)

    def park(self, envelope: TaskEnvelope, request: Any) -> None:
        """Park a running task for input, approval, or a named signal.

        The worker relinquishes its lease and completes the physical attempt;
        a later accepted command returns the logical task to ``READY`` for a
        fresh worker attempt. ``request`` must expose ``to_data()`` returning a
        canonical interaction payload.
        """
        to_data = getattr(request, "to_data", None)
        if not callable(to_data):
            raise TypeError("interaction request must provide to_data()")
        request_data = to_data()
        if not isinstance(request_data, dict):
            raise TypeError("interaction request payload must be a mapping")
        self.store.park_task(envelope._claim(), request_data)

    async def run_once(
        self,
        *,
        task_id: str | None = None,
        lease_for: timedelta = timedelta(minutes=5),
        heartbeat_every: timedelta | None = None,
    ) -> BoundaryRecord | None:
        """Claim and execute at most one durable task.

        Parameters
        ----------
        task_id
            Optional exact task to claim; otherwise any eligible task may be
            leased.
        lease_for
            Duration of each claim and automatic heartbeat extension.
        heartbeat_every
            Interval between automatic heartbeats. Defaults to one third of
            ``lease_for`` and must be shorter than the lease.

        Returns
        -------
        BoundaryRecord or None
            Accepted result, or ``None`` when no task can be claimed.

        Raises
        ------
        RuntimeContractError
            If no matching module is registered, its digest differs from the
            task definition, or persisted input violates the module contract.
        """
        interval = lease_for / 3 if heartbeat_every is None else heartbeat_every
        if interval <= timedelta(0) or interval >= lease_for:
            raise ValueError("heartbeat_every must be positive and shorter than lease_for")
        envelope = self.claim(task_id=task_id, lease_for=lease_for)
        if envelope is None:
            return None
        envelope = self.start(envelope)
        heartbeat = asyncio.create_task(self._heartbeat_loop(envelope, lease_for, interval))
        try:
            claim = envelope._claim()
            key = ReplayKey(claim.task.module_id, claim.task.logical_step)
            try:
                module = self.modules[key]
            except KeyError as exc:
                self.fail(
                    envelope,
                    {"reason": f"no module registered for {key.as_string()}"},
                    retry=False,
                )
                raise RuntimeContractError(f"no module registered for {key.as_string()}") from exc
            try:
                input_data = _rehydrate(
                    self.store.values.decode(envelope.input_ref), module.input_type
                )
            except Exception as exc:
                self.fail(
                    envelope,
                    {"reason": str(exc), "exception_type": type(exc).__qualname__},
                    retry=False,
                )
                raise RuntimeContractError("task input reference could not be resolved") from exc
            return await self._execute_claim(
                module,
                claim,
                input_data,
                branch_decisions=claim.task.branch_decisions,
                map_decisions=claim.task.map_decisions,
            )
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    async def serve(
        self,
        *,
        stop: Callable[[], bool],
        poll_interval: float = 0.25,
    ) -> int:
        """Poll and execute compatible tasks until ``stop`` returns true.

        Parameters
        ----------
        stop
            Caller-owned predicate used for graceful process shutdown.
        poll_interval
            Delay after an empty claim pass.

        Returns
        -------
        int
            Number of accepted boundaries produced by this worker.
        """
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        processed = 0
        while not stop():
            boundary = await self.run_once()
            if boundary is None:
                await asyncio.sleep(poll_interval)
            else:
                processed += 1
        return processed

    async def _heartbeat_loop(
        self,
        envelope: TaskEnvelope,
        lease_for: timedelta,
        interval: timedelta,
    ) -> None:
        while True:
            await asyncio.sleep(interval.total_seconds())
            self.heartbeat(envelope, lease_for=lease_for)

    async def _execute_claim(
        self,
        module: Module[Any, Any],
        claim: ClaimedTask,
        input_data: Any,
        *,
        branch_decisions: tuple[dict[str, Any], ...] = (),
        map_decisions: tuple[dict[str, Any], ...] = (),
    ) -> BoundaryRecord:
        key = key_for(claim)
        envelope = TaskEnvelope.from_claim(claim)
        if claim.task.input_value is None:
            self.fail(envelope, {"reason": "ready task has no input reference"}, retry=False)
            raise RuntimeContractError("ready task has no input reference")
        if self.module_digests.get(key) != claim.task.module_digest:
            self.fail(
                envelope,
                {"reason": f"module digest mismatch for pinned task {key.as_string()}"},
                retry=False,
            )
            raise RuntimeContractError(f"module digest mismatch for pinned task {key.as_string()}")
        if not value_matches_type(input_data, module.input_type):
            diagnostic = {"reason": "persisted task input violates the module input contract"}
            self.fail(envelope, diagnostic, retry=False)
            raise RuntimeContractError(diagnostic["reason"])
        metadata: dict[str, Any] = {}
        try:
            broker = AccessBroker(
                self.connectors,
                declarations=cast(tuple[Capability[Any, Any], ...], module.capabilities),
                effects=cast(tuple[EffectSpec[Any, Any], ...], module.effects),
                grant=claim.task.capability_grant,
                run_id=claim.task.run_id,
                task_id=claim.task.task_id,
                attempt_id=str(claim.attempt.attempt_id),
                module_id=claim.task.module_id,
                logical_step=claim.task.logical_step,
                policy=self.access_policy,
                audit=lambda event_type, payload: self.store.append_access_event(
                    claim.task.run_id,
                    event_type,
                    payload,
                    task_id=claim.task.task_id,
                    attempt_id=str(claim.attempt.attempt_id),
                ),
                metadata=metadata,
                _effect_operations=self.store,
                _claim=claim,
            )
        except Exception as exc:
            self.fail(
                envelope,
                {
                    "reason": "task access contract could not be bound",
                    "exception_type": type(exc).__qualname__,
                },
                retry=False,
            )
            raise RuntimeContractError("task access contract could not be bound") from exc
        context = ExecutionContext(
            run_id=claim.task.run_id,
            task_id=claim.task.task_id,
            step_instance_id=claim.task.step_instance_id,
            broker=broker,
            metadata=metadata,
        )
        started = time.perf_counter()
        try:
            output = await module.execute(input_data, context)
            if not value_matches_type(output, module.output_type):
                raise RuntimeContractError(
                    f"module {key_for(claim).as_string()} returned a value outside "
                    "its output contract"
                )
        except Exception as exc:
            retry = claim.attempt.attempt_number < self.max_attempts and (
                not isinstance(exc, AccessContractError) or exc.retryable
            )
            self.fail(
                envelope,
                {"reason": str(exc), "exception_type": type(exc).__qualname__},
                retry=retry,
            )
            raise
        elapsed_ms = (time.perf_counter() - started) * 1000
        output_value = self.store.values.encode(
            output,
            schema_digest=schema_digest(module.output_type),
        )
        usage_data = dict(metadata.get("usage", {}))
        usage_data.setdefault("latency_ms", elapsed_ms)
        effects = [_coerce_effect(item) for item in broker._boundary_effect_records()]
        if module.effectful and not module.effects and not broker._effect_called:
            effects.extend(
                (
                    EffectRecord(
                        EffectKind.ATTEMPTED,
                        claim.task.module_id,
                        "execute",
                        claim.task.input_value.digest,
                    ),
                    EffectRecord(
                        EffectKind.COMMITTED,
                        claim.task.module_id,
                        "execute",
                        claim.task.input_value.digest,
                        output_value.digest,
                    ),
                )
            )
        now = datetime.now(UTC).isoformat()
        boundary = BoundaryRecord(
            workflow_id=self.workflow_id,
            definition_digest=self.definition_digest,
            module_id=claim.task.module_id,
            logical_step=claim.task.logical_step,
            step_instance_id=claim.task.step_instance_id,
            module_digest=claim.task.module_digest,
            dependency_instance_keys=claim.task.dependency_instance_keys,
            input_value=claim.task.input_value,
            output_value=output_value,
            input_schema_digest=claim.task.input_value.schema_digest,
            output_schema_digest=output_value.schema_digest,
            accepted_attempt=AcceptedAttemptProvenance(
                claim.attempt.attempt_id,
                claim.attempt.attempt_number,
                claim.worker_id,
                claim.attempt.started_at.isoformat() if claim.attempt.started_at else now,
                now,
            ),
            trajectories=tuple(
                _coerce_trajectory(item) for item in metadata.get("trajectories", [])
            ),
            usage=Usage(**usage_data),
            branch_decisions=branch_decisions,
            map_decisions=map_decisions,
            effects=tuple(effects),
        )
        return self.complete(envelope, boundary)


class Executor(Protocol):
    """Infrastructure adapter that runs claim-capable workers.

    A conforming executor may use the current process, a container, a warm VM,
    or a remote worker. Workflow and scheduler semantics depend only on the
    worker protocol, never on how the environment is provisioned.
    """

    @property
    def worker_id(self) -> str:
        """Return the diagnostic identity used for physical attempts."""
        ...

    @property
    def capabilities(self) -> ExecutorCapabilities:
        """Return environments and resources this executor can satisfy."""
        ...

    async def run_once(self) -> BoundaryRecord | None:
        """Claim, start, and finish at most one compatible task."""
        ...


class LocalExecutor:
    """Run the standard worker protocol in the current Python process.

    This executor is intended for development and tests. It does not bypass the
    durable queue: every module still starts from a claimable task envelope and
    commits through the same lease-token compare-and-swap boundary used by
    remote executors.
    """

    def __init__(self, worker: TaskWorker) -> None:
        self.worker = worker

    @property
    def worker_id(self) -> str:
        """Return the wrapped worker's diagnostic identity."""
        return self.worker.worker_id

    @property
    def capabilities(self) -> ExecutorCapabilities:
        """Return the wrapped worker's executor capabilities."""
        return self.worker.capabilities

    async def run_once(self) -> BoundaryRecord | None:
        """Execute at most one compatible durable task."""
        return await self.worker.run_once()


def key_for(claim: ClaimedTask) -> ReplayKey:
    """Return the exact replay key carried by a claimed task."""
    return ReplayKey(claim.task.module_id, claim.task.logical_step)


class WorkflowScheduler:
    """Advance a durable workflow graph without executing module handlers.

    A scheduler pass evaluates only control nodes whose durable dependencies
    have completed, materializes task inputs, and marks tasks ready. It never
    claims tasks and never calls :meth:`Module.execute`, so the process that
    submits a workflow has no execution affinity with any task it creates.
    """

    def __init__(
        self,
        store: DurableRuntimeStore,
        definition: BoundWorkflow,
        *,
        run_id: str,
        tenant_id: str,
    ) -> None:
        self.store = store
        self.definition = definition
        self.plan = definition.plan
        # Compatibility for callers that still inspect the native symbolic
        # graph. Scheduling itself uses only the compiled plan.
        self.output = definition._authoring_output
        self.run_id = run_id
        self.tenant_id = tenant_id
        self.steps = {step.node_id: step for step in self.plan.steps}
        self._tasks: dict[tuple[str, str, str], Task] = {}
        self._cache: dict[str, _Evaluation | None] = {}
        self._branch_context: dict[tuple[str, ...], tuple[dict[str, Any], ...]] = {}
        self._control_events: set[tuple[str, str]] = set()

    @classmethod
    def submit[InputT, OutputT](
        cls,
        store: DurableRuntimeStore,
        workflow: Workflow[InputT, OutputT] | BoundWorkflow,
        value: InputT,
        *,
        tenant_id: str = "local",
        execution_mode: ExecutionMode = ExecutionMode.LIVE,
    ) -> WorkflowScheduler:
        """Compile and durably submit a workflow without running its modules.

        Static module occurrences are created as blocked tasks in the same
        durable run. A later scheduler pass materializes root-ready inputs;
        executors may live in unrelated processes or machines.
        """
        definition = workflow if isinstance(workflow, BoundWorkflow) else bind_workflow(workflow)
        if not definition.accepts_input(value) or (
            definition.input_type is not Any
            and not value_matches_type(value, definition.input_type)
        ):
            raise RuntimeContractError("root input violates the workflow input contract")
        root_input = store.values.encode(value, schema_digest=schema_digest(definition.input_type))
        run = store.create_run(
            definition.plan,
            tenant_id=tenant_id,
            root_input=root_input,
            execution_mode=execution_mode,
        )
        scheduler = cls(
            store,
            definition,
            run_id=run.run_id,
            tenant_id=tenant_id,
        )
        scheduler._precreate_static_tasks()
        return scheduler

    @classmethod
    def resume(
        cls,
        store: DurableRuntimeStore,
        workflow: Workflow[Any, Any] | BoundWorkflow,
        run_id: str,
        *,
        tenant_id: str = "local",
    ) -> WorkflowScheduler:
        """Reconstruct a scheduler from durable state in a new process.

        The supplied workflow must compile to the definition digest pinned by
        the run. No module handler is executed while reconstructing control
        state.
        """
        definition = workflow if isinstance(workflow, BoundWorkflow) else bind_workflow(workflow)
        history = store.load_run_history(run_id, tenant_id=tenant_id)
        if history.run.definition_digest != definition.plan.digest:
            raise RuntimeContractError("workflow definition does not match the submitted run")
        return cls(
            store,
            definition,
            run_id=run_id,
            tenant_id=tenant_id,
        )

    def advance(self) -> ScheduleProgress:
        """Perform one idempotent scheduling pass and return current progress."""
        history = self.store.load_run_history(self.run_id, tenant_id=self.tenant_id)
        if history.run.status is RunStatus.SUCCEEDED:
            if history.run.root_output is None:
                raise RuntimeContractError("successful run has no root output")
            output = _rehydrate(
                self.store.values.decode(history.run.root_output), self.definition.output_type
            )
            return ScheduleProgress(self.run_id, RunStatus.SUCCEEDED, 0, output)
        if history.run.status is not RunStatus.RUNNING:
            return ScheduleProgress(self.run_id, history.run.status, 0)

        self._tasks = {
            (task.module_id, task.logical_step, task.step_instance_id): task
            for task in history.tasks
        }
        self._cache = {}
        self._branch_context = {}
        self._control_events = {
            (event.event_type, str(event.payload.get("control_node")))
            for event in history.events
            if event.event_type in {"BRANCH_DECISION", "MAP_DECISION"}
        }
        root_value = _rehydrate(
            self.store.values.decode(history.run.root_input), self.definition.input_type
        )
        try:
            result = self._evaluate_node(
                self.plan.output_node,
                external=_Evaluation(root_value),
                scope=(),
            )
        except RuntimeExecutionError as exc:
            self.store.fail_run(
                self.run_id,
                {"reason": str(exc), "exception_type": type(exc).__qualname__},
            )
            raise

        if result is not None:
            if not self.definition.accepts_output(result.value):
                raise RuntimeContractError("root output violates the workflow output contract")
            root_output = self.store.values.encode(
                result.value, schema_digest=schema_digest(self.definition.output_type)
            )
            self.store.complete_run(self.run_id, root_output)
            return ScheduleProgress(self.run_id, RunStatus.SUCCEEDED, 0, result.value)

        refreshed = self.store.load_run_history(self.run_id, tenant_id=self.tenant_id)
        ready = sum(task.status is TaskStatus.READY for task in refreshed.tasks)
        return ScheduleProgress(self.run_id, refreshed.run.status, ready)

    def _precreate_static_tasks(self) -> None:
        seen: set[str] = set()

        def visit(node_id: str, scope: tuple[str, ...]) -> None:
            if node_id == "input" or node_id in seen:
                return
            seen.add(node_id)
            step = self.steps[node_id]
            scope = _with_nested_scope(node_id, scope)
            if step.kind == "when":
                visit(step.dependencies[0], scope)
                return
            if step.kind == "parallel":
                for index, dependency in enumerate(step.dependencies):
                    visit(dependency, (*scope, f"parallel:{index}"))
                return
            if step.kind == "map_module":
                visit(step.dependencies[0], scope)
                return
            if step.kind != "module":
                raise RuntimeContractError(f"unsupported runtime IR step {step.kind!r}")
            if step.input_binding is None:
                raise RuntimeContractError("module step has no input binding")
            for dependency in step.dependencies:
                visit(dependency, scope)
            self.store.enqueue_task(
                self.run_id,
                step,
                step_instance_id=_stable_instance_id(step, scope),
                input_value=None,
            )

        visit(self.plan.output_node, ())

    def _evaluate_node(
        self,
        node_id: str,
        *,
        external: _Evaluation,
        scope: tuple[str, ...],
    ) -> _Evaluation | None:
        if node_id == "input":
            return external
        if node_id in self._cache:
            return self._cache[node_id]
        result = self._evaluate_step(
            self.steps[node_id],
            external=external,
            scope=_with_nested_scope(node_id, scope),
        )
        self._cache[node_id] = result
        return result

    def _evaluate_step(
        self,
        step: StepIR,
        *,
        external: _Evaluation,
        scope: tuple[str, ...],
    ) -> _Evaluation | None:
        if step.kind == "module":
            if step.input_binding is None or step.replay_key is None:
                raise RuntimeContractError("module step has an incomplete definition")
            source = self._resolve_binding(
                step.input_binding,
                external=external,
                scope=scope,
            )
            if source is None:
                return None
            source = self._include_order_dependencies(step, source, external=external, scope=scope)
            if source is None:
                return None
            inherited = self._inherited_branches(scope)
            if inherited:
                source = dataclasses.replace(
                    source,
                    branch_decisions=(*source.branch_decisions, *inherited),
                )
            module = self.definition.modules[step.replay_key]
            hydrated = _rehydrate(source.value, module.input_type)
            if not value_matches_type(hydrated, module.input_type):
                raise RuntimeContractError(
                    f"resolved input for {step.replay_key.as_string()} violates its contract"
                )
            return self._module_result(
                step,
                module,
                dataclasses.replace(source, value=hydrated),
                scope,
            )
        if step.kind == "when":
            condition = self._evaluate_node(
                step.dependencies[0],
                external=external,
                scope=scope,
            )
            if condition is None:
                return None
            branch_index = 1 if bool(condition.value) else 2
            branch_decision = {
                "control_node": step.node_id,
                "selected": "true" if branch_index == 1 else "false",
            }
            branch_scope = (*scope, f"branch:{branch_index}")
            self._branch_context[branch_scope] = (branch_decision,)
            self._append_control_once("BRANCH_DECISION", step.node_id, branch_decision)
            branch = self._evaluate_node(
                step.dependencies[branch_index],
                external=external,
                scope=branch_scope,
            )
            if branch is None:
                return None
            return _Evaluation(
                branch.value,
                _unique((*condition.dependency_instance_keys, *branch.dependency_instance_keys)),
                (*condition.branch_decisions, *branch.branch_decisions, branch_decision),
                (*condition.map_decisions, *branch.map_decisions),
            )
        if step.kind == "parallel":
            branches = tuple(
                self._evaluate_node(
                    dependency,
                    external=external,
                    scope=(*scope, f"parallel:{index}"),
                )
                for index, dependency in enumerate(step.dependencies)
            )
            if any(branch is None for branch in branches):
                return None
            complete = cast(tuple[_Evaluation, ...], branches)
            return _Evaluation(
                tuple(branch.value for branch in complete),
                _unique(
                    tuple(
                        instance
                        for branch in complete
                        for instance in branch.dependency_instance_keys
                    )
                ),
                tuple(decision for branch in complete for decision in branch.branch_decisions),
                tuple(decision for branch in complete for decision in branch.map_decisions),
            )
        if step.kind == "map_module":
            if step.replay_key is None:
                raise RuntimeContractError("mapped module step has no replay key")
            source = self._evaluate_node(
                step.dependencies[0],
                external=external,
                scope=scope,
            )
            if source is None:
                return None
            order_source = self._include_order_dependencies(
                step, source, external=external, scope=scope
            )
            if order_source is None:
                return None
            source = order_source
            if not isinstance(source.value, (list, tuple)):
                raise RuntimeContractError("map_over input must evaluate to a sequence")
            key_binding = self._map_key_binding(step)
            keyed = [(self._item_key(item, key_binding), item) for item in source.value]
            keys = [item_key for item_key, _ in keyed]
            if len(keys) != len(set(keys)):
                raise RuntimeContractError("map_over item keys must be unique within an execution")
            map_decision: dict[str, Any] = {"control_node": step.node_id, "item_keys": keys}
            self._append_control_once("MAP_DECISION", step.node_id, map_decision)
            mapped: list[Any] = []
            boundaries: list[str] = []
            pending = False
            for item_key, item in keyed:
                item_source = _Evaluation(
                    item,
                    source.dependency_instance_keys,
                    source.branch_decisions,
                    (*source.map_decisions, map_decision),
                )
                item_result = self._module_result(
                    step,
                    self.definition.modules[step.replay_key],
                    item_source,
                    (*scope, f"item:{item_key}"),
                )
                if item_result is None:
                    pending = True
                    continue
                mapped.append(item_result.value)
                boundaries.extend(item_result.dependency_instance_keys)
            if pending:
                return None
            return _Evaluation(
                mapped,
                tuple(boundaries),
                source.branch_decisions,
                (*source.map_decisions, map_decision),
            )
        raise RuntimeContractError(f"unsupported runtime IR step {step.kind!r}")

    def _include_order_dependencies(
        self,
        step: StepIR,
        source: _Evaluation,
        *,
        external: _Evaluation,
        scope: tuple[str, ...],
    ) -> _Evaluation | None:
        data_nodes = set(step.input_binding.source_nodes if step.input_binding else ())
        ordered = tuple(
            self._evaluate_node(node, external=external, scope=scope)
            for node in step.dependencies
            if node not in data_nodes
        )
        if any(item is None for item in ordered):
            return None
        complete = cast(tuple[_Evaluation, ...], ordered)
        return dataclasses.replace(
            source,
            dependency_instance_keys=_unique(
                (
                    *source.dependency_instance_keys,
                    *(key for item in complete for key in item.dependency_instance_keys),
                )
            ),
            branch_decisions=(
                *source.branch_decisions,
                *(decision for item in complete for decision in item.branch_decisions),
            ),
            map_decisions=(
                *source.map_decisions,
                *(decision for item in complete for decision in item.map_decisions),
            ),
        )

    def _resolve_binding(
        self,
        binding: BindingIR,
        *,
        external: _Evaluation,
        scope: tuple[str, ...],
    ) -> _Evaluation | None:
        if binding.kind in {"source", "field"}:
            if binding.source is None:
                raise RuntimeContractError("source binding has no node identifier")
            source = self._evaluate_node(binding.source, external=external, scope=scope)
            if source is None or binding.kind == "source":
                return source
            value = source.value
            try:
                for name in binding.path:
                    value = value[name] if isinstance(value, Mapping) else getattr(value, name)
            except (AttributeError, KeyError, TypeError) as exc:
                raise RuntimeContractError(
                    f"field binding {'.'.join(binding.path)!r} is unavailable"
                ) from exc
            return dataclasses.replace(source, value=value)
        if binding.kind == "literal":
            return _Evaluation(binding.value)
        children: tuple[tuple[str | None, BindingIR], ...]
        if binding.kind == "object":
            children = tuple((name, child) for name, child in binding.fields)
        elif binding.kind in {"list", "tuple"}:
            children = tuple((None, child) for child in binding.items)
        else:
            raise RuntimeContractError(f"unsupported input binding {binding.kind!r}")
        resolved = tuple(
            self._resolve_binding(child, external=external, scope=scope) for _, child in children
        )
        if any(item is None for item in resolved):
            return None
        complete = cast(tuple[_Evaluation, ...], resolved)
        if binding.kind == "object":
            resolved_value: Any = {
                cast(str, name): item.value
                for (name, _), item in zip(children, complete, strict=True)
            }
        elif binding.kind == "tuple":
            resolved_value = tuple(item.value for item in complete)
        else:
            resolved_value = [item.value for item in complete]
        return _Evaluation(
            value=resolved_value,
            dependency_instance_keys=_unique(
                tuple(key for item in complete for key in item.dependency_instance_keys)
            ),
            branch_decisions=tuple(
                decision for item in complete for decision in item.branch_decisions
            ),
            map_decisions=tuple(decision for item in complete for decision in item.map_decisions),
        )

    def _map_key_binding(self, step: StepIR) -> str | Callable[[Any], str]:
        bound = self.definition.map_item_keys
        if bound is not None and step.node_id in bound:
            return bound[step.node_id]
        item_key = (step.control or {}).get("item_key")
        if isinstance(item_key, Mapping) and isinstance(item_key.get("field"), str):
            return cast(str, item_key["field"])
        raise RuntimeContractError(
            f"map step {step.node_id!r} requires a trusted item-key callback binding"
        )

    def _module_result(
        self,
        step: StepIR,
        module: Module[Any, Any],
        source: _Evaluation,
        scope: tuple[str, ...],
    ) -> _Evaluation | None:
        if step.replay_key is None:  # pragma: no cover - compiler contract
            raise RuntimeContractError("module step has no replay key")
        instance_id = _stable_instance_id(step, scope)
        identity = (step.replay_key.module_id, step.replay_key.logical_step, instance_id)
        task = self._tasks.get(identity)
        input_value = self.store.values.encode(
            source.value, schema_digest=schema_digest(module.input_type)
        )
        if task is None:
            task = self.store.enqueue_task(
                self.run_id,
                step,
                step_instance_id=instance_id,
                input_value=input_value,
                dependency_instance_keys=source.dependency_instance_keys,
                branch_decisions=source.branch_decisions,
                map_decisions=source.map_decisions,
            )
            self._tasks[identity] = task
            return None
        if task.status is TaskStatus.BLOCKED:
            self.store.ready_task(
                task.task_id,
                input_value=input_value,
                dependency_instance_keys=source.dependency_instance_keys,
                branch_decisions=source.branch_decisions,
                map_decisions=source.map_decisions,
            )
            return None
        if task.status is TaskStatus.FAILED:
            raise RuntimeExecutionError(f"task {task.task_id} failed permanently")
        if task.status is not TaskStatus.SUCCEEDED:
            return None
        if task.accepted_boundary is None:
            raise RuntimeContractError(f"successful task {task.task_id} has no accepted boundary")
        output = _rehydrate(
            self.store.values.decode(task.accepted_boundary.output_value), module.output_type
        )
        return _Evaluation(output, (task.accepted_boundary.instance_key,))

    def _append_control_once(self, event_type: str, node_id: str, payload: dict[str, Any]) -> None:
        identity = (event_type, node_id)
        if identity in self._control_events:
            return
        self.store.append_event(self.run_id, event_type, payload)
        self._control_events.add(identity)

    def _inherited_branches(self, scope: tuple[str, ...]) -> tuple[dict[str, Any], ...]:
        return tuple(
            decision
            for context_scope, decisions in self._branch_context.items()
            if scope[: len(context_scope)] == context_scope
            for decision in decisions
        )

    @staticmethod
    def _item_key(item: Any, key: str | Callable[[Any], str]) -> str:
        try:
            if isinstance(key, str):
                raw = item.get(key) if isinstance(item, Mapping) else getattr(item, key, None)
            else:
                raw = key(item)
        except Exception as exc:
            raise RuntimeContractError(f"map_over item-key callback failed: {exc}") from exc
        if raw is None or not str(raw):
            raise RuntimeContractError("map_over produced an empty item key")
        return str(raw)


class WorkflowRunner:
    """Run the durable scheduler and local executor as a development convenience.

    Production deployments should run :class:`WorkflowScheduler` and workers in
    separate processes. This facade preserves the same queue, lease, and task
    envelope semantics while hosting both roles in one process for local use.

    Parameters
    ----------
    store
        Durable runtime store used by both the scheduler and local worker.
    worker_id
        Diagnostic worker identity recorded on physical attempts.
    max_attempts
        Maximum physical attempts allowed for each logical task result.
    connectors
        Provider-neutral read/effect adapter registry. The runner creates a fresh
        :class:`~maida.workflows.access.AccessBroker` for every task attempt;
        adapters and their credentials are never added to task envelopes.
    access_policy
        Optional policy that may narrow, but cannot expand, the access grant
        compiled and persisted for each task. Approval-required effects also
        need a durable evidence reference verified against that same task.

    Notes
    -----
    ``ExecutionSpec.capabilities`` controls which executor may claim a task. It
    is intentionally separate from connector authorization, which is derived
    exclusively from the module's compiled capability and effect declarations.
    """

    def __init__(
        self,
        store: DurableRuntimeStore,
        *,
        worker_id: str = "local-worker",
        max_attempts: int = 3,
        connectors: ConnectorRegistry | None = None,
        access_policy: AccessPolicy | None = None,
    ) -> None:
        self.store = store
        self.worker_id = worker_id
        self.max_attempts = max_attempts
        self.connectors = connectors or ConnectorRegistry()
        self.access_policy = access_policy

    async def run[InputT, OutputT](
        self,
        workflow: Workflow[InputT, OutputT] | BoundWorkflow,
        value: InputT,
        *,
        tenant_id: str = "local",
        execution_mode: ExecutionMode = ExecutionMode.LIVE,
    ) -> RunResult:
        """Submit and drain a process-isolated workflow through durable tasks."""
        definition = workflow if isinstance(workflow, BoundWorkflow) else bind_workflow(workflow)
        scheduler = WorkflowScheduler.submit(
            self.store,
            definition,
            value,
            tenant_id=tenant_id,
            execution_mode=execution_mode,
        )
        executor = LocalExecutor(
            TaskWorker(
                self.store,
                workflow_id=scheduler.plan.workflow_id,
                definition_digest=scheduler.plan.digest,
                modules=definition.modules,
                worker_id=self.worker_id,
                max_attempts=self.max_attempts,
                capabilities=ExecutorCapabilities.local_process(),
                connectors=self.connectors,
                access_policy=self.access_policy,
            )
        )
        last_error: Exception | None = None
        while True:
            try:
                progress = scheduler.advance()
            except RuntimeExecutionError:
                if last_error is not None:
                    raise last_error from None
                raise
            if progress.status is RunStatus.SUCCEEDED:
                return RunResult(scheduler.run_id, progress.output, scheduler.plan.digest)
            if progress.status is RunStatus.FAILED:
                if last_error is not None:
                    raise last_error
                raise RuntimeExecutionError(f"run {scheduler.run_id} failed")
            try:
                boundary = await executor.run_once()
            except Exception as exc:
                last_error = exc
                continue
            if boundary is None:
                raise RuntimeExecutionError(
                    "no local executor can claim the remaining durable tasks; "
                    "start an executor matching their execution requirements"
                )


def _unique(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(values))


def _with_nested_scope(node_id: str, scope: tuple[str, ...]) -> tuple[str, ...]:
    nested = tuple(f"workflow:{name}" for name in _NESTED_SCOPE_PATTERN.findall(node_id))
    return (*scope, *(item for item in nested if item not in scope))
