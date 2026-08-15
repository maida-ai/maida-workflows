"""Execute compiled workflows as durable typed module boundaries.

The runtime records runs, tasks, attempts, accepted boundary values, usage, and
control decisions through a :class:`DurableRuntimeStore`. Most applications use
:class:`WorkflowRunner`; worker services use :class:`TaskWorker` to claim and
complete already-created tasks.
"""

from __future__ import annotations

import asyncio
import dataclasses
import time
import types
import typing
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

from ._canonical import digest_data, schema_digest, value_matches_type
from .authoring import (
    ExecutionContext,
    Module,
    RuntimeValue,
    Workflow,
    _MapBinding,
    _ModuleBinding,
    _WorkflowBinding,
)
from .ir import PlanIR, ReplayKey, StepIR, _compile_workflow_graph, module_digest
from .models import (
    AcceptedAttemptProvenance,
    BoundaryRecord,
    EffectKind,
    EffectRecord,
    ExecutionMode,
    StoredValue,
    TrajectoryRecord,
    Usage,
)
from .persistence import ClaimedTask


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
        input_value: StoredValue,
        dependency_instance_keys: tuple[str, ...] = (),
        task_id: str | None = None,
    ) -> Any:
        """Create or return one durable logical task for an execution instance."""
        ...

    def claim_task(
        self,
        *,
        worker_id: str,
        lease_for: timedelta = timedelta(minutes=5),
        task_id: str | None = None,
    ) -> ClaimedTask | None:
        """Lease one pending task, optionally restricted to a task ID."""
        ...

    def complete_task(self, claim: ClaimedTask, boundary: BoundaryRecord) -> None:
        """Accept a boundary result using the claim's compare-and-swap token."""
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

    async def run_once(self, *, task_id: str | None = None) -> BoundaryRecord | None:
        """Claim and execute at most one durable task.

        Parameters
        ----------
        task_id
            Optional exact task to claim; otherwise any eligible task may be
            leased.

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
        claim = self.store.claim_task(worker_id=self.worker_id, task_id=task_id)
        if claim is None:
            return None
        key = ReplayKey(claim.task.module_id, claim.task.logical_step)
        try:
            module = self.modules[key]
        except KeyError as exc:
            self.store.fail_task(
                claim,
                {"reason": f"no module registered for {key.as_string()}"},
                retry=False,
            )
            raise RuntimeContractError(f"no module registered for {key.as_string()}") from exc
        input_data = _rehydrate(self.store.values.decode(claim.task.input_value), module.input_type)
        return await self._execute_claim(module, claim, input_data)

    async def _execute_claim(
        self,
        module: Module[Any, Any],
        claim: ClaimedTask,
        input_data: Any,
        *,
        branch_decisions: tuple[dict[str, Any], ...] = (),
        map_decisions: tuple[dict[str, Any], ...] = (),
        broker: Any = None,
    ) -> BoundaryRecord:
        key = key_for(claim)
        if self.module_digests.get(key) != claim.task.module_digest:
            self.store.fail_task(
                claim,
                {"reason": f"module digest mismatch for pinned task {key.as_string()}"},
                retry=False,
            )
            raise RuntimeContractError(f"module digest mismatch for pinned task {key.as_string()}")
        if not value_matches_type(input_data, module.input_type):
            diagnostic = {"reason": "persisted task input violates the module input contract"}
            self.store.fail_task(claim, diagnostic, retry=False)
            raise RuntimeContractError(diagnostic["reason"])
        metadata: dict[str, Any] = {}
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
            retry = claim.attempt.attempt_number < self.max_attempts
            self.store.fail_task(
                claim,
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
        effects = [_coerce_effect(item) for item in metadata.get("effects", [])]
        if module.effectful:
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
        self.store.complete_task(claim, boundary)
        return boundary


def key_for(claim: ClaimedTask) -> ReplayKey:
    """Return the exact replay key carried by a claimed task."""
    return ReplayKey(claim.task.module_id, claim.task.logical_step)


class WorkflowRunner:
    """Compile and execute a workflow while recording durable history.

    Parameters
    ----------
    store
        Durable runtime store used for definitions, runs, tasks, and values.
    worker_id
        Diagnostic identity attached to locally executed attempts.
    max_attempts
        Maximum attempts allowed for each logical task.
    broker
        Optional runtime-managed broker exposed through :class:`ExecutionContext`.
    """

    def __init__(
        self,
        store: DurableRuntimeStore,
        *,
        worker_id: str = "local-worker",
        max_attempts: int = 3,
        broker: Any = None,
    ) -> None:
        self.store = store
        self.worker_id = worker_id
        self.max_attempts = max_attempts
        self.broker = broker

    async def run[InputT, OutputT](
        self,
        workflow: Workflow[InputT, OutputT],
        value: InputT,
        *,
        tenant_id: str = "local",
        execution_mode: ExecutionMode = ExecutionMode.LIVE,
    ) -> RunResult:
        """Execute one workflow from a concrete root input.

        Parameters
        ----------
        workflow
            Typed workflow instance to compile and execute.
        value
            Concrete root input matching ``workflow.input_type``.
        tenant_id
            Tenant scope recorded on the durable run.
        execution_mode
            Live or verification-live mode stored with the run.

        Returns
        -------
        RunResult
            Durable run ID, concrete terminal output, and definition digest.

        Raises
        ------
        RuntimeContractError
            If root or module values violate declared contracts.
        RuntimeExecutionError
            If a task cannot be claimed or completed successfully.
        """
        if not value_matches_type(value, workflow.input_type):
            raise RuntimeContractError("root input violates the workflow input contract")
        compiled = _compile_workflow_graph(workflow)
        plan = compiled.plan
        root_input = self.store.values.encode(
            value, schema_digest=schema_digest(workflow.input_type)
        )
        run = self.store.create_run(
            plan,
            tenant_id=tenant_id,
            root_input=root_input,
            execution_mode=execution_mode,
        )
        evaluator = _WorkflowEvaluator(
            self.store,
            plan,
            run.run_id,
            workflow,
            self.worker_id,
            self.max_attempts,
            self.broker,
        )
        try:
            result = await evaluator.evaluate(compiled.output, value)
            root_output = self.store.values.encode(
                result.value,
                schema_digest=schema_digest(workflow.output_type),
            )
            self.store.complete_run(run.run_id, root_output)
        except Exception as exc:
            self.store.fail_run(
                run.run_id,
                {"reason": str(exc), "exception_type": type(exc).__qualname__},
            )
            raise
        return RunResult(run.run_id, result.value, plan.digest)


class _WorkflowEvaluator:
    def __init__(
        self,
        store: DurableRuntimeStore,
        plan: PlanIR,
        run_id: str,
        workflow: Workflow[Any, Any],
        worker_id: str,
        max_attempts: int,
        broker: Any,
    ) -> None:
        self.store = store
        self.plan = plan
        self.run_id = run_id
        self.root_workflow = workflow
        self.worker_id = worker_id
        self.max_attempts = max_attempts
        self.broker = broker
        self.steps = {step.node_id: step for step in plan.steps}
        self.cache: dict[int, _Evaluation] = {}
        self.cache_locks: dict[int, asyncio.Lock] = {}
        self.branch_context: dict[tuple[str, ...], tuple[dict[str, Any], ...]] = {}

    async def evaluate(self, output: RuntimeValue[Any], root_input: Any) -> _Evaluation:
        return await self._visit(
            output,
            path="root",
            workflow=self.root_workflow,
            external=_Evaluation(root_input),
            scope=(),
        )

    async def _visit(
        self,
        value: RuntimeValue[Any],
        *,
        path: str,
        workflow: Workflow[Any, Any],
        external: _Evaluation,
        scope: tuple[str, ...],
    ) -> _Evaluation:
        if value._expression.kind == "input":
            return external
        cache_key = id(value)
        lock = self.cache_locks.setdefault(cache_key, asyncio.Lock())
        async with lock:
            if cache_key in self.cache:
                return self.cache[cache_key]
            result = await self._visit_uncached(
                value,
                path=path,
                workflow=workflow,
                external=external,
                scope=scope,
            )
            self.cache[cache_key] = result
            return result

    async def _visit_uncached(
        self,
        value: RuntimeValue[Any],
        *,
        path: str,
        workflow: Workflow[Any, Any],
        external: _Evaluation,
        scope: tuple[str, ...],
    ) -> _Evaluation:
        expression = value._expression
        if expression.kind == "input":
            return external
        if expression.kind == "workflow":
            source = await self._visit(
                expression.dependencies[0],
                path=f"{path}.input",
                workflow=workflow,
                external=external,
                scope=scope,
            )
            binding = cast(_WorkflowBinding, expression.payload)
            nested = binding.workflow
            result = await self._visit(
                binding.output,
                path=f"{path}.nested[{nested.workflow_id}]",
                workflow=nested,
                external=source,
                scope=(*scope, f"workflow:{nested.workflow_id}"),
            )
            return result
        if expression.kind == "module":
            source = await self._visit(
                expression.dependencies[0],
                path=f"{path}.dep0",
                workflow=workflow,
                external=external,
                scope=scope,
            )
            module_binding = cast(_ModuleBinding, expression.payload)
            inherited_branches = self.branch_context.get(scope, ())
            if inherited_branches:
                source = dataclasses.replace(
                    source,
                    branch_decisions=(*source.branch_decisions, *inherited_branches),
                )
            result = await self._execute_module(
                self.steps[path],
                module_binding.module,
                source,
                scope,
            )
        elif expression.kind == "when":
            condition = await self._visit(
                expression.dependencies[0],
                path=f"{path}.dep0",
                workflow=workflow,
                external=external,
                scope=scope,
            )
            branch_index = 1 if bool(condition.value) else 2
            branch_decision = {
                "control_node": path,
                "selected": "true" if branch_index == 1 else "false",
            }
            branch_scope = (*scope, f"branch:{branch_index}")
            self.branch_context[branch_scope] = (branch_decision,)
            self.store.append_event(self.run_id, "BRANCH_DECISION", branch_decision)
            branch = await self._visit(
                expression.dependencies[branch_index],
                path=f"{path}.dep{branch_index}",
                workflow=workflow,
                external=external,
                scope=branch_scope,
            )
            result = _Evaluation(
                branch.value,
                _unique((*condition.dependency_instance_keys, *branch.dependency_instance_keys)),
                (*condition.branch_decisions, *branch.branch_decisions, branch_decision),
                (*condition.map_decisions, *branch.map_decisions),
            )
        elif expression.kind == "parallel":
            branches = await asyncio.gather(
                *(
                    self._visit(
                        dependency,
                        path=f"{path}.dep{index}",
                        workflow=workflow,
                        external=external,
                        scope=(*scope, f"parallel:{index}"),
                    )
                    for index, dependency in enumerate(expression.dependencies)
                )
            )
            result = _Evaluation(
                tuple(branch.value for branch in branches),
                _unique(
                    tuple(
                        instance
                        for branch in branches
                        for instance in branch.dependency_instance_keys
                    )
                ),
                tuple(decision for branch in branches for decision in branch.branch_decisions),
                tuple(decision for branch in branches for decision in branch.map_decisions),
            )
        elif expression.kind == "map":
            source = await self._visit(
                expression.dependencies[0],
                path=f"{path}.dep0",
                workflow=workflow,
                external=external,
                scope=scope,
            )
            map_binding = cast(_MapBinding, expression.payload)
            if not isinstance(source.value, (list, tuple)):
                raise RuntimeContractError("map_over input must evaluate to a sequence")
            keyed = [(self._item_key(item, map_binding.item_key), item) for item in source.value]
            keys = [item_key for item_key, _ in keyed]
            if len(keys) != len(set(keys)):
                raise RuntimeContractError("map_over item keys must be unique within an execution")
            map_decision: dict[str, Any] = {"control_node": path, "item_keys": keys}
            self.store.append_event(self.run_id, "MAP_DECISION", map_decision)
            mapped = []
            boundaries: list[str] = []
            for item_key, item in keyed:
                item_source = _Evaluation(
                    item,
                    source.dependency_instance_keys,
                    source.branch_decisions,
                    (*source.map_decisions, map_decision),
                )
                item_result = await self._execute_module(
                    self.steps[path],
                    map_binding.module,
                    item_source,
                    (*scope, f"item:{item_key}"),
                )
                mapped.append(item_result.value)
                boundaries.extend(item_result.dependency_instance_keys)
            result = _Evaluation(
                mapped,
                tuple(boundaries),
                source.branch_decisions,
                (*source.map_decisions, map_decision),
            )
        else:  # pragma: no cover - the compiler rejects this first
            raise RuntimeContractError(f"unsupported runtime expression {expression.kind!r}")
        return result

    async def _execute_module(
        self,
        step: StepIR,
        module: Module[Any, Any],
        source: _Evaluation,
        scope: tuple[str, ...],
    ) -> _Evaluation:
        step_instance_id = _stable_instance_id(step, scope)
        input_value = self.store.values.encode(
            source.value,
            schema_digest=schema_digest(module.input_type),
        )
        task = self.store.enqueue_task(
            self.run_id,
            step,
            step_instance_id=step_instance_id,
            input_value=input_value,
            dependency_instance_keys=source.dependency_instance_keys,
        )
        modules = {cast(ReplayKey, step.replay_key): module}
        worker = TaskWorker(
            self.store,
            workflow_id=self.plan.workflow_id,
            definition_digest=self.plan.digest,
            modules=modules,
            worker_id=self.worker_id,
            max_attempts=self.max_attempts,
        )
        while True:
            claim = self.store.claim_task(worker_id=self.worker_id, task_id=task.task_id)
            if claim is None:
                raise RuntimeExecutionError(f"task {task.task_id} could not be claimed")
            try:
                boundary = await worker._execute_claim(
                    module,
                    claim,
                    source.value,
                    branch_decisions=source.branch_decisions,
                    map_decisions=source.map_decisions,
                    broker=self.broker,
                )
                return _Evaluation(
                    _rehydrate(self.store.values.decode(boundary.output_value), module.output_type),
                    (boundary.instance_key,),
                )
            except Exception:
                if claim.attempt.attempt_number >= self.max_attempts:
                    raise

    @staticmethod
    def _item_key(item: Any, key: str | Callable[[Any], str]) -> str:
        if isinstance(key, str):
            raw = item.get(key) if isinstance(item, Mapping) else getattr(item, key, None)
        else:
            raw = key(item)
        if raw is None or not str(raw):
            raise RuntimeContractError("map_over produced an empty item key")
        return str(raw)


def _unique(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(values))


def _rehydrate(value: Any, annotation: Any) -> Any:
    if dataclasses.is_dataclass(annotation) and isinstance(value, dict):
        hints = typing.get_type_hints(annotation)
        data_class = cast(Callable[..., Any], annotation)
        return data_class(
            **{
                field.name: _rehydrate(value[field.name], hints.get(field.name, Any))
                for field in dataclasses.fields(annotation)
                if field.name in value
            }
        )
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is list and isinstance(value, list):
        return [_rehydrate(item, args[0] if args else Any) for item in value]
    if origin is tuple and isinstance(value, list):
        return tuple(
            _rehydrate(item, args[index] if index < len(args) else Any)
            for index, item in enumerate(value)
        )
    if origin in (typing.Union, types.UnionType):
        for candidate in args:
            hydrated = _rehydrate(value, candidate)
            if value_matches_type(hydrated, candidate):
                return hydrated
    return value
