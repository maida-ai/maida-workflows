from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from functools import partial
from typing import Any, Protocol, cast

from ._canonical import digest_data, schema_digest, value_matches_type
from .alignment import GraphAligner, GraphChange, GraphDiff
from .authoring import (
    ExecutionContext,
    Module,
    RuntimeValue,
    Workflow,
    _MapBinding,
    _ModuleBinding,
)
from .fixture import ReplayFixture, _validate_loaded_integrity
from .ir import PlanIR, ReplayKey, compile_workflow
from .models import EffectKind, EffectRecord, Usage
from .runtime import _coerce_trajectory, _rehydrate


class ReplayMode(StrEnum):
    FULL_STUB = "full-stub"
    SELECTIVE = "selective"


class ReplayStatus(StrEnum):
    PASS = "PASS"
    CHANGED = "CHANGED"
    REPLAY_DIVERGENCE = "REPLAY_DIVERGENCE"
    REPLAY_LIVE_FAILURE = "REPLAY_LIVE_FAILURE"
    REPLAY_EFFECT_VIOLATION = "REPLAY_EFFECT_VIOLATION"
    REPLAY_BUDGET_EXCEEDED = "REPLAY_BUDGET_EXCEEDED"


class ReplayContractError(RuntimeError):
    pass


class ReplaySelectorError(ValueError):
    pass


class ReplayEffectViolation(RuntimeError):
    pass


@dataclass(frozen=True)
class ReplayBudget:
    max_cost_usd: float | None = None
    max_latency_ms: float | None = None
    max_model_calls: int | None = None


@dataclass(frozen=True)
class ReplayCase:
    fixture: ReplayFixture
    mode: ReplayMode
    live_steps: tuple[ReplayKey, ...] = ()
    budget: ReplayBudget | None = None


@dataclass(frozen=True)
class StepComparison:
    replay_key: ReplayKey
    step_instance_id: str
    executed_live: bool
    effect_stubbed: bool
    historical_output_digest: str
    current_output_digest: str
    output_changed: bool
    trajectory_changed: bool
    historical_usage: Usage
    current_usage: Usage
    trace_id: str | None = None


@dataclass(frozen=True)
class ReplayDivergence:
    change: GraphChange
    diff: GraphDiff


@dataclass(frozen=True)
class ReplayResult:
    status: ReplayStatus
    mode: ReplayMode
    output: Any = None
    comparisons: tuple[StepComparison, ...] = ()
    divergence: ReplayDivergence | None = None
    trace_ids: tuple[str, ...] = ()
    live_usage: Usage = field(default_factory=Usage)
    blocking: bool = False
    message: str = ""


class TraceBridge(Protocol):
    async def trace[OutputT](
        self, name: str, callback: Callable[[], Awaitable[OutputT]]
    ) -> tuple[OutputT, str | None]: ...


class MaidaTraceBridge:
    async def trace[OutputT](
        self, name: str, callback: Callable[[], Awaitable[OutputT]]
    ) -> tuple[OutputT, str | None]:
        from maida.tracing import traced_run  # type: ignore[import-untyped]
        from opentelemetry import trace

        with traced_run(name=name):
            context = trace.get_current_span().get_span_context()
            trace_id = format(context.trace_id, "032x") if context.is_valid else None
            return await callback(), trace_id


@dataclass(frozen=True)
class ReplayWorkerPolicy:
    grant: str = "replay-only"
    production_effect_adapters: tuple[str, ...] = ()
    allowed_environment: tuple[str, ...] = (
        "LANG",
        "LC_ALL",
        "PATH",
        "PYTHONPATH",
        "TZ",
    )

    def scrub_environment(self, environment: Mapping[str, str]) -> dict[str, str]:
        return {key: value for key, value in environment.items() if key in self.allowed_environment}


class ReplayBroker:
    def __init__(
        self,
        *,
        recorded_reads: Mapping[tuple[str, str], Any] | None = None,
        replay_safe_live_reads: Mapping[str, Callable[..., Awaitable[Any]]] | None = None,
    ) -> None:
        self.recorded_reads = dict(recorded_reads or {})
        self.replay_safe_live_reads = dict(replay_safe_live_reads or {})
        self.effect_attempts = 0

    async def effect(self, adapter: str, operation: str, request: Any) -> Any:
        self.effect_attempts += 1
        raise ReplayEffectViolation(
            f"supported effect path {adapter}.{operation} was attempted during replay"
        )

    async def read(
        self,
        adapter: str,
        operation: str,
        request: Any,
        *,
        allow_live: bool = False,
    ) -> Any:
        key = (adapter, digest_data({"operation": operation, "request": request}))
        if key in self.recorded_reads:
            return self.recorded_reads[key]
        live = self.replay_safe_live_reads.get(adapter)
        if allow_live and live is not None:
            return await live(operation, request)
        raise ReplayContractError(
            f"read {adapter}.{operation} has no recorded response or replay-safe live adapter"
        )


class ReplayEffectAdapter:
    """Compare a proposed effect without calling its handler or connector."""

    def validate(
        self,
        *,
        adapter: str,
        operation: str,
        request: Any,
        recorded: EffectRecord,
        recorded_result: Any = None,
    ) -> Any:
        proposed_digest = digest_data(request)
        if (
            recorded.kind is not EffectKind.ATTEMPTED
            or recorded.adapter != adapter
            or recorded.operation != operation
            or recorded.request_digest != proposed_digest
        ):
            raise ReplayContractError("proposed effect does not match the recorded effect boundary")
        return recorded_result if recorded_result is not None else {"replay_ack": True}


class ReplayEngine:
    def __init__(
        self,
        *,
        aligner: GraphAligner | None = None,
        trace_bridge: TraceBridge | None = None,
        broker_factory: Callable[[], ReplayBroker] = ReplayBroker,
        worker_policy: ReplayWorkerPolicy | None = None,
    ) -> None:
        self.aligner = aligner or GraphAligner()
        self.trace_bridge = trace_bridge or MaidaTraceBridge()
        self.broker_factory = broker_factory
        self.worker_policy = worker_policy or ReplayWorkerPolicy()
        if self.worker_policy.production_effect_adapters:
            raise ReplayContractError("replay workers cannot register production effect adapters")

    async def replay[InputT, OutputT](
        self,
        workflow: Workflow[InputT, OutputT],
        case: ReplayCase,
    ) -> ReplayResult:
        _validate_loaded_integrity(case.fixture)
        current = compile_workflow(workflow)
        alignment = self.aligner.align(case.fixture.workflow_ir, current)
        if alignment.diff.first_divergence is not None:
            change = alignment.diff.first_divergence
            return ReplayResult(
                status=ReplayStatus.REPLAY_DIVERGENCE,
                mode=case.mode,
                divergence=ReplayDivergence(change, alignment.diff),
                blocking=False,
                message=f"graph correspondence stopped at {change.location}",
            )
        self._validate_contracts(workflow, current, case.fixture)
        if case.mode is ReplayMode.FULL_STUB:
            if case.live_steps:
                raise ReplaySelectorError("full-stub replay does not accept live step selectors")
            output = _rehydrate(
                case.fixture.values.decode(case.fixture.root_output),
                workflow.output_type,
            )
            return ReplayResult(
                status=ReplayStatus.PASS,
                mode=case.mode,
                output=output,
                message="all accepted outputs injected; zero live boundaries executed",
            )
        if not case.live_steps:
            raise ReplaySelectorError("selective replay requires at least one exact live step")
        return await self._selective(workflow, current, case)

    def _validate_contracts(
        self,
        workflow: Workflow[Any, Any],
        current: PlanIR,
        fixture: ReplayFixture,
    ) -> None:
        if fixture.root_input.schema_digest != schema_digest(workflow.input_type):
            raise ReplayContractError(
                "recorded root input is incompatible with the current workflow"
            )
        if fixture.root_output.schema_digest != schema_digest(workflow.output_type):
            raise ReplayContractError(
                "recorded root output is incompatible with the current workflow"
            )
        current_by_key = {
            step.replay_key: step
            for step in current.executable_steps
            if step.replay_key is not None
        }
        seen_instances: set[str] = set()
        for boundary in fixture.boundaries:
            key = ReplayKey(boundary.module_id, boundary.logical_step)
            step = current_by_key[key]
            input_schema = step.input_binding.schema_digest if step.input_binding else None
            if (
                boundary.input_schema_digest != input_schema
                or boundary.output_schema_digest != step.output_schema_digest
            ):
                raise ReplayContractError(
                    f"recorded boundary {boundary.instance_key} cannot be injected "
                    "into current schemas"
                )
            if boundary.instance_key in seen_instances:
                raise ReplayContractError(f"duplicate executed instance {boundary.instance_key}")
            missing = set(boundary.dependency_instance_keys) - seen_instances
            if missing:
                raise ReplayContractError(
                    f"boundary {boundary.instance_key} has unavailable dependencies: "
                    f"{sorted(missing)}"
                )
            seen_instances.add(boundary.instance_key)

    async def _selective(
        self,
        workflow: Workflow[Any, Any],
        current: PlanIR,
        case: ReplayCase,
    ) -> ReplayResult:
        selected = set(case.live_steps)
        available = {
            step.replay_key for step in current.executable_steps if step.replay_key is not None
        }
        unknown = selected - available
        if unknown:
            labels = ", ".join(sorted(key.as_string() for key in unknown))
            raise ReplaySelectorError(f"unknown replay selector(s): {labels}")
        modules = build_module_registry(workflow, current)
        comparisons: list[StepComparison] = []
        traces: list[str] = []
        usage = Usage()
        changed = False
        broker = self.broker_factory()
        for boundary in case.fixture.boundaries:
            key = ReplayKey(boundary.module_id, boundary.logical_step)
            if key not in selected:
                continue
            module = modules[key]
            if module.effectful:
                comparisons.append(
                    StepComparison(
                        key,
                        boundary.step_instance_id,
                        False,
                        True,
                        boundary.output_value.digest,
                        boundary.output_value.digest,
                        False,
                        False,
                        boundary.usage,
                        Usage(),
                    )
                )
                continue
            recorded_input_bytes = case.fixture.values.bytes(boundary.input_value)
            input_data = _rehydrate(
                json_from_bytes(recorded_input_bytes),
                module.input_type,
            )
            if not value_matches_type(input_data, module.input_type):
                raise ReplayContractError(f"recorded input violates {key.as_string()} contract")
            metadata: dict[str, Any] = {}
            context = ExecutionContext(
                run_id=f"replay:{case.fixture.source.run_id}",
                task_id=f"replay:{boundary.instance_key}",
                step_instance_id=boundary.step_instance_id,
                replay=True,
                broker=broker,
                metadata=metadata,
            )

            started = time.perf_counter()
            try:
                output, trace_id = await self.trace_bridge.trace(
                    f"replay:{key.as_string()}",
                    partial(
                        _invoke_module,
                        module,
                        input_data,
                        context,
                        self.worker_policy,
                    ),
                )
            except ReplayEffectViolation as exc:
                return ReplayResult(
                    status=ReplayStatus.REPLAY_EFFECT_VIOLATION,
                    mode=case.mode,
                    comparisons=tuple(comparisons),
                    blocking=True,
                    message=str(exc),
                )
            except Exception as exc:
                return ReplayResult(
                    status=ReplayStatus.REPLAY_LIVE_FAILURE,
                    mode=case.mode,
                    comparisons=tuple(comparisons),
                    blocking=True,
                    message=f"{type(exc).__qualname__}: {exc}",
                )
            elapsed_ms = (time.perf_counter() - started) * 1000
            if not value_matches_type(output, module.output_type):
                raise ReplayContractError(
                    f"selective output from {key.as_string()} violates its current schema"
                )
            current_digest = digest_data(output)
            current_trajectories = tuple(
                _coerce_trajectory(item) for item in metadata.get("trajectories", [])
            )
            usage_data = dict(metadata.get("usage", {}))
            usage_data.setdefault("latency_ms", elapsed_ms)
            current_usage = Usage(**usage_data)
            trajectory_changed = current_trajectories != boundary.trajectories
            output_changed = current_digest != boundary.output_value.digest
            changed = changed or trajectory_changed or output_changed
            comparisons.append(
                StepComparison(
                    key,
                    boundary.step_instance_id,
                    True,
                    False,
                    boundary.output_value.digest,
                    current_digest,
                    output_changed,
                    trajectory_changed,
                    boundary.usage,
                    current_usage,
                    trace_id,
                )
            )
            if trace_id:
                traces.append(trace_id)
            usage = _add_usage(usage, current_usage)
        budget_message = _budget_violation(case.budget, usage, comparisons)
        output = _rehydrate(
            case.fixture.values.decode(case.fixture.root_output),
            workflow.output_type,
        )
        if budget_message:
            return ReplayResult(
                ReplayStatus.REPLAY_BUDGET_EXCEEDED,
                case.mode,
                output,
                tuple(comparisons),
                trace_ids=tuple(traces),
                live_usage=usage,
                blocking=True,
                message=budget_message,
            )
        return ReplayResult(
            ReplayStatus.CHANGED if changed else ReplayStatus.PASS,
            case.mode,
            output,
            tuple(comparisons),
            trace_ids=tuple(traces),
            live_usage=usage,
            blocking=False,
            message="downstream continuation used accepted historical outputs",
        )


def build_module_registry(
    workflow: Workflow[Any, Any], plan: PlanIR | None = None
) -> dict[ReplayKey, Module[Any, Any]]:
    compiled = plan or compile_workflow(workflow)
    steps = {step.node_id: step for step in compiled.executable_steps}
    found: dict[ReplayKey, Module[Any, Any]] = {}
    seen: set[tuple[int, str]] = set()

    def visit(value: RuntimeValue[Any], path: str, owner: Workflow[Any, Any]) -> None:
        marker = (id(value), path)
        if marker in seen:
            return
        seen.add(marker)
        expression = value._expression
        if expression.kind == "input":
            return
        if expression.kind == "workflow":
            visit(expression.dependencies[0], f"{path}.input", owner)
            nested = cast(Workflow[Any, Any], expression.payload)
            nested_output = nested.build(RuntimeValue.input(nested.input_type))
            visit(nested_output, f"{path}.nested[{nested.workflow_id}]", nested)
            return
        for index, dependency in enumerate(expression.dependencies):
            visit(dependency, f"{path}.dep{index}", owner)
        if expression.kind == "module":
            module_binding = cast(_ModuleBinding, expression.payload)
            key = steps[path].replay_key
            if key is not None:
                found[key] = module_binding.module
        elif expression.kind == "map":
            map_binding = cast(_MapBinding, expression.payload)
            key = steps[path].replay_key
            if key is not None:
                found[key] = map_binding.module

    visit(workflow.build(RuntimeValue.input(workflow.input_type)), "root", workflow)
    return found


def resolve_selectors(plan: PlanIR, selectors: Sequence[str]) -> tuple[ReplayKey, ...]:
    keys = tuple(step.replay_key for step in plan.executable_steps if step.replay_key is not None)
    selected: list[ReplayKey] = []
    for selector in selectors:
        prefix, separator, value = selector.partition(":")
        if not separator or prefix not in {"module", "step"} or not value:
            raise ReplaySelectorError("selectors must use module:ID or step:ID")
        matches = tuple(
            key
            for key in keys
            if (key.module_id == value if prefix == "module" else key.logical_step == value)
        )
        if not matches:
            raise ReplaySelectorError(f"selector {selector!r} matched no current step")
        if len(matches) > 1:
            raise ReplaySelectorError(
                f"selector {selector!r} is ambiguous; use an exact module/step pair"
            )
        selected.append(matches[0])
    return tuple(dict.fromkeys(selected))


def json_from_bytes(content: bytes) -> Any:
    import json

    return json.loads(content)


async def _invoke_module(
    module: Module[Any, Any],
    input_data: Any,
    context: ExecutionContext,
    policy: ReplayWorkerPolicy,
) -> Any:
    async with _REPLAY_ENVIRONMENT_LOCK:
        with _scrubbed_environment(policy):
            return await module.execute(input_data, context)


_REPLAY_ENVIRONMENT_LOCK = asyncio.Lock()


@contextmanager
def _scrubbed_environment(policy: ReplayWorkerPolicy) -> Iterator[None]:
    original = dict(os.environ)
    os.environ.clear()
    os.environ.update(policy.scrub_environment(original))
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(original)


def _add_usage(left: Usage, right: Usage) -> Usage:
    return Usage(
        left.input_tokens + right.input_tokens,
        left.output_tokens + right.output_tokens,
        left.cost_usd + right.cost_usd,
        left.latency_ms + right.latency_ms,
    )


def _budget_violation(
    budget: ReplayBudget | None,
    usage: Usage,
    comparisons: Sequence[StepComparison],
) -> str | None:
    if budget is None:
        return None
    if budget.max_cost_usd is not None and usage.cost_usd > budget.max_cost_usd:
        return f"live replay cost {usage.cost_usd} exceeded {budget.max_cost_usd}"
    if budget.max_latency_ms is not None and usage.latency_ms > budget.max_latency_ms:
        return f"live replay latency {usage.latency_ms}ms exceeded {budget.max_latency_ms}ms"
    if budget.max_model_calls is not None:
        model_calls = sum(
            1
            for comparison in comparisons
            if comparison.executed_live and comparison.current_usage.input_tokens > 0
        )
        if model_calls > budget.max_model_calls:
            return f"live replay model calls {model_calls} exceeded {budget.max_model_calls}"
    return None


def assert_replay_worker_environment(policy: ReplayWorkerPolicy | None = None) -> dict[str, str]:
    selected = policy or ReplayWorkerPolicy()
    if selected.production_effect_adapters:
        raise ReplayContractError("replay workers cannot register production effect adapters")
    return selected.scrub_environment(os.environ)
