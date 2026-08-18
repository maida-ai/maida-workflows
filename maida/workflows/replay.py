"""Replay accepted workflow boundaries without repeating production effects.

Full-stub replay validates and injects every recorded output on the historical
execution path. Selective replay executes only explicitly selected,
non-effectful module boundaries against their exact recorded inputs, compares
the new behavior, and continues downstream with historical outputs.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from functools import partial
from typing import Any, Protocol, cast

from ._canonical import canonical_json, digest_data, schema_digest, value_matches_type
from .alignment import GraphAligner, GraphChange, GraphDiff, project_execution_path
from .authoring import (
    ExecutionContext,
    Module,
    RuntimeValue,
    Workflow,
    _FieldBinding,
    _MapBinding,
    _ModuleBinding,
    _StructuredBinding,
    _WorkflowBinding,
)
from .budget import BudgetExceededError, BudgetUsage
from .dynamic import (
    PlanFragmentIR,
    PlanSignature,
    PlanValidationError,
    PlanValidator,
    _plan_from_signature,
)
from .fixture import ReplayFixture, _validate_loaded_integrity
from .interactions import _InteractionModule
from .ir import PlanIR, ReplayKey, _compile_workflow_graph, module_digest
from .model import ModelAdapterRegistry, ModelBroker, ModelSpec
from .models import BoundaryRecord, EffectKind, EffectRecord, Usage
from .runtime import _coerce_trajectory, _rehydrate, _stable_instance_id


class ReplayMode(StrEnum):
    """Supported strategy for substituting historical module boundaries."""

    FULL_STUB = "full-stub"
    SELECTIVE = "selective"


class ReplayStatus(StrEnum):
    """Outcome classification returned by :class:`ReplayEngine`."""

    PASS = "PASS"
    CHANGED = "CHANGED"
    REPLAY_DIVERGENCE = "REPLAY_DIVERGENCE"
    REPLAY_LIVE_FAILURE = "REPLAY_LIVE_FAILURE"
    REPLAY_EFFECT_VIOLATION = "REPLAY_EFFECT_VIOLATION"
    REPLAY_BUDGET_EXCEEDED = "REPLAY_BUDGET_EXCEEDED"


class ReplayContractError(RuntimeError):
    """Raised when fixture data cannot satisfy current workflow contracts."""


class ReplaySelectorError(ValueError):
    """Raised when a selective replay target is invalid or ambiguous."""


class ReplayEffectViolation(RuntimeError):
    """Raised when replay code attempts a runtime-managed external effect."""


@dataclass(frozen=True)
class ReplayBudget:
    """Optional limits applied only to selectively executed live work.

    Attributes
    ----------
    max_cost_usd
        Maximum cumulative cost reported by selected boundaries.
    max_latency_ms
        Maximum cumulative latency for selected boundaries.
    max_model_calls
        Maximum selected boundaries that report model-token usage.
    """

    max_cost_usd: float | None = None
    max_latency_ms: float | None = None
    max_model_calls: int | None = None


@dataclass(frozen=True)
class ReplayCase:
    """Fixture, mode, selectors, and optional budget for one replay test.

    Attributes
    ----------
    fixture
        Validated native replay fixture.
    mode
        Full-stub or selective substitution strategy.
    live_steps
        Exact replay keys to execute in selective mode.
    budget
        Optional limits for new selective execution only.
    """

    fixture: ReplayFixture
    mode: ReplayMode
    live_steps: tuple[ReplayKey, ...] = ()
    budget: ReplayBudget | None = None


@dataclass(frozen=True)
class StepComparison:
    """Historical-versus-current evidence for one selected boundary instance.

    Output and trajectory changes are recorded directly. Token, cost, and
    latency changes are derived from the two immutable usage records so callers
    can inspect each dimension without reimplementing comparison semantics.

    Notes
    -----
    :attr:`behavior_changed` includes output, trajectory, token, and cost drift.
    Raw wall-clock latency is exposed separately because ordinary scheduling
    noise should not turn every otherwise-identical replay into a behavioral
    change. Use :class:`ReplayBudget` or a verification policy when latency
    needs a blocking threshold.
    """

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

    @property
    def token_usage_changed(self) -> bool:
        """Return whether reported input or output token usage changed."""
        return (
            self.historical_usage.input_tokens != self.current_usage.input_tokens
            or self.historical_usage.output_tokens != self.current_usage.output_tokens
        )

    @property
    def cost_changed(self) -> bool:
        """Return whether reported monetary cost changed."""
        return self.historical_usage.cost_usd != self.current_usage.cost_usd

    @property
    def latency_changed(self) -> bool:
        """Return whether measured wall-clock latency changed."""
        return self.historical_usage.latency_ms != self.current_usage.latency_ms

    @property
    def usage_changed(self) -> bool:
        """Return whether any reported token, cost, or latency value changed."""
        return self.token_usage_changed or self.cost_changed or self.latency_changed

    @property
    def behavior_changed(self) -> bool:
        """Return whether output, trajectory, token, or cost behavior changed."""
        return (
            self.output_changed
            or self.trajectory_changed
            or self.token_usage_changed
            or self.cost_changed
        )


@dataclass(frozen=True)
class ReplayDivergence:
    """First unresolvable graph change and its complete structural diff.

    Attributes
    ----------
    change
        First insertion, deletion, reorder, topology, or control-flow mismatch.
    diff
        Structured changes observed before correspondence stopped.
    """

    change: GraphChange
    diff: GraphDiff


@dataclass(frozen=True)
class ReplayResult:
    """Deterministic outcome and evidence produced by a replay case.

    Attributes
    ----------
    status
        Replay outcome classification.
    mode
        Strategy used for this result.
    output
        Historical terminal output after validated continuation.
    comparisons
        Per-instance comparisons for selectively targeted boundaries.
    divergence
        Structured graph divergence when correspondence is impossible.
    trace_ids
        Maida trace identifiers created by selectively live attempts.
    live_usage
        Usage charged only by selectively executed boundaries.
    blocking
        Whether this result must fail verification under the active policy.
    message
        Human-readable result summary.
    """

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
    """Adapter that wraps selectively live attempts in a tracing system."""

    async def trace[OutputT](
        self, name: str, callback: Callable[[], Awaitable[OutputT]]
    ) -> tuple[OutputT, str | None]:
        """Execute ``callback`` and return its output with an optional trace ID."""
        ...


class MaidaTraceBridge:
    """Trace selectively executed boundaries with ordinary Maida tracing."""

    async def trace[OutputT](
        self, name: str, callback: Callable[[], Awaitable[OutputT]]
    ) -> tuple[OutputT, str | None]:
        """Execute a callback inside a Maida run and return its trace ID."""
        from maida.tracing import traced_run  # type: ignore[import-untyped]
        from opentelemetry import trace

        with traced_run(name=name):
            context = trace.get_current_span().get_span_context()
            trace_id = format(context.trace_id, "032x") if context.is_valid else None
            return await callback(), trace_id


@dataclass(frozen=True)
class ReplayWorkerPolicy:
    """Credential and adapter restrictions for selectively live execution.

    ``production_effect_adapters`` must remain empty. ``allowed_environment``
    names the small set of process variables copied into the replay attempt.
    """

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
        """Return only environment entries explicitly allowed for replay."""
        return {key: value for key, value in environment.items() if key in self.allowed_environment}


class ReplayBroker:
    """Serve recorded or explicitly replay-safe reads and deny every effect.

    Parameters
    ----------
    recorded_reads
        Responses keyed by ``(adapter, request_digest)``.
    replay_safe_live_reads
        Explicitly approved read-only adapters available when a call opts in.
    """

    def __init__(
        self,
        *,
        recorded_reads: Mapping[tuple[str, str], Any] | None = None,
        replay_safe_live_reads: Mapping[str, Callable[..., Awaitable[Any]]] | None = None,
    ) -> None:
        self.recorded_reads = dict(recorded_reads or {})
        self.replay_safe_live_reads = dict(replay_safe_live_reads or {})
        self.effect_attempts = 0

    async def effect(
        self,
        adapter: str,
        operation: str,
        request: Any,
        *,
        connector_version: str | None = None,
    ) -> Any:
        """Record an effect attempt and fail replay without calling an adapter."""
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
        connector_version: str | None = None,
        allow_live: bool = False,
    ) -> Any:
        """Return a recorded read or an explicitly approved live-read response.

        Raises
        ------
        ReplayContractError
            If no recorded response exists and no approved live-read adapter is
            both registered and requested.
        """
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
        connector_version: str | None = None,
        effect_name: str | None = None,
        ordinal: int | None = None,
    ) -> Any:
        """Compare a proposed effect with history and return a safe response.

        No connector or effect handler is invoked. A matching proposal returns
        ``recorded_result`` or a synthetic acknowledgement.

        Raises
        ------
        ReplayContractError
            If adapter, operation, request content, or any recorded logical
            effect identity differs from history. Legacy records without the
            newer identity fields retain their narrower comparison behavior.
        """
        proposed_digest = digest_data(request)
        if (
            recorded.kind is not EffectKind.ATTEMPTED
            or recorded.adapter != adapter
            or recorded.operation != operation
            or recorded.request_digest != proposed_digest
            or (
                recorded.connector_version is not None
                and recorded.connector_version != connector_version
            )
            or (recorded.effect_name is not None and recorded.effect_name != effect_name)
            or (recorded.ordinal is not None and recorded.ordinal != ordinal)
        ):
            raise ReplayContractError("proposed effect does not match the recorded effect boundary")
        return recorded_result if recorded_result is not None else {"replay_ack": True}


class _ReplayModelMeter:
    def __init__(self) -> None:
        self._reservations: dict[str, BudgetUsage] = {}
        self._ordinal = 0

    def reserve_model(self, name: str, usage: BudgetUsage) -> str:
        key = f"model:{self._ordinal}:{name}"
        self._ordinal += 1
        self._reservations[key] = usage
        return key

    def commit_model(self, reservation: str, usage: BudgetUsage) -> None:
        estimated = self._reservations.get(reservation)
        if estimated is None:
            raise BudgetExceededError("selective model usage was not reserved")
        if (
            usage.wall_time > estimated.wall_time
            or usage.model_tokens > estimated.model_tokens
            or usage.tool_calls > estimated.tool_calls
            or usage.cost_usd > estimated.cost_usd
        ):
            raise BudgetExceededError("selective model usage exceeded its reservation")


class ReplayEngine:
    """Validate graph correspondence and execute full or selective replay.

    Parameters
    ----------
    aligner
        Exact graph correspondence engine. Defaults to :class:`GraphAligner`.
    trace_bridge
        Trace adapter used only for selectively executed module attempts.
    broker_factory
        Factory for a fresh effect-denying :class:`ReplayBroker`.
    worker_policy
        Credential and adapter restrictions for live replay work.
    model_adapters
        Explicit replay-safe model providers available to selected modules.
    generated_validators
        Trusted current validators keyed by generated-region identity. A
        fixture containing generated plans cannot replay without the matching
        validator because imported resolved signatures are never authority.
    generated_modules
        Exact current module objects for generated replay keys. Full-stub uses
        them only to validate typed contracts; selective replay may execute an
        explicitly selected non-effectful boundary.

    Raises
    ------
    ReplayContractError
        If the worker policy registers a production effect adapter.
    """

    def __init__(
        self,
        *,
        aligner: GraphAligner | None = None,
        trace_bridge: TraceBridge | None = None,
        broker_factory: Callable[[], ReplayBroker] = ReplayBroker,
        worker_policy: ReplayWorkerPolicy | None = None,
        model_adapters: ModelAdapterRegistry | None = None,
        generated_validators: Mapping[str, PlanValidator] | None = None,
        generated_modules: Mapping[ReplayKey, Module[Any, Any]] | None = None,
    ) -> None:
        self.aligner = aligner or GraphAligner()
        self.trace_bridge = trace_bridge or MaidaTraceBridge()
        self.broker_factory = broker_factory
        self.worker_policy = worker_policy or ReplayWorkerPolicy()
        self.model_adapters = model_adapters or ModelAdapterRegistry()
        self.generated_validators = dict(generated_validators or {})
        self.generated_modules = dict(generated_modules or {})
        if self.worker_policy.production_effect_adapters:
            raise ReplayContractError("replay workers cannot register production effect adapters")

    async def replay[InputT, OutputT](
        self,
        workflow: Workflow[InputT, OutputT],
        case: ReplayCase,
    ) -> ReplayResult:
        """Replay a fixture against the current workflow definition.

        Parameters
        ----------
        workflow
            Current workflow whose contracts and identities are authoritative.
        case
            Fixture, replay mode, selective targets, and optional budget.

        Returns
        -------
        ReplayResult
            Validated historical output, comparisons, divergence, and usage.

        Raises
        ------
        ReplayFixtureError
            If fixture values or artifacts fail integrity validation.
        ReplayContractError
            If recorded data cannot be injected into current typed contracts.
        ReplaySelectorError
            If live selectors are missing, unknown, or invalid for the mode.

        Notes
        -----
        Full-stub mode invokes no module handlers. Selective mode never executes
        an ``effectful`` module and treats any broker effect attempt as a hard
        violation.
        """
        _validate_loaded_integrity(case.fixture)
        compiled = _compile_workflow_graph(workflow)
        current = compiled.plan
        historical_path = project_execution_path(
            case.fixture.workflow_ir,
            case.fixture.control_decisions,
        )
        current_path = project_execution_path(current, case.fixture.control_decisions)
        alignment = self.aligner.align(historical_path, current_path)
        if alignment.diff.first_divergence is not None:
            change = alignment.diff.first_divergence
            return ReplayResult(
                status=ReplayStatus.REPLAY_DIVERGENCE,
                mode=case.mode,
                divergence=ReplayDivergence(change, alignment.diff),
                blocking=False,
                message=f"graph correspondence stopped at {change.location}",
            )
        generated_signatures, generated_divergence = self._align_generated(case.fixture)
        if generated_divergence is not None:
            return ReplayResult(
                status=ReplayStatus.REPLAY_DIVERGENCE,
                mode=case.mode,
                divergence=generated_divergence,
                blocking=False,
                message=(
                    "generated graph correspondence stopped at "
                    f"{generated_divergence.change.location}"
                ),
            )
        self._validate_contracts(workflow, current, case.fixture)
        self._validate_generated_contracts(case.fixture, generated_signatures)
        historical_output = await _StubGraphExecutor(current, case.fixture).execute(
            workflow,
            compiled.output,
        )
        if case.mode is ReplayMode.FULL_STUB:
            if case.live_steps:
                raise ReplaySelectorError("full-stub replay does not accept live step selectors")
            return ReplayResult(
                status=ReplayStatus.PASS,
                mode=case.mode,
                output=historical_output,
                message="all accepted outputs injected; zero live boundaries executed",
            )
        if not case.live_steps:
            raise ReplaySelectorError("selective replay requires at least one exact live step")
        return await self._selective(
            workflow,
            current,
            case,
            historical_output,
            compiled.output,
            generated_signatures,
        )

    def _align_generated(
        self, fixture: ReplayFixture
    ) -> tuple[dict[str, PlanSignature], ReplayDivergence | None]:
        current: dict[str, PlanSignature] = {}
        for record in fixture.generated_plans:
            validator = self.generated_validators.get(record.region_id)
            if validator is None:
                raise ReplayContractError(
                    f"generated region {record.region_id!r} has no trusted current validator"
                )
            source = _boundary_by_instance(fixture, record.source_instance_key)
            fragment = _decode_fragment(fixture, source)
            try:
                signature = validator.validate(
                    fragment,
                    region_input_schema_digest=source.input_schema_digest,
                    expected_output_schema_digests=record.signature.output_schema_digests,
                )
            except PlanValidationError as exc:
                raise ReplayContractError(
                    f"generated region {record.region_id!r} no longer satisfies policy: {exc.code}"
                ) from exc
            alignment = self.aligner.align(
                _plan_from_signature(record.signature),
                _plan_from_signature(signature),
            )
            if alignment.diff.first_divergence is not None:
                change = alignment.diff.first_divergence
                return current, ReplayDivergence(change, alignment.diff)
            current[record.region_instance_id] = signature
        return current, None

    def _validate_generated_contracts(
        self,
        fixture: ReplayFixture,
        signatures: Mapping[str, PlanSignature],
    ) -> None:
        for record in fixture.generated_plans:
            signature = signatures[record.region_instance_id]
            descriptors = {
                cast(str, descriptor["key"]): descriptor for descriptor in signature.resolved_nodes
            }
            for node_key, instance_key in record.node_instances:
                boundary = _boundary_by_instance(fixture, instance_key)
                descriptor = descriptors[node_key]
                key = ReplayKey(
                    cast(str, descriptor["module_id"]),
                    f"dynamic/{record.region_id}/nodes/{node_key}",
                )
                module = self.generated_modules.get(key)
                if module is None:
                    raise ReplayContractError(
                        f"generated boundary {key.as_string()} has no current module binding"
                    )
                if module_digest(module) != descriptor["module_digest"]:
                    raise ReplayContractError(
                        f"generated module {key.as_string()} does not match its trusted catalog pin"
                    )
                if boundary.input_schema_digest != schema_digest(
                    module.input_type
                ) or boundary.output_schema_digest != schema_digest(module.output_type):
                    raise ReplayContractError(
                        f"generated boundary {boundary.instance_key} cannot be injected "
                        "into current schemas"
                    )
                recorded_input = _rehydrate(
                    fixture.values.decode(boundary.input_value), module.input_type
                )
                recorded_output = _rehydrate(
                    fixture.values.decode(boundary.output_value), module.output_type
                )
                if not value_matches_type(recorded_input, module.input_type):
                    raise ReplayContractError(f"recorded input violates {key.as_string()} contract")
                if not value_matches_type(recorded_output, module.output_type):
                    raise ReplayContractError(
                        f"recorded output violates {key.as_string()} contract"
                    )

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
        generated_instances = {
            instance_key
            for record in fixture.generated_plans
            for _node_key, instance_key in record.node_instances
        }
        seen_instances: set[str] = set()
        for boundary in fixture.boundaries:
            if boundary.instance_key in generated_instances:
                continue
            key = ReplayKey(boundary.module_id, boundary.logical_step)
            step = current_by_key.get(key)
            if step is None:
                raise ReplayContractError(
                    f"recorded boundary {boundary.instance_key} has no current replay key"
                )
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
        historical_output: Any,
        built_output: RuntimeValue[Any],
        generated_signatures: Mapping[str, PlanSignature],
    ) -> ReplayResult:
        selected = set(case.live_steps)
        available = {
            step.replay_key for step in current.executable_steps if step.replay_key is not None
        }
        available.update(self.generated_modules)
        unknown = selected - available
        if unknown:
            labels = ", ".join(sorted(key.as_string() for key in unknown))
            raise ReplaySelectorError(f"unknown replay selector(s): {labels}")
        executed = {
            ReplayKey(boundary.module_id, boundary.logical_step)
            for boundary in case.fixture.boundaries
        }
        unavailable = selected - executed
        if unavailable:
            labels = ", ".join(sorted(key.as_string() for key in unavailable))
            raise ReplaySelectorError(
                f"selected step(s) have no recorded execution in this fixture: {labels}"
            )
        modules = build_module_registry(workflow, current, output=built_output)
        modules.update(self.generated_modules)
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
            if module.effectful or isinstance(module, _InteractionModule):
                comparisons.append(
                    StepComparison(
                        key,
                        boundary.step_instance_id,
                        False,
                        module.effectful,
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
            model_meter = _ReplayModelMeter()
            model_broker = ModelBroker(
                self.model_adapters,
                cast(tuple[ModelSpec[Any, Any], ...], module.models),
                meter=model_meter,
                metadata=metadata,
                audit=lambda event_type, payload: None,
            )
            context = ExecutionContext(
                run_id=f"replay:{case.fixture.source.run_id}",
                task_id=f"replay:{boundary.instance_key}",
                step_instance_id=boundary.step_instance_id,
                replay=True,
                broker=broker,
                models=model_broker,
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
                if broker.effect_attempts:
                    return ReplayResult(
                        status=ReplayStatus.REPLAY_EFFECT_VIOLATION,
                        mode=case.mode,
                        comparisons=tuple(comparisons),
                        blocking=True,
                        message="a supported effect path was attempted during replay",
                    )
                return ReplayResult(
                    status=ReplayStatus.REPLAY_LIVE_FAILURE,
                    mode=case.mode,
                    comparisons=tuple(comparisons),
                    blocking=True,
                    message=f"{type(exc).__qualname__}: {exc}",
                )
            if broker.effect_attempts:
                return ReplayResult(
                    status=ReplayStatus.REPLAY_EFFECT_VIOLATION,
                    mode=case.mode,
                    comparisons=tuple(comparisons),
                    blocking=True,
                    message="a supported effect path was attempted during replay",
                )
            elapsed_ms = (time.perf_counter() - started) * 1000
            if not value_matches_type(output, module.output_type):
                raise ReplayContractError(
                    f"selective output from {key.as_string()} violates its current schema"
                )
            generated_divergence = self._validate_live_planner_output(
                case.fixture,
                boundary,
                output,
                generated_signatures,
            )
            if generated_divergence is not None:
                return ReplayResult(
                    status=ReplayStatus.REPLAY_DIVERGENCE,
                    mode=case.mode,
                    comparisons=tuple(comparisons),
                    divergence=generated_divergence,
                    trace_ids=tuple([*traces, *([trace_id] if trace_id else [])]),
                    blocking=False,
                    message=(
                        "selective planner produced an incompatible generated graph at "
                        f"{generated_divergence.change.location}"
                    ),
                )
            current_digest = digest_data(output)
            current_trajectories = tuple(
                _coerce_trajectory(item) for item in metadata.get("trajectories", [])
            )
            usage_data = dict(metadata.get("usage", {}))
            usage_data.setdefault("latency_ms", elapsed_ms)
            current_usage = Usage(**usage_data)
            comparison = StepComparison(
                key,
                boundary.step_instance_id,
                True,
                False,
                boundary.output_value.digest,
                current_digest,
                current_digest != boundary.output_value.digest,
                current_trajectories != boundary.trajectories,
                boundary.usage,
                current_usage,
                trace_id,
            )
            comparisons.append(comparison)
            changed = changed or comparison.behavior_changed
            if trace_id:
                traces.append(trace_id)
            usage = _add_usage(usage, current_usage)
        budget_message = _budget_violation(case.budget, usage, comparisons)
        if budget_message:
            return ReplayResult(
                ReplayStatus.REPLAY_BUDGET_EXCEEDED,
                case.mode,
                historical_output,
                tuple(comparisons),
                trace_ids=tuple(traces),
                live_usage=usage,
                blocking=True,
                message=budget_message,
            )
        return ReplayResult(
            ReplayStatus.CHANGED if changed else ReplayStatus.PASS,
            case.mode,
            historical_output,
            tuple(comparisons),
            trace_ids=tuple(traces),
            live_usage=usage,
            blocking=False,
            message="downstream continuation used accepted historical outputs",
        )

    def _validate_live_planner_output(
        self,
        fixture: ReplayFixture,
        boundary: BoundaryRecord,
        output: Any,
        current_signatures: Mapping[str, PlanSignature],
    ) -> ReplayDivergence | None:
        records = tuple(
            record
            for record in fixture.generated_plans
            if record.source_instance_key == boundary.instance_key
        )
        if not records:
            return None
        try:
            fragment = PlanFragmentIR.from_dict(cast(Mapping[str, Any], output))
        except (TypeError, ValueError) as exc:
            raise ReplayContractError(
                "selective planner output is not a canonical generated fragment"
            ) from exc
        for record in records:
            validator = self.generated_validators[record.region_id]
            try:
                proposed = validator.validate(
                    fragment,
                    region_input_schema_digest=boundary.input_schema_digest,
                    expected_output_schema_digests=record.signature.output_schema_digests,
                )
            except PlanValidationError as exc:
                raise ReplayContractError(
                    f"selective planner output failed generated policy: {exc.code}"
                ) from exc
            current = current_signatures[record.region_instance_id]
            alignment = self.aligner.align(
                _plan_from_signature(current), _plan_from_signature(proposed)
            )
            if alignment.diff.first_divergence is not None:
                return ReplayDivergence(alignment.diff.first_divergence, alignment.diff)
        return None


@dataclass(frozen=True)
class _StubEvaluation:
    value: Any
    dependency_instance_keys: tuple[str, ...] = ()


def _boundary_by_instance(fixture: ReplayFixture, instance_key: str) -> BoundaryRecord:
    boundary = next(
        (item for item in fixture.boundaries if item.instance_key == instance_key),
        None,
    )
    if boundary is None:
        raise ReplayContractError(f"recorded boundary {instance_key!r} is unavailable")
    return boundary


def _decode_fragment(fixture: ReplayFixture, boundary: BoundaryRecord) -> PlanFragmentIR:
    try:
        value = fixture.values.decode(boundary.output_value)
        return PlanFragmentIR.from_dict(cast(Mapping[str, Any], value))
    except (TypeError, ValueError) as exc:
        raise ReplayContractError(
            f"recorded planner boundary {boundary.instance_key} is not a canonical fragment"
        ) from exc


class _StubGraphExecutor:
    """Interpret current graph structure while injecting every historical boundary."""

    def __init__(self, plan: PlanIR, fixture: ReplayFixture) -> None:
        self.plan = plan
        self.fixture = fixture
        self.steps = {step.node_id: step for step in plan.steps}
        self.boundaries = {
            (
                ReplayKey(boundary.module_id, boundary.logical_step),
                boundary.step_instance_id,
            ): boundary
            for boundary in fixture.boundaries
        }
        self.used_boundaries: set[str] = set()
        self.cache: dict[int, _StubEvaluation] = {}
        self.control_decisions: list[dict[str, Any]] = []

    async def execute(
        self,
        workflow: Workflow[Any, Any],
        output: RuntimeValue[Any],
    ) -> Any:
        root_input = _rehydrate(
            self.fixture.values.decode(self.fixture.root_input),
            workflow.input_type,
        )
        if not value_matches_type(root_input, workflow.input_type):
            raise ReplayContractError("recorded root input violates its current contract")
        result = await self._visit(
            output,
            path="root",
            workflow=workflow,
            external=_StubEvaluation(root_input),
            scope=(),
        )
        recorded_output = _rehydrate(
            self.fixture.values.decode(self.fixture.root_output),
            workflow.output_type,
        )
        if not value_matches_type(recorded_output, workflow.output_type):
            raise ReplayContractError("recorded root output violates its current contract")
        if digest_data(result.value) != self.fixture.root_output.digest:
            raise ReplayContractError(
                "injected graph result does not match the recorded root output"
            )
        generated_instances = {
            instance_key
            for record in self.fixture.generated_plans
            for _node_key, instance_key in record.node_instances
        }
        expected_boundaries = {
            boundary.instance_key
            for boundary in self.fixture.boundaries
            if boundary.instance_key not in generated_instances
        }
        missing = expected_boundaries - self.used_boundaries
        if missing:
            raise ReplayContractError(
                "fixture contains boundary records outside the reconstructed path: "
                f"{sorted(missing)}"
            )
        if Counter(map(canonical_json, self.control_decisions)) != Counter(
            map(canonical_json, self.fixture.control_decisions)
        ):
            raise ReplayContractError(
                "recorded control decisions do not match the reconstructed execution path"
            )
        return recorded_output

    async def _visit(
        self,
        value: RuntimeValue[Any],
        *,
        path: str,
        workflow: Workflow[Any, Any],
        external: _StubEvaluation,
        scope: tuple[str, ...],
    ) -> _StubEvaluation:
        expression = value._expression
        if expression.kind == "input":
            return external
        cached = self.cache.get(id(value))
        if cached is not None:
            return cached
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
                path=f"{path}.nested",
                workflow=nested,
                external=source,
                scope=scope,
            )
        elif expression.kind == "module":
            source = await self._visit(
                expression.dependencies[0],
                path=f"{path}.dep0",
                workflow=workflow,
                external=external,
                scope=scope,
            )
            module_binding = cast(_ModuleBinding, expression.payload)
            result = self._inject(path, module_binding.module, source, scope)
        elif expression.kind == "literal":
            result = _StubEvaluation(expression.payload)
        elif expression.kind == "field":
            root, projected_path = self._field_root(value)
            source = await self._visit(
                root,
                path=path,
                workflow=workflow,
                external=external,
                scope=scope,
            )
            projected = source.value
            try:
                for name in projected_path:
                    projected = (
                        projected[name]
                        if isinstance(projected, Mapping)
                        else getattr(projected, name)
                    )
            except (AttributeError, KeyError, TypeError) as exc:
                raise ReplayContractError(
                    f"field binding {'.'.join(projected_path)!r} is unavailable"
                ) from exc
            result = _StubEvaluation(projected, source.dependency_instance_keys)
        elif expression.kind == "object":
            structured = cast(_StructuredBinding, expression.payload)
            children = [
                await self._visit(
                    dependency,
                    path=f"{path}.field[{name}]",
                    workflow=workflow,
                    external=external,
                    scope=scope,
                )
                for name, dependency in zip(structured.names, expression.dependencies, strict=True)
            ]
            result = _StubEvaluation(
                {name: child.value for name, child in zip(structured.names, children, strict=True)},
                _unique(
                    tuple(
                        instance
                        for child in children
                        for instance in child.dependency_instance_keys
                    )
                ),
            )
        elif expression.kind == "when":
            condition = await self._visit(
                expression.dependencies[0],
                path=f"{path}.dep0",
                workflow=workflow,
                external=external,
                scope=scope,
            )
            if not isinstance(condition.value, bool):
                raise ReplayContractError("recorded when condition is not a boolean")
            branch_index = 1 if condition.value else 2
            decision = {
                "event_type": "BRANCH_DECISION",
                "payload": {
                    "control_node": path,
                    "selected": "true" if branch_index == 1 else "false",
                },
            }
            self.control_decisions.append(decision)
            branch = await self._visit(
                expression.dependencies[branch_index],
                path=f"{path}.dep{branch_index}",
                workflow=workflow,
                external=external,
                scope=(*scope, f"branch:{branch_index}"),
            )
            result = _StubEvaluation(
                branch.value,
                _unique((*condition.dependency_instance_keys, *branch.dependency_instance_keys)),
            )
        elif expression.kind == "parallel":
            branches = [
                await self._visit(
                    dependency,
                    path=f"{path}.dep{index}",
                    workflow=workflow,
                    external=external,
                    scope=(*scope, f"parallel:{index}"),
                )
                for index, dependency in enumerate(expression.dependencies)
            ]
            result = _StubEvaluation(
                tuple(branch.value for branch in branches),
                _unique(
                    tuple(
                        instance
                        for branch in branches
                        for instance in branch.dependency_instance_keys
                    )
                ),
            )
        elif expression.kind == "map":
            source = await self._visit(
                expression.dependencies[0],
                path=f"{path}.dep0",
                workflow=workflow,
                external=external,
                scope=scope,
            )
            if not isinstance(source.value, (list, tuple)):
                raise ReplayContractError("recorded map_over input is not a sequence")
            map_binding = cast(_MapBinding, expression.payload)
            keyed = [(self._item_key(item, map_binding.item_key), item) for item in source.value]
            keys = [item_key for item_key, _ in keyed]
            if len(keys) != len(set(keys)):
                raise ReplayContractError("recorded map_over item keys are not unique")
            self.control_decisions.append(
                {
                    "event_type": "MAP_DECISION",
                    "payload": {"control_node": path, "item_keys": keys},
                }
            )
            mapped: list[Any] = []
            boundaries: list[str] = []
            for item_key, item in keyed:
                item_result = self._inject(
                    path,
                    map_binding.module,
                    _StubEvaluation(item, source.dependency_instance_keys),
                    (*scope, f"item:{item_key}"),
                )
                mapped.append(item_result.value)
                boundaries.extend(item_result.dependency_instance_keys)
            result = _StubEvaluation(mapped, tuple(boundaries))
        else:  # pragma: no cover - compilation rejects unsupported expressions
            raise ReplayContractError(f"unsupported replay expression {expression.kind!r}")
        self.cache[id(value)] = result
        return result

    @staticmethod
    def _field_root(value: RuntimeValue[Any]) -> tuple[RuntimeValue[Any], tuple[str, ...]]:
        path: tuple[str, ...] = ()
        current = value
        while current._expression.kind == "field":
            binding = cast(_FieldBinding, current._expression.payload)
            path = (*binding.path, *path)
            current = current._expression.dependencies[0]
        return current, path

    def _inject(
        self,
        path: str,
        module: Module[Any, Any],
        source: _StubEvaluation,
        scope: tuple[str, ...],
    ) -> _StubEvaluation:
        step = self.steps[path]
        key = step.replay_key
        if key is None:
            raise ReplayContractError(f"executable path {path} has no replay key")
        step_instance_id = _stable_instance_id(step, scope)
        boundary = self.boundaries.get((key, step_instance_id))
        if boundary is None:
            raise ReplayContractError(
                f"required replay boundary {key.as_string()}#{step_instance_id} is missing"
            )
        if boundary.instance_key in self.used_boundaries:
            raise ReplayContractError(f"replay boundary {boundary.instance_key} was injected twice")
        if boundary.input_value.digest != digest_data(source.value):
            raise ReplayContractError(
                f"recorded input for boundary {boundary.instance_key} does not match its handoff"
            )
        if boundary.dependency_instance_keys != source.dependency_instance_keys:
            raise ReplayContractError(
                f"recorded dependencies for boundary {boundary.instance_key} do not match topology"
            )
        recorded_input = _rehydrate(
            self.fixture.values.decode(boundary.input_value),
            module.input_type,
        )
        if not value_matches_type(recorded_input, module.input_type):
            raise ReplayContractError(f"recorded input violates {key.as_string()} contract")
        recorded_output = _rehydrate(
            self.fixture.values.decode(boundary.output_value),
            module.output_type,
        )
        if not value_matches_type(recorded_output, module.output_type):
            raise ReplayContractError(f"recorded output violates {key.as_string()} contract")
        self.used_boundaries.add(boundary.instance_key)
        return _StubEvaluation(recorded_output, (boundary.instance_key,))

    @staticmethod
    def _item_key(item: Any, key: str | Callable[[Any], str]) -> str:
        if isinstance(key, str):
            raw = item.get(key) if isinstance(item, Mapping) else getattr(item, key, None)
        else:
            raw = key(item)
        if raw is None or not str(raw):
            raise ReplayContractError("map_over produced an empty item key during replay")
        return str(raw)


def _unique(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(values))


def build_module_registry(
    workflow: Workflow[Any, Any],
    plan: PlanIR | None = None,
    *,
    output: RuntimeValue[Any] | None = None,
) -> dict[ReplayKey, Module[Any, Any]]:
    """Return current module instances indexed by exact replay key.

    This is an advanced integration helper for workers that need to bind a
    compiled definition to executable Python module instances.
    """
    if plan is None or output is None:
        compiled_graph = _compile_workflow_graph(workflow)
        compiled = compiled_graph.plan
        built_output = compiled_graph.output
    else:
        compiled = plan
        built_output = output
    steps = {step.node_id: step for step in compiled.executable_steps}
    found: dict[ReplayKey, Module[Any, Any]] = {}
    seen: set[int] = set()

    def visit(value: RuntimeValue[Any], path: str, owner: Workflow[Any, Any]) -> None:
        marker = id(value)
        if marker in seen:
            return
        seen.add(marker)
        expression = value._expression
        if expression.kind == "input":
            return
        if expression.kind == "workflow":
            visit(expression.dependencies[0], f"{path}.input", owner)
            binding = cast(_WorkflowBinding, expression.payload)
            nested = binding.workflow
            visit(binding.output, f"{path}.nested", nested)
            return
        if expression.kind in {"input", "literal"}:
            return
        if expression.kind == "field":
            root = value
            while root._expression.kind == "field":
                root = root._expression.dependencies[0]
            visit(root, path, owner)
            return
        if expression.kind == "object":
            structured = cast(_StructuredBinding, expression.payload)
            for name, dependency in zip(structured.names, expression.dependencies, strict=True):
                visit(dependency, f"{path}.field[{name}]", owner)
            return
        if expression.kind == "module":
            module_binding = cast(_ModuleBinding, expression.payload)
            visit(module_binding.input_value, f"{path}.dep0", owner)
            key = steps[path].replay_key
            if key is not None:
                found[key] = module_binding.module
            return
        for index, dependency in enumerate(expression.dependencies):
            visit(dependency, f"{path}.dep{index}", owner)
        if expression.kind == "map":
            map_binding = cast(_MapBinding, expression.payload)
            key = steps[path].replay_key
            if key is not None:
                found[key] = map_binding.module

    visit(built_output, "root", workflow)
    return found


def resolve_selectors(plan: PlanIR, selectors: Sequence[str]) -> tuple[ReplayKey, ...]:
    """Resolve ``module:ID`` and ``step:ID`` selectors to exact replay keys.

    Raises
    ------
    ReplaySelectorError
        If a selector is malformed, matches nothing, or matches multiple steps.
    """
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
    """Decode UTF-8 JSON bytes used by a recorded replay value."""
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
        input_tokens=left.input_tokens + right.input_tokens,
        output_tokens=left.output_tokens + right.output_tokens,
        cost_usd=left.cost_usd + right.cost_usd,
        latency_ms=left.latency_ms + right.latency_ms,
        tool_calls=left.tool_calls + right.tool_calls,
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
    """Validate a replay policy and return its scrubbed current environment.

    Raises
    ------
    ReplayContractError
        If the selected policy registers any production effect adapter.
    """
    selected = policy or ReplayWorkerPolicy()
    if selected.production_effect_adapters:
        raise ReplayContractError("replay workers cannot register production effect adapters")
    return selected.scrub_environment(os.environ)
