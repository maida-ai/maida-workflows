"""Connect external workflow systems to Maida's reliability boundary.

The interoperability API separates three concerns that are often conflated:
provider-owned authentication and invocation, external workflow authoring, and
verification fidelity. External systems remain replaceable. Maida retains the
typed module boundary, capability/effect policy, durable history, replay
sandbox, structural diff, and verification result appropriate to the strongest
contract an adapter can supply.

An opaque external flow is still useful, but only its typed boundary can be
verified. A trace-aware adapter additionally supplies canonical Maida boundary
records. An IR-aware adapter imports a :class:`~maida.workflows.spec.WorkflowSpec`
and receives full static validation, diff, and replay semantics.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, cast

from ._canonical import canonical_data
from .access import Capability, EffectSpec, Idempotency
from .authoring import ExecutionContext, Module
from .fixture import ReplayFixture
from .spec import WorkflowSpec

_IDENTITY = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")


class InteropUnsupportedError(RuntimeError):
    """Requested interoperability fidelity is unavailable for an adapter.

    The exception distinguishes an honest typed opaque integration from a
    trace-aware or IR-aware integration. Callers can present the adapter's
    :class:`VerificationSurface` before attempting a stronger operation.
    """


class InteropFidelity(StrEnum):
    """Strongest reliability surface supplied by an external integration.

    ``TYPED_BOUNDARY`` verifies only the external flow's declared input/output
    and access boundary. ``TRACE_AWARE`` also enables behavioral comparison and
    replay when the adapter provides canonical Maida boundaries. ``IR_AWARE``
    imports an explicit graph and enables the full static and behavioral suite.
    """

    TYPED_BOUNDARY = "typed-boundary"
    TRACE_AWARE = "trace-aware"
    IR_AWARE = "ir-aware"


@dataclass(frozen=True)
class VerificationSurface:
    """Explain the verification guarantees available for one adapter.

    Parameters
    ----------
    fidelity
        Strongest supported interoperability level.
    typed_boundary
        Whether typed root input/output validation is available.
    canonical_boundaries
        Whether internal external-flow steps are supplied as exact Maida
        boundary observations rather than inferred from span names or timing.
    behavioral_replay
        Whether those boundaries can participate in behavioral comparison and
        replay.
    static_graph, structural_diff
        Whether an imported explicit graph receives static validation and
        structural comparison.
    explanation
        Concise user-facing statement of what is and is not verified.
    """

    fidelity: InteropFidelity
    typed_boundary: bool
    canonical_boundaries: bool
    behavioral_replay: bool
    static_graph: bool
    structural_diff: bool
    explanation: str

    @classmethod
    def for_fidelity(cls, fidelity: InteropFidelity) -> VerificationSurface:
        """Return the canonical guarantee matrix for ``fidelity``."""
        if fidelity is InteropFidelity.IR_AWARE:
            return cls(
                fidelity,
                True,
                True,
                True,
                True,
                True,
                "The external definition imports as explicit Workflow IR; graph, "
                "contracts, diff, and replay are available.",
            )
        if fidelity is InteropFidelity.TRACE_AWARE:
            return cls(
                fidelity,
                True,
                True,
                True,
                False,
                False,
                "The external flow supplies canonical boundaries for behavioral "
                "replay, but its static graph remains external.",
            )
        return cls(
            fidelity,
            True,
            False,
            False,
            False,
            False,
            "The external flow is opaque; Maida verifies its typed boundary and "
            "declared access, not its internal graph or behavior.",
        )


@dataclass(frozen=True)
class WorkflowStartRequest:
    """Transport-neutral idempotent request to start a registered workflow.

    Parameters
    ----------
    workflow_id
        Application address resolved by a deployment-pinned workflow catalog.
    input
        Canonical trigger-derived root input.
    idempotency_key
        Stable external event identity scoped by tenant at persistence time.

    Notes
    -----
    The request carries no provider credentials or session state. HTTP, queue,
    and in-process adapters may all translate it to
    :meth:`~maida.workflows.userplane.WorkflowClient.start` with the same
    idempotency key.
    """

    workflow_id: str
    input: Any
    idempotency_key: str

    def __post_init__(self) -> None:
        """Validate stable workflow and retry identities."""
        _stable("workflow_id", self.workflow_id)
        if not isinstance(self.idempotency_key, str) or not self.idempotency_key.strip():
            raise ValueError("idempotency_key must be non-empty")
        object.__setattr__(self, "input", canonical_data(self.input))

    def to_data(self) -> dict[str, Any]:
        """Return the userplane start body without provider-specific metadata."""
        return {"input": self.input, "idempotency_key": self.idempotency_key}


class ExternalWorkflowProvider(Protocol):
    """Deployment-owned invocation provider for named external workflows.

    Provider objects may hold SDK clients and credentials, but are registered
    only with workers and are never serialized into workflow definitions,
    tasks, histories, or bundles.
    """

    provider: str
    version: str | None
    read_only_workflows: frozenset[str]
    effect_workflows: frozenset[str]
    idempotent_workflows: frozenset[str]

    async def invoke(
        self,
        workflow: str,
        value: Any,
        *,
        idempotency_key: str | None,
    ) -> Any:
        """Invoke one declared external flow and return provider-neutral data."""
        ...


class ExternalWorkflowImporter(Protocol):
    """Compile an external authoring representation into portable workflow data."""

    provider: str
    version: str | None

    def import_workflow(self, source: Any) -> WorkflowSpec:
        """Return an explicit, inspectable :class:`WorkflowSpec` from ``source``."""
        ...


class ExternalTraceImporter(Protocol):
    """Convert canonical external boundary observations into a replay fixture.

    Implementations must receive exact module identities, logical steps,
    dependency instance keys, typed values, effects, and accepted-result
    provenance. Ordinary spans are not sufficient and must fail closed rather
    than relying on heuristic name or timing correspondence.
    """

    provider: str
    version: str | None

    def import_trace(self, source: Any) -> ReplayFixture:
        """Return one integrity-checked replay fixture from canonical boundaries."""
        ...


class InteropConnectorAdapter:
    """Route external-flow calls through the existing capability/effect broker.

    Parameters
    ----------
    provider
        Deployment provider implementing named read-only and consequential
        workflows. Its client and credentials remain private runtime state.

    Notes
    -----
    Read-only workflows implement the connector read protocol. Consequential
    workflows implement the effect protocol and receive Maida's stable logical
    idempotency key. Replay stops at the Maida effect boundary before this
    adapter can be invoked.
    """

    def __init__(self, provider: ExternalWorkflowProvider) -> None:
        _validate_provider(provider)
        self._provider = provider

    @property
    def connector(self) -> str:
        """Return the provider's stable deployment registry key."""
        return self._provider.provider

    @property
    def connector_version(self) -> str | None:
        """Return the exact provider configuration version, when pinned."""
        return self._provider.version

    @property
    def operations(self) -> frozenset[str]:
        """Return external workflows declared safe for read-only invocation."""
        return self._provider.read_only_workflows

    @property
    def effect_operations(self) -> frozenset[str]:
        """Return consequential external workflows available as effects."""
        return self._provider.effect_workflows

    @property
    def idempotent_effects(self) -> frozenset[str]:
        """Return effect workflows that honor destination idempotency keys."""
        return self._provider.idempotent_workflows

    async def read(self, operation: str, request: Any) -> Any:
        """Invoke one declared read-only external workflow."""
        if operation not in self.operations:
            raise LookupError(f"external read workflow {operation!r} is not registered")
        return await self._provider.invoke(operation, request, idempotency_key=None)

    async def effect(self, operation: str, request: Any, idempotency_key: str) -> Any:
        """Invoke one consequential flow with its stable logical effect key."""
        if operation not in self.effect_operations:
            raise LookupError(f"external effect workflow {operation!r} is not registered")
        return await self._provider.invoke(
            operation,
            request,
            idempotency_key=idempotency_key,
        )


class ExternalWorkflow[InputT, OutputT](Module[InputT, OutputT]):
    """Represent a whole external flow as one typed distributed module.

    Parameters
    ----------
    module_id
        Stable semantic identity used for diff and replay alignment.
    workflow
        Provider-owned stable flow name. It becomes the broker operation, not
        an import path or runtime state noun.
    provider
        Deployment connector registry key.
    input_type, output_type
        Contracts validated immediately before and after provider invocation.
    provider_version
        Optional immutable adapter/configuration pin. Credentials must never be
        placed here.
    effectful
        ``False`` for a genuinely read-only flow; ``True`` for any flow that
        can change external state.
    idempotency
        Destination guarantee required from a consequential provider adapter.
    approval_required, policy_tags
        Existing Maida effect-policy declarations applied before invocation.

    Notes
    -----
    The module records only provider-neutral identity and contracts. It cannot
    access a provider directly: live execution must supply the ordinary Maida
    broker, and replay stubs or denies that boundary before provider code runs.
    """

    def __init__(
        self,
        *,
        module_id: str,
        workflow: str,
        provider: str,
        input_type: type[InputT],
        output_type: type[OutputT],
        provider_version: str | None = None,
        effectful: bool,
        idempotency: Idempotency = Idempotency.REQUIRED,
        approval_required: bool = False,
        policy_tags: tuple[str, ...] = (),
    ) -> None:
        _stable("module_id", module_id)
        _stable("workflow", workflow)
        _stable("provider", provider)
        if provider_version is not None and not provider_version.strip():
            raise ValueError("provider_version must be non-empty when supplied")
        if not isinstance(effectful, bool):
            raise TypeError("effectful must be a boolean")
        if not isinstance(idempotency, Idempotency):
            raise TypeError("idempotency must be an Idempotency value")
        self.module_id = module_id
        self.workflow = workflow
        self.provider = provider
        self.provider_version = provider_version
        self.input_type = input_type
        self.output_type = output_type
        self.effectful = effectful
        self.idempotency = idempotency
        if effectful:
            self.capabilities = ()
            self.effects = (
                EffectSpec(
                    name=f"external.{workflow}.invoke",
                    connector=provider,
                    operation=workflow,
                    input_type=input_type,
                    output_type=output_type,
                    connector_version=provider_version,
                    idempotency=idempotency,
                    approval_required=approval_required,
                    policy_tags=policy_tags,
                ),
            )
        else:
            self.capabilities = (
                Capability(
                    name=f"external.{workflow}.read",
                    connector=provider,
                    operation=workflow,
                    input_type=input_type,
                    output_type=output_type,
                    connector_version=provider_version,
                    policy_tags=policy_tags,
                ),
            )
            self.effects = ()

    async def execute(self, value: InputT, ctx: ExecutionContext) -> OutputT:
        """Invoke the external flow through the runtime-managed broker.

        Raises
        ------
        RuntimeError
            If code attempts to execute outside a configured Maida worker.
        AccessContractError
            If declarations, grants, policy, provider identity, typed values,
            approval, or idempotency requirements fail.
        """
        if ctx.broker is None:
            raise RuntimeError("ExternalWorkflow requires a runtime access broker")
        if self.effectful:
            return cast(
                OutputT,
                await ctx.broker.effect(
                    self.provider,
                    self.workflow,
                    value,
                    connector_version=self.provider_version,
                ),
            )
        return cast(
            OutputT,
            await ctx.broker.read(
                self.provider,
                self.workflow,
                value,
                connector_version=self.provider_version,
            ),
        )


class WorkflowInterop:
    """Bundle provider invocation and optional stronger import adapters.

    Parameters
    ----------
    provider
        Runtime invocation provider exposed through
        :class:`InteropConnectorAdapter`.
    workflow_importer
        Optional adapter that produces explicit portable workflow data.
    trace_importer
        Optional adapter that produces replay-complete canonical fixtures.

    Notes
    -----
    The strongest available adapter determines :attr:`surface`, but lower
    levels remain usable. Supplying an IR importer does not make provider
    credentials serializable; trusted module registries still resolve every
    executable alias at compile time.
    """

    def __init__(
        self,
        provider: ExternalWorkflowProvider,
        *,
        workflow_importer: ExternalWorkflowImporter | None = None,
        trace_importer: ExternalTraceImporter | None = None,
    ) -> None:
        _validate_provider(provider)
        _validate_companion(
            provider, workflow_importer, "workflow importer", method="import_workflow"
        )
        _validate_companion(provider, trace_importer, "trace importer", method="import_trace")
        self._provider = provider
        self._workflow_importer = workflow_importer
        self._trace_importer = trace_importer

    @property
    def surface(self) -> VerificationSurface:
        """Return an explicit, user-facing verification guarantee matrix."""
        fidelity = (
            InteropFidelity.IR_AWARE
            if self._workflow_importer is not None
            else (
                InteropFidelity.TRACE_AWARE
                if self._trace_importer is not None
                else InteropFidelity.TYPED_BOUNDARY
            )
        )
        return VerificationSurface.for_fidelity(fidelity)

    def connector_adapter(self) -> InteropConnectorAdapter:
        """Return the broker adapter for this deployment provider."""
        return InteropConnectorAdapter(self._provider)

    def import_workflow(self, source: Any) -> WorkflowSpec:
        """Import external authoring data as an explicit portable workflow.

        Raises
        ------
        InteropUnsupportedError
            If the integration offers only a typed or trace-aware boundary.
        TypeError
            If the trusted importer violates its return contract.
        """
        if self._workflow_importer is None:
            raise InteropUnsupportedError("this integration has no Workflow IR importer")
        result = self._workflow_importer.import_workflow(source)
        if not isinstance(result, WorkflowSpec):
            raise TypeError("workflow importer must return WorkflowSpec")
        return result

    def import_trace(self, source: Any) -> ReplayFixture:
        """Import replay-complete canonical external boundaries.

        Raises
        ------
        InteropUnsupportedError
            If the integration cannot supply replay-complete boundaries.
        TypeError
            If the trusted importer violates its return contract.
        """
        if self._trace_importer is None:
            raise InteropUnsupportedError("this integration has no canonical trace importer")
        result = self._trace_importer.import_trace(source)
        if not isinstance(result, ReplayFixture):
            raise TypeError("trace importer must return ReplayFixture")
        return result

    def module[InputT, OutputT](
        self,
        *,
        module_id: str,
        workflow: str,
        input_type: type[InputT],
        output_type: type[OutputT],
        effectful: bool,
        idempotency: Idempotency = Idempotency.REQUIRED,
        approval_required: bool = False,
        policy_tags: tuple[str, ...] = (),
    ) -> ExternalWorkflow[InputT, OutputT]:
        """Create a typed external-flow module bound only to provider identity."""
        return ExternalWorkflow(
            module_id=module_id,
            workflow=workflow,
            provider=self._provider.provider,
            provider_version=self._provider.version,
            input_type=input_type,
            output_type=output_type,
            effectful=effectful,
            idempotency=idempotency,
            approval_required=approval_required,
            policy_tags=policy_tags,
        )


def _stable(label: str, value: Any) -> str:
    if not isinstance(value, str) or _IDENTITY.fullmatch(value) is None:
        raise ValueError(f"{label} must be a stable identifier")
    return value


def _validate_provider(provider: ExternalWorkflowProvider) -> None:
    _stable("provider", getattr(provider, "provider", None))
    version = getattr(provider, "version", None)
    if version is not None and (not isinstance(version, str) or not version.strip()):
        raise ValueError("provider version must be non-empty when supplied")
    collections = {}
    for name in ("read_only_workflows", "effect_workflows", "idempotent_workflows"):
        values = getattr(provider, name, None)
        if not isinstance(values, frozenset):
            raise TypeError(f"provider {name} must be a frozenset")
        for value in values:
            _stable(f"provider {name} item", value)
        collections[name] = values
    if collections["read_only_workflows"] & collections["effect_workflows"]:
        raise ValueError("provider workflows cannot be both read-only and effectful")
    if not collections["idempotent_workflows"].issubset(collections["effect_workflows"]):
        raise ValueError("idempotent workflows must be declared effects")
    if not callable(getattr(provider, "invoke", None)):
        raise TypeError("provider invoke must be callable")


def _validate_companion(
    provider: ExternalWorkflowProvider,
    companion: Any,
    label: str,
    *,
    method: str,
) -> None:
    if companion is None:
        return
    if getattr(companion, "provider", None) != provider.provider:
        raise ValueError(f"{label} provider does not match invocation provider")
    if getattr(companion, "version", None) != provider.version:
        raise ValueError(f"{label} version does not match invocation provider")
    if not callable(getattr(companion, method, None)):
        raise TypeError(f"{label} must implement {method}")
