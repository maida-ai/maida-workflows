"""Declare typed external reads and effects as replaceable workflow modules.

Access declarations describe stable graph behavior without embedding provider
credentials or runtime clients. :class:`Connector` and :class:`Effect` are the
recommended explicit boundaries: they compile into inspectable IR, route live
access through the runtime broker, and can be stubbed or compared during replay.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, cast

from ._canonical import _rehydrate_value, digest_data, schema_digest, value_matches_type
from .authoring import ExecutionContext, Module
from .models import (
    CapabilityGrant as CapabilityGrant,
)
from .models import (
    StoredValue,
    _EffectOperation,
    _EffectOperationConflict,
    _EffectOperationStatus,
)

_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")
_BROKER_AUDIT_EVENT_TYPES = frozenset(
    {
        "CAPABILITY_AUTHORIZED",
        "CAPABILITY_DENIED",
        "CAPABILITY_FAILED",
        "CAPABILITY_USED",
        "EFFECT_DENIED",
        "EFFECT_FAILED",
    }
)


class AccessContractError(RuntimeError):
    """Raised when supported external access cannot proceed safely.

    Parameters
    ----------
    message
        Credential-free diagnostic suitable for durable attempt history.
    retryable
        Whether a fresh physical task attempt may safely retry the operation.
        Contract, grant, policy, approval, and idempotency denials default to
        non-retryable; transient adapter failures opt in explicitly.
    """

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


class Idempotency(StrEnum):
    """Guarantee an effect requires from its destination adapter."""

    NONE = "none"
    OPTIONAL = "optional"
    REQUIRED = "required"


@dataclass(frozen=True)
class ApprovalEvidence:
    """Reference one durable approval command for an exact effect request.

    Policies may return this value with an allow decision, but the reference is
    not trusted on its own. The durable effect store verifies that the same run
    and task contain a matching ``APPROVAL_REQUIRED`` request and later
    approving ``APPROVAL_RESOLVED`` event before the adapter is invoked.

    Parameters
    ----------
    request_id
        Stable interaction request identity whose metadata binds the effect
        name, logical ordinal, and request digest.
    command_id
        Idempotency identity of the command that approved that request.
    """

    request_id: str
    command_id: str

    def __post_init__(self) -> None:
        """Reject empty durable event references."""
        if not isinstance(self.request_id, str) or not self.request_id.strip():
            raise ValueError("approval request_id must be non-empty")
        if not isinstance(self.command_id, str) or not self.command_id.strip():
            raise ValueError("approval command_id must be non-empty")


@dataclass(frozen=True)
class PolicyDecision:
    """Stable allow-or-deny result returned by an :class:`AccessPolicy`.

    Policy decisions contain identifiers and reason codes, never free-form
    provider messages or request data, so they can be written safely to the
    durable audit stream.

    Parameters
    ----------
    allowed
        Whether the already-declared and already-granted read may proceed.
    policy_id
        Stable identity of the policy implementation or configuration.
    reason_code
        Stable machine-readable explanation suitable for audit records.
    approval
        Optional durable approval reference. The broker's store validates the
        referenced request and resolution transactionally before an
        approval-required effect is attempted. Read authorization ignores it.
    """

    allowed: bool
    policy_id: str
    reason_code: str
    approval: ApprovalEvidence | None = None

    def __post_init__(self) -> None:
        """Validate that the decision is safe for canonical audit storage."""
        if type(self.allowed) is not bool:
            raise ValueError("allowed must be a boolean")
        if self.approval is not None and not isinstance(self.approval, ApprovalEvidence):
            raise ValueError("approval must be ApprovalEvidence when supplied")
        _validate_identity("policy_id", self.policy_id)
        _validate_identity("reason_code", self.reason_code)

    @classmethod
    def allow(
        cls,
        policy_id: str = "default",
        reason_code: str = "allowed",
        *,
        approval: ApprovalEvidence | None = None,
    ) -> PolicyDecision:
        """Permit granted access and optionally reference durable approval."""
        return cls(True, policy_id, reason_code, approval)

    @classmethod
    def deny(cls, policy_id: str, reason_code: str) -> PolicyDecision:
        """Construct a stable decision denying an otherwise-granted request."""
        return cls(False, policy_id, reason_code)


def _validate_identity(label: str, value: str) -> None:
    if not value or _NAME_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{label} must be a non-empty stable name")


def _validate_common(
    *,
    name: str,
    connector: str,
    operation: str,
    connector_version: str | None,
    policy_tags: tuple[str, ...],
) -> tuple[str, ...]:
    _validate_identity("name", name)
    _validate_identity("connector", connector)
    _validate_identity("operation", operation)
    if connector_version is not None and (
        not isinstance(connector_version, str) or not connector_version.strip()
    ):
        raise ValueError("connector_version must be non-empty when supplied")
    if any(not isinstance(tag, str) or not tag.strip() for tag in policy_tags):
        raise ValueError("policy_tags must contain non-empty names")
    if len(set(policy_tags)) != len(policy_tags):
        raise ValueError("policy_tags must be unique")
    return tuple(sorted(policy_tags))


@dataclass(frozen=True)
class Capability[RequestT, ResponseT]:
    """Typed declaration of one read-only external operation.

    Parameters
    ----------
    name
        Stable authorization identity, for example ``crm.customer.read``.
    connector
        Adapter registry key chosen by the deployment.
    operation
        Stable operation name implemented by that adapter.
    input_type, output_type
        Python contracts validated before and after provider invocation.
    connector_version
        Optional immutable adapter/configuration identity. It must never contain
        credentials.
    policy_tags
        Stable tags available to policy hooks and structural verification.
    """

    name: str
    connector: str
    operation: str
    input_type: type[RequestT]
    output_type: type[ResponseT]
    connector_version: str | None = None
    policy_tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate and canonicalize public capability identity fields."""
        tags = _validate_common(
            name=self.name,
            connector=self.connector,
            operation=self.operation,
            connector_version=self.connector_version,
            policy_tags=self.policy_tags,
        )
        object.__setattr__(self, "policy_tags", tags)

    @property
    def input_schema_digest(self) -> str:
        """Return the canonical request-schema digest."""
        return schema_digest(self.input_type)

    @property
    def output_schema_digest(self) -> str:
        """Return the canonical response-schema digest."""
        return schema_digest(self.output_type)

    def to_data(self) -> dict[str, Any]:
        """Return the credential-free canonical IR declaration.

        Returns
        -------
        dict
            JSON-compatible data containing stable adapter, operation, schema,
            and policy identities. Runtime clients and credentials are never
            included.
        """
        return {
            "kind": "capability",
            "name": self.name,
            "connector": self.connector,
            "operation": self.operation,
            "connector_version": self.connector_version,
            "input_schema_digest": self.input_schema_digest,
            "output_schema_digest": self.output_schema_digest,
            "policy_tags": list(self.policy_tags),
        }


class AccessPolicy(Protocol):
    """Deployment policy that may narrow a compiled task access grant.

    The broker invokes policy only after declaration and grant checks succeed.
    Returning an allow decision therefore cannot introduce a capability or
    effect that the workflow definition did not declare or the task was not
    granted. Approval-required effects additionally require an explicit
    :class:`ApprovalEvidence` reference that the durable store verifies.
    """

    async def authorize(
        self,
        access: Capability[Any, Any] | EffectSpec[Any, Any],
        request: Any,
        *,
        grant: CapabilityGrant,
        run_id: str,
        task_id: str,
        attempt_id: str,
    ) -> PolicyDecision:
        """Decide whether one already-granted typed access may proceed.

        Parameters
        ----------
        access
            Exact compiled capability or effect selected by connector,
            operation, and optional connector version.
        request
            Typed request available for deployment-specific policy checks. It
            is not included in the broker's audit record.
        grant
            Attempt's immutable compiled capability grant.
        run_id, task_id, attempt_id
            Durable execution identities available for contextual policy.

        Returns
        -------
        PolicyDecision
            Stable authorization result recorded by the broker.
        """
        ...


class ConnectorAdapter(Protocol):
    """Provider-neutral implementation of one or more read operations.

    Adapters may hold provider clients and credentials privately. Only their
    stable connector identity, supported operation names, and optional version
    are registered; adapter instances are never serialized into workflow IR,
    task envelopes, or audit records.

    Attributes
    ----------
    connector
        Stable deployment registry name matching a capability declaration.
    connector_version
        Optional exact immutable adapter or configuration identity.
    operations
        Stable read operation names this adapter can execute. Implement
        :class:`EffectAdapter` as well when one object supplies both access
        categories.
    """

    @property
    def connector(self) -> str:
        """Return the stable connector registry identity."""
        ...

    @property
    def connector_version(self) -> str | None:
        """Return the exact adapter version, or ``None`` when unversioned."""
        ...

    @property
    def operations(self) -> frozenset[str]:
        """Return the stable read operations implemented by this adapter."""
        ...

    async def read(self, operation: str, request: Any) -> Any:
        """Execute one typed read and return the provider-neutral result."""
        ...


class EffectAdapter(Protocol):
    """Provider-neutral implementation of consequential external operations.

    An effect adapter receives a stable idempotency key for every logical
    operation. Adapters declaring an operation in ``idempotent_effects`` must
    forward that key to a destination that guarantees repeated calls with the
    same key do not repeat the external action. Credentials and provider
    clients remain private adapter state and are never serialized.

    Attributes
    ----------
    connector
        Stable deployment registry identity matching an effect declaration.
    connector_version
        Optional exact immutable adapter or configuration identity.
    effect_operations
        Stable consequential operation names implemented by the adapter.
    idempotent_effects
        Subset of ``effect_operations`` for which the destination honors the
        supplied stable idempotency key.
    """

    @property
    def connector(self) -> str:
        """Return the stable connector registry identity."""
        ...

    @property
    def connector_version(self) -> str | None:
        """Return the exact adapter version, or ``None`` when unversioned."""
        ...

    @property
    def effect_operations(self) -> frozenset[str]:
        """Return the stable consequential operations this adapter implements."""
        ...

    @property
    def idempotent_effects(self) -> frozenset[str]:
        """Return operations whose destinations honor the supplied stable key."""
        ...

    async def effect(
        self,
        operation: str,
        request: Any,
        *,
        idempotency_key: str,
    ) -> Any:
        """Execute one typed effect using the broker's stable logical key."""
        ...


class ConnectorRegistry:
    """Resolve read and effect adapters by exact operation identity.

    Parameters
    ----------
    adapters
        Optional initial read, effect, or combined adapters. Duplicate
        resolution keys are rejected so deployments cannot depend on
        registration order.
    """

    def __init__(self, adapters: Iterable[ConnectorAdapter | EffectAdapter] = ()) -> None:
        self._adapters: dict[tuple[str, str, str | None], ConnectorAdapter] = {}
        self._effect_adapters: dict[tuple[str, str, str | None], EffectAdapter] = {}
        for adapter in adapters:
            self.register(adapter)

    def register(self, adapter: ConnectorAdapter | EffectAdapter) -> None:
        """Register an adapter without exposing its client or credentials.

        Raises
        ------
        ValueError
            If identity fields are malformed, no operations are declared, or
            any exact resolution key is already registered.
        """
        connector = getattr(adapter, "connector", None)
        version = getattr(adapter, "connector_version", None)
        raw_operations: Any = getattr(adapter, "operations", frozenset())
        raw_effect_operations: Any = getattr(adapter, "effect_operations", frozenset())
        raw_idempotent_effects: Any = getattr(adapter, "idempotent_effects", frozenset())
        if not isinstance(connector, str):
            raise ValueError("adapter connector must be a stable name")
        _validate_identity("adapter connector", connector)
        if version is not None and (not isinstance(version, str) or not version.strip()):
            raise ValueError("adapter connector_version must be non-empty when supplied")
        if not isinstance(raw_operations, frozenset) or not isinstance(
            raw_effect_operations, frozenset
        ):
            raise ValueError("adapter read and effect operations must be frozensets")
        if not raw_operations and not raw_effect_operations:
            raise ValueError("adapter must declare at least one read or effect operation")
        if not isinstance(raw_idempotent_effects, frozenset):
            raise ValueError("adapter idempotent_effects must be a frozenset")
        if not raw_idempotent_effects.issubset(raw_effect_operations):
            raise ValueError("adapter idempotent_effects must be declared effect operations")
        operations = tuple(sorted(raw_operations))
        effect_operations = tuple(sorted(raw_effect_operations))
        for operation in (*operations, *effect_operations):
            if not isinstance(operation, str):
                raise ValueError("adapter operations must contain stable names")
            _validate_identity("adapter operation", operation)
        keys = tuple((connector, operation, version) for operation in operations)
        effect_keys = tuple((connector, operation, version) for operation in effect_operations)
        if any(key in self._adapters for key in keys):
            raise ValueError("an adapter is already registered for this exact access key")
        if any(key in self._effect_adapters for key in effect_keys):
            raise ValueError("an adapter is already registered for this exact effect key")
        if effect_keys and not callable(getattr(adapter, "effect", None)):
            raise ValueError("an effect-capable adapter must provide async effect()")
        for key in keys:
            self._adapters[key] = cast(ConnectorAdapter, adapter)
        for key in effect_keys:
            self._effect_adapters[key] = cast(EffectAdapter, adapter)

    def resolve(
        self,
        connector: str,
        operation: str,
        *,
        connector_version: str | None = None,
    ) -> ConnectorAdapter:
        """Return the adapter registered for one exact access identity.

        Omitting ``connector_version`` selects only an explicitly unversioned
        adapter. It never guesses among registered versions.

        Raises
        ------
        AccessContractError
            If no adapter is registered for the exact identity.
        """
        adapter = self._adapters.get((connector, operation, connector_version))
        if adapter is None:
            raise AccessContractError(
                f"connector operation {connector}.{operation} at the requested version "
                "is not registered"
            )
        return adapter

    def resolve_effect(
        self,
        connector: str,
        operation: str,
        *,
        connector_version: str | None = None,
    ) -> EffectAdapter:
        """Return the adapter registered for one exact effect identity.

        Raises
        ------
        AccessContractError
            If no effect adapter is registered for the exact connector,
            operation, and optional version.
        """
        adapter = self._effect_adapters.get((connector, operation, connector_version))
        if adapter is None:
            raise AccessContractError(
                f"effect operation {connector}.{operation} at the requested version "
                "is not registered"
            )
        return adapter

    def effect_is_idempotent(self, adapter: EffectAdapter, operation: str) -> bool:
        """Return whether an adapter guarantees idempotency for an effect."""
        declared: Any = getattr(adapter, "idempotent_effects", frozenset())
        return isinstance(declared, frozenset) and operation in declared


class _EffectOperations(Protocol):
    values: Any

    def _lookup_effect(
        self,
        claim: Any,
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

    def _reserve_effect(
        self,
        claim: Any,
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

    def _mark_effect_attempted(
        self,
        claim: Any,
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
        claim: Any,
        operation: _EffectOperation,
        result: StoredValue,
        *,
        latency_ms: float,
    ) -> _EffectOperation: ...


class AccessBroker:
    """Attempt-scoped enforcement point for declared reads and effects.

    A broker is bound to one durable task attempt and one module definition. It
    checks the compiled declaration, persisted grant, optional narrowing
    policy, exact adapter registration, and request/response contracts around
    every read. Effects additionally use a durable logical-operation ledger,
    stable destination idempotency keys, and lease-checked commits. Audit
    records contain only stable metadata and content digests; request values,
    response values, credentials, and provider errors are never included.

    Parameters
    ----------
    registry
        Deployment registry containing provider-neutral read and effect
        adapters.
    declarations
        Exact capability declarations compiled for the current module.
    effects
        Exact effect declarations compiled for the current module. The worker
        supplies durable effect storage and the active claim internally.
    grant
        Persisted task grant derived from the compiled workflow step.
    run_id, task_id, attempt_id
        Durable attempt identities attached to audit records.
    module_id, logical_step
        Stable workflow boundary identity attached to audit records.
    policy
        Optional deployment policy that may deny, but never add, access.
    audit
        Optional callback receiving allowlisted access diagnostics as
        ``(event_type, safe_payload)``. Control-plane and command events cannot
        be emitted through this callback.
    metadata
        Optional module metadata mapping. Successful reads append a canonical
        capability trajectory for accepted boundary history and replay.
    """

    def __init__(
        self,
        registry: ConnectorRegistry,
        *,
        declarations: Iterable[Capability[Any, Any]],
        effects: Iterable[EffectSpec[Any, Any]] = (),
        grant: CapabilityGrant,
        run_id: str,
        task_id: str,
        attempt_id: str,
        module_id: str,
        logical_step: str,
        policy: AccessPolicy | None = None,
        audit: Callable[[str, dict[str, Any]], None] | None = None,
        metadata: dict[str, Any] | None = None,
        _effect_operations: _EffectOperations | None = None,
        _claim: Any = None,
        _budget_meter: Any = None,
    ) -> None:
        declared = tuple(declarations)
        if any(not isinstance(item, Capability) for item in declared):
            raise AccessContractError("access broker declarations must contain Capability values")
        names = tuple(item.name for item in declared)
        if len(set(names)) != len(names):
            raise AccessContractError("access broker capability names must be unique")
        endpoints = tuple(
            (item.connector, item.operation, item.connector_version) for item in declared
        )
        if len(set(endpoints)) != len(endpoints):
            raise AccessContractError(
                "access broker declarations cannot ambiguously share an exact endpoint"
            )
        if not set(grant.capabilities).issubset(names):
            raise AccessContractError("task capability grant exceeds compiled module declarations")
        declared_effects = tuple(effects)
        if any(not isinstance(item, EffectSpec) for item in declared_effects):
            raise AccessContractError("access broker effects must contain EffectSpec values")
        effect_names = tuple(item.name for item in declared_effects)
        if len(set(effect_names)) != len(effect_names):
            raise AccessContractError("access broker effect names must be unique")
        effect_endpoints = tuple(
            (item.connector, item.operation, item.connector_version) for item in declared_effects
        )
        if len(set(effect_endpoints)) != len(effect_endpoints):
            raise AccessContractError(
                "access broker effects cannot ambiguously share an exact endpoint"
            )
        if not set(grant.effects).issubset(effect_names):
            raise AccessContractError("task effect grant exceeds compiled module declarations")
        self.registry = registry
        self.declarations = declared
        self.effect_declarations = declared_effects
        self.grant = grant
        self.run_id = run_id
        self.task_id = task_id
        self.attempt_id = attempt_id
        self.module_id = module_id
        self.logical_step = logical_step
        self.policy = policy
        self._audit = audit
        self.metadata = metadata if metadata is not None else {}
        self._effect_operations = _effect_operations
        self._claim = _claim
        self._budget_meter = _budget_meter
        self._effect_ordinals: dict[str, int] = {}
        self._effect_called = False
        self._effect_records: list[dict[str, Any]] = []
        self._records: list[dict[str, Any]] = []

    @property
    def records(self) -> tuple[dict[str, Any], ...]:
        """Return defensive copies of credential-free records emitted so far."""
        return tuple(dict(record) for record in self._records)

    async def read(
        self,
        connector: str,
        operation: str,
        request: Any,
        *,
        connector_version: str | None = None,
    ) -> Any:
        """Authorize and execute one exact typed read through its adapter.

        Parameters
        ----------
        connector, operation, connector_version
            Exact identity of the compiled capability and registered adapter.
        request
            Value validated against the capability input contract.

        Returns
        -------
        Any
            Adapter response after output-contract validation.

        Raises
        ------
        AccessContractError
            If declaration, grant, policy, adapter resolution, provider call,
            or either typed contract fails. Provider errors are sanitized.
        """
        matches = tuple(
            capability
            for capability in self.declarations
            if capability.connector == connector
            and capability.operation == operation
            and capability.connector_version == connector_version
        )
        if len(matches) != 1:
            self._emit(
                "CAPABILITY_DENIED",
                self._payload(
                    connector=connector,
                    operation=operation,
                    connector_version=connector_version,
                    reason_code="not-declared",
                ),
            )
            raise AccessContractError("connector read is not declared exactly once by the module")
        capability = matches[0]
        if not self.grant.allows_capability(capability.name):
            self._emit(
                "CAPABILITY_DENIED",
                self._payload(capability=capability, reason_code="not-granted"),
            )
            raise AccessContractError(f"task grant denies capability {capability.name}")
        if not value_matches_type(request, capability.input_type):
            self._emit(
                "CAPABILITY_DENIED",
                self._payload(capability=capability, reason_code="request-contract"),
            )
            raise AccessContractError("connector request violates the capability request contract")
        try:
            request_digest = digest_data(request)
        except Exception as exc:
            self._emit(
                "CAPABILITY_DENIED",
                self._payload(capability=capability, reason_code="request-not-canonical"),
            )
            raise AccessContractError("connector request is not canonically digestible") from exc

        decision = PolicyDecision.allow()
        if self.policy is not None:
            try:
                decision = await self.policy.authorize(
                    capability,
                    request,
                    grant=self.grant,
                    run_id=self.run_id,
                    task_id=self.task_id,
                    attempt_id=self.attempt_id,
                )
            except Exception:
                self._emit(
                    "CAPABILITY_DENIED",
                    self._payload(
                        capability=capability,
                        request_digest=request_digest,
                        policy_id="policy-error",
                        reason_code="policy-error",
                    ),
                )
                raise AccessContractError("access policy evaluation failed") from None
            if not isinstance(decision, PolicyDecision):
                self._emit(
                    "CAPABILITY_DENIED",
                    self._payload(
                        capability=capability,
                        request_digest=request_digest,
                        policy_id="policy-error",
                        reason_code="invalid-decision",
                    ),
                )
                raise AccessContractError("access policy returned an invalid decision")
        if not decision.allowed:
            self._emit(
                "CAPABILITY_DENIED",
                self._payload(
                    capability=capability,
                    request_digest=request_digest,
                    policy_id=decision.policy_id,
                    reason_code=decision.reason_code,
                ),
            )
            raise AccessContractError(f"access policy denied capability {capability.name}")

        authorized = self._payload(
            capability=capability,
            request_digest=request_digest,
            policy_id=decision.policy_id,
            reason_code=decision.reason_code,
        )
        self._emit("CAPABILITY_AUTHORIZED", authorized)
        try:
            adapter = self.registry.resolve(
                connector,
                operation,
                connector_version=connector_version,
            )
        except AccessContractError:
            self._emit(
                "CAPABILITY_FAILED",
                {**authorized, "reason_code": "adapter-not-registered"},
            )
            raise
        if self._budget_meter is not None:
            self._budget_meter.charge_tool(capability.name)
        started = time.perf_counter()
        try:
            response = await adapter.read(operation, request)
        except Exception:
            self._emit(
                "CAPABILITY_FAILED",
                {**authorized, "reason_code": "adapter-error"},
            )
            raise AccessContractError("connector adapter read failed", retryable=True) from None
        latency_ms = (time.perf_counter() - started) * 1000
        if not value_matches_type(response, capability.output_type):
            self._emit(
                "CAPABILITY_FAILED",
                {
                    **authorized,
                    "reason_code": "response-contract",
                    "latency_ms": latency_ms,
                },
            )
            raise AccessContractError(
                "connector response violates the capability response contract"
            )
        try:
            response_digest = digest_data(response)
        except Exception as exc:
            self._emit(
                "CAPABILITY_FAILED",
                {
                    **authorized,
                    "reason_code": "response-not-canonical",
                    "latency_ms": latency_ms,
                },
            )
            raise AccessContractError("connector response is not canonically digestible") from exc
        used = {
            **authorized,
            "response_digest": response_digest,
            "latency_ms": latency_ms,
        }
        self._emit("CAPABILITY_USED", used)
        self.metadata.setdefault("trajectories", []).append(
            {
                "kind": "capability",
                "name": capability.name,
                "request_digest": request_digest,
                "response_digest": response_digest,
                "metadata": {
                    "connector": connector,
                    "operation": operation,
                    "connector_version": connector_version,
                    "policy_id": decision.policy_id,
                    "reason_code": decision.reason_code,
                },
            }
        )
        return response

    async def effect(
        self,
        connector: str,
        operation: str,
        request: Any,
        *,
        connector_version: str | None = None,
    ) -> Any:
        """Authorize and durably commit one exact typed external effect.

        The logical identity is the current task, effect declaration name, and
        deterministic per-name invocation ordinal. Retries reuse the ledger's
        destination idempotency key. A previously committed result is decoded
        and returned without invoking the adapter again.

        Parameters
        ----------
        connector, operation, connector_version
            Exact identity of the compiled effect and registered adapter.
        request
            Value validated and content-digested before reservation.

        Returns
        -------
        Any
            Newly committed or previously stored typed effect result.

        Raises
        ------
        AccessContractError
            If declaration, grant, policy, approval, adapter idempotency,
            request/response contracts, or durable identity validation fails.

        Notes
        -----
        A previously committed logical result is recovery state, not a new
        external access attempt. After declaration, grant, request, and durable
        identity checks, it is restored without consulting a live adapter or
        re-running deployment policy.
        """
        self._effect_called = True
        matches = tuple(
            effect
            for effect in self.effect_declarations
            if effect.connector == connector
            and effect.operation == operation
            and effect.connector_version == connector_version
        )
        if len(matches) != 1:
            self._emit(
                "EFFECT_DENIED",
                self._effect_payload(
                    connector=connector,
                    operation=operation,
                    connector_version=connector_version,
                    reason_code="not-declared",
                ),
            )
            raise AccessContractError("external effect is not declared exactly once by the module")
        effect = matches[0]
        if not self.grant.allows_effect(effect.name):
            self._emit(
                "EFFECT_DENIED",
                self._effect_payload(effect=effect, reason_code="not-granted"),
            )
            raise AccessContractError(f"task grant denies effect {effect.name}")
        if not value_matches_type(request, effect.input_type):
            self._emit(
                "EFFECT_DENIED",
                self._effect_payload(effect=effect, reason_code="request-contract"),
            )
            raise AccessContractError("effect request violates the declared request contract")
        try:
            request_digest = digest_data(request)
        except Exception as exc:
            self._emit(
                "EFFECT_DENIED",
                self._effect_payload(effect=effect, reason_code="request-not-canonical"),
            )
            raise AccessContractError("effect request is not canonically digestible") from exc

        if self._effect_operations is None or self._claim is None:
            self._emit(
                "EFFECT_DENIED",
                self._effect_payload(
                    effect=effect,
                    request_digest=request_digest,
                    reason_code="durability-unavailable",
                ),
            )
            raise AccessContractError("effect execution requires durable worker state")
        ordinal = self._effect_ordinals.get(effect.name, 0)
        self._effect_ordinals[effect.name] = ordinal + 1
        try:
            existing = self._effect_operations._lookup_effect(
                self._claim,
                effect_name=effect.name,
                ordinal=ordinal,
                connector=connector,
                operation=operation,
                connector_version=connector_version,
                idempotency_requirement=effect.idempotency.value,
                request_digest=request_digest,
                result_schema_digest=effect.output_schema_digest,
            )
        except _EffectOperationConflict as exc:
            self._emit(
                "EFFECT_DENIED",
                self._effect_payload(
                    effect=effect,
                    ordinal=ordinal,
                    request_digest=request_digest,
                    reason_code="identity-conflict",
                ),
            )
            raise AccessContractError(str(exc)) from None
        if existing is not None and existing.status is _EffectOperationStatus.COMMITTED:
            response = self._load_effect_result(effect, existing)
            self._record_effect_attempt(effect, existing)
            self._record_effect_commit(effect, existing)
            return response

        decision = PolicyDecision.allow()
        if self.policy is not None:
            try:
                decision = await self.policy.authorize(
                    effect,
                    request,
                    grant=self.grant,
                    run_id=self.run_id,
                    task_id=self.task_id,
                    attempt_id=self.attempt_id,
                )
            except Exception:
                self._emit(
                    "EFFECT_DENIED",
                    self._effect_payload(
                        effect=effect,
                        request_digest=request_digest,
                        policy_id="policy-error",
                        reason_code="policy-error",
                    ),
                )
                raise AccessContractError("effect access policy evaluation failed") from None
            if not isinstance(decision, PolicyDecision):
                self._emit(
                    "EFFECT_DENIED",
                    self._effect_payload(
                        effect=effect,
                        request_digest=request_digest,
                        policy_id="policy-error",
                        reason_code="invalid-decision",
                    ),
                )
                raise AccessContractError("effect access policy returned an invalid decision")
        if not decision.allowed:
            self._emit(
                "EFFECT_DENIED",
                self._effect_payload(
                    effect=effect,
                    request_digest=request_digest,
                    policy_id=decision.policy_id,
                    reason_code=decision.reason_code,
                ),
            )
            raise AccessContractError(f"access policy denied effect {effect.name}")
        if effect.approval_required and decision.approval is None:
            self._emit(
                "EFFECT_DENIED",
                self._effect_payload(
                    effect=effect,
                    request_digest=request_digest,
                    policy_id=decision.policy_id,
                    reason_code="approval-required",
                ),
            )
            raise AccessContractError(f"effect {effect.name} requires durable approval")

        try:
            adapter = self.registry.resolve_effect(
                connector,
                operation,
                connector_version=connector_version,
            )
        except AccessContractError:
            self._emit(
                "EFFECT_DENIED",
                self._effect_payload(
                    effect=effect,
                    request_digest=request_digest,
                    policy_id=decision.policy_id,
                    reason_code="adapter-not-registered",
                ),
            )
            raise
        adapter_idempotent = self.registry.effect_is_idempotent(adapter, operation)
        if effect.idempotency is Idempotency.REQUIRED and not adapter_idempotent:
            self._emit(
                "EFFECT_DENIED",
                self._effect_payload(
                    effect=effect,
                    request_digest=request_digest,
                    policy_id=decision.policy_id,
                    reason_code="idempotency-required",
                ),
            )
            raise AccessContractError(f"effect {effect.name} requires adapter idempotency support")
        try:
            reserved = self._effect_operations._reserve_effect(
                self._claim,
                effect_name=effect.name,
                ordinal=ordinal,
                connector=connector,
                operation=operation,
                connector_version=connector_version,
                idempotency_requirement=effect.idempotency.value,
                adapter_idempotent=adapter_idempotent,
                request_digest=request_digest,
                result_schema_digest=effect.output_schema_digest,
            )
        except _EffectOperationConflict as exc:
            self._emit(
                "EFFECT_DENIED",
                self._effect_payload(
                    effect=effect,
                    ordinal=ordinal,
                    request_digest=request_digest,
                    policy_id=decision.policy_id,
                    reason_code="identity-conflict",
                ),
            )
            raise AccessContractError(str(exc)) from None

        if reserved.status is _EffectOperationStatus.COMMITTED:  # pragma: no cover - lookup owns it
            response = self._load_effect_result(effect, reserved)
            self._record_effect_attempt(effect, reserved)
            self._record_effect_commit(effect, reserved)
            return response
        if reserved.status is _EffectOperationStatus.ATTEMPTED and not adapter_idempotent:
            self._emit(
                "EFFECT_DENIED",
                self._effect_payload(
                    effect=effect,
                    ordinal=ordinal,
                    request_digest=request_digest,
                    policy_id=decision.policy_id,
                    reason_code="unsafe-retry",
                ),
            )
            raise AccessContractError(
                f"effect {effect.name} unsafe retry denied because the adapter is not idempotent"
            )

        if self._budget_meter is not None:
            self._budget_meter.charge_tool(effect.name)

        try:
            attempted = self._effect_operations._mark_effect_attempted(
                self._claim,
                reserved,
                policy_id=decision.policy_id,
                reason_code=decision.reason_code,
                approval_required=effect.approval_required,
                approval_request_id=(
                    decision.approval.request_id if decision.approval is not None else None
                ),
                approval_command_id=(
                    decision.approval.command_id if decision.approval is not None else None
                ),
            )
        except _EffectOperationConflict as exc:
            self._emit(
                "EFFECT_DENIED",
                self._effect_payload(
                    effect=effect,
                    ordinal=ordinal,
                    request_digest=request_digest,
                    policy_id=decision.policy_id,
                    reason_code="approval-evidence-invalid",
                ),
            )
            raise AccessContractError(str(exc)) from None
        attempted_payload = self._effect_payload(
            effect=effect,
            ordinal=ordinal,
            request_digest=request_digest,
            policy_id=decision.policy_id,
            reason_code=decision.reason_code,
            idempotency_key=attempted.idempotency_key,
            adapter_idempotent=adapter_idempotent,
            approval_request_id=(
                decision.approval.request_id if decision.approval is not None else None
            ),
            approval_command_id=(
                decision.approval.command_id if decision.approval is not None else None
            ),
        )
        self._remember("EFFECT_ATTEMPTED", attempted_payload)
        self._record_effect_attempt(effect, attempted)
        started = time.perf_counter()
        try:
            response = await adapter.effect(
                operation,
                request,
                idempotency_key=attempted.idempotency_key,
            )
        except Exception:
            latency_ms = (time.perf_counter() - started) * 1000
            self._emit(
                "EFFECT_FAILED",
                {
                    **attempted_payload,
                    "reason_code": "adapter-error",
                    "latency_ms": latency_ms,
                },
            )
            raise AccessContractError("effect adapter call failed", retryable=True) from None
        latency_ms = (time.perf_counter() - started) * 1000
        if not value_matches_type(response, effect.output_type):
            self._emit(
                "EFFECT_FAILED",
                {
                    **attempted_payload,
                    "reason_code": "response-contract",
                    "latency_ms": latency_ms,
                },
            )
            raise AccessContractError("effect response violates the declared response contract")
        try:
            stored_result = self._effect_operations.values.encode(
                response,
                schema_digest=effect.output_schema_digest,
            )
        except Exception as exc:
            self._emit(
                "EFFECT_FAILED",
                {
                    **attempted_payload,
                    "reason_code": "response-not-canonical",
                    "latency_ms": latency_ms,
                },
            )
            raise AccessContractError("effect response is not canonically storable") from exc
        committed = self._effect_operations._commit_effect(
            self._claim,
            attempted,
            stored_result,
            latency_ms=latency_ms,
        )
        committed_payload = {
            **attempted_payload,
            "result_digest": stored_result.digest,
            "result_schema_digest": stored_result.schema_digest,
            "latency_ms": latency_ms,
        }
        self._remember("EFFECT_COMMITTED", committed_payload)
        self._record_effect_commit(effect, committed)
        return response

    def _load_effect_result(
        self,
        effect: EffectSpec[Any, Any],
        operation: _EffectOperation,
    ) -> Any:
        if operation.result_value is None:
            raise AccessContractError("committed effect is missing its durable result reference")
        if operation.result_value.schema_digest != effect.output_schema_digest:
            raise AccessContractError("committed effect result schema no longer matches")
        effect_operations = self._effect_operations
        if effect_operations is None:  # pragma: no cover - checked before reservation
            raise AccessContractError("effect execution requires durable worker state")
        try:
            decoded = effect_operations.values.decode(operation.result_value)
            response = _rehydrate_value(decoded, effect.output_type)
        except Exception:
            raise AccessContractError("committed effect result could not be restored") from None
        if not value_matches_type(response, effect.output_type):
            raise AccessContractError("committed effect result violates its output contract")
        return response

    def _record_effect_attempt(
        self,
        effect: EffectSpec[Any, Any],
        operation: _EffectOperation,
    ) -> None:
        self._effect_records.append(
            {
                "kind": "EFFECT_ATTEMPTED",
                "adapter": effect.connector,
                "operation": effect.operation,
                "request_digest": operation.request_digest,
                "result_digest": None,
                "effect_name": effect.name,
                "ordinal": operation.ordinal,
                "idempotency_key": operation.idempotency_key,
                "connector_version": effect.connector_version,
                "_reservation_order": operation.reservation_order,
            }
        )

    def _record_effect_commit(
        self,
        effect: EffectSpec[Any, Any],
        operation: _EffectOperation,
    ) -> None:
        if operation.result_value is None:
            raise AccessContractError("committed effect has no result reference")
        self._effect_records.append(
            {
                "kind": "EFFECT_COMMITTED",
                "adapter": effect.connector,
                "operation": effect.operation,
                "request_digest": operation.request_digest,
                "result_digest": operation.result_value.digest,
                "effect_name": effect.name,
                "ordinal": operation.ordinal,
                "idempotency_key": operation.idempotency_key,
                "connector_version": effect.connector_version,
                "_reservation_order": operation.reservation_order,
            }
        )

    def _boundary_effect_records(self) -> tuple[dict[str, Any], ...]:
        kind_order = {"EFFECT_ATTEMPTED": 0, "EFFECT_COMMITTED": 1}
        grouped: dict[int, list[dict[str, Any]]] = {}
        for record in self._effect_records:
            grouped.setdefault(int(record.get("_reservation_order", 0)), []).append(record)
        complete: list[dict[str, Any]] = []
        for reservation_order in sorted(grouped):
            records = sorted(
                grouped[reservation_order],
                key=lambda record: kind_order.get(str(record.get("kind")), 2),
            )
            if [record.get("kind") for record in records] == [
                "EFFECT_ATTEMPTED",
                "EFFECT_COMMITTED",
            ]:
                complete.extend(records)
        return tuple(dict(record) for record in complete)

    def _effect_payload(
        self,
        *,
        effect: EffectSpec[Any, Any] | None = None,
        connector: str | None = None,
        operation: str | None = None,
        connector_version: str | None = None,
        ordinal: int | None = None,
        request_digest: str | None = None,
        policy_id: str | None = None,
        reason_code: str,
        idempotency_key: str | None = None,
        adapter_idempotent: bool | None = None,
        approval_request_id: str | None = None,
        approval_command_id: str | None = None,
    ) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "task_id": self.task_id,
            "attempt_id": self.attempt_id,
            "module_id": self.module_id,
            "logical_step": self.logical_step,
            "effect_name": effect.name if effect is not None else None,
            "ordinal": ordinal,
            "connector": effect.connector if effect is not None else connector,
            "operation": effect.operation if effect is not None else operation,
            "connector_version": effect.connector_version
            if effect is not None
            else connector_version,
            "request_digest": request_digest,
            "policy_id": policy_id,
            "reason_code": reason_code,
            "idempotency_key": idempotency_key,
            "adapter_idempotent": adapter_idempotent,
            "approval_request_id": approval_request_id,
            "approval_command_id": approval_command_id,
        }

    def _payload(
        self,
        *,
        capability: Capability[Any, Any] | None = None,
        connector: str | None = None,
        operation: str | None = None,
        connector_version: str | None = None,
        request_digest: str | None = None,
        policy_id: str | None = None,
        reason_code: str,
    ) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "task_id": self.task_id,
            "attempt_id": self.attempt_id,
            "module_id": self.module_id,
            "logical_step": self.logical_step,
            "capability": capability.name if capability is not None else None,
            "connector": capability.connector if capability is not None else connector,
            "operation": capability.operation if capability is not None else operation,
            "connector_version": capability.connector_version
            if capability is not None
            else connector_version,
            "request_digest": request_digest,
            "policy_id": policy_id,
            "reason_code": reason_code,
        }

    def _emit(self, event_type: str, payload: Mapping[str, Any]) -> None:
        if event_type not in _BROKER_AUDIT_EVENT_TYPES:
            raise AccessContractError("unsupported access broker audit event type")
        safe = dict(payload)
        self._records.append({"event_type": event_type, **safe})
        if self._audit is not None:
            self._audit(event_type, dict(safe))

    def _remember(self, event_type: str, payload: Mapping[str, Any]) -> None:
        self._records.append({"event_type": event_type, **dict(payload)})


@dataclass(frozen=True)
class EffectSpec[RequestT, ResponseT]:
    """Typed declaration of one consequential external write.

    Effect declarations add stable idempotency and approval semantics to the
    same connector/operation abstraction used for reads. They contain no live
    adapter or credential material.

    Parameters
    ----------
    name
        Stable authorization and audit identity, for example ``email.send``.
    connector
        Adapter registry key chosen by the deployment.
    operation
        Stable operation name implemented by that adapter.
    input_type, output_type
        Python contracts validated around the provider invocation.
    connector_version
        Optional immutable adapter/configuration identity. It must not contain
        credentials.
    idempotency
        Guarantee required from the selected adapter before live execution.
    approval_required
        Whether policy must supply a durable approval reference for the same
        task and exact effect request before dispatch.
    policy_tags
        Stable tags available to policy hooks and structural verification.
    """

    name: str
    connector: str
    operation: str
    input_type: type[RequestT]
    output_type: type[ResponseT]
    connector_version: str | None = None
    idempotency: Idempotency = Idempotency.REQUIRED
    approval_required: bool = False
    policy_tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate and canonicalize public effect identity fields."""
        tags = _validate_common(
            name=self.name,
            connector=self.connector,
            operation=self.operation,
            connector_version=self.connector_version,
            policy_tags=self.policy_tags,
        )
        if not isinstance(self.idempotency, Idempotency):
            raise ValueError("idempotency must be an Idempotency value")
        if type(self.approval_required) is not bool:
            raise ValueError("approval_required must be a boolean")
        object.__setattr__(self, "policy_tags", tags)

    @property
    def input_schema_digest(self) -> str:
        """Return the canonical effect-request schema digest."""
        return schema_digest(self.input_type)

    @property
    def output_schema_digest(self) -> str:
        """Return the canonical effect-result schema digest."""
        return schema_digest(self.output_type)

    def to_data(self) -> dict[str, Any]:
        """Return the credential-free canonical IR declaration.

        Returns
        -------
        dict
            JSON-compatible data containing stable adapter, operation, schema,
            idempotency, approval, and policy identities. Runtime clients and
            credentials are never included.
        """
        return {
            "kind": "effect",
            "name": self.name,
            "connector": self.connector,
            "operation": self.operation,
            "connector_version": self.connector_version,
            "input_schema_digest": self.input_schema_digest,
            "output_schema_digest": self.output_schema_digest,
            "idempotency": self.idempotency.value,
            "approval_required": self.approval_required,
            "policy_tags": list(self.policy_tags),
        }


class Connector[RequestT, ResponseT](Module[RequestT, ResponseT]):
    """Explicit workflow module for a typed read-only connector operation.

    Parameters
    ----------
    capability
        Stable typed access declaration compiled into the workflow definition.
    module_id
        Optional semantic identity override. By default the trusted capability
        name identifies the connector module.
    """

    effectful = False

    def __init__(
        self,
        capability: Capability[RequestT, ResponseT],
        *,
        module_id: str | None = None,
    ) -> None:
        if module_id is not None and not module_id.strip():
            raise ValueError("module_id must be non-empty when supplied")
        self.input_type = capability.input_type
        self.output_type = capability.output_type
        self.module_id = capability.name if module_id is None else module_id
        self.capabilities = (capability,)
        self.effects: tuple[EffectSpec[Any, Any], ...] = ()

    async def execute(self, value: RequestT, ctx: ExecutionContext) -> ResponseT:
        """Invoke the declared read through the runtime-managed broker.

        Parameters
        ----------
        value
            Concrete request matching the capability input contract.
        ctx
            Runtime context containing the attempt-scoped access broker.

        Returns
        -------
        ResponseT
            Broker result after runtime contract and policy enforcement.

        Raises
        ------
        AccessContractError
            If no broker is bound or the module declaration was corrupted.
        """
        if ctx.broker is None or not callable(getattr(ctx.broker, "read", None)):
            raise AccessContractError("connector execution requires a runtime access broker")
        if len(self.capabilities) != 1 or not isinstance(self.capabilities[0], Capability):
            raise AccessContractError("connector module requires exactly one Capability")
        capability = self.capabilities[0]
        result = await ctx.broker.read(
            capability.connector,
            capability.operation,
            value,
            connector_version=capability.connector_version,
        )
        return cast(ResponseT, result)


class Effect[RequestT, ResponseT](Module[RequestT, ResponseT]):
    """Explicit workflow module for a typed consequential external operation.

    Live execution always routes through a broker. Replay workers may validate
    the proposed effect but never construct or call a production adapter.

    Parameters
    ----------
    effect
        Stable typed effect declaration compiled into the workflow definition.
    module_id
        Optional semantic identity override. By default the trusted effect name
        identifies the effect module.

    Notes
    -----
    An effect module is always classified as effectful. The runtime broker is
    the only supported path from this boundary to an external write adapter.
    """

    effectful = True

    def __init__(
        self,
        effect: EffectSpec[RequestT, ResponseT],
        *,
        module_id: str | None = None,
    ) -> None:
        if module_id is not None and not module_id.strip():
            raise ValueError("module_id must be non-empty when supplied")
        self.input_type = effect.input_type
        self.output_type = effect.output_type
        self.module_id = effect.name if module_id is None else module_id
        self.capabilities: tuple[Capability[Any, Any], ...] = ()
        self.effects = (effect,)

    async def execute(self, value: RequestT, ctx: ExecutionContext) -> ResponseT:
        """Invoke the declared write through the runtime-managed broker.

        Parameters
        ----------
        value
            Concrete request matching the effect input contract.
        ctx
            Runtime context containing the attempt-scoped access broker.

        Returns
        -------
        ResponseT
            Durable or newly committed effect result returned by the broker.

        Raises
        ------
        AccessContractError
            If no broker is bound or the module declaration was corrupted.
        """
        if ctx.broker is None or not callable(getattr(ctx.broker, "effect", None)):
            raise AccessContractError("effect execution requires a runtime access broker")
        if len(self.effects) != 1 or not isinstance(self.effects[0], EffectSpec):
            raise AccessContractError("effect module requires exactly one EffectSpec")
        effect = self.effects[0]
        result = await ctx.broker.effect(
            effect.connector,
            effect.operation,
            value,
            connector_version=effect.connector_version,
        )
        return cast(ResponseT, result)
