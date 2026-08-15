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

from ._canonical import digest_data, schema_digest, value_matches_type
from .authoring import ExecutionContext, Module
from .models import CapabilityGrant as CapabilityGrant

_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")


class AccessContractError(RuntimeError):
    """Raised when supported external access lacks a valid runtime broker."""


class Idempotency(StrEnum):
    """Guarantee an effect requires from its destination adapter."""

    NONE = "none"
    OPTIONAL = "optional"
    REQUIRED = "required"


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
    """

    allowed: bool
    policy_id: str
    reason_code: str

    def __post_init__(self) -> None:
        """Validate that the decision is safe for canonical audit storage."""
        if type(self.allowed) is not bool:
            raise ValueError("allowed must be a boolean")
        _validate_identity("policy_id", self.policy_id)
        _validate_identity("reason_code", self.reason_code)

    @classmethod
    def allow(cls, policy_id: str = "default", reason_code: str = "allowed") -> PolicyDecision:
        """Construct a stable decision permitting an already-granted request."""
        return cls(True, policy_id, reason_code)

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
    """Deployment policy that may narrow a compiled task capability grant.

    The broker invokes policy only after declaration and grant checks succeed.
    Returning an allow decision therefore cannot introduce a capability that
    the workflow definition did not declare or the task was not granted.
    """

    async def authorize(
        self,
        capability: Capability[Any, Any],
        request: Any,
        *,
        grant: CapabilityGrant,
        run_id: str,
        task_id: str,
        attempt_id: str,
    ) -> PolicyDecision:
        """Decide whether one already-granted typed read may proceed.

        Parameters
        ----------
        capability
            Exact compiled capability selected by connector, operation, and
            optional connector version.
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
        Stable operation names this adapter can execute.
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


class ConnectorRegistry:
    """Resolve read adapters by exact connector, operation, and version.

    Parameters
    ----------
    adapters
        Optional initial adapters. Duplicate resolution keys are rejected so
        deployments cannot depend on registration order.
    """

    def __init__(self, adapters: Iterable[ConnectorAdapter] = ()) -> None:
        self._adapters: dict[tuple[str, str, str | None], ConnectorAdapter] = {}
        for adapter in adapters:
            self.register(adapter)

    def register(self, adapter: ConnectorAdapter) -> None:
        """Register an adapter without exposing its client or credentials.

        Raises
        ------
        ValueError
            If identity fields are malformed, no operations are declared, or
            any exact resolution key is already registered.
        """
        connector = getattr(adapter, "connector", None)
        version = getattr(adapter, "connector_version", None)
        raw_operations = getattr(adapter, "operations", None)
        if not isinstance(connector, str):
            raise ValueError("adapter connector must be a stable name")
        _validate_identity("adapter connector", connector)
        if version is not None and (not isinstance(version, str) or not version.strip()):
            raise ValueError("adapter connector_version must be non-empty when supplied")
        if not isinstance(raw_operations, frozenset) or not raw_operations:
            raise ValueError("adapter operations must be a non-empty frozenset")
        operations = tuple(sorted(raw_operations))
        for operation in operations:
            if not isinstance(operation, str):
                raise ValueError("adapter operations must contain stable names")
            _validate_identity("adapter operation", operation)
        keys = tuple((connector, operation, version) for operation in operations)
        if any(key in self._adapters for key in keys):
            raise ValueError("an adapter is already registered for this exact access key")
        for key in keys:
            self._adapters[key] = adapter

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


class AccessBroker:
    """Attempt-scoped enforcement point for declared read capabilities.

    A broker is bound to one durable task attempt and one module definition. It
    checks the compiled declaration, persisted grant, optional narrowing
    policy, exact adapter registration, and request/response contracts around
    every read. Audit callbacks receive only stable metadata and content
    digests; request values, response values, credentials, and provider errors
    are never included.

    Parameters
    ----------
    registry
        Deployment registry containing provider-neutral read adapters.
    declarations
        Exact capability declarations compiled for the current module.
    grant
        Persisted task grant derived from the compiled workflow step.
    run_id, task_id, attempt_id
        Durable attempt identities attached to audit records.
    module_id, logical_step
        Stable workflow boundary identity attached to audit records.
    policy
        Optional deployment policy that may deny, but never add, access.
    audit
        Optional callback receiving ``(event_type, safe_payload)``.
    metadata
        Optional module metadata mapping. Successful reads append a canonical
        capability trajectory for accepted boundary history and replay.
    """

    def __init__(
        self,
        registry: ConnectorRegistry,
        *,
        declarations: Iterable[Capability[Any, Any]],
        grant: CapabilityGrant,
        run_id: str,
        task_id: str,
        attempt_id: str,
        module_id: str,
        logical_step: str,
        policy: AccessPolicy | None = None,
        audit: Callable[[str, dict[str, Any]], None] | None = None,
        metadata: dict[str, Any] | None = None,
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
        self.registry = registry
        self.declarations = declared
        self.grant = grant
        self.run_id = run_id
        self.task_id = task_id
        self.attempt_id = attempt_id
        self.module_id = module_id
        self.logical_step = logical_step
        self.policy = policy
        self.audit = audit
        self.metadata = metadata if metadata is not None else {}
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
        started = time.perf_counter()
        try:
            response = await adapter.read(operation, request)
        except Exception:
            self._emit(
                "CAPABILITY_FAILED",
                {**authorized, "reason_code": "adapter-error"},
            )
            raise AccessContractError("connector adapter read failed") from None
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
        safe = dict(payload)
        self._records.append({"event_type": event_type, **safe})
        if self.audit is not None:
            self.audit(event_type, dict(safe))


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
        Whether policy must observe a prior approval before dispatch.
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
        Optional replay-stable semantic identity.
    """

    effectful = False

    def __init__(
        self,
        capability: Capability[RequestT, ResponseT],
        *,
        module_id: str | None = None,
    ) -> None:
        self.input_type = capability.input_type
        self.output_type = capability.output_type
        self.module_id = module_id
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
        Optional replay-stable semantic identity.

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
        self.input_type = effect.input_type
        self.output_type = effect.output_type
        self.module_id = module_id
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
        )
        return cast(ResponseT, result)
