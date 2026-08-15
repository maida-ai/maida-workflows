"""Declare typed external reads and effects as replaceable workflow modules.

Access declarations describe stable graph behavior without embedding provider
credentials or runtime clients. :class:`Connector` and :class:`Effect` are the
recommended explicit boundaries: they compile into inspectable IR, route live
access through the runtime broker, and can be stubbed or compared during replay.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, cast

from ._canonical import schema_digest
from .authoring import ExecutionContext, Module

_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")


class AccessContractError(RuntimeError):
    """Raised when supported external access lacks a valid runtime broker."""


class Idempotency(StrEnum):
    """Guarantee an effect requires from its destination adapter."""

    NONE = "none"
    OPTIONAL = "optional"
    REQUIRED = "required"


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
