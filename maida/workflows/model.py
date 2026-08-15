"""Invoke interchangeable model providers through a metered typed boundary.

Workflow modules declare stable model contracts while deployments register the
provider adapters that satisfy them. The runtime reserves declared resources
before invocation, records both declared and served model identities, validates
typed responses, and emits canonical trajectory evidence for replay and drift
analysis. Credentials and provider client objects never enter Workflow IR.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Protocol, cast

from ._canonical import canonical_data, digest_data, schema_digest, value_matches_type
from .budget import BudgetUsage

_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")
_SENSITIVE_NAMES = frozenset(
    {"api_key", "credential", "credentials", "password", "secret", "token"}
)


@dataclass(frozen=True)
class ModelSpec[RequestT, ResponseT]:
    """Behavior-bearing declaration of one typed model endpoint.

    Parameters
    ----------
    name
        Stable module-local name used by :meth:`ModelBroker.call`.
    provider
        Stable provider adapter family selected by deployment configuration.
    model
        Declared provider model identity.
    input_type, output_type
        Python boundary contracts validated around every invocation.
    pinned_version
        Optional immutable provider version requested by the definition.
    configuration
        Canonical non-sensitive behavior configuration, such as temperature.

    Notes
    -----
    The declaration enters module and workflow content identity. Provider
    credentials, endpoints, client instances, and the model actually served at
    runtime do not. The latter is recorded as execution evidence instead.
    """

    name: str
    provider: str
    model: str
    input_type: type[RequestT]
    output_type: type[ResponseT]
    pinned_version: str | None = None
    configuration: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate stable identities and freeze canonical configuration."""
        for label, value in (("name", self.name), ("provider", self.provider)):
            if not isinstance(value, str) or _NAME_PATTERN.fullmatch(value) is None:
                raise ValueError(f"model {label} must be a stable identifier")
        if not isinstance(self.model, str) or not self.model.strip():
            raise ValueError("declared model identity must be non-empty")
        if self.pinned_version is not None and not self.pinned_version.strip():
            raise ValueError("pinned model version must be non-empty when supplied")
        configuration = canonical_data(dict(self.configuration))
        if _contains_sensitive_configuration(configuration):
            raise ValueError("model configuration cannot contain credentials or secrets")
        object.__setattr__(self, "configuration", MappingProxyType(configuration))

    @property
    def input_schema_digest(self) -> str:
        """Return the canonical request schema digest."""
        return schema_digest(self.input_type)

    @property
    def output_schema_digest(self) -> str:
        """Return the canonical response schema digest."""
        return schema_digest(self.output_type)

    def to_data(self) -> dict[str, Any]:
        """Return the credential-free declaration compiled into Workflow IR."""
        return {
            "configuration": canonical_data(self.configuration),
            "input_schema_digest": self.input_schema_digest,
            "model": self.model,
            "name": self.name,
            "output_schema_digest": self.output_schema_digest,
            "pinned_version": self.pinned_version,
            "provider": self.provider,
        }


@dataclass(frozen=True)
class ModelCallResult[ResponseT]:
    """Typed provider result plus authoritative served identity and usage.

    Parameters
    ----------
    output
        Provider response validated against the selected declaration.
    served_model
        Best stable identity the provider reports for the model actually used.
    usage
        Measured tokens, cost, and elapsed provider time. It must not exceed the
        adapter's conservative pre-call reservation.
    input_tokens, output_tokens
        Optional provider-reported token split. When omitted, combined model
        tokens are attributed to input for backward-compatible accounting.
    metadata
        Optional canonical, non-sensitive provider evidence.
    """

    output: ResponseT
    served_model: str
    usage: BudgetUsage
    input_tokens: int | None = None
    output_tokens: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate served identity, usage, and canonical metadata."""
        if not isinstance(self.served_model, str) or not self.served_model.strip():
            raise ValueError("served_model must be non-empty")
        if not isinstance(self.usage, BudgetUsage):
            raise TypeError("model result usage must be BudgetUsage")
        if self.input_tokens is None and self.output_tokens is None:
            object.__setattr__(self, "input_tokens", self.usage.model_tokens)
            object.__setattr__(self, "output_tokens", 0)
        elif (
            type(self.input_tokens) is not int
            or type(self.output_tokens) is not int
            or self.input_tokens < 0
            or self.output_tokens < 0
            or self.input_tokens + self.output_tokens != self.usage.model_tokens
        ):
            raise ValueError(
                "model token breakdown must be nonnegative and equal measured model_tokens"
            )
        object.__setattr__(
            self,
            "metadata",
            MappingProxyType(cast(dict[str, Any], canonical_data(dict(self.metadata)))),
        )


class ModelAdapter(Protocol):
    """Deployment-owned provider interface used by :class:`ModelBroker`."""

    def estimate_call(self, model: ModelSpec[Any, Any], request: Any) -> BudgetUsage:
        """Return a conservative reservation before any provider access."""
        ...

    async def call(self, model: ModelSpec[Any, Any], request: Any) -> ModelCallResult[Any]:
        """Invoke the provider and return typed output with measured usage."""
        ...


class ModelAdapterRegistry:
    """Resolve model provider adapters without serializing their credentials.

    Parameters
    ----------
    adapters
        Mapping from stable provider names to deployment-owned adapters.
    """

    def __init__(self, adapters: Mapping[str, ModelAdapter] | None = None) -> None:
        values = dict(adapters or {})
        for name, adapter in values.items():
            if _NAME_PATTERN.fullmatch(name) is None:
                raise ValueError("model adapter names must be stable identifiers")
            if not callable(getattr(adapter, "estimate_call", None)) or not callable(
                getattr(adapter, "call", None)
            ):
                raise TypeError("model adapters must implement estimate_call() and call()")
        self._adapters = MappingProxyType(values)

    def resolve(self, provider: str) -> ModelAdapter:
        """Return the adapter for a declared provider or fail closed.

        Raises
        ------
        LookupError
            If deployment configuration has no adapter for ``provider``.
        """
        try:
            return self._adapters[provider]
        except KeyError as exc:
            raise LookupError(f"model provider {provider!r} is not registered") from exc


class _ModelBudgetMeter(Protocol):
    def reserve_model(self, name: str, usage: BudgetUsage) -> str: ...

    def commit_model(self, reservation: str, usage: BudgetUsage) -> None: ...


class ModelBroker:
    """Validate, meter, invoke, and record declared model calls.

    Modules access this object as ``ctx.models``. Direct provider clients are
    intentionally absent from :class:`~maida.workflows.ExecutionContext`, which
    keeps metering and served-model evidence mechanically unavoidable for the
    supported model path.
    """

    def __init__(
        self,
        registry: ModelAdapterRegistry,
        declarations: tuple[ModelSpec[Any, Any], ...],
        *,
        meter: _ModelBudgetMeter,
        metadata: dict[str, Any],
        audit: Any,
    ) -> None:
        if any(not isinstance(item, ModelSpec) for item in declarations):
            raise TypeError("model declarations must contain ModelSpec values")
        if len({item.name for item in declarations}) != len(declarations):
            raise ValueError("model declaration names must be unique")
        self.registry = registry
        self.declarations = {item.name: item for item in declarations}
        self.meter = meter
        self.metadata = metadata
        self.audit = audit

    async def call[ResponseT](self, name: str, request: Any) -> ResponseT:
        """Invoke one declared model after durable budget preflight.

        Parameters
        ----------
        name
            Exact module-local :class:`ModelSpec` name.
        request
            Typed, canonically digestible provider request.

        Returns
        -------
        ResponseT
            Validated provider output.

        Raises
        ------
        LookupError
            If the model or provider adapter is not registered.
        TypeError
            If request, estimate, result, or output violates its contract.
        BudgetExceededError
            If the conservative reservation exceeds remaining task resources.
        """
        try:
            declaration = self.declarations[name]
        except KeyError as exc:
            raise LookupError(f"model {name!r} is not declared by the module") from exc
        if not value_matches_type(request, declaration.input_type):
            raise TypeError("model request violates its declared input contract")
        request_digest = digest_data(request)
        adapter = self.registry.resolve(declaration.provider)
        estimate = adapter.estimate_call(declaration, request)
        if not isinstance(estimate, BudgetUsage):
            raise TypeError("model adapter estimate must be BudgetUsage")
        reservation = self.meter.reserve_model(name, estimate)
        result = await adapter.call(declaration, request)
        if not isinstance(result, ModelCallResult):
            raise TypeError("model adapter must return ModelCallResult")
        if not value_matches_type(result.output, declaration.output_type):
            raise TypeError("model response violates its declared output contract")
        self.meter.commit_model(reservation, result.usage)
        response_digest = digest_data(result.output)
        evidence = {
            "declared_model": declaration.model,
            "pinned_version": declaration.pinned_version,
            "provider": declaration.provider,
            "served_model": result.served_model,
        }
        self.audit("MODEL_RESOLVED", {**evidence, "model_name": name})
        self.audit(
            "MODEL_CALLED",
            {
                **evidence,
                "model_name": name,
                "request_digest": request_digest,
                "response_digest": response_digest,
                "usage": result.usage.to_data(),
            },
        )
        self.metadata.setdefault("trajectories", []).append(
            {
                "kind": "model",
                "name": name,
                "request_digest": request_digest,
                "response_digest": response_digest,
                "metadata": {**evidence, **canonical_data(result.metadata)},
            }
        )
        usage = self.metadata.setdefault("usage", {})
        usage["input_tokens"] = int(usage.get("input_tokens", 0)) + cast(int, result.input_tokens)
        usage["output_tokens"] = int(usage.get("output_tokens", 0)) + cast(
            int, result.output_tokens
        )
        usage["cost_usd"] = float(usage.get("cost_usd", 0.0)) + result.usage.cost_usd
        return cast(ResponseT, result.output)


def _model_contract(module: Any) -> tuple[dict[str, Any], ...]:
    declarations = getattr(module, "models", ())
    if not isinstance(declarations, tuple) or any(
        not isinstance(item, ModelSpec) for item in declarations
    ):
        raise TypeError("module models must be a tuple of ModelSpec declarations")
    names = [item.name for item in declarations]
    if len(names) != len(set(names)):
        raise ValueError("module model declaration names must be unique")
    return tuple(sorted((item.to_data() for item in declarations), key=lambda item: item["name"]))


def _validated_model_contract(
    raw: Any, *, require_canonical: bool = True
) -> tuple[dict[str, Any], ...]:
    if not isinstance(raw, (list, tuple)):
        raise ValueError("model declarations must be an array")
    expected = {
        "configuration",
        "input_schema_digest",
        "model",
        "name",
        "output_schema_digest",
        "pinned_version",
        "provider",
    }
    declarations: list[dict[str, Any]] = []
    for index, item in enumerate(raw):
        if not isinstance(item, Mapping) or set(item) != expected:
            raise ValueError(f"model declaration {index} fields are invalid")
        data = cast(dict[str, Any], canonical_data(dict(item)))
        for field_name in ("name", "provider"):
            value = data[field_name]
            if not isinstance(value, str) or _NAME_PATTERN.fullmatch(value) is None:
                raise ValueError(f"model declaration {index} {field_name} is invalid")
        if not isinstance(data["model"], str) or not data["model"].strip():
            raise ValueError(f"model declaration {index} model is invalid")
        if data["pinned_version"] is not None and (
            not isinstance(data["pinned_version"], str) or not data["pinned_version"].strip()
        ):
            raise ValueError(f"model declaration {index} pinned_version is invalid")
        if not isinstance(data["configuration"], Mapping):
            raise ValueError(f"model declaration {index} configuration is invalid")
        if _contains_sensitive_configuration(data["configuration"]):
            raise ValueError(f"model declaration {index} contains sensitive configuration")
        for field_name in ("input_schema_digest", "output_schema_digest"):
            digest = data[field_name]
            if (
                not isinstance(digest, str)
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ValueError(f"model declaration {index} {field_name} is invalid")
        declarations.append(data)
    names = [item["name"] for item in declarations]
    if len(names) != len(set(names)):
        raise ValueError("model declaration names must be unique")
    canonical = tuple(sorted(declarations, key=lambda item: item["name"]))
    if require_canonical and tuple(declarations) != canonical:
        raise ValueError("model declarations must be in canonical order")
    return canonical


def _contains_sensitive_configuration(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(
            str(name).lower().replace("-", "_") in _SENSITIVE_NAMES
            or _contains_sensitive_configuration(child)
            for name, child in value.items()
        )
    if isinstance(value, list):
        return any(_contains_sensitive_configuration(item) for item in value)
    return False
