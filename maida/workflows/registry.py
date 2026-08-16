"""Register trusted modules for portable and generated workflow definitions.

Workflow files contain stable aliases and canonical configuration, never
Python import paths or executable code.  Applications decide which factories
those aliases may resolve through :class:`ModuleRegistry`.  Exact module
digests and schemas are recomputed at the trust boundary before a definition
can execute.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Self, cast, get_args, get_origin

from ._canonical import (
    _rehydrate_value,
    canonical_data,
    canonical_json,
    digest_data,
    schema_digest,
    type_schema,
    value_matches_type,
)
from .authoring import Module
from .budget import Budget
from .ir import (
    PlanIR,
    ReplayKey,
    _access_contract,
    _validated_access_declarations,
    module_digest,
)
from .model import _model_contract
from .models import ExecutionSpec

_ALIAS_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")
_HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")
ModuleFactory = Callable[[], Module[Any, Any]]


@dataclass(frozen=True)
class ModuleTemplate[ConfigT]:
    """Trusted parameterized factory exposed to workflow authors.

    Parameters
    ----------
    template_id
        Stable provider-owned identity for the template's semantics.
    version
        Immutable template contract version. Change it when accepted
        configuration or factory interpretation changes incompatibly.
    config_type
        Dataclass, typed dictionary, or other runtime type used to validate and
        document configuration.
    factory
        Application-owned callable that receives validated configuration and
        returns a fresh :class:`~maida.workflows.Module`.

    Raises
    ------
    ValueError
        If the template identity or version is not stable.
    TypeError
        If ``factory`` is not callable.

    Notes
    -----
    The factory itself is never serialized.  A portable workflow records only
    the alias chosen by its registry, this identity/version pair, canonical
    configuration, and the exact resolved module contract.

    Examples
    --------
    >>> template = ModuleTemplate(  # doctest: +SKIP
    ...     "acme.text.prefix", "1", PrefixConfig, PrefixModule
    ... )
    """

    template_id: str
    version: str
    config_type: type[ConfigT]
    factory: Callable[[ConfigT], Module[Any, Any]]

    def __post_init__(self) -> None:
        """Validate stable template metadata."""
        _require_stable("template_id", self.template_id)
        if not isinstance(self.version, str) or not self.version.strip():
            raise ValueError("template version must be a non-empty string")
        if not callable(self.factory):
            raise TypeError("template factory must be callable")

    @property
    def config_schema(self) -> dict[str, Any]:
        """Return deterministic JSON Schema for authoring configuration."""
        return type_schema(self.config_type)

    def create(self, config: Mapping[str, Any]) -> tuple[Module[Any, Any], dict[str, Any]]:
        """Validate canonical configuration and create a fresh module.

        Parameters
        ----------
        config
            JSON-compatible mapping supplied by a human or authoring agent.

        Returns
        -------
        tuple
            Fresh module and canonical configuration including declared
            dataclass defaults.

        Raises
        ------
        ValueError
            If configuration is malformed or violates ``config_type``.
        TypeError
            If the trusted factory does not return a module.
        """
        if not isinstance(config, Mapping):
            raise ValueError("module template config must be an object")
        try:
            encoded = canonical_data(dict(config))
            restored = _rehydrate_value(encoded, self.config_type)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"module template config is invalid: {exc}") from exc
        if not value_matches_type(restored, self.config_type):
            raise ValueError("module template config violates its declared schema")
        module = self.factory(cast(ConfigT, restored))
        if not isinstance(module, Module):
            raise TypeError("module template factory must return a Module instance")
        canonical_config = canonical_data(restored)
        if not isinstance(canonical_config, dict):
            raise ValueError("module template config must encode as an object")
        return module, canonical_config


@dataclass(frozen=True)
class _CatalogEntry:
    module_id: str
    module_digest: str
    input_schema_digests: tuple[str, ...]
    output_schema_digest: str
    execution: Mapping[str, Any]
    capabilities: tuple[Mapping[str, Any], ...]
    effects: tuple[Mapping[str, Any], ...]
    budget: Budget

    def __post_init__(self) -> None:
        object.__setattr__(self, "execution", cast(Mapping[str, Any], _freeze_json(self.execution)))
        object.__setattr__(
            self,
            "capabilities",
            tuple(cast(Mapping[str, Any], _freeze_json(item)) for item in self.capabilities),
        )
        object.__setattr__(
            self,
            "effects",
            tuple(cast(Mapping[str, Any], _freeze_json(item)) for item in self.effects),
        )

    def to_dict(self) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            canonical_data(
                {
                    "budget": self.budget.to_data(),
                    "capabilities": self.capabilities,
                    "effects": self.effects,
                    "execution": self.execution,
                    "input_schema_digests": self.input_schema_digests,
                    "module_digest": self.module_digest,
                    "module_id": self.module_id,
                    "output_schema_digest": self.output_schema_digest,
                }
            ),
        )


def _catalog_entry(
    *,
    module_id: Any,
    module_digest: Any,
    input_schema_digests: Any,
    output_schema_digest: Any,
    execution: Any,
    capabilities: Any,
    effects: Any,
    budget: Any,
    require_canonical: bool = False,
) -> _CatalogEntry:
    if not isinstance(module_id, str) or not module_id.strip():
        raise ValueError("module_id must be a non-empty stable identity")
    _require_digest("module_digest", module_digest)
    if not isinstance(input_schema_digests, (list, tuple)):
        raise ValueError("input_schema_digests must be an ordered sequence")
    inputs = tuple(
        _require_digest(f"input_schema_digests[{index}]", item)
        for index, item in enumerate(input_schema_digests)
    )
    _require_digest("output_schema_digest", output_schema_digest)
    if not isinstance(execution, Mapping):
        raise ValueError("execution must be a canonical ExecutionSpec mapping")
    encoded_execution = cast(dict[str, Any], canonical_data(dict(execution)))
    try:
        restored_execution = ExecutionSpec.from_data(encoded_execution)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"execution is invalid: {exc}") from exc
    if canonical_json(restored_execution.to_data()) != canonical_json(encoded_execution):
        raise ValueError("execution fields do not match the ExecutionSpec contract")
    encoded_capabilities = _validated_access_declarations(
        capabilities,
        expected_kind="capability",
        location="registry capabilities",
        require_canonical=require_canonical,
        error_type=ValueError,
    )
    encoded_effects = _validated_access_declarations(
        effects,
        expected_kind="effect",
        location="registry effects",
        require_canonical=require_canonical,
        error_type=ValueError,
    )
    if not isinstance(budget, Budget):
        if not isinstance(budget, Mapping):
            raise ValueError("budget must be a Budget or canonical budget mapping")
        try:
            restored_budget = Budget.from_data(budget)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"budget is invalid: {exc}") from exc
        if canonical_json(restored_budget.to_data()) != canonical_json(budget):
            raise ValueError("budget fields do not match the canonical budget contract")
        budget = restored_budget
    return _CatalogEntry(
        module_id=module_id,
        module_digest=module_digest,
        input_schema_digests=inputs,
        output_schema_digest=output_schema_digest,
        execution=encoded_execution,
        capabilities=encoded_capabilities,
        effects=encoded_effects,
        budget=budget,
    )


@dataclass(frozen=True)
class _Registration:
    factory: ModuleFactory | None = None
    template: ModuleTemplate[Any] | None = None
    descriptor: _CatalogEntry | None = None


class ModuleRegistry:
    """One trusted registry for authoring, validation, and exact execution.

    Parameters
    ----------
    modules
        Fixed aliases mapped to zero-argument factories. Each resolution must
        return a fresh module with the same immutable behavior contract.
    templates
        Parameterized aliases mapped to :class:`ModuleTemplate` objects with
        explicit configuration schemas.

    Raises
    ------
    ValueError
        If an alias is unstable or appears in both collections.
    TypeError
        If a fixed factory or template has the wrong type.

    Notes
    -----
    Registries are application infrastructure, not durable runtime state. They
    must not contain credentials in descriptions or serialized requirements.
    Binding always recomputes module digests rather than trusting claims from a
    workflow file.

    Examples
    --------
    >>> registry = ModuleRegistry(modules={"text.upper": Upper})  # doctest: +SKIP
    >>> module = registry.resolve("text.upper")  # doctest: +SKIP
    """

    def __init__(
        self,
        modules: Mapping[str, ModuleFactory] | None = None,
        templates: Mapping[str, ModuleTemplate[Any]] | None = None,
    ) -> None:
        registrations: dict[str, _Registration] = {}
        for alias, factory in (modules or {}).items():
            _require_stable("module alias", alias)
            if not callable(factory):
                raise TypeError(f"fixed module factory {alias!r} must be callable")
            registrations[alias] = _Registration(factory=factory)
        for alias, template in (templates or {}).items():
            _require_stable("module alias", alias)
            if not isinstance(template, ModuleTemplate):
                raise TypeError(f"module template {alias!r} must be a ModuleTemplate")
            if alias in registrations:
                raise ValueError(f"module alias {alias!r} is registered twice")
            registrations[alias] = _Registration(template=template)
        self._registrations = MappingProxyType(registrations)

    @classmethod
    def _from_registrations(cls, registrations: Mapping[str, _Registration]) -> Self:
        registry = cls()
        registry._registrations = MappingProxyType(dict(registrations))
        return registry

    def allow(
        self,
        alias: str,
        *,
        module_id: str,
        module_digest: str,
        input_schema_digests: tuple[str, ...],
        output_schema_digest: str,
        execution: Mapping[str, Any],
        budget: Budget,
        capabilities: tuple[Mapping[str, Any], ...] = (),
        effects: tuple[Mapping[str, Any], ...] = (),
    ) -> Self:
        """Return a registry with one trusted validation-only descriptor added."""
        _require_stable("module alias", alias)
        if not isinstance(budget, Budget):
            raise TypeError("budget must be a Budget")
        if alias in self._registrations:
            raise ValueError(f"module alias {alias!r} is already registered")
        entry = _catalog_entry(
            module_id=module_id,
            module_digest=module_digest,
            input_schema_digests=input_schema_digests,
            output_schema_digest=output_schema_digest,
            execution=execution,
            budget=budget,
            capabilities=capabilities,
            effects=effects,
        )
        return self._from_registrations(
            {**self._registrations, alias: _Registration(descriptor=entry)}
        )

    @classmethod
    def from_plan(cls, plan: PlanIR, aliases: Mapping[str, ReplayKey]) -> Self:
        """Build validation descriptors from selected executable plan steps."""
        by_key = {step.replay_key: step for step in plan.executable_steps}
        registry = cls()
        for alias in sorted(aliases):
            replay_key = aliases[alias]
            if not isinstance(replay_key, ReplayKey) or replay_key not in by_key:
                raise ValueError(f"replay key for alias {alias!r} is not in the plan")
            step = by_key[replay_key]
            if step.module_id is None or step.module_digest is None or step.input_binding is None:
                raise ValueError(f"replay key for alias {alias!r} is not executable")
            registry = registry.allow(
                alias,
                module_id=step.module_id,
                module_digest=step.module_digest,
                input_schema_digests=(step.input_binding.schema_digest,),
                output_schema_digest=step.output_schema_digest,
                execution=step.execution or ExecutionSpec().to_data(),
                budget=Budget.from_data(step.budget) if step.budget is not None else Budget(),
                capabilities=step.capabilities,
                effects=step.effects,
            )
        return registry

    @property
    def aliases(self) -> tuple[str, ...]:
        """Return every registered alias in canonical order."""
        return tuple(sorted(self._registrations))

    def resolve(
        self,
        alias: str,
        config: Mapping[str, Any] | None = None,
        *,
        expected_digest: str | None = None,
    ) -> Module[Any, Any]:
        """Resolve an alias and verify an optional exact module digest.

        Parameters
        ----------
        alias
            Stable registry address stored in an authoring specification.
        config
            Configuration for a parameterized template. Fixed modules accept
            only ``None`` or an empty mapping.
        expected_digest
            Optional behavior digest pinned by a compiled definition or
            portable bundle.

        Returns
        -------
        Module
            Fresh trusted module instance.

        Raises
        ------
        KeyError
            If the alias is not registered.
        ValueError
            If configuration or the exact digest does not match.
        TypeError
            If a trusted factory violates its registration contract.
        """
        module, _ = self._resolve(alias, config)
        actual_digest = module_digest(module)
        if expected_digest is not None and actual_digest != expected_digest:
            raise ValueError(f"resolved module digest does not match alias {alias!r}")
        return module

    def resolve_exact(
        self,
        module_id: str,
        expected_digest: str,
    ) -> Module[Any, Any]:
        """Resolve a self-declared module identity and recomputed exact digest."""
        for alias in self.aliases:
            registration = self._registrations[alias]
            if registration.factory is None:
                continue
            module, _ = self._resolve(alias, None)
            if module.module_id == module_id and module_digest(module) == expected_digest:
                return module
        raise LookupError("no exact trusted module binding is registered")

    def descriptor(self, alias: str) -> dict[str, Any]:
        """Return one credential-free generated-plan validation descriptor."""
        return self._entry(alias).to_dict()

    @property
    def digest(self) -> str:
        """Return the content digest of all generated-plan descriptors."""
        return digest_data({alias: self.descriptor(alias) for alias in self.aliases})

    def requirement(
        self,
        alias: str,
        config: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Return credential-free exact binding data for serialization.

        Parameters
        ----------
        alias, config
            Registry address and optional validated template configuration.

        Returns
        -------
        dict
            Canonical alias, template metadata, configuration, module digest,
            schemas, execution requirements, and resource declaration.
        """
        module, canonical_config = self._resolve(alias, config)
        registration = self._registrations[alias]
        template = registration.template
        access = _access_contract(module)
        requirement = {
            "alias": alias,
            "template": (
                {"id": template.template_id, "version": template.version}
                if template is not None
                else None
            ),
            "config": canonical_config,
            "module_digest": module_digest(module),
            "input_schema_digest": schema_digest(module.input_type),
            "output_schema_digest": schema_digest(module.output_type),
            "execution": module.execution.to_data(),
            "budget": module.budget.to_data(),
            "capabilities": list(access["capabilities"]),
            "effects": list(access["effects"]),
            "effectful": module.effectful,
        }
        models = _model_contract(module)
        if models:
            requirement["models"] = list(models)
        return requirement

    def describe(self) -> tuple[dict[str, Any], ...]:
        """Return deterministic authoring metadata without factories or imports."""
        descriptions: list[dict[str, Any]] = []
        for alias in self.aliases:
            registration = self._registrations[alias]
            if registration.descriptor is not None:
                descriptions.append(
                    {"alias": alias, "kind": "trusted", **registration.descriptor.to_dict()}
                )
            elif registration.template is not None:
                template = registration.template
                descriptions.append(
                    {
                        "alias": alias,
                        "kind": "template",
                        "template": {"id": template.template_id, "version": template.version},
                        "config_schema": template.config_schema,
                    }
                )
            else:
                module, _ = self._resolve(alias, None)
                descriptions.append(
                    {
                        "alias": alias,
                        "kind": "fixed",
                        "module_digest": module_digest(module),
                        "input_schema": type_schema(module.input_type),
                        "output_schema": type_schema(module.output_type),
                    }
                )
        return tuple(descriptions)

    def _entry(self, alias: str) -> _CatalogEntry:
        registration = self._registrations.get(alias)
        if registration is None:
            raise KeyError(f"module alias {alias!r} is not allowlisted")
        if registration.descriptor is not None:
            return registration.descriptor
        if registration.template is not None:
            raise ValueError("generated plan aliases cannot require author-supplied config")
        module, _ = self._resolve(alias, None)
        module_id = module.module_id
        if not isinstance(module_id, str) or not module_id.strip():
            raise ValueError(
                f"generated module alias {alias!r} must resolve to a self-declared module_id"
            )
        access = _access_contract(module)
        return _catalog_entry(
            module_id=module_id,
            module_digest=module_digest(module),
            input_schema_digests=_input_schema_digests(module.input_type),
            output_schema_digest=schema_digest(module.output_type),
            execution=module.execution.to_data(),
            budget=module.budget,
            capabilities=access["capabilities"],
            effects=access["effects"],
        )

    def _resolve(
        self,
        alias: str,
        config: Mapping[str, Any] | None,
    ) -> tuple[Module[Any, Any], dict[str, Any]]:
        registration = self._registrations.get(alias)
        if registration is None:
            raise KeyError(f"module alias {alias!r} is not registered")
        if registration.template is not None:
            return registration.template.create(config or {})
        if registration.descriptor is not None:
            raise TypeError(f"module alias {alias!r} has no executable factory")
        if config:
            raise ValueError(f"fixed module alias {alias!r} does not accept config")
        factory = registration.factory
        if factory is None:  # pragma: no cover - registration invariant
            raise TypeError("fixed module registration has no factory")
        module = factory()
        if not isinstance(module, Module):
            raise TypeError(f"fixed module factory {alias!r} must return a Module instance")
        return module, {}


def _require_stable(label: str, value: str) -> None:
    if not isinstance(value, str) or _ALIAS_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{label} must be a stable identifier")


def _require_digest(label: str, value: Any) -> str:
    if not isinstance(value, str) or _HEX_DIGEST.fullmatch(value) is None:
        raise ValueError(f"{label} must be a lowercase sha256 digest")
    return value


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_json(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    return value


def _input_schema_digests(input_type: Any) -> tuple[str, ...]:
    arguments = get_args(input_type)
    if get_origin(input_type) is tuple and arguments and arguments[-1] is not Ellipsis:
        return tuple(schema_digest(argument) for argument in arguments)
    return (schema_digest(input_type),)
