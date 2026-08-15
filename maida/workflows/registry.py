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
from typing import Any, cast

from ._canonical import (
    _rehydrate_value,
    canonical_data,
    schema_digest,
    type_schema,
    value_matches_type,
)
from .authoring import Module
from .ir import _access_contract, module_digest
from .model import _model_contract

_ALIAS_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")
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
class _Registration:
    factory: ModuleFactory | None = None
    template: ModuleTemplate[Any] | None = None


class ModuleRegistry:
    """Trusted alias registry for reconstructable workflow modules.

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
            if registration.template is not None:
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
