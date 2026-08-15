"""Validate generated graph fragments against trusted module definitions.

This module keeps generated planning output deliberately smaller than the
static workflow IR. A :class:`PlanFragmentIR` contains graph choices only;
module identities, schemas, execution requirements, and external-access
declarations remain in a trusted :class:`ModuleCatalog` and are resolved by
:class:`PlanValidator` before any later materialization step may use them.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Self, cast

from ._canonical import canonical_data, canonical_json, digest_data
from .ir import PlanIR, ReplayKey, _validated_access_declarations
from .models import ExecutionSpec

PLAN_FRAGMENT_VERSION = "0.1.0"
PLAN_SIGNATURE_VERSION = "0.1.0"
_REGION_INPUT = "$input"
_STABLE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
_HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FRAGMENT_FIELDS = {
    "fragment_id",
    "nodes",
    "outputs",
    "revision",
    "supersedes",
    "version",
}
_NODE_FIELDS = {"dependencies", "key", "module_alias"}


class PlanValidationError(ValueError):
    """Generated plan data failed a stable validation rule.

    Parameters
    ----------
    code
        Machine-readable reason suitable for policy reports and API errors.
    message
        Human-readable diagnostic that identifies the rejected contract.

    Attributes
    ----------
    code
        Stable machine-readable failure category.
    """

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


def _error(code: str, message: str) -> PlanValidationError:
    return PlanValidationError(code, message)


def _require_stable_name(
    value: Any,
    *,
    label: str,
    error_code: str = "PLAN_FRAGMENT_INVALID",
) -> str:
    if not isinstance(value, str) or _STABLE_NAME.fullmatch(value) is None:
        raise _error(error_code, f"{label} must be a stable name")
    return value


def _require_digest(
    value: Any,
    *,
    label: str,
    error_code: str = "PLAN_FRAGMENT_INVALID",
) -> str:
    if not isinstance(value, str) or _HEX_DIGEST.fullmatch(value) is None:
        raise _error(error_code, f"{label} must be a lowercase sha256 digest")
    return value


def _require_nonnegative_integer(
    value: Any,
    *,
    label: str,
    error_code: str | None = None,
) -> int:
    if type(value) is not int:
        if error_code is not None:
            raise _error(error_code, f"{label} must be an integer")
        raise ValueError(f"{label} must be an integer")
    if value < 0:
        if error_code is not None:
            raise _error(error_code, f"{label} must be non-negative")
        raise ValueError(f"{label} must be non-negative")
    return value


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_json(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    return value


def _require_exact_fields(
    data: Mapping[str, Any],
    expected: set[str],
    *,
    label: str,
    error_code: str = "PLAN_FRAGMENT_INVALID",
) -> None:
    if set(data) != expected:
        raise _error(error_code, f"{label} fields do not match the contract")


@dataclass(frozen=True)
class PlanNode:
    """One generated module selection and its ordered dependencies.

    A node carries only planner-controlled graph data. The selected alias is
    resolved against a trusted :class:`ModuleCatalog`; generated content never
    supplies module digests, schemas, code locations, execution requirements,
    credentials, grants, capabilities, effects, or budgets.

    Parameters
    ----------
    key
        Stable identity for this node within its fragment and revision.
    module_alias
        Allowlisted alias to resolve in the trusted module catalog.
    dependencies
        Ordered source keys for the module input ports. ``$input`` identifies
        the trusted surrounding region input.
    """

    key: str
    module_alias: str
    dependencies: tuple[str, ...]

    def __post_init__(self) -> None:
        """Canonicalize sequence storage and reject unsafe identity syntax."""
        _require_stable_name(self.key, label="plan node key")
        _require_stable_name(self.module_alias, label="module alias")
        if not isinstance(self.dependencies, (list, tuple)):
            raise _error(
                "PLAN_FRAGMENT_INVALID",
                "PlanNode dependencies must be an ordered sequence",
            )
        dependencies = tuple(self.dependencies)
        for dependency in dependencies:
            if dependency != _REGION_INPUT:
                _require_stable_name(dependency, label="plan dependency")
        object.__setattr__(self, "dependencies", dependencies)

    def to_dict(self) -> dict[str, Any]:
        """Return the minimal canonical generated-node representation.

        Returns
        -------
        dict
            JSON-compatible graph data containing no trusted module metadata.
        """
        return {
            "dependencies": list(self.dependencies),
            "key": self.key,
            "module_alias": self.module_alias,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        """Load a node while rejecting unknown or non-generated fields.

        Parameters
        ----------
        data
            Mapping with exactly ``key``, ``module_alias``, and
            ``dependencies``.

        Returns
        -------
        PlanNode
            Strictly decoded generated graph node.

        Raises
        ------
        PlanValidationError
            If trusted, executable, or malformed data is present.
        """
        if not isinstance(data, Mapping):
            raise _error("PLAN_FRAGMENT_INVALID", "PlanNode must be an object")
        _require_exact_fields(data, _NODE_FIELDS, label="PlanNode")
        dependencies = data["dependencies"]
        if not isinstance(dependencies, list):
            raise _error("PLAN_FRAGMENT_INVALID", "PlanNode dependencies must be an array")
        try:
            return cls(
                key=data["key"],
                module_alias=data["module_alias"],
                dependencies=tuple(dependencies),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, PlanValidationError):
                raise
            raise _error("PLAN_FRAGMENT_INVALID", str(exc)) from exc


@dataclass(frozen=True)
class PlanFragmentIR:
    """Versioned generated DAG fragment containing graph choices only.

    Parameters
    ----------
    fragment_id
        Stable diagnostic identity for this planner output. It is not used to
        resolve module definitions.
    revision
        One-based revision number checked against trusted runtime lineage.
    supersedes
        Digest of the preceding fragment, or ``None`` for the initial plan.
    nodes
        Generated module aliases and dependency topology.
    outputs
        Ordered node keys returned from the surrounding dynamic region.
    version
        Fragment schema version. Only ``0.1.0`` is currently accepted.

    Notes
    -----
    This type is intentionally separate from :class:`~maida.workflows.ir.PlanIR`.
    It cannot carry executable code or authorize runtime behavior.
    """

    fragment_id: str
    revision: int
    supersedes: str | None
    nodes: tuple[PlanNode, ...]
    outputs: tuple[str, ...]
    version: str = PLAN_FRAGMENT_VERSION

    def __post_init__(self) -> None:
        """Canonicalize node order and validate fragment identity fields."""
        if self.version != PLAN_FRAGMENT_VERSION:
            raise _error(
                "PLAN_FRAGMENT_VERSION_UNSUPPORTED",
                f"unsupported PlanFragmentIR version {self.version!r}",
            )
        _require_stable_name(self.fragment_id, label="fragment_id")
        _require_nonnegative_integer(
            self.revision,
            label="revision",
            error_code="PLAN_FRAGMENT_INVALID",
        )
        if self.revision == 0:
            raise _error("PLAN_FRAGMENT_INVALID", "revision must be at least one")
        if self.supersedes is not None:
            _require_digest(self.supersedes, label="supersedes")
        if self.revision == 1 and self.supersedes is not None:
            raise _error("PLAN_REVISION_INVALID", "initial plan supersedes must be null")
        if self.revision > 1 and self.supersedes is None:
            raise _error("PLAN_REVISION_INVALID", "revised plan must provide a supersedes digest")
        if not isinstance(self.nodes, (list, tuple)) or any(
            not isinstance(node, PlanNode) for node in self.nodes
        ):
            raise _error("PLAN_FRAGMENT_INVALID", "nodes must contain PlanNode values")
        if not isinstance(self.outputs, (list, tuple)):
            raise _error("PLAN_FRAGMENT_INVALID", "outputs must be an ordered sequence")
        nodes = tuple(sorted(tuple(self.nodes), key=lambda node: node.key))
        outputs = tuple(self.outputs)
        for output in outputs:
            _require_stable_name(output, label="fragment output")
        object.__setattr__(self, "nodes", nodes)
        object.__setattr__(self, "outputs", outputs)

    def to_dict(self) -> dict[str, Any]:
        """Return canonical JSON-compatible generated fragment data."""
        return {
            "fragment_id": self.fragment_id,
            "nodes": [node.to_dict() for node in self.nodes],
            "outputs": list(self.outputs),
            "revision": self.revision,
            "supersedes": self.supersedes,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        """Strictly import untrusted generated fragment data.

        Parameters
        ----------
        data
            Mapping encoded according to the supported fragment version.

        Returns
        -------
        PlanFragmentIR
            Canonical fragment ready for trusted validation.

        Raises
        ------
        PlanValidationError
            If the version, shape, canonical order, or identity is invalid.
        """
        if not isinstance(data, Mapping):
            raise _error("PLAN_FRAGMENT_INVALID", "PlanFragmentIR must be an object")
        _require_exact_fields(data, _FRAGMENT_FIELDS, label="PlanFragmentIR")
        if data["version"] != PLAN_FRAGMENT_VERSION:
            raise _error(
                "PLAN_FRAGMENT_VERSION_UNSUPPORTED",
                f"unsupported PlanFragmentIR version {data['version']!r}",
            )
        raw_nodes = data["nodes"]
        raw_outputs = data["outputs"]
        if not isinstance(raw_nodes, list):
            raise _error("PLAN_FRAGMENT_INVALID", "PlanFragmentIR nodes must be an array")
        if not isinstance(raw_outputs, list):
            raise _error("PLAN_FRAGMENT_INVALID", "PlanFragmentIR outputs must be an array")
        nodes = tuple(PlanNode.from_dict(item) for item in raw_nodes)
        keys = [node.key for node in nodes]
        if keys != sorted(keys):
            raise _error(
                "PLAN_FRAGMENT_INVALID",
                "PlanFragmentIR nodes must be in canonical key order",
            )
        if len(keys) != len(set(keys)):
            raise _error("PLAN_TOPOLOGY_INVALID", "fragment contains a duplicate node key")
        try:
            return cls(
                fragment_id=data["fragment_id"],
                revision=data["revision"],
                supersedes=data["supersedes"],
                nodes=nodes,
                outputs=tuple(raw_outputs),
                version=data["version"],
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, PlanValidationError):
                raise
            raise _error("PLAN_FRAGMENT_INVALID", str(exc)) from exc

    def canonical_json(self) -> str:
        """Serialize the generated fragment as deterministic canonical JSON."""
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        """Return the SHA-256 digest of this fragment and its lineage fields."""
        return digest_data(self.to_dict())


@dataclass(frozen=True)
class PlanLimits:
    """Hard structural limits applied to every generated fragment.

    Parameters
    ----------
    max_nodes
        Maximum number of generated module nodes.
    max_depth
        Maximum dependency depth, counting the first module as depth one.
    max_fanout
        Maximum number of direct consumers of any node or region input.
    max_replans
        Maximum number of replans after the initial revision one.
    """

    max_nodes: int
    max_depth: int
    max_fanout: int
    max_replans: int

    def __post_init__(self) -> None:
        """Reject nonsensical or boolean structural limits."""
        for label, value in (
            ("max_nodes", self.max_nodes),
            ("max_depth", self.max_depth),
            ("max_fanout", self.max_fanout),
            ("max_replans", self.max_replans),
        ):
            _require_nonnegative_integer(value, label=label)
        if self.max_nodes == 0:
            raise ValueError("max_nodes must be positive")
        if self.max_depth == 0:
            raise ValueError("max_depth must be positive")


_CATALOG_FIELDS = {
    "capabilities",
    "effects",
    "execution",
    "input_schema_digests",
    "module_digest",
    "module_id",
    "output_schema_digest",
}
_RESOLVED_NODE_FIELDS = _CATALOG_FIELDS | {"dependencies", "key", "module_alias"}


@dataclass(frozen=True)
class _CatalogEntry:
    module_id: str
    module_digest: str
    input_schema_digests: tuple[str, ...]
    output_schema_digest: str
    execution: Mapping[str, Any]
    capabilities: tuple[Mapping[str, Any], ...]
    effects: tuple[Mapping[str, Any], ...]

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
) -> _CatalogEntry:
    if not isinstance(module_id, str) or not module_id.strip():
        raise ValueError("module_id must be a non-empty stable identity")
    _require_digest(module_digest, label="module_digest")
    if not isinstance(input_schema_digests, (list, tuple)):
        raise ValueError("input_schema_digests must be an ordered sequence")
    inputs = tuple(
        _require_digest(item, label=f"input_schema_digests[{index}]")
        for index, item in enumerate(input_schema_digests)
    )
    _require_digest(output_schema_digest, label="output_schema_digest")
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
        location="catalog capabilities",
        require_canonical=False,
        error_type=ValueError,
    )
    encoded_effects = _validated_access_declarations(
        effects,
        expected_kind="effect",
        location="catalog effects",
        require_canonical=False,
        error_type=ValueError,
    )
    return _CatalogEntry(
        module_id=module_id,
        module_digest=module_digest,
        input_schema_digests=inputs,
        output_schema_digest=output_schema_digest,
        execution=encoded_execution,
        capabilities=encoded_capabilities,
        effects=encoded_effects,
    )


class ModuleCatalog:
    """Immutable allowlist of trusted module aliases and definition pins.

    A catalog is constructed by trusted application or deployment code, never
    from generated plan content. Each alias resolves to immutable module and
    schema digests plus credential-free execution, capability, and effect
    declarations. Runtime grants and adapter credentials are intentionally not
    representable.

    Notes
    -----
    Use :meth:`allow` to build a catalog explicitly or :meth:`from_plan` to
    select replay-addressable steps from an existing static workflow IR.
    """

    def __init__(self) -> None:
        self._entries: Mapping[str, _CatalogEntry] = MappingProxyType({})

    @classmethod
    def _from_entries(cls, entries: Mapping[str, _CatalogEntry]) -> ModuleCatalog:
        catalog = cls()
        catalog._entries = MappingProxyType(dict(entries))
        return catalog

    @property
    def aliases(self) -> tuple[str, ...]:
        """Return registered aliases in deterministic lexical order."""
        return tuple(sorted(self._entries))

    def allow(
        self,
        alias: str,
        *,
        module_id: str,
        module_digest: str,
        input_schema_digests: tuple[str, ...],
        output_schema_digest: str,
        execution: Mapping[str, Any],
        capabilities: tuple[Mapping[str, Any], ...] = (),
        effects: tuple[Mapping[str, Any], ...] = (),
    ) -> ModuleCatalog:
        """Return a new catalog containing one additional trusted alias.

        Parameters
        ----------
        alias
            Stable planner-visible name. An existing alias cannot be replaced.
        module_id, module_digest
            Trusted semantic and immutable behavior identities.
        input_schema_digests
            Ordered input-port schema digests matched to generated dependencies.
        output_schema_digest
            Typed output contract exposed to downstream generated nodes.
        execution
            Canonical credential-free execution requirements.
        capabilities, effects
            Canonical external-access declarations inherited from the trusted
            module definition. They cannot be altered by generated plans.

        Returns
        -------
        ModuleCatalog
            New immutable allowlist retaining all existing aliases.

        Raises
        ------
        ValueError
            If an identity or descriptor is invalid or the alias already exists.
        """
        try:
            _require_stable_name(alias, label="module alias")
            entry = _catalog_entry(
                module_id=module_id,
                module_digest=module_digest,
                input_schema_digests=input_schema_digests,
                output_schema_digest=output_schema_digest,
                execution=execution,
                capabilities=capabilities,
                effects=effects,
            )
        except PlanValidationError as exc:
            raise ValueError(str(exc)) from exc
        if alias in self._entries:
            raise ValueError(f"module alias {alias!r} is already registered")
        return ModuleCatalog._from_entries({**self._entries, alias: entry})

    @classmethod
    def from_plan(
        cls,
        plan: PlanIR,
        aliases: Mapping[str, ReplayKey],
    ) -> ModuleCatalog:
        """Build an allowlist from selected executable static-plan steps.

        Parameters
        ----------
        plan
            Validated static workflow definition containing trusted module pins.
        aliases
            Planner-visible aliases mapped to exact replay keys in ``plan``.

        Returns
        -------
        ModuleCatalog
            Credential-free immutable projection of the selected definitions.

        Raises
        ------
        ValueError
            If an alias is invalid or a replay key is absent or non-executable.

        Notes
        -----
        Static modules expose one aggregate input contract. A later runtime may
        register explicit multi-port adapters with :meth:`allow` when needed.
        """
        by_key = {step.replay_key: step for step in plan.executable_steps}
        catalog = cls()
        for alias in sorted(aliases):
            replay_key = aliases[alias]
            if not isinstance(replay_key, ReplayKey) or replay_key not in by_key:
                raise ValueError(f"replay key for alias {alias!r} is not in the plan")
            step = by_key[replay_key]
            if step.module_id is None or step.module_digest is None or step.input_binding is None:
                raise ValueError(f"replay key for alias {alias!r} is not executable")
            catalog = catalog.allow(
                alias,
                module_id=step.module_id,
                module_digest=step.module_digest,
                input_schema_digests=(step.input_binding.schema_digest,),
                output_schema_digest=step.output_schema_digest,
                execution=step.execution or ExecutionSpec().to_data(),
                capabilities=step.capabilities,
                effects=step.effects,
            )
        return catalog

    def resolve(self, alias: str) -> dict[str, Any]:
        """Return a copy of one trusted descriptor for inspection.

        Parameters
        ----------
        alias
            Exact planner-visible alias to resolve.

        Returns
        -------
        dict
            Canonical credential-free module descriptor.

        Raises
        ------
        KeyError
            If the alias is not allowlisted.
        """
        try:
            return self._entries[alias].to_dict()
        except KeyError as exc:
            raise KeyError(f"module alias {alias!r} is not allowlisted") from exc

    def to_dict(self) -> dict[str, Any]:
        """Return the canonical trusted alias descriptor mapping."""
        return {alias: self._entries[alias].to_dict() for alias in sorted(self._entries)}

    @property
    def digest(self) -> str:
        """Return the content digest of the complete trusted allowlist."""
        return digest_data(self.to_dict())

    def _entry(self, alias: str) -> _CatalogEntry:
        try:
            return self._entries[alias]
        except KeyError as exc:
            raise _error(
                "PLAN_MODULE_NOT_ALLOWED",
                f"module alias {alias!r} is not allowlisted",
            ) from exc


_SIGNATURE_FIELDS = {
    "max_depth",
    "max_fanout",
    "module_composition",
    "node_count",
    "output_schema_digests",
    "outputs",
    "region_input_schema_digest",
    "resolved_nodes",
    "revision",
    "supersedes",
    "topology_digest",
    "version",
}


@dataclass(frozen=True)
class PlanSignature:
    """Trusted behavioral signature of a validated generated fragment.

    The signature resolves every generated alias through the immutable catalog
    and therefore contains the module pins, typed ports, execution requirements,
    and external-access declarations that the generated fragment itself cannot
    provide. It is suitable for structural comparison and later materialization;
    it is not an authorization grant.

    Parameters
    ----------
    revision, supersedes
        Validated lineage copied from the generated fragment.
    node_count, max_depth, max_fanout
        Structural measurements checked against :class:`PlanLimits`.
    module_composition
        Canonically ordered ``(alias, count)`` pairs.
    topology_digest
        Digest of resolved nodes and ordered output keys, excluding the
        diagnostic fragment label.
    resolved_nodes
        Canonical trusted module descriptors joined to generated topology.
    region_input_schema_digest
        Trusted schema supplied to ``$input`` dependency ports.
    outputs
        Ordered resolved node keys returned by the generated region.
    output_schema_digests
        Validated ordered schemas returned by the dynamic region.
    version
        Signature schema version.
    """

    revision: int
    supersedes: str | None
    node_count: int
    max_depth: int
    max_fanout: int
    module_composition: tuple[tuple[str, int], ...]
    topology_digest: str
    resolved_nodes: tuple[Mapping[str, Any], ...]
    region_input_schema_digest: str
    outputs: tuple[str, ...]
    output_schema_digests: tuple[str, ...]
    version: str = PLAN_SIGNATURE_VERSION

    def __post_init__(self) -> None:
        """Canonicalize trusted records and validate signature primitives."""
        if self.version != PLAN_SIGNATURE_VERSION:
            raise _error(
                "PLAN_SIGNATURE_VERSION_UNSUPPORTED",
                f"unsupported PlanSignature version {self.version!r}",
            )
        _require_nonnegative_integer(
            self.revision,
            label="revision",
            error_code="PLAN_SIGNATURE_INVALID",
        )
        if self.revision == 0:
            raise _error("PLAN_SIGNATURE_INVALID", "revision must be at least one")
        if self.supersedes is not None:
            _require_digest(
                self.supersedes,
                label="supersedes",
                error_code="PLAN_SIGNATURE_INVALID",
            )
        if self.revision == 1 and self.supersedes is not None:
            raise _error("PLAN_SIGNATURE_INVALID", "initial signature supersedes must be null")
        if self.revision > 1 and self.supersedes is None:
            raise _error("PLAN_SIGNATURE_INVALID", "revised signature requires supersedes")
        for label, value in (
            ("node_count", self.node_count),
            ("max_depth", self.max_depth),
            ("max_fanout", self.max_fanout),
        ):
            _require_nonnegative_integer(
                value,
                label=label,
                error_code="PLAN_SIGNATURE_INVALID",
            )
        _require_digest(
            self.topology_digest,
            label="topology_digest",
            error_code="PLAN_SIGNATURE_INVALID",
        )
        composition = tuple(sorted(tuple(self.module_composition)))
        for alias, count in composition:
            _require_stable_name(
                alias,
                label="module composition alias",
                error_code="PLAN_SIGNATURE_INVALID",
            )
            if type(count) is not int or count < 1:
                raise _error(
                    "PLAN_SIGNATURE_INVALID",
                    "module composition counts must be positive integers",
                )
        if len(composition) != len({alias for alias, _count in composition}):
            raise _error("PLAN_SIGNATURE_INVALID", "module composition contains duplicate aliases")
        mutable_resolved = tuple(
            sorted(
                (cast(dict[str, Any], canonical_data(dict(node))) for node in self.resolved_nodes),
                key=lambda node: str(node.get("key")),
            )
        )
        for node in mutable_resolved:
            _validate_resolved_node(node)
        region_input = _require_digest(
            self.region_input_schema_digest,
            label="region_input_schema_digest",
            error_code="PLAN_SIGNATURE_INVALID",
        )
        if not isinstance(self.outputs, (list, tuple)):
            raise _error("PLAN_SIGNATURE_INVALID", "outputs must be an ordered sequence")
        output_keys = tuple(
            _require_stable_name(
                value,
                label="signature output",
                error_code="PLAN_SIGNATURE_INVALID",
            )
            for value in self.outputs
        )
        resolved = tuple(cast(Mapping[str, Any], _freeze_json(node)) for node in mutable_resolved)
        output_schemas = tuple(
            _require_digest(
                value,
                label="output_schema_digest",
                error_code="PLAN_SIGNATURE_INVALID",
            )
            for value in self.output_schema_digests
        )
        metrics = _resolved_metrics(mutable_resolved, region_input, output_keys)
        if self.node_count != len(mutable_resolved):
            raise _error("PLAN_SIGNATURE_INVALID", "signature node count is inconsistent")
        if self.max_depth != metrics.max_depth:
            raise _error("PLAN_SIGNATURE_INVALID", "signature maximum depth is inconsistent")
        if self.max_fanout != metrics.max_fanout:
            raise _error("PLAN_SIGNATURE_INVALID", "signature maximum fanout is inconsistent")
        if composition != metrics.module_composition:
            raise _error("PLAN_SIGNATURE_INVALID", "signature module composition is inconsistent")
        if output_schemas != metrics.output_schema_digests:
            raise _error("PLAN_SIGNATURE_INVALID", "signature output schemas are inconsistent")
        if self.topology_digest != metrics.topology_digest:
            raise _error("PLAN_SIGNATURE_INVALID", "signature topology digest is inconsistent")
        object.__setattr__(self, "module_composition", composition)
        object.__setattr__(self, "resolved_nodes", resolved)
        object.__setattr__(self, "region_input_schema_digest", region_input)
        object.__setattr__(self, "outputs", output_keys)
        object.__setattr__(self, "output_schema_digests", output_schemas)

    def to_dict(self) -> dict[str, Any]:
        """Return the canonical JSON-compatible resolved signature."""
        return cast(
            dict[str, Any],
            canonical_data(
                {
                    "max_depth": self.max_depth,
                    "max_fanout": self.max_fanout,
                    "module_composition": [
                        {"alias": alias, "count": count} for alias, count in self.module_composition
                    ],
                    "node_count": self.node_count,
                    "output_schema_digests": self.output_schema_digests,
                    "outputs": self.outputs,
                    "region_input_schema_digest": self.region_input_schema_digest,
                    "resolved_nodes": self.resolved_nodes,
                    "revision": self.revision,
                    "supersedes": self.supersedes,
                    "topology_digest": self.topology_digest,
                    "version": self.version,
                }
            ),
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        """Strictly import a previously resolved signature.

        Parameters
        ----------
        data
            Mapping with exactly the supported signature fields.

        Returns
        -------
        PlanSignature
            Canonically validated trusted signature.

        Raises
        ------
        PlanValidationError
            If fields, ordering, descriptors, or digests are invalid.
        """
        if not isinstance(data, Mapping):
            raise _error("PLAN_SIGNATURE_INVALID", "PlanSignature must be an object")
        _require_exact_fields(
            data,
            _SIGNATURE_FIELDS,
            label="PlanSignature",
            error_code="PLAN_SIGNATURE_INVALID",
        )
        if data["version"] != PLAN_SIGNATURE_VERSION:
            raise _error(
                "PLAN_SIGNATURE_VERSION_UNSUPPORTED",
                f"unsupported PlanSignature version {data['version']!r}",
            )
        raw_composition = data["module_composition"]
        raw_nodes = data["resolved_nodes"]
        raw_output_keys = data["outputs"]
        raw_outputs = data["output_schema_digests"]
        if not isinstance(raw_composition, list):
            raise _error("PLAN_SIGNATURE_INVALID", "module_composition must be an array")
        if not isinstance(raw_nodes, list):
            raise _error("PLAN_SIGNATURE_INVALID", "resolved_nodes must be an array")
        if not isinstance(raw_output_keys, list):
            raise _error("PLAN_SIGNATURE_INVALID", "outputs must be an array")
        if not isinstance(raw_outputs, list):
            raise _error("PLAN_SIGNATURE_INVALID", "output schemas must be an array")
        composition: list[tuple[str, int]] = []
        for item in raw_composition:
            if not isinstance(item, Mapping) or set(item) != {"alias", "count"}:
                raise _error(
                    "PLAN_SIGNATURE_INVALID",
                    "module composition fields do not match the contract",
                )
            composition.append((item["alias"], item["count"]))
        if composition != sorted(composition):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "module composition must be in canonical alias order",
            )
        node_keys: list[str] = []
        for node in raw_nodes:
            if not isinstance(node, Mapping) or not isinstance(node.get("key"), str):
                raise _error(
                    "PLAN_SIGNATURE_INVALID",
                    "resolved nodes must contain string keys",
                )
            node_keys.append(node["key"])
        if node_keys != sorted(node_keys):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "resolved nodes must be in canonical key order",
            )
        if len(node_keys) != len(set(node_keys)):
            raise _error("PLAN_SIGNATURE_INVALID", "resolved nodes contain duplicate keys")
        try:
            return cls(
                revision=data["revision"],
                supersedes=data["supersedes"],
                node_count=data["node_count"],
                max_depth=data["max_depth"],
                max_fanout=data["max_fanout"],
                module_composition=tuple(composition),
                topology_digest=data["topology_digest"],
                resolved_nodes=tuple(cast(Mapping[str, Any], node) for node in raw_nodes),
                region_input_schema_digest=data["region_input_schema_digest"],
                outputs=tuple(raw_output_keys),
                output_schema_digests=tuple(raw_outputs),
                version=data["version"],
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, PlanValidationError):
                raise
            raise _error("PLAN_SIGNATURE_INVALID", str(exc)) from exc

    def canonical_json(self) -> str:
        """Serialize the resolved signature as deterministic canonical JSON."""
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        """Return the SHA-256 digest of the complete resolved signature."""
        return digest_data(self.to_dict())


def _validate_resolved_node(node: Mapping[str, Any]) -> None:
    _require_exact_fields(
        node,
        _RESOLVED_NODE_FIELDS,
        label="resolved PlanNode",
        error_code="PLAN_SIGNATURE_INVALID",
    )
    _require_stable_name(
        node["key"],
        label="resolved node key",
        error_code="PLAN_SIGNATURE_INVALID",
    )
    _require_stable_name(
        node["module_alias"],
        label="resolved module alias",
        error_code="PLAN_SIGNATURE_INVALID",
    )
    dependencies = node["dependencies"]
    if not isinstance(dependencies, list):
        raise _error("PLAN_SIGNATURE_INVALID", "resolved dependencies must be an array")
    for dependency in dependencies:
        if dependency != _REGION_INPUT:
            _require_stable_name(
                dependency,
                label="resolved dependency",
                error_code="PLAN_SIGNATURE_INVALID",
            )
    try:
        _catalog_entry(
            module_id=node["module_id"],
            module_digest=node["module_digest"],
            input_schema_digests=node["input_schema_digests"],
            output_schema_digest=node["output_schema_digest"],
            execution=node["execution"],
            capabilities=node["capabilities"],
            effects=node["effects"],
        )
    except ValueError as exc:
        raise _error("PLAN_SIGNATURE_INVALID", str(exc)) from exc


@dataclass(frozen=True)
class _ResolvedMetrics:
    max_depth: int
    max_fanout: int
    module_composition: tuple[tuple[str, int], ...]
    topology_digest: str
    output_schema_digests: tuple[str, ...]


def _resolved_metrics(
    nodes: tuple[dict[str, Any], ...],
    region_input_schema_digest: str,
    outputs: tuple[str, ...],
) -> _ResolvedMetrics:
    if not nodes:
        raise _error("PLAN_SIGNATURE_INVALID", "resolved signature requires at least one node")
    keys = [cast(str, node["key"]) for node in nodes]
    if len(keys) != len(set(keys)):
        raise _error("PLAN_SIGNATURE_INVALID", "resolved nodes contain duplicate keys")
    by_key = {cast(str, node["key"]): node for node in nodes}
    if not outputs:
        raise _error("PLAN_SIGNATURE_INVALID", "resolved signature requires at least one output")
    if len(outputs) != len(set(outputs)):
        raise _error("PLAN_SIGNATURE_INVALID", "signature outputs contain duplicate keys")
    for output in outputs:
        if output not in by_key:
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                f"signature output {output!r} does not exist",
            )

    for node in nodes:
        node_key = cast(str, node["key"])
        dependencies = cast(list[str], node["dependencies"])
        input_schemas = cast(list[str], node["input_schema_digests"])
        if len(dependencies) != len(set(dependencies)):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                f"resolved node {node_key!r} contains a duplicate dependency",
            )
        if len(dependencies) != len(input_schemas):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                f"resolved node {node_key!r} input count is inconsistent",
            )
        for index, (dependency, expected_schema) in enumerate(
            zip(dependencies, input_schemas, strict=True)
        ):
            if dependency == _REGION_INPUT:
                actual_schema = region_input_schema_digest
            else:
                if dependency not in by_key:
                    raise _error(
                        "PLAN_SIGNATURE_INVALID",
                        f"resolved dependency {dependency!r} does not exist",
                    )
                actual_schema = cast(str, by_key[dependency]["output_schema_digest"])
            if actual_schema != expected_schema:
                raise _error(
                    "PLAN_SIGNATURE_INVALID",
                    f"resolved edge schema at {node_key!r} input {index} is inconsistent",
                )

    visiting: set[str] = set()
    depths: dict[str, int] = {}

    def visit(key: str) -> int:
        if key in depths:
            return depths[key]
        if key in visiting:
            raise _error("PLAN_SIGNATURE_INVALID", "resolved topology must be acyclic")
        visiting.add(key)
        dependencies = [
            dependency
            for dependency in cast(list[str], by_key[key]["dependencies"])
            if dependency != _REGION_INPUT
        ]
        depth = 1 + max((visit(dependency) for dependency in dependencies), default=0)
        visiting.remove(key)
        depths[key] = depth
        return depth

    for key in keys:
        visit(key)
    fanouts = Counter(
        dependency for node in nodes for dependency in cast(list[str], node["dependencies"])
    )
    composition = tuple(sorted(Counter(cast(str, node["module_alias"]) for node in nodes).items()))
    output_schemas = tuple(cast(str, by_key[output]["output_schema_digest"]) for output in outputs)
    return _ResolvedMetrics(
        max_depth=max(depths.values()),
        max_fanout=max(fanouts.values(), default=0),
        module_composition=composition,
        topology_digest=digest_data({"nodes": nodes, "outputs": outputs}),
        output_schema_digests=output_schemas,
    )


class PlanValidator:
    """Resolve and validate generated fragments inside a trusted policy boundary.

    Parameters
    ----------
    catalog
        Immutable allowlist that supplies all module pins and behavior metadata.
    limits
        Hard structural and revision limits.
    budget_check
        Required deployment-owned callback receiving the fully resolved
        :class:`PlanSignature`. It must return ``None`` or raise
        :class:`PlanValidationError` to reject the plan.

    Notes
    -----
    The package currently has no shared live-execution ``Budget`` contract.
    Consequently this validator does not invent a cost estimator or budget
    schema. Deployments must provide the explicit ``budget_check`` seam, which
    can consult their authoritative module budget registry using resolved pins.
    """

    def __init__(
        self,
        catalog: ModuleCatalog,
        limits: PlanLimits,
        *,
        budget_check: Callable[[PlanSignature], None],
    ) -> None:
        if not isinstance(catalog, ModuleCatalog):
            raise TypeError("catalog must be a ModuleCatalog")
        if not isinstance(limits, PlanLimits):
            raise TypeError("limits must be PlanLimits")
        if not callable(budget_check):
            raise TypeError("budget_check must be a callable trusted policy seam")
        self.catalog = catalog
        self.limits = limits
        self.budget_check = budget_check

    def validate(
        self,
        fragment: PlanFragmentIR,
        *,
        region_input_schema_digest: str,
        expected_output_schema_digests: tuple[str, ...],
        expected_revision: int,
        expected_supersedes: str | None,
    ) -> PlanSignature:
        """Validate generated topology and return its resolved signature.

        Parameters
        ----------
        fragment
            Strictly decoded generated graph choices.
        region_input_schema_digest
            Trusted schema supplied to the inserted dynamic region.
        expected_output_schema_digests
            Trusted ordered contracts the surrounding workflow expects back.
        expected_revision, expected_supersedes
            Trusted lineage state used to reject skipped, repeated, or
            fabricated revisions.

        Returns
        -------
        PlanSignature
            Resolved behavior-bearing graph ready for later persistence or
            materialization by a separate runtime component.

        Raises
        ------
        PlanValidationError
            If aliases, topology, schemas, lineage, limits, or budget policy fail.
        """
        if not isinstance(fragment, PlanFragmentIR):
            raise _error("PLAN_FRAGMENT_INVALID", "fragment must be PlanFragmentIR")
        region_input = _require_digest(
            region_input_schema_digest,
            label="region_input_schema_digest",
        )
        expected_outputs = tuple(
            _require_digest(value, label="expected_output_schema_digest")
            for value in expected_output_schema_digests
        )
        if type(expected_revision) is not int or expected_revision < 1:
            raise _error("PLAN_REVISION_INVALID", "expected revision must be at least one")
        if expected_supersedes is not None:
            _require_digest(expected_supersedes, label="expected supersedes")
        self._validate_lineage(fragment, expected_revision, expected_supersedes)
        signature = self._resolve_graph(fragment, region_input, expected_outputs)
        try:
            result = self.budget_check(signature)
        except PlanValidationError:
            raise
        except Exception as exc:
            raise _error(
                "PLAN_BUDGET_VALIDATION_FAILED",
                f"budget validation failed: {exc}",
            ) from exc
        if result is not None:
            raise _error(
                "PLAN_BUDGET_VALIDATION_FAILED",
                "budget validation failed: budget_check must return None",
            )
        return signature

    def _validate_lineage(
        self,
        fragment: PlanFragmentIR,
        expected_revision: int,
        expected_supersedes: str | None,
    ) -> None:
        if fragment.revision != expected_revision:
            raise _error(
                "PLAN_REVISION_INVALID",
                f"fragment revision {fragment.revision} does not match expected revision "
                f"{expected_revision}",
            )
        if fragment.supersedes != expected_supersedes:
            raise _error(
                "PLAN_REVISION_INVALID",
                "fragment supersedes digest does not match trusted lineage",
            )
        replan_count = fragment.revision - 1
        if replan_count > self.limits.max_replans:
            raise _error(
                "PLAN_LIMIT_EXCEEDED",
                f"replan count {replan_count} exceeds {self.limits.max_replans}",
            )

    def _resolve_graph(
        self,
        fragment: PlanFragmentIR,
        region_input_schema_digest: str,
        expected_output_schema_digests: tuple[str, ...],
    ) -> PlanSignature:
        if not fragment.nodes:
            raise _error("PLAN_TOPOLOGY_INVALID", "plan requires at least one node")
        if len(fragment.nodes) > self.limits.max_nodes:
            raise _error(
                "PLAN_LIMIT_EXCEEDED",
                f"plan node count {len(fragment.nodes)} exceeds {self.limits.max_nodes}",
            )
        keys = [node.key for node in fragment.nodes]
        if len(keys) != len(set(keys)):
            raise _error("PLAN_TOPOLOGY_INVALID", "fragment contains a duplicate node key")
        by_key = {node.key: node for node in fragment.nodes}
        if not fragment.outputs:
            raise _error("PLAN_TOPOLOGY_INVALID", "plan requires at least one output")
        if len(fragment.outputs) != len(set(fragment.outputs)):
            raise _error("PLAN_TOPOLOGY_INVALID", "fragment contains a duplicate output")
        for output in fragment.outputs:
            if output not in by_key:
                raise _error(
                    "PLAN_TOPOLOGY_INVALID",
                    f"fragment output {output!r} does not exist",
                )

        entries: dict[str, _CatalogEntry] = {}
        for node in fragment.nodes:
            entries[node.key] = self.catalog._entry(node.module_alias)
            if len(node.dependencies) != len(set(node.dependencies)):
                raise _error(
                    "PLAN_TOPOLOGY_INVALID",
                    f"node {node.key!r} contains a duplicate dependency",
                )
            if len(node.dependencies) != len(entries[node.key].input_schema_digests):
                raise _error(
                    "PLAN_SCHEMA_INVALID",
                    f"node {node.key!r} input count does not match its trusted module contract",
                )
            for dependency in node.dependencies:
                if dependency != _REGION_INPUT and dependency not in by_key:
                    raise _error(
                        "PLAN_TOPOLOGY_INVALID",
                        f"node {node.key!r} dependency {dependency!r} does not exist",
                    )

        depths = self._depths(by_key)
        max_depth = max(depths.values())
        if max_depth > self.limits.max_depth:
            raise _error(
                "PLAN_LIMIT_EXCEEDED",
                f"plan depth {max_depth} exceeds {self.limits.max_depth}",
            )
        fanouts = Counter(dependency for node in fragment.nodes for dependency in node.dependencies)
        max_fanout = max(fanouts.values(), default=0)
        if max_fanout > self.limits.max_fanout:
            raise _error(
                "PLAN_LIMIT_EXCEEDED",
                f"plan fanout {max_fanout} exceeds {self.limits.max_fanout}",
            )

        resolved: list[dict[str, Any]] = []
        for node in fragment.nodes:
            entry = entries[node.key]
            for index, (dependency, expected_schema) in enumerate(
                zip(node.dependencies, entry.input_schema_digests, strict=True)
            ):
                actual_schema = (
                    region_input_schema_digest
                    if dependency == _REGION_INPUT
                    else entries[dependency].output_schema_digest
                )
                if actual_schema != expected_schema:
                    source = (
                        "region input schema"
                        if dependency == _REGION_INPUT
                        else f"dependency {dependency!r} edge schema"
                    )
                    raise _error(
                        "PLAN_SCHEMA_INVALID",
                        f"node {node.key!r} {source} at input {index} is incompatible",
                    )
            resolved.append(
                {
                    **entry.to_dict(),
                    "dependencies": list(node.dependencies),
                    "key": node.key,
                    "module_alias": node.module_alias,
                }
            )

        if len(expected_output_schema_digests) != len(fragment.outputs):
            raise _error(
                "PLAN_SCHEMA_INVALID",
                "fragment output contract count does not match trusted region output count",
            )
        actual_outputs = tuple(entries[key].output_schema_digest for key in fragment.outputs)
        for index, (actual, expected) in enumerate(
            zip(actual_outputs, expected_output_schema_digests, strict=True)
        ):
            if actual != expected:
                raise _error(
                    "PLAN_SCHEMA_INVALID",
                    f"fragment output schema at index {index} is incompatible",
                )

        composition = tuple(sorted(Counter(node.module_alias for node in fragment.nodes).items()))
        topology_digest = digest_data(
            {
                "nodes": resolved,
                "outputs": fragment.outputs,
            }
        )
        return PlanSignature(
            revision=fragment.revision,
            supersedes=fragment.supersedes,
            node_count=len(fragment.nodes),
            max_depth=max_depth,
            max_fanout=max_fanout,
            module_composition=composition,
            topology_digest=topology_digest,
            resolved_nodes=tuple(resolved),
            region_input_schema_digest=region_input_schema_digest,
            outputs=fragment.outputs,
            output_schema_digests=actual_outputs,
        )

    @staticmethod
    def _depths(by_key: Mapping[str, PlanNode]) -> dict[str, int]:
        visiting: set[str] = set()
        memo: dict[str, int] = {}

        def visit(key: str) -> int:
            if key in memo:
                return memo[key]
            if key in visiting:
                raise _error("PLAN_TOPOLOGY_INVALID", "plan topology must be acyclic")
            visiting.add(key)
            dependencies = [
                dependency for dependency in by_key[key].dependencies if dependency != _REGION_INPUT
            ]
            depth = 1 + max((visit(dependency) for dependency in dependencies), default=0)
            visiting.remove(key)
            memo[key] = depth
            return depth

        for node_key in by_key:
            visit(node_key)
        return memo
