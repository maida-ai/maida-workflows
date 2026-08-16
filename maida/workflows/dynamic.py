"""Validate generated graph fragments against trusted module definitions.

This module keeps generated planning output deliberately smaller than the
static workflow IR. A :class:`PlanFragmentIR` contains graph choices only;
module identities, schemas, execution requirements, and external-access
declarations remain in a trusted :class:`ModuleRegistry` and are resolved by
:class:`PlanValidator` before any later materialization step may use them.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Self, cast

from ._canonical import canonical_data, canonical_json, digest_data, schema_digest
from .budget import Budget
from .models import CapabilityGrant
from .registry import ModuleRegistry, _catalog_entry, _CatalogEntry

if TYPE_CHECKING:
    from .ir import PlanIR

PLAN_FRAGMENT_VERSION = "0.2.0"
PLAN_SIGNATURE_VERSION = "0.3.0"
_REGION_INPUT = "$input"
_STABLE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
_HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FRAGMENT_FIELDS = {
    "fragment_id",
    "nodes",
    "outputs",
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
    resolved against a trusted :class:`ModuleRegistry`; generated content never
    supplies module digests, schemas, code locations, execution requirements,
    credentials, grants, capabilities, effects, or budgets.

    Parameters
    ----------
    key
        Stable identity for this node within its fragment.
    module_alias
        Allowlisted alias to resolve in the trusted module registry.
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
    nodes
        Generated module aliases and dependency topology.
    outputs
        Ordered node keys returned from the surrounding dynamic region.
    version
        Fragment schema version. Only ``0.2.0`` is currently accepted.

    Notes
    -----
    This type is intentionally separate from :class:`~maida.workflows.ir.PlanIR`.
    It cannot carry executable code or authorize runtime behavior.
    """

    fragment_id: str
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
        """Return the SHA-256 digest of this generated fragment."""
        return digest_data(self.to_dict())


@dataclass(frozen=True)
class PlanLimits:
    """Trusted structural and resource limits for a generated graph region.

    Parameters
    ----------
    max_nodes
        Maximum number of generated module nodes.
    max_depth
        Maximum dependency depth, counting the first module as depth one.
    max_fanout
        Maximum number of direct consumers of any node or region input.
    budget
        Maximum aggregate resource envelope for the generated region. Token,
        tool, and cost limits are summed across node occurrences. Wall time is
        measured along the DAG's longest dependency path.
    """

    max_nodes: int
    max_depth: int
    max_fanout: int
    budget: Budget

    def __post_init__(self) -> None:
        """Reject nonsensical structural limits or a missing budget."""
        for label, value in (
            ("max_nodes", self.max_nodes),
            ("max_depth", self.max_depth),
            ("max_fanout", self.max_fanout),
        ):
            _require_nonnegative_integer(value, label=label)
        if self.max_nodes == 0:
            raise ValueError("max_nodes must be positive")
        if self.max_depth == 0:
            raise ValueError("max_depth must be positive")
        if not isinstance(self.budget, Budget):
            raise TypeError("budget must be a Budget")


@dataclass(frozen=True)
class PlanBoundary:
    """Trusted declaration that a module result defines a generated root plan.

    The declaration lives on an application-owned planner module. It supplies
    the registry, structural limits, output contract, and maximum child grant
    that generated bytes are forbidden to carry for themselves.

    Parameters
    ----------
    registry
        Trusted factories used for both plan validation and exact execution.
    limits
        Structural and aggregate resource limits enforced before insertion.
    region_id
        Stable identity used in resolved plan signatures and replay addresses.
    output_type
        Python contract for the generated root plan's single terminal value.
    region_grant
        Maximum external-access grant available to generated child modules.
    """

    registry: ModuleRegistry
    limits: PlanLimits
    region_id: str
    output_type: Any
    region_grant: CapabilityGrant = field(default_factory=CapabilityGrant)

    def __post_init__(self) -> None:
        """Validate the trusted marker without inspecting generated data."""
        if not isinstance(self.registry, ModuleRegistry):
            raise TypeError("registry must be a ModuleRegistry")
        if not isinstance(self.limits, PlanLimits):
            raise TypeError("limits must be PlanLimits")
        try:
            region_id = _require_stable_name(self.region_id, label="region_id")
        except PlanValidationError as exc:
            raise ValueError(str(exc)) from exc
        if not isinstance(self.region_grant, CapabilityGrant):
            raise TypeError("region_grant must be a CapabilityGrant")
        schema_digest(self.output_type)
        object.__setattr__(self, "region_id", region_id)

    def to_data(self) -> dict[str, Any]:
        """Return credential-free policy identity for the planner contract."""
        return cast(
            dict[str, Any],
            canonical_data(
                {
                    "limits": {
                        "budget": self.limits.budget.to_data(),
                        "max_depth": self.limits.max_depth,
                        "max_fanout": self.limits.max_fanout,
                        "max_nodes": self.limits.max_nodes,
                    },
                    "output_schema_digest": schema_digest(self.output_type),
                    "region_grant": self.region_grant.to_data(),
                    "region_id": self.region_id,
                    "registry_digest": self.registry.digest,
                }
            ),
        )


_CATALOG_FIELDS = {
    "budget",
    "capabilities",
    "effects",
    "execution",
    "input_schema_digests",
    "module_digest",
    "module_id",
    "output_schema_digest",
}


_SIGNATURE_FIELDS = {
    "aggregate_budget",
    "alias_provenance",
    "approval_requirements",
    "catalog_digest",
    "max_depth",
    "max_fanout",
    "module_composition",
    "node_count",
    "output_schema_digests",
    "outputs",
    "region_grant",
    "region_id",
    "region_input_schema_digest",
    "required_grant",
    "resolved_nodes",
    "source_fragment_digest",
    "topology_digest",
    "version",
}
_RESOLVED_NODE_FIELDS = _CATALOG_FIELDS | {
    "capability_grant",
    "dependencies",
    "key",
}


def _canonical_grant(data: Any, *, label: str) -> CapabilityGrant:
    if not isinstance(data, Mapping):
        raise _error("PLAN_SIGNATURE_INVALID", f"{label} must be an object")
    try:
        grant = CapabilityGrant.from_data(data)
    except (TypeError, ValueError) as exc:
        raise _error("PLAN_SIGNATURE_INVALID", f"{label} is invalid: {exc}") from exc
    if canonical_json(grant.to_data()) != canonical_json(data):
        raise _error("PLAN_SIGNATURE_INVALID", f"{label} is not canonical")
    return grant


def _entry_grant(entry: _CatalogEntry) -> CapabilityGrant:
    return CapabilityGrant(
        capabilities=tuple(cast(str, item["name"]) for item in entry.capabilities),
        effects=tuple(cast(str, item["name"]) for item in entry.effects),
    )


def _topological_depths(
    dependencies_by_key: Mapping[str, tuple[str, ...]],
    *,
    error_code: str,
    cycle_message: str,
) -> tuple[dict[str, int], tuple[str, ...]]:
    """Return iterative DAG depths and order without recursion limits."""
    indegrees: dict[str, int] = {}
    consumers: dict[str, list[str]] = {key: [] for key in dependencies_by_key}
    for key, dependencies in dependencies_by_key.items():
        graph_dependencies = tuple(
            dependency for dependency in dependencies if dependency != _REGION_INPUT
        )
        indegrees[key] = len(graph_dependencies)
        for dependency in graph_dependencies:
            consumers[dependency].append(key)
    ready = sorted((key for key, count in indegrees.items() if count == 0), reverse=True)
    depths: dict[str, int] = {}
    order: list[str] = []
    while ready:
        key = ready.pop()
        order.append(key)
        dependencies = tuple(
            dependency for dependency in dependencies_by_key[key] if dependency != _REGION_INPUT
        )
        depths[key] = 1 + max((depths[dependency] for dependency in dependencies), default=0)
        for consumer in sorted(consumers[key], reverse=True):
            indegrees[consumer] -= 1
            if indegrees[consumer] == 0:
                ready.append(consumer)
    if len(order) != len(dependencies_by_key):
        raise _error(error_code, cycle_message)
    return depths, tuple(order)


@dataclass(frozen=True)
class PlanSignature:
    """Resolved behavior and provenance for one validated generated DAG.

    A signature contains only behavior selected from a trusted
    :class:`ModuleRegistry`: immutable module pins, typed ports, execution
    requirements, exact child grants, resource envelopes, and approval-policy
    requirements. Parsing a signature checks its shape and internal
    consistency but does **not** authorize execution. Before any future
    materializer uses an imported signature, call
    :meth:`PlanValidator.revalidate` with the source fragment and current
    trusted context.

    Planner aliases are retained in :attr:`alias_provenance` for diagnostics,
    but are excluded from equality and :attr:`digest`. Renaming an alias that
    resolves to the same module pin therefore does not look like behavior
    drift.

    Parameters
    ----------
    region_id
        Stable identity of the trusted dynamic region.
    region_grant, required_grant
        Region-wide maximum grant and the exact union required by resolved
        nodes. Every node also stores its own least-privilege grant.
    aggregate_budget
        Mechanically derived occurrence budget. Count and cost dimensions are
        sums; wall time is the longest DAG dependency path.
    node_count, max_depth, max_fanout
        Structural measurements recomputed from :attr:`resolved_nodes`.
    module_composition
        Canonical ``(module_id, module_digest, occurrence_count)`` tuples.
        Aliases are deliberately absent.
    topology_digest
        Digest of alias-free resolved nodes and ordered output keys.
    resolved_nodes
        Canonical behavior-bearing node descriptors. Each descriptor includes
        the trusted module pin, schemas, execution environment, external-access
        declarations, budget, dependencies, and exact child grant.
    region_input_schema_digest
        Trusted schema supplied to every ``$input`` dependency.
    outputs, output_schema_digests
        Ordered output node keys and their verified surrounding-region schemas.
    approval_requirements
        Canonical ``(node_key, effect_name)`` policy-eligibility checks. These
        are not runtime effect approvals.
    source_fragment_digest, catalog_digest, alias_provenance
        Diagnostic provenance excluded from behavioral equality and digest.
    version
        Resolved signature wire-contract version.
    """

    region_id: str
    region_grant: CapabilityGrant
    required_grant: CapabilityGrant
    aggregate_budget: Budget
    node_count: int
    max_depth: int
    max_fanout: int
    module_composition: tuple[tuple[str, str, int], ...]
    topology_digest: str
    resolved_nodes: tuple[Mapping[str, Any], ...]
    region_input_schema_digest: str
    outputs: tuple[str, ...]
    output_schema_digests: tuple[str, ...]
    approval_requirements: tuple[tuple[str, str], ...]
    source_fragment_digest: str = field(compare=False)
    catalog_digest: str = field(compare=False)
    alias_provenance: tuple[tuple[str, str], ...] = field(compare=False)
    version: str = PLAN_SIGNATURE_VERSION

    def __post_init__(self) -> None:
        """Canonicalize records and verify every internally derived field."""
        if self.version != PLAN_SIGNATURE_VERSION:
            raise _error(
                "PLAN_SIGNATURE_VERSION_UNSUPPORTED",
                f"unsupported PlanSignature version {self.version!r}",
            )
        region_id = _require_stable_name(
            self.region_id, label="region_id", error_code="PLAN_SIGNATURE_INVALID"
        )
        if not isinstance(self.region_grant, CapabilityGrant):
            raise _error("PLAN_SIGNATURE_INVALID", "region_grant must be CapabilityGrant")
        if not isinstance(self.required_grant, CapabilityGrant):
            raise _error("PLAN_SIGNATURE_INVALID", "required_grant must be CapabilityGrant")
        if not isinstance(self.aggregate_budget, Budget):
            raise _error("PLAN_SIGNATURE_INVALID", "aggregate_budget must be Budget")
        for label, value in (
            ("node_count", self.node_count),
            ("max_depth", self.max_depth),
            ("max_fanout", self.max_fanout),
        ):
            _require_nonnegative_integer(value, label=label, error_code="PLAN_SIGNATURE_INVALID")
        topology_digest = _require_digest(
            self.topology_digest,
            label="topology_digest",
            error_code="PLAN_SIGNATURE_INVALID",
        )
        source_digest = _require_digest(
            self.source_fragment_digest,
            label="source_fragment_digest",
            error_code="PLAN_SIGNATURE_INVALID",
        )
        catalog_digest = _require_digest(
            self.catalog_digest,
            label="catalog_digest",
            error_code="PLAN_SIGNATURE_INVALID",
        )
        if not isinstance(self.module_composition, (list, tuple)) or any(
            not isinstance(item, (list, tuple)) or len(item) != 3
            for item in self.module_composition
        ):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "module composition must contain three-field tuples",
            )
        composition = tuple(
            cast(tuple[str, str, int], tuple(item)) for item in self.module_composition
        )
        for module_id, module_digest, count in composition:
            if not isinstance(module_id, str) or not module_id.strip():
                raise _error("PLAN_SIGNATURE_INVALID", "composition module_id must be non-empty")
            _require_digest(
                module_digest,
                label="composition module_digest",
                error_code="PLAN_SIGNATURE_INVALID",
            )
            if type(count) is not int or count < 1:
                raise _error(
                    "PLAN_SIGNATURE_INVALID",
                    "module composition counts must be positive integers",
                )
        composition = tuple(sorted(composition))
        if len(composition) != len({item[:2] for item in composition}):
            raise _error("PLAN_SIGNATURE_INVALID", "module composition contains duplicate pins")
        if not isinstance(self.resolved_nodes, (list, tuple)) or any(
            not isinstance(node, Mapping) for node in self.resolved_nodes
        ):
            raise _error("PLAN_SIGNATURE_INVALID", "resolved_nodes must contain objects")
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
                value, label="signature output", error_code="PLAN_SIGNATURE_INVALID"
            )
            for value in self.outputs
        )
        if not isinstance(self.output_schema_digests, (list, tuple)):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "output_schema_digests must be an ordered sequence",
            )
        output_schemas = tuple(
            _require_digest(
                value,
                label="output_schema_digest",
                error_code="PLAN_SIGNATURE_INVALID",
            )
            for value in self.output_schema_digests
        )
        if not isinstance(self.approval_requirements, (list, tuple)) or any(
            not isinstance(item, (list, tuple)) or len(item) != 2
            for item in self.approval_requirements
        ):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "approval requirements must contain two-field tuples",
            )
        approvals = tuple(cast(tuple[str, str], tuple(item)) for item in self.approval_requirements)
        for node_key, effect_name in approvals:
            _require_stable_name(
                node_key, label="approval node key", error_code="PLAN_SIGNATURE_INVALID"
            )
            _require_stable_name(
                effect_name, label="approval effect name", error_code="PLAN_SIGNATURE_INVALID"
            )
        approvals = tuple(sorted(approvals))
        if len(approvals) != len(set(approvals)):
            raise _error("PLAN_SIGNATURE_INVALID", "approval requirements contain duplicates")
        if not isinstance(self.alias_provenance, (list, tuple)) or any(
            not isinstance(item, (list, tuple)) or len(item) != 2 for item in self.alias_provenance
        ):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "alias provenance must contain two-field tuples",
            )
        provenance = tuple(cast(tuple[str, str], tuple(item)) for item in self.alias_provenance)
        for node_key, alias in provenance:
            _require_stable_name(
                node_key, label="alias node key", error_code="PLAN_SIGNATURE_INVALID"
            )
            _require_stable_name(alias, label="module alias", error_code="PLAN_SIGNATURE_INVALID")
        provenance = tuple(sorted(provenance))
        if len(provenance) != len({key for key, _alias in provenance}):
            raise _error("PLAN_SIGNATURE_INVALID", "alias provenance contains duplicate node keys")
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
        if topology_digest != metrics.topology_digest:
            raise _error("PLAN_SIGNATURE_INVALID", "signature topology digest is inconsistent")
        if self.aggregate_budget != metrics.aggregate_budget:
            raise _error("PLAN_SIGNATURE_INVALID", "signature aggregate budget is inconsistent")
        if self.required_grant != metrics.required_grant:
            raise _error("PLAN_SIGNATURE_INVALID", "signature required grant is inconsistent")
        if approvals != metrics.approval_requirements:
            raise _error(
                "PLAN_SIGNATURE_INVALID", "signature approval requirements are inconsistent"
            )
        if {key for key, _alias in provenance} != {node["key"] for node in mutable_resolved}:
            raise _error("PLAN_SIGNATURE_INVALID", "alias provenance does not cover resolved nodes")
        try:
            self.region_grant.narrow(
                capabilities=self.required_grant.capabilities,
                effects=self.required_grant.effects,
            )
        except ValueError as exc:
            raise _error("PLAN_SIGNATURE_INVALID", "required grant exceeds region grant") from exc
        resolved = tuple(cast(Mapping[str, Any], _freeze_json(node)) for node in mutable_resolved)
        object.__setattr__(self, "region_id", region_id)
        object.__setattr__(self, "module_composition", composition)
        object.__setattr__(self, "resolved_nodes", resolved)
        object.__setattr__(self, "region_input_schema_digest", region_input)
        object.__setattr__(self, "outputs", output_keys)
        object.__setattr__(self, "output_schema_digests", output_schemas)
        object.__setattr__(self, "approval_requirements", approvals)
        object.__setattr__(self, "source_fragment_digest", source_digest)
        object.__setattr__(self, "catalog_digest", catalog_digest)
        object.__setattr__(self, "alias_provenance", provenance)

    def _behavior_data(self) -> dict[str, Any]:
        return {
            "aggregate_budget": self.aggregate_budget.to_data(),
            "approval_requirements": [
                {"effect_name": effect_name, "node_key": node_key}
                for node_key, effect_name in self.approval_requirements
            ],
            "max_depth": self.max_depth,
            "max_fanout": self.max_fanout,
            "module_composition": [
                {"count": count, "module_digest": digest, "module_id": module_id}
                for module_id, digest, count in self.module_composition
            ],
            "node_count": self.node_count,
            "output_schema_digests": self.output_schema_digests,
            "outputs": self.outputs,
            "region_grant": self.region_grant.to_data(),
            "region_id": self.region_id,
            "region_input_schema_digest": self.region_input_schema_digest,
            "required_grant": self.required_grant.to_data(),
            "resolved_nodes": self.resolved_nodes,
            "topology_digest": self.topology_digest,
            "version": self.version,
        }

    def to_dict(self) -> dict[str, Any]:
        """Return canonical behavior plus diagnostic source provenance.

        Returns
        -------
        dict
            JSON-compatible signature data. This serialized form remains
            untrusted after transport; parse it with :meth:`from_dict` and
            authenticate it with :meth:`PlanValidator.revalidate`.
        """
        return cast(
            dict[str, Any],
            canonical_data(
                {
                    **self._behavior_data(),
                    "alias_provenance": [
                        {"alias": alias, "node_key": node_key}
                        for node_key, alias in self.alias_provenance
                    ],
                    "catalog_digest": self.catalog_digest,
                    "source_fragment_digest": self.source_fragment_digest,
                }
            ),
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        """Parse a signature without treating imported pins as trusted.

        This method strictly checks canonical encoding and internal graph
        consistency. It cannot prove that module pins, grants, or budgets still
        match the current trusted catalog. Call :meth:`PlanValidator.revalidate`
        before using an imported value for any execution decision.

        Parameters
        ----------
        data
            Mapping containing exactly the supported signature fields and
            canonical nested declarations.

        Returns
        -------
        PlanSignature
            Deeply immutable, internally consistent parsed signature. The
            return value is not authorization to execute.

        Raises
        ------
        PlanValidationError
            If the version, fields, canonical ordering, graph, schemas, grants,
            budgets, or internally derived digests are invalid.
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
        arrays = {
            "alias_provenance": data["alias_provenance"],
            "approval_requirements": data["approval_requirements"],
            "module_composition": data["module_composition"],
            "output_schema_digests": data["output_schema_digests"],
            "outputs": data["outputs"],
            "resolved_nodes": data["resolved_nodes"],
        }
        for label, value in arrays.items():
            if not isinstance(value, list):
                raise _error("PLAN_SIGNATURE_INVALID", f"{label} must be an array")
        composition: list[tuple[str, str, int]] = []
        for item in cast(list[Any], arrays["module_composition"]):
            if not isinstance(item, Mapping) or set(item) != {
                "count",
                "module_digest",
                "module_id",
            }:
                raise _error(
                    "PLAN_SIGNATURE_INVALID",
                    "module composition fields do not match the contract",
                )
            if (
                not isinstance(item["module_id"], str)
                or not isinstance(item["module_digest"], str)
                or type(item["count"]) is not int
            ):
                raise _error(
                    "PLAN_SIGNATURE_INVALID",
                    "module composition values have invalid types",
                )
            composition.append((item["module_id"], item["module_digest"], item["count"]))
        approvals: list[tuple[str, str]] = []
        for item in cast(list[Any], arrays["approval_requirements"]):
            if not isinstance(item, Mapping) or set(item) != {"effect_name", "node_key"}:
                raise _error(
                    "PLAN_SIGNATURE_INVALID",
                    "approval requirement fields do not match the contract",
                )
            if not isinstance(item["node_key"], str) or not isinstance(item["effect_name"], str):
                raise _error(
                    "PLAN_SIGNATURE_INVALID",
                    "approval requirement values must be strings",
                )
            approvals.append((item["node_key"], item["effect_name"]))
        provenance: list[tuple[str, str]] = []
        for item in cast(list[Any], arrays["alias_provenance"]):
            if not isinstance(item, Mapping) or set(item) != {"alias", "node_key"}:
                raise _error(
                    "PLAN_SIGNATURE_INVALID",
                    "alias provenance fields do not match the contract",
                )
            if not isinstance(item["node_key"], str) or not isinstance(item["alias"], str):
                raise _error(
                    "PLAN_SIGNATURE_INVALID",
                    "alias provenance values must be strings",
                )
            provenance.append((item["node_key"], item["alias"]))
        raw_nodes = cast(list[Any], arrays["resolved_nodes"])
        node_keys: list[str] = []
        for node in raw_nodes:
            if not isinstance(node, Mapping) or not isinstance(node.get("key"), str):
                raise _error("PLAN_SIGNATURE_INVALID", "resolved nodes must contain string keys")
            node_keys.append(node["key"])
        for label, value in (
            ("module composition", composition),
            ("approval requirements", approvals),
            ("alias provenance", provenance),
            ("resolved nodes", node_keys),
        ):
            if value != sorted(value):
                raise _error("PLAN_SIGNATURE_INVALID", f"{label} must be in canonical order")
        try:
            region_grant = _canonical_grant(data["region_grant"], label="region_grant")
            required_grant = _canonical_grant(data["required_grant"], label="required_grant")
            aggregate_budget = Budget.from_data(data["aggregate_budget"])
            if canonical_json(aggregate_budget.to_data()) != canonical_json(
                data["aggregate_budget"]
            ):
                raise ValueError("aggregate_budget is not canonical")
            return cls(
                region_id=data["region_id"],
                region_grant=region_grant,
                required_grant=required_grant,
                aggregate_budget=aggregate_budget,
                node_count=data["node_count"],
                max_depth=data["max_depth"],
                max_fanout=data["max_fanout"],
                module_composition=tuple(composition),
                topology_digest=data["topology_digest"],
                resolved_nodes=tuple(cast(Mapping[str, Any], node) for node in raw_nodes),
                region_input_schema_digest=data["region_input_schema_digest"],
                outputs=tuple(cast(list[Any], arrays["outputs"])),
                output_schema_digests=tuple(cast(list[Any], arrays["output_schema_digests"])),
                approval_requirements=tuple(approvals),
                source_fragment_digest=data["source_fragment_digest"],
                catalog_digest=data["catalog_digest"],
                alias_provenance=tuple(provenance),
                version=data["version"],
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, PlanValidationError):
                raise
            raise _error("PLAN_SIGNATURE_INVALID", str(exc)) from exc

    def canonical_json(self) -> str:
        """Serialize behavior and diagnostic provenance as canonical JSON.

        Returns
        -------
        str
            Deterministic JSON suitable for persistence and byte comparison.
        """
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        """Return an alias-invariant digest of resolved graph behavior.

        Diagnostic fragment labels, catalog aliases, and their provenance are
        excluded. Module identities, content digests, topology, schemas,
        execution requirements, grants, budgets, and region identity remain.
        """
        return digest_data(self._behavior_data())


def _plan_from_signature(signature: PlanSignature) -> PlanIR:
    """Project one resolved generated signature into the shared graph model."""
    from .ir import BindingIR, PlanIR, StepIR

    steps: list[StepIR] = []
    by_key = {cast(str, descriptor["key"]): descriptor for descriptor in signature.resolved_nodes}
    _depths, order = _topological_depths(
        {
            key: cast(tuple[str, ...], descriptor["dependencies"])
            for key, descriptor in by_key.items()
        },
        error_code="PLAN_SIGNATURE_INVALID",
        cycle_message="resolved plan topology must be acyclic",
    )
    for node_key in order:
        descriptor = by_key[node_key]
        dependencies = tuple(
            "input" if dependency == _REGION_INPUT else f"nodes/{dependency}"
            for dependency in cast(tuple[str, ...], descriptor["dependencies"])
        )
        input_schemas = tuple(cast(tuple[str, ...], descriptor["input_schema_digests"]))
        input_schema = (
            input_schemas[0]
            if len(input_schemas) == 1
            else digest_data({"ordered_input_schemas": input_schemas})
        )
        steps.append(
            StepIR(
                node_id=f"nodes/{node_key}",
                kind="module",
                dependencies=dependencies,
                output_schema_digest=cast(str, descriptor["output_schema_digest"]),
                module_id=cast(str, descriptor["module_id"]),
                logical_step=f"dynamic/{signature.region_id}/nodes/{node_key}",
                module_digest=cast(str, descriptor["module_digest"]),
                definition_digest=digest_data(
                    {
                        "logical_step": f"dynamic/{signature.region_id}/nodes/{node_key}",
                        "module_digest": descriptor["module_digest"],
                        "module_id": descriptor["module_id"],
                    }
                ),
                input_binding=BindingIR(
                    schema_digest=input_schema,
                    kind="source",
                    source=dependencies[0] if dependencies else "input",
                ),
                execution=cast(Mapping[str, Any], canonical_data(descriptor["execution"])),
                capabilities=tuple(
                    cast(
                        list[Mapping[str, Any]],
                        canonical_data(descriptor["capabilities"]),
                    )
                ),
                effects=tuple(
                    cast(
                        list[Mapping[str, Any]],
                        canonical_data(descriptor["effects"]),
                    )
                ),
                budget=cast(Mapping[str, int | float | None], canonical_data(descriptor["budget"])),
            )
        )
    steps.append(
        StepIR(
            node_id="output",
            kind="parallel",
            dependencies=tuple(f"nodes/{key}" for key in signature.outputs),
            output_schema_digest=digest_data(
                {"ordered_output_schemas": signature.output_schema_digests}
            ),
            control={"outputs": signature.outputs, "region": "generated_output"},
        )
    )
    return PlanIR(
        version="0.4.0",
        workflow_id=f"dynamic:{signature.region_id}",
        input_schema={"digest": signature.region_input_schema_digest},
        output_schema={"digests": list(signature.output_schema_digests)},
        steps=tuple(steps),
        output_node="output",
    )


def _validate_resolved_node(node: Mapping[str, Any]) -> None:
    _require_exact_fields(
        node,
        _RESOLVED_NODE_FIELDS,
        label="resolved PlanNode",
        error_code="PLAN_SIGNATURE_INVALID",
    )
    _require_stable_name(
        node["key"], label="resolved node key", error_code="PLAN_SIGNATURE_INVALID"
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
        entry = _catalog_entry(
            module_id=node["module_id"],
            module_digest=node["module_digest"],
            input_schema_digests=node["input_schema_digests"],
            output_schema_digest=node["output_schema_digest"],
            execution=node["execution"],
            capabilities=node["capabilities"],
            effects=node["effects"],
            budget=node["budget"],
            require_canonical=True,
        )
        grant = _canonical_grant(node["capability_grant"], label="node capability_grant")
    except ValueError as exc:
        if isinstance(exc, PlanValidationError):
            raise
        raise _error("PLAN_SIGNATURE_INVALID", str(exc)) from exc
    if grant != _entry_grant(entry):
        raise _error("PLAN_SIGNATURE_INVALID", "node capability grant is inconsistent")


@dataclass(frozen=True)
class _ResolvedMetrics:
    max_depth: int
    max_fanout: int
    module_composition: tuple[tuple[str, str, int], ...]
    topology_digest: str
    output_schema_digests: tuple[str, ...]
    aggregate_budget: Budget
    required_grant: CapabilityGrant
    approval_requirements: tuple[tuple[str, str], ...]


def _aggregate_budget(
    nodes: tuple[dict[str, Any], ...],
    by_key: Mapping[str, dict[str, Any]],
    outputs: tuple[str, ...],
    topological_order: tuple[str, ...],
) -> Budget:
    budgets = [Budget.from_data(cast(Mapping[str, Any], node["budget"])) for node in nodes]

    def count_sum(attribute: str) -> int | None:
        values = [cast(int | None, getattr(budget, attribute)) for budget in budgets]
        return None if any(value is None for value in values) else sum(cast(list[int], values))

    costs = [budget.cost_usd for budget in budgets]
    cost = (
        None
        if any(value is None for value in costs)
        else float(sum((Decimal(str(value)) for value in costs), start=Decimal(0)))
    )
    path_ms: dict[str, int | None] = {}
    for key in topological_order:
        budget_data = cast(Mapping[str, Any], by_key[key]["budget"])
        own = cast(int | None, budget_data["wall_time_ms"])
        dependencies = [
            dependency
            for dependency in cast(list[str], by_key[key]["dependencies"])
            if dependency != _REGION_INPUT
        ]
        dependency_paths = [path_ms[dependency] for dependency in dependencies]
        if own is None or any(value is None for value in dependency_paths):
            path_ms[key] = None
        else:
            path_ms[key] = own + max(cast(list[int], dependency_paths), default=0)

    output_paths = [path_ms[output] for output in outputs]
    wall_time_ms = (
        None
        if any(value is None for value in output_paths)
        else max(cast(list[int], output_paths), default=0)
    )
    return Budget(
        wall_time=timedelta(milliseconds=wall_time_ms) if wall_time_ms is not None else None,
        model_tokens=count_sum("model_tokens"),
        tool_calls=count_sum("tool_calls"),
        cost_usd=cost,
    )


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
            raise _error("PLAN_SIGNATURE_INVALID", f"signature output {output!r} does not exist")
    reachable: set[str] = set()
    pending = list(outputs)
    while pending:
        key = pending.pop()
        if key in reachable:
            continue
        reachable.add(key)
        for dependency in cast(list[str], by_key[key]["dependencies"]):
            if dependency == _REGION_INPUT:
                continue
            if dependency not in by_key:
                raise _error(
                    "PLAN_SIGNATURE_INVALID",
                    f"resolved dependency {dependency!r} does not exist",
                )
            pending.append(dependency)
    if reachable != set(keys):
        raise _error(
            "PLAN_SIGNATURE_INVALID", "resolved graph contains a node unreachable from outputs"
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
                "PLAN_SIGNATURE_INVALID", f"resolved node {node_key!r} input count is inconsistent"
            )
        for index, (dependency, expected_schema) in enumerate(
            zip(dependencies, input_schemas, strict=True)
        ):
            actual_schema = (
                region_input_schema_digest
                if dependency == _REGION_INPUT
                else cast(str, by_key[dependency]["output_schema_digest"])
            )
            if actual_schema != expected_schema:
                raise _error(
                    "PLAN_SIGNATURE_INVALID",
                    f"resolved edge schema at {node_key!r} input {index} is inconsistent",
                )
    dependencies_by_key = {key: tuple(cast(list[str], by_key[key]["dependencies"])) for key in keys}
    depths, topological_order = _topological_depths(
        dependencies_by_key,
        error_code="PLAN_SIGNATURE_INVALID",
        cycle_message="resolved topology must be acyclic",
    )
    fanouts = Counter(
        dependency for node in nodes for dependency in cast(list[str], node["dependencies"])
    )
    pin_counts = Counter(
        (cast(str, node["module_id"]), cast(str, node["module_digest"])) for node in nodes
    )
    composition = tuple(
        (module_id, module_digest, count)
        for (module_id, module_digest), count in sorted(pin_counts.items())
    )
    capability_names = {
        cast(str, item["name"])
        for node in nodes
        for item in cast(list[dict[str, Any]], node["capabilities"])
    }
    effect_names = {
        cast(str, item["name"])
        for node in nodes
        for item in cast(list[dict[str, Any]], node["effects"])
    }
    approvals = tuple(
        sorted(
            (cast(str, node["key"]), cast(str, effect["name"]))
            for node in nodes
            for effect in cast(list[dict[str, Any]], node["effects"])
            if effect["approval_required"] is True
        )
    )
    output_schemas = tuple(cast(str, by_key[output]["output_schema_digest"]) for output in outputs)
    try:
        aggregate_budget = _aggregate_budget(nodes, by_key, outputs, topological_order)
    except (OverflowError, ValueError):
        raise _error(
            "PLAN_BUDGET_INVALID",
            "aggregate budget exceeds the supported numeric range",
        ) from None
    return _ResolvedMetrics(
        max_depth=max(depths.values()),
        max_fanout=max(fanouts.values(), default=0),
        module_composition=composition,
        topology_digest=digest_data({"nodes": nodes, "outputs": outputs}),
        output_schema_digests=output_schemas,
        aggregate_budget=aggregate_budget,
        required_grant=CapabilityGrant(
            capabilities=tuple(capability_names), effects=tuple(effect_names)
        ),
        approval_requirements=approvals,
    )


class PlanValidator:
    """Bind generated fragments to a trusted region policy boundary.

    Parameters
    ----------
    registry
        Immutable allowlist that supplies all module pins and behavior metadata.
    limits
        Hard structural and aggregate resource limits.
    region_id
        Stable identity of the surrounding dynamic region. It becomes part of
        the behavioral signature and prevents reuse in another region.
    region_grant
        Exact maximum external-access grant available to child nodes. The
        validator derives least-privilege child grants and rejects widening.
    approval_check
        Optional trusted policy callback invoked as
        ``approval_check(region_id, node_key, effect_name)`` for every
        approval-required effect declaration. It must return ``None``. This is
        policy eligibility only; runtime effect requests still need durable,
        request-scoped approval evidence.

    Notes
    -----
    ``validate`` is the only operation that creates a signature from generated
    graph choices. Imported signatures remain untrusted until
    :meth:`revalidate` rebuilds them from the source fragment, current registry,
    region policy, schemas, and limits.
    """

    def __init__(
        self,
        registry: ModuleRegistry,
        limits: PlanLimits,
        *,
        region_id: str,
        region_grant: CapabilityGrant,
        approval_check: Callable[[str, str, str], None] | None = None,
    ) -> None:
        if not isinstance(registry, ModuleRegistry):
            raise TypeError("registry must be a ModuleRegistry")
        if not isinstance(limits, PlanLimits):
            raise TypeError("limits must be PlanLimits")
        try:
            trusted_region_id = _require_stable_name(region_id, label="region_id")
        except PlanValidationError as exc:
            raise ValueError(str(exc)) from exc
        if not isinstance(region_grant, CapabilityGrant):
            raise TypeError("region_grant must be a CapabilityGrant")
        if approval_check is not None and not callable(approval_check):
            raise TypeError("approval_check must be callable or None")
        self.registry = registry
        self.limits = limits
        self.region_id = trusted_region_id
        self.region_grant = region_grant
        self.approval_check = approval_check

    def validate(
        self,
        fragment: PlanFragmentIR,
        *,
        region_input_schema_digest: str,
        expected_output_schema_digests: tuple[str, ...],
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

        Returns
        -------
        PlanSignature
            Resolved behavior-bearing graph ready for later persistence or
            materialization by a separate runtime component.

        Raises
        ------
        PlanValidationError
            If aliases, topology, schemas, limits, grants, budgets, or
            approval policy fail.
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
        signature = self._resolve_graph(fragment, region_input, expected_outputs)
        self._validate_budget(signature.aggregate_budget)
        self._validate_approvals(signature.approval_requirements)
        return signature

    def revalidate(
        self,
        signature: PlanSignature,
        fragment: PlanFragmentIR,
        *,
        region_input_schema_digest: str,
        expected_output_schema_digests: tuple[str, ...],
    ) -> PlanSignature:
        """Rebuild and authenticate an imported signature against trusted state.

        The method never trusts imported resolved nodes. It resolves the source
        fragment again through this validator's current registry, grant, limits,
        schemas and approval policy, then requires an exact canonical
        match.

        Parameters
        ----------
        signature
            Parsed signature to authenticate. A mapping must first be decoded
            with :meth:`PlanSignature.from_dict`.
        fragment
            Original generated graph choices referenced by the signature.
        region_input_schema_digest
            Current trusted schema supplied to ``$input`` dependency ports.
        expected_output_schema_digests
            Current ordered output contracts of the surrounding region.

        Returns
        -------
        PlanSignature
            Newly rebuilt trusted signature. Future materializers must use this
            returned value and its exact per-node grants, not the imported
            object's descriptors.

        Raises
        ------
        PlanValidationError
            If the value is malformed or any trusted fact has changed or was
            forged.
        """
        if not isinstance(signature, PlanSignature):
            raise _error("PLAN_SIGNATURE_INVALID", "signature must be PlanSignature")
        rebuilt = self.validate(
            fragment,
            region_input_schema_digest=region_input_schema_digest,
            expected_output_schema_digests=expected_output_schema_digests,
        )
        if signature.canonical_json() != rebuilt.canonical_json():
            raise _error(
                "PLAN_SIGNATURE_UNTRUSTED",
                "signature does not match the current trusted validation context",
            )
        return rebuilt

    def _validate_budget(self, aggregate: Budget) -> None:
        limit = self.limits.budget
        dimensions: tuple[tuple[str, int | float | None, int | float | None], ...] = (
            ("model_tokens", aggregate.model_tokens, limit.model_tokens),
            ("tool_calls", aggregate.tool_calls, limit.tool_calls),
            ("cost_usd", aggregate.cost_usd, limit.cost_usd),
            (
                "wall_time",
                cast(int | None, aggregate.to_data()["wall_time_ms"]),
                cast(int | None, limit.to_data()["wall_time_ms"]),
            ),
        )
        for name, actual, maximum in dimensions:
            if maximum is None:
                continue
            if actual is None:
                raise _error(
                    "PLAN_BUDGET_EXCEEDED",
                    f"unbounded child {name} exceeds the finite region budget",
                )
            if name == "cost_usd":
                exceeded = Decimal(str(actual)) > Decimal(str(maximum))
            else:
                exceeded = actual > maximum
            if exceeded:
                raise _error(
                    "PLAN_BUDGET_EXCEEDED",
                    f"aggregate {name} exceeds the region budget",
                )

    def _validate_approvals(self, requirements: tuple[tuple[str, str], ...]) -> None:
        if requirements and self.approval_check is None:
            raise _error(
                "PLAN_APPROVAL_REQUIRED",
                "approval-required effects need a trusted approval policy check",
            )
        for node_key, effect_name in requirements:
            try:
                result = cast(Callable[[str, str, str], None], self.approval_check)(
                    self.region_id, node_key, effect_name
                )
            except Exception:
                raise _error(
                    "PLAN_APPROVAL_VALIDATION_FAILED",
                    "trusted approval policy validation failed",
                ) from None
            if result is not None:
                raise _error(
                    "PLAN_APPROVAL_VALIDATION_FAILED",
                    "trusted approval policy must return None",
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
        reachable: set[str] = set()
        pending = list(fragment.outputs)
        while pending:
            key = pending.pop()
            if key in reachable:
                continue
            reachable.add(key)
            for dependency in by_key[key].dependencies:
                if dependency == _REGION_INPUT:
                    continue
                if dependency not in by_key:
                    raise _error(
                        "PLAN_TOPOLOGY_INVALID",
                        f"node {key!r} dependency {dependency!r} does not exist",
                    )
                pending.append(dependency)
        if reachable != set(keys):
            raise _error(
                "PLAN_TOPOLOGY_INVALID",
                "plan contains a node unreachable from outputs",
            )

        entries: dict[str, _CatalogEntry] = {}
        for node in fragment.nodes:
            try:
                entries[node.key] = self.registry._entry(node.module_alias)
            except KeyError as exc:
                raise _error(
                    "PLAN_MODULE_NOT_ALLOWED",
                    f"module alias {node.module_alias!r} is not allowlisted",
                ) from exc
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
                    "capability_grant": _entry_grant(entry).to_data(),
                    "dependencies": list(node.dependencies),
                    "key": node.key,
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

        resolved_tuple = tuple(resolved)
        metrics = _resolved_metrics(resolved_tuple, region_input_schema_digest, fragment.outputs)
        try:
            self.region_grant.narrow(
                capabilities=metrics.required_grant.capabilities,
                effects=metrics.required_grant.effects,
            )
        except ValueError as exc:
            raise _error(
                "PLAN_CAPABILITY_ESCALATION",
                "generated plan requires access outside the trusted region grant",
            ) from exc
        return PlanSignature(
            region_id=self.region_id,
            region_grant=self.region_grant,
            required_grant=metrics.required_grant,
            aggregate_budget=metrics.aggregate_budget,
            node_count=len(fragment.nodes),
            max_depth=max_depth,
            max_fanout=max_fanout,
            module_composition=metrics.module_composition,
            topology_digest=metrics.topology_digest,
            resolved_nodes=resolved_tuple,
            region_input_schema_digest=region_input_schema_digest,
            outputs=fragment.outputs,
            output_schema_digests=actual_outputs,
            approval_requirements=metrics.approval_requirements,
            source_fragment_digest=fragment.digest,
            catalog_digest=self.registry.digest,
            alias_provenance=tuple((node.key, node.module_alias) for node in fragment.nodes),
        )

    @staticmethod
    def _depths(by_key: Mapping[str, PlanNode]) -> dict[str, int]:
        depths, _order = _topological_depths(
            {key: node.dependencies for key, node in by_key.items()},
            error_code="PLAN_TOPOLOGY_INVALID",
            cycle_message="plan topology must be acyclic",
        )
        return depths
