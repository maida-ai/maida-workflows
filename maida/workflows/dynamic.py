"""Validate generated graph fragments against trusted module definitions.

Generated planning output is a deliberately restricted mapping containing
graph choices only. Module identities, schemas, execution requirements, and
external-access declarations remain in a trusted :class:`ModuleRegistry` and
are resolved directly into the canonical :class:`~maida.workflows.ir.PlanIR`
before any later materialization step may use them.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal
from functools import cached_property
from types import MappingProxyType
from typing import Any, Self, cast

from ._canonical import canonical_data, canonical_json, digest_data, schema_digest
from .budget import Budget
from .ir import BindingIR, PlanIR, StepIR, _definition_digest, _finalize_plan
from .models import CapabilityGrant
from .registry import ModuleRegistry, _catalog_entry, _CatalogEntry

PLAN_FRAGMENT_VERSION = "0.2.0"
PLAN_SIGNATURE_VERSION = "0.4.0"
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


def _decode_generated_plan(data: Mapping[str, Any]) -> dict[str, Any]:
    """Strictly decode model-produced graph choices without creating a plan type."""
    if not isinstance(data, Mapping):
        raise _error("PLAN_FRAGMENT_INVALID", "generated plan must be an object")
    _require_exact_fields(data, _FRAGMENT_FIELDS, label="generated plan")
    if data["version"] != PLAN_FRAGMENT_VERSION:
        raise _error(
            "PLAN_FRAGMENT_VERSION_UNSUPPORTED",
            f"unsupported generated plan version {data['version']!r}",
        )
    fragment_id = _require_stable_name(data["fragment_id"], label="fragment_id")
    raw_nodes = data["nodes"]
    raw_outputs = data["outputs"]
    if not isinstance(raw_nodes, list):
        raise _error("PLAN_FRAGMENT_INVALID", "generated plan nodes must be an array")
    if not isinstance(raw_outputs, list):
        raise _error("PLAN_FRAGMENT_INVALID", "generated plan outputs must be an array")
    nodes: list[dict[str, Any]] = []
    for raw_node in raw_nodes:
        if not isinstance(raw_node, Mapping):
            raise _error("PLAN_FRAGMENT_INVALID", "generated plan node must be an object")
        _require_exact_fields(raw_node, _NODE_FIELDS, label="generated plan node")
        dependencies = raw_node["dependencies"]
        if not isinstance(dependencies, list):
            raise _error("PLAN_FRAGMENT_INVALID", "generated dependencies must be an array")
        key = _require_stable_name(raw_node["key"], label="plan node key")
        alias = _require_stable_name(raw_node["module_alias"], label="module alias")
        for dependency in dependencies:
            if dependency != _REGION_INPUT:
                _require_stable_name(dependency, label="plan dependency")
        nodes.append({"dependencies": list(dependencies), "key": key, "module_alias": alias})
    keys = [cast(str, node["key"]) for node in nodes]
    if keys != sorted(keys):
        raise _error("PLAN_FRAGMENT_INVALID", "generated nodes must be in canonical key order")
    if len(keys) != len(set(keys)):
        raise _error("PLAN_TOPOLOGY_INVALID", "generated plan contains a duplicate node key")
    outputs = [_require_stable_name(output, label="fragment output") for output in raw_outputs]
    restored = {
        "fragment_id": fragment_id,
        "nodes": nodes,
        "outputs": outputs,
        "version": PLAN_FRAGMENT_VERSION,
    }
    if canonical_json(restored) != canonical_json(data):
        raise _error("PLAN_FRAGMENT_INVALID", "generated plan is not canonical")
    return restored


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
    "plan",
    "region_grant",
    "region_id",
    "region_input_schema_digest",
    "required_grant",
    "resolved_nodes",
    "source_fragment_digest",
    "topology_digest",
    "version",
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


def _resolved_nodes_from_plan(plan: PlanIR) -> tuple[dict[str, Any], ...]:
    """Derive core evidence from the authoritative generated PlanIR graph."""
    resolved: list[dict[str, Any]] = []
    for step in plan.executable_steps:
        control = step.control
        if not isinstance(control, Mapping) or control.get("region") != "generated":
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "generated plan executable steps require generated control provenance",
            )
        key = _require_stable_name(
            control.get("key"),
            label="generated plan node key",
            error_code="PLAN_SIGNATURE_INVALID",
        )
        raw_inputs = control.get("input_schema_digests")
        if not isinstance(raw_inputs, (list, tuple)):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "generated plan input schemas must be an ordered sequence",
            )
        input_schemas = tuple(
            _require_digest(
                value,
                label="generated plan input schema",
                error_code="PLAN_SIGNATURE_INVALID",
            )
            for value in raw_inputs
        )
        dependencies = [
            _REGION_INPUT if dependency == "input" else dependency.removeprefix("nodes/")
            for dependency in step.dependencies
        ]
        if any(
            dependency != _REGION_INPUT and f"nodes/{dependency}" not in step.dependencies
            for dependency in dependencies
        ):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "generated plan dependencies must use canonical node addresses",
            )
        if any(
            value is None
            for value in (
                step.module_id,
                step.module_digest,
                step.execution,
                step.budget,
            )
        ):
            raise _error("PLAN_SIGNATURE_INVALID", "generated executable step is incomplete")
        entry = _catalog_entry(
            module_id=step.module_id,
            module_digest=step.module_digest,
            input_schema_digests=input_schemas,
            output_schema_digest=step.output_schema_digest,
            execution=step.execution,
            capabilities=step.capabilities,
            effects=step.effects,
            models=step.models,
            budget=step.budget,
            require_canonical=True,
        )
        input_schema = (
            input_schemas[0]
            if len(input_schemas) == 1
            else digest_data({"ordered_input_schemas": input_schemas})
        )
        if step.logical_step is None or step.definition_digest != _definition_digest(
            module_id=entry.module_id,
            logical_step=step.logical_step,
            module_digest=entry.module_digest,
            input_schema_digest=input_schema,
            output_schema_digest=entry.output_schema_digest,
            execution=entry.execution,
            capabilities=entry.capabilities,
            effects=entry.effects,
            models=entry.models,
            budget=entry.budget.to_data(),
            control=control,
        ):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "generated plan definition identity is inconsistent",
            )
        resolved.append(
            {
                **entry.to_dict(),
                "capability_grant": _entry_grant(entry).to_data(),
                "dependencies": dependencies,
                "key": key,
            }
        )
    return tuple(sorted(resolved, key=lambda node: cast(str, node["key"])))


@dataclass(frozen=True)
class PlanSignature:
    """Trusted provenance and core evidence derived from one canonical PlanIR."""

    plan: PlanIR
    region_grant: CapabilityGrant
    source_fragment_digest: str = field(compare=False)
    catalog_digest: str = field(compare=False)
    alias_provenance: tuple[tuple[str, str], ...] = field(compare=False)
    version: str = PLAN_SIGNATURE_VERSION

    def __post_init__(self) -> None:
        if self.version != PLAN_SIGNATURE_VERSION:
            raise _error(
                "PLAN_SIGNATURE_VERSION_UNSUPPORTED",
                f"unsupported PlanSignature version {self.version!r}",
            )
        if not isinstance(self.plan, PlanIR):
            raise _error("PLAN_SIGNATURE_INVALID", "plan must be PlanIR")
        try:
            plan = PlanIR.from_dict(self.plan.to_dict())
        except (KeyError, TypeError, ValueError) as exc:
            raise _error("PLAN_SIGNATURE_INVALID", f"plan is invalid: {exc}") from exc
        if not isinstance(self.region_grant, CapabilityGrant):
            raise _error("PLAN_SIGNATURE_INVALID", "region_grant must be CapabilityGrant")
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
        if not isinstance(self.alias_provenance, (list, tuple)) or any(
            not isinstance(item, (list, tuple)) or len(item) != 2 for item in self.alias_provenance
        ):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "alias provenance must contain two-field tuples",
            )
        provenance = tuple(
            sorted(cast(tuple[str, str], tuple(item)) for item in self.alias_provenance)
        )
        for node_key, alias in provenance:
            _require_stable_name(
                node_key, label="alias node key", error_code="PLAN_SIGNATURE_INVALID"
            )
            _require_stable_name(alias, label="module alias", error_code="PLAN_SIGNATURE_INVALID")
        if len(provenance) != len({key for key, _alias in provenance}):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "alias provenance contains duplicate node keys",
            )
        resolved = _resolved_nodes_from_plan(plan)
        if {key for key, _alias in provenance} != {cast(str, node["key"]) for node in resolved}:
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "alias provenance does not cover generated plan nodes",
            )
        metrics = _resolved_metrics(
            resolved,
            self._region_input_schema_digest(plan),
            self._outputs(plan),
        )
        if metrics.output_schema_digests != self._output_schema_digests(plan):
            raise _error("PLAN_SIGNATURE_INVALID", "plan output schemas are inconsistent")
        output_step = next(
            (step for step in plan.steps if step.node_id == plan.output_node),
            None,
        )
        if (
            output_step is None
            or output_step.kind != "parallel"
            or not isinstance(output_step.control, Mapping)
            or output_step.control.get("region") != "generated_output"
            or output_step.dependencies != tuple(f"nodes/{key}" for key in self._outputs(plan))
        ):
            raise _error("PLAN_SIGNATURE_INVALID", "plan output node is inconsistent")
        try:
            self.region_grant.narrow(
                capabilities=metrics.required_grant.capabilities,
                effects=metrics.required_grant.effects,
            )
        except ValueError as exc:
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "required grant exceeds region grant",
            ) from exc
        object.__setattr__(self, "plan", plan)
        object.__setattr__(self, "source_fragment_digest", source_digest)
        object.__setattr__(self, "catalog_digest", catalog_digest)
        object.__setattr__(self, "alias_provenance", provenance)

    @staticmethod
    def _region_id(plan: PlanIR) -> str:
        prefix = "dynamic:"
        if not plan.workflow_id.startswith(prefix):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "generated plan workflow identity is invalid",
            )
        return _require_stable_name(
            plan.workflow_id.removeprefix(prefix),
            label="region_id",
            error_code="PLAN_SIGNATURE_INVALID",
        )

    @staticmethod
    def _region_input_schema_digest(plan: PlanIR) -> str:
        if set(plan.input_schema) != {"digest"}:
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "generated plan input schema fields are invalid",
            )
        return _require_digest(
            plan.input_schema["digest"],
            label="region_input_schema_digest",
            error_code="PLAN_SIGNATURE_INVALID",
        )

    @staticmethod
    def _output_schema_digests(plan: PlanIR) -> tuple[str, ...]:
        if set(plan.output_schema) != {"digests"}:
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "generated plan output schema fields are invalid",
            )
        values = plan.output_schema["digests"]
        if not isinstance(values, (list, tuple)):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "generated plan output schemas must be an array",
            )
        return tuple(
            _require_digest(
                value,
                label="output_schema_digest",
                error_code="PLAN_SIGNATURE_INVALID",
            )
            for value in values
        )

    @staticmethod
    def _outputs(plan: PlanIR) -> tuple[str, ...]:
        output_step = next(
            (step for step in plan.steps if step.node_id == plan.output_node),
            None,
        )
        if output_step is None or not isinstance(output_step.control, Mapping):
            raise _error("PLAN_SIGNATURE_INVALID", "generated plan has no output control")
        values = output_step.control.get("outputs")
        if not isinstance(values, (list, tuple)):
            raise _error(
                "PLAN_SIGNATURE_INVALID",
                "generated plan outputs must be an array",
            )
        return tuple(
            _require_stable_name(
                value,
                label="signature output",
                error_code="PLAN_SIGNATURE_INVALID",
            )
            for value in values
        )

    @property
    def region_id(self) -> str:
        """Return the trusted dynamic-region identity carried by the plan."""
        return self._region_id(self.plan)

    @property
    def resolved_nodes(self) -> tuple[Mapping[str, Any], ...]:
        """Derive immutable core evidence nodes from authoritative PlanIR."""
        return self._resolved_evidence

    @cached_property
    def _resolved_evidence(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(
            cast(Mapping[str, Any], _freeze_json(node))
            for node in _resolved_nodes_from_plan(self.plan)
        )

    @cached_property
    def _derived_metrics(self) -> _ResolvedMetrics:
        return _resolved_metrics(
            tuple(cast(dict[str, Any], canonical_data(dict(node))) for node in self.resolved_nodes),
            self.region_input_schema_digest,
            self.outputs,
        )

    @property
    def required_grant(self) -> CapabilityGrant:
        """Return the least-privilege grant derived from all plan steps."""
        return self._derived_metrics.required_grant

    @property
    def aggregate_budget(self) -> Budget:
        """Return the resource envelope derived from the canonical graph."""
        return self._derived_metrics.aggregate_budget

    @property
    def node_count(self) -> int:
        """Return the number of executable generated nodes in the plan."""
        return len(self.resolved_nodes)

    @property
    def max_depth(self) -> int:
        """Return the maximum dependency depth of the generated graph."""
        return self._derived_metrics.max_depth

    @property
    def max_fanout(self) -> int:
        """Return the maximum direct consumer count in the generated graph."""
        return self._derived_metrics.max_fanout

    @property
    def module_composition(self) -> tuple[tuple[str, str, int], ...]:
        """Return canonical module pins and occurrence counts for core evidence."""
        return self._derived_metrics.module_composition

    @property
    def topology_digest(self) -> str:
        """Return the alias-free digest of generated nodes and outputs."""
        return self._derived_metrics.topology_digest

    @property
    def region_input_schema_digest(self) -> str:
        """Return the trusted schema digest for the generated region input."""
        return self._region_input_schema_digest(self.plan)

    @property
    def outputs(self) -> tuple[str, ...]:
        """Return ordered generated node keys selected as region outputs."""
        return self._outputs(self.plan)

    @property
    def output_schema_digests(self) -> tuple[str, ...]:
        """Return trusted ordered schema digests for generated outputs."""
        return self._output_schema_digests(self.plan)

    @property
    def approval_requirements(self) -> tuple[tuple[str, str], ...]:
        """Return approval-required effect occurrences derived from PlanIR."""
        return self._derived_metrics.approval_requirements

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
        """Serialize authoritative PlanIR with its derived evidence and provenance."""
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
                    "plan": self.plan.to_dict(),
                    "source_fragment_digest": self.source_fragment_digest,
                }
            ),
        )

    def to_core_dict(self) -> dict[str, Any]:
        """Return the resolved evidence contract consumed by Maida core."""
        data = self._behavior_data()
        data["alias_provenance"] = [
            {"alias": alias, "node_key": node_key} for node_key, alias in self.alias_provenance
        ]
        data["resolved_nodes"] = [
            {key: value for key, value in dict(node).items() if key != "models"}
            for node in self.resolved_nodes
        ]
        return cast(dict[str, Any], canonical_data(data))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        """Restore a signature only when evidence agrees with its canonical plan."""
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
        try:
            raw_plan = data["plan"]
            if not isinstance(raw_plan, Mapping):
                raise ValueError("plan must be an object")
            raw_provenance = data["alias_provenance"]
            if not isinstance(raw_provenance, list):
                raise ValueError("alias_provenance must be an array")
            provenance: list[tuple[str, str]] = []
            for item in raw_provenance:
                if not isinstance(item, Mapping) or set(item) != {"alias", "node_key"}:
                    raise ValueError("alias provenance fields do not match the contract")
                node_key = item["node_key"]
                alias = item["alias"]
                if not isinstance(node_key, str) or not isinstance(alias, str):
                    raise ValueError("alias provenance values must be strings")
                provenance.append((node_key, alias))
            region_grant = _canonical_grant(data["region_grant"], label="region_grant")
            result = cls(
                plan=PlanIR.from_dict(raw_plan),
                region_grant=region_grant,
                source_fragment_digest=data["source_fragment_digest"],
                catalog_digest=data["catalog_digest"],
                alias_provenance=tuple(provenance),
                version=data["version"],
            )
            if canonical_json(result.to_dict()) != canonical_json(data):
                raise ValueError("serialized evidence does not match its authoritative PlanIR")
            return result
        except (KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, PlanValidationError):
                raise
            raise _error("PLAN_SIGNATURE_INVALID", str(exc)) from exc

    def canonical_json(self) -> str:
        """Serialize plan evidence and provenance as deterministic JSON."""
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        """Return the alias-invariant digest of derived plan behavior."""
        return digest_data(self._behavior_data())


def _build_generated_plan(
    *,
    region_id: str,
    resolved_nodes: tuple[dict[str, Any], ...],
    region_input_schema_digest: str,
    outputs: tuple[str, ...],
    output_schema_digests: tuple[str, ...],
) -> PlanIR:
    """Resolve validated generated choices directly into canonical PlanIR."""
    steps: list[StepIR] = []
    by_key = {cast(str, descriptor["key"]): descriptor for descriptor in resolved_nodes}
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
        binding = (
            BindingIR(schema_digest=input_schema, kind="source", source=dependencies[0])
            if len(dependencies) == 1
            else BindingIR(
                schema_digest=input_schema,
                kind="tuple",
                items=tuple(
                    BindingIR(schema_digest=schema, kind="source", source=dependency)
                    for dependency, schema in zip(dependencies, input_schemas, strict=True)
                ),
            )
        )
        logical_step = f"dynamic/{region_id}/nodes/{node_key}"
        control = {
            "input_schema_digests": list(input_schemas),
            "key": node_key,
            "region": "generated",
        }
        steps.append(
            StepIR(
                node_id=f"nodes/{node_key}",
                kind="module",
                dependencies=dependencies,
                output_schema_digest=cast(str, descriptor["output_schema_digest"]),
                module_id=cast(str, descriptor["module_id"]),
                logical_step=logical_step,
                module_digest=cast(str, descriptor["module_digest"]),
                definition_digest=_definition_digest(
                    module_id=cast(str, descriptor["module_id"]),
                    logical_step=logical_step,
                    module_digest=cast(str, descriptor["module_digest"]),
                    input_schema_digest=input_schema,
                    output_schema_digest=cast(str, descriptor["output_schema_digest"]),
                    execution=cast(Mapping[str, Any], descriptor["execution"]),
                    capabilities=tuple(
                        cast(tuple[Mapping[str, Any], ...], descriptor["capabilities"])
                    ),
                    effects=tuple(cast(tuple[Mapping[str, Any], ...], descriptor["effects"])),
                    models=tuple(cast(tuple[Mapping[str, Any], ...], descriptor["models"])),
                    budget=cast(Mapping[str, int | float | None], descriptor["budget"]),
                    control=control,
                ),
                input_binding=binding,
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
                models=tuple(
                    cast(
                        list[Mapping[str, Any]],
                        canonical_data(descriptor["models"]),
                    )
                ),
                budget=cast(Mapping[str, int | float | None], canonical_data(descriptor["budget"])),
                control=control,
            )
        )
    steps.append(
        StepIR(
            node_id="output",
            kind="parallel",
            dependencies=tuple(f"nodes/{key}" for key in outputs),
            output_schema_digest=digest_data({"ordered_output_schemas": output_schema_digests}),
            control={"outputs": outputs, "region": "generated_output"},
        )
    )
    return _finalize_plan(
        workflow_id=f"dynamic:{region_id}",
        input_schema={"digest": region_input_schema_digest},
        output_schema={"digests": list(output_schema_digests)},
        steps=tuple(steps),
        output_node="output",
    )


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
        fragment: Mapping[str, Any],
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
        source = _decode_generated_plan(fragment)
        region_input = _require_digest(
            region_input_schema_digest,
            label="region_input_schema_digest",
        )
        expected_outputs = tuple(
            _require_digest(value, label="expected_output_schema_digest")
            for value in expected_output_schema_digests
        )
        signature = self._resolve_graph(source, region_input, expected_outputs)
        self._validate_budget(signature.aggregate_budget)
        self._validate_approvals(signature.approval_requirements)
        return signature

    def revalidate(
        self,
        signature: PlanSignature,
        fragment: Mapping[str, Any],
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
        fragment: Mapping[str, Any],
        region_input_schema_digest: str,
        expected_output_schema_digests: tuple[str, ...],
    ) -> PlanSignature:
        nodes = cast(list[dict[str, Any]], fragment["nodes"])
        outputs = tuple(cast(list[str], fragment["outputs"]))
        if not nodes:
            raise _error("PLAN_TOPOLOGY_INVALID", "plan requires at least one node")
        if len(nodes) > self.limits.max_nodes:
            raise _error(
                "PLAN_LIMIT_EXCEEDED",
                f"plan node count {len(nodes)} exceeds {self.limits.max_nodes}",
            )
        keys = [cast(str, node["key"]) for node in nodes]
        if len(keys) != len(set(keys)):
            raise _error("PLAN_TOPOLOGY_INVALID", "fragment contains a duplicate node key")
        by_key = {cast(str, node["key"]): node for node in nodes}
        if not outputs:
            raise _error("PLAN_TOPOLOGY_INVALID", "plan requires at least one output")
        if len(outputs) != len(set(outputs)):
            raise _error("PLAN_TOPOLOGY_INVALID", "fragment contains a duplicate output")
        for output in outputs:
            if output not in by_key:
                raise _error(
                    "PLAN_TOPOLOGY_INVALID",
                    f"fragment output {output!r} does not exist",
                )
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
        for node in nodes:
            node_key = cast(str, node["key"])
            alias = cast(str, node["module_alias"])
            dependencies = cast(list[str], node["dependencies"])
            try:
                entries[node_key] = self.registry._entry(alias)
            except KeyError as exc:
                raise _error(
                    "PLAN_MODULE_NOT_ALLOWED",
                    f"module alias {alias!r} is not allowlisted",
                ) from exc
            if len(dependencies) != len(set(dependencies)):
                raise _error(
                    "PLAN_TOPOLOGY_INVALID",
                    f"node {node_key!r} contains a duplicate dependency",
                )
            if len(dependencies) != len(entries[node_key].input_schema_digests):
                raise _error(
                    "PLAN_SCHEMA_INVALID",
                    f"node {node_key!r} input count does not match its trusted module contract",
                )
            for dependency in dependencies:
                if dependency != _REGION_INPUT and dependency not in by_key:
                    raise _error(
                        "PLAN_TOPOLOGY_INVALID",
                        f"node {node_key!r} dependency {dependency!r} does not exist",
                    )

        depths = self._depths(by_key)
        max_depth = max(depths.values())
        if max_depth > self.limits.max_depth:
            raise _error(
                "PLAN_LIMIT_EXCEEDED",
                f"plan depth {max_depth} exceeds {self.limits.max_depth}",
            )
        fanouts = Counter(
            dependency for node in nodes for dependency in cast(list[str], node["dependencies"])
        )
        max_fanout = max(fanouts.values(), default=0)
        if max_fanout > self.limits.max_fanout:
            raise _error(
                "PLAN_LIMIT_EXCEEDED",
                f"plan fanout {max_fanout} exceeds {self.limits.max_fanout}",
            )

        resolved: list[dict[str, Any]] = []
        for node in nodes:
            node_key = cast(str, node["key"])
            dependencies = cast(list[str], node["dependencies"])
            entry = entries[node_key]
            for index, (dependency, expected_schema) in enumerate(
                zip(dependencies, entry.input_schema_digests, strict=True)
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
                        f"node {node_key!r} {source} at input {index} is incompatible",
                    )
            resolved.append(
                {
                    **entry.to_dict(),
                    "capability_grant": _entry_grant(entry).to_data(),
                    "dependencies": list(dependencies),
                    "key": node_key,
                }
            )

        if len(expected_output_schema_digests) != len(outputs):
            raise _error(
                "PLAN_SCHEMA_INVALID",
                "fragment output contract count does not match trusted region output count",
            )
        actual_outputs = tuple(entries[key].output_schema_digest for key in outputs)
        for index, (actual, expected) in enumerate(
            zip(actual_outputs, expected_output_schema_digests, strict=True)
        ):
            if actual != expected:
                raise _error(
                    "PLAN_SCHEMA_INVALID",
                    f"fragment output schema at index {index} is incompatible",
                )

        resolved_tuple = tuple(resolved)
        metrics = _resolved_metrics(resolved_tuple, region_input_schema_digest, outputs)
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
        plan = _build_generated_plan(
            region_id=self.region_id,
            resolved_nodes=resolved_tuple,
            region_input_schema_digest=region_input_schema_digest,
            outputs=outputs,
            output_schema_digests=actual_outputs,
        )
        return PlanSignature(
            plan=plan,
            region_grant=self.region_grant,
            source_fragment_digest=digest_data(fragment),
            catalog_digest=self.registry.digest,
            alias_provenance=tuple(
                (cast(str, node["key"]), cast(str, node["module_alias"])) for node in nodes
            ),
        )

    @staticmethod
    def _depths(by_key: Mapping[str, Mapping[str, Any]]) -> dict[str, int]:
        depths, _order = _topological_depths(
            {key: tuple(cast(list[str], node["dependencies"])) for key, node in by_key.items()},
            error_code="PLAN_TOPOLOGY_INVALID",
            cycle_message="plan topology must be acyclic",
        )
        return depths
