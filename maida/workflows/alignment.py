"""Compare compiled workflow graphs by stable replay identity and topology.

The aligner uses one exact correspondence model for static definition diffs and
replay validation. Behavior and schema changes remain aligned, while ambiguous
identity, ordering, dependency, or control-flow changes stop correspondence.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any

from .budget import Budget
from .ir import PlanIR, ReplayKey, StepIR


class DiffKind(StrEnum):
    """Classification of a structural or behavior-bearing graph change."""

    MODULE_DIGEST_CHANGED = "MODULE_DIGEST_CHANGED"
    BUDGET_CHANGED = "BUDGET_CHANGED"
    CAPABILITY_CHANGED = "CAPABILITY_CHANGED"
    EFFECT_CHANGED = "EFFECT_CHANGED"
    CONNECTOR_CHANGED = "CONNECTOR_CHANGED"
    POLICY_CHANGED = "POLICY_CHANGED"
    SCHEMA_CHANGED = "SCHEMA_CHANGED"
    INSERTION = "INSERTION"
    DELETION = "DELETION"
    REORDER = "REORDER"
    TOPOLOGY_CHANGED = "TOPOLOGY_CHANGED"
    CONTROL_FLOW_CHANGED = "CONTROL_FLOW_CHANGED"


@dataclass(frozen=True)
class GraphChange:
    """One localized difference between historical and current workflow IR."""

    kind: DiffKind
    location: str
    replay_key: ReplayKey | None
    before: Any
    after: Any
    resolvable: bool


@dataclass(frozen=True)
class GraphDiff:
    """Ordered structural changes discovered during exact graph alignment."""

    changes: tuple[GraphChange, ...] = ()

    @property
    def has_changes(self) -> bool:
        """Return whether any resolvable or divergent change was recorded."""
        return bool(self.changes)

    @property
    def first_divergence(self) -> GraphChange | None:
        """Return the first change that prevents exact correspondence."""
        return next((change for change in self.changes if not change.resolvable), None)

    @property
    def aligned(self) -> bool:
        """Return whether all observed changes preserve graph correspondence."""
        return self.first_divergence is None


@dataclass(frozen=True)
class AlignmentPair:
    """Historical and current executable steps sharing one replay key."""

    replay_key: ReplayKey
    historical: StepIR
    current: StepIR


@dataclass(frozen=True)
class GraphAlignment:
    """Matched executable steps together with their structured graph diff."""

    pairs: tuple[AlignmentPair, ...]
    diff: GraphDiff


class GraphAligner:
    """Align two workflow definitions without guessing correspondence."""

    def align(self, historical: PlanIR, current: PlanIR) -> GraphAlignment:
        """Compare historical and current plans by exact replay identity.

        Parameters
        ----------
        historical
            Baseline or source workflow definition.
        current
            Definition being evaluated.

        Returns
        -------
        GraphAlignment
            Matched steps and ordered changes up to the first divergence.
        """
        old_steps = historical.executable_steps
        new_steps = current.executable_steps
        old_by_key = {step.replay_key: step for step in old_steps}
        new_by_key = {step.replay_key: step for step in new_steps}
        old_keys = tuple(key for step in old_steps if (key := step.replay_key) is not None)
        new_keys = tuple(key for step in new_steps if (key := step.replay_key) is not None)
        changes: list[GraphChange] = []

        for index, key in enumerate(old_keys):
            if key not in new_by_key:
                changes.append(
                    GraphChange(
                        DiffKind.DELETION, f"steps[{index}]", key, key.as_string(), None, False
                    )
                )
                return GraphAlignment((), GraphDiff(tuple(changes)))
        for index, key in enumerate(new_keys):
            if key not in old_by_key:
                changes.append(
                    GraphChange(
                        DiffKind.INSERTION, f"steps[{index}]", key, None, key.as_string(), False
                    )
                )
                return GraphAlignment((), GraphDiff(tuple(changes)))
        if old_keys != new_keys:
            mismatch = next(
                index
                for index, (old_key, new_key) in enumerate(zip(old_keys, new_keys, strict=True))
                if old_key != new_key
            )
            changes.append(
                GraphChange(
                    DiffKind.REORDER,
                    f"steps[{mismatch}]",
                    old_keys[mismatch],
                    old_keys[mismatch].as_string(),
                    new_keys[mismatch].as_string(),
                    False,
                )
            )
            return GraphAlignment((), GraphDiff(tuple(changes)))

        old_controls = _control_signature(historical)
        new_controls = _control_signature(current)
        if old_controls != new_controls:
            location = _first_mismatch_location(old_controls, new_controls, "controls")
            changes.append(
                GraphChange(
                    DiffKind.CONTROL_FLOW_CHANGED,
                    location,
                    None,
                    old_controls,
                    new_controls,
                    False,
                )
            )
            return GraphAlignment((), GraphDiff(tuple(changes)))

        pairs: list[AlignmentPair] = []
        for index, key in enumerate(old_keys):
            old = old_by_key[key]
            new = new_by_key[key]
            old_control = (old.kind, old.control)
            new_control = (new.kind, new.control)
            if old_control != new_control:
                changes.append(
                    GraphChange(
                        DiffKind.CONTROL_FLOW_CHANGED,
                        f"steps[{index}].control",
                        key,
                        old_control,
                        new_control,
                        False,
                    )
                )
                return GraphAlignment(tuple(pairs), GraphDiff(tuple(changes)))
            old_topology = _dependency_keys(historical, old)
            new_topology = _dependency_keys(current, new)
            if old_topology != new_topology:
                changes.append(
                    GraphChange(
                        DiffKind.TOPOLOGY_CHANGED,
                        f"steps[{index}].dependencies",
                        key,
                        tuple(item.as_string() for item in old_topology),
                        tuple(item.as_string() for item in new_topology),
                        False,
                    )
                )
                return GraphAlignment(tuple(pairs), GraphDiff(tuple(changes)))
            if old.module_digest != new.module_digest:
                changes.append(
                    GraphChange(
                        DiffKind.MODULE_DIGEST_CHANGED,
                        f"steps[{index}].module_digest",
                        key,
                        old.module_digest,
                        new.module_digest,
                        True,
                    )
                )
            if _budget_signature(old) != _budget_signature(new):
                changes.append(
                    GraphChange(
                        DiffKind.BUDGET_CHANGED,
                        f"steps[{index}].budget",
                        key,
                        _budget_signature(old),
                        _budget_signature(new),
                        True,
                    )
                )
            if old.capabilities != new.capabilities:
                changes.append(
                    GraphChange(
                        DiffKind.CAPABILITY_CHANGED,
                        f"steps[{index}].capabilities",
                        key,
                        old.capabilities,
                        new.capabilities,
                        True,
                    )
                )
            if old.effects != new.effects:
                changes.append(
                    GraphChange(
                        DiffKind.EFFECT_CHANGED,
                        f"steps[{index}].effects",
                        key,
                        old.effects,
                        new.effects,
                        True,
                    )
                )
            if _connector_signature(old) != _connector_signature(new):
                changes.append(
                    GraphChange(
                        DiffKind.CONNECTOR_CHANGED,
                        f"steps[{index}].connectors",
                        key,
                        _connector_signature(old),
                        _connector_signature(new),
                        True,
                    )
                )
            if _policy_signature(old) != _policy_signature(new):
                changes.append(
                    GraphChange(
                        DiffKind.POLICY_CHANGED,
                        f"steps[{index}].policies",
                        key,
                        _policy_signature(old),
                        _policy_signature(new),
                        True,
                    )
                )
            old_schemas = (
                old.input_binding.schema_digest if old.input_binding else None,
                old.output_schema_digest,
            )
            new_schemas = (
                new.input_binding.schema_digest if new.input_binding else None,
                new.output_schema_digest,
            )
            if old_schemas != new_schemas:
                changes.append(
                    GraphChange(
                        DiffKind.SCHEMA_CHANGED,
                        f"steps[{index}].schemas",
                        key,
                        old_schemas,
                        new_schemas,
                        True,
                    )
                )
            pairs.append(AlignmentPair(key, old, new))
        return GraphAlignment(tuple(pairs), GraphDiff(tuple(changes)))


def project_execution_path(
    plan: PlanIR,
    control_decisions: tuple[dict[str, Any], ...],
) -> PlanIR:
    """Project static IR onto one recorded branch path without guessing correspondence."""

    decisions = {
        str(decision.get("payload", {}).get("control_node")): str(
            decision.get("payload", {}).get("selected")
        )
        for decision in control_decisions
        if decision.get("event_type") == "BRANCH_DECISION"
    }
    by_node = {step.node_id: step for step in plan.steps}
    included: dict[str, StepIR] = {}

    def visit(node_id: str) -> None:
        if node_id == "input" or node_id in included:
            return
        step = by_node[node_id]
        if step.kind != "when":
            for dependency in step.dependencies:
                visit(dependency)
            included[node_id] = step
            return
        selected = decisions.get(node_id)
        branch_index = {"true": 1, "false": 2}.get(selected) if selected is not None else None
        if branch_index is None:
            for dependency in step.dependencies:
                visit(dependency)
            included[node_id] = replace(
                step,
                control={**(step.control or {}), "recorded_decision": "missing"},
            )
            return
        selected_dependencies = (step.dependencies[0], step.dependencies[branch_index])
        for dependency in selected_dependencies:
            visit(dependency)
        included[node_id] = replace(
            step,
            dependencies=selected_dependencies,
            control={**(step.control or {}), "recorded_decision": selected},
        )

    visit(plan.output_node)
    return replace(plan, steps=tuple(step for step in plan.steps if step.node_id in included))


def _control_signature(plan: PlanIR) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        (
            step.node_id,
            step.kind,
            step.dependencies,
            step.input_binding.to_data() if step.input_binding is not None else None,
            step.control,
        )
        for step in plan.steps
        if step.replay_key is None
    )


def _first_mismatch_location(before: tuple[Any, ...], after: tuple[Any, ...], root: str) -> str:
    for index, pair in enumerate(zip(before, after, strict=False)):
        if pair[0] != pair[1]:
            return f"{root}[{index}]"
    return f"{root}[{min(len(before), len(after))}]"


def _dependency_keys(plan: PlanIR, step: StepIR) -> tuple[ReplayKey, ...]:
    by_node = {candidate.node_id: candidate for candidate in plan.steps}
    found: list[ReplayKey] = []
    seen: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in seen or node_id == "input":
            return
        seen.add(node_id)
        node = by_node.get(node_id)
        if node is None:
            return
        if node.replay_key is not None:
            found.append(node.replay_key)
            return
        for dependency in node.dependencies:
            visit(dependency)

    for dependency in step.dependencies:
        visit(dependency)
    return tuple(found)


def _connector_signature(step: StepIR) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        (
            declaration.get("name"),
            declaration.get("connector"),
            declaration.get("operation"),
            declaration.get("connector_version"),
        )
        for declaration in (*step.capabilities, *step.effects)
    )


def _budget_signature(step: StepIR) -> dict[str, int | float | None]:
    """Normalize legacy missing declarations to the unbounded budget."""
    return dict(step.budget) if step.budget is not None else Budget().to_data()


def _policy_signature(step: StepIR) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        (
            declaration.get("name"),
            tuple(declaration.get("policy_tags", ())),
            declaration.get("approval_required"),
        )
        for declaration in (*step.capabilities, *step.effects)
    )
