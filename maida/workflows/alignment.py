from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any

from .ir import PlanIR, ReplayKey, StepIR


class DiffKind(StrEnum):
    MODULE_DIGEST_CHANGED = "MODULE_DIGEST_CHANGED"
    SCHEMA_CHANGED = "SCHEMA_CHANGED"
    INSERTION = "INSERTION"
    DELETION = "DELETION"
    REORDER = "REORDER"
    TOPOLOGY_CHANGED = "TOPOLOGY_CHANGED"
    CONTROL_FLOW_CHANGED = "CONTROL_FLOW_CHANGED"


@dataclass(frozen=True)
class GraphChange:
    kind: DiffKind
    location: str
    replay_key: ReplayKey | None
    before: Any
    after: Any
    resolvable: bool


@dataclass(frozen=True)
class GraphDiff:
    changes: tuple[GraphChange, ...] = ()

    @property
    def has_changes(self) -> bool:
        return bool(self.changes)

    @property
    def first_divergence(self) -> GraphChange | None:
        return next((change for change in self.changes if not change.resolvable), None)

    @property
    def aligned(self) -> bool:
        return self.first_divergence is None


@dataclass(frozen=True)
class AlignmentPair:
    replay_key: ReplayKey
    historical: StepIR
    current: StepIR


@dataclass(frozen=True)
class GraphAlignment:
    pairs: tuple[AlignmentPair, ...]
    diff: GraphDiff


class GraphAligner:
    """One exact correspondence algorithm shared by structural diff and replay."""

    def align(self, historical: PlanIR, current: PlanIR) -> GraphAlignment:
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
        (step.node_id, step.kind, step.dependencies, step.control)
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
