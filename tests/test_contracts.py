from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Any

import pytest

from maida.workflows import (
    ExecutionContext,
    Module,
    RuntimeValue,
    Workflow,
    compile_workflow,
    parallel,
)
from maida.workflows._canonical import (
    CanonicalValueError,
    canonical_data,
    canonical_json,
    schema_digest,
    type_schema,
    value_matches_type,
)
from maida.workflows.alignment import DiffKind, GraphAligner


class Color(Enum):
    RED = "red"


@dataclass(frozen=True)
class Contract:
    required: int
    optional: str = "default"


def test_canonical_values_cover_supported_shapes_and_reject_lossy_data(tmp_path: Path) -> None:
    assert canonical_data(Contract(1)) == {"required": 1, "optional": "default"}
    assert canonical_data(Color.RED) == "red"
    assert canonical_data(b"\x00\xff") == {"$bytes": "00ff"}
    assert canonical_data(tmp_path) == str(tmp_path)
    assert canonical_data({"b": 2, "a": 1}) == {"a": 1, "b": 2}
    assert canonical_data({3, 1, 2}) == [1, 2, 3]
    assert canonical_json((True, None, 1.5)) == "[true,null,1.5]"
    with pytest.raises(CanonicalValueError, match="non-finite"):
        canonical_data(float("nan"))
    with pytest.raises(CanonicalValueError, match="keys"):
        canonical_data({1: "not a string key"})
    with pytest.raises(CanonicalValueError, match="unsupported"):
        canonical_data(object())


def test_type_schemas_and_runtime_type_checks_cover_boundary_shapes() -> None:
    schema = type_schema(Contract)
    assert schema["required"] == ["required"]
    assert schema["properties"]["optional"] == {"type": "string"}
    assert type_schema(None) == {"type": "null"}
    assert type_schema(bytes)["contentEncoding"] == "hex"
    assert type_schema(list[int]) == {"type": "array", "items": {"type": "integer"}}
    assert type_schema(tuple[int, str])["prefixItems"] == [
        {"type": "integer"},
        {"type": "string"},
    ]
    assert type_schema(dict[str, bool])["additionalProperties"] == {"type": "boolean"}
    assert "anyOf" in type_schema(int | None)
    assert type_schema(Any) == {}
    assert schema_digest(int) != schema_digest(str)

    assert value_matches_type(None, int | None)
    assert value_matches_type([1, 2], list[int])
    assert not value_matches_type([1, "bad"], list[int])
    assert value_matches_type((1, "ok"), tuple[int, str])
    assert not value_matches_type((1,), tuple[int, str])
    assert value_matches_type({"key": 1}, dict[str, int])
    assert not value_matches_type({1: "bad"}, dict[str, int])
    assert not value_matches_type({"key": "bad"}, dict[str, int])
    assert value_matches_type(Contract(1), Contract)
    assert not value_matches_type(Contract("bad"), Contract)  # type: ignore[arg-type]


class Identity(Module[int, int]):
    module_id = "contracts.identity"
    input_type = int
    output_type = int

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return value


class PairWorkflow(Workflow[int, tuple[int, int]]):
    workflow_id = "alignment-pair"
    input_type = int
    output_type = tuple[int, int]

    def __init__(self) -> None:
        self.first = Identity()
        self.second = Identity()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[tuple[int, int]]:
        first = self.first.at("first")(value)
        return parallel(first, self.second.at("second")(first))


def test_graph_aligner_reports_each_unresolvable_correspondence_class() -> None:
    source = compile_workflow(PairWorkflow())
    aligner = GraphAligner()

    deletion = replace(
        source, steps=tuple(step for step in source.steps if step.logical_step != "first")
    )
    deleted = aligner.align(source, deletion).diff
    assert deleted.first_divergence is not None
    assert deleted.first_divergence.kind is DiffKind.DELETION
    assert not deleted.aligned

    inserted = aligner.align(deletion, source).diff
    assert inserted.first_divergence is not None
    assert inserted.first_divergence.kind is DiffKind.INSERTION

    executable = [step for step in source.steps if step.replay_key is not None]
    controls = [step for step in source.steps if step.replay_key is None]
    reordered_plan = replace(source, steps=tuple(reversed(executable)) + tuple(controls))
    reordered = aligner.align(source, reordered_plan).diff
    assert reordered.first_divergence is not None
    assert reordered.first_divergence.kind is DiffKind.REORDER

    changed_control = replace(controls[0], control={"region": "changed"})
    controlled_plan = replace(
        source,
        steps=tuple(changed_control if step is controls[0] else step for step in source.steps),
    )
    controlled = aligner.align(source, controlled_plan).diff
    assert controlled.first_divergence is not None
    assert controlled.first_divergence.kind is DiffKind.CONTROL_FLOW_CHANGED

    second = next(step for step in source.steps if step.logical_step == "second")
    changed_second = replace(second, dependencies=("input",))
    topology_plan = replace(
        source,
        steps=tuple(changed_second if step is second else step for step in source.steps),
    )
    topology = aligner.align(source, topology_plan).diff
    assert topology.first_divergence is not None
    assert topology.first_divergence.kind is DiffKind.TOPOLOGY_CHANGED


def test_graph_diff_reports_resolvable_schema_and_digest_changes() -> None:
    source = compile_workflow(PairWorkflow())
    first = next(step for step in source.steps if step.logical_step == "first")
    changed = replace(
        first,
        module_digest="changed",
        output_schema_digest="changed-schema",
    )
    current = replace(
        source,
        steps=tuple(changed if step is first else step for step in source.steps),
    )
    diff = GraphAligner().align(source, current).diff
    assert diff.has_changes
    assert diff.aligned
    assert [change.kind for change in diff.changes] == [
        DiffKind.MODULE_DIGEST_CHANGED,
        DiffKind.SCHEMA_CHANGED,
    ]


def test_imported_plan_rejects_unknown_versions_duplicate_keys_and_broken_topology() -> None:
    source = compile_workflow(PairWorkflow())
    data = source.to_dict()

    legacy_identity = {**data, "version": "0.5.0"}
    with pytest.raises(ValueError, match="graph-independent module identity"):
        type(source).from_dict(legacy_identity)

    unsupported = {**data, "version": "9.9.9"}
    with pytest.raises(ValueError, match="unsupported Workflow IR"):
        type(source).from_dict(unsupported)

    steps = list(data["steps"])
    executable = [step for step in steps if step["module_id"] is not None]
    duplicate = {**executable[1], "module_id": executable[0]["module_id"]}
    duplicate["logical_step"] = executable[0]["logical_step"]
    duplicate_data = {
        **data,
        "steps": [*steps, {**duplicate, "node_id": "duplicate"}],
        "output_node": "duplicate",
    }
    with pytest.raises(ValueError, match="duplicate replay key"):
        type(source).from_dict(duplicate_data)

    broken = {**data, "steps": [{**steps[0], "dependencies": ["missing"]}, *steps[1:]]}
    with pytest.raises(ValueError, match="unknown or forward dependencies"):
        type(source).from_dict(broken)
