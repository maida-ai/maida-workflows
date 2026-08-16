from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, replace
from typing import Any

import pytest

from maida.workflows import (
    ApprovalDecision,
    BindingSpec,
    Capability,
    EffectSpec,
    ExecutionContext,
    Module,
    ModuleRegistry,
    NodeSpec,
    WorkflowSpec,
    WorkflowSpecError,
    compile_workflow_spec,
)
from maida.workflows._canonical import type_schema


class Echo(Module[str, str]):
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value


class Length(Module[str, int]):
    input_type = str
    output_type = int

    async def execute(self, value: str, ctx: ExecutionContext) -> int:
        return len(value)


class Positive(Module[int, bool]):
    input_type = int
    output_type = bool

    async def execute(self, value: int, ctx: ExecutionContext) -> bool:
        return value > 0


@dataclass(frozen=True)
class StructuredInput:
    names: list[str]
    pair: tuple[int, int]
    label: str


class Structured(Module[StructuredInput, str]):
    input_type = StructuredInput
    output_type = str

    async def execute(self, value: StructuredInput, ctx: ExecutionContext) -> str:
        return value.label


READ = Capability("record.read", "records", "read", str, str)
WRITE = EffectSpec("record.write", "records", "write", str, str)


class AccessModule(Echo):
    capabilities = (READ,)
    effects = (WRITE,)
    effectful = True


def registry() -> ModuleRegistry:
    return ModuleRegistry(
        modules={
            "text.echo": Echo,
            "text.length": Length,
            "number.positive": Positive,
            "data.structured": Structured,
            "record.access": AccessModule,
        }
    )


def one_node_spec(
    node: NodeSpec,
    *,
    input_schema: dict[str, Any] | None = None,
    output_schema: dict[str, Any] | None = None,
    capabilities: tuple[str, ...] = (),
    effects: tuple[str, ...] = (),
) -> WorkflowSpec:
    return WorkflowSpec(
        workflow_id="contract-spec",
        input_schema=input_schema or type_schema(str),
        output_schema=output_schema or type_schema(str),
        nodes=(node,),
        output=BindingSpec.node(node.key),
        capabilities=capabilities,
        effects=effects,
    )


@pytest.mark.parametrize(
    ("factory", "message"),
    (
        (lambda: BindingSpec("unknown"), "unsupported"),
        (lambda: BindingSpec("root", source="node"), "cannot declare a source"),
        (lambda: BindingSpec("root", path=("",)), "non-empty"),
        (lambda: BindingSpec("literal", path=("value",)), "cannot project"),
        (lambda: BindingSpec("root", value=1), "cannot contain a literal"),
        (
            lambda: BindingSpec("object", fields=(("", BindingSpec.root()),)),
            "field names",
        ),
        (
            lambda: BindingSpec(
                "object",
                fields=(("name", BindingSpec.root()), ("name", BindingSpec.root())),
            ),
            "unique",
        ),
        (
            lambda: BindingSpec("root", fields=(("name", BindingSpec.root()),)),
            "cannot contain object fields",
        ),
        (lambda: BindingSpec("root", items=(BindingSpec.root(),)), "sequence items"),
        (lambda: BindingSpec.root("bad..path"), "dot-separated"),
    ),
)
def test_binding_specs_reject_ambiguous_shapes(
    factory: Callable[[], BindingSpec], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        factory()


def test_binding_variants_round_trip_and_report_node_sources() -> None:
    binding = BindingSpec.object(
        literal=BindingSpec.literal("safe"),
        root=BindingSpec.root("name"),
        sequence=BindingSpec.list(
            BindingSpec.node("first"),
            BindingSpec.tuple(BindingSpec.node("second"), BindingSpec.node("first")),
        ),
    )

    restored = BindingSpec.from_dict(binding.to_dict())

    assert restored == binding
    assert restored.node_sources == ("first", "second")


@pytest.mark.parametrize(
    ("data", "message"),
    (
        ([], "must be an object"),
        ({"kind": "bad"}, "kind is invalid"),
        ({"kind": "root"}, "fields do not match"),
        ({"kind": "root", "path": "name"}, "array of strings"),
        ({"kind": "object", "fields": {}}, "must be an array"),
        ({"kind": "object", "fields": [1]}, "field is invalid"),
        (
            {"kind": "object", "fields": [{"name": 1, "binding": {}}]},
            "name must be a string",
        ),
        (
            {"kind": "object", "fields": [{"name": "x", "binding": 1}]},
            "child must be an object",
        ),
        ({"kind": "list", "items": [1]}, "items must be objects"),
        (
            {
                "kind": "object",
                "fields": [
                    {"name": "z", "binding": {"kind": "root", "path": []}},
                    {"name": "a", "binding": {"kind": "root", "path": []}},
                ],
            },
            "not canonical",
        ),
    ),
)
def test_binding_import_is_strict(data: Any, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        BindingSpec.from_dict(data)


@pytest.mark.parametrize(
    ("factory", "message"),
    (
        (lambda: NodeSpec("step", "unknown"), "unsupported node kind"),
        (lambda: NodeSpec("step", "module", module="text.echo"), "input binding"),
        (
            lambda: NodeSpec.task(
                "step",
                "text.echo",
                BindingSpec.root(),
                item_key="id",  # type: ignore[call-arg]
            ),
            "unexpected keyword",
        ),
        (
            lambda: NodeSpec(
                "step",
                "module",
                module="text.echo",
                input=BindingSpec.root(),
                condition="check",
            ),
            "branch targets",
        ),
        (
            lambda: NodeSpec(
                "step", "map", module="text.echo", input=BindingSpec.root(), item_key=""
            ),
            "stable item_key",
        ),
        (
            lambda: NodeSpec(
                "step",
                "module",
                module="text.echo",
                input=BindingSpec.root(),
                item_key="id",
            ),
            "cannot declare item_key",
        ),
        (
            lambda: NodeSpec(
                "step",
                "module",
                module="text.echo",
                input=BindingSpec.root(),
                workflow=one_node_spec(NodeSpec.task("child", "text.echo", BindingSpec.root())),
            ),
            "nested workflow",
        ),
        (
            lambda: NodeSpec(
                "step",
                "module",
                module="text.echo",
                input=BindingSpec.root(),
                prompt="not allowed",
            ),
            "interaction fields",
        ),
        (
            lambda: NodeSpec(
                "choice", "branch", module="text.echo", condition="a", then="b", otherwise="c"
            ),
            "module fields",
        ),
        (
            lambda: NodeSpec(
                "choice",
                "branch",
                condition="a",
                then="b",
                otherwise="c",
                after=("a",),
            ),
            "module configuration",
        ),
        (
            lambda: NodeSpec(
                "choice",
                "branch",
                condition="a",
                then="b",
                otherwise="c",
                workflow=one_node_spec(NodeSpec.task("child", "text.echo", BindingSpec.root())),
            ),
            "nested workflow",
        ),
        (
            lambda: NodeSpec(
                "choice",
                "branch",
                condition="a",
                then="b",
                otherwise="c",
                prompt="not allowed",
            ),
            "interaction fields",
        ),
        (lambda: NodeSpec("child", "nested"), "requires a WorkflowSpec"),
        (
            lambda: NodeSpec(
                "child",
                "nested",
                workflow=one_node_spec(NodeSpec.task("task", "text.echo", BindingSpec.root())),
            ),
            "input binding",
        ),
        (
            lambda: NodeSpec.nested(
                "child",
                one_node_spec(NodeSpec.task("task", "text.echo", BindingSpec.root())),
                BindingSpec.root(),
                after=("not valid",),
            ),
            "stable identifier",
        ),
        (
            lambda: NodeSpec(
                "child",
                "nested",
                workflow=one_node_spec(NodeSpec.task("task", "text.echo", BindingSpec.root())),
                input=BindingSpec.root(),
                module="text.echo",
            ),
            "module or branch fields",
        ),
        (
            lambda: NodeSpec(
                "child",
                "nested",
                workflow=one_node_spec(NodeSpec.task("task", "text.echo", BindingSpec.root())),
                input=BindingSpec.root(),
                config={"value": 1},
            ),
            "module config",
        ),
        (
            lambda: NodeSpec(
                "child",
                "nested",
                workflow=one_node_spec(NodeSpec.task("task", "text.echo", BindingSpec.root())),
                input=BindingSpec.root(),
                prompt="not allowed",
            ),
            "interaction fields",
        ),
        (lambda: NodeSpec("approval", "approval", prompt="review"), "input binding"),
        (
            lambda: NodeSpec("approval", "approval", input=BindingSpec.root(), prompt=""),
            "non-empty prompt",
        ),
        (
            lambda: NodeSpec(
                "approval",
                "approval",
                input=BindingSpec.root(),
                prompt="review",
                module="text.echo",
            ),
            "module or control fields",
        ),
        (
            lambda: NodeSpec(
                "approval",
                "approval",
                input=BindingSpec.root(),
                prompt="review",
                response_schema=type_schema(str),
            ),
            "fixed decision schema",
        ),
        (
            lambda: NodeSpec("input", "input", input=BindingSpec.root(), prompt="answer"),
            "response schema",
        ),
        (
            lambda: NodeSpec(
                "signal",
                "signal",
                input=BindingSpec.root(),
                prompt="wait",
                response_schema=type_schema(str),
                signal_name="",
            ),
            "signal name",
        ),
        (
            lambda: NodeSpec(
                "input",
                "input",
                input=BindingSpec.root(),
                prompt="answer",
                response_schema=type_schema(str),
                signal_name="wrong",
            ),
            "cannot declare a signal",
        ),
        (
            lambda: NodeSpec.task("step", "text.echo", BindingSpec.root(), logical_step=" "),
            "logical_step",
        ),
    ),
)
def test_node_specs_reject_kind_specific_field_leaks(
    factory: Callable[[], NodeSpec], message: str
) -> None:
    if message == "unexpected keyword":
        with pytest.raises(TypeError):
            factory()
    else:
        with pytest.raises(ValueError, match=message):
            factory()


def test_interaction_nodes_round_trip_and_canonicalize_dependencies() -> None:
    nodes = (
        NodeSpec.approval(
            "approval",
            BindingSpec.root(),
            prompt="Review",
            metadata={"screen": "review"},
            after=("prior", "prior"),
        ),
        NodeSpec.request_input(
            "input",
            BindingSpec.node("approval"),
            response_schema=type_schema(str),
            prompt="Explain",
        ),
        NodeSpec.wait_for_signal(
            "signal",
            BindingSpec.node("input"),
            payload_schema=type_schema(str),
            name="continue",
        ),
    )

    assert nodes[0].after == ("prior",)
    assert [NodeSpec.from_dict(node.to_dict()) for node in nodes] == list(nodes)
    assert nodes[2].prompt == "Wait for signal continue"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("input", [], "input must be an object"),
        ("after", "step", "after must be an array"),
        ("config", [], "config must be an object"),
        ("workflow", [], "nested workflow must be an object"),
        ("response_schema", [], "response schema"),
        ("metadata", [], "metadata must be an object"),
    ),
)
def test_node_import_rejects_untrusted_nested_types(field: str, value: Any, message: str) -> None:
    data = NodeSpec.task("step", "text.echo", BindingSpec.root()).to_dict()
    data[field] = value

    with pytest.raises(ValueError, match=message):
        NodeSpec.from_dict(data)


def test_node_import_rejects_unknown_and_noncanonical_fields() -> None:
    data = NodeSpec.task("step", "text.echo", BindingSpec.root(), after=("a", "b")).to_dict()
    with pytest.raises(ValueError, match="fields"):
        NodeSpec.from_dict({"key": "step"})

    data["after"] = ["b", "a"]
    with pytest.raises(ValueError, match="not canonical"):
        NodeSpec.from_dict(data)


def test_workflow_spec_constructor_and_import_are_strict() -> None:
    node = NodeSpec.task("step", "text.echo", BindingSpec.root())
    valid = one_node_spec(node)

    invalid_factories: tuple[tuple[Callable[[], WorkflowSpec], str], ...] = (
        (
            lambda: WorkflowSpec(
                "contract-spec",
                type_schema(str),
                type_schema(str),
                (node,),
                BindingSpec.root(),
            ),
            "directly reference",
        ),
        (
            lambda: WorkflowSpec(
                "contract-spec",
                type_schema(str),
                type_schema(str),
                (node, node),
                BindingSpec.node("step"),
            ),
            "unique",
        ),
        (lambda: replace(valid, version="9"), "version"),
    )
    for factory, message in invalid_factories:
        with pytest.raises(ValueError, match=message):
            factory()

    data = valid.to_dict()
    with pytest.raises(ValueError, match="fields"):
        WorkflowSpec.from_dict({"format": "maida-workflow-spec"})
    for field, value, message in (
        ("format", "wrong", "format"),
        ("nodes", {}, "array of objects"),
        ("output", [], "output must be an object"),
        ("capabilities", {}, "must be arrays"),
        ("effects", {}, "must be arrays"),
    ):
        changed = deepcopy(data)
        changed[field] = value
        with pytest.raises(ValueError, match=message):
            WorkflowSpec.from_dict(changed)

    changed = deepcopy(data)
    changed["capabilities"] = ["z", "a"]
    with pytest.raises(ValueError, match="not canonical"):
        WorkflowSpec.from_dict(changed)


def test_workflow_compilation_reports_topology_and_schema_failures() -> None:
    specs = (
        WorkflowSpec(
            "unknown-dependency",
            type_schema(str),
            type_schema(str),
            (NodeSpec.task("step", "text.echo", BindingSpec.node("missing")),),
            BindingSpec.node("step"),
        ),
        WorkflowSpec(
            "invalid-output",
            type_schema(str),
            type_schema(str),
            (NodeSpec.task("step", "text.echo", BindingSpec.root()),),
            BindingSpec.node("missing"),
        ),
        WorkflowSpec(
            "unreachable",
            type_schema(str),
            type_schema(str),
            (
                NodeSpec.task("used", "text.echo", BindingSpec.root()),
                NodeSpec.task("dead", "text.echo", BindingSpec.root()),
            ),
            BindingSpec.node("used"),
        ),
        one_node_spec(
            NodeSpec.map("mapped", "text.echo", BindingSpec.root(), item_key="id"),
            input_schema={"type": "array", "items": type_schema(int)},
            output_schema={"type": "array", "items": type_schema(str)},
        ),
        one_node_spec(
            NodeSpec.task("step", "text.echo", BindingSpec.root("missing")),
            input_schema={
                "type": "object",
                "properties": {"known": type_schema(str)},
                "required": ["known"],
                "additionalProperties": False,
            },
        ),
    )

    codes = [
        {issue.code for issue in compile_workflow_spec(spec, registry()).issues} for spec in specs
    ]

    assert "UNKNOWN_DEPENDENCY" in codes[0]
    assert "INVALID_OUTPUT" in codes[1]
    assert "UNREACHABLE_NODE" in codes[2]
    assert "SCHEMA_MISMATCH" in codes[3]
    assert "UNKNOWN_FIELD" in codes[4]


def test_workflow_compilation_enforces_access_and_replay_identity() -> None:
    denied = one_node_spec(NodeSpec.task("access", "record.access", BindingSpec.root()))
    allowed = one_node_spec(
        NodeSpec.task("access", "record.access", BindingSpec.root()),
        capabilities=(READ.name,),
        effects=(WRITE.name,),
    )
    duplicate = WorkflowSpec(
        "duplicate-replay",
        type_schema(str),
        type_schema(str),
        (
            NodeSpec.task(
                "first",
                "text.echo",
                BindingSpec.root(),
                module_id="same.module",
                logical_step="same-step",
            ),
            NodeSpec.task(
                "second",
                "text.echo",
                BindingSpec.node("first"),
                module_id="same.module",
                logical_step="same-step",
            ),
        ),
        BindingSpec.node("second"),
    )

    denied_codes = {issue.code for issue in compile_workflow_spec(denied, registry()).issues}
    assert denied_codes == {"CAPABILITY_DENIED", "EFFECT_DENIED"}
    assert compile_workflow_spec(allowed, registry()).ok
    assert "DUPLICATE_REPLAY_KEY" in {
        issue.code for issue in compile_workflow_spec(duplicate, registry()).issues
    }


def test_structured_and_interaction_bindings_compile_to_explainable_ir() -> None:
    structured = one_node_spec(
        NodeSpec.task(
            "structured",
            "data.structured",
            BindingSpec.object(
                names=BindingSpec.list(BindingSpec.root()),
                pair=BindingSpec.tuple(BindingSpec.literal(1), BindingSpec.literal(2)),
                label=BindingSpec.literal("ready"),
            ),
        )
    )
    interactions = (
        one_node_spec(
            NodeSpec.approval("review", BindingSpec.root(), prompt="Review"),
            output_schema=type_schema(ApprovalDecision),
        ),
        one_node_spec(
            NodeSpec.request_input(
                "answer",
                BindingSpec.root(),
                response_schema=type_schema(str),
                prompt="Answer",
            )
        ),
        one_node_spec(
            NodeSpec.wait_for_signal(
                "signal",
                BindingSpec.root(),
                payload_schema=type_schema(str),
                name="continue",
            )
        ),
    )

    compiled = compile_workflow_spec(structured, registry())
    assert compiled.ok
    binding = compiled.plan.executable_steps[0].input_binding  # type: ignore[union-attr]
    assert binding is not None and binding.kind == "object"
    for spec in interactions:
        result = compile_workflow_spec(spec, registry())
        assert result.ok
        assert result.plan is not None
        assert result.plan.executable_steps[0].control is not None


def test_branch_and_nested_compilation_failures_are_location_aware() -> None:
    bad_condition = WorkflowSpec(
        "bad-condition",
        type_schema(str),
        type_schema(str),
        (
            NodeSpec.task("condition", "text.echo", BindingSpec.root()),
            NodeSpec.task("then", "text.echo", BindingSpec.root()),
            NodeSpec.task("otherwise", "text.echo", BindingSpec.root()),
            NodeSpec.branch("choice", "condition", "then", "otherwise"),
        ),
        BindingSpec.node("choice"),
    )
    bad_branches = WorkflowSpec(
        "bad-branches",
        type_schema(str),
        type_schema(str),
        (
            NodeSpec.task("length", "text.length", BindingSpec.root()),
            NodeSpec.task("condition", "number.positive", BindingSpec.node("length")),
            NodeSpec.task("text", "text.echo", BindingSpec.root()),
            NodeSpec.branch("choice", "condition", "length", "text"),
        ),
        BindingSpec.node("choice"),
    )
    child = one_node_spec(
        NodeSpec.task("access", "record.access", BindingSpec.root()),
        capabilities=(READ.name,),
        effects=(WRITE.name,),
    )
    denied_nested = one_node_spec(NodeSpec.nested("child", child, BindingSpec.root()))
    mismatched_nested = one_node_spec(
        NodeSpec.nested(
            "child",
            one_node_spec(NodeSpec.task("echo", "text.echo", BindingSpec.root())),
            BindingSpec.root(),
        ),
        input_schema=type_schema(int),
    )
    invalid_child = one_node_spec(
        NodeSpec.nested(
            "child",
            one_node_spec(NodeSpec.task("missing", "unknown.module", BindingSpec.root())),
            BindingSpec.root(),
        )
    )

    results = [
        compile_workflow_spec(spec, registry())
        for spec in (
            bad_condition,
            bad_branches,
            denied_nested,
            mismatched_nested,
            invalid_child,
        )
    ]

    assert "SCHEMA_MISMATCH" in {issue.code for issue in results[0].issues}
    assert "SCHEMA_MISMATCH" in {issue.code for issue in results[1].issues}
    assert "ACCESS_DENIED" in {issue.code for issue in results[2].issues}
    assert "SCHEMA_MISMATCH" in {issue.code for issue in results[3].issues}
    assert "UNKNOWN_MODULE" in {issue.code for issue in results[4].issues}
    with pytest.raises(WorkflowSpecError, match="SCHEMA_MISMATCH"):
        results[0].raise_for_errors()


def test_sensitive_literals_and_metadata_are_rejected_without_echoing_values() -> None:
    specs = (
        one_node_spec(
            NodeSpec.task(
                "step",
                "text.echo",
                BindingSpec.root(),
                config={"nested": [{"password": "do-not-log"}]},
            )
        ),
        one_node_spec(
            NodeSpec.approval(
                "review",
                BindingSpec.root(),
                prompt="Review",
                metadata={"api-key": "do-not-log"},
            ),
            output_schema=type_schema(ApprovalDecision),
        ),
        one_node_spec(
            NodeSpec.task(
                "step",
                "text.echo",
                BindingSpec.object(value=BindingSpec.literal({"token": "do-not-log"})),
            )
        ),
    )

    for spec in specs:
        result = compile_workflow_spec(spec, registry())
        assert {issue.code for issue in result.issues} == {"SECRET_LITERAL"}
        assert "do-not-log" not in str(result.issues)
