from __future__ import annotations

from dataclasses import dataclass

import pytest

from maida.workflows import (
    CompileError,
    ExecutionContext,
    Module,
    RuntimeValue,
    SymbolicValueError,
    Workflow,
    compile_workflow,
    map_over,
    parallel,
    when,
)
from maida.workflows.alignment import DiffKind, GraphAligner


class AddOne(Module[int, int]):
    input_type = int
    output_type = int

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return value + 1


class IsPositive(Module[int, bool]):
    input_type = int
    output_type = bool

    async def execute(self, value: int, ctx: ExecutionContext) -> bool:
        return value > 0


class Identity(Module[int, int]):
    input_type = int
    output_type = int

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return value


class Simple(Workflow[int, int]):
    workflow_id = "simple"
    input_type = int
    output_type = int
    add = AddOne()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.add(value)


def test_default_identity_and_canonical_ir_are_stable() -> None:
    first = compile_workflow(Simple())
    second = compile_workflow(Simple())

    assert first.version == "0.2.0"
    assert first.canonical_json() == second.canonical_json()
    assert first.digest == second.digest
    assert first.digest == "8783a3d322b3edd68ee32edbcb527c8ef1a42a1b0d3084395a6742671f0c3995"
    step = first.executable_steps[0]
    assert (
        step.definition_digest == "15894aeadf64d4ab56280ffdab3b3a658de429c63e0d2a9ef408ea2abb6741da"
    )
    assert step.module_id == "simple.add"
    assert step.logical_step == "root"
    assert step.replay_key is not None


def test_explicit_identity_is_stable_while_behavior_digest_changes() -> None:
    class Changed(AddOne):
        module_id = "shared.math"

        async def execute(self, value: int, ctx: ExecutionContext) -> int:
            return value + 2

    class OriginalWorkflow(Workflow[int, int]):
        workflow_id = "original"
        input_type = int
        output_type = int
        add = AddOne()

        def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
            return self.add.at("calculate")(value)

    class ChangedWorkflow(Workflow[int, int]):
        workflow_id = "changed"
        input_type = int
        output_type = int
        add = Changed()

        def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
            return self.add.at("calculate")(value)

    OriginalWorkflow.add.module_id = "shared.math"

    old = compile_workflow(OriginalWorkflow()).executable_steps[0]
    new = compile_workflow(ChangedWorkflow()).executable_steps[0]

    assert old.replay_key == new.replay_key
    assert old.module_digest != new.module_digest
    assert old.definition_digest != new.definition_digest


def test_reused_module_requires_explicit_occurrence_identity() -> None:
    class Ambiguous(Workflow[int, int]):
        workflow_id = "ambiguous"
        input_type = int
        output_type = int
        shared = AddOne()

        def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
            return self.shared(self.shared(value))

    with pytest.raises(CompileError, match="reused module"):
        compile_workflow(Ambiguous())


def test_explicit_reuse_and_duplicate_replay_key_validation() -> None:
    class Reused(Workflow[int, int]):
        workflow_id = "reused"
        input_type = int
        output_type = int
        shared = AddOne()

        def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
            return self.shared.at("second")(self.shared.at("first")(value))

    plan = compile_workflow(Reused())
    assert {step.logical_step for step in plan.executable_steps} == {"first", "second"}

    class Duplicate(Reused):
        def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
            return self.shared.at("same")(self.shared.at("same")(value))

    with pytest.raises(CompileError, match="duplicate replay key"):
        compile_workflow(Duplicate())

    class Mixed(Reused):
        def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
            return self.shared.at("second")(self.shared(value))

    with pytest.raises(CompileError, match="every occurrence"):
        compile_workflow(Mixed())


@dataclass(frozen=True)
class Item:
    item_id: str
    value: int


class ReadItem(Module[Item, int]):
    input_type = Item
    output_type = int

    async def execute(self, value: Item, ctx: ExecutionContext) -> int:
        return value.value


def test_map_requires_and_records_stable_item_key() -> None:
    class Mapped(Workflow[list[Item], list[int]]):
        workflow_id = "mapped"
        input_type = list[Item]
        output_type = list[int]
        read = ReadItem()

        def build(self, value: RuntimeValue[list[Item]]) -> RuntimeValue[list[int]]:
            return map_over(value, self.read, item_key="item_id")

    step = compile_workflow(Mapped()).executable_steps[0]
    assert step.kind == "map_module"
    assert step.control == {"region": "map", "item_key": {"field": "item_id"}}

    with pytest.raises(ValueError, match="non-empty"):
        map_over(RuntimeValue.input(list[Item]), ReadItem(), item_key="")
    with pytest.raises(ValueError, match="not a field"):
        map_over(RuntimeValue.input(list[Item]), ReadItem(), item_key="missing")


def test_branch_parallel_and_nested_workflows_are_replay_addressable() -> None:
    class Child(Workflow[int, int]):
        workflow_id = "child"
        input_type = int
        output_type = int
        identity = Identity()

        def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
            return self.identity(value)

    class Parent(Workflow[int, tuple[int, int]]):
        workflow_id = "parent"
        input_type = int
        output_type = tuple[int, int]
        check = IsPositive()
        left = AddOne()
        right = Identity()
        child = Child()

        def build(self, value: RuntimeValue[int]) -> RuntimeValue[tuple[int, int]]:
            decision = self.check(value)
            branch = when(decision, self.left(value), self.right(value))
            return parallel(branch, self.child(value))

    plan = compile_workflow(Parent())
    keys = {step.replay_key.as_string() for step in plan.executable_steps if step.replay_key}

    assert keys == {
        "parent.check@root.dep0.dep0",
        "parent.left@root.dep0.dep1",
        "parent.right@root.dep0.dep2",
        "child.identity@root.dep1.nested[child]",
    }
    assert any(step.kind == "when" for step in plan.steps)
    assert plan.steps[-1].kind == "parallel"


def test_invalid_control_and_workflow_contracts_fail_early() -> None:
    with pytest.raises(TypeError, match="when condition"):
        when(RuntimeValue.input(int), RuntimeValue.input(int), RuntimeValue.input(int))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="same output type"):
        when(RuntimeValue.input(bool), RuntimeValue.input(int), RuntimeValue.input(str))
    with pytest.raises(ValueError, match="at least one"):
        parallel()

    class WrongOutput(Workflow[int, str]):
        workflow_id = "wrong-output"
        input_type = int
        output_type = str
        add = AddOne()

        def build(self, value: RuntimeValue[int]) -> RuntimeValue[str]:
            return self.add(value)  # type: ignore[return-value]

    with pytest.raises(CompileError, match="declares output"):
        compile_workflow(WrongOutput())

    class InvalidChild(Workflow[int, str]):
        workflow_id = "invalid-child"
        input_type = int
        output_type = str
        add = AddOne()

        def build(self, value: RuntimeValue[int]) -> RuntimeValue[str]:
            return self.add(value)  # type: ignore[return-value]

    class Parent(Workflow[int, str]):
        workflow_id = "parent-with-invalid-child"
        input_type = int
        output_type = str
        child = InvalidChild()

        def build(self, value: RuntimeValue[int]) -> RuntimeValue[str]:
            return self.child(value)

    with pytest.raises(TypeError, match=r"invalid-child.*output contract"):
        compile_workflow(Parent())


def test_runtime_values_reject_ordinary_python_control_flow() -> None:
    value = RuntimeValue.input(int)

    with pytest.raises(SymbolicValueError, match="when"):
        bool(value)
    with pytest.raises(SymbolicValueError, match="map_over"):
        iter(value)
    with pytest.raises(SymbolicValueError, match="map_over"):
        len(value)


def test_incompatible_module_handoff_fails_during_graph_construction() -> None:
    class AsText(Module[int, str]):
        input_type = int
        output_type = str

        async def execute(self, value: int, ctx: ExecutionContext) -> str:
            return str(value)

    with pytest.raises(TypeError, match="input contract"):
        AddOne()(AsText()(RuntimeValue.input(int)))  # type: ignore[arg-type]


def test_class_level_behavior_declarations_change_module_digest() -> None:
    class Prompted(AddOne):
        prompt = "version one"
        offset = 0

    class PromptedWorkflow(Workflow[int, int]):
        workflow_id = "prompted"
        input_type = int
        output_type = int

        def __init__(self) -> None:
            self.module = Prompted()

        def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
            return self.module(value)

    workflow = PromptedWorkflow()
    original = compile_workflow(workflow).executable_steps[0].module_digest
    Prompted.prompt = "version two"
    changed = compile_workflow(workflow).executable_steps[0].module_digest

    assert original != changed

    workflow.module.offset = 1
    configured = compile_workflow(workflow).executable_steps[0].module_digest
    workflow.module.offset = 2
    reconfigured = compile_workflow(workflow).executable_steps[0].module_digest

    assert configured != reconfigured


def test_map_item_identity_changes_are_structural_divergences() -> None:
    class Mapped(Workflow[list[Item], list[int]]):
        workflow_id = "map-control-diff"
        input_type = list[Item]
        output_type = list[int]

        def __init__(self, item_key: str) -> None:
            self.item_key = item_key
            self.read = ReadItem()

        def build(self, value: RuntimeValue[list[Item]]) -> RuntimeValue[list[int]]:
            return map_over(value, self.read, item_key=self.item_key)

    diff = (
        GraphAligner()
        .align(
            compile_workflow(Mapped("item_id")),
            compile_workflow(Mapped("value")),
        )
        .diff
    )

    assert diff.first_divergence is not None
    assert diff.first_divergence.kind is DiffKind.CONTROL_FLOW_CHANGED


def test_all_adversarial_workflows_compile_to_replay_addressable_ir() -> None:
    from examples.adversarial_workflows import ADVERSARIAL_WORKFLOWS

    plans = [compile_workflow(workflow) for workflow in ADVERSARIAL_WORKFLOWS]
    assert all(plan.executable_steps for plan in plans)
    assert all(step.replay_key is not None for plan in plans for step in plan.executable_steps)
    assert any(step.kind == "map_module" for step in plans[1].steps)
    assert plans[2].executable_steps[-1].logical_step == "audit-effect"
