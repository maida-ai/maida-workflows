from __future__ import annotations

from dataclasses import dataclass

import pytest

from maida_workflows import (
    CompileError,
    ExecutionContext,
    Module,
    RuntimeValue,
    Workflow,
    compile_workflow,
    map_over,
    parallel,
    when,
)


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

    assert first.version == "0.1.0"
    assert first.canonical_json() == second.canonical_json()
    assert first.digest == second.digest
    step = first.executable_steps[0]
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


def test_all_adversarial_workflows_compile_to_replay_addressable_ir() -> None:
    from examples.adversarial_workflows import ADVERSARIAL_WORKFLOWS

    plans = [compile_workflow(workflow) for workflow in ADVERSARIAL_WORKFLOWS]
    assert all(plan.executable_steps for plan in plans)
    assert all(step.replay_key is not None for plan in plans for step in plan.executable_steps)
    assert any(step.kind == "map_module" for step in plans[1].steps)
    assert plans[2].executable_steps[-1].logical_step == "audit-effect"
