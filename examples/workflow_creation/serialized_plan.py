"""Serialize the canonical PlanIR emitted by ordinary workflow authoring."""

from __future__ import annotations

from pathlib import Path

from maida.workflows import (
    ExecutionContext,
    Module,
    ModuleRegistry,
    RunResult,
    RuntimeValue,
    Workflow,
    WorkflowBundle,
    WorkflowRunner,
    compile_workflow,
)
from maida.workflows.persistence import PostgresStore


class _Title(Module[str, str]):
    module_id = "demo.title"
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value.strip().title()


class _Prefix(Module[str, str]):
    module_id = "demo.prefix"
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return f"[portable] {value}"


registry = ModuleRegistry(
    modules={"text.prefix": _Prefix, "text.title": _Title},
)


class _Onboarding(Workflow[str, str]):
    workflow_id = "onboarding-portable"
    input_type = str
    output_type = str
    title = _Title()
    prefix = _Prefix()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.prefix.at("prefix")(self.title.at("title")(value))


plan = compile_workflow(_Onboarding())
bundle = WorkflowBundle.from_plan(plan, registry)
workflow = bundle.bind(module_registry=registry)

EXAMPLE_INPUT = "  hello ada  "
EXPECTED_OUTPUT = "[portable] Hello Ada"


def save_and_restore(path: Path) -> WorkflowBundle:
    """Save canonical data privately, reload it, and verify trusted rebinding."""
    bundle.save(path)
    restored = WorkflowBundle.load(path)
    restored.bind(module_registry=registry)
    return restored


async def run_example(
    store: PostgresStore,
    value: str = EXAMPLE_INPUT,
) -> RunResult:
    """Execute the registry-bound plan through the ordinary durable runner."""
    return await WorkflowRunner(store).run(workflow, value)
