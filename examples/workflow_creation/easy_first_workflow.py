"""Easy: register one module and connect it to a workflow input."""

from __future__ import annotations

from maida_workflows import ExecutionContext, Module, RuntimeValue, Workflow


class Greet(Module[str, str]):
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return f"Hello, {value}!"


class GreetingWorkflow(Workflow[str, str]):
    workflow_id = "onboarding-first-workflow"
    input_type = str
    output_type = str

    def __init__(self) -> None:
        self.greet = Greet()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.greet(value)


workflow = GreetingWorkflow()
EXAMPLE_INPUT = "Ada"
EXPECTED_OUTPUT = "Hello, Ada!"
