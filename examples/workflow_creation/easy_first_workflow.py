"""Create a first workflow from one typed module.

This example mirrors the smallest PyTorch model: register a reusable component
on the parent object, then connect it to the symbolic input in ``build``.
"""

from __future__ import annotations

from maida.workflows import ExecutionContext, Module, RuntimeValue, Workflow


class Greet(Module[str, str]):
    """Format a concrete name as a friendly greeting."""

    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return f"Hello, {value}!"


class GreetingWorkflow(Workflow[str, str]):
    """Connect the root string input directly to :class:`Greet`."""

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
