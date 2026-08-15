"""Compose a two-stage workflow by chaining symbolic outputs.

``NormalizeName`` produces the exact typed input consumed by ``Greet``. The
workflow remains declarative: neither module executes while ``build`` runs.
"""

from __future__ import annotations

from maida.workflows import ExecutionContext, Module, RuntimeValue, Workflow


class NormalizeName(Module[str, str]):
    """Collapse whitespace and title-case a supplied name."""

    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return " ".join(value.split()).title()


class Greet(Module[str, str]):
    """Format a normalized name as a greeting."""

    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return f"Hello, {value}!"


class SequentialGreetingWorkflow(Workflow[str, str]):
    """Normalize a name and pass its symbolic output to the greeting module."""

    workflow_id = "onboarding-sequential"
    input_type = str
    output_type = str

    def __init__(self) -> None:
        self.normalize = NormalizeName()
        self.greet = Greet()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        normalized = self.normalize(value)
        return self.greet(normalized)


workflow = SequentialGreetingWorkflow()
EXAMPLE_INPUT = "  ADA LOVELACE "
EXPECTED_OUTPUT = "Hello, Ada Lovelace!"
