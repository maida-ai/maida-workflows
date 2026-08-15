"""Advanced: register a reusable child workflow inside a parent workflow."""

from __future__ import annotations

from maida_workflows import ExecutionContext, Module, RuntimeValue, Workflow, parallel


class ExtractTitle(Module[str, str]):
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value.splitlines()[0].strip()


class CountWords(Module[str, int]):
    input_type = str
    output_type = int

    async def execute(self, value: str, ctx: ExecutionContext) -> int:
        return len(value.split())


class AnalysisWorkflow(Workflow[str, tuple[str, int]]):
    workflow_id = "onboarding-analysis"
    input_type = str
    output_type = tuple[str, int]

    def __init__(self) -> None:
        self.title = ExtractTitle()
        self.words = CountWords()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[tuple[str, int]]:
        return parallel(self.title(value), self.words(value))


class RenderReport(Module[tuple[str, int], str]):
    input_type = tuple[str, int]
    output_type = str

    async def execute(self, value: tuple[str, int], ctx: ExecutionContext) -> str:
        title, word_count = value
        return f"{title} ({word_count} words)"


class ReportWorkflow(Workflow[str, str]):
    workflow_id = "onboarding-nested"
    input_type = str
    output_type = str

    def __init__(self) -> None:
        self.analysis = AnalysisWorkflow()
        self.render = RenderReport()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        analysis = self.analysis(value)
        return self.render(analysis)


workflow = ReportWorkflow()
EXAMPLE_INPUT = "Reliable workflows\nmake changes reviewable"
EXPECTED_OUTPUT = "Reliable workflows (5 words)"
