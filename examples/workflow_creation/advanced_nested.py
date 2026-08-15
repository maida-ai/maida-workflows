"""Compose a reusable child workflow inside a parent workflow.

``AnalysisWorkflow`` owns two independent analysis modules. The parent treats
that child like a typed component and passes its symbolic tuple output to the
final renderer.
"""

from __future__ import annotations

from maida.workflows import ExecutionContext, Module, RuntimeValue, Workflow, parallel


class ExtractTitle(Module[str, str]):
    """Extract and trim the first line of a document."""

    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value.splitlines()[0].strip()


class CountWords(Module[str, int]):
    """Count whitespace-delimited words in the complete document."""

    input_type = str
    output_type = int

    async def execute(self, value: str, ctx: ExecutionContext) -> int:
        return len(value.split())


class AnalysisWorkflow(Workflow[str, tuple[str, int]]):
    """Produce a title and word count as a typed parallel result."""

    workflow_id = "onboarding-analysis"
    input_type = str
    output_type = tuple[str, int]

    def __init__(self) -> None:
        self.title = ExtractTitle()
        self.words = CountWords()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[tuple[str, int]]:
        return parallel(self.title(value), self.words(value))


class RenderReport(Module[tuple[str, int], str]):
    """Render title and word-count analysis as a compact report."""

    input_type = tuple[str, int]
    output_type = str

    async def execute(self, value: tuple[str, int], ctx: ExecutionContext) -> str:
        title, word_count = value
        return f"{title} ({word_count} words)"


class ReportWorkflow(Workflow[str, str]):
    """Compose :class:`AnalysisWorkflow` with the final report renderer."""

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
