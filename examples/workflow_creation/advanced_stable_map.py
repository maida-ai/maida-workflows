"""Advanced: map over runtime data using stable domain keys for replay identity."""

from __future__ import annotations

from maida.workflows import ExecutionContext, Module, RuntimeValue, Workflow, map_over


class NormalizeDocument(Module[dict[str, str], str]):
    input_type = dict[str, str]
    output_type = str

    async def execute(self, value: dict[str, str], ctx: ExecutionContext) -> str:
        return " ".join(value["text"].split()).lower()


class JoinDocuments(Module[list[str], str]):
    input_type = list[str]
    output_type = str

    async def execute(self, value: list[str], ctx: ExecutionContext) -> str:
        return " | ".join(value)


class StableDocumentMapWorkflow(Workflow[list[dict[str, str]], str]):
    workflow_id = "onboarding-stable-map"
    input_type = list[dict[str, str]]
    output_type = str

    def __init__(self) -> None:
        self.normalize = NormalizeDocument()
        self.join = JoinDocuments()

    def build(self, value: RuntimeValue[list[dict[str, str]]]) -> RuntimeValue[str]:
        normalized = map_over(value, self.normalize, item_key="id")
        return self.join(normalized)


workflow = StableDocumentMapWorkflow()
EXAMPLE_INPUT = [
    {"id": "doc-b", "text": "  Beta "},
    {"id": "doc-a", "text": " ALPHA "},
]
EXPECTED_OUTPUT = "beta | alpha"
