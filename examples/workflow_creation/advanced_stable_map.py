"""Map over runtime data using stable domain keys for execution identity.

Each document is addressed by its ``id`` field rather than list position. A
reordered input therefore keeps the same per-document step instance identities
while preserving the caller's output order.
"""

from __future__ import annotations

from maida.workflows import ExecutionContext, Module, RuntimeValue, Workflow, map_over


class NormalizeDocument(Module[dict[str, str], str]):
    """Normalize one keyed document to compact lowercase text."""

    input_type = dict[str, str]
    output_type = str

    async def execute(self, value: dict[str, str], ctx: ExecutionContext) -> str:
        return " ".join(value["text"].split()).lower()


class JoinDocuments(Module[list[str], str]):
    """Join normalized documents while preserving their runtime order."""

    input_type = list[str]
    output_type = str

    async def execute(self, value: list[str], ctx: ExecutionContext) -> str:
        return " | ".join(value)


class StableDocumentMapWorkflow(Workflow[list[dict[str, str]], str]):
    """Normalize a runtime document collection with ID-based map identity."""

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
