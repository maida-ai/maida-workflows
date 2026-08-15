"""Combine stable maps, parallel joins, branches, nesting, and replay keys.

The workflow demonstrates a larger static graph while remaining deterministic
and offline. Explicit ``.at(...)`` positions make reused or semantically named
module occurrences easy to align across definition changes.
"""

from __future__ import annotations

from maida.workflows import (
    ExecutionContext,
    Module,
    RuntimeValue,
    Workflow,
    map_over,
    parallel,
    when,
)


class ReviewDocument(Module[dict[str, str], str]):
    """Normalize one document and label it as flagged or acceptable."""

    input_type = dict[str, str]
    output_type = str

    async def execute(self, value: dict[str, str], ctx: ExecutionContext) -> str:
        text = " ".join(value["text"].split()).lower()
        verdict = "flag" if "refund" in text else "ok"
        return f"{verdict}:{text}"


class CountFlags(Module[list[str], int]):
    """Count flagged reviews in a mapped result collection."""

    input_type = list[str]
    output_type = int

    async def execute(self, value: list[str], ctx: ExecutionContext) -> int:
        return sum(review.startswith("flag:") for review in value)


class SummarizeReviews(Module[list[str], str]):
    """Join mapped document reviews into one deterministic summary."""

    input_type = list[str]
    output_type = str

    async def execute(self, value: list[str], ctx: ExecutionContext) -> str:
        return " | ".join(value)


class NeedsEscalation(Module[tuple[int, str], bool]):
    """Return whether the joined review context contains any flags."""

    input_type = tuple[int, str]
    output_type = bool

    async def execute(self, value: tuple[int, str], ctx: ExecutionContext) -> bool:
        flag_count, _ = value
        return flag_count > 0


class BuildEscalation(Module[tuple[int, str], str]):
    """Render the human-escalation branch from review context."""

    input_type = tuple[int, str]
    output_type = str

    async def execute(self, value: tuple[int, str], ctx: ExecutionContext) -> str:
        flag_count, summary = value
        noun = "flag" if flag_count == 1 else "flags"
        return f"ESCALATE ({flag_count} {noun}): {summary}"


class BuildAutomaticResult(Module[tuple[int, str], str]):
    """Render the automatic branch when no escalation is required."""

    input_type = tuple[int, str]
    output_type = str

    async def execute(self, value: tuple[int, str], ctx: ExecutionContext) -> str:
        flag_count, summary = value
        return f"AUTO ({flag_count} flags): {summary}"


class MarkReady(Module[str, str]):
    """Mark a routed report as ready for delivery."""

    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return f"READY: {value}"


class DeliveryWorkflow(Workflow[str, str]):
    """Reusable child workflow for the final delivery marker."""

    workflow_id = "onboarding-delivery"
    input_type = str
    output_type = str

    def __init__(self) -> None:
        self.mark_ready = MarkReady()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.mark_ready.at("mark-ready")(value)


class PolishReport(Module[str, str]):
    """Normalize report whitespace at two explicitly named occurrences."""

    module_id = "onboarding.report-polish"
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return " ".join(value.split())


class ReplayReadyReviewWorkflow(Workflow[list[dict[str, str]], str]):
    """Review documents and compose the result with stable replay addresses."""

    workflow_id = "onboarding-replay-ready"
    input_type = list[dict[str, str]]
    output_type = str

    def __init__(self) -> None:
        self.review = ReviewDocument()
        self.count_flags = CountFlags()
        self.summarize = SummarizeReviews()
        self.needs_escalation = NeedsEscalation()
        self.escalation = BuildEscalation()
        self.automatic = BuildAutomaticResult()
        self.polish = PolishReport()
        self.delivery = DeliveryWorkflow()

    def build(self, value: RuntimeValue[list[dict[str, str]]]) -> RuntimeValue[str]:
        reviews = map_over(
            value,
            self.review.at("review-document"),
            item_key="id",
        )
        review_context = parallel(
            self.count_flags.at("count-flags")(reviews),
            self.summarize.at("summarize-reviews")(reviews),
        )
        should_escalate = self.needs_escalation.at("needs-escalation")(review_context)
        routed = when(
            should_escalate,
            then=self.escalation.at("build-escalation")(review_context),
            otherwise=self.automatic.at("build-automatic")(review_context),
        )
        draft = self.polish.at("draft")(routed)
        ready = self.delivery(draft)
        return self.polish.at("final")(ready)


workflow = ReplayReadyReviewWorkflow()
EXAMPLE_INPUT = [
    {"id": "doc-refund", "text": "Payment refund requested"},
    {"id": "doc-login", "text": "Account login question"},
]
EXPECTED_OUTPUT = (
    "READY: ESCALATE (1 flag): flag:payment refund requested | ok:account login question"
)
