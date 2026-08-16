"""Compose a durable approval as an ordinary typed graph boundary.

The approval task relinquishes its worker while it waits. An application sends
an ``ApproveCommand`` or ``RejectCommand`` later, and an unrelated worker
reclaims the task. The explicit ``when`` keeps both outcomes visible before the
workflow runs.
"""

from __future__ import annotations

from maida.workflows import (
    Approval,
    ApprovalDecision,
    ExecutionContext,
    Module,
    RuntimeValue,
    Workflow,
    when,
)


class _Prepare(Module[str, dict[str, str]]):
    input_type = str
    output_type = dict[str, str]

    async def execute(self, value: str, ctx: ExecutionContext) -> dict[str, str]:
        return {"change": value.strip()}


class _Approved(Module[ApprovalDecision, str]):
    input_type = ApprovalDecision
    output_type = str

    async def execute(self, value: ApprovalDecision, ctx: ExecutionContext) -> str:
        return f"approved by {value.command_id}"


class _Rejected(Module[ApprovalDecision, str]):
    input_type = ApprovalDecision
    output_type = str

    async def execute(self, value: ApprovalDecision, ctx: ExecutionContext) -> str:
        return f"rejected: {value.reason or 'no reason supplied'}"


class ReviewChange(Workflow[str, str]):
    """Prepare a change, wait durably for review, and route the decision."""

    workflow_id = "onboarding-interactive"
    input_type = str
    output_type = str

    def __init__(self) -> None:
        self.prepare = _Prepare()
        self.approval = Approval(
            dict[str, str],
            prompt="Apply this prepared change?",
            metadata={"screen": "change-review"},
        )
        self.approved = _Approved()
        self.rejected = _Rejected()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        """Construct the review request and both durable result branches."""
        decision = self.approval(self.prepare(value))
        return when(
            decision.field("approved"),
            self.approved(decision),
            self.rejected(decision),
        )


workflow = ReviewChange()
EXAMPLE_INPUT = "rotate the signing key"
