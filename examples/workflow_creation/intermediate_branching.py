"""Choose one runtime path with the explicit :func:`when` primitive.

Both routes are present in the compiled graph, while only the route selected by
the concrete priority executes. Ordinary Python ``if`` cannot inspect the
symbolic condition during workflow construction.
"""

from __future__ import annotations

from maida.workflows import ExecutionContext, Module, RuntimeValue, Workflow, when


class IsUrgent(Module[dict[str, str], bool]):
    """Return whether a ticket requests urgent handling."""

    input_type = dict[str, str]
    output_type = bool

    async def execute(self, value: dict[str, str], ctx: ExecutionContext) -> bool:
        return value.get("priority") == "urgent"


class RouteToHumanReview(Module[dict[str, str], str]):
    """Choose the human-review queue for an urgent ticket."""

    input_type = dict[str, str]
    output_type = str

    async def execute(self, value: dict[str, str], ctx: ExecutionContext) -> str:
        return "human-review"


class RouteToAutomaticReply(Module[dict[str, str], str]):
    """Choose automatic reply handling for a routine ticket."""

    input_type = dict[str, str]
    output_type = str

    async def execute(self, value: dict[str, str], ctx: ExecutionContext) -> str:
        return "automatic-reply"


class TicketRoutingWorkflow(Workflow[dict[str, str], str]):
    """Compile both ticket routes and select one from a runtime condition."""

    workflow_id = "onboarding-branching"
    input_type = dict[str, str]
    output_type = str

    def __init__(self) -> None:
        self.is_urgent = IsUrgent()
        self.human_review = RouteToHumanReview()
        self.automatic_reply = RouteToAutomaticReply()

    def build(self, value: RuntimeValue[dict[str, str]]) -> RuntimeValue[str]:
        urgent = self.is_urgent(value)
        return when(
            urgent,
            then=self.human_review(value),
            otherwise=self.automatic_reply(value),
        )


workflow = TicketRoutingWorkflow()
EXAMPLE_INPUT = {"priority": "urgent", "text": "Login broken"}
EXPECTED_OUTPUT = "human-review"
