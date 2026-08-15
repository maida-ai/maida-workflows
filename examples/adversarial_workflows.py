"""Exercise branch, stable-map, nested, parallel, and effect boundaries.

These deterministic workflows provide compact integration fixtures for
compiler, runtime, fixture, and full-stub replay tests. They are also useful as
reference compositions when validating custom stores or replay integrations.
"""

from __future__ import annotations

from dataclasses import dataclass

from maida.workflows import (
    ExecutionContext,
    Module,
    RuntimeValue,
    Workflow,
    map_over,
    parallel,
    when,
)


class IsEscalated(Module[dict[str, object], bool]):
    """Read an escalation flag from a request mapping."""

    input_type = dict[str, object]
    output_type = bool

    async def execute(self, value: dict[str, object], ctx: ExecutionContext) -> bool:
        return bool(value.get("escalated"))


class Route(Module[dict[str, object], str]):
    """Return the queue configured for one explicit branch occurrence."""

    input_type = dict[str, object]
    output_type = str

    def __init__(self, queue: str) -> None:
        self.queue = queue

    async def execute(self, value: dict[str, object], ctx: ExecutionContext) -> str:
        return self.queue


class AdversarialBranchWorkflow(Workflow[dict[str, object], str]):
    """Compile two routes and choose one from a runtime escalation flag."""

    workflow_id = "adversarial-branch"
    input_type = dict[str, object]
    output_type = str
    check = IsEscalated()
    urgent = Route("urgent")
    normal = Route("normal")

    def build(self, value: RuntimeValue[dict[str, object]]) -> RuntimeValue[str]:
        return when(
            self.check.at("check-escalation")(value),
            self.urgent.at("route-urgent")(value),
            self.normal.at("route-normal")(value),
        )


@dataclass(frozen=True)
class BatchItem:
    """Mapped item with stable domain identity and a text payload."""

    stable_id: str
    payload: str


class NormalizeItem(Module[BatchItem, str]):
    """Normalize the payload of one stable mapped item."""

    input_type = BatchItem
    output_type = str

    async def execute(self, value: BatchItem, ctx: ExecutionContext) -> str:
        return value.payload.strip().lower()


class AdversarialMapWorkflow(Workflow[list[BatchItem], list[str]]):
    """Map normalization by stable item ID rather than list position."""

    workflow_id = "adversarial-map"
    input_type = list[BatchItem]
    output_type = list[str]
    normalize = NormalizeItem()

    def build(self, value: RuntimeValue[list[BatchItem]]) -> RuntimeValue[list[str]]:
        return map_over(
            value,
            self.normalize.at("normalize-item"),
            item_key="stable_id",
        )


class Review(Module[str, str]):
    """Produce a deterministic review marker for an input string."""

    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return f"reviewed:{value}"


class ChildReviewWorkflow(Workflow[str, str]):
    """Wrap the review module as a reusable child workflow."""

    workflow_id = "adversarial-child-review"
    input_type = str
    output_type = str
    review = Review()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.review.at("review")(value)


class AuditEffect(Module[str, str]):
    """Effect-classified boundary used to prove replay never invokes effects."""

    input_type = str
    output_type = str
    effectful = True

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value


class AdversarialNestedEffectWorkflow(Workflow[str, tuple[str, str]]):
    """Join a nested review with an explicitly effectful audit boundary."""

    workflow_id = "adversarial-nested-effect"
    input_type = str
    output_type = tuple[str, str]
    child = ChildReviewWorkflow()
    audit = AuditEffect()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[tuple[str, str]]:
        reviewed = self.child(value)
        return parallel(reviewed, self.audit.at("audit-effect")(reviewed))


ADVERSARIAL_WORKFLOWS = (
    AdversarialBranchWorkflow(),
    AdversarialMapWorkflow(),
    AdversarialNestedEffectWorkflow(),
)
