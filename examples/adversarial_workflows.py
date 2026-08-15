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
    input_type = dict[str, object]
    output_type = bool

    async def execute(self, value: dict[str, object], ctx: ExecutionContext) -> bool:
        return bool(value.get("escalated"))


class Route(Module[dict[str, object], str]):
    input_type = dict[str, object]
    output_type = str

    def __init__(self, queue: str) -> None:
        self.queue = queue

    async def execute(self, value: dict[str, object], ctx: ExecutionContext) -> str:
        return self.queue


class AdversarialBranchWorkflow(Workflow[dict[str, object], str]):
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
    stable_id: str
    payload: str


class NormalizeItem(Module[BatchItem, str]):
    input_type = BatchItem
    output_type = str

    async def execute(self, value: BatchItem, ctx: ExecutionContext) -> str:
        return value.payload.strip().lower()


class AdversarialMapWorkflow(Workflow[list[BatchItem], list[str]]):
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
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return f"reviewed:{value}"


class ChildReviewWorkflow(Workflow[str, str]):
    workflow_id = "adversarial-child-review"
    input_type = str
    output_type = str
    review = Review()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.review.at("review")(value)


class AuditEffect(Module[str, str]):
    input_type = str
    output_type = str
    effectful = True

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value


class AdversarialNestedEffectWorkflow(Workflow[str, tuple[str, str]]):
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
