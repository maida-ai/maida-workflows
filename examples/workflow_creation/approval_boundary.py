"""Compose a durable approval as an ordinary typed graph boundary.

The approval task relinquishes its worker while it waits. An application sends
an ``ApproveCommand`` or ``RejectCommand`` later, and any compatible worker can
reclaim the task. The explicit ``when`` keeps both outcomes visible before the
workflow runs.
"""

from __future__ import annotations

from maida.workflows import (
    Approval,
    ApprovalDecision,
    ApproveCommand,
    ExecutionContext,
    Module,
    RunResult,
    RunStatus,
    RuntimeValue,
    TaskWorker,
    Workflow,
    WorkflowRun,
    WorkflowScheduler,
    bind_workflow,
    when,
)
from maida.workflows.persistence import PostgresStore


class _Prepare(Module[str, dict[str, str]]):
    module_id = "demo.change.prepare"
    input_type = str
    output_type = dict[str, str]

    async def execute(self, value: str, ctx: ExecutionContext) -> dict[str, str]:
        return {"change": value.strip()}


class _Approved(Module[ApprovalDecision, str]):
    module_id = "demo.change.approved"
    input_type = ApprovalDecision
    output_type = str

    async def execute(self, value: ApprovalDecision, ctx: ExecutionContext) -> str:
        return f"approved by {value.command_id}"


class _Rejected(Module[ApprovalDecision, str]):
    module_id = "demo.change.rejected"
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
EXPECTED_OUTPUT = "approved by example-approval"


async def run_example(
    store: PostgresStore,
    value: str = EXAMPLE_INPUT,
) -> RunResult:
    """Execute the workflow and supply one deterministic durable approval."""
    bound = bind_workflow(workflow)
    scheduler = WorkflowScheduler.submit(store, bound, value)
    worker = TaskWorker(
        store,
        workflow_id=bound.plan.workflow_id,
        definition_digest=bound.plan.digest,
        modules=bound.modules,
        worker_id="approval-example-worker",
    )
    approval_sent = False

    for _step in range(20):
        progress = scheduler.advance()
        if progress.status is RunStatus.SUCCEEDED:
            return RunResult(scheduler.run_id, progress.output, bound.plan.digest)
        if progress.status is RunStatus.FAILED:
            raise RuntimeError("approval example failed")

        boundary = await worker.run_once()
        if boundary is not None or approval_sent:
            continue
        history = store.load_run_history(scheduler.run_id, tenant_id="local")
        request = next(
            (event for event in history.events if event.event_type == "APPROVAL_REQUIRED"),
            None,
        )
        if request is None:
            continue
        WorkflowRun(store, scheduler.run_id).send(
            ApproveCommand(
                request_id=str(request.payload["request_id"]),
                command_id="example-approval",
            )
        )
        approval_sent = True

    raise RuntimeError("approval example did not complete")
