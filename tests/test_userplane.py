from __future__ import annotations

from dataclasses import dataclass

import pytest

from maida.workflows import ExecutionContext, Module, RuntimeValue, Workflow
from maida.workflows._canonical import schema_digest
from maida.workflows.models import AttemptStatus, RunStatus, TaskStatus
from maida.workflows.persistence import (
    InvalidRunStateError,
    PostgresStore,
    StaleLeaseError,
    TenantAccessError,
    blank_boundary,
)
from maida.workflows.replay import build_module_registry
from maida.workflows.runtime import TaskWorker, WorkflowScheduler
from maida.workflows.userplane import (
    ApproveCommand,
    CancelCommand,
    InputCommand,
    InteractionKind,
    InteractionRequest,
    PauseCommand,
    RejectCommand,
    ResumeCommand,
    RetryCommand,
    SignalCommand,
    WorkflowClient,
)


class Upper(Module[str, str]):
    input_type = str
    output_type = str

    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        self.calls += 1
        return value.upper()


class UpperWorkflow(Workflow[str, str]):
    workflow_id = "userplane-upper"
    input_type = str
    output_type = str

    def __init__(self) -> None:
        self.upper = Upper()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.upper(value)


@dataclass(frozen=True)
class Account:
    account_id: str


def test_typed_commands_have_canonical_transport_payloads() -> None:
    commands = (
        SignalCommand(command_id="c-1", name="refresh", value={"scope": "all"}),
        ApproveCommand(command_id="c-2", request_id="publish", comment="looks good"),
        RejectCommand(command_id="c-3", request_id="publish", reason="needs changes"),
        InputCommand(command_id="c-4", request_id="account", value=Account("acct-1")),
        PauseCommand(command_id="c-5", reason="maintenance"),
        ResumeCommand(command_id="c-6"),
        CancelCommand(command_id="c-7", reason="user requested"),
        RetryCommand(command_id="c-8", task_id="task-1"),
    )

    assert [command.to_data()["type"] for command in commands] == [
        "signal",
        "approve",
        "reject",
        "input",
        "pause",
        "resume",
        "cancel",
        "retry",
    ]
    assert commands[0].to_data() == {
        "command_id": "c-1",
        "name": "refresh",
        "type": "signal",
        "value": {"scope": "all"},
    }
    assert commands[3].to_data()["value"] == {"account_id": "acct-1"}


def test_typed_commands_reject_ambiguous_addresses() -> None:
    with pytest.raises(ValueError, match="command_id"):
        ResumeCommand(command_id=" ")
    with pytest.raises(ValueError, match="name"):
        SignalCommand(command_id="c-1", name="", value=None)
    with pytest.raises(ValueError, match="request_id"):
        ApproveCommand(command_id="c-1", request_id="")
    with pytest.raises(ValueError, match="task_id"):
        RetryCommand(command_id="c-1", task_id="")
    with pytest.raises(ValueError, match="request_id"):
        RejectCommand(command_id="c-1", request_id="")
    with pytest.raises(ValueError, match="request_id"):
        InputCommand(command_id="c-1", request_id="", value=None)


def test_interaction_requests_validate_durable_addresses() -> None:
    with pytest.raises(ValueError, match="request_id"):
        InteractionRequest(request_id="", kind=InteractionKind.INPUT, prompt="Choose")
    with pytest.raises(ValueError, match="prompt"):
        InteractionRequest(request_id="account", kind=InteractionKind.INPUT, prompt="")
    with pytest.raises(ValueError, match="signal_name"):
        InteractionRequest(request_id="wake", kind=InteractionKind.SIGNAL, prompt="Continue?")


@pytest.mark.postgres
def test_run_api_starts_without_execution_and_projects_cursor_events(
    postgres_store: PostgresStore,
) -> None:
    workflow = UpperWorkflow()
    client = WorkflowClient(postgres_store)

    run = client.start(workflow, "hello", tenant_id="tenant-a")

    assert workflow.upper.calls == 0
    assert run.snapshot().status is RunStatus.RUNNING
    first = run.events(limit=2)
    assert [event.type for event in first.events] == ["run.started", "task.created"]
    assert first.has_more
    assert first.next_cursor == first.events[-1].sequence
    second = run.events(after=first.next_cursor, limit=2)
    assert [event.type for event in second.events] == ["task.ready"]
    assert not second.has_more
    assert second.next_cursor == second.events[-1].sequence
    assert second.events[0].to_data()["run_id"] == run.run_id

    attached = client.attach(run.run_id, tenant_id="tenant-a")
    assert attached.snapshot().definition_digest == run.snapshot().definition_digest
    with pytest.raises(TenantAccessError, match="not accessible"):
        client.attach(run.run_id, tenant_id="tenant-b").events()


def test_event_page_validates_cursor_arguments() -> None:
    # Validation happens before storage access, so this test needs no database.
    run = WorkflowClient(object()).attach("run-1")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="after"):
        run.events(after=-1)
    with pytest.raises(ValueError, match="limit"):
        run.events(limit=0)
    with pytest.raises(ValueError, match="limit"):
        run.events(limit=1001)


@pytest.mark.postgres
def test_commands_are_idempotent_and_pause_resume_claiming(
    postgres_store: PostgresStore,
) -> None:
    workflow = UpperWorkflow()
    run = WorkflowClient(postgres_store).start(workflow, "hello")
    paused = PauseCommand(command_id="pause-1", reason="maintenance")

    accepted = run.send(paused)
    duplicate = run.send(paused)

    assert accepted.accepted
    assert not accepted.duplicate
    assert duplicate.duplicate
    assert run.snapshot().status is RunStatus.PAUSED
    scheduler = WorkflowScheduler.resume(postgres_store, workflow, run.run_id)
    worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=build_module_registry(workflow, scheduler.plan),
        worker_id="worker-1",
    )
    assert worker.claim() is None

    resumed = run.send(ResumeCommand(command_id="resume-1"))
    assert resumed.accepted
    assert run.snapshot().status is RunStatus.RUNNING
    assert worker.claim() is not None
    with pytest.raises(InvalidRunStateError, match="different content"):
        run.send(PauseCommand(command_id="resume-1"))

    second = WorkflowClient(postgres_store).start(UpperWorkflow(), "other")
    second_scheduler = WorkflowScheduler.resume(postgres_store, UpperWorkflow(), second.run_id)
    second_worker = TaskWorker(
        postgres_store,
        workflow_id=second_scheduler.plan.workflow_id,
        definition_digest=second_scheduler.plan.digest,
        modules=build_module_registry(UpperWorkflow(), second_scheduler.plan),
        worker_id="worker-2",
    )
    claimed = second_worker.claim()
    assert claimed is not None
    second.send(PauseCommand(command_id="pause-before-start"))
    with pytest.raises(StaleLeaseError, match="stale"):
        second_worker.start(claimed)


@pytest.mark.postgres
def test_cancellation_revokes_active_leases_and_prevents_late_completion(
    postgres_store: PostgresStore,
) -> None:
    workflow = UpperWorkflow()
    run = WorkflowClient(postgres_store).start(workflow, "hello")
    scheduler = WorkflowScheduler.resume(postgres_store, workflow, run.run_id)
    worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=build_module_registry(workflow, scheduler.plan),
        worker_id="worker-1",
    )
    envelope = worker.claim()
    assert envelope is not None
    envelope = worker.start(envelope)

    receipt = run.send(CancelCommand(command_id="cancel-1", reason="no longer needed"))

    assert receipt.run_status is RunStatus.CANCELLED
    history = postgres_store.load_run_history(run.run_id, tenant_id="local")
    assert history.tasks[0].status is TaskStatus.CANCELLED
    assert history.attempts[0].status is AttemptStatus.CANCELLED
    output = postgres_store.values.encode("HELLO", schema_digest=schema_digest(str))
    boundary = blank_boundary(
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        claim=envelope._claim(),
        input_value=envelope.input_ref,
        output_value=output,
    )
    with pytest.raises(StaleLeaseError, match="stale"):
        worker.complete(envelope, boundary)
    assert worker.claim() is None
    with pytest.raises(InvalidRunStateError, match="not running"):
        postgres_store.enqueue_task(
            run.run_id,
            scheduler.plan.executable_steps[0],
            step_instance_id="late-insertion",
            input_value=envelope.input_ref,
        )


@pytest.mark.postgres
def test_approval_and_input_requests_park_without_holding_compute(
    postgres_store: PostgresStore,
) -> None:
    workflow = UpperWorkflow()
    run = WorkflowClient(postgres_store).start(workflow, "hello")
    scheduler = WorkflowScheduler.resume(postgres_store, workflow, run.run_id)
    worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=build_module_registry(workflow, scheduler.plan),
        worker_id="worker-1",
    )
    envelope = worker.claim()
    assert envelope is not None
    envelope = worker.start(envelope)
    request = InteractionRequest(
        request_id="publish",
        kind=InteractionKind.APPROVAL,
        prompt="Publish this response?",
    )

    worker.park(envelope, request)

    parked = postgres_store.load_run_history(run.run_id, tenant_id="local")
    assert parked.tasks[0].status is TaskStatus.NEEDS_APPROVAL
    assert parked.attempts[0].status is AttemptStatus.PARKED
    assert worker.claim() is None
    receipt = run.send(
        ApproveCommand(command_id="approve-1", request_id="publish", comment="ship it")
    )
    assert receipt.task_id == envelope.task_id
    resumed = postgres_store.load_run_history(run.run_id, tenant_id="local")
    assert resumed.tasks[0].status is TaskStatus.READY
    second = worker.claim()
    assert second is not None
    second = worker.start(second)
    worker.park(
        second,
        InteractionRequest(
            request_id="publish-again",
            kind=InteractionKind.APPROVAL,
            prompt="Publish the revised response?",
        ),
    )
    assert (
        run.send(
            RejectCommand(command_id="reject-1", request_id="publish-again", reason="not yet")
        ).task_id
        == second.task_id
    )


@pytest.mark.postgres
def test_signals_inputs_and_retries_are_tenant_scoped_typed_events(
    postgres_store: PostgresStore,
) -> None:
    workflow = UpperWorkflow()
    run = WorkflowClient(postgres_store).start(workflow, "hello", tenant_id="tenant-a")
    scheduler = WorkflowScheduler.resume(postgres_store, workflow, run.run_id, tenant_id="tenant-a")
    worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=build_module_registry(workflow, scheduler.plan),
        worker_id="worker-1",
    )
    envelope = worker.claim()
    assert envelope is not None
    envelope = worker.start(envelope)
    worker.park(
        envelope,
        InteractionRequest(
            request_id="account",
            kind=InteractionKind.INPUT,
            prompt="Choose an account",
            schema_digest=schema_digest(Account),
        ),
    )
    with pytest.raises(TenantAccessError, match="not accessible"):
        WorkflowClient(postgres_store).attach(run.run_id, tenant_id="tenant-b").send(
            InputCommand(command_id="input-wrong-tenant", request_id="account", value={})
        )
    assert (
        run.send(
            InputCommand(command_id="input-1", request_id="account", value=Account("acct-1"))
        ).task_id
        == envelope.task_id
    )
    signal = run.send(SignalCommand(command_id="signal-1", name="refresh", value={"scope": "all"}))
    assert signal.accepted

    retry_envelope = worker.claim()
    assert retry_envelope is not None
    retry_envelope = worker.start(retry_envelope)
    worker.fail(retry_envelope, {"reason": "terminal"}, retry=False)
    assert (
        run.send(RetryCommand(command_id="retry-1", task_id=retry_envelope.task_id)).task_id
        == retry_envelope.task_id
    )

    event_types = [event.type for event in run.events(limit=100).events]
    assert "input.required" in event_types
    assert "input.received" in event_types
    assert "signal.received" in event_types
    assert "task.retry.requested" in event_types


@pytest.mark.postgres
def test_targeted_signal_resumes_waiting_task_and_invalid_targets_fail(
    postgres_store: PostgresStore,
) -> None:
    workflow = UpperWorkflow()
    run = WorkflowClient(postgres_store).start(workflow, "hello")
    scheduler = WorkflowScheduler.resume(postgres_store, workflow, run.run_id)
    worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=build_module_registry(workflow, scheduler.plan),
        worker_id="worker-1",
    )
    envelope = worker.claim()
    assert envelope is not None
    envelope = worker.start(envelope)
    worker.park(
        envelope,
        InteractionRequest(
            request_id="wake",
            kind=InteractionKind.SIGNAL,
            prompt="Continue processing?",
            signal_name="continue",
        ),
    )

    receipt = run.send(
        SignalCommand(
            command_id="wake-1",
            name="continue",
            request_id="wake",
            value={"confirmed": True},
        )
    )
    assert receipt.task_id == envelope.task_id
    assert (
        postgres_store.load_run_history(run.run_id, tenant_id="local").tasks[0].status
        is TaskStatus.READY
    )
    with pytest.raises(InvalidRunStateError, match="not awaiting approve"):
        run.send(ApproveCommand(command_id="missing-approval", request_id="missing"))
    with pytest.raises(InvalidRunStateError, match="not a failed task"):
        run.send(RetryCommand(command_id="missing-retry", task_id=envelope.task_id))
    with pytest.raises(InvalidRunStateError, match="not awaiting a signal"):
        run.send(
            SignalCommand(
                command_id="missing-signal",
                name="continue",
                request_id="missing",
                value=None,
            )
        )
    with pytest.raises(ValueError, match="command_id"):
        postgres_store.submit_command(
            run.run_id,
            tenant_id="local",
            command={"command_id": "", "type": "pause"},
        )
    with pytest.raises(ValueError, match="unsupported"):
        postgres_store.submit_command(
            run.run_id,
            tenant_id="local",
            command={"command_id": "unknown-1", "type": "unknown"},
        )
