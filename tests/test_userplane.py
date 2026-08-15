from __future__ import annotations

from dataclasses import dataclass

import pytest

from maida.workflows import ExecutionContext, Module, RuntimeValue, Workflow
from maida.workflows.models import RunStatus
from maida.workflows.persistence import PostgresStore, TenantAccessError
from maida.workflows.userplane import (
    ApproveCommand,
    CancelCommand,
    InputCommand,
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
