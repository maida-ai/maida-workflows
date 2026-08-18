from __future__ import annotations

import pytest

from maida.workflows import ExecutionContext, Module, RuntimeValue, Workflow, WorkflowClient
from maida.workflows.persistence import InvalidRunStateError, PostgresStore


class Echo(Module[int, int]):
    module_id = "idempotent.echo"
    input_type = int
    output_type = int

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return value


class EchoWorkflow(Workflow[int, int]):
    workflow_id = "idempotent-start"
    input_type = int
    output_type = int
    echo = Echo()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.echo(value)


@pytest.mark.postgres
def test_start_idempotency_reuses_one_run_and_task_graph(
    postgres_store: PostgresStore,
) -> None:
    client = WorkflowClient(postgres_store)
    first = client.start(EchoWorkflow(), 7, idempotency_key="event-123")
    repeated = client.start(EchoWorkflow(), 7, idempotency_key="event-123")

    assert repeated.run_id == first.run_id
    history = postgres_store.load_run_history(first.run_id, tenant_id="local")
    assert len(history.tasks) == 1
    assert [event.event_type for event in history.events].count("RUN_STARTED") == 1
    assert [event.event_type for event in history.events].count("TASK_CREATED") == 1


@pytest.mark.postgres
def test_start_idempotency_rejects_changed_content_but_is_tenant_scoped(
    postgres_store: PostgresStore,
) -> None:
    client = WorkflowClient(postgres_store)
    first = client.start(
        EchoWorkflow(),
        7,
        tenant_id="tenant-a",
        idempotency_key="event-123",
    )
    with pytest.raises(InvalidRunStateError, match="different content"):
        client.start(
            EchoWorkflow(),
            8,
            tenant_id="tenant-a",
            idempotency_key="event-123",
        )

    other_tenant = client.start(
        EchoWorkflow(),
        8,
        tenant_id="tenant-b",
        idempotency_key="event-123",
    )
    assert other_tenant.run_id != first.run_id


@pytest.mark.postgres
def test_start_without_idempotency_key_remains_a_fresh_invocation(
    postgres_store: PostgresStore,
) -> None:
    client = WorkflowClient(postgres_store)
    first = client.start(EchoWorkflow(), 7)
    second = client.start(EchoWorkflow(), 7)
    assert first.run_id != second.run_id
