from __future__ import annotations

import shutil
import time
from datetime import timedelta
from pathlib import Path

import pytest

from maida.workflows import ExecutionContext, Module, RuntimeValue, Workflow, compile_workflow
from maida.workflows._canonical import schema_digest
from maida.workflows.models import AttemptStatus, RunStatus, TaskStatus
from maida.workflows.persistence import (
    MigrationChecksumError,
    MigrationRunner,
    PostgresStore,
    StaleLeaseError,
    TenantAccessError,
    blank_boundary,
)


class Increment(Module[int, int]):
    input_type = int
    output_type = int

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return value + 1


class IncrementWorkflow(Workflow[int, int]):
    workflow_id = "increment"
    input_type = int
    output_type = int
    increment = Increment()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.increment(value)


@pytest.mark.postgres
def test_migrations_are_idempotent_and_checksummed(
    postgres_store: PostgresStore, tmp_path: Path
) -> None:
    postgres_store.upgrade()
    source = Path(__file__).parents[1] / "maida" / "workflows" / "migrations"
    copied = tmp_path / "migrations"
    shutil.copytree(source, copied)
    migration = copied / "0001_initial.sql"
    migration.write_text(migration.read_text() + "\n-- changed after application\n")

    with (
        postgres_store.connect() as connection,
        pytest.raises(MigrationChecksumError, match="checksum changed"),
    ):
        MigrationRunner(connection, copied).upgrade()


@pytest.mark.postgres
def test_leases_retries_stale_completion_and_history(postgres_store: PostgresStore) -> None:
    plan = compile_workflow(IncrementWorkflow())
    encoded_input = postgres_store.values.encode(4, schema_digest=schema_digest(int))
    run = postgres_store.create_run(plan, tenant_id="tenant-a", root_input=encoded_input)
    step = plan.executable_steps[0]
    task = postgres_store.enqueue_task(
        run.run_id,
        step,
        step_instance_id="singleton",
        input_value=encoded_input,
    )

    first = postgres_store.claim_task(
        worker_id="crashed-worker", lease_for=timedelta(milliseconds=5), task_id=task.task_id
    )
    assert first is not None
    time.sleep(0.02)
    recovered = postgres_store.claim_task(worker_id="recovery-worker", task_id=task.task_id)
    assert recovered is not None
    assert recovered.attempt.attempt_number == 2

    encoded_output = postgres_store.values.encode(5, schema_digest=schema_digest(int))
    stale_boundary = blank_boundary(
        workflow_id=plan.workflow_id,
        definition_digest=plan.digest,
        claim=first,
        input_value=encoded_input,
        output_value=encoded_output,
    )
    with pytest.raises(StaleLeaseError, match="stale"):
        postgres_store.complete_task(first, stale_boundary)

    postgres_store.fail_task(recovered, {"reason": "transient"}, retry=True)
    final = postgres_store.claim_task(worker_id="recovery-worker", task_id=task.task_id)
    assert final is not None
    assert final.attempt.attempt_number == 3
    boundary = blank_boundary(
        workflow_id=plan.workflow_id,
        definition_digest=plan.digest,
        claim=final,
        input_value=encoded_input,
        output_value=encoded_output,
    )
    postgres_store.complete_task(final, boundary)
    postgres_store.complete_run(run.run_id, encoded_output)

    history = postgres_store.load_run_history(run.run_id, tenant_id="tenant-a")
    assert history.run.status is RunStatus.SUCCEEDED
    assert history.tasks[0].status is TaskStatus.SUCCEEDED
    assert history.tasks[0].accepted_boundary is not None
    assert [attempt.status for attempt in history.attempts] == [
        AttemptStatus.ABANDONED,
        AttemptStatus.FAILED,
        AttemptStatus.SUCCEEDED,
    ]
    assert [event.event_id for event in history.events] == sorted(
        event.event_id for event in history.events
    )
    assert history.accepted_boundaries == (history.tasks[0].accepted_boundary,)

    with pytest.raises(TenantAccessError, match="not accessible"):
        postgres_store.load_run_history(run.run_id, tenant_id="tenant-b")


@pytest.mark.postgres
def test_run_cannot_complete_with_unfinished_tasks(postgres_store: PostgresStore) -> None:
    plan = compile_workflow(IncrementWorkflow())
    value = postgres_store.values.encode(1, schema_digest=schema_digest(int))
    run = postgres_store.create_run(plan, tenant_id="local", root_input=value)
    postgres_store.enqueue_task(
        run.run_id,
        plan.executable_steps[0],
        step_instance_id="singleton",
        input_value=value,
    )

    from maida.workflows.persistence import InvalidRunStateError

    with pytest.raises(InvalidRunStateError, match="incomplete"):
        postgres_store.complete_run(run.run_id, value)
