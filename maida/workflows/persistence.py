"""Persist workflow definitions, runs, tasks, attempts, events, and artifacts.

The PostgreSQL store provides checksummed migrations, tenant-scoped history,
task leasing, compare-and-swap completion, and immutable accepted boundaries.
Application code normally constructs :class:`PostgresStore` with a
:class:`~maida.workflows.artifacts.ValueCodec` and passes it to the runtime.
"""

from __future__ import annotations

import hashlib
import importlib.resources
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from ._canonical import digest_data
from .artifacts import ValueCodec
from .ir import PlanIR, StepIR
from .models import (
    AcceptedAttemptProvenance,
    Attempt,
    AttemptStatus,
    BoundaryRecord,
    Definition,
    Event,
    ExecutionMode,
    Run,
    RunHistory,
    RunStatus,
    StoredValue,
    Task,
    TaskStatus,
    ValueStorage,
)


class PersistenceError(RuntimeError):
    """Base error for durable store and migration failures."""


class MigrationChecksumError(PersistenceError):
    """Raised when an applied migration no longer matches its checksum."""


class StaleLeaseError(PersistenceError):
    """Raised when task completion uses an expired or replaced lease token."""


class TenantAccessError(PersistenceError):
    """Raised when a run is requested from the wrong tenant scope."""


class InvalidRunStateError(PersistenceError):
    """Raised when a run transition is invalid for its current state."""


@dataclass(frozen=True)
class ClaimedTask:
    """Durable task and attempt leased to one worker until a deadline."""

    task: Task
    attempt: Attempt
    worker_id: str
    lease_expires_at: datetime


def _migration_files(directory: Path | None = None) -> tuple[Path, ...]:
    if directory is not None:
        return tuple(sorted(directory.glob("*.sql")))
    resource = importlib.resources.files("maida.workflows").joinpath("migrations")
    with importlib.resources.as_file(resource) as path:
        return tuple(sorted(path.glob("*.sql")))


class MigrationRunner:
    """Apply packaged SQL migrations exactly once and verify their checksums.

    Parameters
    ----------
    connection
        Open psycopg connection used inside migration transactions.
    directory
        Optional migration directory, primarily for tests. Packaged migrations
        are used by default.
    """

    def __init__(self, connection: psycopg.Connection[Any], directory: Path | None = None) -> None:
        self.connection = connection
        self.directory = directory

    def upgrade(self) -> None:
        """Apply pending migrations under a PostgreSQL advisory lock.

        Raises
        ------
        PersistenceError
            If no migrations are available.
        MigrationChecksumError
            If an already-applied migration file has changed.
        """
        files = _migration_files(self.directory)
        if not files:
            raise PersistenceError("no database migrations were packaged")
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (0x4D57464C,))
            exists = cursor.execute(
                "SELECT to_regclass('workflow_schema_migrations') IS NOT NULL AS present"
            ).fetchone()
            applied: dict[str, str] = {}
            if exists is None:
                present = False
            elif isinstance(exists, dict):
                present = bool(exists["present"])
            else:
                present = bool(exists[0])
            if present:
                cursor.execute("SELECT version, checksum FROM workflow_schema_migrations")
                for row in cursor.fetchall():
                    if isinstance(row, dict):
                        applied[row["version"]] = row["checksum"]
                    else:
                        applied[row[0]] = row[1]
            for path in files:
                version = path.stem
                content = path.read_bytes()
                checksum = hashlib.sha256(content).hexdigest()
                if version in applied:
                    if applied[version] != checksum:
                        raise MigrationChecksumError(
                            f"migration {version} checksum changed after application"
                        )
                    continue
                cursor.execute(content.decode())
                cursor.execute(
                    "INSERT INTO workflow_schema_migrations (version, checksum) VALUES (%s, %s)",
                    (version, checksum),
                )


class PostgresStore:
    """PostgreSQL implementation of durable workflow storage.

    Parameters
    ----------
    dsn
        PostgreSQL connection string. It is retained locally and never embedded
        in fixtures or baselines.
    values
        Codec used for typed inline and artifact-backed payload references.
    """

    def __init__(self, dsn: str, values: ValueCodec) -> None:
        self.dsn = dsn
        self.values = values

    @contextmanager
    def connect(self) -> Iterator[psycopg.Connection[dict[str, Any]]]:
        """Yield a dictionary-row psycopg connection and close it afterward."""
        with psycopg.connect(self.dsn, row_factory=dict_row) as connection:
            yield connection

    def upgrade(self) -> None:
        """Apply all packaged database migrations safely."""
        with self.connect() as connection:
            MigrationRunner(connection).upgrade()

    def register_definition(self, plan: PlanIR) -> Definition:
        """Persist a canonical workflow definition if it is not already known."""
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO workflow_definitions (digest, workflow_id, ir_version, canonical_ir)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (digest) DO NOTHING
                """,
                (plan.digest, plan.workflow_id, plan.version, Jsonb(plan.to_dict())),
            )
        return Definition(plan.digest, plan.workflow_id, plan.version, plan.to_dict())

    def create_run(
        self,
        plan: PlanIR,
        *,
        tenant_id: str,
        root_input: StoredValue,
        execution_mode: ExecutionMode = ExecutionMode.LIVE,
        run_id: str | None = None,
    ) -> Run:
        """Create a running workflow instance pinned to a compiled definition.

        Parameters
        ----------
        plan
            Canonical workflow definition.
        tenant_id
            Tenant scope used for later history access.
        root_input
            Typed immutable reference to the concrete root input.
        execution_mode
            Live or verification-live execution classification.
        run_id
            Optional caller-supplied identifier; a UUID is generated otherwise.
        """
        self.register_definition(plan)
        identifier = run_id or str(uuid4())
        with self.connect() as connection, connection.cursor() as cursor:
            self._register_value_artifact(cursor, root_input)
            cursor.execute(
                """
                INSERT INTO workflow_runs (
                    run_id, tenant_id, definition_digest, execution_mode, status,
                    root_input, root_input_schema_digest
                ) VALUES (%s, %s, %s, %s, 'RUNNING', %s, %s)
                """,
                (
                    identifier,
                    tenant_id,
                    plan.digest,
                    execution_mode.value,
                    Jsonb(root_input.to_data()),
                    root_input.schema_digest,
                ),
            )
            self._append_event(
                cursor,
                identifier,
                "RUN_STARTED",
                {"execution_mode": execution_mode.value},
            )
        return Run(
            run_id=identifier,
            tenant_id=tenant_id,
            definition_digest=plan.digest,
            execution_mode=execution_mode,
            status=RunStatus.RUNNING,
            root_input=root_input,
            root_input_schema_digest=root_input.schema_digest,
        )

    def enqueue_task(
        self,
        run_id: str,
        step: StepIR,
        *,
        step_instance_id: str,
        input_value: StoredValue,
        dependency_instance_keys: tuple[str, ...] = (),
        task_id: str | None = None,
    ) -> Task:
        """Create one durable executable task pinned to a module digest.

        Raises
        ------
        ValueError
            If ``step`` is a control node without executable module identity.
        """
        if step.module_id is None or step.logical_step is None or step.module_digest is None:
            raise ValueError("only executable module steps can be enqueued")
        identifier = task_id or str(uuid4())
        with self.connect() as connection, connection.cursor() as cursor:
            self._register_value_artifact(cursor, input_value)
            cursor.execute(
                """
                INSERT INTO workflow_tasks (
                    task_id, run_id, module_id, logical_step, step_instance_id,
                    module_digest, dependency_instance_keys, task_input
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    identifier,
                    run_id,
                    step.module_id,
                    step.logical_step,
                    step_instance_id,
                    step.module_digest,
                    Jsonb(list(dependency_instance_keys)),
                    Jsonb(input_value.to_data()),
                ),
            )
            self._append_event(
                cursor,
                run_id,
                "TASK_CREATED",
                {
                    "module_id": step.module_id,
                    "logical_step": step.logical_step,
                    "step_instance_id": step_instance_id,
                },
                task_id=identifier,
            )
        return Task(
            task_id=identifier,
            run_id=run_id,
            module_id=step.module_id,
            logical_step=step.logical_step,
            step_instance_id=step_instance_id,
            module_digest=step.module_digest,
            dependency_instance_keys=dependency_instance_keys,
            input_value=input_value,
            status=TaskStatus.PENDING,
        )

    def claim_task(
        self,
        *,
        worker_id: str,
        lease_for: timedelta = timedelta(minutes=5),
        task_id: str | None = None,
    ) -> ClaimedTask | None:
        """Lease one pending or expired task using ``SKIP LOCKED`` semantics.

        Parameters
        ----------
        worker_id
            Diagnostic identity of the claiming worker.
        lease_for
            Positive period before another worker may reclaim the task.
        task_id
            Optional exact task restriction.

        Returns
        -------
        ClaimedTask or None
            New attempt and lease, or ``None`` when nothing is eligible.
        """
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        lease_token = uuid4()
        attempt_id = uuid4()
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT task_id, status FROM workflow_tasks
                WHERE (%s::uuid IS NULL OR task_id = %s::uuid)
                  AND (status = 'PENDING' OR (status = 'LEASED' AND lease_expires_at < now()))
                ORDER BY created_at, task_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """,
                (task_id, task_id),
            )
            selected = cursor.fetchone()
            if selected is None:
                return None
            selected_id = selected["task_id"]
            if selected["status"] == TaskStatus.LEASED.value:
                cursor.execute(
                    """
                    UPDATE workflow_attempts
                    SET status = 'ABANDONED', completed_at = now(),
                        diagnostic = '{"reason":"lease expired"}'::jsonb
                    WHERE task_id = %s AND status = 'RUNNING'
                    """,
                    (selected_id,),
                )
            expires_at = datetime.now(UTC) + lease_for
            cursor.execute(
                """
                UPDATE workflow_tasks
                SET status = 'LEASED', lease_owner = %s, lease_token = %s,
                    lease_expires_at = %s
                WHERE task_id = %s
                RETURNING *
                """,
                (worker_id, lease_token, expires_at, selected_id),
            )
            row = cursor.fetchone()
            if row is None:  # pragma: no cover - protected by row lock
                raise PersistenceError("claimed task disappeared")
            cursor.execute(
                "SELECT COALESCE(MAX(attempt_number), 0) + 1 AS number "
                "FROM workflow_attempts WHERE task_id = %s",
                (selected_id,),
            )
            number_row = cursor.fetchone()
            attempt_number = int(number_row["number"] if number_row else 1)
            cursor.execute(
                """
                INSERT INTO workflow_attempts (
                    attempt_id, task_id, attempt_number, lease_token, status
                ) VALUES (%s, %s, %s, %s, 'RUNNING')
                RETURNING started_at
                """,
                (attempt_id, selected_id, attempt_number, lease_token),
            )
            attempt_row = cursor.fetchone()
            self._append_event(
                cursor,
                str(row["run_id"]),
                "TASK_CLAIMED",
                {"worker_id": worker_id, "attempt_number": attempt_number},
                task_id=str(selected_id),
                attempt_id=str(attempt_id),
            )
        task = self._task_from_row(row)
        attempt = Attempt(
            attempt_id=str(attempt_id),
            task_id=str(selected_id),
            attempt_number=attempt_number,
            lease_token=str(lease_token),
            status=AttemptStatus.RUNNING,
            started_at=attempt_row["started_at"] if attempt_row else None,
        )
        return ClaimedTask(task, attempt, worker_id, expires_at)

    def complete_task(self, claim: ClaimedTask, boundary: BoundaryRecord) -> None:
        """Accept one logical task result using lease-token compare-and-swap.

        Raises
        ------
        ValueError
            If boundary identity differs from the claimed task.
        StaleLeaseError
            If another worker has already replaced or completed the lease.
        """
        task = claim.task
        if (
            boundary.module_id != task.module_id
            or boundary.logical_step != task.logical_step
            or boundary.step_instance_id != task.step_instance_id
            or boundary.module_digest != task.module_digest
        ):
            raise ValueError("accepted boundary identity does not match the claimed task")
        completed_at = datetime.now(UTC)
        boundary_data = boundary.to_data()
        boundary_data["accepted_attempt"] = {
            "attempt_id": claim.attempt.attempt_id,
            "attempt_number": claim.attempt.attempt_number,
            "worker_id": claim.worker_id,
            "started_at": claim.attempt.started_at.isoformat()
            if claim.attempt.started_at
            else completed_at.isoformat(),
            "completed_at": completed_at.isoformat(),
        }
        with self.connect() as connection, connection.cursor() as cursor:
            self._register_value_artifact(cursor, boundary.input_value)
            self._register_value_artifact(cursor, boundary.output_value)
            cursor.execute(
                """
                UPDATE workflow_tasks
                SET status = 'SUCCEEDED', accepted_attempt_id = %s,
                    accepted_boundary = %s, completed_at = %s,
                    lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
                WHERE task_id = %s AND status = 'LEASED' AND lease_token = %s
                """,
                (
                    claim.attempt.attempt_id,
                    Jsonb(boundary_data),
                    completed_at,
                    task.task_id,
                    claim.attempt.lease_token,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease for task {task.task_id} is stale")
            cursor.execute(
                """
                UPDATE workflow_attempts SET status = 'SUCCEEDED', completed_at = %s
                WHERE attempt_id = %s AND status = 'RUNNING'
                """,
                (completed_at, claim.attempt.attempt_id),
            )
            self._append_event(
                cursor,
                task.run_id,
                "TASK_COMPLETED",
                {"boundary_digest": digest_data(boundary_data)},
                task_id=task.task_id,
                attempt_id=claim.attempt.attempt_id,
            )

    def fail_task(
        self,
        claim: ClaimedTask,
        diagnostic: dict[str, Any],
        *,
        retry: bool,
    ) -> None:
        """Record a failed attempt and optionally return its task to pending.

        Raises
        ------
        StaleLeaseError
            If the claim no longer owns the task lease.
        """
        next_status = TaskStatus.PENDING if retry else TaskStatus.FAILED
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE workflow_tasks
                SET status = %s, lease_owner = NULL, lease_token = NULL,
                    lease_expires_at = NULL
                WHERE task_id = %s AND status = 'LEASED' AND lease_token = %s
                """,
                (next_status.value, claim.task.task_id, claim.attempt.lease_token),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease for task {claim.task.task_id} is stale")
            cursor.execute(
                """
                UPDATE workflow_attempts
                SET status = 'FAILED', diagnostic = %s, completed_at = now()
                WHERE attempt_id = %s AND status = 'RUNNING'
                """,
                (Jsonb(diagnostic), claim.attempt.attempt_id),
            )
            self._append_event(
                cursor,
                claim.task.run_id,
                "TASK_RETRY" if retry else "TASK_FAILED",
                diagnostic,
                task_id=claim.task.task_id,
                attempt_id=claim.attempt.attempt_id,
            )

    def complete_run(self, run_id: str, root_output: StoredValue) -> None:
        """Mark a running workflow successful after every task is accepted.

        Raises
        ------
        InvalidRunStateError
            If tasks remain incomplete or the run is no longer running.
        """
        with self.connect() as connection, connection.cursor() as cursor:
            self._register_value_artifact(cursor, root_output)
            cursor.execute(
                """
                SELECT count(*) FILTER (WHERE status <> 'SUCCEEDED') AS incomplete
                FROM workflow_tasks WHERE run_id = %s
                """,
                (run_id,),
            )
            row = cursor.fetchone()
            if row and int(row["incomplete"]) != 0:
                raise InvalidRunStateError("a run cannot succeed while tasks are incomplete")
            cursor.execute(
                """
                UPDATE workflow_runs
                SET status = 'SUCCEEDED', root_output = %s,
                    root_output_schema_digest = %s, completed_at = now()
                WHERE run_id = %s AND status = 'RUNNING'
                """,
                (Jsonb(root_output.to_data()), root_output.schema_digest, run_id),
            )
            if cursor.rowcount != 1:
                raise InvalidRunStateError(f"run {run_id} is not running")
            self._append_event(cursor, run_id, "RUN_COMPLETED", {})

    def fail_run(self, run_id: str, diagnostic: dict[str, Any]) -> None:
        """Mark a running workflow failed and append its diagnostic event."""
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE workflow_runs SET status = 'FAILED', completed_at = now(),
                    replayable_reason = %s
                WHERE run_id = %s AND status = 'RUNNING'
                """,
                (diagnostic.get("reason", "run failed"), run_id),
            )
            self._append_event(cursor, run_id, "RUN_FAILED", diagnostic)

    def append_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        task_id: str | None = None,
        attempt_id: str | None = None,
    ) -> None:
        """Append an ordered diagnostic or control event to a run."""
        with self.connect() as connection, connection.cursor() as cursor:
            self._append_event(cursor, run_id, event_type, payload, task_id, attempt_id)

    @staticmethod
    def _append_event(
        cursor: psycopg.Cursor[Any],
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
        task_id: str | None = None,
        attempt_id: str | None = None,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO workflow_events (run_id, task_id, attempt_id, event_type, payload)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (run_id, task_id, attempt_id, event_type, Jsonb(payload)),
        )

    def _register_value_artifact(self, cursor: psycopg.Cursor[Any], value: StoredValue) -> None:
        if value.storage is not ValueStorage.ARTIFACT:
            return
        if value.artifact_digest is None:
            raise PersistenceError("artifact-backed value has no digest")
        content = self.values.artifacts.get(value.artifact_digest)
        relative_path = str(self.values.artifacts.relative_path(value.artifact_digest))
        cursor.execute(
            """
            INSERT INTO workflow_artifacts (digest, size_bytes, media_type, relative_path)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (digest) DO NOTHING
            """,
            (value.artifact_digest, len(content), value.media_type, relative_path),
        )

    def load_run_history(self, run_id: str, *, tenant_id: str) -> RunHistory:
        """Load complete tenant-scoped definition and execution history.

        Raises
        ------
        PersistenceError
            If ``run_id`` does not exist.
        TenantAccessError
            If the run belongs to a different tenant.
        """
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT r.*, d.workflow_id, d.ir_version, d.canonical_ir,
                       d.created_at AS definition_created_at
                FROM workflow_runs r
                JOIN workflow_definitions d ON d.digest = r.definition_digest
                WHERE r.run_id = %s
                """,
                (run_id,),
            )
            run_row = cursor.fetchone()
            if run_row is None:
                raise PersistenceError(f"run {run_id} was not found")
            if run_row["tenant_id"] != tenant_id:
                raise TenantAccessError("run is not accessible to this tenant")
            cursor.execute(
                "SELECT * FROM workflow_tasks WHERE run_id = %s ORDER BY created_at, task_id",
                (run_id,),
            )
            tasks = tuple(self._task_from_row(row) for row in cursor.fetchall())
            cursor.execute(
                """
                SELECT a.* FROM workflow_attempts a
                JOIN workflow_tasks t ON t.task_id = a.task_id
                WHERE t.run_id = %s ORDER BY a.started_at, a.attempt_id
                """,
                (run_id,),
            )
            attempts = tuple(self._attempt_from_row(row) for row in cursor.fetchall())
            cursor.execute(
                "SELECT * FROM workflow_events WHERE run_id = %s ORDER BY event_id",
                (run_id,),
            )
            events = tuple(self._event_from_row(row) for row in cursor.fetchall())
        definition = Definition(
            digest=run_row["definition_digest"],
            workflow_id=run_row["workflow_id"],
            ir_version=run_row["ir_version"],
            canonical_ir=run_row["canonical_ir"],
            created_at=run_row["definition_created_at"],
        )
        run = Run(
            run_id=str(run_row["run_id"]),
            tenant_id=run_row["tenant_id"],
            definition_digest=run_row["definition_digest"],
            execution_mode=ExecutionMode(run_row["execution_mode"]),
            status=RunStatus(run_row["status"]),
            root_input=StoredValue.from_data(run_row["root_input"]),
            root_input_schema_digest=run_row["root_input_schema_digest"],
            root_output=StoredValue.from_data(run_row["root_output"])
            if run_row["root_output"]
            else None,
            root_output_schema_digest=run_row["root_output_schema_digest"],
            replayable_reason=run_row["replayable_reason"],
            created_at=run_row["created_at"],
            completed_at=run_row["completed_at"],
        )
        return RunHistory(definition, run, tasks, attempts, events)

    @staticmethod
    def _task_from_row(row: dict[str, Any]) -> Task:
        boundary = row.get("accepted_boundary")
        return Task(
            task_id=str(row["task_id"]),
            run_id=str(row["run_id"]),
            module_id=row["module_id"],
            logical_step=row["logical_step"],
            step_instance_id=row["step_instance_id"],
            module_digest=row["module_digest"],
            dependency_instance_keys=tuple(row["dependency_instance_keys"]),
            input_value=StoredValue.from_data(row["task_input"]),
            status=TaskStatus(row["status"]),
            accepted_attempt_id=str(row["accepted_attempt_id"])
            if row.get("accepted_attempt_id")
            else None,
            accepted_boundary=BoundaryRecord.from_data(boundary) if boundary else None,
        )

    @staticmethod
    def _attempt_from_row(row: dict[str, Any]) -> Attempt:
        return Attempt(
            attempt_id=str(row["attempt_id"]),
            task_id=str(row["task_id"]),
            attempt_number=row["attempt_number"],
            lease_token=str(row["lease_token"]),
            status=AttemptStatus(row["status"]),
            diagnostic=row["diagnostic"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
        )

    @staticmethod
    def _event_from_row(row: dict[str, Any]) -> Event:
        return Event(
            event_id=row["event_id"],
            run_id=str(row["run_id"]),
            event_type=row["event_type"],
            payload=row["payload"],
            task_id=str(row["task_id"]) if row["task_id"] else None,
            attempt_id=str(row["attempt_id"]) if row["attempt_id"] else None,
            created_at=row["created_at"],
        )


def blank_boundary(
    *,
    workflow_id: str,
    definition_digest: str,
    claim: ClaimedTask,
    input_value: StoredValue,
    output_value: StoredValue,
) -> BoundaryRecord:
    """Construct a minimal accepted boundary for persistence integration tests.

    This helper is intended for store conformance tests that exercise leasing
    and compare-and-swap behavior without invoking a module handler.
    """
    now = datetime.now(UTC).isoformat()
    return BoundaryRecord(
        workflow_id=workflow_id,
        definition_digest=definition_digest,
        module_id=claim.task.module_id,
        logical_step=claim.task.logical_step,
        step_instance_id=claim.task.step_instance_id,
        module_digest=claim.task.module_digest,
        dependency_instance_keys=claim.task.dependency_instance_keys,
        input_value=input_value,
        output_value=output_value,
        input_schema_digest=input_value.schema_digest,
        output_schema_digest=output_value.schema_digest,
        accepted_attempt=AcceptedAttemptProvenance(
            claim.attempt.attempt_id,
            claim.attempt.attempt_number,
            claim.worker_id,
            claim.attempt.started_at.isoformat() if claim.attempt.started_at else now,
            now,
        ),
    )
