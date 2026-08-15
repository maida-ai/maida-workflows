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
    ExecutionSpec,
    ExecutorCapabilities,
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
        input_value: StoredValue | None,
        dependency_instance_keys: tuple[str, ...] = (),
        dependency_node_ids: tuple[str, ...] | None = None,
        capability_grant: tuple[str, ...] | None = None,
        branch_decisions: tuple[dict[str, Any], ...] = (),
        map_decisions: tuple[dict[str, Any], ...] = (),
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
        execution = ExecutionSpec.from_data(dict(step.execution or {}))
        grant = execution.capabilities if capability_grant is None else capability_grant
        initial_status = TaskStatus.READY if input_value is not None else TaskStatus.BLOCKED
        dependencies = dependency_node_ids if dependency_node_ids is not None else step.dependencies
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT status FROM workflow_runs WHERE run_id = %s FOR SHARE",
                (run_id,),
            )
            run_row = cursor.fetchone()
            if run_row is None:
                raise PersistenceError(f"run {run_id} was not found")
            if run_row["status"] != RunStatus.RUNNING.value:
                raise InvalidRunStateError(f"run {run_id} is not running")
            if input_value is not None:
                self._register_value_artifact(cursor, input_value)
            cursor.execute(
                """
                INSERT INTO workflow_tasks (
                    task_id, run_id, module_id, logical_step, step_instance_id,
                    module_digest, node_id, dependency_instance_keys,
                    dependency_node_ids, task_input, execution_requirements,
                    execution_isolation, execution_image, execution_cpu,
                    execution_memory_bytes, required_executor_capabilities,
                    capability_grant, branch_decisions, map_decisions, status, ready_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s,
                          CASE WHEN %s = 'READY' THEN now() ELSE NULL END)
                ON CONFLICT (run_id, module_id, logical_step, step_instance_id)
                DO NOTHING
                RETURNING *
                """,
                (
                    identifier,
                    run_id,
                    step.module_id,
                    step.logical_step,
                    step_instance_id,
                    step.module_digest,
                    step.node_id,
                    Jsonb(list(dependency_instance_keys)),
                    Jsonb(list(dependencies)),
                    Jsonb(input_value.to_data()) if input_value is not None else None,
                    Jsonb(execution.to_data()),
                    execution.isolation,
                    execution.image,
                    execution.cpu,
                    execution.memory_bytes,
                    Jsonb(list(execution.capabilities)),
                    Jsonb(list(grant)),
                    Jsonb(list(branch_decisions)),
                    Jsonb(list(map_decisions)),
                    initial_status.value,
                    initial_status.value,
                ),
            )
            row = cursor.fetchone()
            if row is None:
                cursor.execute(
                    """
                    SELECT * FROM workflow_tasks
                    WHERE run_id = %s AND module_id = %s AND logical_step = %s
                      AND step_instance_id = %s
                    """,
                    (run_id, step.module_id, step.logical_step, step_instance_id),
                )
                row = cursor.fetchone()
                if row is None:  # pragma: no cover - unique conflict guarantees a row
                    raise PersistenceError("idempotent task creation lost its existing task")
                recorded_input = (
                    StoredValue.from_data(row["task_input"]) if row["task_input"] else None
                )
                if (
                    input_value is not None
                    and recorded_input is not None
                    and input_value.digest != recorded_input.digest
                ):
                    raise InvalidRunStateError("task identity was reused with a different input")
            else:
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
                if initial_status is TaskStatus.READY:
                    self._append_event(cursor, run_id, "TASK_READY", {}, task_id=identifier)
        return self._task_from_row(row)

    def ready_task(
        self,
        task_id: str,
        *,
        input_value: StoredValue,
        dependency_instance_keys: tuple[str, ...],
        branch_decisions: tuple[dict[str, Any], ...] = (),
        map_decisions: tuple[dict[str, Any], ...] = (),
    ) -> Task:
        """Materialize a blocked task input and atomically make it claimable.

        The scheduler calls this only after every dependency has a durable
        accepted result. Workers never wait for dependencies themselves.
        """
        with self.connect() as connection, connection.cursor() as cursor:
            self._register_value_artifact(cursor, input_value)
            cursor.execute(
                """
                UPDATE workflow_tasks
                SET task_input = %s, dependency_instance_keys = %s,
                    branch_decisions = %s, map_decisions = %s,
                    status = 'READY', ready_at = now()
                FROM workflow_runs
                WHERE workflow_tasks.task_id = %s
                  AND workflow_tasks.status = 'BLOCKED'
                  AND workflow_runs.run_id = workflow_tasks.run_id
                  AND workflow_runs.status = 'RUNNING'
                RETURNING *
                """,
                (
                    Jsonb(input_value.to_data()),
                    Jsonb(list(dependency_instance_keys)),
                    Jsonb(list(branch_decisions)),
                    Jsonb(list(map_decisions)),
                    task_id,
                ),
            )
            row = cursor.fetchone()
            if row is None:
                cursor.execute("SELECT * FROM workflow_tasks WHERE task_id = %s", (task_id,))
                row = cursor.fetchone()
                if row is None:
                    raise PersistenceError(f"task {task_id} was not found")
                if row["status"] != TaskStatus.READY.value:
                    raise InvalidRunStateError(f"task {task_id} cannot become ready")
                recorded_input = StoredValue.from_data(row["task_input"])
                if (
                    recorded_input.digest != input_value.digest
                    or tuple(row["dependency_instance_keys"]) != dependency_instance_keys
                    or tuple(row["branch_decisions"]) != branch_decisions
                    or tuple(row["map_decisions"]) != map_decisions
                ):
                    raise InvalidRunStateError(
                        f"task {task_id} was concurrently readied with different inputs"
                    )
            else:
                self._append_event(cursor, str(row["run_id"]), "TASK_READY", {}, task_id=task_id)
        return self._task_from_row(row)

    def claim_task(
        self,
        *,
        worker_id: str,
        capabilities: ExecutorCapabilities | None = None,
        definition_digest: str | None = None,
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
        offered = capabilities or ExecutorCapabilities.local_process()
        lease_token = uuid4()
        attempt_id = uuid4()
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT t.* FROM workflow_tasks t
                JOIN workflow_runs r ON r.run_id = t.run_id
                WHERE (%s::uuid IS NULL OR t.task_id = %s::uuid)
                  AND (%s::text IS NULL OR r.definition_digest = %s::text)
                  AND r.status = 'RUNNING'
                  AND (
                    t.status = 'READY'
                    OR (t.status IN ('LEASED', 'RUNNING') AND t.lease_expires_at < now())
                  )
                  AND t.execution_isolation = ANY(%s)
                  AND (t.execution_image IS NULL OR t.execution_image = ANY(%s))
                  AND (t.execution_cpu IS NULL OR t.execution_cpu <= %s)
                  AND (t.execution_memory_bytes IS NULL OR t.execution_memory_bytes <= %s)
                  AND t.required_executor_capabilities <@ %s::jsonb
                ORDER BY COALESCE(t.ready_at, t.created_at), t.task_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """,
                (
                    task_id,
                    task_id,
                    definition_digest,
                    definition_digest,
                    list(offered.isolations),
                    list(offered.images),
                    offered.cpu if offered.cpu is not None else -1,
                    offered.memory_bytes if offered.memory_bytes is not None else -1,
                    Jsonb(sorted(offered.capabilities)),
                ),
            )
            selected = cursor.fetchone()
            if selected is None:
                return None
            selected_id = selected["task_id"]
            if selected["status"] in {TaskStatus.LEASED.value, TaskStatus.RUNNING.value}:
                cursor.execute(
                    """
                    UPDATE workflow_attempts
                    SET status = 'ABANDONED', completed_at = now(),
                        diagnostic = '{"reason":"lease expired"}'::jsonb
                    WHERE task_id = %s AND status IN ('CLAIMED', 'RUNNING')
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
                    attempt_id, task_id, attempt_number, lease_token, status, worker_id
                ) VALUES (%s, %s, %s, %s, 'CLAIMED', %s)
                RETURNING claimed_at
                """,
                (attempt_id, selected_id, attempt_number, lease_token, worker_id),
            )
            attempt_row = cursor.fetchone()
            self._append_event(
                cursor,
                str(row["run_id"]),
                "ATTEMPT_CLAIMED",
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
            status=AttemptStatus.CLAIMED,
            claimed_at=attempt_row["claimed_at"] if attempt_row else None,
        )
        return ClaimedTask(task, attempt, worker_id, expires_at)

    def start_task(self, claim: ClaimedTask) -> ClaimedTask:
        """Mark a claimed attempt running using its lease token."""
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE workflow_tasks SET status = 'RUNNING'
                WHERE task_id = %s AND status = 'LEASED' AND lease_token = %s
                  AND EXISTS (
                      SELECT 1 FROM workflow_runs
                      WHERE workflow_runs.run_id = workflow_tasks.run_id
                        AND workflow_runs.status = 'RUNNING'
                  )
                RETURNING *
                """,
                (claim.task.task_id, claim.attempt.lease_token),
            )
            row = cursor.fetchone()
            if row is None:
                raise StaleLeaseError(f"lease for task {claim.task.task_id} is stale")
            cursor.execute(
                """
                UPDATE workflow_attempts SET status = 'RUNNING', started_at = now()
                WHERE attempt_id = %s AND status = 'CLAIMED'
                RETURNING started_at
                """,
                (claim.attempt.attempt_id,),
            )
            attempt_row = cursor.fetchone()
            if attempt_row is None:
                raise StaleLeaseError(f"attempt {claim.attempt.attempt_id} cannot start")
            self._append_event(
                cursor,
                claim.task.run_id,
                "ATTEMPT_STARTED",
                {"worker_id": claim.worker_id},
                task_id=claim.task.task_id,
                attempt_id=claim.attempt.attempt_id,
            )
        return ClaimedTask(
            self._task_from_row(row),
            Attempt(
                attempt_id=claim.attempt.attempt_id,
                task_id=claim.attempt.task_id,
                attempt_number=claim.attempt.attempt_number,
                lease_token=claim.attempt.lease_token,
                status=AttemptStatus.RUNNING,
                claimed_at=claim.attempt.claimed_at,
                started_at=attempt_row["started_at"],
            ),
            claim.worker_id,
            claim.lease_expires_at,
        )

    def heartbeat_task(
        self,
        claim: ClaimedTask,
        *,
        lease_for: timedelta = timedelta(minutes=5),
    ) -> datetime:
        """Extend a live attempt lease and return the new deadline."""
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        expires_at = datetime.now(UTC) + lease_for
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE workflow_tasks SET lease_expires_at = %s
                WHERE task_id = %s AND status IN ('LEASED', 'RUNNING') AND lease_token = %s
                """,
                (expires_at, claim.task.task_id, claim.attempt.lease_token),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease for task {claim.task.task_id} is stale")
            self._append_event(
                cursor,
                claim.task.run_id,
                "ATTEMPT_HEARTBEAT",
                {"lease_expires_at": expires_at.isoformat()},
                task_id=claim.task.task_id,
                attempt_id=claim.attempt.attempt_id,
            )
        return expires_at

    def checkpoint_task(self, claim: ClaimedTask, checkpoint: StoredValue) -> None:
        """Persist an immutable checkpoint reference for a running attempt."""
        with self.connect() as connection, connection.cursor() as cursor:
            self._register_value_artifact(cursor, checkpoint)
            cursor.execute(
                """
                UPDATE workflow_attempts SET checkpoint_ref = %s
                WHERE attempt_id = %s AND status = 'RUNNING' AND lease_token = %s
                """,
                (Jsonb(checkpoint.to_data()), claim.attempt.attempt_id, claim.attempt.lease_token),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"attempt {claim.attempt.attempt_id} cannot checkpoint")
            self._append_event(
                cursor,
                claim.task.run_id,
                "CHECKPOINT_WRITTEN",
                {"checkpoint_digest": checkpoint.digest},
                task_id=claim.task.task_id,
                attempt_id=claim.attempt.attempt_id,
            )

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
                WHERE task_id = %s AND status = 'RUNNING' AND lease_token = %s
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
        next_status = TaskStatus.READY if retry else TaskStatus.FAILED
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE workflow_tasks
                SET status = %s, lease_owner = NULL, lease_token = NULL,
                    lease_expires_at = NULL
                WHERE task_id = %s AND status IN ('LEASED', 'RUNNING') AND lease_token = %s
                """,
                (next_status.value, claim.task.task_id, claim.attempt.lease_token),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease for task {claim.task.task_id} is stale")
            cursor.execute(
                """
                UPDATE workflow_attempts
                SET status = 'FAILED', diagnostic = %s, completed_at = now()
                WHERE attempt_id = %s AND status IN ('CLAIMED', 'RUNNING')
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

    def park_task(self, claim: ClaimedTask, request: dict[str, Any]) -> None:
        """Relinquish a running attempt while awaiting a durable command.

        The request becomes an append-only event and the logical task enters a
        non-claimable state. No worker or Python stack remains allocated. A
        matching accepted userplane command later returns the task to
        ``READY`` for a new physical attempt.

        Raises
        ------
        ValueError
            If the interaction kind or required request identity is invalid.
        StaleLeaseError
            If the attempt no longer owns the running task.
        """
        kind = request.get("kind")
        transitions = {
            "input": (TaskStatus.NEEDS_INPUT, "INPUT_REQUIRED"),
            "approval": (TaskStatus.NEEDS_APPROVAL, "APPROVAL_REQUIRED"),
            "signal": (TaskStatus.WAITING_SIGNAL, "SIGNAL_REQUIRED"),
        }
        if kind not in transitions:
            raise ValueError("interaction kind must be input, approval, or signal")
        if not str(request.get("request_id", "")).strip():
            raise ValueError("interaction request_id must be non-empty")
        next_status, event_type = transitions[str(kind)]
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE workflow_tasks
                SET status = %s, lease_owner = NULL, lease_token = NULL,
                    lease_expires_at = NULL
                WHERE task_id = %s AND status = 'RUNNING' AND lease_token = %s
                """,
                (next_status.value, claim.task.task_id, claim.attempt.lease_token),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease for task {claim.task.task_id} is stale")
            cursor.execute(
                """
                UPDATE workflow_attempts SET status = 'PARKED', completed_at = now()
                WHERE attempt_id = %s AND status = 'RUNNING' AND lease_token = %s
                """,
                (claim.attempt.attempt_id, claim.attempt.lease_token),
            )
            if cursor.rowcount != 1:  # pragma: no cover - task CAS protects the paired attempt
                raise StaleLeaseError(f"attempt {claim.attempt.attempt_id} cannot park")
            self._append_event(
                cursor,
                claim.task.run_id,
                event_type,
                request,
                task_id=claim.task.task_id,
                attempt_id=claim.attempt.attempt_id,
            )

    def submit_command(
        self,
        run_id: str,
        *,
        tenant_id: str,
        command: dict[str, Any],
    ) -> tuple[Event, bool, RunStatus]:
        """Apply one tenant-scoped command and transition in a transaction.

        Identical reuse of ``command_id`` returns the original command event.
        Reuse with different content fails closed. The command event and any
        resulting run/task transition are committed atomically.

        Returns
        -------
        tuple
            ``(command_event, duplicate, resulting_run_status)``.

        Raises
        ------
        InvalidRunStateError
            If the command identity conflicts, its target is unavailable, or
            the requested transition is invalid.
        TenantAccessError
            If the run belongs to a different tenant.
        """
        command_id = str(command.get("command_id", ""))
        command_type = str(command.get("type", ""))
        known = {"signal", "approve", "reject", "input", "pause", "resume", "cancel", "retry"}
        if not command_id.strip():
            raise ValueError("command_id must be non-empty")
        if command_type not in known:
            raise ValueError(f"unsupported command type {command_type!r}")
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM workflow_runs WHERE run_id = %s FOR UPDATE",
                (run_id,),
            )
            run_row = cursor.fetchone()
            if run_row is None:
                raise PersistenceError(f"run {run_id} was not found")
            if run_row["tenant_id"] != tenant_id:
                raise TenantAccessError("run is not accessible to this tenant")
            cursor.execute(
                """
                SELECT * FROM workflow_events
                WHERE run_id = %s AND event_type = 'COMMAND_RECEIVED'
                  AND payload->>'command_id' = %s
                """,
                (run_id, command_id),
            )
            existing = cursor.fetchone()
            if existing is not None:
                if existing["payload"].get("command") != command:
                    raise InvalidRunStateError(
                        f"command_id {command_id!r} was reused with different content"
                    )
                return self._event_from_row(existing), True, RunStatus(run_row["status"])

            run_status = RunStatus(run_row["status"])
            target_task_id: str | None = None
            if command_type in {"approve", "reject", "input"}:
                request_id = str(command.get("request_id", ""))
                event_type = "INPUT_REQUIRED" if command_type == "input" else "APPROVAL_REQUIRED"
                required_status = (
                    TaskStatus.NEEDS_INPUT if command_type == "input" else TaskStatus.NEEDS_APPROVAL
                )
                cursor.execute(
                    """
                    SELECT e.task_id FROM workflow_events e
                    JOIN workflow_tasks t ON t.task_id = e.task_id
                    WHERE e.run_id = %s AND e.event_type = %s
                      AND e.payload->>'request_id' = %s AND t.status = %s
                    ORDER BY e.event_id DESC LIMIT 1
                    """,
                    (run_id, event_type, request_id, required_status.value),
                )
                target = cursor.fetchone()
                if target is None:
                    raise InvalidRunStateError(
                        f"interaction request {request_id!r} is not awaiting {command_type}"
                    )
                target_task_id = str(target["task_id"])
            elif command_type == "retry":
                target_task_id = str(command.get("task_id", ""))
                cursor.execute(
                    """
                    SELECT task_id FROM workflow_tasks
                    WHERE run_id = %s AND task_id = %s AND status = 'FAILED'
                    FOR UPDATE
                    """,
                    (run_id, target_task_id),
                )
                if cursor.fetchone() is None:
                    raise InvalidRunStateError(
                        f"task {target_task_id!r} is not a failed task in this run"
                    )
            elif command_type == "signal":
                signal_request_id = command.get("request_id")
                if signal_request_id:
                    cursor.execute(
                        """
                        SELECT e.task_id FROM workflow_events e
                        JOIN workflow_tasks t ON t.task_id = e.task_id
                        WHERE e.run_id = %s AND e.event_type = 'SIGNAL_REQUIRED'
                          AND e.payload->>'request_id' = %s AND t.status = 'WAITING_SIGNAL'
                        ORDER BY e.event_id DESC LIMIT 1
                        """,
                        (run_id, signal_request_id),
                    )
                    target = cursor.fetchone()
                    if target is None:
                        raise InvalidRunStateError(
                            f"signal request {signal_request_id!r} is not awaiting a signal"
                        )
                    target_task_id = str(target["task_id"])

            cursor.execute(
                """
                INSERT INTO workflow_events (
                    run_id, task_id, event_type, payload
                ) VALUES (%s, %s, 'COMMAND_RECEIVED', %s)
                RETURNING *
                """,
                (run_id, target_task_id, Jsonb({"command_id": command_id, "command": command})),
            )
            command_row = cursor.fetchone()
            if command_row is None:  # pragma: no cover - INSERT RETURNING contract
                raise PersistenceError("accepted command event was not returned")

            resulting_status = run_status
            if command_type == "pause":
                if run_status is not RunStatus.RUNNING:
                    raise InvalidRunStateError("only a running run can be paused")
                cursor.execute(
                    "UPDATE workflow_runs SET status = 'PAUSED' WHERE run_id = %s",
                    (run_id,),
                )
                resulting_status = RunStatus.PAUSED
                self._append_event(cursor, run_id, "RUN_PAUSED", {"reason": command.get("reason")})
            elif command_type == "resume":
                if run_status is not RunStatus.PAUSED:
                    raise InvalidRunStateError("only a paused run can be resumed")
                cursor.execute(
                    "UPDATE workflow_runs SET status = 'RUNNING' WHERE run_id = %s",
                    (run_id,),
                )
                resulting_status = RunStatus.RUNNING
                self._append_event(cursor, run_id, "RUN_RESUMED", {})
            elif command_type == "cancel":
                if run_status not in {RunStatus.RUNNING, RunStatus.PAUSED}:
                    raise InvalidRunStateError("only a running or paused run can be cancelled")
                cursor.execute(
                    """
                    UPDATE workflow_tasks
                    SET status = 'CANCELLED', lease_owner = NULL, lease_token = NULL,
                        lease_expires_at = NULL, completed_at = now()
                    WHERE run_id = %s AND status IN (
                        'BLOCKED', 'READY', 'LEASED', 'RUNNING',
                        'NEEDS_INPUT', 'NEEDS_APPROVAL', 'WAITING_SIGNAL'
                    )
                    RETURNING task_id
                    """,
                    (run_id,),
                )
                cancelled_tasks = [str(row["task_id"]) for row in cursor.fetchall()]
                cursor.execute(
                    """
                    UPDATE workflow_attempts SET status = 'CANCELLED', completed_at = now(),
                        diagnostic = %s
                    WHERE task_id = ANY(%s::uuid[]) AND status IN ('CLAIMED', 'RUNNING')
                    """,
                    (Jsonb({"reason": command.get("reason")}), cancelled_tasks),
                )
                cursor.execute(
                    """
                    UPDATE workflow_runs SET status = 'CANCELLED', completed_at = now(),
                        replayable_reason = 'cancelled run'
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                for cancelled_task_id in cancelled_tasks:
                    self._append_event(
                        cursor,
                        run_id,
                        "TASK_CANCELLED",
                        {"reason": command.get("reason")},
                        task_id=cancelled_task_id,
                    )
                resulting_status = RunStatus.CANCELLED
                self._append_event(
                    cursor, run_id, "RUN_CANCELLED", {"reason": command.get("reason")}
                )
            elif command_type in {"approve", "reject", "input"}:
                if run_status not in {RunStatus.RUNNING, RunStatus.PAUSED}:
                    raise InvalidRunStateError("terminal runs cannot accept interaction commands")
                cursor.execute(
                    """
                    UPDATE workflow_tasks SET status = 'READY', ready_at = now()
                    WHERE task_id = %s AND status IN ('NEEDS_INPUT', 'NEEDS_APPROVAL')
                    """,
                    (target_task_id,),
                )
                domain_type = "INPUT_RECEIVED" if command_type == "input" else "APPROVAL_RESOLVED"
                payload = {key: value for key, value in command.items() if key != "type"}
                if command_type in {"approve", "reject"}:
                    payload["decision"] = command_type
                self._append_event(cursor, run_id, domain_type, payload, task_id=target_task_id)
                self._append_event(cursor, run_id, "TASK_READY", {}, task_id=target_task_id)
            elif command_type == "retry":
                if run_status not in {RunStatus.RUNNING, RunStatus.PAUSED}:
                    raise InvalidRunStateError("terminal runs cannot retry tasks")
                cursor.execute(
                    """
                    UPDATE workflow_tasks SET status = 'READY', ready_at = now(),
                        completed_at = NULL
                    WHERE task_id = %s AND status = 'FAILED'
                    """,
                    (target_task_id,),
                )
                self._append_event(
                    cursor,
                    run_id,
                    "TASK_RETRY_REQUESTED",
                    {"command_id": command_id},
                    task_id=target_task_id,
                )
            elif command_type == "signal":
                if run_status not in {RunStatus.RUNNING, RunStatus.PAUSED}:
                    raise InvalidRunStateError("terminal runs cannot accept signals")
                if target_task_id is not None:
                    cursor.execute(
                        """
                        UPDATE workflow_tasks SET status = 'READY', ready_at = now()
                        WHERE task_id = %s AND status = 'WAITING_SIGNAL'
                        """,
                        (target_task_id,),
                    )
                payload = {key: value for key, value in command.items() if key != "type"}
                self._append_event(
                    cursor, run_id, "SIGNAL_RECEIVED", payload, task_id=target_task_id
                )
                if target_task_id is not None:
                    self._append_event(cursor, run_id, "TASK_READY", {}, task_id=target_task_id)
            return self._event_from_row(command_row), False, resulting_status

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
                WHERE t.run_id = %s ORDER BY a.claimed_at, a.attempt_id
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

    def list_events(
        self,
        run_id: str,
        *,
        tenant_id: str,
        after: int = 0,
        limit: int = 100,
    ) -> tuple[Event, ...]:
        """Read tenant-scoped events after a monotonic sequence cursor.

        Parameters
        ----------
        run_id
            Durable run whose canonical event log should be projected.
        tenant_id
            Tenant scope required to access the run.
        after
            Exclusive event sequence cursor.
        limit
            Maximum rows to return.

        Raises
        ------
        PersistenceError
            If the run does not exist.
        TenantAccessError
            If the run belongs to another tenant.
        ValueError
            If the cursor or limit is invalid.
        """
        if after < 0:
            raise ValueError("after cursor must be non-negative")
        if limit < 1 or limit > 1001:
            raise ValueError("limit must be between 1 and 1001")
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT tenant_id FROM workflow_runs WHERE run_id = %s", (run_id,))
            run_row = cursor.fetchone()
            if run_row is None:
                raise PersistenceError(f"run {run_id} was not found")
            if run_row["tenant_id"] != tenant_id:
                raise TenantAccessError("run is not accessible to this tenant")
            cursor.execute(
                """
                SELECT * FROM workflow_events
                WHERE run_id = %s AND event_id > %s
                ORDER BY event_id
                LIMIT %s
                """,
                (run_id, after, limit),
            )
            return tuple(self._event_from_row(row) for row in cursor.fetchall())

    def list_active_runs(self, *, limit: int = 100) -> tuple[tuple[str, str, str], ...]:
        """Return running run identities for definition-pinned coordination.

        The control-plane query carries only run, tenant, and definition
        identities. Root values and application payloads remain in durable
        storage and are loaded only by the matching scheduler.
        """
        if limit < 1 or limit > 10_000:
            raise ValueError("limit must be between 1 and 10000")
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT run_id, tenant_id, definition_digest
                FROM workflow_runs
                WHERE status = 'RUNNING'
                ORDER BY created_at, run_id
                LIMIT %s
                """,
                (limit,),
            )
            return tuple(
                (str(row["run_id"]), row["tenant_id"], row["definition_digest"])
                for row in cursor.fetchall()
            )

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
            node_id=row.get("node_id", row["logical_step"]),
            dependency_instance_keys=tuple(row["dependency_instance_keys"]),
            dependency_node_ids=tuple(row.get("dependency_node_ids", ())),
            input_value=StoredValue.from_data(row["task_input"]) if row["task_input"] else None,
            execution=ExecutionSpec.from_data(dict(row.get("execution_requirements") or {})),
            capability_grant=tuple(row.get("capability_grant", ())),
            branch_decisions=tuple(row.get("branch_decisions", ())),
            map_decisions=tuple(row.get("map_decisions", ())),
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
            checkpoint=StoredValue.from_data(row["checkpoint_ref"])
            if row.get("checkpoint_ref")
            else None,
            diagnostic=row["diagnostic"],
            claimed_at=row.get("claimed_at"),
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
