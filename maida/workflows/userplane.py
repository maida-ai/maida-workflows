"""Start workflow runs and observe them through a stable application protocol.

The userplane is deliberately a projection over durable runtime state. It adds
ergonomic run handles, typed command values, and cursor-addressed events without
creating a second session, stream, or approval persistence model. The same
objects can back an in-process application, an HTTP adapter, or another
transport supplied by a third party.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, ClassVar, Protocol
from uuid import uuid4

from ._canonical import canonical_data
from .authoring import Workflow
from .models import Event, ExecutionMode, RunStatus
from .runtime import DurableRuntimeStore, WorkflowScheduler


class CommandType(StrEnum):
    """Stable wire names accepted by the workflow userplane."""

    SIGNAL = "signal"
    APPROVE = "approve"
    REJECT = "reject"
    INPUT = "input"
    PAUSE = "pause"
    RESUME = "resume"
    CANCEL = "cancel"
    RETRY = "retry"


class InteractionKind(StrEnum):
    """Durable reasons that a task may relinquish compute and await a command."""

    INPUT = "input"
    APPROVAL = "approval"
    SIGNAL = "signal"


@dataclass(frozen=True, kw_only=True)
class InteractionRequest:
    """Transport-neutral request that parks a running task.

    This object is runtime infrastructure rather than a workflow-authoring
    primitive. A future ``Approval`` or ``Input`` module can emit the same
    contract without changing persistence or frontend integrations.

    Parameters
    ----------
    request_id
        Stable identity used by input, approval, or signal commands.
    kind
        Interaction category and resulting durable task state.
    prompt
        Human-readable application prompt.
    schema_digest
        Optional declared input schema digest.
    signal_name
        Optional named signal required by a signal wait.
    metadata
        Canonical non-sensitive presentation metadata.
    """

    request_id: str
    kind: InteractionKind
    prompt: str
    schema_digest: str | None = None
    signal_name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate stable request addressing and signal requirements."""
        if not self.request_id.strip():
            raise ValueError("request_id must be non-empty")
        if not self.prompt.strip():
            raise ValueError("interaction prompt must be non-empty")
        if self.kind is InteractionKind.SIGNAL and not (self.signal_name or "").strip():
            raise ValueError("signal_name is required for a signal interaction")

    def to_data(self) -> dict[str, Any]:
        """Return a canonical request event payload."""
        payload = canonical_data(asdict(self))
        if not isinstance(payload, dict):  # pragma: no cover - dataclasses encode as objects
            raise TypeError("interaction request must encode as an object")
        return {key: value for key, value in payload.items() if value is not None}


@dataclass(frozen=True, kw_only=True)
class RunCommand:
    """Base value for an idempotent command addressed to a durable run.

    Parameters
    ----------
    command_id
        Caller-controlled idempotency key. Reusing it with the same command is
        safe; reusing it with different content is rejected.

    Notes
    -----
    Applications normally construct one of the concrete command subclasses.
    Commands contain canonical JSON-compatible data so any transport can carry
    the same contract.
    """

    command_type: ClassVar[CommandType]
    command_id: str = field(default_factory=lambda: str(uuid4()))

    def __post_init__(self) -> None:
        """Validate the stable command identity before transport encoding."""
        if not self.command_id.strip():
            raise ValueError("command_id must be a non-empty idempotency key")

    def to_data(self) -> dict[str, Any]:
        """Return the canonical JSON-compatible command envelope."""
        payload = canonical_data(asdict(self))
        if not isinstance(payload, dict):  # pragma: no cover - dataclasses encode as objects
            raise TypeError("command payload must encode as an object")
        return {
            "type": self.command_type.value,
            **{key: value for key, value in payload.items() if value is not None},
        }


@dataclass(frozen=True, kw_only=True)
class SignalCommand(RunCommand):
    """Deliver a named durable signal to a workflow run.

    Parameters
    ----------
    name
        Application-defined signal name.
    value
        Canonical JSON-compatible signal payload.
    command_id
        Caller-controlled idempotency key.
    """

    command_type: ClassVar[CommandType] = CommandType.SIGNAL
    name: str
    value: Any
    request_id: str | None = None

    def __post_init__(self) -> None:
        """Validate command and signal identities."""
        super().__post_init__()
        if not self.name.strip():
            raise ValueError("signal name must be non-empty")


@dataclass(frozen=True, kw_only=True)
class ApproveCommand(RunCommand):
    """Approve a specific durable interaction request.

    ``request_id`` addresses the request event rather than an ambient approval
    service, which keeps reconnects and retries deterministic.
    """

    command_type: ClassVar[CommandType] = CommandType.APPROVE
    request_id: str
    comment: str | None = None

    def __post_init__(self) -> None:
        """Validate command and request identities."""
        super().__post_init__()
        if not self.request_id.strip():
            raise ValueError("request_id must be non-empty")


@dataclass(frozen=True, kw_only=True)
class RejectCommand(RunCommand):
    """Reject a specific durable approval request with an optional reason."""

    command_type: ClassVar[CommandType] = CommandType.REJECT
    request_id: str
    reason: str | None = None

    def __post_init__(self) -> None:
        """Validate command and request identities."""
        super().__post_init__()
        if not self.request_id.strip():
            raise ValueError("request_id must be non-empty")


@dataclass(frozen=True, kw_only=True)
class InputCommand(RunCommand):
    """Provide typed application input for a specific parked request."""

    command_type: ClassVar[CommandType] = CommandType.INPUT
    request_id: str
    value: Any

    def __post_init__(self) -> None:
        """Validate command and request identities."""
        super().__post_init__()
        if not self.request_id.strip():
            raise ValueError("request_id must be non-empty")


@dataclass(frozen=True, kw_only=True)
class PauseCommand(RunCommand):
    """Prevent new work from being claimed for a running workflow."""

    command_type: ClassVar[CommandType] = CommandType.PAUSE
    reason: str | None = None


@dataclass(frozen=True, kw_only=True)
class ResumeCommand(RunCommand):
    """Allow schedulable work in a previously paused workflow to continue."""

    command_type: ClassVar[CommandType] = CommandType.RESUME


@dataclass(frozen=True, kw_only=True)
class CancelCommand(RunCommand):
    """Cancel future cancellable work without undoing committed effects."""

    command_type: ClassVar[CommandType] = CommandType.CANCEL
    reason: str | None = None


@dataclass(frozen=True, kw_only=True)
class RetryCommand(RunCommand):
    """Request another attempt for one failed logical task."""

    command_type: ClassVar[CommandType] = CommandType.RETRY
    task_id: str

    def __post_init__(self) -> None:
        """Validate command and task identities."""
        super().__post_init__()
        if not self.task_id.strip():
            raise ValueError("task_id must be non-empty")


_EVENT_NAMES = {
    "RUN_STARTED": "run.started",
    "RUN_COMPLETED": "run.completed",
    "RUN_FAILED": "run.failed",
    "RUN_CANCELLED": "run.cancelled",
    "RUN_PAUSED": "run.paused",
    "RUN_RESUMED": "run.resumed",
    "TASK_CREATED": "task.created",
    "TASK_READY": "task.ready",
    "ATTEMPT_STARTED": "task.started",
    "TASK_SUCCEEDED": "task.completed",
    "TASK_COMPLETED": "task.completed",
    "TASK_FAILED": "task.failed",
    "INPUT_REQUIRED": "input.required",
    "APPROVAL_REQUIRED": "approval.required",
    "OUTPUT_DELTA": "output.delta",
    "ARTIFACT_CREATED": "artifact.created",
}


@dataclass(frozen=True)
class RunEvent:
    """Canonical application event projected from one durable runtime event.

    Attributes
    ----------
    sequence
        Monotonic event ID used as a reconnect cursor.
    type
        Stable dotted event name such as ``run.started`` or ``task.completed``.
    run_id, task_id, attempt_id
        Durable runtime identities associated with the transition.
    data
        Canonical event payload. Sensitive module values are not added by the
        projection layer.
    created_at
        Database timestamp for the accepted transition.
    """

    sequence: int
    type: str
    run_id: str
    data: dict[str, Any]
    task_id: str | None = None
    attempt_id: str | None = None
    created_at: datetime | None = None

    @classmethod
    def from_runtime(cls, event: Event) -> RunEvent:
        """Project an internal event without changing its durable sequence."""
        name = _EVENT_NAMES.get(event.event_type, event.event_type.lower().replace("_", "."))
        payload = canonical_data(event.payload)
        if not isinstance(payload, dict):  # pragma: no cover - event payload contract is an object
            raise TypeError("runtime event payload must encode as an object")
        return cls(
            sequence=event.event_id,
            type=name,
            run_id=event.run_id,
            task_id=event.task_id,
            attempt_id=event.attempt_id,
            data=payload,
            created_at=event.created_at,
        )

    def to_data(self) -> dict[str, Any]:
        """Return the transport-neutral event envelope."""
        return {
            "sequence": self.sequence,
            "type": self.type,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "attempt_id": self.attempt_id,
            "data": self.data,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


@dataclass(frozen=True)
class EventPage:
    """One bounded page of ordered workflow events.

    Use :attr:`next_cursor` as the next call's ``after`` value. Empty pages
    preserve the requested cursor, which makes polling and reconnect loops
    straightforward.
    """

    events: tuple[RunEvent, ...]
    next_cursor: int
    has_more: bool


@dataclass(frozen=True)
class RunSnapshot:
    """Current user-facing state of a durable workflow run."""

    run_id: str
    status: RunStatus
    definition_digest: str
    output: Any = None


@dataclass(frozen=True)
class CommandReceipt:
    """Acknowledgement for an accepted or idempotently repeated command."""

    command_id: str
    sequence: int
    run_status: RunStatus
    task_id: str | None = None
    duplicate: bool = False

    @property
    def accepted(self) -> bool:
        """Return ``True`` because a receipt exists only for accepted commands."""
        return True


class _UserplaneStore(DurableRuntimeStore, Protocol):
    def list_events(
        self,
        run_id: str,
        *,
        tenant_id: str,
        after: int,
        limit: int,
    ) -> tuple[Event, ...]: ...

    def submit_command(
        self,
        run_id: str,
        *,
        tenant_id: str,
        command: dict[str, Any],
    ) -> tuple[Event, bool, RunStatus]: ...


@dataclass(frozen=True)
class WorkflowRun:
    """Handle for observing and commanding one durable workflow invocation.

    A handle is cheap and reconnectable: it stores only the run identity,
    tenant scope, and durable store adapter. It owns no execution state and has
    no affinity with the workers that execute the run.
    """

    _store: _UserplaneStore
    run_id: str
    tenant_id: str = "local"

    def snapshot(self) -> RunSnapshot:
        """Load the run's current status and terminal output, if available."""
        history = self._store.load_run_history(self.run_id, tenant_id=self.tenant_id)
        output = (
            self._store.values.decode(history.run.root_output)
            if history.run.root_output is not None
            else None
        )
        return RunSnapshot(
            run_id=self.run_id,
            status=history.run.status,
            definition_digest=history.run.definition_digest,
            output=output,
        )

    def events(self, *, after: int = 0, limit: int = 100) -> EventPage:
        """Read an ordered event page after a durable sequence cursor.

        Parameters
        ----------
        after
            Last sequence already observed. Zero reads from the beginning.
        limit
            Maximum events to return, from 1 through 1000.

        Returns
        -------
        EventPage
            Ordered events, reconnect cursor, and continuation indicator.
        """
        if after < 0:
            raise ValueError("after cursor must be non-negative")
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        loaded = self._store.list_events(
            self.run_id,
            tenant_id=self.tenant_id,
            after=after,
            limit=limit + 1,
        )
        has_more = len(loaded) > limit
        projected = tuple(RunEvent.from_runtime(event) for event in loaded[:limit])
        cursor = projected[-1].sequence if projected else after
        return EventPage(projected, cursor, has_more)

    def send(self, command: RunCommand) -> CommandReceipt:
        """Validate and durably apply one idempotent typed command.

        Repeating the same ``command_id`` and content returns a duplicate
        receipt without applying the transition twice. Reusing an ID with
        different content fails explicitly.
        """
        event, duplicate, status = self._store.submit_command(
            self.run_id,
            tenant_id=self.tenant_id,
            command=command.to_data(),
        )
        return CommandReceipt(
            command_id=command.command_id,
            sequence=event.event_id,
            run_status=status,
            task_id=event.task_id,
            duplicate=duplicate,
        )


class WorkflowClient:
    """Application-facing entry point for durable workflow runs.

    Parameters
    ----------
    store
        Durable runtime store shared with schedulers and workers.

    Notes
    -----
    :meth:`start` performs compilation and scheduling only. Module handlers are
    executed later by eligible workers, so an API process can remain entirely
    separate from the execution fleet.
    """

    def __init__(self, store: _UserplaneStore) -> None:
        self.store = store

    def start[InputT, OutputT](
        self,
        workflow: Workflow[InputT, OutputT],
        value: InputT,
        *,
        tenant_id: str = "local",
        execution_mode: ExecutionMode = ExecutionMode.LIVE,
    ) -> WorkflowRun:
        """Create and schedule a run, returning its durable identity immediately.

        The call validates the root input, persists the definition and run, and
        performs one non-executing scheduler pass so dependency-free tasks are
        ready for remote executors.
        """
        scheduler = WorkflowScheduler.submit(
            self.store,
            workflow,
            value,
            tenant_id=tenant_id,
            execution_mode=execution_mode,
        )
        scheduler.advance()
        return WorkflowRun(self.store, scheduler.run_id, tenant_id)

    def attach(self, run_id: str, *, tenant_id: str = "local") -> WorkflowRun:
        """Create a reconnectable handle for an existing durable run."""
        if not run_id.strip():
            raise ValueError("run_id must be non-empty")
        return WorkflowRun(self.store, run_id, tenant_id)
