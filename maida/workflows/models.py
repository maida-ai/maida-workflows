"""Immutable records shared by persistence, fixtures, runtime, and replay.

These data classes form the durable value and boundary contracts exposed to
workflow operators. They contain identifiers, digests, provenance, and usage;
large payload bytes remain in the configured artifact store.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Self, cast

from ._canonical import canonical_data


class ExecutionMode(StrEnum):
    """Execution context recorded for a workflow run."""

    LIVE = "LIVE"
    REPLAY_FULL_STUB = "REPLAY_FULL_STUB"
    REPLAY_SELECTIVE = "REPLAY_SELECTIVE"
    VERIFY_LIVE = "VERIFY_LIVE"


class RunStatus(StrEnum):
    """Lifecycle state of a durable workflow run."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    PAUSED = "PAUSED"


class TaskStatus(StrEnum):
    """Lifecycle state of one durable logical task."""

    BLOCKED = "BLOCKED"
    READY = "READY"
    LEASED = "LEASED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class AttemptStatus(StrEnum):
    """Outcome or current state of one leased task attempt."""

    CLAIMED = "CLAIMED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ABANDONED = "ABANDONED"


class EffectKind(StrEnum):
    """Boundary event identifying an attempted or committed effect."""

    ATTEMPTED = "EFFECT_ATTEMPTED"
    COMMITTED = "EFFECT_COMMITTED"


class ValueStorage(StrEnum):
    """Storage strategy used by a :class:`StoredValue`."""

    INLINE = "inline"
    ARTIFACT = "artifact"
    UNAVAILABLE = "unavailable"


_MEMORY_PATTERN = re.compile(r"^(?P<amount>[1-9][0-9]*)(?P<unit>KiB|MiB|GiB|TiB)$")
_MEMORY_MULTIPLIERS = {
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
}


@dataclass(frozen=True)
class ExecutionSpec:
    """Immutable environment requirements for a module attempt.

    Execution requirements are part of a module definition, not graph nodes.
    They describe the reproducible environment an executor must provide while
    leaving physical placement, worker identity, and VM allocation operational.

    Parameters
    ----------
    isolation
        Isolation mechanism required by the module. Supported values are
        ``process``, ``container``, ``vm``, and ``microvm``.
    image
        Immutable OCI-style image reference. Container and VM isolation require
        an ``@sha256:`` digest rather than a mutable tag.
    dependency_lock
        Optional immutable digest of the runtime dependency lock.
    cpu
        Minimum logical CPU count required by the task.
    memory
        Minimum memory using a binary unit such as ``512MiB`` or ``8GiB``.
    capabilities
        Named executor capabilities required before a task can be claimed.
    """

    isolation: str = "process"
    image: str | None = None
    dependency_lock: str | None = None
    cpu: int | None = None
    memory: str | None = None
    capabilities: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate that requirements are immutable and canonically representable."""
        if self.isolation not in {"process", "container", "vm", "microvm"}:
            raise ValueError("isolation must be process, container, vm, or microvm")
        if self.image is not None:
            marker, separator, digest = self.image.rpartition("@sha256:")
            if (
                not separator
                or not marker
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ValueError("execution image must use an immutable sha256 digest")
        if self.isolation != "process" and self.image is None:
            raise ValueError(f"{self.isolation} isolation requires an immutable image digest")
        if self.dependency_lock is not None:
            prefix, separator, digest = self.dependency_lock.partition(":")
            if (
                prefix != "sha256"
                or not separator
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ValueError("dependency_lock must be a sha256 digest")
        if self.cpu is not None and self.cpu < 1:
            raise ValueError("cpu must be positive")
        if self.memory is not None and _MEMORY_PATTERN.fullmatch(self.memory) is None:
            raise ValueError("memory must use a positive KiB, MiB, GiB, or TiB value")
        if any(not capability.strip() for capability in self.capabilities):
            raise ValueError("capabilities must contain non-empty names")
        if len(set(self.capabilities)) != len(self.capabilities):
            raise ValueError("capabilities must be unique")

    @property
    def memory_bytes(self) -> int | None:
        """Return the memory requirement in bytes for executor matching."""
        if self.memory is None:
            return None
        match = _MEMORY_PATTERN.fullmatch(self.memory)
        if match is None:  # pragma: no cover - validated during construction
            raise ValueError("invalid memory requirement")
        return int(match.group("amount")) * _MEMORY_MULTIPLIERS[match.group("unit")]

    def to_data(self) -> dict[str, Any]:
        """Return the canonical mapping persisted in IR and task envelopes."""
        return cast(dict[str, Any], canonical_data(asdict(self)))

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> Self:
        """Construct execution requirements from persisted canonical data."""
        return cls(
            isolation=str(data.get("isolation", "process")),
            image=data.get("image"),
            dependency_lock=data.get("dependency_lock"),
            cpu=int(data["cpu"]) if data.get("cpu") is not None else None,
            memory=data.get("memory"),
            capabilities=tuple(data.get("capabilities", ())),
        )


@dataclass(frozen=True)
class ExecutorCapabilities:
    """Resources and immutable environments offered by one executor.

    The scheduler uses this value only for eligibility. The executor's worker
    or VM identity is deliberately absent so physical placement cannot affect a
    module digest or workflow definition.
    """

    isolations: frozenset[str] = frozenset({"process"})
    images: frozenset[str] = frozenset()
    cpu: int | None = None
    memory: str | None = None
    capabilities: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        """Validate executor resource declarations."""
        unknown = self.isolations - {"process", "container", "vm", "microvm"}
        if unknown:
            raise ValueError(f"unknown executor isolation modes: {sorted(unknown)}")
        if self.cpu is not None and self.cpu < 1:
            raise ValueError("executor cpu must be positive")
        if self.memory is not None and _MEMORY_PATTERN.fullmatch(self.memory) is None:
            raise ValueError("executor memory must use KiB, MiB, GiB, or TiB")

    @classmethod
    def local_process(cls) -> Self:
        """Return capabilities for the built-in local process executor."""
        return cls(isolations=frozenset({"process"}))

    @property
    def memory_bytes(self) -> int | None:
        """Return available memory in bytes for requirement matching."""
        if self.memory is None:
            return None
        match = _MEMORY_PATTERN.fullmatch(self.memory)
        if match is None:  # pragma: no cover - validated during construction
            raise ValueError("invalid executor memory")
        return int(match.group("amount")) * _MEMORY_MULTIPLIERS[match.group("unit")]

    def supports(self, spec: ExecutionSpec) -> bool:
        """Return whether this executor can satisfy an execution specification."""
        if spec.isolation not in self.isolations:
            return False
        if spec.image is not None and spec.image not in self.images:
            return False
        if spec.cpu is not None and (self.cpu is None or self.cpu < spec.cpu):
            return False
        if spec.memory_bytes is not None and (
            self.memory_bytes is None or self.memory_bytes < spec.memory_bytes
        ):
            return False
        return set(spec.capabilities).issubset(self.capabilities)


@dataclass(frozen=True)
class StoredValue:
    """Typed reference to an inline, artifact-backed, or unavailable value.

    Attributes
    ----------
    schema_digest
        Digest of the declared Python type contract.
    digest
        Digest of the canonical JSON value bytes.
    storage
        Storage strategy used for the payload.
    inline
        Canonical JSON-compatible value when stored inline.
    artifact_digest
        Content address when the value is artifact-backed.
    media_type
        Media type of the encoded payload.
    unavailable_reason
        Explanation when a required value was redacted or otherwise lost.
    """

    schema_digest: str
    digest: str
    storage: ValueStorage
    inline: Any = None
    artifact_digest: str | None = None
    media_type: str = "application/json"
    unavailable_reason: str | None = None

    def to_data(self) -> dict[str, Any]:
        """Return a canonical JSON-compatible record for persistence."""
        return cast(dict[str, Any], canonical_data(asdict(self)))

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> Self:
        """Construct a stored-value reference from persisted record data."""
        return cls(
            schema_digest=str(data["schema_digest"]),
            digest=str(data["digest"]),
            storage=ValueStorage(data["storage"]),
            inline=data.get("inline"),
            artifact_digest=data.get("artifact_digest"),
            media_type=str(data.get("media_type", "application/json")),
            unavailable_reason=data.get("unavailable_reason"),
        )


@dataclass(frozen=True)
class Usage:
    """Token, monetary cost, and latency measurements for one boundary."""

    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    latency_ms: float = 0.0


@dataclass(frozen=True)
class TrajectoryRecord:
    """Canonical model or tool interaction observed within a module boundary."""

    kind: str
    name: str
    request_digest: str
    response_digest: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EffectRecord:
    """Attempted or committed runtime-managed external effect."""

    kind: EffectKind
    adapter: str
    operation: str
    request_digest: str
    result_digest: str | None = None


@dataclass(frozen=True)
class AcceptedAttemptProvenance:
    """Worker and timing provenance for the accepted logical result."""

    attempt_id: str
    attempt_number: int
    worker_id: str
    started_at: str
    completed_at: str


@dataclass(frozen=True)
class BoundaryRecord:
    """Replay-complete accepted result for one module execution instance.

    A boundary records stable definition and instance identities, exact typed
    input/output references, dependency instance keys, accepted-attempt
    provenance, trajectories, usage, control decisions, and effects. Retry
    attempts that were not accepted remain separate diagnostic history.

    Attributes
    ----------
    workflow_id, definition_digest
        Identity of the workflow definition that produced the result.
    module_id, logical_step, step_instance_id, module_digest
        Semantic, positional, execution-instance, and content identities.
    dependency_instance_keys
        Accepted boundary instances that supplied this step's input.
    input_value, output_value
        Immutable typed value references.
    accepted_attempt
        Provenance of the single attempt accepted for substitution.
    trajectories, usage, branch_decisions, map_decisions, effects
        Behavioral evidence captured at the boundary.
    """

    workflow_id: str
    definition_digest: str
    module_id: str
    logical_step: str
    step_instance_id: str
    module_digest: str
    dependency_instance_keys: tuple[str, ...]
    input_value: StoredValue
    output_value: StoredValue
    input_schema_digest: str
    output_schema_digest: str
    accepted_attempt: AcceptedAttemptProvenance
    trajectories: tuple[TrajectoryRecord, ...] = ()
    usage: Usage = field(default_factory=Usage)
    branch_decisions: tuple[dict[str, Any], ...] = ()
    map_decisions: tuple[dict[str, Any], ...] = ()
    effects: tuple[EffectRecord, ...] = ()

    @property
    def instance_key(self) -> str:
        """Return the canonical address of this concrete boundary instance."""
        return f"{self.module_id}@{self.logical_step}#{self.step_instance_id}"

    def to_data(self) -> dict[str, Any]:
        """Return a canonical JSON-compatible boundary representation."""
        return cast(dict[str, Any], canonical_data(asdict(self)))

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> Self:
        """Construct a boundary record from persisted or fixture data."""
        accepted = data["accepted_attempt"]
        return cls(
            workflow_id=str(data["workflow_id"]),
            definition_digest=str(data["definition_digest"]),
            module_id=str(data["module_id"]),
            logical_step=str(data["logical_step"]),
            step_instance_id=str(data["step_instance_id"]),
            module_digest=str(data["module_digest"]),
            dependency_instance_keys=tuple(data["dependency_instance_keys"]),
            input_value=StoredValue.from_data(data["input_value"]),
            output_value=StoredValue.from_data(data["output_value"]),
            input_schema_digest=str(data["input_schema_digest"]),
            output_schema_digest=str(data["output_schema_digest"]),
            accepted_attempt=AcceptedAttemptProvenance(**accepted),
            trajectories=tuple(TrajectoryRecord(**item) for item in data.get("trajectories", [])),
            usage=Usage(**data.get("usage", {})),
            branch_decisions=tuple(data.get("branch_decisions", [])),
            map_decisions=tuple(data.get("map_decisions", [])),
            effects=tuple(
                EffectRecord(
                    kind=EffectKind(item["kind"]), **{k: v for k, v in item.items() if k != "kind"}
                )
                for item in data.get("effects", [])
            ),
        )


@dataclass(frozen=True)
class Definition:
    """Persisted canonical workflow definition and its content digest."""

    digest: str
    workflow_id: str
    ir_version: str
    canonical_ir: dict[str, Any]
    created_at: datetime | None = None


@dataclass(frozen=True)
class Run:
    """Durable workflow-run record including root value references."""

    run_id: str
    tenant_id: str
    definition_digest: str
    execution_mode: ExecutionMode
    status: RunStatus
    root_input: StoredValue
    root_input_schema_digest: str
    root_output: StoredValue | None = None
    root_output_schema_digest: str | None = None
    replayable_reason: str | None = None
    created_at: datetime | None = None
    completed_at: datetime | None = None


@dataclass(frozen=True)
class Task:
    """Durable logical module task pinned to a module definition digest."""

    task_id: str
    run_id: str
    module_id: str
    logical_step: str
    step_instance_id: str
    module_digest: str
    node_id: str
    dependency_instance_keys: tuple[str, ...]
    dependency_node_ids: tuple[str, ...]
    input_value: StoredValue | None
    execution: ExecutionSpec
    capability_grant: tuple[str, ...]
    branch_decisions: tuple[dict[str, Any], ...]
    map_decisions: tuple[dict[str, Any], ...]
    status: TaskStatus
    accepted_attempt_id: str | None = None
    accepted_boundary: BoundaryRecord | None = None


@dataclass(frozen=True)
class Attempt:
    """One leased execution attempt for a durable task."""

    attempt_id: str
    task_id: str
    attempt_number: int
    lease_token: str
    status: AttemptStatus
    checkpoint: StoredValue | None = None
    diagnostic: dict[str, Any] | None = None
    claimed_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


@dataclass(frozen=True)
class Event:
    """Ordered diagnostic or control event emitted by a workflow run."""

    event_id: int
    run_id: str
    event_type: str
    payload: dict[str, Any]
    task_id: str | None = None
    attempt_id: str | None = None
    created_at: datetime | None = None


@dataclass(frozen=True)
class Artifact:
    """Metadata for an immutable content-addressed artifact."""

    digest: str
    size_bytes: int
    media_type: str
    relative_path: str
    created_at: datetime | None = None


@dataclass(frozen=True)
class RunHistory:
    """Definition, run, task, attempt, and event records for one run.

    Attributes
    ----------
    definition
        Canonical workflow definition used by the run.
    run
        Root run record and terminal state.
    tasks
        Logical module tasks created during execution.
    attempts
        Historical worker attempts, including failures and abandoned leases.
    events
        Ordered runtime and control-flow events.
    """

    definition: Definition
    run: Run
    tasks: tuple[Task, ...]
    attempts: tuple[Attempt, ...]
    events: tuple[Event, ...]

    @property
    def accepted_boundaries(self) -> tuple[BoundaryRecord, ...]:
        """Return the single accepted boundary from each successful task."""
        return tuple(task.accepted_boundary for task in self.tasks if task.accepted_boundary)
