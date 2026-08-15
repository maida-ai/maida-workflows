from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Self, cast

from ._canonical import canonical_data


class ExecutionMode(StrEnum):
    LIVE = "LIVE"
    REPLAY_FULL_STUB = "REPLAY_FULL_STUB"
    REPLAY_SELECTIVE = "REPLAY_SELECTIVE"
    VERIFY_LIVE = "VERIFY_LIVE"


class RunStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    PAUSED = "PAUSED"


class TaskStatus(StrEnum):
    PENDING = "PENDING"
    LEASED = "LEASED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class AttemptStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ABANDONED = "ABANDONED"


class EffectKind(StrEnum):
    ATTEMPTED = "EFFECT_ATTEMPTED"
    COMMITTED = "EFFECT_COMMITTED"


class ValueStorage(StrEnum):
    INLINE = "inline"
    ARTIFACT = "artifact"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class StoredValue:
    schema_digest: str
    digest: str
    storage: ValueStorage
    inline: Any = None
    artifact_digest: str | None = None
    media_type: str = "application/json"
    unavailable_reason: str | None = None

    def to_data(self) -> dict[str, Any]:
        return cast(dict[str, Any], canonical_data(asdict(self)))

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> Self:
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
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    latency_ms: float = 0.0


@dataclass(frozen=True)
class TrajectoryRecord:
    kind: str
    name: str
    request_digest: str
    response_digest: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EffectRecord:
    kind: EffectKind
    adapter: str
    operation: str
    request_digest: str
    result_digest: str | None = None


@dataclass(frozen=True)
class AcceptedAttemptProvenance:
    attempt_id: str
    attempt_number: int
    worker_id: str
    started_at: str
    completed_at: str


@dataclass(frozen=True)
class BoundaryRecord:
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
        return f"{self.module_id}@{self.logical_step}#{self.step_instance_id}"

    def to_data(self) -> dict[str, Any]:
        return cast(dict[str, Any], canonical_data(asdict(self)))

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> Self:
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
    digest: str
    workflow_id: str
    ir_version: str
    canonical_ir: dict[str, Any]
    created_at: datetime | None = None


@dataclass(frozen=True)
class Run:
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
    task_id: str
    run_id: str
    module_id: str
    logical_step: str
    step_instance_id: str
    module_digest: str
    dependency_instance_keys: tuple[str, ...]
    input_value: StoredValue
    status: TaskStatus
    accepted_attempt_id: str | None = None
    accepted_boundary: BoundaryRecord | None = None


@dataclass(frozen=True)
class Attempt:
    attempt_id: str
    task_id: str
    attempt_number: int
    lease_token: str
    status: AttemptStatus
    diagnostic: dict[str, Any] | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


@dataclass(frozen=True)
class Event:
    event_id: int
    run_id: str
    event_type: str
    payload: dict[str, Any]
    task_id: str | None = None
    attempt_id: str | None = None
    created_at: datetime | None = None


@dataclass(frozen=True)
class Artifact:
    digest: str
    size_bytes: int
    media_type: str
    relative_path: str
    created_at: datetime | None = None


@dataclass(frozen=True)
class RunHistory:
    definition: Definition
    run: Run
    tasks: tuple[Task, ...]
    attempts: tuple[Attempt, ...]
    events: tuple[Event, ...]

    @property
    def accepted_boundaries(self) -> tuple[BoundaryRecord, ...]:
        return tuple(task.accepted_boundary for task in self.tasks if task.accepted_boundary)
