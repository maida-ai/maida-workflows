from __future__ import annotations

import json
import os
from collections import Counter
from dataclasses import asdict, dataclass, field, replace
from enum import StrEnum
from pathlib import Path
from typing import Any, Never, Protocol, cast

from ._canonical import canonical_data, canonical_json, digest_data
from .alignment import project_execution_path
from .artifacts import ArtifactError, ArtifactStore, ValueCodec
from .ir import PlanIR
from .models import (
    BoundaryRecord,
    ExecutionMode,
    RunHistory,
    RunStatus,
    StoredValue,
    TaskStatus,
    ValueStorage,
)
from .persistence import PostgresStore

FIXTURE_VERSION = "0.1.0"


class FixtureErrorCode(StrEnum):
    TRACE_NOT_REPLAYABLE = "TRACE_NOT_REPLAYABLE"
    RUN_NOT_TERMINAL = "RUN_NOT_TERMINAL"
    RUN_NOT_SUCCESSFUL = "RUN_NOT_SUCCESSFUL"
    HISTORY_INCOMPLETE = "HISTORY_INCOMPLETE"
    VALUE_UNAVAILABLE = "VALUE_UNAVAILABLE"
    ARTIFACT_INTEGRITY = "ARTIFACT_INTEGRITY"
    FIXTURE_INVALID = "FIXTURE_INVALID"
    FIXTURE_VERSION_UNSUPPORTED = "FIXTURE_VERSION_UNSUPPORTED"


class ReplayFixtureError(RuntimeError):
    def __init__(self, code: FixtureErrorCode, message: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {message}")


@dataclass(frozen=True)
class SourceProvenance:
    kind: str
    run_id: str
    tenant_id: str
    execution_mode: str
    completed_at: str


@dataclass(frozen=True)
class ArtifactIntegrity:
    digest: str
    size_bytes: int
    media_type: str


@dataclass(frozen=True)
class ReplayFixture:
    version: str
    source: SourceProvenance
    workflow_ir: PlanIR
    root_input: StoredValue
    root_output: StoredValue
    boundaries: tuple[BoundaryRecord, ...]
    control_decisions: tuple[dict[str, Any], ...]
    artifacts: tuple[ArtifactIntegrity, ...]
    values: ValueCodec = field(compare=False, repr=False)
    bundle_path: Path | None = field(default=None, compare=False)

    def to_manifest(self) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            canonical_data(
                {
                    "version": self.version,
                    "source": asdict(self.source),
                    "workflow": {
                        "workflow_id": self.workflow_ir.workflow_id,
                        "digest": self.workflow_ir.digest,
                        "ir": self.workflow_ir.to_dict(),
                    },
                    "root_input": self.root_input.to_data(),
                    "root_output": self.root_output.to_data(),
                    "executed_graph": [
                        {
                            "instance_key": boundary.instance_key,
                            "module_id": boundary.module_id,
                            "logical_step": boundary.logical_step,
                            "step_instance_id": boundary.step_instance_id,
                            "module_digest": boundary.module_digest,
                            "dependency_instance_keys": boundary.dependency_instance_keys,
                            "input_schema_digest": boundary.input_schema_digest,
                            "output_schema_digest": boundary.output_schema_digest,
                        }
                        for boundary in self.boundaries
                    ],
                    "accepted_steps": [boundary.to_data() for boundary in self.boundaries],
                    "control_decisions": self.control_decisions,
                    "artifacts": [asdict(artifact) for artifact in self.artifacts],
                }
            ),
        )

    @property
    def digest(self) -> str:
        return digest_data(self.to_manifest())


class ReplayFixtureImporter(Protocol):
    def import_source(self, source: str) -> ReplayFixture: ...


class NativeRunFixtureImporter:
    def __init__(self, store: PostgresStore, *, tenant_id: str) -> None:
        self.store = store
        self.tenant_id = tenant_id

    def import_source(self, source: str) -> ReplayFixture:
        try:
            history = self.store.load_run_history(source, tenant_id=self.tenant_id)
        except Exception as exc:
            raise ReplayFixtureError(
                FixtureErrorCode.TRACE_NOT_REPLAYABLE,
                "source is not a completed native Maida Workflow run ID",
            ) from exc
        return ReplayFixtureExporter(self.store.values).project(history)


class CanonicalBundleImporter:
    def import_source(self, source: str) -> ReplayFixture:
        return load_fixture(Path(source))


class ReplayFixtureExporter:
    def __init__(self, source_values: ValueCodec) -> None:
        self.source_values = source_values

    def project(self, history: RunHistory) -> ReplayFixture:
        self._validate_history(history)
        if history.run.root_output is None or history.run.completed_at is None:
            raise ReplayFixtureError(
                FixtureErrorCode.HISTORY_INCOMPLETE,
                "successful run is missing terminal root output",
            )
        values = [history.run.root_input, history.run.root_output]
        for boundary in history.accepted_boundaries:
            values.extend((boundary.input_value, boundary.output_value))
        artifacts: dict[str, ArtifactIntegrity] = {}
        for stored in values:
            self._validate_value(stored)
            if stored.storage is ValueStorage.ARTIFACT and stored.artifact_digest:
                content = self.source_values.bytes(stored)
                artifacts[stored.artifact_digest] = ArtifactIntegrity(
                    stored.artifact_digest,
                    len(content),
                    stored.media_type,
                )
        controls = tuple(
            {"event_type": event.event_type, "payload": event.payload}
            for event in history.events
            if event.event_type in {"BRANCH_DECISION", "MAP_DECISION"}
        )
        return ReplayFixture(
            version=FIXTURE_VERSION,
            source=SourceProvenance(
                kind="native_workflow_run",
                run_id=history.run.run_id,
                tenant_id=history.run.tenant_id,
                execution_mode=history.run.execution_mode.value,
                completed_at=history.run.completed_at.isoformat(),
            ),
            workflow_ir=PlanIR.from_dict(history.definition.canonical_ir),
            root_input=history.run.root_input,
            root_output=history.run.root_output,
            boundaries=history.accepted_boundaries,
            control_decisions=controls,
            artifacts=tuple(artifacts[digest] for digest in sorted(artifacts)),
            values=self.source_values,
        )

    def export(self, history: RunHistory, output: Path) -> ReplayFixture:
        projected = self.project(history)
        try:
            output.mkdir(mode=0o700, parents=True, exist_ok=False)
        except FileExistsError as exc:
            raise ReplayFixtureError(
                FixtureErrorCode.FIXTURE_INVALID,
                f"output already exists: {output}",
            ) from exc
        os.chmod(output, 0o700)
        bundle_values = ValueCodec(ArtifactStore(output / "blobs"), inline_limit=0)
        for artifact in projected.artifacts:
            content = self.source_values.artifacts.get(artifact.digest)
            actual = bundle_values.artifacts.put(content)
            if actual != artifact.digest:  # pragma: no cover - content addressing guarantees this
                raise ReplayFixtureError(
                    FixtureErrorCode.ARTIFACT_INTEGRITY,
                    f"artifact digest changed while exporting {artifact.digest}",
                )
        manifest = canonical_json(projected.to_manifest()).encode()
        manifest_path = output / "manifest.json"
        descriptor = os.open(manifest_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(manifest)
            stream.flush()
            os.fsync(stream.fileno())
        return replace(projected, values=bundle_values, bundle_path=output)

    def _validate_history(self, history: RunHistory) -> None:
        if history.run.status is not RunStatus.SUCCEEDED:
            code = (
                FixtureErrorCode.RUN_NOT_TERMINAL
                if history.run.status in {RunStatus.PENDING, RunStatus.RUNNING, RunStatus.PAUSED}
                else FixtureErrorCode.RUN_NOT_SUCCESSFUL
            )
            raise ReplayFixtureError(
                code,
                f"only successful terminal runs are replayable (found {history.run.status.value})",
            )
        if history.run.execution_mode not in {ExecutionMode.LIVE, ExecutionMode.VERIFY_LIVE}:
            raise ReplayFixtureError(
                FixtureErrorCode.TRACE_NOT_REPLAYABLE,
                "replay-mode runs cannot become native source fixtures",
            )
        if any(
            task.status is not TaskStatus.SUCCEEDED or task.accepted_boundary is None
            for task in history.tasks
        ):
            raise ReplayFixtureError(
                FixtureErrorCode.HISTORY_INCOMPLETE,
                "every executed task requires one accepted boundary record",
            )
        controls = tuple(
            {"event_type": event.event_type, "payload": event.payload}
            for event in history.events
            if event.event_type in {"BRANCH_DECISION", "MAP_DECISION"}
        )
        executed_plan = project_execution_path(
            PlanIR.from_dict(history.definition.canonical_ir),
            controls,
        )
        decisions_by_node: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for decision in controls:
            payload = cast(dict[str, Any], decision["payload"])
            identity = (
                str(decision["event_type"]),
                str(payload.get("control_node")),
            )
            decisions_by_node.setdefault(identity, []).append(payload)

        expected: Counter[tuple[str, str]] = Counter()
        for step in executed_plan.steps:
            if step.kind == "when":
                branches = decisions_by_node.get(("BRANCH_DECISION", step.node_id), [])
                if len(branches) != 1 or branches[0].get("selected") not in {"true", "false"}:
                    self._history_incomplete(
                        f"control node {step.node_id!r} requires one valid branch decision"
                    )
            if step.replay_key is None:
                continue
            key = (step.replay_key.module_id, step.replay_key.logical_step)
            if step.kind != "map_module":
                expected[key] += 1
                continue
            maps = decisions_by_node.get(("MAP_DECISION", step.node_id), [])
            if len(maps) != 1 or not isinstance(maps[0].get("item_keys"), list):
                self._history_incomplete(
                    f"map node {step.node_id!r} requires one valid item-key decision"
                )
            item_keys = maps[0]["item_keys"]
            if len(item_keys) != len(set(map(str, item_keys))):
                self._history_incomplete(f"map node {step.node_id!r} has duplicate item keys")
            expected[key] += len(item_keys)

        actual = Counter(
            (boundary.module_id, boundary.logical_step) for boundary in history.accepted_boundaries
        )
        if actual != expected:
            self._history_incomplete("accepted boundaries do not cover the recorded execution path")

    @staticmethod
    def _history_incomplete(message: str) -> Never:
        raise ReplayFixtureError(FixtureErrorCode.HISTORY_INCOMPLETE, message)

    def _validate_value(self, value: StoredValue) -> None:
        if value.storage is ValueStorage.UNAVAILABLE:
            raise ReplayFixtureError(
                FixtureErrorCode.VALUE_UNAVAILABLE,
                value.unavailable_reason or "required value is unavailable",
            )
        try:
            self.source_values.bytes(value)
        except ArtifactError as exc:
            raise ReplayFixtureError(FixtureErrorCode.ARTIFACT_INTEGRITY, str(exc)) from exc


def load_fixture(path: Path) -> ReplayFixture:
    manifest_path = path / "manifest.json" if path.is_dir() else path
    if not manifest_path.is_file():
        raise ReplayFixtureError(
            FixtureErrorCode.TRACE_NOT_REPLAYABLE,
            "ordinary Maida, OpenTelemetry, Langfuse, and maida export traces do not "
            "contain replay-complete module boundaries",
        )
    try:
        content = manifest_path.read_bytes()
        data = json.loads(content)
    except (OSError, json.JSONDecodeError) as exc:
        raise ReplayFixtureError(
            FixtureErrorCode.FIXTURE_INVALID,
            "fixture manifest is unreadable or invalid JSON",
        ) from exc
    if content != canonical_json(data).encode():
        raise ReplayFixtureError(
            FixtureErrorCode.FIXTURE_INVALID,
            "fixture manifest is not canonical JSON",
        )
    if data.get("version") != FIXTURE_VERSION:
        raise ReplayFixtureError(
            FixtureErrorCode.FIXTURE_VERSION_UNSUPPORTED,
            f"expected fixture {FIXTURE_VERSION}, found {data.get('version')!r}",
        )
    try:
        workflow = data["workflow"]
        plan = PlanIR.from_dict(workflow["ir"])
        if plan.digest != workflow["digest"]:
            raise ReplayFixtureError(
                FixtureErrorCode.FIXTURE_INVALID,
                "source workflow IR digest does not match its manifest",
            )
        bundle_root = manifest_path.parent
        codec = ValueCodec(ArtifactStore(bundle_root / "blobs", create=False), inline_limit=0)
        fixture = ReplayFixture(
            version=data["version"],
            source=SourceProvenance(**data["source"]),
            workflow_ir=plan,
            root_input=StoredValue.from_data(data["root_input"]),
            root_output=StoredValue.from_data(data["root_output"]),
            boundaries=tuple(BoundaryRecord.from_data(item) for item in data["accepted_steps"]),
            control_decisions=tuple(data["control_decisions"]),
            artifacts=tuple(ArtifactIntegrity(**item) for item in data["artifacts"]),
            values=codec,
            bundle_path=bundle_root,
        )
    except ReplayFixtureError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise ReplayFixtureError(
            FixtureErrorCode.FIXTURE_INVALID,
            "fixture manifest does not satisfy ReplayFixture 0.1.0",
        ) from exc
    if fixture.to_manifest() != data:
        raise ReplayFixtureError(
            FixtureErrorCode.FIXTURE_INVALID,
            "fixture manifest contains inconsistent derived graph data",
        )
    _validate_loaded_integrity(fixture)
    return fixture


def _validate_loaded_integrity(fixture: ReplayFixture) -> None:
    expected = {artifact.digest: artifact for artifact in fixture.artifacts}
    values = [fixture.root_input, fixture.root_output]
    for boundary in fixture.boundaries:
        values.extend((boundary.input_value, boundary.output_value))
    for stored in values:
        if stored.storage is ValueStorage.UNAVAILABLE:
            raise ReplayFixtureError(
                FixtureErrorCode.VALUE_UNAVAILABLE,
                stored.unavailable_reason or "required value is unavailable",
            )
        try:
            content = fixture.values.bytes(stored)
        except ArtifactError as exc:
            raise ReplayFixtureError(FixtureErrorCode.ARTIFACT_INTEGRITY, str(exc)) from exc
        if stored.storage is ValueStorage.ARTIFACT:
            if stored.artifact_digest not in expected:
                raise ReplayFixtureError(
                    FixtureErrorCode.ARTIFACT_INTEGRITY,
                    f"artifact {stored.artifact_digest} is absent from integrity metadata",
                )
            integrity = expected[stored.artifact_digest]
            if len(content) != integrity.size_bytes:
                raise ReplayFixtureError(
                    FixtureErrorCode.ARTIFACT_INTEGRITY,
                    f"artifact {integrity.digest} size does not match its manifest",
                )
