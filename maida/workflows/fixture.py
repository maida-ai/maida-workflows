"""Project completed native runs into portable replay fixtures.

A replay fixture is a canonical manifest plus content-addressed blobs. This
module validates source history, preserves accepted module boundaries and
control decisions, exports bundles with restrictive permissions, and fails
closed when required values or integrity evidence are unavailable.
"""

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
from .dynamic import PlanFragmentIR, PlanSignature
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

FIXTURE_VERSION = "0.2.0"
LEGACY_FIXTURE_VERSION = "0.1.0"


class FixtureErrorCode(StrEnum):
    """Stable reason code for fixture import, export, or integrity failure."""

    TRACE_NOT_REPLAYABLE = "TRACE_NOT_REPLAYABLE"
    RUN_NOT_TERMINAL = "RUN_NOT_TERMINAL"
    RUN_NOT_SUCCESSFUL = "RUN_NOT_SUCCESSFUL"
    HISTORY_INCOMPLETE = "HISTORY_INCOMPLETE"
    VALUE_UNAVAILABLE = "VALUE_UNAVAILABLE"
    ARTIFACT_INTEGRITY = "ARTIFACT_INTEGRITY"
    FIXTURE_INVALID = "FIXTURE_INVALID"
    FIXTURE_VERSION_UNSUPPORTED = "FIXTURE_VERSION_UNSUPPORTED"


class ReplayFixtureError(RuntimeError):
    """Raised when a source cannot satisfy the replay fixture contract.

    Attributes
    ----------
    code
        Machine-readable :class:`FixtureErrorCode` describing the failure.
    """

    def __init__(self, code: FixtureErrorCode, message: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {message}")


@dataclass(frozen=True)
class SourceProvenance:
    """Origin and completion metadata for a replay fixture."""

    kind: str
    run_id: str
    tenant_id: str
    execution_mode: str
    completed_at: str


@dataclass(frozen=True)
class ArtifactIntegrity:
    """Digest, size, and media type of one fixture blob."""

    digest: str
    size_bytes: int
    media_type: str


@dataclass(frozen=True)
class GeneratedPlanRecord:
    """Replay-complete provenance for one materialized generated region.

    The source fragment bytes remain in the accepted planner boundary. This
    record binds that boundary to its trusted resolved signature and every
    generated node instance without duplicating planner payloads.
    """

    region_id: str
    region_instance_id: str
    source_task_id: str
    source_instance_key: str
    revision: int
    supersedes: str | None
    plan_digest: str
    signature: PlanSignature
    outputs: tuple[str, ...]
    node_instances: tuple[tuple[str, str], ...]

    def to_data(self) -> dict[str, Any]:
        """Return canonical plan provenance and resolved behavior data."""
        return {
            "node_instances": [
                {"instance_key": instance_key, "node_key": node_key}
                for node_key, instance_key in self.node_instances
            ],
            "outputs": list(self.outputs),
            "plan_digest": self.plan_digest,
            "region_id": self.region_id,
            "region_instance_id": self.region_instance_id,
            "revision": self.revision,
            "signature": self.signature.to_dict(),
            "source_instance_key": self.source_instance_key,
            "source_task_id": self.source_task_id,
            "supersedes": self.supersedes,
        }

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> GeneratedPlanRecord:
        """Restore strict generated-plan provenance from a fixture manifest."""
        expected = {
            "node_instances",
            "outputs",
            "plan_digest",
            "region_id",
            "region_instance_id",
            "revision",
            "signature",
            "source_instance_key",
            "source_task_id",
            "supersedes",
        }
        if not isinstance(data, dict) or set(data) != expected:
            raise ValueError("generated plan record fields are invalid")
        nodes = data["node_instances"]
        if not isinstance(nodes, list) or any(
            not isinstance(item, dict) or set(item) != {"instance_key", "node_key"}
            for item in nodes
        ):
            raise ValueError("generated plan node instances are invalid")
        record = cls(
            region_id=str(data["region_id"]),
            region_instance_id=str(data["region_instance_id"]),
            source_task_id=str(data["source_task_id"]),
            source_instance_key=str(data["source_instance_key"]),
            revision=int(data["revision"]),
            supersedes=data["supersedes"],
            plan_digest=str(data["plan_digest"]),
            signature=PlanSignature.from_dict(data["signature"]),
            outputs=tuple(data["outputs"]),
            node_instances=tuple(
                (str(item["node_key"]), str(item["instance_key"])) for item in nodes
            ),
        )
        if record.to_data() != data:
            raise ValueError("generated plan record is not canonical")
        return record


@dataclass(frozen=True)
class ReplayFixture:
    """Immutable replay projection of one successful native workflow run.

    Attributes
    ----------
    version
        Replay fixture schema version.
    source
        Native run provenance without external payload upload.
    workflow_ir
        Canonical workflow definition used by the source run.
    root_input, root_output
        Typed references to the recorded root values.
    boundaries
        Accepted replay-complete module boundary records.
    control_decisions
        Recorded branch and map decisions for the executed path.
    artifacts
        Integrity metadata for content-addressed blobs.
    values
        Codec used to resolve inline and artifact-backed value references.
    bundle_path
        Local bundle location when loaded from or exported to disk.
    """

    version: str
    source: SourceProvenance
    workflow_ir: PlanIR
    root_input: StoredValue
    root_output: StoredValue
    boundaries: tuple[BoundaryRecord, ...]
    control_decisions: tuple[dict[str, Any], ...]
    artifacts: tuple[ArtifactIntegrity, ...]
    values: ValueCodec = field(compare=False, repr=False)
    generated_plans: tuple[GeneratedPlanRecord, ...] = ()
    bundle_path: Path | None = field(default=None, compare=False)

    def to_manifest(self) -> dict[str, Any]:
        """Return the canonical JSON-compatible fixture manifest."""
        manifest = cast(
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
        if self.version == FIXTURE_VERSION:
            manifest["generated_plans"] = [record.to_data() for record in self.generated_plans]
        return manifest

    @property
    def digest(self) -> str:
        """Return the SHA-256 digest of the canonical fixture manifest."""
        return digest_data(self.to_manifest())


class ReplayFixtureImporter(Protocol):
    """Interface for converting an approved source into a replay fixture."""

    def import_source(self, source: str) -> ReplayFixture:
        """Import a source identifier or path as a validated fixture.

        Parameters
        ----------
        source
            Source-specific native run identifier or canonical bundle path.

        Returns
        -------
        ReplayFixture
            Validated fixture ready for replay.
        """
        ...


class NativeRunFixtureImporter:
    """Import completed native runs from a tenant-scoped PostgreSQL store."""

    def __init__(self, store: PostgresStore, *, tenant_id: str) -> None:
        self.store = store
        self.tenant_id = tenant_id

    def import_source(self, source: str) -> ReplayFixture:
        """Project a completed native run ID into an in-memory fixture.

        Raises
        ------
        ReplayFixtureError
            If the run is inaccessible, incomplete, or not replayable.
        """
        try:
            history = self.store.load_run_history(source, tenant_id=self.tenant_id)
        except Exception as exc:
            raise ReplayFixtureError(
                FixtureErrorCode.TRACE_NOT_REPLAYABLE,
                "source is not a completed native Maida Workflow run ID",
            ) from exc
        return ReplayFixtureExporter(self.store.values).project(history)


class CanonicalBundleImporter:
    """Load previously exported canonical fixture bundles from local storage."""

    def import_source(self, source: str) -> ReplayFixture:
        """Load and validate the bundle at ``source``."""
        return load_fixture(Path(source))


class ReplayFixtureExporter:
    """Validate native history and export deterministic private bundles.

    Parameters
    ----------
    source_values
        Codec capable of resolving every value referenced by the source run.
    """

    def __init__(self, source_values: ValueCodec) -> None:
        self.source_values = source_values

    def project(self, history: RunHistory) -> ReplayFixture:
        """Create an in-memory fixture projection from native run history.

        Parameters
        ----------
        history
            Successful terminal run with complete accepted boundaries and
            available root/module values.

        Returns
        -------
        ReplayFixture
            Canonical projection that still references the source value store.

        Raises
        ------
        ReplayFixtureError
            If status, execution coverage, values, or artifacts are incomplete.
        """
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
        generated = self._generated_records(history)
        return ReplayFixture(
            version=FIXTURE_VERSION if generated else LEGACY_FIXTURE_VERSION,
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
            generated_plans=generated,
        )

    def export(self, history: RunHistory, output: Path) -> ReplayFixture:
        """Write a canonical fixture bundle with restrictive permissions.

        Parameters
        ----------
        history
            Replay-complete successful native run history.
        output
            New local directory for ``manifest.json`` and blob content.

        Returns
        -------
        ReplayFixture
            Fixture bound to the newly written bundle.

        Raises
        ------
        ReplayFixtureError
            If the source is invalid, an artifact fails integrity checks, or
            the output path already exists.
        """
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

        generated_instances = {
            task.accepted_boundary.instance_key
            for task in history.tasks
            if task.plan_provenance is not None and task.accepted_boundary is not None
        }
        actual = Counter(
            (boundary.module_id, boundary.logical_step)
            for boundary in history.accepted_boundaries
            if boundary.instance_key not in generated_instances
        )
        if actual != expected:
            self._history_incomplete("accepted boundaries do not cover the recorded execution path")

        self._generated_records(history)

    def _generated_records(self, history: RunHistory) -> tuple[GeneratedPlanRecord, ...]:
        boundaries_by_task = {
            task.task_id: task.accepted_boundary
            for task in history.tasks
            if task.accepted_boundary is not None
        }
        tasks_by_id = {task.task_id: task for task in history.tasks}
        records: list[GeneratedPlanRecord] = []
        seen_instances: set[str] = set()
        for event in history.events:
            if event.event_type != "PLAN_MATERIALIZED":
                continue
            payload = event.payload
            try:
                signature = PlanSignature.from_dict(payload["signature"])
                source_task_id = str(payload["source_task_id"])
                source_boundary = boundaries_by_task[source_task_id]
                if source_boundary is None:
                    raise KeyError(source_task_id)
                source_data = self.source_values.decode(source_boundary.output_value)
                fragment = PlanFragmentIR.from_dict(cast(dict[str, Any], source_data))
                if fragment.digest != payload["plan_digest"]:
                    raise ValueError("accepted fragment digest changed")
                if (
                    signature.source_fragment_digest != fragment.digest
                    or signature.region_id != payload["region_id"]
                    or signature.revision != fragment.revision
                    or signature.supersedes != fragment.supersedes
                    or signature.outputs != fragment.outputs
                    or signature.digest != payload["signature_digest"]
                ):
                    raise ValueError("resolved plan signature changed")
                node_instances: list[tuple[str, str]] = []
                raw_nodes = payload["node_task_ids"]
                if not isinstance(raw_nodes, list):
                    raise ValueError("node task identities are invalid")
                for item in raw_nodes:
                    node_key = str(item["node_key"])
                    task = tasks_by_id[str(item["task_id"])]
                    provenance = task.plan_provenance
                    if (
                        provenance is None
                        or provenance.node_key != node_key
                        or provenance.plan_digest != fragment.digest
                        or task.accepted_boundary is None
                    ):
                        raise ValueError("generated task provenance is incomplete")
                    instance_key = task.accepted_boundary.instance_key
                    if instance_key in seen_instances:
                        raise ValueError("generated boundary instance is duplicated")
                    seen_instances.add(instance_key)
                    node_instances.append((node_key, instance_key))
                records.append(
                    GeneratedPlanRecord(
                        region_id=str(payload["region_id"]),
                        region_instance_id=str(payload["region_instance_id"]),
                        source_task_id=source_task_id,
                        source_instance_key=source_boundary.instance_key,
                        revision=int(payload["revision"]),
                        supersedes=payload.get("supersedes"),
                        plan_digest=fragment.digest,
                        signature=signature,
                        outputs=tuple(payload["outputs"]),
                        node_instances=tuple(sorted(node_instances)),
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                self._history_incomplete(f"generated plan history is incomplete: {exc}")
        generated_tasks = {
            task.accepted_boundary.instance_key
            for task in history.tasks
            if task.plan_provenance is not None and task.accepted_boundary is not None
        }
        if generated_tasks != seen_instances:
            self._history_incomplete("generated boundaries are not covered by plan history")
        return tuple(
            sorted(
                records,
                key=lambda item: (
                    item.region_instance_id,
                    item.revision,
                    item.plan_digest,
                ),
            )
        )

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
    """Load and validate a canonical replay bundle from local storage.

    Parameters
    ----------
    path
        Bundle directory or direct path to its canonical ``manifest.json``.

    Returns
    -------
    ReplayFixture
        Integrity-checked fixture with a local blob codec.

    Raises
    ------
    ReplayFixtureError
        If the source is not a canonical bundle or any manifest, value, schema,
        or artifact integrity check fails.
    """
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
    if data.get("version") not in {LEGACY_FIXTURE_VERSION, FIXTURE_VERSION}:
        raise ReplayFixtureError(
            FixtureErrorCode.FIXTURE_VERSION_UNSUPPORTED,
            f"expected a supported fixture version, found {data.get('version')!r}",
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
            generated_plans=tuple(
                GeneratedPlanRecord.from_data(item) for item in data.get("generated_plans", [])
            ),
            bundle_path=bundle_root,
        )
    except ReplayFixtureError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise ReplayFixtureError(
            FixtureErrorCode.FIXTURE_INVALID,
            "fixture manifest does not satisfy its replay contract",
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
    boundaries = {boundary.instance_key: boundary for boundary in fixture.boundaries}
    generated_instances: set[str] = set()
    for record in fixture.generated_plans:
        source = boundaries.get(record.source_instance_key)
        if source is None:
            raise ReplayFixtureError(
                FixtureErrorCode.FIXTURE_INVALID,
                "generated plan source boundary is absent",
            )
        try:
            source_data = fixture.values.decode(source.output_value)
            fragment = PlanFragmentIR.from_dict(cast(dict[str, Any], source_data))
        except (TypeError, ValueError, ArtifactError) as exc:
            raise ReplayFixtureError(
                FixtureErrorCode.FIXTURE_INVALID,
                "generated plan source value is invalid",
            ) from exc
        if (
            fragment.digest != record.plan_digest
            or record.signature.source_fragment_digest != record.plan_digest
            or fragment.revision != record.revision
            or fragment.supersedes != record.supersedes
            or fragment.outputs != record.outputs
            or record.signature.outputs != record.outputs
            or record.signature.region_id != record.region_id
        ):
            raise ReplayFixtureError(
                FixtureErrorCode.FIXTURE_INVALID,
                "generated plan lineage or signature is inconsistent",
            )
        instances = dict(record.node_instances)
        descriptors = {
            cast(str, descriptor["key"]): descriptor
            for descriptor in record.signature.resolved_nodes
        }
        if set(instances) != set(descriptors):
            raise ReplayFixtureError(
                FixtureErrorCode.FIXTURE_INVALID,
                "generated plan node coverage is inconsistent",
            )
        for node_key, instance_key in record.node_instances:
            if instance_key in generated_instances:
                raise ReplayFixtureError(
                    FixtureErrorCode.FIXTURE_INVALID,
                    "generated boundary instance appears in multiple plans",
                )
            generated_instances.add(instance_key)
            generated_boundary = boundaries.get(instance_key)
            descriptor = descriptors[node_key]
            if generated_boundary is None or (
                generated_boundary.module_id != descriptor["module_id"]
                or generated_boundary.logical_step != f"dynamic/{record.region_id}/nodes/{node_key}"
                or generated_boundary.module_digest != descriptor["module_digest"]
                or generated_boundary.output_schema_digest != descriptor["output_schema_digest"]
            ):
                raise ReplayFixtureError(
                    FixtureErrorCode.FIXTURE_INVALID,
                    "generated boundary does not match its trusted descriptor",
                )
            expected_dependencies = [record.source_instance_key]
            for dependency in cast(tuple[str, ...], descriptor["dependencies"]):
                if dependency != "$input":
                    expected_dependencies.append(instances[dependency])
            if generated_boundary.dependency_instance_keys != tuple(
                dict.fromkeys(expected_dependencies)
            ):
                raise ReplayFixtureError(
                    FixtureErrorCode.FIXTURE_INVALID,
                    "generated boundary dependency topology is inconsistent",
                )
