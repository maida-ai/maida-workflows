"""Materialize validated generated plans into the durable distributed queue.

Generated plan bytes select only allowlisted aliases and topology. This module
revalidates those bytes against trusted policy, resolves exact module objects,
and inserts the complete child graph in one transaction. Planner workers never
create child workers or invoke child handlers; ordinary unrelated workers claim
the resulting tasks through the standard lease protocol.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, cast
from uuid import NAMESPACE_URL, uuid5

from psycopg.types.json import Jsonb

from ._canonical import canonical_data, canonical_json, digest_data, schema_digest
from .authoring import Module
from .budget import Budget
from .dynamic import PlanFragmentIR, PlanSignature, PlanValidator
from .ir import ReplayKey, _access_contract, module_digest
from .model import _model_contract
from .models import (
    CapabilityGrant,
    ExecutionSpec,
    PlanTaskProvenance,
    StoredValue,
    Task,
    TaskStatus,
)
from .persistence import InvalidRunStateError, PersistenceError, PostgresStore
from .registry import ModuleRegistry
from .runtime import TaskWorker


@dataclass(frozen=True)
class MaterializedPlan:
    """Identity and task addresses produced by one atomic materialization."""

    run_id: str
    region_instance_id: str
    plan_digest: str
    signature: PlanSignature
    task_ids: tuple[str, ...]


@dataclass(frozen=True)
class DynamicPlanProgress:
    """Observable result of one non-blocking generated-graph scheduler pass."""

    tasks: tuple[Task, ...]
    ready_tasks: int
    complete: bool
    outputs: tuple[Any, ...] = ()


class PlanMaterializer:
    """Validate and atomically insert one generated graph into a live run.

    Parameters
    ----------
    store
        Durable PostgreSQL workflow store.
    registry
        Trusted module registry. Generated content never supplies import paths
        or executable factories.
    """

    def __init__(self, store: PostgresStore, registry: ModuleRegistry) -> None:
        self.store = store
        self.registry = registry

    def materialize(
        self,
        *,
        run_id: str,
        tenant_id: str,
        source_task_id: str,
        region_instance_id: str,
        fragment: PlanFragmentIR,
        validator: PlanValidator,
        expected_output_schema_digests: tuple[str, ...],
    ) -> MaterializedPlan:
        """Insert all child tasks or none after exact trusted validation.

        The source planner task must already have one accepted result containing
        the canonical fragment. Initial dependency-free nodes become ``READY``;
        every other node remains ``BLOCKED`` until
        :class:`DynamicPlanScheduler` observes accepted upstream boundaries.
        Repeating the exact region and digest is idempotent.
        """
        if not isinstance(fragment, PlanFragmentIR):
            raise TypeError("fragment must be PlanFragmentIR")
        if not region_instance_id.strip():
            raise ValueError("region_instance_id must be non-empty")
        history = self.store.load_run_history(run_id, tenant_id=tenant_id)
        source = next((task for task in history.tasks if task.task_id == source_task_id), None)
        if source is None or source.status is not TaskStatus.SUCCEEDED:
            raise InvalidRunStateError("plan source task must have an accepted result")
        if source.accepted_boundary is None:
            raise PersistenceError("successful plan source has no accepted boundary")
        source_data = self.store.values.decode(source.accepted_boundary.output_value)
        try:
            restored = PlanFragmentIR.from_dict(cast(Mapping[str, Any], source_data))
        except (TypeError, ValueError) as exc:
            raise InvalidRunStateError("accepted plan source is not a canonical fragment") from exc
        if restored.canonical_json() != fragment.canonical_json():
            raise InvalidRunStateError("fragment does not match the accepted planner output")

        existing_events = tuple(
            event
            for event in history.events
            if event.event_type == "PLAN_MATERIALIZED"
            and event.payload.get("region_instance_id") == region_instance_id
        )
        latest = existing_events[-1] if existing_events else None
        if latest is not None:
            if latest.payload["plan_digest"] != fragment.digest:
                raise InvalidRunStateError("a different plan is already materialized for region")
            tasks = self._region_tasks(history.tasks, region_instance_id)
            signature = validator.validate(
                fragment,
                region_input_schema_digest=source.accepted_boundary.input_schema_digest,
                expected_output_schema_digests=expected_output_schema_digests,
            )
            return MaterializedPlan(
                run_id,
                region_instance_id,
                fragment.digest,
                signature,
                tuple(task.task_id for task in tasks),
            )
        signature = validator.validate(
            fragment,
            region_input_schema_digest=source.accepted_boundary.input_schema_digest,
            expected_output_schema_digests=expected_output_schema_digests,
        )
        modules = self._resolve_modules(signature)
        tasks = self._insert(
            source,
            region_instance_id,
            fragment,
            signature,
            modules,
            tenant_id=tenant_id,
        )
        return MaterializedPlan(
            run_id,
            region_instance_id,
            fragment.digest,
            signature,
            tuple(task.task_id for task in tasks),
        )

    def _resolve_modules(self, signature: PlanSignature) -> Mapping[str, Module[Any, Any]]:
        modules: dict[str, Module[Any, Any]] = {}
        for descriptor in signature.resolved_nodes:
            key = cast(str, descriptor["key"])
            module = self.registry.resolve_exact(
                cast(str, descriptor["module_id"]),
                cast(str, descriptor["module_digest"]),
            )
            _verify_descriptor(module, descriptor)
            modules[key] = module
        return MappingProxyType(modules)

    def _insert(
        self,
        source: Task,
        region_instance_id: str,
        fragment: PlanFragmentIR,
        signature: PlanSignature,
        modules: Mapping[str, Module[Any, Any]],
        *,
        tenant_id: str,
    ) -> tuple[Task, ...]:
        if source.accepted_boundary is None:  # pragma: no cover - caller checked
            raise PersistenceError("plan source has no boundary")
        source_boundary = source.accepted_boundary
        rows: list[dict[str, Any]] = []
        by_key = {cast(str, node["key"]): node for node in signature.resolved_nodes}
        with self.store.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT tenant_id, status FROM workflow_runs
                WHERE run_id = %s FOR UPDATE
                """,
                (source.run_id,),
            )
            run = cursor.fetchone()
            if run is None or run["tenant_id"] != tenant_id:
                raise PersistenceError("run is not accessible for plan materialization")
            if run["status"] != "RUNNING":
                raise InvalidRunStateError("run must be running to materialize a plan")
            cursor.execute(
                """
                SELECT status, accepted_boundary FROM workflow_tasks
                WHERE task_id = %s AND run_id = %s FOR UPDATE
                """,
                (source.task_id, source.run_id),
            )
            locked_source = cursor.fetchone()
            if locked_source is None or locked_source["status"] != "SUCCEEDED":
                raise InvalidRunStateError("plan source is no longer successful")
            persisted_boundary = locked_source["accepted_boundary"]
            if (
                not isinstance(persisted_boundary, Mapping)
                or persisted_boundary.get("output_value") != source_boundary.output_value.to_data()
            ):
                raise InvalidRunStateError("plan source boundary changed during materialization")
            cursor.execute(
                """
                SELECT payload FROM workflow_events
                WHERE run_id = %s AND event_type = 'PLAN_MATERIALIZED'
                  AND payload->>'region_instance_id' = %s
                ORDER BY event_id DESC LIMIT 1
                """,
                (source.run_id, region_instance_id),
            )
            previous = cursor.fetchone()
            if previous is not None:
                raise InvalidRunStateError("a plan was materialized concurrently for region")

            for node_key in sorted(by_key):
                descriptor = by_key[node_key]
                module = modules[node_key]
                dependencies = tuple(cast(list[str], descriptor["dependencies"]))
                input_value = _initial_input(
                    self.store,
                    module,
                    dependencies,
                    source_boundary.input_value,
                )
                if input_value is not None:
                    self.store._register_value_artifact(cursor, input_value)
                task_id = str(
                    uuid5(
                        NAMESPACE_URL,
                        f"{source.run_id}:{region_instance_id}:{node_key}",
                    )
                )
                logical_step = f"dynamic/{signature.region_id}/nodes/{node_key}"
                step_instance_id = digest_data(
                    {
                        "parent_instance": source_boundary.instance_key,
                        "region_instance_id": region_instance_id,
                        "node_key": node_key,
                    }
                )[:24]
                execution = ExecutionSpec.from_data(
                    cast(dict[str, Any], canonical_data(descriptor["execution"]))
                )
                grant = CapabilityGrant.from_data(canonical_data(descriptor["capability_grant"]))
                budget = Budget.from_data(cast(Mapping[str, Any], descriptor["budget"]))
                status = "READY" if input_value is not None else "BLOCKED"
                cursor.execute(
                    """
                    INSERT INTO workflow_tasks (
                        task_id, run_id, module_id, logical_step, step_instance_id,
                        module_digest, node_id, dependency_instance_keys,
                        dependency_node_ids, task_input, execution_requirements,
                        execution_isolation, execution_image, execution_cpu,
                        execution_memory_bytes, required_executor_capabilities,
                        capability_grant, budget_declaration, branch_decisions,
                        map_decisions, status, ready_at, parent_task_id,
                        plan_region_id, plan_region_instance_id, plan_node_key,
                        plan_digest
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, '[]'::jsonb, '[]'::jsonb, %s,
                        CASE WHEN %s = 'READY' THEN now() ELSE NULL END,
                        %s, %s, %s, %s, %s
                    ) RETURNING *
                    """,
                    (
                        task_id,
                        source.run_id,
                        descriptor["module_id"],
                        logical_step,
                        step_instance_id,
                        descriptor["module_digest"],
                        f"dynamic/{region_instance_id}/{node_key}",
                        Jsonb([source_boundary.instance_key]),
                        Jsonb(list(dependencies)),
                        Jsonb(input_value.to_data()) if input_value is not None else None,
                        Jsonb(execution.to_data()),
                        execution.isolation,
                        execution.image,
                        execution.cpu,
                        execution.memory_bytes,
                        Jsonb(list(execution.capabilities)),
                        Jsonb(grant.to_data()),
                        Jsonb(budget.to_data()),
                        status,
                        status,
                        source.task_id,
                        signature.region_id,
                        region_instance_id,
                        node_key,
                        fragment.digest,
                    ),
                )
                row = cursor.fetchone()
                if row is None:  # pragma: no cover - insert returns row
                    raise PersistenceError("generated task insert failed")
                rows.append(row)
            self.store._append_event(
                cursor,
                source.run_id,
                "PLAN_MATERIALIZED",
                {
                    "fragment_id": fragment.fragment_id,
                    "node_task_ids": [
                        {"node_key": row["plan_node_key"], "task_id": str(row["task_id"])}
                        for row in rows
                    ],
                    "outputs": list(fragment.outputs),
                    "plan_digest": fragment.digest,
                    "region_id": signature.region_id,
                    "region_instance_id": region_instance_id,
                    "signature": signature.to_dict(),
                    "signature_digest": signature.digest,
                    "source_task_id": source.task_id,
                },
                task_id=source.task_id,
            )
        return tuple(self.store._task_from_row(row) for row in rows)

    @staticmethod
    def _region_tasks(tasks: tuple[Task, ...], region_instance_id: str) -> tuple[Task, ...]:
        return tuple(
            sorted(
                (
                    task
                    for task in tasks
                    if task.plan_provenance is not None
                    and task.plan_provenance.region_instance_id == region_instance_id
                ),
                key=lambda task: cast(PlanTaskProvenance, task.plan_provenance).node_key,
            )
        )


class DynamicPlanScheduler:
    """Advance materialized child dependencies without executing handlers.

    Each pass reads durable accepted boundaries, materializes newly available
    inputs, and marks eligible tasks ``READY``. Workers never wait on other
    workers, and scheduler restart requires no Python call stack.
    """

    def __init__(
        self,
        store: PostgresStore,
        registry: ModuleRegistry,
        *,
        run_id: str,
        region_instance_id: str,
        tenant_id: str = "local",
    ) -> None:
        self.store = store
        self.registry = registry
        self.run_id = run_id
        self.region_instance_id = region_instance_id
        self.tenant_id = tenant_id

    def worker(self, *, worker_id: str, **kwargs: Any) -> TaskWorker:
        """Create an ordinary claim worker bound to this generated graph."""
        history = self.store.load_run_history(self.run_id, tenant_id=self.tenant_id)
        modules = {
            ReplayKey(task.module_id, task.logical_step): self.registry.resolve_exact(
                task.module_id, task.module_digest
            )
            for task in self._tasks(history.tasks)
        }
        return TaskWorker(
            self.store,
            workflow_id=history.definition.workflow_id,
            definition_digest=history.run.definition_digest,
            modules=modules,
            worker_id=worker_id,
            **kwargs,
        )

    def advance(self) -> DynamicPlanProgress:
        """Make dependency-complete children ready and project output values."""
        history = self.store.load_run_history(self.run_id, tenant_id=self.tenant_id)
        tasks = self._tasks(history.tasks)
        by_key = {cast(PlanTaskProvenance, task.plan_provenance).node_key: task for task in tasks}
        source = next(
            task
            for task in history.tasks
            if tasks
            and task.task_id == cast(PlanTaskProvenance, tasks[0].plan_provenance).parent_task_id
        )
        if source.accepted_boundary is None:
            raise PersistenceError("generated graph source boundary is unavailable")
        made_ready = 0
        for task in tasks:
            if task.status is not TaskStatus.BLOCKED:
                continue
            values: list[StoredValue] = []
            dependency_instances = [source.accepted_boundary.instance_key]
            available = True
            for dependency in task.dependency_node_ids:
                if dependency == "$input":
                    values.append(source.accepted_boundary.input_value)
                    continue
                upstream = by_key.get(dependency)
                if (
                    upstream is None
                    or upstream.status is not TaskStatus.SUCCEEDED
                    or upstream.accepted_boundary is None
                ):
                    available = False
                    break
                values.append(upstream.accepted_boundary.output_value)
                dependency_instances.append(upstream.accepted_boundary.instance_key)
            if not available:
                continue
            module = self.registry.resolve_exact(task.module_id, task.module_digest)
            decoded = [self.store.values.decode(value) for value in values]
            concrete = decoded[0] if len(decoded) == 1 else tuple(decoded)
            input_value = self.store.values.encode(
                concrete, schema_digest=schema_digest(module.input_type)
            )
            self.store.ready_task(
                task.task_id,
                input_value=input_value,
                dependency_instance_keys=tuple(dict.fromkeys(dependency_instances)),
            )
            made_ready += 1
        history = self.store.load_run_history(self.run_id, tenant_id=self.tenant_id)
        tasks = self._tasks(history.tasks)
        output_keys = self._output_keys(history.events)
        outputs: list[Any] = []
        complete = True
        by_key = {cast(PlanTaskProvenance, task.plan_provenance).node_key: task for task in tasks}
        for key in output_keys:
            task = by_key[key]
            if task.status is not TaskStatus.SUCCEEDED or task.accepted_boundary is None:
                complete = False
                break
            outputs.append(self.store.values.decode(task.accepted_boundary.output_value))
        return DynamicPlanProgress(tasks, made_ready, complete, tuple(outputs) if complete else ())

    def _tasks(self, tasks: tuple[Task, ...]) -> tuple[Task, ...]:
        return tuple(
            task
            for task in tasks
            if task.plan_provenance is not None
            and task.plan_provenance.region_instance_id == self.region_instance_id
        )

    def _output_keys(self, events: tuple[Any, ...]) -> tuple[str, ...]:
        event = next(
            (
                event
                for event in reversed(events)
                if event.event_type == "PLAN_MATERIALIZED"
                and event.payload.get("region_instance_id") == self.region_instance_id
            ),
            None,
        )
        if event is None:
            raise PersistenceError("generated plan materialization event is unavailable")
        return tuple(event.payload["outputs"])


def _initial_input(
    store: PostgresStore,
    module: Module[Any, Any],
    dependencies: tuple[str, ...],
    region_input: StoredValue,
) -> StoredValue | None:
    if not dependencies or any(dependency != "$input" for dependency in dependencies):
        return None
    decoded = [store.values.decode(region_input) for _dependency in dependencies]
    concrete = decoded[0] if len(decoded) == 1 else tuple(decoded)
    return store.values.encode(concrete, schema_digest=schema_digest(module.input_type))


def _verify_descriptor(module: Module[Any, Any], descriptor: Mapping[str, Any]) -> None:
    if module_digest(module) != descriptor["module_digest"]:
        raise ValueError("resolved generated module digest changed")
    if schema_digest(module.output_type) != descriptor["output_schema_digest"]:
        raise ValueError("resolved generated module output schema changed")
    input_schemas = tuple(cast(list[str], descriptor["input_schema_digests"]))
    if len(input_schemas) == 1 and schema_digest(module.input_type) != input_schemas[0]:
        raise ValueError("resolved generated module input schema changed")
    if canonical_json(module.execution.to_data()) != canonical_json(descriptor["execution"]):
        raise ValueError("resolved generated module execution environment changed")
    if canonical_json(module.budget.to_data()) != canonical_json(descriptor["budget"]):
        raise ValueError("resolved generated module budget changed")
    access = _access_contract(module)
    if canonical_json(access["capabilities"]) != canonical_json(descriptor["capabilities"]):
        raise ValueError("resolved generated module capabilities changed")
    if canonical_json(access["effects"]) != canonical_json(descriptor["effects"]):
        raise ValueError("resolved generated module effects changed")
    # Model declarations are covered by module_digest. They remain absent from
    # generated plan bytes and are resolved only from the trusted module.
    _model_contract(module)
