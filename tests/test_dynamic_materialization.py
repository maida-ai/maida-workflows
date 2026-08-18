from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from maida.workflows import (
    Budget,
    CapabilityGrant,
    ExecutionContext,
    Module,
    ModuleRegistry,
    PlanFragmentIR,
    PlanLimits,
    PlanNode,
    PlanTaskProvenance,
    PlanValidator,
    RuntimeValue,
    TaskStatus,
    TaskWorker,
    Workflow,
    WorkflowScheduler,
    bind_workflow,
    module_digest,
)
from maida.workflows._canonical import schema_digest
from maida.workflows.materialization import (
    DynamicPlanScheduler,
    PlanMaterializer,
    _verify_descriptor,
)
from maida.workflows.persistence import InvalidRunStateError, PersistenceError, PostgresStore

RESOURCE_BOUND = Budget(
    wall_time=timedelta(seconds=1),
    model_tokens=0,
    tool_calls=0,
    cost_usd=0.0,
)


def fragment() -> PlanFragmentIR:
    return PlanFragmentIR(
        fragment_id="generated-math",
        nodes=(
            PlanNode("increment", "math.increment", ("$input",)),
            PlanNode("double", "math.double", ("$input",)),
            PlanNode("join", "math.join", ("increment", "double")),
        ),
        outputs=("join",),
    )


class Planner(Module[int, dict[str, Any]]):
    module_id = "planner.dynamic-materialization"
    input_type = int
    output_type = dict[str, Any]

    async def execute(self, value: int, ctx: ExecutionContext) -> dict[str, Any]:
        return fragment().to_dict()


class PlannerWorkflow(Workflow[int, dict[str, Any]]):
    workflow_id = "dynamic-materialization"
    input_type = int
    output_type = dict[str, Any]
    planner = Planner()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[dict[str, Any]]:
        return self.planner(value)


class Add(Module[int, int]):
    input_type = int
    output_type = int
    budget = RESOURCE_BOUND

    def __init__(self, amount: int, module_id: str) -> None:
        self.amount = amount
        self.module_id = module_id

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return value + self.amount


class Join(Module[tuple[int, int], int]):
    input_type = tuple[int, int]
    output_type = int
    budget = RESOURCE_BOUND
    module_id = "math.join"

    async def execute(self, value: tuple[int, int], ctx: ExecutionContext) -> int:
        return value[0] + value[1]


def generated_contracts() -> ModuleRegistry:
    increment = Add(1, "math.increment")
    double = Add(2, "math.double")
    join = Join()
    return ModuleRegistry(
        modules={
            "math.increment": lambda: increment,
            "math.double": lambda: double,
            "math.join": lambda: join,
        }
    )


def test_registry_requires_exact_recomputed_generated_module_identity() -> None:
    module = Add(1, "math.increment")
    with pytest.raises(TypeError, match="callable"):
        ModuleRegistry(modules={"math.increment": object()})  # type: ignore[dict-item]
    with pytest.raises(ValueError, match="stable"):
        ModuleRegistry(modules={"": lambda: module})

    registry = ModuleRegistry(modules={"math.increment": lambda: module})
    assert registry.resolve_exact("math.increment", module_digest(module)) is module
    module.amount = 2
    with pytest.raises(LookupError, match="exact trusted"):
        registry.resolve_exact("math.increment", module_digest(Add(1, "math.increment")))


def test_generated_module_descriptor_is_recomputed_before_materialization() -> None:
    module = Add(1, "math.increment")
    registry = generated_contracts()
    descriptor = registry.descriptor("math.increment")
    _verify_descriptor(module, descriptor)

    mutations: tuple[tuple[str, Any, str], ...] = (
        ("module_digest", "0" * 64, "digest"),
        ("output_schema_digest", "0" * 64, "output schema"),
        ("input_schema_digests", ["0" * 64], "input schema"),
        ("execution", {**descriptor["execution"], "isolation": "vm"}, "environment"),
        ("budget", Budget().to_data(), "budget"),
        ("capabilities", [{"name": "unexpected"}], "capabilities"),
        ("effects", [{"name": "unexpected"}], "effects"),
    )
    for field, value, message in mutations:
        changed = deepcopy(descriptor)
        changed[field] = value
        with pytest.raises(ValueError, match=message):
            _verify_descriptor(module, changed)


def test_materializer_fails_closed_before_inserting_untrusted_or_conflicting_plans() -> None:
    registry = generated_contracts()
    inert = cast(PostgresStore, SimpleNamespace())
    materializer = PlanMaterializer(inert, registry)
    arguments: dict[str, Any] = {
        "run_id": "run",
        "tenant_id": "local",
        "source_task_id": "source",
        "region_instance_id": "region",
        "fragment": fragment(),
        "validator": object(),
        "expected_output_schema_digests": (schema_digest(int),),
    }
    with pytest.raises(TypeError, match="PlanFragmentIR"):
        materializer.materialize(**{**arguments, "fragment": object()})
    with pytest.raises(ValueError, match="region_instance_id"):
        materializer.materialize(**{**arguments, "region_instance_id": ""})

    empty_store = cast(
        PostgresStore,
        SimpleNamespace(load_run_history=lambda *args, **kwargs: SimpleNamespace(tasks=())),
    )
    with pytest.raises(InvalidRunStateError, match="source task"):
        PlanMaterializer(empty_store, registry).materialize(**arguments)

    source_without_boundary = SimpleNamespace(
        task_id="source", status=TaskStatus.SUCCEEDED, accepted_boundary=None
    )
    missing_boundary_store = cast(
        PostgresStore,
        SimpleNamespace(
            load_run_history=lambda *args, **kwargs: SimpleNamespace(
                tasks=(source_without_boundary,)
            )
        ),
    )
    with pytest.raises(PersistenceError, match="no accepted boundary"):
        PlanMaterializer(missing_boundary_store, registry).materialize(**arguments)

    boundary = SimpleNamespace(output_value=object())
    source = SimpleNamespace(
        task_id="source", status=TaskStatus.SUCCEEDED, accepted_boundary=boundary
    )
    invalid_output_store = cast(
        PostgresStore,
        SimpleNamespace(
            load_run_history=lambda *args, **kwargs: SimpleNamespace(tasks=(source,)),
            values=SimpleNamespace(decode=lambda value: {"invalid": True}),
        ),
    )
    with pytest.raises(InvalidRunStateError, match="canonical fragment"):
        PlanMaterializer(invalid_output_store, registry).materialize(**arguments)

    conflicting_history = SimpleNamespace(
        tasks=(source,),
        events=(
            SimpleNamespace(
                event_type="PLAN_MATERIALIZED",
                payload={"region_instance_id": "region", "plan_digest": "f" * 64},
            ),
        ),
    )
    conflict_store = cast(
        PostgresStore,
        SimpleNamespace(
            load_run_history=lambda *args, **kwargs: conflicting_history,
            values=SimpleNamespace(decode=lambda value: fragment().to_dict()),
        ),
    )
    with pytest.raises(InvalidRunStateError, match="different plan"):
        PlanMaterializer(conflict_store, registry).materialize(**arguments)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_validated_plan_materializes_and_schedules_one_distributed_dag(
    postgres_store: PostgresStore,
) -> None:
    workflow = PlannerWorkflow()
    bound = bind_workflow(workflow)
    scheduler = WorkflowScheduler.submit(postgres_store, bound, 3)
    scheduler.advance()
    planner_worker = TaskWorker(
        postgres_store,
        workflow_id=bound.plan.workflow_id,
        definition_digest=bound.plan.digest,
        modules=bound.modules,
        worker_id="planner-vm",
    )
    assert await planner_worker.run_once() is not None
    source = postgres_store.load_run_history(scheduler.run_id, tenant_id="local").tasks[0]
    registry = generated_contracts()
    validator = PlanValidator(
        registry,
        PlanLimits(
            10,
            5,
            5,
            Budget(
                wall_time=timedelta(seconds=3),
                model_tokens=0,
                tool_calls=0,
                cost_usd=0.0,
            ),
        ),
        region_id="math-region",
        region_grant=CapabilityGrant(),
    )

    materialized = PlanMaterializer(postgres_store, registry).materialize(
        run_id=scheduler.run_id,
        tenant_id="local",
        source_task_id=source.task_id,
        region_instance_id="math-for-root",
        fragment=fragment(),
        validator=validator,
        expected_output_schema_digests=(schema_digest(int),),
    )
    repeated = PlanMaterializer(postgres_store, registry).materialize(
        run_id=scheduler.run_id,
        tenant_id="local",
        source_task_id=source.task_id,
        region_instance_id="math-for-root",
        fragment=fragment(),
        validator=validator,
        expected_output_schema_digests=(schema_digest(int),),
    )

    assert repeated.task_ids == materialized.task_ids
    history = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    generated = [task for task in history.tasks if task.plan_provenance is not None]
    assert len(generated) == 3
    assert {task.status for task in generated} == {TaskStatus.READY, TaskStatus.BLOCKED}
    assert not any(attempt.task_id in materialized.task_ids for attempt in history.attempts)

    dynamic = DynamicPlanScheduler(
        postgres_store,
        registry,
        run_id=scheduler.run_id,
        region_instance_id="math-for-root",
    )
    roots = {
        cast(PlanTaskProvenance, task.plan_provenance).node_key: task
        for task in generated
        if task.status is TaskStatus.READY
    }
    abandoned_worker = dynamic.worker(worker_id="child-vm-17")
    abandoned = abandoned_worker.claim(task_id=roots["increment"].task_id)
    assert abandoned is not None
    abandoned_worker.start(abandoned)
    with postgres_store.connect() as connection:
        connection.execute(
            "UPDATE workflow_tasks SET lease_expires_at = %s WHERE task_id = %s",
            (datetime.now(UTC) - timedelta(seconds=1), roots["increment"].task_id),
        )
    assert (
        await dynamic.worker(worker_id="replacement-vm-23").run_once(
            task_id=roots["increment"].task_id
        )
        is not None
    )
    assert (
        await dynamic.worker(worker_id="child-vm-42").run_once(task_id=roots["double"].task_id)
        is not None
    )

    progress = dynamic.advance()
    assert progress.ready_tasks == 1
    join_task = next(
        task
        for task in progress.tasks
        if cast(PlanTaskProvenance, task.plan_provenance).node_key == "join"
    )
    assert (
        await dynamic.worker(worker_id="child-vm-8").run_once(task_id=join_task.task_id) is not None
    )

    completed = dynamic.advance()
    assert completed.complete
    assert completed.outputs == (9,)
    final_history = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    increment_attempts = [
        attempt
        for attempt in final_history.attempts
        if attempt.task_id == roots["increment"].task_id
    ]
    assert len(increment_attempts) == 2
    assert sum(task.status is TaskStatus.SUCCEEDED for task in completed.tasks) == 3
