from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from maida.workflows import (
    Budget,
    CapabilityGrant,
    DynamicPlanScheduler,
    ExecutionContext,
    ExecutionSpec,
    Module,
    ModuleCatalog,
    ModuleResolverRegistry,
    PlanFragmentIR,
    PlanLimits,
    PlanMaterializer,
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
from maida.workflows.materialization import _verify_descriptor
from maida.workflows.persistence import PostgresStore

RESOURCE_BOUND = Budget(
    wall_time=timedelta(seconds=1),
    model_tokens=0,
    tool_calls=0,
    cost_usd=0.0,
)


def fragment() -> PlanFragmentIR:
    return PlanFragmentIR(
        fragment_id="generated-math",
        revision=1,
        supersedes=None,
        nodes=(
            PlanNode("increment", "math.increment", ("$input",)),
            PlanNode("double", "math.double", ("$input",)),
            PlanNode("join", "math.join", ("increment", "double")),
        ),
        outputs=("join",),
    )


class Planner(Module[int, dict[str, Any]]):
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


def generated_contracts() -> tuple[ModuleCatalog, ModuleResolverRegistry]:
    increment = Add(1, "math.increment")
    double = Add(2, "math.double")
    join = Join()
    catalog = ModuleCatalog()
    for alias, module, inputs in (
        ("math.increment", increment, (schema_digest(int),)),
        ("math.double", double, (schema_digest(int),)),
        ("math.join", join, (schema_digest(int), schema_digest(int))),
    ):
        catalog = catalog.allow(
            alias,
            module_id=str(module.module_id),
            module_digest=module_digest(module),
            input_schema_digests=inputs,
            output_schema_digest=schema_digest(module.output_type),
            execution=ExecutionSpec().to_data(),
            budget=module.budget,
        )
    resolver = ModuleResolverRegistry()
    for module in (increment, double, join):
        resolver.register(str(module.module_id), module)
    return catalog, resolver


def test_generated_module_resolver_requires_exact_trusted_identity() -> None:
    module = Add(1, "math.increment")
    resolver = ModuleResolverRegistry()

    with pytest.raises(TypeError, match="Module instances"):
        resolver.register("math.increment", object())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="module_id"):
        resolver.register("", module)
    with pytest.raises(ValueError, match="definition_digest"):
        resolver.register("math.increment", module, definition_digest="")

    resolver.register("math.increment", module, definition_digest="definition")
    assert resolver.resolve("definition", "math.increment", module_digest(module)) is module
    with pytest.raises(ValueError, match="already registered"):
        resolver.register("math.increment", module, definition_digest="definition")
    with pytest.raises(LookupError, match="exact trusted"):
        resolver.resolve("other", "math.increment", module_digest(module))

    shared = ModuleResolverRegistry()
    shared.register("math.increment", module)
    assert shared.resolve("any-definition", "math.increment", module_digest(module)) is module
    module.amount = 2
    with pytest.raises(LookupError, match="exact trusted"):
        shared.resolve("any-definition", "math.increment", module_digest(Add(1, "math.increment")))


def test_generated_module_descriptor_is_recomputed_before_materialization() -> None:
    module = Add(1, "math.increment")
    catalog, _ = generated_contracts()
    descriptor = catalog.resolve("math.increment")
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
    catalog, resolver = generated_contracts()
    validator = PlanValidator(
        catalog,
        PlanLimits(
            10,
            5,
            5,
            0,
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

    materialized = PlanMaterializer(postgres_store, resolver).materialize(
        run_id=scheduler.run_id,
        tenant_id="local",
        source_task_id=source.task_id,
        region_instance_id="math-for-root",
        fragment=fragment(),
        validator=validator,
        expected_output_schema_digests=(schema_digest(int),),
    )
    repeated = PlanMaterializer(postgres_store, resolver).materialize(
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
        resolver,
        run_id=scheduler.run_id,
        region_instance_id="math-for-root",
        revision=1,
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
