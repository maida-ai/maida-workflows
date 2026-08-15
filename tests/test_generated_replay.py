from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import timedelta
from pathlib import Path
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
    PlanValidator,
    ReplayCase,
    ReplayKey,
    ReplayMode,
    RuntimeValue,
    TaskWorker,
    Workflow,
    WorkflowScheduler,
    bind_workflow,
    module_digest,
)
from maida.workflows._canonical import canonical_json, schema_digest
from maida.workflows.fixture import (
    FixtureErrorCode,
    ReplayFixture,
    ReplayFixtureError,
    ReplayFixtureExporter,
    load_fixture,
)
from maida.workflows.persistence import PostgresStore
from maida.workflows.replay import ReplayContractError, ReplayEngine, ReplayStatus

NODE_BUDGET = Budget(
    wall_time=timedelta(seconds=1),
    model_tokens=0,
    tool_calls=0,
    cost_usd=0.0,
)


def _fragment(*, include_double: bool = True) -> PlanFragmentIR:
    nodes = [PlanNode("increment", "math.increment", ("$input",))]
    if include_double:
        nodes.extend(
            (
                PlanNode("double", "math.double", ("$input",)),
                PlanNode("join", "math.join", ("increment", "double")),
            )
        )
        outputs = ("join",)
    else:
        outputs = ("increment",)
    return PlanFragmentIR(
        fragment_id="generated-math",
        revision=1,
        supersedes=None,
        nodes=tuple(nodes),
        outputs=outputs,
    )


class Planner(Module[int, dict[str, Any]]):
    input_type = int
    output_type = dict[str, Any]

    def __init__(self, *, include_double: bool = True) -> None:
        self.include_double = include_double
        self._calls = 0

    @property
    def calls(self) -> int:
        return self._calls

    async def execute(self, value: int, ctx: ExecutionContext) -> dict[str, Any]:
        self._calls += 1
        return _fragment(include_double=self.include_double).to_dict()


class GeneratedWorkflow(Workflow[int, dict[str, Any]]):
    workflow_id = "generated-replay"
    input_type = int
    output_type = dict[str, Any]

    def __init__(self, planner: Planner | None = None) -> None:
        self.planner = planner or Planner()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[dict[str, Any]]:
        return self.planner(value)


class Offset(Module[int, int]):
    input_type = int
    output_type = int
    budget = NODE_BUDGET

    def __init__(self, amount: int, module_id: str) -> None:
        self.amount = amount
        self.module_id = module_id
        self._calls = 0
        self._seen: list[int] = []

    @property
    def calls(self) -> int:
        return self._calls

    @property
    def seen(self) -> list[int]:
        return self._seen

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        self._calls += 1
        self._seen.append(value)
        return value + self.amount


class Join(Module[tuple[int, int], int]):
    input_type = tuple[int, int]
    output_type = int
    budget = NODE_BUDGET
    module_id = "math.join"

    def __init__(self) -> None:
        self._calls = 0

    @property
    def calls(self) -> int:
        return self._calls

    async def execute(self, value: tuple[int, int], ctx: ExecutionContext) -> int:
        self._calls += 1
        return value[0] + value[1]


class TraceCounter:
    def __init__(self) -> None:
        self.calls = 0

    async def trace[OutputT](
        self, name: str, callback: Callable[[], Awaitable[OutputT]]
    ) -> tuple[OutputT, str]:
        self.calls += 1
        return await callback(), f"trace-{self.calls}"


def _contracts(
    *, increment_amount: int = 1
) -> tuple[ModuleCatalog, ModuleResolverRegistry, dict[ReplayKey, Module[Any, Any]]]:
    increment = Offset(increment_amount, "math.increment")
    double = Offset(2, "math.double")
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
    modules: dict[ReplayKey, Module[Any, Any]] = {}
    for key, module in (
        ("increment", increment),
        ("double", double),
        ("join", join),
    ):
        resolver.register(str(module.module_id), module)
        modules[ReplayKey(str(module.module_id), f"dynamic/math-region/nodes/{key}")] = module
    return catalog, resolver, modules


def _validator(catalog: ModuleCatalog) -> PlanValidator:
    return PlanValidator(
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


async def _capture(postgres_store: PostgresStore, output: Path) -> ReplayFixture:
    workflow = GeneratedWorkflow()
    bound = bind_workflow(workflow)
    scheduler = WorkflowScheduler.submit(postgres_store, bound, 3)
    scheduler.advance()
    planner_worker = TaskWorker(
        postgres_store,
        workflow_id=bound.plan.workflow_id,
        definition_digest=bound.plan.digest,
        modules=bound.modules,
        worker_id="planner-worker",
    )
    assert await planner_worker.run_once() is not None
    history = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    source = next(task for task in history.tasks if task.plan_provenance is None)
    catalog, resolver, _modules = _contracts()
    PlanMaterializer(postgres_store, resolver).materialize(
        run_id=scheduler.run_id,
        tenant_id="local",
        source_task_id=source.task_id,
        region_instance_id="math-for-root",
        fragment=_fragment(),
        validator=_validator(catalog),
        expected_output_schema_digests=(schema_digest(int),),
    )
    dynamic = DynamicPlanScheduler(
        postgres_store,
        resolver,
        run_id=scheduler.run_id,
        region_instance_id="math-for-root",
        revision=1,
    )
    progress = dynamic.advance()
    for task in progress.tasks:
        if task.status.value == "READY":
            assert (
                await dynamic.worker(worker_id=f"worker-{task.task_id}").run_once(
                    task_id=task.task_id
                )
                is not None
            )
    progress = dynamic.advance()
    join = next(
        task for task in progress.tasks if cast(Any, task.plan_provenance).node_key == "join"
    )
    assert await dynamic.worker(worker_id="join-worker").run_once(task_id=join.task_id) is not None
    assert dynamic.advance().outputs == (9,)
    assert scheduler.advance().status.value == "SUCCEEDED"
    completed = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    return ReplayFixtureExporter(postgres_store.values).export(completed, output)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_generated_fixture_round_trips_and_full_stub_invokes_nothing(
    postgres_store: PostgresStore, tmp_path: Path
) -> None:
    fixture = await _capture(postgres_store, tmp_path / "generated")
    loaded = load_fixture(tmp_path / "generated")
    assert loaded.version == "0.2.0"
    assert loaded.digest == fixture.digest
    assert len(loaded.generated_plans) == 1
    assert {key for key, _instance in loaded.generated_plans[0].node_instances} == {
        "increment",
        "double",
        "join",
    }
    history = postgres_store.load_run_history(fixture.source.run_id, tenant_id="local")
    repeated = ReplayFixtureExporter(postgres_store.values).export(
        history, tmp_path / "generated-again"
    )
    assert repeated.digest == fixture.digest
    assert (tmp_path / "generated" / "manifest.json").read_bytes() == (
        tmp_path / "generated-again" / "manifest.json"
    ).read_bytes()

    with pytest.raises(ReplayContractError, match="trusted current validator"):
        await ReplayEngine(trace_bridge=TraceCounter()).replay(
            GeneratedWorkflow(), ReplayCase(loaded, ReplayMode.FULL_STUB)
        )

    current = GeneratedWorkflow()
    catalog, _resolver, modules = _contracts()
    trace = TraceCounter()
    result = await ReplayEngine(
        trace_bridge=trace,
        generated_validators={"math-region": _validator(catalog)},
        generated_modules=modules,
    ).replay(current, ReplayCase(loaded, ReplayMode.FULL_STUB))

    assert result.status is ReplayStatus.PASS
    assert current.planner.calls == trace.calls == 0
    assert all(cast(Any, module).calls == 0 for module in modules.values())


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_generated_selective_replay_runs_only_exact_changed_node(
    postgres_store: PostgresStore, tmp_path: Path
) -> None:
    fixture = await _capture(postgres_store, tmp_path / "generated")
    catalog, _resolver, modules = _contracts(increment_amount=10)
    selected = ReplayKey("math.increment", "dynamic/math-region/nodes/increment")
    trace = TraceCounter()

    result = await ReplayEngine(
        trace_bridge=trace,
        generated_validators={"math-region": _validator(catalog)},
        generated_modules=modules,
    ).replay(
        GeneratedWorkflow(),
        ReplayCase(fixture, ReplayMode.SELECTIVE, (selected,)),
    )

    assert result.status is ReplayStatus.CHANGED
    assert result.comparisons[0].replay_key == selected
    assert result.comparisons[0].output_changed
    assert cast(Offset, modules[selected]).seen == [3]
    assert trace.calls == 1
    assert sum(cast(Any, module).calls for module in modules.values()) == 1


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_selective_planner_reports_generated_topology_divergence(
    postgres_store: PostgresStore, tmp_path: Path
) -> None:
    fixture = await _capture(postgres_store, tmp_path / "generated")
    current = GeneratedWorkflow(Planner(include_double=False))
    catalog, _resolver, modules = _contracts()

    result = await ReplayEngine(
        trace_bridge=TraceCounter(),
        generated_validators={"math-region": _validator(catalog)},
        generated_modules=modules,
    ).replay(
        current,
        ReplayCase(
            fixture,
            ReplayMode.SELECTIVE,
            (ReplayKey("generated-replay.planner", "root"),),
        ),
    )

    assert result.status is ReplayStatus.REPLAY_DIVERGENCE
    assert result.divergence is not None
    assert not result.blocking


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_generated_fixture_rejects_corrupt_instance_provenance(
    postgres_store: PostgresStore, tmp_path: Path
) -> None:
    bundle = tmp_path / "generated"
    await _capture(postgres_store, bundle)
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["generated_plans"][0]["node_instances"][0]["instance_key"] = "forged"
    manifest_path.write_text(canonical_json(manifest))

    with pytest.raises(ReplayFixtureError) as captured:
        load_fixture(bundle)
    assert captured.value.code is FixtureErrorCode.FIXTURE_INVALID
