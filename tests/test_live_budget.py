from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from maida.workflows import (
    Budget,
    BudgetExceededError,
    BudgetUsage,
    Capability,
    Connector,
    ConnectorRegistry,
    ExecutionContext,
    ModelAdapterRegistry,
    ModelCallResult,
    ModelSpec,
    Module,
    RuntimeValue,
    Workflow,
    WorkflowRunner,
    compile_workflow,
)
from maida.workflows.ir import IR_VERSION
from maida.workflows.persistence import PostgresStore


@dataclass(frozen=True)
class Prompt:
    text: str


@dataclass(frozen=True)
class Answer:
    text: str


MODEL = ModelSpec(
    name="writer",
    provider="test",
    model="writer-v1",
    input_type=Prompt,
    output_type=Answer,
)


class FakeModelAdapter:
    def __init__(self, *, estimate: BudgetUsage, actual: BudgetUsage | None = None) -> None:
        self.estimate = estimate
        self.actual = actual or estimate
        self.calls = 0

    def estimate_call(self, model: ModelSpec[Any, Any], request: Any) -> BudgetUsage:
        return self.estimate

    async def call(self, model: ModelSpec[Any, Any], request: Any) -> ModelCallResult[Any]:
        self.calls += 1
        return ModelCallResult(
            output=Answer(f"reply:{request.text}"),
            served_model="writer-revision-7",
            usage=self.actual,
        )


class ModelModule(Module[str, str]):
    module_id = "budget.model"
    input_type = str
    output_type = str
    models = (MODEL,)

    def __init__(self, budget: Budget) -> None:
        self.budget = budget

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        result = await ctx.models.call("writer", Prompt(value))
        return str(result.text)


class ModelWorkflow(Workflow[str, str]):
    workflow_id = "live-budget-model"
    input_type = str
    output_type = str

    def __init__(self, budget: Budget) -> None:
        self.model = ModelModule(budget)

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.model(value)


class SlowModule(Module[int, int]):
    module_id = "budget.slow"
    input_type = int
    output_type = int
    budget = Budget(wall_time=timedelta(milliseconds=10))

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        await asyncio.sleep(0.1)
        return value


class SlowWorkflow(Workflow[int, int]):
    workflow_id = "live-budget-wall-time"
    input_type = int
    output_type = int
    slow = SlowModule()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.slow(value)


READ = Capability(
    name="record.read",
    connector="records",
    operation="read",
    input_type=str,
    output_type=str,
)


class ReadAdapter:
    connector = "records"
    connector_version = None
    operations = frozenset({"read"})
    effect_operations: frozenset[str] = frozenset()
    idempotent_effects: frozenset[str] = frozenset()

    def __init__(self) -> None:
        self.calls = 0

    async def read(self, operation: str, request: Any) -> str:
        self.calls += 1
        return f"record:{request}"


class ReadWorkflow(Workflow[str, str]):
    workflow_id = "live-budget-tool"
    input_type = str
    output_type = str

    def __init__(self, budget: Budget) -> None:
        self.read = Connector(READ)
        self.read.budget = budget

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.read(value)


def test_budget_usage_is_strict_immutable_and_canonical() -> None:
    usage = BudgetUsage(
        model_tokens=12,
        tool_calls=2,
        cost_usd=0.125,
        wall_time=timedelta(milliseconds=4),
    )

    assert BudgetUsage.from_data(usage.to_data()) == usage
    with pytest.raises(ValueError, match="nonnegative"):
        BudgetUsage(model_tokens=-1)
    with pytest.raises(TypeError, match="integer"):
        BudgetUsage(tool_calls=True)


def test_declared_models_are_canonical_behavior_bearing_ir() -> None:
    first = compile_workflow(ModelWorkflow(Budget(model_tokens=10)))
    changed_model = ModelSpec(
        name="writer",
        provider="test",
        model="writer-v2",
        input_type=Prompt,
        output_type=Answer,
    )
    second_workflow = ModelWorkflow(Budget(model_tokens=10))
    second_workflow.model.models = (changed_model,)
    second = compile_workflow(second_workflow)

    assert first.version == IR_VERSION
    assert first.executable_steps[0].models == (MODEL.to_data(),)
    assert first.executable_steps[0].replay_key == second.executable_steps[0].replay_key
    assert first.executable_steps[0].module_digest != second.executable_steps[0].module_digest

    with pytest.raises(ValueError, match="credentials"):
        ModelSpec(
            name="unsafe",
            provider="test",
            model="writer-v1",
            input_type=Prompt,
            output_type=Answer,
            configuration={"api_key": "must-not-enter-ir"},
        )


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_model_calls_are_preflighted_and_record_served_identity(
    postgres_store: PostgresStore,
) -> None:
    adapter = FakeModelAdapter(
        estimate=BudgetUsage(model_tokens=10, cost_usd=0.02),
        actual=BudgetUsage(model_tokens=8, cost_usd=0.015),
    )
    result = await WorkflowRunner(
        postgres_store,
        model_adapters=ModelAdapterRegistry({"test": adapter}),
    ).run(ModelWorkflow(Budget(model_tokens=10, cost_usd=0.02)), "hello")
    history = postgres_store.load_run_history(result.run_id, tenant_id="local")
    boundary = history.accepted_boundaries[0]

    assert result.output == "reply:hello"
    assert adapter.calls == 1
    assert boundary.usage.input_tokens + boundary.usage.output_tokens == 8
    assert boundary.usage.cost_usd == 0.015
    assert boundary.trajectories[0].metadata["declared_model"] == "writer-v1"
    assert boundary.trajectories[0].metadata["served_model"] == "writer-revision-7"
    assert any(event.event_type == "MODEL_RESOLVED" for event in history.events)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_budget_denies_a_model_call_before_the_provider_is_invoked(
    postgres_store: PostgresStore,
) -> None:
    adapter = FakeModelAdapter(estimate=BudgetUsage(model_tokens=11, cost_usd=0.02))

    with pytest.raises(BudgetExceededError, match="model_tokens"):
        await WorkflowRunner(
            postgres_store,
            max_attempts=1,
            model_adapters=ModelAdapterRegistry({"test": adapter}),
        ).run(ModelWorkflow(Budget(model_tokens=10, cost_usd=0.02)), "hello")

    assert adapter.calls == 0


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_model_adapter_cannot_underestimate_usage(
    postgres_store: PostgresStore,
) -> None:
    adapter = FakeModelAdapter(
        estimate=BudgetUsage(model_tokens=5, cost_usd=0.01),
        actual=BudgetUsage(model_tokens=6, cost_usd=0.01),
    )

    with pytest.raises(BudgetExceededError, match="reservation"):
        await WorkflowRunner(
            postgres_store,
            max_attempts=1,
            model_adapters=ModelAdapterRegistry({"test": adapter}),
        ).run(ModelWorkflow(Budget(model_tokens=10, cost_usd=0.02)), "hello")

    assert adapter.calls == 1


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_wall_time_budget_cancels_the_handler_attempt(
    postgres_store: PostgresStore,
) -> None:
    with pytest.raises(BudgetExceededError, match="wall_time"):
        await WorkflowRunner(postgres_store, max_attempts=1).run(SlowWorkflow(), 1)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_tool_budget_is_charged_before_the_connector_adapter(
    postgres_store: PostgresStore,
) -> None:
    adapter = ReadAdapter()
    with pytest.raises(BudgetExceededError, match="tool_calls"):
        await WorkflowRunner(
            postgres_store,
            max_attempts=1,
            connectors=ConnectorRegistry((adapter,)),
        ).run(ReadWorkflow(Budget(tool_calls=0)), "ada")

    assert adapter.calls == 0

    result = await WorkflowRunner(
        postgres_store,
        connectors=ConnectorRegistry((adapter,)),
    ).run(ReadWorkflow(Budget(tool_calls=1)), "ada")
    history = postgres_store.load_run_history(result.run_id, tenant_id="local")

    assert result.output == "record:ada"
    assert adapter.calls == 1
    assert history.accepted_boundaries[0].usage.tool_calls == 1
