from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from maida_workflows import (
    ExecutionContext,
    Module,
    ReplayKey,
    RuntimeValue,
    Workflow,
    compile_workflow,
)
from maida_workflows.alignment import DiffKind, GraphAligner
from maida_workflows.baseline import create_baseline
from maida_workflows.fixture import (
    FixtureErrorCode,
    ReplayFixture,
    ReplayFixtureError,
    ReplayFixtureExporter,
    load_fixture,
)
from maida_workflows.models import RunStatus
from maida_workflows.persistence import PostgresStore
from maida_workflows.replay import (
    ReplayBudget,
    ReplayCase,
    ReplayContractError,
    ReplayEngine,
    ReplayMode,
    ReplaySelectorError,
    ReplayStatus,
    ReplayWorkerPolicy,
    resolve_selectors,
)
from maida_workflows.runtime import WorkflowRunner
from maida_workflows.verification import (
    VerificationPolicy,
    VerificationSuite,
    VerificationVerdict,
    verify_workflow,
)


class FakeTraceBridge:
    def __init__(self) -> None:
        self.calls = 0

    async def trace[OutputT](
        self, name: str, callback: Callable[[], Awaitable[OutputT]]
    ) -> tuple[OutputT, str]:
        self.calls += 1
        return await callback(), f"trace-{self.calls}"


class First(Module[int, int]):
    input_type = int
    output_type = int

    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        self.calls += 1
        return value + 1


class Second(Module[int, int]):
    input_type = int
    output_type = int

    def __init__(self) -> None:
        self.calls = 0
        self.seen: list[int] = []

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        self.calls += 1
        self.seen.append(value)
        return value * 2


class Chain(Workflow[int, int]):
    workflow_id = "chain"
    input_type = int
    output_type = int

    def __init__(self) -> None:
        self.first = First()
        self.second = Second()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.second.at("second")(self.first.at("first")(value))


class ChangedSecond(Second):
    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        self.calls += 1
        self.seen.append(value)
        ctx.metadata["usage"] = {
            "input_tokens": 5,
            "output_tokens": 2,
            "cost_usd": 0.25,
            "latency_ms": 3.0,
        }
        return value * 20


class ChangedChain(Chain):
    def __init__(self) -> None:
        self.first = First()
        self.second = ChangedSecond()


async def capture_fixture(
    postgres_store: PostgresStore,
    workflow: Workflow[Any, Any],
    value: Any,
) -> ReplayFixture:
    result = await WorkflowRunner(postgres_store).run(workflow, value)
    history = postgres_store.load_run_history(result.run_id, tenant_id="local")
    return ReplayFixtureExporter(postgres_store.values).project(history)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_full_stub_and_selective_replay_are_isolated(postgres_store: PostgresStore) -> None:
    fixture = await capture_fixture(postgres_store, Chain(), 1)
    current = ChangedChain()
    bridge = FakeTraceBridge()
    engine = ReplayEngine(trace_bridge=bridge)

    full = await engine.replay(current, ReplayCase(fixture, ReplayMode.FULL_STUB))
    assert full.status is ReplayStatus.PASS
    assert full.output == 4
    assert current.first.calls == current.second.calls == bridge.calls == 0

    key = ReplayKey("chain.second", "second")
    selective = await engine.replay(
        current,
        ReplayCase(fixture, ReplayMode.SELECTIVE, (key,)),
    )
    assert selective.status is ReplayStatus.CHANGED
    assert selective.output == 4
    assert current.first.calls == 0
    assert current.second.calls == 1
    assert current.second.seen == [2]
    assert selective.comparisons[0].output_changed
    assert selective.trace_ids == ("trace-1",)
    assert selective.live_usage.cost_usd == 0.25


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_selective_budget_counts_only_live_usage(postgres_store: PostgresStore) -> None:
    fixture = await capture_fixture(postgres_store, Chain(), 2)
    result = await ReplayEngine(trace_bridge=FakeTraceBridge()).replay(
        ChangedChain(),
        ReplayCase(
            fixture,
            ReplayMode.SELECTIVE,
            (ReplayKey("chain.second", "second"),),
            ReplayBudget(max_cost_usd=0.1),
        ),
    )
    assert result.status is ReplayStatus.REPLAY_BUDGET_EXCEEDED
    assert result.blocking


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_fixture_exports_are_deterministic_private_and_integrity_checked(
    postgres_store: PostgresStore, tmp_path: Path
) -> None:
    result = await WorkflowRunner(postgres_store).run(Chain(), 3)
    history = postgres_store.load_run_history(result.run_id, tenant_id="local")
    first_path = tmp_path / "first"
    second_path = tmp_path / "second"
    first = ReplayFixtureExporter(postgres_store.values).export(history, first_path)
    second = ReplayFixtureExporter(postgres_store.values).export(history, second_path)

    assert first.digest == second.digest
    assert (first_path / "manifest.json").read_bytes() == (
        second_path / "manifest.json"
    ).read_bytes()
    assert os.stat(first_path).st_mode & 0o777 == 0o700
    assert os.stat(first_path / "manifest.json").st_mode & 0o777 == 0o600
    assert load_fixture(first_path).digest == first.digest


class Echo(Module[str, str]):
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value


class EchoWorkflow(Workflow[str, str]):
    workflow_id = "echo"
    input_type = str
    output_type = str
    echo = Echo()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.echo(value)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_corrupt_and_missing_artifacts_make_bundle_non_replayable(
    postgres_store: PostgresStore, tmp_path: Path
) -> None:
    fixture = await capture_fixture(postgres_store, EchoWorkflow(), "x" * 200)
    history = postgres_store.load_run_history(fixture.source.run_id, tenant_id="local")
    bundle = tmp_path / "bundle"
    exported = ReplayFixtureExporter(postgres_store.values).export(history, bundle)
    digest = exported.artifacts[0].digest
    blob = bundle / "blobs" / digest[:2] / digest[2:]
    blob.write_bytes(b"corrupt")

    with pytest.raises(ReplayFixtureError) as captured:
        load_fixture(bundle)
    assert captured.value.code is FixtureErrorCode.ARTIFACT_INTEGRITY


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_failed_incomplete_and_generic_traces_are_rejected(
    postgres_store: PostgresStore, tmp_path: Path
) -> None:
    fixture = await capture_fixture(postgres_store, Chain(), 1)
    history = postgres_store.load_run_history(fixture.source.run_id, tenant_id="local")
    failed = replace(history, run=replace(history.run, status=RunStatus.FAILED))
    with pytest.raises(ReplayFixtureError) as captured:
        ReplayFixtureExporter(postgres_store.values).project(failed)
    assert captured.value.code is FixtureErrorCode.RUN_NOT_SUCCESSFUL

    incomplete = replace(history, run=replace(history.run, status=RunStatus.RUNNING))
    with pytest.raises(ReplayFixtureError) as captured:
        ReplayFixtureExporter(postgres_store.values).project(incomplete)
    assert captured.value.code is FixtureErrorCode.RUN_NOT_TERMINAL

    generic = tmp_path / "ordinary-maida-trace"
    generic.mkdir()
    (generic / "meta.json").write_text("{}")
    with pytest.raises(ReplayFixtureError) as captured:
        load_fixture(generic)
    assert captured.value.code is FixtureErrorCode.TRACE_NOT_REPLAYABLE


class Added(Module[int, int]):
    input_type = int
    output_type = int

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return value


class InsertedChain(ChangedChain):
    def __init__(self) -> None:
        super().__init__()
        self.added = Added()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.second.at("second")(self.added(self.first.at("first")(value)))


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_divergence_uses_structured_diff_and_policy_controls_blocking(
    postgres_store: PostgresStore,
) -> None:
    fixture = await capture_fixture(postgres_store, Chain(), 1)
    alignment = GraphAligner().align(fixture.workflow_ir, compile_workflow(InsertedChain()))
    assert alignment.diff.first_divergence is not None
    assert alignment.diff.first_divergence.kind is DiffKind.INSERTION

    suite = VerificationSuite((ReplayCase(fixture, ReplayMode.FULL_STUB),))
    diagnostic = await verify_workflow(InsertedChain(), suite)
    blocking = await verify_workflow(
        InsertedChain(),
        suite,
        policy=VerificationPolicy(replay_divergence_blocking=True),
    )
    assert diagnostic.verdict is VerificationVerdict.PASS
    assert blocking.verdict is VerificationVerdict.FAIL


class StringSecond(Module[int, str]):
    input_type = int
    output_type = str

    async def execute(self, value: int, ctx: ExecutionContext) -> str:
        return str(value)


class SchemaChangedChain(Workflow[int, str]):
    workflow_id = "chain"
    input_type = int
    output_type = str

    def __init__(self) -> None:
        self.first = First()
        self.second = StringSecond()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[str]:
        return self.second.at("second")(self.first.at("first")(value))


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_contract_invalid_injection_is_a_hard_error(postgres_store: PostgresStore) -> None:
    fixture = await capture_fixture(postgres_store, Chain(), 1)
    with pytest.raises(ReplayContractError, match="root output"):
        await ReplayEngine(trace_bridge=FakeTraceBridge()).replay(
            SchemaChangedChain(), ReplayCase(fixture, ReplayMode.FULL_STUB)
        )


class SentinelEffect(Module[int, int]):
    input_type = int
    output_type = int
    effectful = True

    def __init__(self, sentinel: list[int]) -> None:
        self.sentinel = sentinel

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        self.sentinel.append(value)
        return value + 1


class EffectWorkflow(Workflow[int, int]):
    workflow_id = "effect"
    input_type = int
    output_type = int

    def __init__(self, sentinel: list[int]) -> None:
        self.effect = SentinelEffect(sentinel)

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.effect(value)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_targeted_effect_module_is_stubbed_and_production_adapter_never_runs(
    postgres_store: PostgresStore,
) -> None:
    live_sentinel: list[int] = []
    fixture = await capture_fixture(postgres_store, EffectWorkflow(live_sentinel), 7)
    assert live_sentinel == [7]
    replay_sentinel: list[int] = []
    current = EffectWorkflow(replay_sentinel)
    engine = ReplayEngine(trace_bridge=FakeTraceBridge())
    full = await engine.replay(current, ReplayCase(fixture, ReplayMode.FULL_STUB))
    selective = await engine.replay(
        current,
        ReplayCase(
            fixture,
            ReplayMode.SELECTIVE,
            (ReplayKey("effect.effect", "root"),),
        ),
    )
    assert full.output == selective.output == 8
    assert replay_sentinel == []
    assert selective.comparisons[0].effect_stubbed


class Safe(Module[int, int]):
    input_type = int
    output_type = int

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return value


class Violating(Safe):
    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        await ctx.broker.effect("production", "send", {"value": value})
        return value


class BrokerWorkflow(Workflow[int, int]):
    workflow_id = "broker"
    input_type = int
    output_type = int

    def __init__(self, module: Module[int, int]) -> None:
        self.boundary = module

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.boundary(value)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_any_broker_effect_attempt_is_a_hard_violation(
    postgres_store: PostgresStore,
) -> None:
    fixture = await capture_fixture(postgres_store, BrokerWorkflow(Safe()), 9)
    result = await ReplayEngine(trace_bridge=FakeTraceBridge()).replay(
        BrokerWorkflow(Violating()),
        ReplayCase(
            fixture,
            ReplayMode.SELECTIVE,
            (ReplayKey("broker.boundary", "root"),),
        ),
    )
    assert result.status is ReplayStatus.REPLAY_EFFECT_VIOLATION
    assert result.blocking


class ReusedWorkflow(Workflow[int, int]):
    workflow_id = "reused-selector"
    input_type = int
    output_type = int
    shared = First()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.shared.at("second")(self.shared.at("first")(value))


def test_ambiguous_and_invalid_selectors_are_rejected() -> None:
    plan = compile_workflow(ReusedWorkflow())
    with pytest.raises(ReplaySelectorError, match="ambiguous"):
        resolve_selectors(plan, ["module:reused-selector.shared"])
    with pytest.raises(ReplaySelectorError, match="module:ID"):
        resolve_selectors(plan, ["bad"])
    assert resolve_selectors(plan, ["step:first", "step:second"]) == (
        ReplayKey("reused-selector.shared", "first"),
        ReplayKey("reused-selector.shared", "second"),
    )


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_baseline_contains_digests_and_provenance_not_payloads(
    postgres_store: PostgresStore,
) -> None:
    secret = "do-not-copy-this-payload"
    fixture = await capture_fixture(postgres_store, EchoWorkflow(), secret)
    baseline = create_baseline([fixture], provenance={"actor": "test"})

    assert baseline.sources[0].fixture_digest == fixture.digest
    assert secret not in str(baseline.to_data())
    assert baseline.provenance == {"actor": "test"}


def test_replay_worker_policy_scrubs_credentials_and_rejects_effect_adapters() -> None:
    policy = ReplayWorkerPolicy()
    scrubbed = policy.scrub_environment(
        {"PATH": "/bin", "OPENAI_API_KEY": "secret", "DATABASE_URL": "production"}
    )
    assert scrubbed == {"PATH": "/bin"}
    assert policy.grant == "replay-only"
    assert policy.production_effect_adapters == ()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_phase_two_exit_demo_proves_native_replay_and_effect_safety(
    postgres_store: PostgresStore, tmp_path: Path
) -> None:
    from examples.native_replay_demo import run_demo

    report = await run_demo(
        postgres_store.dsn,
        tmp_path / "demo-artifacts",
        tmp_path / "demo-fixture",
        trace_bridge=FakeTraceBridge(),
    )
    assert report["diff_kinds"] == ["MODULE_DIGEST_CHANGED"]
    assert report["full_stub_status"] == "PASS"
    assert report["full_stub_live_calls"] == 0
    assert report["selective_prepare_calls"] == 0
    assert report["selective_decide_calls"] == 1
    assert report["new_output_changed"] is True
    assert report["source_effect_calls"] == 1
    assert report["replay_effect_calls"] == 0
