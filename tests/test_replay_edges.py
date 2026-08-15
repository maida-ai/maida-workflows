from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from maida_workflows import ExecutionContext, Module, ReplayKey, RuntimeValue, Workflow
from maida_workflows.fixture import ReplayFixture, ReplayFixtureExporter
from maida_workflows.models import EffectKind, EffectRecord
from maida_workflows.persistence import PostgresStore
from maida_workflows.replay import (
    MaidaTraceBridge,
    ReplayBroker,
    ReplayCase,
    ReplayContractError,
    ReplayEffectAdapter,
    ReplayEffectViolation,
    ReplayEngine,
    ReplayMode,
    ReplaySelectorError,
    ReplayStatus,
    ReplayWorkerPolicy,
    assert_replay_worker_environment,
)
from maida_workflows.runtime import WorkflowRunner
from maida_workflows.verification import VerificationSuite, VerificationVerdict, verify_workflow


class NoTraceBridge:
    async def trace[OutputT](
        self, name: str, callback: Callable[[], Awaitable[OutputT]]
    ) -> tuple[OutputT, None]:
        return await callback(), None


class Good(Module[int, int]):
    input_type = int
    output_type = int

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return value + 1


class Fails(Good):
    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        raise RuntimeError("selected boundary failed")


class BadOutput(Good):
    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return "wrong"  # type: ignore[return-value]


class CredentialProbe(Good):
    def __init__(self) -> None:
        self.saw_credential = False

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        import os

        self.saw_credential = "PRODUCTION_TOKEN" in os.environ
        return value + 1


class SingleWorkflow(Workflow[int, int]):
    workflow_id = "replay-edges"
    input_type = int
    output_type = int

    def __init__(self, module: Module[int, int] | None = None) -> None:
        self.boundary = module or Good()

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        return self.boundary.at("only")(value)


async def captured(postgres_store: PostgresStore) -> ReplayFixture:
    result = await WorkflowRunner(postgres_store).run(SingleWorkflow(), 1)
    history = postgres_store.load_run_history(result.run_id, tenant_id="local")
    return ReplayFixtureExporter(postgres_store.values).project(history)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_replay_mode_and_selector_errors_fail_before_execution(
    postgres_store: PostgresStore,
) -> None:
    fixture = await captured(postgres_store)
    engine = ReplayEngine(trace_bridge=NoTraceBridge())
    key = ReplayKey("replay-edges.boundary", "only")
    with pytest.raises(ReplaySelectorError, match="does not accept"):
        await engine.replay(SingleWorkflow(), ReplayCase(fixture, ReplayMode.FULL_STUB, (key,)))
    with pytest.raises(ReplaySelectorError, match="requires"):
        await engine.replay(SingleWorkflow(), ReplayCase(fixture, ReplayMode.SELECTIVE))
    with pytest.raises(ReplaySelectorError, match="unknown"):
        await engine.replay(
            SingleWorkflow(),
            ReplayCase(
                fixture,
                ReplayMode.SELECTIVE,
                (ReplayKey("unknown", "unknown"),),
            ),
        )


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_selective_live_failures_and_bad_outputs_are_hard_failures(
    postgres_store: PostgresStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = await captured(postgres_store)
    key = ReplayKey("replay-edges.boundary", "only")
    engine = ReplayEngine(trace_bridge=NoTraceBridge())
    failed = await engine.replay(
        SingleWorkflow(Fails()), ReplayCase(fixture, ReplayMode.SELECTIVE, (key,))
    )
    assert failed.status is ReplayStatus.REPLAY_LIVE_FAILURE
    assert failed.blocking
    with pytest.raises(ReplayContractError, match="violates"):
        await engine.replay(
            SingleWorkflow(BadOutput()),
            ReplayCase(fixture, ReplayMode.SELECTIVE, (key,)),
        )

    monkeypatch.setenv("PRODUCTION_TOKEN", "must-not-reach-replay")
    probe = CredentialProbe()
    scrubbed = await engine.replay(
        SingleWorkflow(probe), ReplayCase(fixture, ReplayMode.SELECTIVE, (key,))
    )
    assert scrubbed.status is ReplayStatus.PASS
    assert not probe.saw_credential


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_fixture_execution_instance_and_dependency_integrity_is_validated(
    postgres_store: PostgresStore,
) -> None:
    fixture = await captured(postgres_store)
    boundary = fixture.boundaries[0]
    duplicate = replace(fixture, boundaries=(boundary, boundary))
    engine = ReplayEngine(trace_bridge=NoTraceBridge())
    with pytest.raises(ReplayContractError, match="duplicate"):
        await engine.replay(SingleWorkflow(), ReplayCase(duplicate, ReplayMode.FULL_STUB))

    missing_dependency = replace(
        boundary,
        dependency_instance_keys=("missing@step#instance",),
    )
    broken = replace(fixture, boundaries=(missing_dependency,))
    with pytest.raises(ReplayContractError, match="unavailable dependencies"):
        await engine.replay(SingleWorkflow(), ReplayCase(broken, ReplayMode.FULL_STUB))

    verification = await verify_workflow(
        SingleWorkflow(),
        VerificationSuite((ReplayCase(duplicate, ReplayMode.FULL_STUB),)),
        engine=engine,
    )
    assert verification.verdict is VerificationVerdict.FAIL
    assert verification.replay_results[0].error_code == "REPLAY_CONTRACT_INVALID"


@pytest.mark.asyncio
async def test_replay_broker_reads_effects_and_worker_policy_are_fail_closed() -> None:
    request = {"ticket": 1}
    from maida_workflows._canonical import digest_data

    read_key = ("inventory", digest_data({"operation": "get", "request": request}))
    broker = ReplayBroker(recorded_reads={read_key: {"stock": 2}})
    assert await broker.read("inventory", "get", request) == {"stock": 2}

    async def safe_read(operation: str, payload: Any) -> dict[str, Any]:
        return {"operation": operation, "payload": payload}

    live_broker = ReplayBroker(replay_safe_live_reads={"safe": safe_read})
    assert await live_broker.read("safe", "get", request, allow_live=True) == {
        "operation": "get",
        "payload": request,
    }
    with pytest.raises(ReplayContractError, match="no recorded response"):
        await live_broker.read("unsafe", "get", request)
    with pytest.raises(ReplayEffectViolation, match="attempted"):
        await broker.effect("production", "send", request)
    assert broker.effect_attempts == 1

    with pytest.raises(ReplayContractError, match="production effect adapters"):
        assert_replay_worker_environment(
            ReplayWorkerPolicy(production_effect_adapters=("production",))
        )


def test_replay_effect_adapter_compares_without_invoking_a_connector() -> None:
    from maida_workflows._canonical import digest_data

    request = {"message": "hello"}
    recorded = EffectRecord(
        EffectKind.ATTEMPTED,
        "email",
        "send",
        digest_data(request),
    )
    adapter = ReplayEffectAdapter()
    assert adapter.validate(
        adapter="email",
        operation="send",
        request=request,
        recorded=recorded,
    ) == {"replay_ack": True}
    with pytest.raises(ReplayContractError, match="does not match"):
        adapter.validate(
            adapter="email",
            operation="send",
            request={"message": "changed"},
            recorded=recorded,
        )


@pytest.mark.asyncio
async def test_maida_trace_bridge_returns_trace_id_without_live_model_or_tool_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MAIDA_DATA_DIR", str(tmp_path / "maida"))

    async def callback() -> int:
        return 42

    output, trace_id = await MaidaTraceBridge().trace("selective-replay-test", callback)
    assert output == 42
    assert trace_id is not None and len(trace_id) == 32
