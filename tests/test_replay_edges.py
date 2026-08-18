from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from maida.workflows import ExecutionContext, Module, ReplayKey, RuntimeValue, Workflow
from maida.workflows._canonical import canonical_json
from maida.workflows.fixture import (
    FixtureErrorCode,
    ReplayFixture,
    ReplayFixtureError,
    ReplayFixtureExporter,
    load_fixture,
)
from maida.workflows.models import EffectKind, EffectRecord
from maida.workflows.persistence import PostgresStore
from maida.workflows.replay import (
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
from maida.workflows.runtime import WorkflowRunner


class NoTraceBridge:
    async def trace[OutputT](
        self, name: str, callback: Callable[[], Awaitable[OutputT]]
    ) -> tuple[OutputT, None]:
        return await callback(), None


class Good(Module[int, int]):
    module_id = "replay-edges.boundary"
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


class CountingBuildWorkflow(SingleWorkflow):
    def __init__(self, module: Module[int, int] | None = None) -> None:
        super().__init__(module)
        self.build_calls = 0

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[int]:
        self.build_calls += 1
        return super().build(value)


async def captured(postgres_store: PostgresStore) -> ReplayFixture:
    result = await WorkflowRunner(postgres_store).run(SingleWorkflow(), 1)
    history = postgres_store.load_run_history(result.run_id, tenant_id="local")
    return ReplayFixtureExporter(postgres_store.values).project(history)


def _managed_effect_pair() -> list[dict[str, Any]]:
    common = {
        "adapter": "messaging",
        "operation": "send",
        "request_digest": "request-digest",
        "effect_name": "messaging.send",
        "ordinal": 0,
        "idempotency_key": "mwf_test-key",
        "connector_version": "v1",
    }
    return [
        {"kind": EffectKind.ATTEMPTED.value, "result_digest": None, **common},
        {"kind": EffectKind.COMMITTED.value, "result_digest": "result-digest", **common},
    ]


def _write_manifest(path: Path, data: dict[str, Any]) -> None:
    path.write_bytes(canonical_json(data).encode())


@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate",
    [
        lambda pair: [
            {
                **pair[0],
                "idempotency_key": None,
            }
        ],
        lambda pair: [
            {
                key: value
                for key, value in pair[0].items()
                if key not in {"effect_name", "ordinal", "idempotency_key"}
            }
        ],
        lambda pair: [*pair, *pair],
        lambda pair: [pair[0], {**pair[1], "operation": "different-operation"}],
        lambda pair: [pair[0]],
    ],
    ids=(
        "partial-identity",
        "version-only-legacy",
        "duplicate-identity",
        "mismatched-pair",
        "unpaired",
    ),
)
async def test_fixture_rejects_malformed_managed_effect_evidence(
    postgres_store: PostgresStore,
    tmp_path: Path,
    mutate: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
) -> None:
    fixture = await captured(postgres_store)
    history = postgres_store.load_run_history(fixture.source.run_id, tenant_id="local")
    bundle = tmp_path / "fixture"
    ReplayFixtureExporter(postgres_store.values).export(history, bundle)
    manifest_path = bundle / "manifest.json"
    manifest: dict[str, Any] = json.loads(manifest_path.read_bytes())
    manifest["accepted_steps"][0]["effects"] = mutate(_managed_effect_pair())
    _write_manifest(manifest_path, manifest)

    with pytest.raises(ReplayFixtureError) as captured_error:
        load_fixture(bundle)

    assert captured_error.value.code is FixtureErrorCode.FIXTURE_INVALID


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_fixture_preserves_legacy_effect_pairs_exactly(
    postgres_store: PostgresStore,
    tmp_path: Path,
) -> None:
    fixture = await captured(postgres_store)
    history = postgres_store.load_run_history(fixture.source.run_id, tenant_id="local")
    bundle = tmp_path / "fixture"
    ReplayFixtureExporter(postgres_store.values).export(history, bundle)
    manifest_path = bundle / "manifest.json"
    manifest: dict[str, Any] = json.loads(manifest_path.read_bytes())
    legacy_pair = [
        {
            "kind": EffectKind.ATTEMPTED.value,
            "adapter": "legacy",
            "operation": "execute",
            "request_digest": "request-digest",
            "result_digest": None,
        },
        {
            "kind": EffectKind.COMMITTED.value,
            "adapter": "legacy",
            "operation": "execute",
            "request_digest": "request-digest",
            "result_digest": "result-digest",
        },
    ]
    manifest["accepted_steps"][0]["effects"] = legacy_pair
    _write_manifest(manifest_path, manifest)

    loaded = load_fixture(bundle)

    assert [effect.to_data() for effect in loaded.boundaries[0].effects] == legacy_pair
    assert loaded.to_manifest() == manifest


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_each_compile_run_or_replay_operation_builds_the_symbolic_graph_once(
    postgres_store: PostgresStore,
) -> None:
    from maida.workflows import compile_workflow

    compiled = CountingBuildWorkflow()
    compile_workflow(compiled)
    assert compiled.build_calls == 1

    source = CountingBuildWorkflow()
    run = await WorkflowRunner(postgres_store).run(source, 1)
    assert source.build_calls == 1
    history = postgres_store.load_run_history(run.run_id, tenant_id="local")
    fixture = ReplayFixtureExporter(postgres_store.values).project(history)

    full = CountingBuildWorkflow()
    await ReplayEngine(trace_bridge=NoTraceBridge()).replay(
        full,
        ReplayCase(fixture, ReplayMode.FULL_STUB),
    )
    assert full.build_calls == 1

    selective = CountingBuildWorkflow()
    await ReplayEngine(trace_bridge=NoTraceBridge()).replay(
        selective,
        ReplayCase(
            fixture,
            ReplayMode.SELECTIVE,
            (ReplayKey(Good.module_id, "only"),),
        ),
    )
    assert selective.build_calls == 1


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_replay_mode_and_selector_errors_fail_before_execution(
    postgres_store: PostgresStore,
) -> None:
    fixture = await captured(postgres_store)
    engine = ReplayEngine(trace_bridge=NoTraceBridge())
    key = ReplayKey(Good.module_id, "only")
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
    key = ReplayKey(Good.module_id, "only")
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


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_full_stub_requires_and_injects_every_recorded_boundary(
    postgres_store: PostgresStore,
) -> None:
    fixture = await captured(postgres_store)
    empty_history = replace(fixture, boundaries=())

    with pytest.raises(ReplayContractError, match="boundary"):
        await ReplayEngine(trace_bridge=NoTraceBridge()).replay(
            SingleWorkflow(),
            ReplayCase(empty_history, ReplayMode.FULL_STUB),
        )

    boundary = fixture.boundaries[0]
    wrong_output = postgres_store.values.encode(
        999,
        schema_digest=boundary.output_schema_digest,
    )
    inconsistent = replace(
        fixture,
        boundaries=(replace(boundary, output_value=wrong_output),),
    )
    with pytest.raises(ReplayContractError, match="root output"):
        await ReplayEngine(trace_bridge=NoTraceBridge()).replay(
            SingleWorkflow(),
            ReplayCase(inconsistent, ReplayMode.FULL_STUB),
        )

    mislabeled_input = postgres_store.values.encode(
        "not-an-integer",
        schema_digest=fixture.root_input.schema_digest,
    )
    invalid_value = replace(fixture, root_input=mislabeled_input)
    with pytest.raises(ReplayContractError, match="root input"):
        await ReplayEngine(trace_bridge=NoTraceBridge()).replay(
            SingleWorkflow(),
            ReplayCase(invalid_value, ReplayMode.FULL_STUB),
        )


@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["branch", "map", "nested-parallel-effect"])
async def test_full_stub_reconstructs_recorded_control_and_composition_paths(
    postgres_store: PostgresStore,
    case: str,
) -> None:
    from examples.adversarial_workflows import (
        AdversarialBranchWorkflow,
        AdversarialMapWorkflow,
        AdversarialNestedEffectWorkflow,
        BatchItem,
    )

    examples: dict[str, tuple[Workflow[Any, Any], Any, Any]] = {
        "branch": (
            AdversarialBranchWorkflow(),
            {"escalated": True},
            "urgent",
        ),
        "map": (
            AdversarialMapWorkflow(),
            [BatchItem("b", " B "), BatchItem("a", " A ")],
            ["b", "a"],
        ),
        "nested-parallel-effect": (
            AdversarialNestedEffectWorkflow(),
            "case",
            ("reviewed:case", "reviewed:case"),
        ),
    }
    workflow, value, expected = examples[case]
    result = await WorkflowRunner(postgres_store).run(workflow, value)
    history = postgres_store.load_run_history(result.run_id, tenant_id="local")
    fixture = ReplayFixtureExporter(postgres_store.values).project(history)

    replayed = await ReplayEngine(trace_bridge=NoTraceBridge()).replay(
        workflow,
        ReplayCase(fixture, ReplayMode.FULL_STUB),
    )

    assert replayed.status is ReplayStatus.PASS
    assert replayed.output == expected


@pytest.mark.asyncio
async def test_replay_broker_reads_effects_and_worker_policy_are_fail_closed() -> None:
    request = {"ticket": 1}
    from maida.workflows._canonical import digest_data

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
    from maida.workflows._canonical import digest_data

    request = {"message": "hello"}
    recorded = EffectRecord(
        EffectKind.ATTEMPTED,
        "email",
        "send",
        digest_data(request),
        connector_version="v1",
        effect_name="email.send",
        ordinal=1,
        idempotency_key="mwf_replay-test",
    )
    adapter = ReplayEffectAdapter()
    assert adapter.validate(
        adapter="email",
        operation="send",
        request=request,
        recorded=recorded,
        connector_version="v1",
        effect_name="email.send",
        ordinal=1,
    ) == {"replay_ack": True}
    with pytest.raises(ReplayContractError, match="does not match"):
        adapter.validate(
            adapter="email",
            operation="send",
            request={"message": "changed"},
            recorded=recorded,
            connector_version="v1",
            effect_name="email.send",
            ordinal=1,
        )
    with pytest.raises(ReplayContractError, match="does not match"):
        adapter.validate(
            adapter="email",
            operation="send",
            request=request,
            recorded=recorded,
            connector_version="v1",
            effect_name="email.send",
            ordinal=0,
        )

    legacy_data: dict[str, Any] = {
        "kind": "EFFECT_ATTEMPTED",
        "adapter": "email",
        "operation": "send",
        "request_digest": digest_data(request),
        "result_digest": None,
    }
    legacy = EffectRecord(
        kind=EffectKind(legacy_data["kind"]),
        adapter=legacy_data["adapter"],
        operation=legacy_data["operation"],
        request_digest=legacy_data["request_digest"],
        result_digest=legacy_data["result_digest"],
    )
    assert legacy.to_data() == legacy_data
    assert adapter.validate(
        adapter="email",
        operation="send",
        request=request,
        recorded=legacy,
    ) == {"replay_ack": True}


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
