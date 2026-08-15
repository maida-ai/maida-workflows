from __future__ import annotations

from typing import Any

import pytest

from maida.workflows import (
    BindingSpec,
    ExecutionContext,
    ExternalWorkflow,
    Idempotency,
    InteropConnectorAdapter,
    InteropFidelity,
    Module,
    ModuleRegistry,
    NodeSpec,
    RuntimeValue,
    Workflow,
    WorkflowInterop,
    WorkflowRunner,
    WorkflowSpec,
    compile_workflow,
    compile_workflow_spec,
    module_digest,
)
from maida.workflows._canonical import type_schema
from maida.workflows.access import ConnectorRegistry
from maida.workflows.fixture import ReplayFixtureExporter
from maida.workflows.interop import InteropUnsupportedError
from maida.workflows.persistence import PostgresStore
from maida.workflows.replay import ReplayCase, ReplayEngine, ReplayMode, ReplayStatus


class Provider:
    provider = "example-provider"
    version: str | None = "provider-v1"
    read_only_workflows = frozenset({"lookup-account"})
    effect_workflows = frozenset({"send-message"})
    idempotent_workflows = effect_workflows

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any, str | None]] = []

    async def invoke(self, workflow: str, value: Any, *, idempotency_key: str | None) -> Any:
        self.calls.append((workflow, value, idempotency_key))
        if workflow == "lookup-account":
            return {"account": str(value)}
        return {"sent": str(value)}


class Echo(Module[int, int]):
    input_type = int
    output_type = int

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return value


class SpecImporter:
    provider = "example-provider"
    version: str | None = "provider-v1"

    def import_workflow(self, source: Any) -> WorkflowSpec:
        assert source == {"flow": "echo"}
        return WorkflowSpec(
            workflow_id="imported-echo",
            input_schema=type_schema(int),
            output_schema=type_schema(int),
            nodes=(NodeSpec.task("echo", "echo", BindingSpec.root()),),
            output=BindingSpec.node("echo"),
        )


class TraceImporter:
    provider = "example-provider"
    version: str | None = "provider-v1"

    def import_trace(self, source: Any) -> Any:
        return source


def test_interop_surface_explains_verification_fidelity() -> None:
    provider = Provider()
    opaque = WorkflowInterop(provider)
    trace_aware = WorkflowInterop(provider, trace_importer=TraceImporter())
    ir_aware = WorkflowInterop(provider, workflow_importer=SpecImporter())

    assert opaque.surface.fidelity is InteropFidelity.TYPED_BOUNDARY
    assert opaque.surface.typed_boundary
    assert not opaque.surface.behavioral_replay
    assert trace_aware.surface.fidelity is InteropFidelity.TRACE_AWARE
    assert trace_aware.surface.behavioral_replay
    assert not trace_aware.surface.static_graph
    assert ir_aware.surface.fidelity is InteropFidelity.IR_AWARE
    assert ir_aware.surface.static_graph
    assert ir_aware.surface.structural_diff
    assert "opaque" in opaque.surface.explanation.lower()


def test_ir_import_is_explicit_and_compiles_through_the_normal_spec_path() -> None:
    interop = WorkflowInterop(Provider(), workflow_importer=SpecImporter())
    spec = interop.import_workflow({"flow": "echo"})
    registry = ModuleRegistry(modules={"echo": Echo})
    compiled = compile_workflow_spec(spec, registry)

    assert spec.workflow_id == "imported-echo"
    assert compiled.bound is not None
    assert compiled.bound.plan.workflow_id == "imported-echo"
    with pytest.raises(InteropUnsupportedError, match="trace"):
        interop.import_trace({"trace": []})


def test_external_identity_and_provider_contract_fail_closed() -> None:
    first = ExternalWorkflow(
        module_id="accounts.lookup",
        workflow="lookup-account",
        provider="example-provider",
        provider_version="provider-v1",
        input_type=str,
        output_type=dict[str, str],
        effectful=False,
    )
    second = ExternalWorkflow(
        module_id="accounts.lookup",
        workflow="lookup-account",
        provider="example-provider",
        provider_version="provider-v2",
        input_type=str,
        output_type=dict[str, str],
        effectful=False,
    )
    assert module_digest(first) != module_digest(second)

    provider = Provider()
    provider.effect_workflows = frozenset({"lookup-account"})
    with pytest.raises(ValueError, match="both read-only and effectful"):
        InteropConnectorAdapter(provider)


class LookupWorkflow(Workflow[str, dict[str, str]]):
    workflow_id = "external-lookup"
    input_type = str
    output_type = dict[str, str]

    def __init__(self) -> None:
        self.lookup = ExternalWorkflow(
            module_id="accounts.lookup",
            workflow="lookup-account",
            provider="example-provider",
            provider_version="provider-v1",
            input_type=str,
            output_type=dict[str, str],
            effectful=False,
        )

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[dict[str, str]]:
        return self.lookup(value)


class SendWorkflow(Workflow[str, dict[str, str]]):
    workflow_id = "external-send"
    input_type = str
    output_type = dict[str, str]

    def __init__(self) -> None:
        self.send = ExternalWorkflow(
            module_id="messages.send",
            workflow="send-message",
            provider="example-provider",
            provider_version="provider-v1",
            input_type=str,
            output_type=dict[str, str],
            effectful=True,
            idempotency=Idempotency.REQUIRED,
        )

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[dict[str, str]]:
        return self.send(value)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_external_workflows_use_the_existing_broker_boundary(
    postgres_store: PostgresStore,
) -> None:
    provider = Provider()
    adapter = InteropConnectorAdapter(provider)
    connectors = ConnectorRegistry()
    connectors.register(adapter)

    lookup = await WorkflowRunner(postgres_store, connectors=connectors).run(
        LookupWorkflow(), "acct-7"
    )
    sent = await WorkflowRunner(postgres_store, connectors=connectors).run(SendWorkflow(), "hello")

    assert lookup.output == {"account": "acct-7"}
    assert sent.output == {"sent": "hello"}
    assert provider.calls[0] == ("lookup-account", "acct-7", None)
    assert provider.calls[1][:2] == ("send-message", "hello")
    assert provider.calls[1][2] is not None
    read_step = compile_workflow(LookupWorkflow()).executable_steps[0]
    effect_step = compile_workflow(SendWorkflow()).executable_steps[0]
    assert [item["name"] for item in read_step.capabilities] == ["external.lookup-account.read"]
    assert [item["name"] for item in effect_step.effects] == ["external.send-message.invoke"]
    history = postgres_store.load_run_history(sent.run_id, tenant_id="local")
    fixture = ReplayFixtureExporter(postgres_store.values).project(history)
    replayed = await ReplayEngine().replay(
        SendWorkflow(), ReplayCase(fixture, ReplayMode.FULL_STUB)
    )
    assert replayed.status is ReplayStatus.PASS
    assert len(provider.calls) == 2


@pytest.mark.asyncio
async def test_external_workflow_without_runtime_broker_fails_closed() -> None:
    module = ExternalWorkflow(
        module_id="accounts.lookup",
        workflow="lookup-account",
        provider="example-provider",
        input_type=str,
        output_type=dict[str, str],
        effectful=False,
    )
    with pytest.raises(RuntimeError, match="broker"):
        await module.execute("acct-7", ExecutionContext("run", "task", "step"))
