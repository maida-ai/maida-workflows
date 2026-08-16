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
    WorkflowStartRequest,
    compile_workflow,
    compile_workflow_spec,
    module_digest,
)
from maida.workflows._canonical import type_schema
from maida.workflows.access import ConnectorRegistry
from maida.workflows.fixture import ReplayFixture, ReplayFixtureExporter
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


def test_start_request_and_external_module_identity_are_strict() -> None:
    with pytest.raises(ValueError, match="workflow_id"):
        WorkflowStartRequest("not valid", {}, "event-1")
    with pytest.raises(ValueError, match="idempotency_key"):
        WorkflowStartRequest("valid", {}, " ")

    common: dict[str, Any] = {
        "module_id": "accounts.lookup",
        "workflow": "lookup-account",
        "provider": "example-provider",
        "input_type": str,
        "output_type": str,
        "effectful": False,
    }
    for field, value, error in (
        ("module_id", "not valid", ValueError),
        ("workflow", "not valid", ValueError),
        ("provider", "not valid", ValueError),
        ("provider_version", "", ValueError),
        ("effectful", 1, TypeError),
        ("idempotency", "required", TypeError),
    ):
        values = {**common, field: value}
        with pytest.raises(error):
            ExternalWorkflow(**values)


@pytest.mark.asyncio
async def test_connector_adapter_and_modules_reject_unknown_operations() -> None:
    provider = Provider()
    interop = WorkflowInterop(provider)
    adapter = interop.connector_adapter()

    assert adapter.connector == provider.provider
    assert adapter.connector_version == provider.version
    assert adapter.idempotent_effects == provider.idempotent_workflows
    with pytest.raises(LookupError, match="read workflow"):
        await adapter.read("missing", {})
    with pytest.raises(LookupError, match="effect workflow"):
        await adapter.effect("missing", {}, "key")

    read = interop.module(
        module_id="accounts.lookup",
        workflow="lookup-account",
        input_type=str,
        output_type=dict[str, str],
        effectful=False,
    )
    effect = interop.module(
        module_id="messages.send",
        workflow="send-message",
        input_type=str,
        output_type=dict[str, str],
        effectful=True,
    )

    class Broker:
        async def read(self, connector: str, operation: str, request: Any, **kwargs: Any) -> Any:
            return {"account": request}

        async def effect(self, connector: str, operation: str, request: Any, **kwargs: Any) -> Any:
            return {"sent": request}

    context = ExecutionContext("run", "task", "step", broker=Broker())
    assert await read.execute("acct", context) == {"account": "acct"}
    assert await effect.execute("hello", context) == {"sent": "hello"}


def test_importers_fail_closed_on_missing_or_wrong_contracts() -> None:
    provider = Provider()
    opaque = WorkflowInterop(provider)
    with pytest.raises(InteropUnsupportedError, match="Workflow IR"):
        opaque.import_workflow({})
    with pytest.raises(InteropUnsupportedError, match="trace"):
        opaque.import_trace({})

    class WrongWorkflowImporter(SpecImporter):
        def import_workflow(self, source: Any) -> Any:
            return source

    class WrongTraceImporter(TraceImporter):
        def import_trace(self, source: Any) -> Any:
            return source

    with pytest.raises(TypeError, match="WorkflowSpec"):
        WorkflowInterop(provider, workflow_importer=WrongWorkflowImporter()).import_workflow({})
    with pytest.raises(TypeError, match="ReplayFixture"):
        WorkflowInterop(provider, trace_importer=WrongTraceImporter()).import_trace({})

    fixture = object.__new__(ReplayFixture)
    assert (
        WorkflowInterop(provider, trace_importer=TraceImporter()).import_trace(fixture) is fixture
    )


def test_provider_and_companion_contracts_validate_exact_identity() -> None:
    def invalid_provider(**changes: Any) -> Provider:
        provider = Provider()
        for name, value in changes.items():
            setattr(provider, name, value)
        return provider

    cases = (
        ({"provider": "not valid"}, ValueError, "stable"),
        ({"version": ""}, ValueError, "version"),
        ({"read_only_workflows": {"lookup-account"}}, TypeError, "frozenset"),
        ({"read_only_workflows": frozenset({"not valid"})}, ValueError, "stable"),
        (
            {"idempotent_workflows": frozenset({"lookup-account"})},
            ValueError,
            "declared effects",
        ),
        ({"invoke": None}, TypeError, "callable"),
    )
    for changes, error, message in cases:
        with pytest.raises(error, match=message):
            WorkflowInterop(invalid_provider(**changes))

    class Companion:
        provider = "other"
        version: str | None = "provider-v1"

        def import_workflow(self, source: Any) -> WorkflowSpec:
            return SpecImporter().import_workflow(source)

    with pytest.raises(ValueError, match="provider does not match"):
        WorkflowInterop(Provider(), workflow_importer=Companion())
    Companion.provider = "example-provider"
    Companion.version = "other"
    with pytest.raises(ValueError, match="version does not match"):
        WorkflowInterop(Provider(), workflow_importer=Companion())
    Companion.version = "provider-v1"
    Companion.import_workflow = None  # type: ignore[assignment]
    with pytest.raises(TypeError, match="implement"):
        WorkflowInterop(Provider(), workflow_importer=Companion())
