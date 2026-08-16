from __future__ import annotations

from dataclasses import dataclass

import pytest

from maida.workflows import (
    Approval,
    ApprovalDecision,
    ApproveCommand,
    BindingSpec,
    Input,
    InputCommand,
    ModuleRegistry,
    NodeSpec,
    RejectCommand,
    ReplayCase,
    ReplayMode,
    RuntimeValue,
    SignalCommand,
    TaskStatus,
    TaskWorker,
    WaitForSignal,
    Workflow,
    WorkflowRun,
    WorkflowScheduler,
    WorkflowSpec,
    bind_workflow,
    compile_workflow_spec,
)
from maida.workflows._canonical import type_schema
from maida.workflows.fixture import ReplayFixtureExporter
from maida.workflows.persistence import InvalidRunStateError, PostgresStore
from maida.workflows.replay import ReplayEngine, ReplayStatus


@dataclass(frozen=True)
class Form:
    answer: str


class ApprovalWorkflow(Workflow[str, ApprovalDecision]):
    workflow_id = "interaction-approval"
    input_type = str
    output_type = ApprovalDecision
    approval = Approval(str, prompt="Deploy this change?")

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[ApprovalDecision]:
        return self.approval(value)


class InputWorkflow(Workflow[str, Form]):
    workflow_id = "interaction-input"
    input_type = str
    output_type = Form
    form = Input(str, Form, prompt="Provide the reviewed answer")

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[Form]:
        return self.form(value)


class SignalWorkflow(Workflow[str, int]):
    workflow_id = "interaction-signal"
    input_type = str
    output_type = int
    signal = WaitForSignal(str, int, name="payment.settled")

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[int]:
        return self.signal(value)


def approval_spec() -> WorkflowSpec:
    return WorkflowSpec(
        workflow_id="spec-interaction-approval",
        input_schema=type_schema(str),
        output_schema=type_schema(ApprovalDecision),
        nodes=(
            NodeSpec.approval(
                "review",
                BindingSpec.root(),
                prompt="Deploy this change?",
                metadata={"audience": "release-manager"},
            ),
        ),
        output=BindingSpec.node("review"),
    )


def test_interactions_are_portable_explainable_workflow_spec_nodes() -> None:
    restored = WorkflowSpec.from_dict(approval_spec().to_dict())
    compilation = compile_workflow_spec(restored, ModuleRegistry())

    assert compilation.ok
    assert compilation.plan is not None
    step = compilation.plan.executable_steps[0]
    assert step.control == {"interaction": "approval"}
    assert compilation.explanation.nodes[0]["kind"] == "approval"


def test_typed_input_and_signal_spec_nodes_validate_their_schemas() -> None:
    spec = WorkflowSpec(
        workflow_id="spec-interactions",
        input_schema=type_schema(str),
        output_schema=type_schema(int),
        nodes=(
            NodeSpec.request_input(
                "answer",
                BindingSpec.root(),
                response_schema=type_schema(int),
                prompt="Answer with a number",
            ),
            NodeSpec.wait_for_signal(
                "settled",
                BindingSpec.node("answer"),
                payload_schema=type_schema(int),
                name="payment.settled",
            ),
        ),
        output=BindingSpec.node("settled"),
    )

    compilation = compile_workflow_spec(spec, ModuleRegistry())

    assert compilation.ok
    assert compilation.plan is not None
    assert [step.control for step in compilation.plan.executable_steps] == [
        {"interaction": "input"},
        {"interaction": "signal", "signal_name": "payment.settled"},
    ]


def test_interaction_modules_validate_configuration_and_resolution_data() -> None:
    with pytest.raises(ValueError, match="approval prompt"):
        Approval(str, prompt=" ")
    with pytest.raises(ValueError, match="input prompt"):
        Input(str, Form, prompt=" ")
    with pytest.raises(ValueError, match="signal name"):
        WaitForSignal(str, int, name=" ")

    approval = Approval(str, prompt="Review", metadata={"screen": "release"})
    request = approval._request_data(run_id="run", task_id="task", step_instance_id="step")
    repeated = approval._request_data(run_id="run", task_id="task", step_instance_id="step")
    assert request == repeated
    assert request["kind"] == "approval"
    assert request["metadata"] == {"screen": "release"}
    assert approval._resolve_data(
        {"decision": "approve", "comment": "looks good", "command_id": "command"}
    ) == ApprovalDecision(True, "looks good", None, "command")
    assert approval._resolve_data(
        {"decision": "reject", "reason": "unsafe", "command_id": "command"}
    ) == ApprovalDecision(False, None, "unsafe", "command")
    with pytest.raises(ValueError, match="valid decision"):
        approval._resolve_data({"decision": "maybe"})

    form = Input(str, Form, prompt="Answer")
    assert form._resolve_data({"value": {"answer": "yes"}}) == Form("yes")
    with pytest.raises(ValueError, match="output contract"):
        form._resolve_data({"value": {"answer": 3}})

    signal = WaitForSignal(str, int, name="payment.settled")
    signal_request = signal._request_data(run_id="run", task_id="task", step_instance_id="signal")
    assert signal_request["signal_name"] == "payment.settled"
    assert signal._resolve_data({"name": "payment.settled", "value": 3}) == 3
    with pytest.raises(ValueError, match="name does not match"):
        signal._resolve_data({"name": "wrong", "value": 3})
    with pytest.raises(ValueError, match="output contract"):
        signal._resolve_data({"name": "payment.settled", "value": "wrong"})


@pytest.mark.asyncio
async def test_interaction_handler_cannot_bypass_durable_worker_protocol() -> None:
    approval = Approval(str, prompt="Review")

    with pytest.raises(RuntimeError, match="TaskWorker"):
        await approval.execute("change", None)  # type: ignore[arg-type]


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_spec_authored_approval_uses_the_same_durable_protocol(
    postgres_store: PostgresStore,
) -> None:
    bound = compile_workflow_spec(approval_spec(), ModuleRegistry()).raise_for_errors()
    scheduler = WorkflowScheduler.submit(postgres_store, bound, "release")
    scheduler.advance()
    worker = TaskWorker(
        postgres_store,
        workflow_id=bound.plan.workflow_id,
        definition_digest=bound.plan.digest,
        modules=bound.modules,
        worker_id="spec-interaction-worker",
    )
    assert await worker.run_once() is None
    history = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    request_id = next(
        event.payload["request_id"]
        for event in history.events
        if event.event_type == "APPROVAL_REQUIRED"
    )

    WorkflowRun(postgres_store, scheduler.run_id).send(
        ApproveCommand(request_id=request_id, command_id="spec-approval")
    )

    assert await worker.run_once() is not None
    assert scheduler.advance().output.approved is True


@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize("approved", [True, False])
async def test_approval_parks_without_a_worker_and_resumes_on_another_worker(
    postgres_store: PostgresStore,
    approved: bool,
) -> None:
    workflow = ApprovalWorkflow()
    bound = bind_workflow(workflow)
    scheduler = WorkflowScheduler.submit(postgres_store, bound, "release")
    scheduler.advance()
    first_worker = TaskWorker(
        postgres_store,
        workflow_id=bound.plan.workflow_id,
        definition_digest=bound.plan.digest,
        modules=bound.modules,
        worker_id="worker-before-approval",
    )

    assert await first_worker.run_once() is None
    parked = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    task = parked.tasks[0]
    request = next(event for event in parked.events if event.event_type == "APPROVAL_REQUIRED")
    assert task.status is TaskStatus.NEEDS_APPROVAL

    handle = WorkflowRun(postgres_store, scheduler.run_id)
    command = (
        ApproveCommand(request_id=request.payload["request_id"], command_id="decision-1")
        if approved
        else RejectCommand(
            request_id=request.payload["request_id"],
            command_id="decision-1",
            reason="not yet",
        )
    )
    handle.send(command)
    second_worker = TaskWorker(
        postgres_store,
        workflow_id=bound.plan.workflow_id,
        definition_digest=bound.plan.digest,
        modules=bound.modules,
        worker_id="worker-after-approval",
    )

    boundary = await second_worker.run_once()
    assert boundary is not None
    completed = scheduler.advance()

    assert completed.output.approved is approved
    assert completed.output.reason == (None if approved else "not yet")
    assert len(postgres_store.load_run_history(scheduler.run_id, tenant_id="local").attempts) == 2


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_interaction_boundaries_are_stubbed_during_all_replay_modes(
    postgres_store: PostgresStore,
) -> None:
    workflow = ApprovalWorkflow()
    bound = bind_workflow(workflow)
    scheduler = WorkflowScheduler.submit(postgres_store, bound, "release")
    scheduler.advance()
    worker = TaskWorker(
        postgres_store,
        workflow_id=bound.plan.workflow_id,
        definition_digest=bound.plan.digest,
        modules=bound.modules,
        worker_id="capture-worker",
    )
    assert await worker.run_once() is None
    pending = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    request_id = next(
        event.payload["request_id"]
        for event in pending.events
        if event.event_type == "APPROVAL_REQUIRED"
    )
    WorkflowRun(postgres_store, scheduler.run_id).send(
        ApproveCommand(request_id=request_id, command_id="captured-approval")
    )
    assert await worker.run_once() is not None
    scheduler.advance()
    history = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    fixture = ReplayFixtureExporter(postgres_store.values).project(history)
    key = next(iter(bound.modules))

    full = await ReplayEngine().replay(workflow, ReplayCase(fixture, ReplayMode.FULL_STUB))
    selective = await ReplayEngine().replay(
        workflow, ReplayCase(fixture, ReplayMode.SELECTIVE, (key,))
    )

    assert full.status is ReplayStatus.PASS
    assert selective.status is ReplayStatus.PASS
    assert selective.comparisons[0].executed_live is False


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_typed_input_rejects_invalid_payload_before_resuming(
    postgres_store: PostgresStore,
) -> None:
    workflow = InputWorkflow()
    bound = bind_workflow(workflow)
    scheduler = WorkflowScheduler.submit(postgres_store, bound, "question")
    scheduler.advance()
    task_worker = TaskWorker(
        postgres_store,
        workflow_id=bound.plan.workflow_id,
        definition_digest=bound.plan.digest,
        modules=bound.modules,
        worker_id="input-worker",
    )
    assert await task_worker.run_once() is None
    history = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    request_id = next(
        event.payload["request_id"]
        for event in history.events
        if event.event_type == "INPUT_REQUIRED"
    )
    handle = WorkflowRun(postgres_store, scheduler.run_id)

    with pytest.raises(InvalidRunStateError, match="schema"):
        handle.send(InputCommand(request_id=request_id, value={"answer": 3}, command_id="bad"))

    handle.send(InputCommand(request_id=request_id, value={"answer": "yes"}, command_id="good"))
    boundary = await task_worker.run_once()

    assert boundary is not None
    assert scheduler.advance().output == Form("yes")


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_named_signal_validates_name_and_payload(
    postgres_store: PostgresStore,
) -> None:
    workflow = SignalWorkflow()
    bound = bind_workflow(workflow)
    scheduler = WorkflowScheduler.submit(postgres_store, bound, "invoice")
    scheduler.advance()
    task_worker = TaskWorker(
        postgres_store,
        workflow_id=bound.plan.workflow_id,
        definition_digest=bound.plan.digest,
        modules=bound.modules,
        worker_id="signal-worker",
    )
    assert await task_worker.run_once() is None
    history = postgres_store.load_run_history(scheduler.run_id, tenant_id="local")
    request_id = next(
        event.payload["request_id"]
        for event in history.events
        if event.event_type == "SIGNAL_REQUIRED"
    )
    handle = WorkflowRun(postgres_store, scheduler.run_id)

    with pytest.raises(InvalidRunStateError, match="name"):
        handle.send(
            SignalCommand(name="wrong", value=1, request_id=request_id, command_id="wrong-name")
        )
    with pytest.raises(InvalidRunStateError, match="schema"):
        handle.send(
            SignalCommand(
                name="payment.settled",
                value="wrong",
                request_id=request_id,
                command_id="wrong-value",
            )
        )

    handle.send(
        SignalCommand(name="payment.settled", value=42, request_id=request_id, command_id="settled")
    )
    assert await task_worker.run_once() is not None
    assert scheduler.advance().output == 42
