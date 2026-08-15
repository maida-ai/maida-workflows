from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from maida.workflows import (
    AccessBroker,
    AccessContractError,
    AccessPolicy,
    Capability,
    CapabilityGrant,
    Connector,
    ConnectorRegistry,
    EffectSpec,
    ExecutionContext,
    ExecutionSpec,
    ExecutorCapabilities,
    Module,
    PolicyDecision,
    RuntimeValue,
    TaskWorker,
    Workflow,
    WorkflowRunner,
    WorkflowScheduler,
)
from maida.workflows.replay import build_module_registry

GET_CUSTOMER = Capability(
    "crm.customer.read",
    connector="crm",
    operation="get_customer",
    input_type=str,
    output_type=dict[str, str],
    connector_version="v1",
)
WRITE_NOTE = EffectSpec(
    "crm.note.write",
    connector="crm",
    operation="write_note",
    input_type=str,
    output_type=str,
    connector_version="v1",
)


class CustomerAdapter:
    connector = "crm"
    connector_version = "v1"
    operations = frozenset({"get_customer"})

    def __init__(self, *, invalid_response: bool = False, fail: bool = False) -> None:
        self.invalid_response = invalid_response
        self.fail = fail
        self.calls: list[tuple[str, Any]] = []
        self.secret = "provider-token-must-not-be-audited"

    async def read(self, operation: str, request: Any) -> Any:
        self.calls.append((operation, request))
        if self.fail:
            raise RuntimeError(f"provider leaked {self.secret}")
        if self.invalid_response:
            return ["wrong-schema", self.secret]
        return {"email": "ada@example.test"}


class DenyCustomerPolicy:
    def __init__(self) -> None:
        self.calls = 0

    async def authorize(
        self,
        capability: Capability[Any, Any],
        request: Any,
        *,
        grant: CapabilityGrant,
        run_id: str,
        task_id: str,
        attempt_id: str,
    ) -> PolicyDecision:
        self.calls += 1
        assert capability is GET_CUSTOMER
        assert request == "customer-secret-7"
        assert grant.allows_capability(GET_CUSTOMER.name)
        assert (run_id, task_id, attempt_id) == ("run-1", "task-1", "attempt-1")
        return PolicyDecision.deny("regional-policy", "region-denied")


def broker_for(
    adapter: CustomerAdapter,
    *,
    grant: CapabilityGrant | None = None,
    policy: AccessPolicy | None = None,
    declarations: tuple[Capability[Any, Any], ...] = (GET_CUSTOMER,),
    audit: list[tuple[str, dict[str, Any]]] | None = None,
    metadata: dict[str, Any] | None = None,
) -> AccessBroker:
    registry = ConnectorRegistry([adapter])
    return AccessBroker(
        registry,
        declarations=declarations,
        grant=grant or CapabilityGrant(capabilities=(GET_CUSTOMER.name,)),
        run_id="run-1",
        task_id="task-1",
        attempt_id="attempt-1",
        module_id="support.customer",
        logical_step="root.dep0",
        policy=policy,
        audit=(lambda kind, record: audit.append((kind, record))) if audit is not None else None,
        metadata=metadata,
    )


def test_capability_grants_are_canonical_narrowable_and_fail_closed() -> None:
    grant = CapabilityGrant(
        capabilities=("web.search", "crm.customer.read"),
        effects=("email.send",),
    )

    assert grant.capabilities == ("crm.customer.read", "web.search")
    assert grant.to_data() == {
        "capabilities": ["crm.customer.read", "web.search"],
        "effects": ["email.send"],
    }
    assert CapabilityGrant.from_data(grant.to_data()) == grant
    assert CapabilityGrant.from_data(["legacy.executor.label"]) == CapabilityGrant()
    assert grant.narrow(capabilities=("crm.customer.read",)).to_data() == {
        "capabilities": ["crm.customer.read"],
        "effects": ["email.send"],
    }
    with pytest.raises(ValueError, match="cannot widen"):
        grant.narrow(capabilities=("payments.read",))
    with pytest.raises(ValueError, match="stable name"):
        CapabilityGrant(capabilities=("contains space",))


def test_connector_registry_resolves_exact_operation_and_optional_version() -> None:
    v1 = CustomerAdapter()
    registry = ConnectorRegistry([v1])

    assert registry.resolve("crm", "get_customer", connector_version="v1") is v1
    with pytest.raises(AccessContractError, match="not registered"):
        registry.resolve("crm", "get_customer", connector_version="v2")
    with pytest.raises(AccessContractError, match="not registered"):
        registry.resolve("crm", "search", connector_version="v1")
    with pytest.raises(ValueError, match="already registered"):
        registry.register(CustomerAdapter())


def test_broker_rejects_ambiguous_declarations_before_any_invocation() -> None:
    duplicate_endpoint = replace(GET_CUSTOMER, name="crm.customer.lookup")

    with pytest.raises(AccessContractError, match="ambiguously share"):
        broker_for(
            CustomerAdapter(),
            declarations=(GET_CUSTOMER, duplicate_endpoint),
            grant=CapabilityGrant(capabilities=(GET_CUSTOMER.name, duplicate_endpoint.name)),
        )


@pytest.mark.asyncio
async def test_broker_allows_declared_granted_typed_read_and_records_only_digests() -> None:
    adapter = CustomerAdapter()
    audit: list[tuple[str, dict[str, Any]]] = []
    metadata: dict[str, Any] = {}
    broker = broker_for(adapter, audit=audit, metadata=metadata)

    result = await broker.read(
        "crm",
        "get_customer",
        "customer-secret-7",
        connector_version="v1",
    )

    assert result == {"email": "ada@example.test"}
    assert adapter.calls == [("get_customer", "customer-secret-7")]
    assert [kind for kind, _ in audit] == ["CAPABILITY_AUTHORIZED", "CAPABILITY_USED"]
    assert metadata["trajectories"][0]["kind"] == "capability"
    serialized = repr((audit, metadata, broker.records))
    assert "customer-secret-7" not in serialized
    assert "ada@example.test" not in serialized
    assert adapter.secret not in serialized
    assert GET_CUSTOMER.name in serialized


@pytest.mark.asyncio
async def test_broker_denies_undeclared_or_ungranted_access_before_policy_and_adapter() -> None:
    adapter = CustomerAdapter()
    policy = DenyCustomerPolicy()
    broker = broker_for(adapter, declarations=(), grant=CapabilityGrant(), policy=policy)
    with pytest.raises(AccessContractError, match="not declared"):
        await broker.read("crm", "get_customer", "customer-secret-7", connector_version="v1")

    broker = broker_for(adapter, grant=CapabilityGrant(), policy=policy)
    with pytest.raises(AccessContractError, match="grant"):
        await broker.read("crm", "get_customer", "customer-secret-7", connector_version="v1")

    assert adapter.calls == []
    assert policy.calls == 0


@pytest.mark.asyncio
async def test_policy_can_narrow_but_never_expand_a_task_grant() -> None:
    adapter = CustomerAdapter()
    policy = DenyCustomerPolicy()
    audit: list[tuple[str, dict[str, Any]]] = []
    broker = broker_for(adapter, policy=policy, audit=audit)

    with pytest.raises(AccessContractError, match="policy"):
        await broker.read("crm", "get_customer", "customer-secret-7", connector_version="v1")

    assert adapter.calls == []
    assert policy.calls == 1
    assert audit[-1][0] == "CAPABILITY_DENIED"
    assert audit[-1][1]["reason_code"] == "region-denied"


@pytest.mark.asyncio
async def test_broker_fails_closed_on_request_response_adapter_and_version_errors() -> None:
    adapter = CustomerAdapter()
    broker = broker_for(adapter)
    with pytest.raises(AccessContractError, match="request contract"):
        await broker.read("crm", "get_customer", 7, connector_version="v1")
    with pytest.raises(AccessContractError, match="not declared"):
        await broker.read("crm", "get_customer", "7", connector_version="v2")
    assert adapter.calls == []

    invalid = CustomerAdapter(invalid_response=True)
    with pytest.raises(AccessContractError, match="response contract"):
        await broker_for(invalid).read(
            "crm", "get_customer", "customer-secret-7", connector_version="v1"
        )
    assert invalid.calls == [("get_customer", "customer-secret-7")]

    failed = CustomerAdapter(fail=True)
    audit: list[tuple[str, dict[str, Any]]] = []
    with pytest.raises(AccessContractError, match="adapter read failed") as raised:
        await broker_for(failed, audit=audit).read(
            "crm", "get_customer", "customer-secret-7", connector_version="v1"
        )
    assert failed.secret not in str(raised.value)
    assert raised.value.__cause__ is None
    assert failed.secret not in repr(audit)


class ReadCustomerWorkflow(Workflow[str, dict[str, str]]):
    workflow_id = "read-customer"
    input_type = str
    output_type = dict[str, str]
    customer = Connector(GET_CUSTOMER)

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[dict[str, str]]:
        return self.customer(value)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_workflow_runner_binds_an_attempt_scoped_broker_for_connector_read(
    postgres_store: Any,
) -> None:
    adapter = CustomerAdapter()
    result = await WorkflowRunner(
        postgres_store,
        connectors=ConnectorRegistry([adapter]),
    ).run(ReadCustomerWorkflow(), "customer-secret-7", tenant_id="tenant-a")

    history = postgres_store.load_run_history(result.run_id, tenant_id="tenant-a")
    task = history.tasks[0]
    boundary = history.accepted_boundaries[0]
    assert result.output == {"email": "ada@example.test"}
    assert task.capability_grant == CapabilityGrant(capabilities=(GET_CUSTOMER.name,))
    assert boundary.trajectories[0].kind == "capability"
    access_events = [
        event for event in history.events if event.event_type.startswith("CAPABILITY_")
    ]
    assert [event.event_type for event in access_events] == [
        "CAPABILITY_AUTHORIZED",
        "CAPABILITY_USED",
    ]
    assert all(event.task_id == task.task_id for event in access_events)
    assert all(event.attempt_id == boundary.accepted_attempt.attempt_id for event in access_events)
    serialized = repr(access_events)
    assert "customer-secret-7" not in serialized
    assert "ada@example.test" not in serialized
    assert adapter.secret not in serialized


class SplitGrantModule(Module[str, str]):
    input_type = str
    output_type = str
    execution = ExecutionSpec(capabilities=("executor.egress",))
    effectful = True
    capabilities = (GET_CUSTOMER,)
    effects = (WRITE_NOTE,)

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value


class SplitGrantWorkflow(Workflow[str, str]):
    workflow_id = "split-grant"
    input_type = str
    output_type = str

    def __init__(self) -> None:
        self.customer = SplitGrantModule()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.customer(value)


@pytest.mark.postgres
def test_task_envelope_separates_executor_eligibility_from_access_grant(
    postgres_store: Any,
) -> None:
    workflow = SplitGrantWorkflow()
    scheduler = WorkflowScheduler.submit(
        postgres_store,
        workflow,
        "customer-7",
        tenant_id="tenant-a",
    )
    assert scheduler.advance().ready_tasks == 1
    worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=build_module_registry(workflow, scheduler.plan, output=scheduler.output),
        worker_id="worker-1",
        capabilities=ExecutorCapabilities(capabilities=frozenset({"executor.egress"})),
    )

    envelope = worker.claim()
    assert envelope is not None
    data = envelope.to_data()
    assert data["execution_requirements"]["capabilities"] == ["executor.egress"]
    assert data["capability_grant"] == {
        "capabilities": [GET_CUSTOMER.name],
        "effects": [WRITE_NOTE.name],
    }
    assert "executor.egress" not in data["capability_grant"]["capabilities"]
