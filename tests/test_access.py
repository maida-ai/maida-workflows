from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from maida.workflows import (
    Capability,
    Connector,
    Effect,
    EffectSpec,
    ExecutionContext,
    Idempotency,
    RuntimeValue,
    Workflow,
    compile_workflow,
)
from maida.workflows._canonical import digest_data
from maida.workflows.alignment import DiffKind, GraphAligner
from maida.workflows.ir import PlanIR

GET_CUSTOMER = Capability(
    "crm.customer.read",
    connector="crm",
    operation="get_customer",
    input_type=str,
    output_type=dict[str, str],
    connector_version="crm-adapter-v1",
    policy_tags=("customer-data",),
)
SEND_EMAIL = EffectSpec(
    "email.send",
    connector="email",
    operation="send",
    input_type=dict[str, str],
    output_type=str,
    connector_version="email-adapter-v1",
    idempotency=Idempotency.REQUIRED,
    approval_required=True,
    policy_tags=("external-write",),
)


class SupportWorkflow(Workflow[str, str]):
    workflow_id = "typed-access"
    input_type = str
    output_type = str

    def __init__(
        self,
        capability: Capability[str, dict[str, str]] = GET_CUSTOMER,
        effect: EffectSpec[dict[str, str], str] = SEND_EMAIL,
    ) -> None:
        self.customer = Connector(capability)
        self.send = Effect(effect)

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.send(self.customer(value))


class FakeBroker:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Any]] = []

    async def read(self, connector: str, operation: str, request: Any) -> Any:
        self.calls.append((connector, operation, request))
        return {"email": "ada@example.test"}

    async def effect(self, connector: str, operation: str, request: Any) -> Any:
        self.calls.append((connector, operation, request))
        return "receipt-1"


def test_external_access_contracts_are_typed_canonical_and_strict() -> None:
    assert GET_CUSTOMER.to_data() == {
        "connector": "crm",
        "connector_version": "crm-adapter-v1",
        "input_schema_digest": GET_CUSTOMER.input_schema_digest,
        "kind": "capability",
        "name": "crm.customer.read",
        "operation": "get_customer",
        "output_schema_digest": GET_CUSTOMER.output_schema_digest,
        "policy_tags": ["customer-data"],
    }
    assert SEND_EMAIL.to_data()["idempotency"] == "required"
    assert SEND_EMAIL.to_data()["approval_required"] is True
    with pytest.raises(ValueError, match="name"):
        Capability("", connector="crm", operation="read", input_type=str, output_type=str)
    with pytest.raises(ValueError, match="connector"):
        Capability("crm.read", connector="", operation="read", input_type=str, output_type=str)
    with pytest.raises(ValueError, match="operation"):
        EffectSpec("email.send", connector="email", operation="", input_type=str, output_type=str)
    with pytest.raises(ValueError, match="unique"):
        replace(GET_CUSTOMER, policy_tags=("same", "same"))


def test_compiled_ir_exposes_access_contracts_and_loads_legacy_ir() -> None:
    plan = compile_workflow(SupportWorkflow())
    customer, send = plan.executable_steps

    assert plan.version == "0.2.0"
    assert customer.capabilities == (GET_CUSTOMER.to_data(),)
    assert customer.effects == ()
    assert send.capabilities == ()
    assert send.effects == (SEND_EMAIL.to_data(),)
    assert plan.canonical_json() == compile_workflow(SupportWorkflow()).canonical_json()

    legacy = plan.to_dict()
    legacy["version"] = "0.1.0"
    for step in legacy["steps"]:
        step.pop("capabilities", None)
        step.pop("effects", None)
    loaded = PlanIR.from_dict(legacy)
    assert loaded.version == "0.1.0"
    assert all(step.capabilities == () and step.effects == () for step in loaded.steps)
    assert loaded.to_dict() == legacy
    assert loaded.digest == digest_data(legacy)

    invalid_legacy = compile_workflow(SupportWorkflow()).to_dict()
    invalid_legacy["version"] = "0.1.0"
    with pytest.raises(ValueError, match="does not define external access"):
        PlanIR.from_dict(invalid_legacy)


def test_access_changes_keep_replay_identity_and_receive_specific_diff_kinds() -> None:
    source = compile_workflow(SupportWorkflow())
    changed_capability = replace(
        GET_CUSTOMER,
        connector_version="crm-adapter-v2",
        policy_tags=("customer-data", "regional"),
    )
    changed_effect = replace(
        SEND_EMAIL,
        idempotency=Idempotency.OPTIONAL,
        connector_version="email-adapter-v2",
    )
    current = compile_workflow(SupportWorkflow(changed_capability, changed_effect))

    assert [step.replay_key for step in source.executable_steps] == [
        step.replay_key for step in current.executable_steps
    ]
    changes = GraphAligner().align(source, current).diff.changes
    kinds = {change.kind for change in changes}
    assert DiffKind.CAPABILITY_CHANGED in kinds
    assert DiffKind.EFFECT_CHANGED in kinds
    assert DiffKind.CONNECTOR_CHANGED in kinds
    assert DiffKind.POLICY_CHANGED in kinds


@pytest.mark.asyncio
async def test_connector_and_effect_modules_route_supported_access_through_broker() -> None:
    broker = FakeBroker()
    context = ExecutionContext("run", "task", "instance", broker=broker)

    customer = await Connector(GET_CUSTOMER).execute("ada", context)
    receipt = await Effect(SEND_EMAIL).execute(customer, context)

    assert receipt == "receipt-1"
    assert broker.calls == [
        ("crm", "get_customer", "ada"),
        ("email", "send", {"email": "ada@example.test"}),
    ]
    with pytest.raises(RuntimeError, match="access broker"):
        await Connector(GET_CUSTOMER).execute("ada", ExecutionContext("run", "task", "instance"))
