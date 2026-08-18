"""Treat a provider-owned flow as one honest typed Maida boundary.

The external system remains replaceable. Maida compiles the typed contract,
provider-neutral connector operation, effect policy, and replay boundary. A
deployment registers an adapter separately; no SDK client, session, connected
account, or credential is stored in this workflow.
"""

from __future__ import annotations

from typing import Any

from maida.workflows import (
    ConnectorRegistry,
    ExternalWorkflow,
    RunResult,
    RuntimeValue,
    Workflow,
    WorkflowRunner,
)
from maida.workflows.persistence import PostgresStore


class SendWelcome(Workflow[dict[str, str], dict[str, str]]):
    """Invoke a provider-owned welcome flow as one consequential module."""

    workflow_id = "onboarding-external"
    input_type = dict[str, str]
    output_type = dict[str, str]

    def __init__(self) -> None:
        self.send = ExternalWorkflow(
            module_id="customer.welcome",
            workflow="send-welcome",
            provider="customer-messaging",
            provider_version="workflow-2026-08",
            input_type=dict[str, str],
            output_type=dict[str, str],
            effectful=True,
            policy_tags=("customer-communication",),
        )

    def build(self, value: RuntimeValue[dict[str, str]]) -> RuntimeValue[dict[str, str]]:
        """Construct the single typed external effect boundary."""
        return self.send(value)


workflow = SendWelcome()
EXAMPLE_INPUT = {"email": "ada@example.com", "name": "Ada"}
EXPECTED_OUTPUT = {"email": "ada@example.com", "name": "Ada", "status": "sent"}


class _LocalAdapter:
    connector = "customer-messaging"
    connector_version = "workflow-2026-08"
    operations: frozenset[str] = frozenset()
    effect_operations = frozenset({"send-welcome"})
    idempotent_effects = frozenset({"send-welcome"})

    async def read(self, operation: str, request: Any) -> Any:
        raise AssertionError("the welcome flow is effect-only")

    async def effect(
        self,
        operation: str,
        request: dict[str, str],
        *,
        idempotency_key: str,
    ) -> dict[str, str]:
        return {**request, "status": "sent"}


connectors = ConnectorRegistry((_LocalAdapter(),))


async def run_example(
    store: PostgresStore,
    value: dict[str, str] | None = None,
) -> RunResult:
    """Execute the boundary through a deterministic deployment-owned adapter."""
    request = EXAMPLE_INPUT if value is None else value
    return await WorkflowRunner(store, connectors=connectors).run(workflow, request)
