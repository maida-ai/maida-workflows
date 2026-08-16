"""Treat a provider-owned flow as one honest typed Maida boundary.

The external system remains replaceable. Maida compiles the typed contract,
provider-neutral connector operation, effect policy, and replay boundary. A
deployment registers an adapter separately; no SDK client, session, connected
account, or credential is stored in this workflow.
"""

from __future__ import annotations

from maida.workflows import ExternalWorkflow, RuntimeValue, Workflow


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
            approval_required=True,
            policy_tags=("customer-communication",),
        )

    def build(self, value: RuntimeValue[dict[str, str]]) -> RuntimeValue[dict[str, str]]:
        """Construct the single typed external effect boundary."""
        return self.send(value)


workflow = SendWelcome()
EXAMPLE_INPUT = {"email": "ada@example.com", "name": "Ada"}
