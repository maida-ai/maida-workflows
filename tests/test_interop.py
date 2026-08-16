from __future__ import annotations

from typing import Any

import pytest

from maida.workflows import (
    ExecutionContext,
    ExternalWorkflow,
    Idempotency,
    module_digest,
)


def _external(*, provider_version: str | None = "provider-v1") -> ExternalWorkflow[str, str]:
    return ExternalWorkflow(
        module_id="accounts.lookup",
        workflow="lookup-account",
        provider="example-provider",
        provider_version=provider_version,
        input_type=str,
        output_type=str,
        effectful=False,
    )


def test_external_workflow_identity_includes_the_provider_contract() -> None:
    assert module_digest(_external()) != module_digest(_external(provider_version="provider-v2"))


@pytest.mark.asyncio
async def test_external_workflow_uses_only_the_runtime_broker_boundary() -> None:
    read = _external()
    effect = ExternalWorkflow(
        module_id="messages.send",
        workflow="send-message",
        provider="example-provider",
        input_type=str,
        output_type=str,
        effectful=True,
    )

    class Broker:
        async def read(self, connector: str, operation: str, request: Any, **kwargs: Any) -> Any:
            return f"read:{request}"

        async def effect(self, connector: str, operation: str, request: Any, **kwargs: Any) -> Any:
            return f"sent:{request}"

    context = ExecutionContext("run", "task", "step", broker=Broker())
    assert await read.execute("acct", context) == "read:acct"
    assert await effect.execute("hello", context) == "sent:hello"


@pytest.mark.asyncio
async def test_external_workflow_without_runtime_broker_fails_closed() -> None:
    with pytest.raises(RuntimeError, match="broker"):
        await _external().execute("acct-7", ExecutionContext("run", "task", "step"))


def test_external_module_identity_is_strict() -> None:
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

    effect = ExternalWorkflow(**{**common, "effectful": True, "idempotency": Idempotency.REQUIRED})
    assert effect.effectful
