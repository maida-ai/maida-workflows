from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from maida.workflows import SignalCommand, WorkflowStartRequest
from maida.workflows.access import ConnectorRegistry
from maida.workflows.providers.composio import (
    ComposioToolAdapter,
    ComposioToolBinding,
    ComposioTriggerEvent,
)


class Session:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], str | None]] = []
        self.response: Any = {"data": {"ok": True}, "error": None, "log_id": "log-1"}

    def execute(
        self,
        tool_slug: str,
        arguments: Mapping[str, Any],
        account: str | None = None,
    ) -> Any:
        self.calls.append((tool_slug, dict(arguments), account))
        return self.response


def _adapter(session: Session) -> ComposioToolAdapter:
    return ComposioToolAdapter(
        connector="email",
        connector_version="composio-tools-2026-08",
        bindings=(
            ComposioToolBinding("find", "GMAIL_FETCH_EMAILS", False),
            ComposioToolBinding(
                "send",
                "GMAIL_SEND_EMAIL",
                True,
                idempotency_argument="request_id",
            ),
        ),
        session_resolver=lambda operation, request: session,
        account_resolver=lambda operation, request: "primary",
    )


@pytest.mark.asyncio
async def test_tool_adapter_uses_sessions_without_exposing_sdk_state() -> None:
    session = Session()
    adapter = _adapter(session)
    registry = ConnectorRegistry()
    registry.register(adapter)

    read = await adapter.read("find", {"query": "from:ada"})
    effected = await adapter.effect("send", {"to": "ada@example.com"}, "mwf-key")

    assert read == effected == {"ok": True}
    assert session.calls == [
        ("GMAIL_FETCH_EMAILS", {"query": "from:ada"}, "primary"),
        (
            "GMAIL_SEND_EMAIL",
            {"request_id": "mwf-key", "to": "ada@example.com"},
            "primary",
        ),
    ]
    assert adapter.connector == "email"
    assert adapter.operations == frozenset({"find"})
    assert adapter.effect_operations == frozenset({"send"})
    assert adapter.idempotent_effects == frozenset({"send"})


@pytest.mark.asyncio
async def test_tool_adapter_fails_closed_on_provider_errors_and_key_conflicts() -> None:
    session = Session()
    adapter = _adapter(session)
    session.response = {"data": None, "error": "private provider diagnostic"}

    with pytest.raises(RuntimeError, match="execution failed") as captured:
        await adapter.read("find", {"query": "x"})
    assert "private provider diagnostic" not in str(captured.value)
    with pytest.raises(ValueError, match="conflict"):
        await adapter.effect("send", {"request_id": "other"}, "mwf-key")
    with pytest.raises(LookupError, match="not configured"):
        await adapter.effect("find", {}, "mwf-key")


def _payload() -> dict[str, Any]:
    return {
        "id": "msg_abc123",
        "type": "composio.trigger.message",
        "metadata": {
            "log_id": "log_abc123",
            "trigger_slug": "GITHUB_COMMIT_EVENT",
            "trigger_id": "ti_xyz789",
            "connected_account_id": "ca_private",
            "auth_config_id": "ac_private",
            "user_id": "user-123",
        },
        "data": {"commit_sha": "a1b2c3d", "message": "fix"},
        "timestamp": "2026-01-15T10:30:00Z",
    }


def test_verified_trigger_translates_to_neutral_start_and_signal_contracts() -> None:
    event = ComposioTriggerEvent.from_verified_payload(_payload())
    start = event.start_request("inspect-commit")
    repeated = event.start_request("inspect-commit")
    signal = event.signal_command("commit.received", request_id="waiting-for-commit")

    assert isinstance(start, WorkflowStartRequest)
    assert start == repeated
    assert start.workflow_id == "inspect-commit"
    assert start.input == {"commit_sha": "a1b2c3d", "message": "fix"}
    assert start.to_data() == {
        "input": {"commit_sha": "a1b2c3d", "message": "fix"},
        "idempotency_key": "composio:msg_abc123:inspect-commit",
    }
    assert isinstance(signal, SignalCommand)
    assert signal == event.signal_command("commit.received", request_id="waiting-for-commit")
    assert signal.value == {"commit_sha": "a1b2c3d", "message": "fix"}
    encoded = repr(start.to_data()) + repr(signal.to_data())
    assert "ca_private" not in encoded
    assert "ac_private" not in encoded


def test_trigger_parser_requires_the_verified_current_envelope() -> None:
    malformed = _payload()
    malformed["type"] = "ordinary.webhook"
    with pytest.raises(ValueError, match="not a Composio trigger"):
        ComposioTriggerEvent.from_verified_payload(malformed)
    with pytest.raises(TypeError, match="metadata and data"):
        ComposioTriggerEvent.from_verified_payload({**_payload(), "metadata": "unverified"})


def test_binding_rejects_unsubstantiated_read_idempotency() -> None:
    with pytest.raises(ValueError, match="read-only"):
        ComposioToolBinding("find", "GMAIL_FETCH_EMAILS", False, "request_id")
