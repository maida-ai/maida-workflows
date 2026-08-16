from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
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


@pytest.mark.parametrize(
    ("values", "error", "message"),
    (
        (("not valid", "GMAIL_FETCH_EMAILS", False, None), ValueError, "operation"),
        (("find", "not valid", False, None), ValueError, "tool_slug"),
        (("find", "GMAIL_FETCH_EMAILS", 1, None), TypeError, "boolean"),
        (("send", "GMAIL_SEND_EMAIL", True, "not valid"), ValueError, "idempotency"),
    ),
)
def test_binding_validates_all_deployment_identities(
    values: tuple[Any, ...], error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        ComposioToolBinding(*values)


def test_adapter_configuration_is_strict() -> None:
    binding = ComposioToolBinding("find", "GMAIL_FETCH_EMAILS", False)
    common: dict[str, Any] = {
        "connector": "email",
        "connector_version": "v1",
        "bindings": (binding,),
        "session_resolver": lambda operation, request: Session(),
    }
    cases: tuple[tuple[dict[str, Any], type[Exception], str], ...] = (
        ({"connector": "not valid"}, ValueError, "connector"),
        ({"connector_version": ""}, ValueError, "connector_version"),
        ({"bindings": ()}, ValueError, "bindings"),
        ({"bindings": (object(),)}, ValueError, "bindings"),
        ({"bindings": (binding, binding)}, ValueError, "unique"),
        ({"session_resolver": 1}, TypeError, "session_resolver"),
        ({"argument_mapper": 1}, TypeError, "argument_mapper"),
        ({"account_resolver": 1}, TypeError, "account_resolver"),
        ({"response_mapper": 1}, TypeError, "response_mapper"),
    )
    for changes, error, message in cases:
        with pytest.raises(error, match=message):
            ComposioToolAdapter(**{**common, **changes})


@pytest.mark.asyncio
async def test_adapter_validates_resolvers_arguments_accounts_and_idempotency() -> None:
    read_binding = ComposioToolBinding("find", "GMAIL_FETCH_EMAILS", False)
    effect_binding = ComposioToolBinding("send", "GMAIL_SEND_EMAIL", True, "request_id")

    invalid_session = ComposioToolAdapter(
        connector="email",
        connector_version="v1",
        bindings=(read_binding,),
        session_resolver=lambda operation, request: object(),  # type: ignore[arg-type,return-value]
    )
    with pytest.raises(TypeError, match="ComposioSession"):
        await invalid_session.read("find", {})

    invalid_arguments = ComposioToolAdapter(
        connector="email",
        connector_version="v1",
        bindings=(read_binding,),
        session_resolver=lambda operation, request: Session(),
        argument_mapper=lambda operation, request: [],  # type: ignore[arg-type,return-value]
    )
    with pytest.raises(TypeError, match="return a mapping"):
        await invalid_arguments.read("find", {})

    invalid_account = ComposioToolAdapter(
        connector="email",
        connector_version="v1",
        bindings=(read_binding,),
        session_resolver=lambda operation, request: Session(),
        account_resolver=lambda operation, request: "",
    )
    with pytest.raises(ValueError, match="non-empty account"):
        await invalid_account.read("find", {})

    default_arguments = ComposioToolAdapter(
        connector="email",
        connector_version="v1",
        bindings=(read_binding,),
        session_resolver=lambda operation, request: Session(),
    )
    with pytest.raises(TypeError, match="request mapping"):
        await default_arguments.read("find", "not-a-mapping")

    idempotent = ComposioToolAdapter(
        connector="email",
        connector_version="v1",
        bindings=(effect_binding,),
        session_resolver=lambda operation, request: Session(),
    )
    with pytest.raises(ValueError, match="requires an idempotency key"):
        await idempotent.effect("send", {}, None)  # type: ignore[arg-type]
    with pytest.raises(LookupError, match="read operation"):
        await idempotent.read("send", {})


class AsyncSession(Session):
    async def execute(
        self,
        tool_slug: str,
        arguments: Mapping[str, Any],
        account: str | None = None,
    ) -> Any:
        self.calls.append((tool_slug, dict(arguments), account))
        return self.response


@dataclass
class ObjectResponse:
    data: Any = None
    error: Any = None


@pytest.mark.asyncio
async def test_adapter_supports_async_sessions_and_object_responses() -> None:
    session = AsyncSession()
    adapter = ComposioToolAdapter(
        connector="email",
        connector_version="v1",
        bindings=(ComposioToolBinding("find", "GMAIL_FETCH_EMAILS", False),),
        session_resolver=lambda operation, request: session,
    )

    session.response = ObjectResponse(data={"ok": True})
    assert await adapter.read("find", {}) == {"ok": True}
    session.response = ObjectResponse(error="private")
    with pytest.raises(RuntimeError, match="execution failed"):
        await adapter.read("find", {})
    session.response = "raw"
    assert await adapter.read("find", {}) == "raw"
    session.response = {"value": 1}
    assert await adapter.read("find", {}) == {"value": 1}


def test_trigger_contract_rejects_missing_or_untrusted_fields() -> None:
    with pytest.raises(TypeError, match="must be a mapping"):
        ComposioTriggerEvent.from_verified_payload([])  # type: ignore[arg-type]
    missing = _payload()
    del missing["id"]
    with pytest.raises(ValueError, match="missing 'id'"):
        ComposioTriggerEvent.from_verified_payload(missing)
    with pytest.raises(TypeError, match="trigger data"):
        ComposioTriggerEvent("id", "slug", "trigger", "user", [], "now")  # type: ignore[arg-type]

    for field in ("event_id", "trigger_slug", "trigger_id", "user_id", "timestamp"):
        values: dict[str, Any] = {
            "event_id": "id",
            "trigger_slug": "slug",
            "trigger_id": "trigger",
            "user_id": "user",
            "data": {},
            "timestamp": "now",
        }
        values[field] = ""
        with pytest.raises(ValueError, match=field):
            ComposioTriggerEvent(**values)


def test_trigger_translation_accepts_explicit_application_values() -> None:
    event = ComposioTriggerEvent.from_verified_payload(_payload())

    start = event.start_request("inspect-commit", input={"selected": True})
    signal = event.signal_command("commit.received", value={"sha": "selected"})

    assert start.input == {"selected": True}
    assert signal.value == {"sha": "selected"}
