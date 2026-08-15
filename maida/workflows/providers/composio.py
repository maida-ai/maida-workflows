"""Use Composio sessions as replaceable tool and trigger infrastructure.

The adapter targets Composio's small session execution boundary: a deployment
resolves a user-scoped session, then the adapter calls ``session.execute`` with
a configured tool slug, arguments, and optional account selector. The Composio
SDK is intentionally not a package dependency; applications may pass a real
SDK session or a compatible client wrapper.

Maida remains authoritative for capability/effect declarations, policy,
idempotency evidence, accepted history, replay denial, and verification.
Composio sessions own authentication, connected-account selection, and tool
execution. Trigger webhook signature verification must occur before constructing
:class:`ComposioTriggerEvent`.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol, cast

from .._canonical import canonical_data, digest_data
from ..interop import WorkflowStartRequest
from ..userplane import SignalCommand

_STABLE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")
_TOOL_SLUG = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")


class ComposioSession(Protocol):
    """Narrow session API consumed by :class:`ComposioToolAdapter`.

    A real Composio Python session already provides this shape. Session objects
    may contain credentials and connected-account state, so they must remain in
    the resolver callback and must never be stored in a module or workflow
    bundle.
    """

    def execute(
        self,
        tool_slug: str,
        arguments: Mapping[str, Any],
        account: str | None = None,
    ) -> Any:
        """Execute one tool in the session and return the SDK response."""
        ...


@dataclass(frozen=True)
class ComposioToolBinding:
    """Map one provider-neutral connector operation to a Composio tool.

    Parameters
    ----------
    operation
        Stable operation named by a Maida capability or effect declaration.
    tool_slug
        Exact tool exposed by the configured Composio session.
    effectful
        Whether the tool can change external state.
    idempotency_argument
        Optional tool argument that the deployment attests is honored as a
        destination idempotency key. Omitting it keeps the effect outside the
        adapter's ``idempotent_effects`` declaration.

    Notes
    -----
    The binding is deployment configuration. Pin changes through the adapter's
    ``connector_version`` so structural verification reports them.
    """

    operation: str
    tool_slug: str
    effectful: bool
    idempotency_argument: str | None = None

    def __post_init__(self) -> None:
        """Validate operation, tool, classification, and argument identities."""
        _identity("operation", self.operation)
        if not isinstance(self.tool_slug, str) or _TOOL_SLUG.fullmatch(self.tool_slug) is None:
            raise ValueError("tool_slug must be a stable Composio tool identity")
        if not isinstance(self.effectful, bool):
            raise TypeError("effectful must be a boolean")
        if self.idempotency_argument is not None:
            _identity("idempotency_argument", self.idempotency_argument)
            if not self.effectful:
                raise ValueError("read-only tools cannot declare effect idempotency")


SessionResolver = Callable[[str, Any], ComposioSession]
ArgumentMapper = Callable[[str, Any], Mapping[str, Any]]
AccountResolver = Callable[[str, Any], str | None]
ResponseMapper = Callable[[str, Any], Any]


class ComposioToolAdapter:
    """Implement Maida connector/effect protocols with Composio sessions.

    Parameters
    ----------
    connector
        Provider-neutral registry identity used by workflow declarations, such
        as ``email`` or ``source-control``. It need not be ``composio``.
    connector_version
        Immutable deployment pin covering tool mappings and session policy.
    bindings
        Logical operations mapped to exact Composio tool slugs.
    session_resolver
        Runtime callback returning a user-scoped session for a request. Session
        clients and credentials remain private to this callback.
    argument_mapper
        Optional callback converting typed module input into tool arguments.
        The default accepts mappings only.
    account_resolver
        Optional callback selecting a connected account or alias.
    response_mapper
        Optional callback converting the SDK response to provider-neutral
        output. The default unwraps successful ``data`` and fails on ``error``.

    Notes
    -----
    Replay never calls this adapter for effects. Read replay also requires an
    explicit replay-safe live-read policy; otherwise recorded responses are
    used or the call is denied.
    """

    def __init__(
        self,
        *,
        connector: str,
        connector_version: str,
        bindings: tuple[ComposioToolBinding, ...],
        session_resolver: SessionResolver,
        argument_mapper: ArgumentMapper | None = None,
        account_resolver: AccountResolver | None = None,
        response_mapper: ResponseMapper | None = None,
    ) -> None:
        _identity("connector", connector)
        if not isinstance(connector_version, str) or not connector_version.strip():
            raise ValueError("connector_version must be non-empty")
        if not bindings or any(not isinstance(item, ComposioToolBinding) for item in bindings):
            raise ValueError("bindings must contain ComposioToolBinding values")
        by_operation = {item.operation: item for item in bindings}
        if len(by_operation) != len(bindings):
            raise ValueError("Composio operations must be unique")
        for label, callback in (
            ("session_resolver", session_resolver),
            ("argument_mapper", argument_mapper),
            ("account_resolver", account_resolver),
            ("response_mapper", response_mapper),
        ):
            if callback is not None and not callable(callback):
                raise TypeError(f"{label} must be callable")
        self._connector = connector
        self._connector_version = connector_version
        self._bindings = MappingProxyType(by_operation)
        self._session_resolver = session_resolver
        self._argument_mapper = argument_mapper or _mapping_arguments
        self._account_resolver = account_resolver or _no_account
        self._response_mapper = response_mapper or _successful_data

    @property
    def connector(self) -> str:
        """Return the provider-neutral connector registry identity."""
        return self._connector

    @property
    def connector_version(self) -> str:
        """Return the immutable tool/session configuration pin."""
        return self._connector_version

    @property
    def operations(self) -> frozenset[str]:
        """Return configured read-only logical operations."""
        return frozenset(
            operation for operation, binding in self._bindings.items() if not binding.effectful
        )

    @property
    def effect_operations(self) -> frozenset[str]:
        """Return configured consequential logical operations."""
        return frozenset(
            operation for operation, binding in self._bindings.items() if binding.effectful
        )

    @property
    def idempotent_effects(self) -> frozenset[str]:
        """Return effects explicitly mapped to a destination idempotency field."""
        return frozenset(
            operation
            for operation, binding in self._bindings.items()
            if binding.idempotency_argument is not None
        )

    async def read(self, operation: str, request: Any) -> Any:
        """Execute a configured read-only tool through a resolved session."""
        binding = self._binding(operation, effectful=False)
        return await self._invoke(binding, request, idempotency_key=None)

    async def effect(self, operation: str, request: Any, idempotency_key: str) -> Any:
        """Execute a configured effect and forward a declared idempotency key."""
        binding = self._binding(operation, effectful=True)
        return await self._invoke(binding, request, idempotency_key=idempotency_key)

    def _binding(self, operation: str, *, effectful: bool) -> ComposioToolBinding:
        binding = self._bindings.get(operation)
        if binding is None or binding.effectful is not effectful:
            category = "effect" if effectful else "read"
            raise LookupError(f"Composio {category} operation {operation!r} is not configured")
        return binding

    async def _invoke(
        self,
        binding: ComposioToolBinding,
        request: Any,
        *,
        idempotency_key: str | None,
    ) -> Any:
        session = self._session_resolver(binding.operation, request)
        if not callable(getattr(session, "execute", None)):
            raise TypeError("session_resolver must return a ComposioSession")
        raw_arguments = self._argument_mapper(binding.operation, request)
        if not isinstance(raw_arguments, Mapping):
            raise TypeError("argument_mapper must return a mapping")
        arguments = dict(cast(Mapping[str, Any], canonical_data(dict(raw_arguments))))
        if binding.idempotency_argument is not None:
            if idempotency_key is None:
                raise ValueError("idempotent effect invocation requires an idempotency key")
            existing = arguments.get(binding.idempotency_argument)
            if existing is not None and existing != idempotency_key:
                raise ValueError("tool arguments conflict with Maida's idempotency key")
            arguments[binding.idempotency_argument] = idempotency_key
        account = self._account_resolver(binding.operation, request)
        if account is not None and (not isinstance(account, str) or not account.strip()):
            raise ValueError("account_resolver must return a non-empty account or None")
        response = session.execute(binding.tool_slug, arguments, account)
        if inspect.isawaitable(response):
            response = await response
        return self._response_mapper(binding.operation, response)


@dataclass(frozen=True)
class ComposioTriggerEvent:
    """Verified, normalized Composio trigger data for workflow routing.

    Parameters
    ----------
    event_id
        Composio message identity used for idempotent workflow starts.
    trigger_slug, trigger_id
        Trigger type and configured trigger-instance identity.
    user_id
        External subject used by the application to derive tenant scope. Maida
        never trusts it directly as a tenant authorization decision.
    data
        Trigger-type-specific canonical payload.
    timestamp
        Provider event timestamp retained for application logic.

    Notes
    -----
    Construct this object only after verifying the webhook signature with the
    provider SDK or an equivalent trusted ingress. Authentication and webhook
    secrets do not enter this value.
    """

    event_id: str
    trigger_slug: str
    trigger_id: str
    user_id: str
    data: Mapping[str, Any]
    timestamp: str

    def __post_init__(self) -> None:
        """Validate identities and freeze canonical trigger data."""
        for label, value in (
            ("event_id", self.event_id),
            ("trigger_slug", self.trigger_slug),
            ("trigger_id", self.trigger_id),
            ("user_id", self.user_id),
            ("timestamp", self.timestamp),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{label} must be non-empty")
        if not isinstance(self.data, Mapping):
            raise TypeError("trigger data must be a mapping")
        object.__setattr__(self, "data", MappingProxyType(canonical_data(dict(self.data))))

    @classmethod
    def from_verified_payload(cls, payload: Mapping[str, Any]) -> ComposioTriggerEvent:
        """Parse the current verified Composio trigger message envelope.

        Raises
        ------
        ValueError
            If the message type, metadata, or required identities are invalid.
        TypeError
            If the envelope or trigger data is not an object.
        """
        if not isinstance(payload, Mapping):
            raise TypeError("verified trigger payload must be a mapping")
        if payload.get("type") != "composio.trigger.message":
            raise ValueError("payload is not a Composio trigger message")
        metadata = payload.get("metadata")
        data = payload.get("data")
        if not isinstance(metadata, Mapping) or not isinstance(data, Mapping):
            raise TypeError("trigger metadata and data must be objects")
        try:
            return cls(
                event_id=payload["id"],
                trigger_slug=metadata["trigger_slug"],
                trigger_id=metadata["trigger_id"],
                user_id=metadata["user_id"],
                data=data,
                timestamp=payload["timestamp"],
            )
        except KeyError as exc:
            raise ValueError(f"trigger payload is missing {exc.args[0]!r}") from exc

    def start_request(
        self,
        workflow_id: str,
        *,
        input: Any | None = None,
    ) -> WorkflowStartRequest:
        """Translate this event into a retry-safe workflow start request."""
        value = dict(self.data) if input is None else input
        return WorkflowStartRequest(
            workflow_id=workflow_id,
            input=value,
            idempotency_key=f"composio:{self.event_id}:{workflow_id}",
        )

    def signal_command(
        self,
        name: str,
        *,
        value: Any | None = None,
        request_id: str | None = None,
    ) -> SignalCommand:
        """Translate this event into a deterministic durable signal command."""
        command_id = digest_data(
            {
                "provider": "composio",
                "event_id": self.event_id,
                "name": name,
                "request_id": request_id,
            }
        )
        return SignalCommand(
            command_id=command_id,
            name=name,
            value=dict(self.data) if value is None else value,
            request_id=request_id,
        )


def _identity(label: str, value: Any) -> str:
    if not isinstance(value, str) or _STABLE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a stable identity")
    return value


def _mapping_arguments(operation: str, request: Any) -> Mapping[str, Any]:
    del operation
    if not isinstance(request, Mapping):
        raise TypeError("default Composio argument mapping requires a request mapping")
    return request


def _no_account(operation: str, request: Any) -> None:
    del operation, request
    return None


def _successful_data(operation: str, response: Any) -> Any:
    del operation
    if isinstance(response, Mapping):
        if response.get("error"):
            raise RuntimeError("Composio tool execution failed")
        return response.get("data", response)
    error = getattr(response, "error", None)
    if error:
        raise RuntimeError("Composio tool execution failed")
    return getattr(response, "data", response)
