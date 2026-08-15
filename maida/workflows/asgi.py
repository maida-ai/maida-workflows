"""Expose the workflow userplane through a small dependency-free ASGI adapter.

The adapter translates JSON requests, typed commands, and server-sent events;
all authoritative state remains in the durable runtime. Applications may mount
it in any ASGI server or replace it with another transport while retaining the
same :mod:`maida.workflows.userplane` contracts.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol
from urllib.parse import parse_qs

from ._canonical import canonical_data
from .coordination import WorkflowCatalog
from .models import RunStatus
from .persistence import InvalidRunStateError, PersistenceError, TenantAccessError
from .userplane import WorkflowClient, parse_command

ASGIScope = dict[str, Any]
ASGIReceive = Callable[[], Awaitable[dict[str, Any]]]
ASGISend = Callable[[dict[str, Any]], Awaitable[None]]
TenantResolver = Callable[[ASGIScope], str]


class _ASGIStore(Protocol):
    values: Any


class UserplaneASGI:
    """ASGI application for starting, observing, and commanding workflow runs.

    Parameters
    ----------
    store
        Durable workflow store shared with the control plane and workers.
    catalog
        Deployment-pinned workflow factories addressable by workflow ID.
    tenant_resolver
        Trusted host callback deriving tenant scope from authenticated ASGI
        context. Request bodies and arbitrary tenant headers are never trusted.
    poll_interval
        Delay between empty SSE projection polls.

    Notes
    -----
    The application never executes module handlers. A separately deployed
    :class:`~maida.workflows.WorkflowCoordinator` and compatible workers make
    progress after a run is accepted.
    """

    def __init__(
        self,
        store: Any,
        catalog: WorkflowCatalog,
        *,
        tenant_resolver: TenantResolver,
        poll_interval: float = 0.25,
    ) -> None:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        self.store = store
        self.catalog = catalog
        self.tenant_resolver = tenant_resolver
        self.poll_interval = poll_interval
        self.client = WorkflowClient(store)

    async def __call__(
        self,
        scope: ASGIScope,
        receive: ASGIReceive,
        send: ASGISend,
    ) -> None:
        """Handle one ASGI HTTP request and emit JSON or SSE responses."""
        if scope.get("type") != "http":
            raise RuntimeError("the workflow userplane supports ASGI HTTP requests only")
        try:
            tenant_id = self.tenant_resolver(scope)
            if not tenant_id.strip():
                raise ValueError("tenant resolver returned an empty identity")
            await self._route(scope, receive, send, tenant_id)
        except TenantAccessError:
            await self._json(send, 404, {"error": "run was not found"})
        except InvalidRunStateError as exc:
            await self._json(send, 409, {"error": str(exc)})
        except PersistenceError as exc:
            await self._json(send, 404, {"error": str(exc)})
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
            await self._json(send, 400, {"error": str(exc)})

    async def _route(
        self,
        scope: ASGIScope,
        receive: ASGIReceive,
        send: ASGISend,
        tenant_id: str,
    ) -> None:
        method = str(scope.get("method", "GET")).upper()
        parts = [part for part in str(scope.get("path", "")).split("/") if part]
        if len(parts) == 4 and parts[:2] == ["v1", "workflows"] and parts[3] == "runs":
            if method != "POST":
                raise ValueError("workflow run creation requires POST")
            payload = await self._body(receive)
            if set(payload) not in ({"input"}, {"input", "idempotency_key"}):
                raise ValueError("run request must contain input and may contain idempotency_key")
            workflow = self.catalog.resolve_workflow(parts[2])
            run = self.client.start(
                workflow,
                payload["input"],
                tenant_id=tenant_id,
                idempotency_key=payload.get("idempotency_key"),
            )
            snapshot = run.snapshot()
            await self._json(
                send,
                202,
                {
                    "run_id": run.run_id,
                    "status": snapshot.status,
                    "definition_digest": snapshot.definition_digest,
                    "events": f"/v1/runs/{run.run_id}/events",
                },
            )
            return
        if len(parts) >= 3 and parts[:2] == ["v1", "runs"]:
            run = self.client.attach(parts[2], tenant_id=tenant_id)
            if len(parts) == 3 and method == "GET":
                await self._json(send, 200, run.snapshot())
                return
            if len(parts) == 4 and parts[3] == "commands" and method == "POST":
                receipt = run.send(parse_command(await self._body(receive)))
                await self._json(send, 200 if receipt.duplicate else 202, receipt)
                return
            if len(parts) == 4 and parts[3] == "events" and method == "GET":
                query = parse_qs(bytes(scope.get("query_string", b"")).decode())
                after = self._cursor(scope, query)
                limit = int(query.get("limit", ["100"])[0])
                if self._accepts_sse(scope):
                    await self._sse(run, receive, send, after=after, limit=limit)
                else:
                    page = run.events(after=after, limit=limit)
                    await self._json(
                        send,
                        200,
                        {
                            "events": [event.to_data() for event in page.events],
                            "next_cursor": page.next_cursor,
                            "has_more": page.has_more,
                        },
                    )
                return
        raise PersistenceError("route was not found")

    async def _sse(
        self,
        run: Any,
        receive: ASGIReceive,
        send: ASGISend,
        *,
        after: int,
        limit: int,
    ) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"text/event-stream"),
                    (b"cache-control", b"no-cache"),
                ],
            }
        )
        cursor = after
        while True:
            page = run.events(after=cursor, limit=limit)
            for event in page.events:
                cursor = event.sequence
                await send(
                    {"type": "http.response.body", "body": event.to_sse(), "more_body": True}
                )
            cursor = max(cursor, page.next_cursor)
            terminal = run.snapshot().status in {
                RunStatus.SUCCEEDED,
                RunStatus.FAILED,
                RunStatus.CANCELLED,
            }
            if terminal and not page.has_more:
                await send({"type": "http.response.body", "body": b"", "more_body": False})
                return
            if page.has_more:
                continue
            await send(
                {"type": "http.response.body", "body": b": keep-alive\n\n", "more_body": True}
            )
            if await self._disconnected(receive):
                return

    async def _disconnected(self, receive: ASGIReceive) -> bool:
        try:
            message = await asyncio.wait_for(receive(), timeout=self.poll_interval)
        except TimeoutError:
            return False
        return message.get("type") == "http.disconnect"

    @staticmethod
    def _accepts_sse(scope: ASGIScope) -> bool:
        return "text/event-stream" in UserplaneASGI._headers(scope).get("accept", "")

    @staticmethod
    def _cursor(scope: ASGIScope, query: Mapping[str, list[str]]) -> int:
        raw = query.get("after", [UserplaneASGI._headers(scope).get("last-event-id", "0")])[0]
        cursor = int(raw)
        if cursor < 0:
            raise ValueError("event cursor must be non-negative")
        return cursor

    @staticmethod
    def _headers(scope: ASGIScope) -> dict[str, str]:
        return {
            bytes(name).decode().lower(): bytes(value).decode()
            for name, value in scope.get("headers", [])
        }

    @staticmethod
    async def _body(receive: ASGIReceive) -> dict[str, Any]:
        content = bytearray()
        while True:
            message = await receive()
            if message.get("type") == "http.disconnect":
                raise ValueError("request disconnected before its body completed")
            content.extend(message.get("body", b""))
            if len(content) > 1024 * 1024:
                raise ValueError("request body exceeds 1 MiB")
            if not message.get("more_body", False):
                break
        value = json.loads(content.decode() or "{}")
        if not isinstance(value, dict):
            raise ValueError("request body must be a JSON object")
        return value

    @staticmethod
    async def _json(send: ASGISend, status: int, value: Any) -> None:
        body = json.dumps(
            canonical_data(value),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": body, "more_body": False})


def create_userplane_app(
    store: Any,
    catalog: WorkflowCatalog,
    *,
    tenant_resolver: TenantResolver | None = None,
    poll_interval: float = 0.25,
) -> UserplaneASGI:
    """Create the supported ASGI adapter for a durable workflow userplane.

    Parameters
    ----------
    store
        Migrated durable workflow store.
    catalog
        Deployment-pinned workflow factories.
    tenant_resolver
        Trusted authentication integration. The local default always returns
        ``"local"`` and is suitable only for single-tenant development.
    poll_interval
        Empty SSE polling and heartbeat interval.
    """
    resolver = tenant_resolver or (lambda scope: "local")
    return UserplaneASGI(
        store,
        catalog,
        tenant_resolver=resolver,
        poll_interval=poll_interval,
    )
