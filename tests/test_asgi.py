from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from examples.userplane_quickstart import GreetingWorkflow, create_app
from maida.workflows import (
    PauseCommand,
    WorkflowCatalog,
    WorkflowClient,
    WorkflowCoordinator,
    create_userplane_app,
)
from maida.workflows.asgi import UserplaneASGI
from maida.workflows.persistence import PostgresStore
from maida.workflows.replay import build_module_registry
from maida.workflows.runtime import TaskWorker, WorkflowScheduler


async def _request(
    app: UserplaneASGI,
    method: str,
    path: str,
    *,
    body: Any = None,
    headers: dict[str, str] | None = None,
    query: str = "",
) -> tuple[int, dict[str, str], bytes]:
    request_body = b"" if body is None else json.dumps(body).encode()
    received = False
    messages: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        nonlocal received
        if not received:
            received = True
            return {"type": "http.request", "body": request_body, "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    raw_headers = [
        (name.lower().encode(), value.encode()) for name, value in (headers or {}).items()
    ]
    await app(
        {
            "type": "http",
            "method": method,
            "path": path,
            "query_string": query.encode(),
            "headers": raw_headers,
            "state": {"tenant_id": "tenant-a"},
        },
        receive,
        send,
    )
    start = next(message for message in messages if message["type"] == "http.response.start")
    response_headers = {key.decode(): value.decode() for key, value in start.get("headers", [])}
    content = b"".join(
        message.get("body", b"") for message in messages if message["type"] == "http.response.body"
    )
    return int(start["status"]), response_headers, content


def _tenant(scope: dict[str, Any]) -> str:
    return str(scope["state"]["tenant_id"])


def test_asgi_adapter_validates_transport_configuration() -> None:
    catalog = WorkflowCatalog([GreetingWorkflow])
    with pytest.raises(ValueError, match="poll_interval"):
        create_userplane_app(object(), catalog, poll_interval=0)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_asgi_starts_observes_and_commands_runs_without_execution_affinity(
    postgres_store: PostgresStore,
) -> None:
    GreetingWorkflow.calls = 0
    catalog = WorkflowCatalog([GreetingWorkflow])
    app = create_userplane_app(postgres_store, catalog, tenant_resolver=_tenant)

    status, headers, content = await _request(
        app,
        "POST",
        "/v1/workflows/greeting-api/runs",
        body={"input": "Ada"},
    )

    assert status == 202
    assert headers["content-type"] == "application/json"
    started = json.loads(content)
    assert started["status"] == "RUNNING"
    assert GreetingWorkflow.calls == 0

    status, _, content = await _request(app, "GET", f"/v1/runs/{started['run_id']}")
    assert status == 200
    assert json.loads(content)["run_id"] == started["run_id"]

    status, _, content = await _request(
        app,
        "GET",
        f"/v1/runs/{started['run_id']}/events",
        query="after=0&limit=20",
    )
    assert status == 200
    events = json.loads(content)
    assert events["events"][0]["type"] == "run.started"
    assert all("worker_id" not in event["data"] for event in events["events"])

    status, _, content = await _request(
        app,
        "POST",
        f"/v1/runs/{started['run_id']}/commands",
        body=PauseCommand(command_id="pause-api").to_data(),
    )
    assert status == 202
    assert json.loads(content)["run_status"] == "PAUSED"
    duplicate_status, _, _ = await _request(
        app,
        "POST",
        f"/v1/runs/{started['run_id']}/commands",
        body=PauseCommand(command_id="pause-api").to_data(),
    )
    conflict_status, _, _ = await _request(
        app,
        "POST",
        f"/v1/runs/{started['run_id']}/commands",
        body=PauseCommand(command_id="pause-again").to_data(),
    )
    assert duplicate_status == 200
    assert conflict_status == 409


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_json_and_sse_share_cursor_contract_and_reconnect(
    postgres_store: PostgresStore,
) -> None:
    catalog = WorkflowCatalog([GreetingWorkflow])
    app = create_userplane_app(postgres_store, catalog, tenant_resolver=_tenant)
    run = WorkflowClient(postgres_store).start(GreetingWorkflow(), "Ada", tenant_id="tenant-a")
    scheduler = WorkflowScheduler.resume(
        postgres_store, GreetingWorkflow(), run.run_id, tenant_id="tenant-a"
    )
    workflow = GreetingWorkflow()
    worker = TaskWorker(
        postgres_store,
        workflow_id=scheduler.plan.workflow_id,
        definition_digest=scheduler.plan.digest,
        modules=build_module_registry(workflow, scheduler.plan),
        worker_id="worker-a",
    )
    assert await worker.run_once() is not None
    WorkflowCoordinator(postgres_store, catalog).run_once()

    page = run.events(limit=2)
    status, headers, content = await _request(
        app,
        "GET",
        f"/v1/runs/{run.run_id}/events",
        headers={"accept": "text/event-stream", "last-event-id": str(page.next_cursor)},
    )

    assert status == 200
    assert headers["content-type"] == "text/event-stream"
    text = content.decode()
    assert f"id: {page.next_cursor}" not in text
    assert "event: task.started" in text
    assert "event: task.completed" in text
    assert "event: run.completed" in text


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_asgi_rejects_malformed_and_cross_tenant_requests(
    postgres_store: PostgresStore,
) -> None:
    catalog = WorkflowCatalog([GreetingWorkflow])
    app = create_userplane_app(postgres_store, catalog, tenant_resolver=_tenant)
    run = WorkflowClient(postgres_store).start(GreetingWorkflow(), "Ada", tenant_id="tenant-b")

    missing, _, _ = await _request(app, "GET", f"/v1/runs/{run.run_id}")
    malformed, _, content = await _request(
        app,
        "POST",
        f"/v1/runs/{run.run_id}/commands",
        body={"type": "unknown", "command_id": "bad"},
    )
    unknown, _, _ = await _request(app, "GET", "/not-a-route")
    wrong_method, _, _ = await _request(app, "GET", "/v1/workflows/greeting-api/runs")
    wrong_shape, _, _ = await _request(
        app,
        "POST",
        "/v1/workflows/greeting-api/runs",
        body={"input": "Ada", "tenant": "untrusted"},
    )
    non_object, _, _ = await _request(
        app,
        "POST",
        "/v1/workflows/greeting-api/runs",
        body=["Ada"],
    )
    negative_cursor, _, _ = await _request(
        app,
        "GET",
        f"/v1/runs/{run.run_id}/events",
        query="after=-1",
    )

    assert missing == 404
    assert malformed == 400
    assert "unknown command type" in json.loads(content)["error"]
    assert unknown == 404
    assert wrong_method == 400
    assert wrong_shape == 400
    assert non_object == 400
    assert negative_cursor == 400


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_nonterminal_sse_emits_heartbeat_then_honors_disconnect(
    postgres_store: PostgresStore,
) -> None:
    app = create_userplane_app(
        postgres_store,
        WorkflowCatalog([GreetingWorkflow]),
        tenant_resolver=_tenant,
        poll_interval=0.001,
    )
    run = WorkflowClient(postgres_store).start(GreetingWorkflow(), "Ada", tenant_id="tenant-a")

    status, _, content = await _request(
        app,
        "GET",
        f"/v1/runs/{run.run_id}/events",
        headers={"accept": "text/event-stream"},
        query="after=999999",
    )

    assert status == 200
    assert content.count(b": keep-alive\n\n") >= 1


def test_quickstart_app_factory_has_no_import_time_database_or_network_access(
    tmp_path: Path,
) -> None:
    app = create_app("postgresql://unused", artifacts=tmp_path)

    assert isinstance(app, UserplaneASGI)
