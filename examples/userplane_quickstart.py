"""Mount a durable workflow as an application backend with JSON and SSE.

The module is deterministic and performs no network or database access during
import. Call :func:`create_app` with your PostgreSQL DSN, then mount the returned
ASGI application in the server your application already uses. Run schedulers
and workers as separate services so the web process never executes modules.
"""

from __future__ import annotations

from pathlib import Path

from maida.workflows import (
    ExecutionContext,
    Module,
    RuntimeValue,
    Workflow,
    WorkflowCatalog,
    create_userplane_app,
)
from maida.workflows.artifacts import ArtifactStore, ValueCodec
from maida.workflows.asgi import UserplaneASGI
from maida.workflows.persistence import PostgresStore


class Greeting(Module[str, str]):
    """Create a deterministic greeting for the supplied display name."""

    module_id = "demo.greeting"
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        """Return a greeting without model, tool, or network calls."""
        GreetingWorkflow.calls += 1
        return f"Hello, {value}!"


class GreetingWorkflow(Workflow[str, str]):
    """Single-step workflow used by the application-backend quickstart."""

    workflow_id = "greeting-api"
    input_type = str
    output_type = str
    calls = 0
    greeting = Greeting()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        """Connect the root name to the deterministic greeting module."""
        return self.greeting(value)


def create_app(dsn: str, *, artifacts: Path = Path(".maida-workflows/artifacts")) -> UserplaneASGI:
    """Build the quickstart ASGI app without opening a database connection.

    Parameters
    ----------
    dsn
        PostgreSQL connection string used lazily when requests arrive.
    artifacts
        Private content-addressed artifact directory.

    Returns
    -------
    UserplaneASGI
        ASGI application exposing ``GreetingWorkflow`` at
        ``POST /v1/workflows/greeting-api/runs``.
    """
    store = PostgresStore(dsn, ValueCodec(ArtifactStore(artifacts)))
    return create_userplane_app(store, WorkflowCatalog([GreetingWorkflow]))
