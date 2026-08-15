"""Command-line entry points for compiling, running, exporting, and replaying.

The supported command surface is exposed through the ``maida-workflows``
executable. Python applications should normally import the typed APIs from
``maida.workflows`` instead of calling command functions directly.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import os
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import typer

from ._canonical import canonical_data
from .alignment import GraphAligner
from .artifacts import ArtifactStore, ValueCodec
from .authoring import Workflow
from .baseline import create_baseline, write_baseline
from .fixture import (
    CanonicalBundleImporter,
    NativeRunFixtureImporter,
    ReplayFixture,
    ReplayFixtureError,
    ReplayFixtureExporter,
)
from .ir import _compile_workflow_graph, compile_workflow
from .models import ExecutorCapabilities
from .persistence import PersistenceError, PostgresStore
from .replay import (
    ReplayCase,
    ReplayEngine,
    ReplayMode,
    ReplaySelectorError,
    assert_replay_worker_environment,
    build_module_registry,
    resolve_selectors,
)
from .runtime import TaskWorker, WorkflowRunner, WorkflowScheduler
from .verification import VerificationPolicy, VerificationSuite, verify_workflow

app = typer.Typer(
    no_args_is_help=True,
    help="Compile, run, export, and safely replay durable Maida Workflows.",
)
db_app = typer.Typer(no_args_is_help=True, help="Manage the workflow database schema.")
trace_app = typer.Typer(no_args_is_help=True, help="Export native workflow run history.")
app.add_typer(db_app, name="db")
app.add_typer(trace_app, name="trace")

DSN_OPTION = typer.Option(None, "--dsn", envvar="MAIDA_WORKFLOWS_DSN", help="PostgreSQL DSN.")
ARTIFACT_OPTION = typer.Option(
    Path(".maida-workflows/artifacts"),
    "--artifacts",
    help="Private content-addressed artifact directory.",
)


def _load_workflow(reference: str) -> Workflow[Any, Any]:
    module_name, separator, object_name = reference.partition(":")
    if not separator or not module_name or not object_name:
        raise typer.BadParameter("workflow must use module:object")
    try:
        candidate = getattr(importlib.import_module(module_name), object_name)
    except (ImportError, AttributeError) as exc:
        raise typer.BadParameter(f"cannot import workflow {reference!r}: {exc}") from exc
    if isinstance(candidate, type):
        candidate = candidate()
    if not isinstance(candidate, Workflow):
        raise typer.BadParameter(f"{reference!r} does not resolve to a Workflow")
    return candidate


def _store(dsn: str | None, artifacts: Path) -> PostgresStore:
    if not dsn:
        raise typer.BadParameter("a PostgreSQL DSN is required via --dsn or MAIDA_WORKFLOWS_DSN")
    return PostgresStore(dsn, ValueCodec(ArtifactStore(artifacts)))


def _load_fixture_source(
    source: str,
    *,
    dsn: str | None,
    artifacts: Path,
    tenant_id: str,
) -> ReplayFixture:
    if Path(source).exists():
        return CanonicalBundleImporter().import_source(source)
    return NativeRunFixtureImporter(_store(dsn, artifacts), tenant_id=tenant_id).import_source(
        source
    )


def _echo_data(value: Any) -> None:
    if is_dataclass(value) and not isinstance(value, type):
        value = asdict(value)
    typer.echo(json.dumps(canonical_data(value), indent=2, ensure_ascii=False, sort_keys=True))


@app.command("compile")
def compile_command(
    workflow: str = typer.Option(..., "--workflow", help="Workflow as module:object."),
    output: Path | None = typer.Option(None, "--output", help="Write canonical IR to this file."),
) -> None:
    plan = compile_workflow(_load_workflow(workflow))
    content = plan.canonical_json()
    if output is None:
        typer.echo(content)
    else:
        output.write_text(content + "\n")
        typer.echo(plan.digest)


@db_app.command("upgrade")
def db_upgrade(
    dsn: str | None = DSN_OPTION,
    artifacts: Path = ARTIFACT_OPTION,
) -> None:
    _store(dsn, artifacts).upgrade()
    typer.echo("Database schema is current.")


@app.command("run")
def run_command(
    workflow: str = typer.Option(..., "--workflow"),
    input_json: str = typer.Option(..., "--input", help="Canonical JSON workflow input."),
    tenant_id: str = typer.Option("local", "--tenant"),
    dsn: str | None = DSN_OPTION,
    artifacts: Path = ARTIFACT_OPTION,
) -> None:
    selected = _load_workflow(workflow)
    result = asyncio.run(
        WorkflowRunner(_store(dsn, artifacts)).run(
            selected,
            json.loads(input_json),
            tenant_id=tenant_id,
        )
    )
    _echo_data(result)


@app.command("submit")
def submit_command(
    workflow: str = typer.Option(..., "--workflow"),
    input_json: str = typer.Option(..., "--input", help="Canonical JSON workflow input."),
    tenant_id: str = typer.Option("local", "--tenant"),
    dsn: str | None = DSN_OPTION,
    artifacts: Path = ARTIFACT_OPTION,
) -> None:
    selected = _load_workflow(workflow)
    scheduler = WorkflowScheduler.submit(
        _store(dsn, artifacts),
        selected,
        json.loads(input_json),
        tenant_id=tenant_id,
    )
    progress = scheduler.advance()
    _echo_data(
        {
            "run_id": scheduler.run_id,
            "definition_digest": scheduler.plan.digest,
            "status": progress.status,
            "ready_tasks": progress.ready_tasks,
        }
    )


@app.command("schedule")
def schedule_command(
    run_id: str,
    workflow: str = typer.Option(..., "--workflow"),
    tenant_id: str = typer.Option("local", "--tenant"),
    dsn: str | None = DSN_OPTION,
    artifacts: Path = ARTIFACT_OPTION,
) -> None:
    scheduler = WorkflowScheduler.resume(
        _store(dsn, artifacts),
        _load_workflow(workflow),
        run_id,
        tenant_id=tenant_id,
    )
    _echo_data(scheduler.advance())


@app.command("worker")
def worker_command(
    workflow: str = typer.Option(..., "--workflow"),
    worker_id: str = typer.Option("workflow-worker", "--worker-id"),
    isolation: list[str] | None = typer.Option(None, "--isolation"),
    image: list[str] | None = typer.Option(None, "--image"),
    cpu: int | None = typer.Option(None, "--cpu"),
    memory: str | None = typer.Option(None, "--memory"),
    capability: list[str] | None = typer.Option(None, "--capability"),
    dsn: str | None = DSN_OPTION,
    artifacts: Path = ARTIFACT_OPTION,
) -> None:
    selected = _load_workflow(workflow)
    compiled = _compile_workflow_graph(selected)
    plan = compiled.plan
    try:
        capabilities = ExecutorCapabilities(
            isolations=frozenset(isolation or ["process"]),
            images=frozenset(image or []),
            cpu=cpu,
            memory=memory,
            capabilities=frozenset(capability or []),
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    worker = TaskWorker(
        _store(dsn, artifacts),
        workflow_id=plan.workflow_id,
        definition_digest=plan.digest,
        modules=build_module_registry(selected, plan, output=compiled.output),
        worker_id=worker_id,
        capabilities=capabilities,
    )
    boundary = asyncio.run(worker.run_once())
    _echo_data(
        {"claimed": boundary is not None, "boundary": boundary.to_data() if boundary else None}
    )


@trace_app.command("export")
def trace_export(
    run_id: str,
    output: Path = typer.Option(..., "--output"),
    tenant_id: str = typer.Option("local", "--tenant"),
    dsn: str | None = DSN_OPTION,
    artifacts: Path = ARTIFACT_OPTION,
) -> None:
    store = _store(dsn, artifacts)
    try:
        history = store.load_run_history(run_id, tenant_id=tenant_id)
        fixture = ReplayFixtureExporter(store.values).export(history, output)
    except (PersistenceError, ReplayFixtureError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2) from exc
    _echo_data({"fixture_digest": fixture.digest, "output": str(output)})


@app.command("diff")
def diff_command(
    source: str,
    workflow: str = typer.Option(..., "--workflow"),
    tenant_id: str = typer.Option("local", "--tenant"),
    dsn: str | None = DSN_OPTION,
    artifacts: Path = ARTIFACT_OPTION,
) -> None:
    fixture = _load_fixture_source(
        source,
        dsn=dsn,
        artifacts=artifacts,
        tenant_id=tenant_id,
    )
    current = compile_workflow(_load_workflow(workflow))
    _echo_data(GraphAligner().align(fixture.workflow_ir, current).diff)


@app.command("replay")
def replay_command(
    source: str,
    workflow: str = typer.Option(..., "--workflow"),
    mode: ReplayMode = typer.Option(ReplayMode.FULL_STUB, "--mode"),
    live: list[str] | None = typer.Option(None, "--live", help="module:ID or step:ID"),
    tenant_id: str = typer.Option("local", "--tenant"),
    dsn: str | None = DSN_OPTION,
    artifacts: Path = ARTIFACT_OPTION,
) -> None:
    selected = _load_workflow(workflow)
    fixture = _load_fixture_source(
        source,
        dsn=dsn,
        artifacts=artifacts,
        tenant_id=tenant_id,
    )
    selectors = live or []
    if selectors and mode is ReplayMode.FULL_STUB:
        mode = ReplayMode.SELECTIVE
    try:
        keys = resolve_selectors(compile_workflow(selected), selectors)
        assert_replay_worker_environment()
        result = asyncio.run(ReplayEngine().replay(selected, ReplayCase(fixture, mode, keys)))
    except ReplaySelectorError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _echo_data(result)
    if result.blocking:
        raise typer.Exit(1)


@app.command("verify")
def verify_command(
    source: str,
    workflow: str = typer.Option(..., "--workflow"),
    live: list[str] | None = typer.Option(None, "--live"),
    block_divergence: bool = typer.Option(False, "--block-divergence"),
    block_changes: bool = typer.Option(False, "--block-changes"),
    tenant_id: str = typer.Option("local", "--tenant"),
    dsn: str | None = DSN_OPTION,
    artifacts: Path = ARTIFACT_OPTION,
) -> None:
    selected = _load_workflow(workflow)
    fixture = _load_fixture_source(
        source,
        dsn=dsn,
        artifacts=artifacts,
        tenant_id=tenant_id,
    )
    keys = resolve_selectors(compile_workflow(selected), live or [])
    cases = [ReplayCase(fixture, ReplayMode.FULL_STUB)]
    if keys:
        cases.append(ReplayCase(fixture, ReplayMode.SELECTIVE, keys))
    result = asyncio.run(
        verify_workflow(
            selected,
            VerificationSuite(tuple(cases)),
            policy=VerificationPolicy(block_divergence, block_changes),
        )
    )
    _echo_data(result)
    if result.verdict.value == "FAIL":
        raise typer.Exit(1)


def _baseline_command(
    source: str,
    output: Path,
    tenant_id: str,
    dsn: str | None,
    artifacts: Path,
    accepted: bool,
) -> None:
    fixture = _load_fixture_source(
        source,
        dsn=dsn,
        artifacts=artifacts,
        tenant_id=tenant_id,
    )
    baseline = create_baseline(
        [fixture],
        provenance={"accepted": accepted, "actor": os.environ.get("USER", "unknown")},
    )
    write_baseline(baseline, output)
    _echo_data({"baseline_digest": baseline.digest, "output": str(output)})


@app.command("baseline")
def baseline_command(
    source: str,
    output: Path = typer.Option(..., "--output"),
    tenant_id: str = typer.Option("local", "--tenant"),
    dsn: str | None = DSN_OPTION,
    artifacts: Path = ARTIFACT_OPTION,
) -> None:
    _baseline_command(source, output, tenant_id, dsn, artifacts, False)


@app.command("accept")
def accept_command(
    source: str,
    output: Path = typer.Option(..., "--output"),
    tenant_id: str = typer.Option("local", "--tenant"),
    dsn: str | None = DSN_OPTION,
    artifacts: Path = ARTIFACT_OPTION,
) -> None:
    _baseline_command(source, output, tenant_id, dsn, artifacts, True)


def main() -> None:
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
