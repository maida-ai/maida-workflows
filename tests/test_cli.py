from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from examples.adversarial_workflows import AdversarialBranchWorkflow
from maida.workflows import compile_workflow
from maida.workflows._canonical import schema_digest
from maida.workflows.cli import app
from maida.workflows.persistence import PostgresStore
from maida.workflows.runtime import WorkflowRunner

runner = CliRunner()


def test_cli_help_and_compile_surface() -> None:
    help_result = runner.invoke(app, ["--help"])
    assert help_result.exit_code == 0
    for command in (
        "compile",
        "run",
        "worker",
        "db",
        "trace",
        "baseline",
        "diff",
        "replay",
        "verify",
        "accept",
    ):
        assert command in help_result.stdout

    compiled = runner.invoke(
        app,
        [
            "compile",
            "--workflow",
            "examples.adversarial_workflows:AdversarialMapWorkflow",
        ],
    )
    assert compiled.exit_code == 0
    assert json.loads(compiled.stdout)["version"] == "0.1.0"


def test_cli_rejects_non_workflow_objects() -> None:
    result = runner.invoke(app, ["compile", "--workflow", "json:loads"])
    assert result.exit_code == 2
    assert "does not resolve to a Workflow" in result.output
    malformed = runner.invoke(app, ["compile", "--workflow", "not-a-reference"])
    assert malformed.exit_code == 2
    missing = runner.invoke(app, ["compile", "--workflow", "missing_module:workflow"])
    assert missing.exit_code == 2
    no_dsn = runner.invoke(app, ["db", "upgrade"])
    assert no_dsn.exit_code == 2


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_cli_exports_replays_diffs_and_baselines_native_history(
    postgres_store: PostgresStore, tmp_path: Path
) -> None:
    workflow_input: dict[str, object] = {"escalated": True}
    result = await WorkflowRunner(postgres_store).run(
        AdversarialBranchWorkflow(),
        workflow_input,
    )
    bundle = tmp_path / "cli-fixture"
    common = [
        "--dsn",
        postgres_store.dsn,
        "--artifacts",
        str(postgres_store.values.artifacts.root),
    ]
    upgraded = runner.invoke(app, ["db", "upgrade", *common])
    assert upgraded.exit_code == 0

    live_run = await asyncio.to_thread(
        runner.invoke,
        app,
        [
            "run",
            "--workflow",
            "examples.adversarial_workflows:AdversarialBranchWorkflow",
            "--input",
            '{"escalated":false}',
            *common,
        ],
    )
    assert live_run.exit_code == 0, live_run.output
    assert '"run_id"' in live_run.stdout
    exported = runner.invoke(
        app,
        [
            "trace",
            "export",
            result.run_id,
            "--output",
            str(bundle),
            *common,
        ],
    )
    assert exported.exit_code == 0, exported.output
    assert (bundle / "manifest.json").is_file()

    replayed = await asyncio.to_thread(
        runner.invoke,
        app,
        [
            "replay",
            str(bundle),
            "--workflow",
            "examples.adversarial_workflows:AdversarialBranchWorkflow",
        ],
    )
    assert replayed.exit_code == 0, replayed.output
    assert '"status": "PASS"' in replayed.stdout

    diffed = runner.invoke(
        app,
        [
            "diff",
            str(bundle),
            "--workflow",
            "examples.adversarial_workflows:AdversarialBranchWorkflow",
        ],
    )
    assert diffed.exit_code == 0, diffed.output
    assert '"changes": []' in diffed.stdout

    baseline = tmp_path / "baseline.json"
    accepted = runner.invoke(
        app,
        ["accept", str(bundle), "--output", str(baseline)],
    )
    assert accepted.exit_code == 0, accepted.output
    assert baseline.is_file()

    baseline_output = tmp_path / "baseline-unaccepted.json"
    baselined = runner.invoke(
        app,
        ["baseline", str(bundle), "--output", str(baseline_output)],
    )
    assert baselined.exit_code == 0

    verified = await asyncio.to_thread(
        runner.invoke,
        app,
        [
            "verify",
            str(bundle),
            "--workflow",
            "examples.adversarial_workflows:AdversarialBranchWorkflow",
        ],
    )
    assert verified.exit_code == 0, verified.output
    assert '"verdict": "PASS"' in verified.stdout

    missing_run = runner.invoke(
        app,
        [
            "trace",
            "export",
            "00000000-0000-0000-0000-000000000000",
            "--output",
            str(tmp_path / "missing"),
            *common,
        ],
    )
    assert missing_run.exit_code == 2

    workflow = AdversarialBranchWorkflow()
    plan = compile_workflow(workflow)
    root = postgres_store.values.encode(
        {"escalated": True}, schema_digest=schema_digest(dict[str, object])
    )
    pending_run = postgres_store.create_run(plan, tenant_id="local", root_input=root)
    urgent = next(step for step in plan.executable_steps if step.logical_step == "route-urgent")
    postgres_store.enqueue_task(
        pending_run.run_id,
        urgent,
        step_instance_id="cli-worker",
        input_value=root,
    )
    worked = await asyncio.to_thread(
        runner.invoke,
        app,
        [
            "worker",
            "--workflow",
            "examples.adversarial_workflows:AdversarialBranchWorkflow",
            *common,
        ],
    )
    assert worked.exit_code == 0, worked.output
    assert '"claimed": true' in worked.stdout
