"""Capture a native run and demonstrate full-stub and selective replay.

The demo changes one same-identity decision module, reports the structural
digest change, proves full-stub replay performs zero live calls, executes only
the changed boundary selectively, and verifies that replay never invokes the
sentinel production effect.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from maida.workflows import (
    ExecutionContext,
    Module,
    ReplayKey,
    RuntimeValue,
    Workflow,
    compile_workflow,
)
from maida.workflows.alignment import GraphAligner
from maida.workflows.artifacts import ArtifactStore, ValueCodec
from maida.workflows.fixture import ReplayFixtureExporter
from maida.workflows.persistence import PostgresStore
from maida.workflows.replay import ReplayCase, ReplayEngine, ReplayMode, TraceBridge
from maida.workflows.runtime import WorkflowRunner


@dataclass(frozen=True)
class Ticket:
    """Input ticket with stable identity and free-form text."""

    ticket_id: str
    text: str


@dataclass(frozen=True)
class Draft:
    """Normalized ticket representation produced by preparation."""

    ticket_id: str
    normalized: str


@dataclass(frozen=True)
class Decision:
    """Queue-selection result for one ticket."""

    ticket_id: str
    queue: str


class Prepare(Module[Ticket, Draft]):
    """Normalize ticket text while counting live handler calls."""

    input_type = Ticket
    output_type = Draft

    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, value: Ticket, ctx: ExecutionContext) -> Draft:
        self.calls += 1
        return Draft(value.ticket_id, value.text.strip().lower())


class DecideV1(Module[Draft, Decision]):
    """Historical decision behavior that always selects the standard queue."""

    input_type = Draft
    output_type = Decision

    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, value: Draft, ctx: ExecutionContext) -> Decision:
        self.calls += 1
        return Decision(value.ticket_id, "standard")


class DecideV2(DecideV1):
    """Current decision behavior that routes refund requests to specialists."""

    async def execute(self, value: Draft, ctx: ExecutionContext) -> Decision:
        self.calls += 1
        queue = "specialist" if "refund" in value.normalized else "standard"
        return Decision(value.ticket_id, queue)


class ProductionNotification(Module[Decision, Decision]):
    """Effect-classified sentinel that records every real invocation."""

    input_type = Decision
    output_type = Decision
    effectful = True

    def __init__(self, sentinel: list[str]) -> None:
        self.sentinel = sentinel

    async def execute(self, value: Decision, ctx: ExecutionContext) -> Decision:
        self.sentinel.append(value.ticket_id)
        return value


class TicketWorkflow(Workflow[Ticket, Decision]):
    """Prepare, route, and notify for one ticket using stable logical steps."""

    workflow_id = "native-replay-demo"
    input_type = Ticket
    output_type = Decision

    def __init__(self, *, changed: bool, effect_sentinel: list[str]) -> None:
        self.prepare = Prepare()
        self.decide = DecideV2() if changed else DecideV1()
        self.notify = ProductionNotification(effect_sentinel)

    def build(self, value: RuntimeValue[Ticket]) -> RuntimeValue[Decision]:
        prepared = self.prepare.at("prepare")(value)
        decided = self.decide.at("decide")(prepared)
        return self.notify.at("notify")(decided)


async def run_demo(
    dsn: str,
    artifact_root: Path,
    fixture_output: Path,
    *,
    trace_bridge: TraceBridge | None = None,
) -> dict[str, Any]:
    """Run the complete deterministic native replay demonstration.

    Parameters
    ----------
    dsn
        PostgreSQL connection string for local durable history.
    artifact_root
        Private directory for source-run content-addressed values.
    fixture_output
        New directory where the canonical fixture bundle is exported.
    trace_bridge
        Optional trace adapter, primarily for deterministic tests.

    Returns
    -------
    dict
        JSON-compatible evidence covering diff, replay status, live-call counts,
        output comparison, and production-effect sentinels.
    """
    store = PostgresStore(dsn, ValueCodec(ArtifactStore(artifact_root), inline_limit=64))
    store.upgrade()
    production_effects: list[str] = []
    historical = TicketWorkflow(changed=False, effect_sentinel=production_effects)
    captured = await WorkflowRunner(store).run(
        historical,
        Ticket("ticket-42", "Please REFUND this charge"),
    )
    history = store.load_run_history(captured.run_id, tenant_id="local")
    fixture = ReplayFixtureExporter(store.values).export(history, fixture_output)

    replay_effects: list[str] = []
    current = TicketWorkflow(changed=True, effect_sentinel=replay_effects)
    current_plan = compile_workflow(current)
    diff = GraphAligner().align(fixture.workflow_ir, current_plan).diff
    engine = ReplayEngine(trace_bridge=trace_bridge) if trace_bridge else ReplayEngine()
    full_stub = await engine.replay(current, ReplayCase(fixture, ReplayMode.FULL_STUB))
    full_stub_live_calls = current.prepare.calls + current.decide.calls
    selective = await engine.replay(
        current,
        ReplayCase(
            fixture,
            ReplayMode.SELECTIVE,
            (ReplayKey("native-replay-demo.decide", "decide"),),
        ),
    )
    return {
        "source_run_id": captured.run_id,
        "fixture_digest": fixture.digest,
        "diff_kinds": [change.kind.value for change in diff.changes],
        "full_stub_status": full_stub.status.value,
        "full_stub_live_calls": full_stub_live_calls,
        "selective_status": selective.status.value,
        "selective_prepare_calls": current.prepare.calls,
        "selective_decide_calls": current.decide.calls,
        "historical_output": asdict(selective.output),
        "new_output_changed": selective.comparisons[0].output_changed,
        "source_effect_calls": len(production_effects),
        "replay_effect_calls": len(replay_effects),
    }


def main() -> None:
    """Parse CLI arguments, run the demo, and print deterministic JSON evidence."""
    parser = argparse.ArgumentParser(description="Run the deterministic native replay demo.")
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    arguments = parser.parse_args()
    print(
        json.dumps(
            asyncio.run(run_demo(arguments.dsn, arguments.artifacts, arguments.fixture)),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
