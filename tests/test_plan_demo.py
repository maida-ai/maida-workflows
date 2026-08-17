"""Offline product demo for pre-execution generated-plan refusal."""

from __future__ import annotations

import socket
from pathlib import Path
from typing import NoReturn

import pytest
from maida.plan_contract import PlanEvidence  # type: ignore[import-untyped]
from maida.schema_versions import (  # type: ignore[import-untyped]
    PLAN_SCHEMA_VERSION,
    POLICY_SCHEMA_VERSION,
    REPORT_SCHEMA_VERSION,
)

from maida.workflows.demo import run_plan_demo


def test_plan_demo_generates_and_refuses_before_child_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_network(*args: object, **kwargs: object) -> NoReturn:
        raise AssertionError("generated-plan demo attempted a network call")

    monkeypatch.setattr(socket, "create_connection", fail_network)

    result = run_plan_demo()
    evidence = result["evidence"]

    assert isinstance(evidence, PlanEvidence)
    assert evidence.valid is False
    assert evidence.issues[0].code == "PLAN_FANOUT_EXCEEDED"
    assert result["node_count"] == 4
    assert result["max_fanout"] == 2
    assert result["execution_attempts"] == 0
    assert result["schemas"] == {
        "policy": POLICY_SCHEMA_VERSION,
        "report": REPORT_SCHEMA_VERSION,
        "plan": PLAN_SCHEMA_VERSION,
    }
    assert str(result["rendered"]).startswith("PLAN REFUSED: PLAN_FANOUT_EXCEEDED")
    assert "policy allows at most 1" in str(result["rendered"])


def test_plan_demo_accepts_an_explicit_core_policy_file(tmp_path: Path) -> None:
    policy = tmp_path / "policy.yaml"
    policy.write_text(
        "version: 2.1\nmetrics:\n  plan_fanout: {kind: measured, direction: upper, limit: 2}\n",
        encoding="utf-8",
    )

    result = run_plan_demo(policy)
    evidence = result["evidence"]

    assert isinstance(evidence, PlanEvidence)
    assert evidence.valid is True
    assert str(result["rendered"]).startswith("PLAN APPROVED:")


def test_plan_demo_planner_selects_a_smaller_graph_from_runtime_input() -> None:
    result = run_plan_demo(request="prepare a brief note")
    evidence = result["evidence"]

    assert isinstance(evidence, PlanEvidence)
    assert evidence.valid is True
    assert result["node_count"] == 2
    assert result["max_fanout"] == 1
    assert result["topology"] == "normalize -> draft"
