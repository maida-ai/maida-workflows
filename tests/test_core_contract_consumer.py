"""The workflows package consumes Maida's authoritative contract snapshot."""

from __future__ import annotations

import json
from pathlib import Path

from maida.schema_versions import (  # type: ignore[import-untyped]
    BASELINE_SCHEMA_VERSION,
    PLAN_SCHEMA_VERSION,
    POLICY_SCHEMA_VERSION,
    REPORT_SCHEMA_VERSION,
    TRACE_SCHEMA_VERSION,
)

CONTRACT = Path(__file__).parent / "contracts" / "current-main.json"


def test_vendored_core_contract_matches_the_installed_schema_streams() -> None:
    snapshot = json.loads(CONTRACT.read_text(encoding="utf-8"))

    assert snapshot["schemas"] == {
        "trace": TRACE_SCHEMA_VERSION,
        "baseline": BASELINE_SCHEMA_VERSION,
        "policy": POLICY_SCHEMA_VERSION,
        "report": REPORT_SCHEMA_VERSION,
        "plan": PLAN_SCHEMA_VERSION,
    }
    assert snapshot["install_requirement"].startswith("maida-ai>=")
