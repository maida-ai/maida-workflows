"""Deterministic generated-plan demo used by the optional Maida CLI path."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path
from typing import Any

from maida.policy import load_policy  # type: ignore[import-untyped]
from maida.schema_versions import (  # type: ignore[import-untyped]
    PLAN_SCHEMA_VERSION,
    POLICY_SCHEMA_VERSION,
    REPORT_SCHEMA_VERSION,
)

from ._canonical import schema_digest
from .authoring import ExecutionContext, Module
from .budget import Budget
from .dynamic import PlanBoundary, PlanFragmentIR, PlanLimits, PlanNode, PlanValidator
from .guardrail import PlanGuardrail
from .registry import ModuleRegistry

_DEMO_POLICY = Path(__file__).with_name("_plan_demo_policy.yaml")
_REQUEST = "prepare a release update"
_BRIEF_TOPOLOGY = "normalize -> draft"
_REVIEWED_TOPOLOGY = "normalize -> [draft, review] -> publish"
_NODE_BUDGET = Budget(
    wall_time=timedelta(seconds=1),
    model_tokens=0,
    tool_calls=0,
    cost_usd=0.0,
)


class _TextModule(Module[str, str]):
    input_type = str
    output_type = str
    budget = _NODE_BUDGET

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        raise AssertionError("the demo must refuse before generated modules execute")


class _Normalize(_TextModule):
    module_id = "demo.normalize"


class _Draft(_TextModule):
    module_id = "demo.draft"


class _Review(_TextModule):
    module_id = "demo.review"


class _Publish(Module[tuple[str, str], str]):
    module_id = "demo.publish"
    input_type = tuple[str, str]
    output_type = str
    budget = _NODE_BUDGET

    async def execute(self, value: tuple[str, str], ctx: ExecutionContext) -> str:
        raise AssertionError("the demo must refuse before generated modules execute")


_REGISTRY = ModuleRegistry(
    modules={
        "text.draft": _Draft,
        "text.normalize": _Normalize,
        "text.publish": _Publish,
        "text.review": _Review,
    }
)
_BOUNDARY = PlanBoundary(
    _REGISTRY,
    PlanLimits(
        max_nodes=6,
        max_depth=5,
        max_fanout=3,
        budget=Budget(
            wall_time=timedelta(seconds=6),
            model_tokens=0,
            tool_calls=0,
            cost_usd=0.0,
        ),
    ),
    region_id="maida-plan-demo",
    output_type=str,
)


class _Planner(Module[str, dict[str, Any]]):
    module_id = "demo.planner"
    input_type = str
    output_type = dict[str, Any]
    plan_boundary = _BOUNDARY

    async def execute(self, value: str, ctx: ExecutionContext) -> dict[str, Any]:
        if "release" not in value.lower():
            return PlanFragmentIR(
                "brief-update",
                (
                    PlanNode("normalize", "text.normalize", ("$input",)),
                    PlanNode("draft", "text.draft", ("normalize",)),
                ),
                ("draft",),
            ).to_dict()
        return PlanFragmentIR(
            "release-update",
            (
                PlanNode("normalize", "text.normalize", ("$input",)),
                PlanNode("draft", "text.draft", ("normalize",)),
                PlanNode("review", "text.review", ("normalize",)),
                PlanNode("publish", "text.publish", ("draft", "review")),
            ),
            ("publish",),
        ).to_dict()


async def _generate(request: str) -> PlanFragmentIR:
    planner = _Planner()
    payload = await planner.execute(
        request,
        ExecutionContext(
            run_id="maida-plan-demo",
            task_id="planner",
            step_instance_id="planner-1",
        ),
    )
    return PlanFragmentIR.from_dict(payload)


def run_plan_demo(
    policy_path: Path | None = None,
    *,
    request: str = _REQUEST,
) -> dict[str, object]:
    """Generate a canned plan and judge it with the real pre-execution guardrail."""
    policy = load_policy(policy_path or _DEMO_POLICY)
    fragment = asyncio.run(_generate(request))
    signature = PlanValidator(
        _BOUNDARY.registry,
        _BOUNDARY.limits,
        region_id=_BOUNDARY.region_id,
        region_grant=_BOUNDARY.region_grant,
    ).validate(
        fragment,
        region_input_schema_digest=schema_digest(str),
        expected_output_schema_digests=(schema_digest(_BOUNDARY.output_type),),
    )
    guardrail = PlanGuardrail(policy=policy)
    evidence = guardrail.evaluate(signature)
    return {
        "evidence": evidence,
        "execution_attempts": 0,
        "max_fanout": signature.max_fanout,
        "node_count": signature.node_count,
        "schemas": {
            "policy": POLICY_SCHEMA_VERSION,
            "report": REPORT_SCHEMA_VERSION,
            "plan": PLAN_SCHEMA_VERSION,
        },
        "topology": (
            _REVIEWED_TOPOLOGY if fragment.fragment_id == "release-update" else _BRIEF_TOPOLOGY
        ),
        "rendered": guardrail.format(evidence),
    }
