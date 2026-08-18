"""Generate, verify, and execute an input-dependent plan with no network access.

The planner emits only node keys, allowlisted aliases, dependencies, and one
output. A trusted ``PlanBoundary`` supplies every module identity, schema,
execution requirement, grant, and budget before ``WorkflowRunner`` inserts any
child task. The local adapter simulates reads and effects deterministically.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from maida.assertions import AssertionPolicy  # type: ignore[import-untyped]

from maida.workflows import (
    Budget,
    Capability,
    CapabilityGrant,
    Connector,
    ConnectorRegistry,
    Effect,
    EffectSpec,
    ExecutionContext,
    Idempotency,
    Module,
    ModuleRegistry,
    PlanBoundary,
    PlanLimits,
    RunResult,
    WorkflowRunner,
)
from maida.workflows.persistence import PostgresStore

NODE_BUDGET = Budget(
    wall_time=timedelta(seconds=1),
    model_tokens=0,
    tool_calls=0,
    cost_usd=0.0,
)
TOOL_BUDGET = Budget(
    wall_time=timedelta(seconds=1),
    model_tokens=0,
    tool_calls=1,
    cost_usd=0.0,
)
CONTEXT = Capability(
    "records.context.read",
    "demo-records",
    "context",
    str,
    str,
    connector_version="1",
)
DELIVER = EffectSpec(
    "messages.deliver",
    "demo-records",
    "deliver",
    str,
    str,
    connector_version="1",
    idempotency=Idempotency.REQUIRED,
)


class _Normalize(Module[str, str]):
    module_id = "demo.normalize"
    input_type = str
    output_type = str
    budget = NODE_BUDGET

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value.upper()


class _Draft(Module[str, str]):
    module_id = "demo.draft"
    input_type = str
    output_type = str
    budget = NODE_BUDGET

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return f"draft:{value}"


class _Audit(Module[tuple[str, str], str]):
    module_id = "demo.audit"
    input_type = tuple[str, str]
    output_type = str
    budget = NODE_BUDGET

    async def execute(self, value: tuple[str, str], ctx: ExecutionContext) -> str:
        return " | ".join(value)


def _context_module() -> Module[Any, Any]:
    module = Connector(CONTEXT)
    module.budget = TOOL_BUDGET
    return module


def _deliver_module() -> Module[Any, Any]:
    module = Effect(DELIVER)
    module.budget = TOOL_BUDGET
    return module


registry = ModuleRegistry(
    modules={
        "messages.deliver": _deliver_module,
        "records.context": _context_module,
        "text.audit": _Audit,
        "text.draft": _Draft,
        "text.normalize": _Normalize,
    }
)
boundary = PlanBoundary(
    registry,
    PlanLimits(
        max_nodes=8,
        max_depth=5,
        max_fanout=3,
        budget=Budget(
            wall_time=timedelta(seconds=5),
            model_tokens=0,
            tool_calls=2,
            cost_usd=0.0,
        ),
    ),
    region_id="request-plan",
    output_type=str,
    region_grant=CapabilityGrant(
        capabilities=(CONTEXT.name,),
        effects=(DELIVER.name,),
    ),
)


def _plan(
    fragment_id: str,
    nodes: list[tuple[str, str, list[str]]],
    outputs: list[str],
) -> dict[str, Any]:
    return {
        "fragment_id": fragment_id,
        "nodes": [
            {"dependencies": dependencies, "key": key, "module_alias": alias}
            for key, alias, dependencies in sorted(nodes)
        ],
        "outputs": outputs,
        "version": "0.2.0",
    }


def _brief_plan() -> dict[str, Any]:
    return _plan(
        "brief-plan",
        [
            ("normalize", "text.normalize", ["$input"]),
            ("draft", "text.draft", ["normalize"]),
        ],
        ["draft"],
    )


def _thorough_plan() -> dict[str, Any]:
    return _plan(
        "thorough-plan",
        [
            ("normalize", "text.normalize", ["$input"]),
            ("context", "records.context", ["$input"]),
            ("draft", "text.draft", ["normalize"]),
            ("audit", "text.audit", ["draft", "context"]),
            ("deliver", "messages.deliver", ["audit"]),
        ],
        ["deliver"],
    )


class _Planner(Module[str, dict[str, Any]]):
    module_id = "demo.planner"
    input_type = str
    output_type = dict[str, Any]
    plan_boundary = boundary

    async def execute(self, value: str, ctx: ExecutionContext) -> dict[str, Any]:
        selected = _thorough_plan() if "thorough" in value else _brief_plan()
        return selected


class _LocalAdapter:
    connector = "demo-records"
    connector_version = "1"
    operations = frozenset({"context"})
    effect_operations = frozenset({"deliver"})
    idempotent_effects = frozenset({"deliver"})

    async def read(self, operation: str, request: Any) -> Any:
        return f"context:{request}"

    async def effect(self, operation: str, request: Any, *, idempotency_key: str) -> Any:
        return f"delivered:{request}"


planner = _Planner()
connectors = ConnectorRegistry((_LocalAdapter(),))
BRIEF_INPUT = "brief request"
BRIEF_EXPECTED_OUTPUT = "draft:BRIEF REQUEST"
EXAMPLE_INPUT = "thorough request"
EXPECTED_OUTPUT = "delivered:draft:THOROUGH REQUEST | context:thorough request"


async def run_example(
    store: PostgresStore,
    value: str = EXAMPLE_INPUT,
    *,
    policy: AssertionPolicy | None = None,
) -> RunResult:
    """Generate a plan and gate it under an optional core policy before execution."""
    return await WorkflowRunner(store, connectors=connectors).run_generated(
        planner,
        value,
        policy=policy,
    )
