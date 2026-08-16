"""Return an allowlisted generated DAG for durable child materialization.

The planner returns graph choices only: stable node keys, allowlist aliases,
dependencies, and outputs. Trusted code supplies module digests, schemas,
execution environments, grants, and budgets. The control plane validates and
materializes the entire graph after the planner boundary commits; the planner
worker never invokes or manages child workers.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from maida.workflows import (
    Budget,
    CapabilityGrant,
    ExecutionContext,
    Module,
    ModuleRegistry,
    PlanFragmentIR,
    PlanLimits,
    PlanNode,
    PlanValidator,
    RuntimeValue,
    Workflow,
)
from maida.workflows._canonical import schema_digest

NODE_BUDGET = Budget(
    wall_time=timedelta(seconds=1),
    model_tokens=0,
    tool_calls=0,
    cost_usd=0.0,
)

fragment = PlanFragmentIR(
    fragment_id="generated-math",
    nodes=(
        PlanNode("increment", "math.increment", ("$input",)),
        PlanNode("double", "math.double", ("$input",)),
        PlanNode("join", "math.join", ("increment", "double")),
    ),
    outputs=("join",),
)


class _Planner(Module[int, dict[str, Any]]):
    input_type = int
    output_type = dict[str, Any]

    def __init__(self, plan: PlanFragmentIR) -> None:
        self.plan = plan.to_dict()

    async def execute(self, value: int, ctx: ExecutionContext) -> dict[str, Any]:
        del value, ctx
        return self.plan


class _Offset(Module[int, int]):
    input_type = int
    output_type = int
    budget = NODE_BUDGET

    def __init__(self, module_id: str, amount: int) -> None:
        self.module_id = module_id
        self.amount = amount

    async def execute(self, value: int, ctx: ExecutionContext) -> int:
        return value + self.amount


class _Join(Module[tuple[int, int], int]):
    module_id = "math.join"
    input_type = tuple[int, int]
    output_type = int
    budget = NODE_BUDGET

    async def execute(self, value: tuple[int, int], ctx: ExecutionContext) -> int:
        return value[0] + value[1]


class GeneratedMath(Workflow[int, dict[str, Any]]):
    """Commit a small generated fan-out/fan-in plan as typed data."""

    workflow_id = "onboarding-generated"
    input_type = int
    output_type = dict[str, Any]
    planner = _Planner(fragment)

    def build(self, value: RuntimeValue[int]) -> RuntimeValue[dict[str, Any]]:
        """Construct the durable planner boundary."""
        return self.planner(value)


increment = _Offset("math.increment", 1)
double = _Offset("math.double", 2)
join = _Join()
registry = ModuleRegistry(
    modules={
        "math.increment": lambda: increment,
        "math.double": lambda: double,
        "math.join": lambda: join,
    }
)

validator = PlanValidator(
    registry,
    PlanLimits(
        max_nodes=8,
        max_depth=4,
        max_fanout=4,
        budget=Budget(
            wall_time=timedelta(seconds=3),
            model_tokens=0,
            tool_calls=0,
            cost_usd=0.0,
        ),
    ),
    region_id="math-region",
    region_grant=CapabilityGrant(),
)

workflow = GeneratedMath()
EXAMPLE_INPUT = 3
EXPECTED_OUTPUT = fragment.to_dict()


def validate_fragment() -> None:
    """Authenticate the generated choices against current trusted contracts."""
    validator.validate(
        fragment,
        region_input_schema_digest=schema_digest(int),
        expected_output_schema_digests=(schema_digest(int),),
    )
