# Your first verified plan

A planner that branches on its input, a registry of modules, and one call that
generates, checks, executes and proves.

The complete working version is
`examples/workflow_creation/generated_plan.py`, executed offline by the test
suite.

## 1. Declare modules

```python
from datetime import timedelta
from typing import Any

from maida.workflows import (
    Budget, ExecutionContext, Module, ModuleRegistry,
    PlanBoundary, PlanLimits, WorkflowRunner,
)

NODE_BUDGET = Budget(wall_time=timedelta(seconds=1), model_tokens=0,
                     tool_calls=0, cost_usd=0.0)


class Normalize(Module[str, str]):
    module_id = "demo.normalize"
    input_type = str
    output_type = str
    budget = NODE_BUDGET

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value.upper()


class Draft(Module[str, str]):
    module_id = "demo.draft"
    input_type = str
    output_type = str
    budget = NODE_BUDGET

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return f"draft:{value}"


class Audit(Module[tuple[str, str], str]):
    module_id = "demo.audit"
    input_type = tuple[str, str]
    output_type = str
    budget = NODE_BUDGET

    async def execute(self, value: tuple[str, str], ctx: ExecutionContext) -> str:
        return " | ".join(value)
```

## 2. Declare the boundary

```python
registry = ModuleRegistry(modules={
    "text.normalize": Normalize,
    "text.draft": Draft,
    "text.audit": Audit,
})

boundary = PlanBoundary(
    registry,
    PlanLimits(
        max_nodes=8,
        max_depth=5,
        max_fanout=3,
        budget=Budget(wall_time=timedelta(seconds=5), model_tokens=0,
                      tool_calls=2, cost_usd=0.0),
    ),
    region_id="request-plan",
    output_type=str,
)
```

Everything a planner is allowed to do is now fixed by trusted code.

## 3. Write a planner

A planner is an ordinary module whose output is plan data and whose
`plan_boundary` marks it as a planning boundary.

```python
class Planner(Module[str, dict[str, Any]]):
    module_id = "demo.planner"
    input_type = str
    output_type = dict[str, Any]
    plan_boundary = boundary

    async def execute(self, value: str, ctx: ExecutionContext) -> dict[str, Any]:
        return thorough_plan() if "thorough" in value else brief_plan()
```

**The output must depend on the input.** A planner returning a constant is not a
planner, and the example's test asserts that two different inputs produce two
different plan digests.

To drive this with a model instead, call it through `ModelBroker` inside
`execute` and return the decoded graph choices. The model chooses topology and
aliases; it can express nothing else.

## 4. Run it

```python
result = await WorkflowRunner(store).run_generated(Planner(), "brief request")
```

That one call:

1. runs the planner as a durable bootstrap task;
2. strictly decodes its output as untrusted graph choices;
3. resolves every node against the registry, recomputing identities, digests,
   schemas, execution requirements, grants and budgets;
4. evaluates core policy and refuses or approves;
5. adopts the approved plan as the run's root definition;
6. materializes all children in one transaction, or none;
7. executes them through durable workers;
8. verifies the executed structure against the approved plan before completing.

A refusal raises before step 5, and no child task exists.

## 5. Read the proof

```python
history = store.load_run_history(result.run_id, tenant_id="local")
```

The history carries `PLAN_APPROVED`, `PLAN_MATERIALIZED` and
`PLAN_EXECUTION_VERIFIED`, plus the accepted boundary for every module. This is
what makes the run checkable after the fact, not just before it.

## Next

- [Connectors and effects](connectors-and-effects.md) — reaching outside
- [Fan-out plans](fan-out-plans.md) — several nodes and what policy sees
