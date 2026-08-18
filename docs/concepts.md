# Concepts

Five types carry the whole model. Everything else is machinery around them.

## Module — the unit of identity

A typed unit of work with a **self-declared** `module_id`. Identity never comes
from where a module sits in a graph, which is what lets a plan generated five
minutes from now name a module you wrote today.

```python
class Normalize(Module[str, str]):
    module_id = "demo.normalize"
    input_type = str
    output_type = str
    budget = Budget(wall_time=timedelta(seconds=1), model_tokens=0,
                    tool_calls=0, cost_usd=0.0)

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value.upper()
```

A missing or blank `module_id` is a compile error, not a default. The design
test: can trusted code name, validate, budget and execute this module from a
plan that did not exist when the module was imported?

## ModuleRegistry — the trusted allowlist

Maps a public *alias* to a factory. It is the single source for both validation
metadata and exact executable resolution, and it is immutable after
construction — so a plan cannot be validated against one registry and run
against another.

```python
registry = ModuleRegistry(modules={
    "text.normalize": Normalize,
    "text.draft": Draft,
})
```

## PlanBoundary — what a planner may do

Trusted application code declaring the registry, structural and budget limits,
the region's output type, and the maximum grant any generated child may hold. It
is folded into the planner module's digest, so generated data can neither forge
nor widen it.

## PlanIR — the one plan representation

Canonical, content-addressed, serializable. Static `Workflow.build()` authoring
and generated planning both terminate here, so diff, replay, policy and proof
all operate on one type.

## The guardrail — refusal before insertion

Given a resolved plan, the guardrail evaluates core Maida policy and either
approves or refuses. It depends on nothing but the plan — no scheduler, no
database, no executor — which is why it can front someone else's runtime.

## What a planner may emit

Only this: a fragment id, ordered node keys, allowlisted aliases, dependencies,
and outputs.

Never: identities, digests, schemas, models, grants, budgets, credentials,
execution targets, queues, retry policy, or control regions. Those are resolved
from the registry and boundary and recomputed before anything runs. Unknown
aliases and extra fields fail closed.

Generated plans are plain DAGs. Static authoring supports `when` and `map_over`
control regions; generated data cannot express them — hiding a twelve-way
fan-out inside one node would make it invisible to `max_fanout`. Same
representation, different permitted subset by provenance.

## Durable events

A generated run leaves a trail:

| Event | Meaning |
| --- | --- |
| `PLAN_REJECTED` | Policy refused. Zero child tasks inserted. |
| `PLAN_APPROVED` | Policy approved; evidence recorded. |
| `PLAN_MATERIALIZED` | Children inserted in one transaction. |
| `PLAN_EXECUTION_VERIFIED` | Executed structure matched the approved plan. |
| `PLAN_EXECUTION_DIVERGENCE` | It did not. The run fails rather than completing. |
