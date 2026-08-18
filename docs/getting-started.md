# Getting started

## Installation

Requires Python 3.12 or 3.13.

**As a tool, with the product CLI:**

```bash
uv tool install --force --python 3.12 \
  --with "maida-workflows>=0.1.0" "maida-ai>=0.5.2.post1"
```

**As a dependency:**

```bash
uv add "maida-workflows>=0.1.0"
```

The `maida-ai>=0.5.2.post1` pin is exact on purpose. Version `0.5.2` exists on
PyPI but its `maida` command has no `--plan`, and under PEP 440 a `>0.5.2`
specifier would exclude the post-release that does.

Installs as the `maida.workflows` namespace subpackage: `from maida import workflows`.

## First refusal

One command. No database, no keys, no clone.

```bash
maida demo --plan
```

```text
── Step 1/2 · A simulated planner generates a runtime plan
   topology: normalize -> [draft, review] -> publish
   resolved: 4 nodes · max fan-out 2
   schemas: policy 2.1 · plan 0.1.0 · report 2.0.1

── Step 2/2 · Gate the trusted plan before execution
   policy source: bundled demo refusal policy

PLAN REFUSED: PLAN_FANOUT_EXCEEDED
Plan fan-out is 2; policy allows at most 1 (plan_fanout).

No generated module executed.
Fix the plan or update .maida/policy.yaml after review, then gate again.
```

**That refusal is the product.** The plan was structurally valid and
type-correct. It was stopped because it exceeded a limit you control, before any
child task was inserted.

## Use your own policy

The demo falls back to a bundled policy, but it discovers `.maida/policy.yaml` —
the same file the trace gate reads — and names which one it used.

```yaml
# .maida/policy.yaml
version: 2.1
metrics:
  plan_fanout:
    kind: measured
    direction: upper
    limit: 2
```

Re-run `maida demo --plan`: the source line becomes `policy source:
.maida/policy.yaml` and the plan is approved. An explicit `--policy path.yaml`
overrides both.

Available plan metrics: `plan_depth`, `plan_fanout`, `plan_budget_cost_usd`,
`plan_budget_model_tokens`, `plan_budget_tool_calls`,
`plan_budget_wall_time_ms`, plus the set invariants `plan_effectful_modules`,
`plan_grants`, `plan_modules`, and `plan_shape_seen`.

## PostgreSQL for durable runs

The demo needs no database. Executing real plans does.

```bash
docker compose up -d postgres
export MAIDA_WORKFLOWS_TEST_DSN=postgresql://maida_workflows:local-only@127.0.0.1:55432/maida_workflows
```

The shipped Compose service is local-only, credential-free and uses tmpfs.

## Next

- [Concepts](concepts.md) — the five types that carry the model
- [Your first verified plan](guides/first-plan.md) — end to end in Python
- [Execution substrates](substrates.md) — running on Celery, and Temporal's status
