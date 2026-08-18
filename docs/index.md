# Maida Workflows

Maida Workflows makes runtime-generated plans verifiable before they run. A
planner emits graph choices; trusted application code supplies every identity,
schema, grant and budget; policy either approves the resolved plan or refuses it
before any work is inserted. After execution, typed evidence proves the approved
plan is what ran.

It is the optional generated-plan backend for the `maida` product.

```bash
uv tool install --force --python 3.12 \
  --with "maida-workflows>=0.1.0" "maida-ai>=0.5.2.post1"

maida demo --plan
```

## Documentation

```{toctree}
:hidden:
:maxdepth: 2

getting-started
concepts
guides/first-plan
guides/connectors-and-effects
guides/fan-out-plans
guides/human-boundaries
substrates
integrations/composio
reference
```

| Page | What it covers |
| --- | --- |
| [Getting started](getting-started.md) | Install, first refusal, using your own policy file |
| [Concepts](concepts.md) | Module, ModuleRegistry, PlanBoundary, PlanIR, the guardrail |
| [Your first verified plan](guides/first-plan.md) | End-to-end tutorial with real code |
| [Connectors and effects](guides/connectors-and-effects.md) | Grant-checked reads and idempotent side effects |
| [Fan-out plans](guides/fan-out-plans.md) | Multi-step graphs and what policy sees |
| [Human boundaries](guides/human-boundaries.md) | Approvals, typed input, external signals |
| [Execution substrates](substrates.md) | Running on Celery today, and what Temporal would take |
| [Composio](integrations/composio.md) | Recipe for using Composio tools behind a Maida contract |
| [API reference](reference.md) | All 105 exports, grouped |

## Scope check

Read this before anything else. Several things people expect are deliberately
absent, and knowing that early saves an afternoon.

**There is no agent abstraction here.** No `Agent` class, no prompt templates,
no tool-calling loop, no memory system. The unit of composition is a typed
`Module`; the unit of verification is a `PlanIR`. Use whatever agent framework
you like, then bring its output here to be checked.

| You may be looking for | Status | What exists instead |
| --- | --- | --- |
| Composio integration | Not shipped | Wrap your own Composio client in a `Connector` / `Effect`. [Recipe](integrations/composio.md). |
| "Start your first agent" | Not here | `Module` — a typed, self-identifying unit with a declared budget. |
| Multi-agent flow | Not here | A generated **plan**: a DAG of modules validated before it runs. |
| Temporal adapter | Not shipped | `CeleryBackend` ships today. The seam a Temporal adapter targets is [documented](substrates.md). |
| Tool use | Different name | `Connector` for grant-checked reads, `Effect` for idempotent side effects. |
| Connectors | Available | Exactly that, via the access broker with capability grants. |

### The boundary, in one question

Does this exist so a plan can be *verified*, or so work can be *run*?
Verification is ours. Scheduling, delivery, leases, retry timing, routing and
compute placement belong to whichever runtime you already operate.

## Requirements

- Python 3.12 or 3.13
- PostgreSQL for durable runs (the repository ships a Compose service)
- `maida-ai>=0.5.2.post1`

Installs as the `maida.workflows` namespace subpackage: `from maida import workflows`.
