# Maida Workflows

Maida Workflows makes runtime-generated plans verifiable before they run. It
resolves minimal planner output against application-owned module contracts,
checks the resulting plan with the core Maida policy, and records evidence at
typed module boundaries so the reliability contract survives distribution onto
someone else's execution substrate. The bundled runner is the offline,
standalone reference path; it is not a hosted scheduler or control plane.

See the pre-execution gate refuse a simulated generated plan in one local,
credential-free command:

```bash
maida demo --plan
```

The generated plan becomes the run's root definition. Planner bytes can choose
only topology and allowlisted aliases; trusted code supplies identities,
schemas, grants, budgets, limits, and execution requirements. An accepted plan
runs through durable workers, while an untrusted or policy-breaking plan fails
closed before generated child insertion.

Python 3.12 and 3.13 are supported. Install the package as the
`maida.workflows` namespace subpackage (`from maida import workflows`).

## Execution backends: bring your own engine

**Maida Workflows is not a workflow engine, and is not trying to become one.**
Temporal, Prefect, Celery, LangGraph, Airflow, and Composio are good at running
work. We are not competing with them; the intent is to run *on* them.

The model we are building toward is the one PyTorch has with compute backends:
you write against a single programming model, and the engine underneath is a
choice rather than a rewrite. A tensor means the same thing on CPU and on CUDA.
A Maida plan should mean the same thing on the bundled runner and on Temporal —
same identity, same pre-execution refusal, same verifiable boundaries.

What is meant to be portable is **the guarantees, not the code**:

- plan identity and content-addressed artifacts;
- the pre-execution gate — a plan refused here is refused everywhere;
- idempotency and dependency contracts at module boundaries;
- replay-complete evidence of what actually ran.

And the point of an adapter is that **you should not have to learn the backend
to get its durability**. Declaring modules and letting a planner emit a graph is
the whole user-facing surface; decomposing that into a particular engine's
workflow/activity/task vocabulary is the adapter's job, not yours.

**Status: this is direction, not a shipped feature.** Today there is one
reference runner — offline, standalone, deliberately boring — and **no
third-party backend adapter ships yet**. The first one lands after the current
realignment, because building adapters before the plan representation settles
would produce exactly the patchwork this project is being corrected away from.

What is already true is the part that makes it possible: the pre-execution gate
does not depend on the bundled runner. `PlanGuardrail` needs a resolved plan and
nothing else — no scheduler, no database, no executor — so the verification
surface is already separable from execution rather than entangled with it.

One honest caveat. Compute backends agree on what a matrix multiply is; workflow
engines disagree about far more — determinism models, retry semantics, and
failure boundaries genuinely differ. So the promise is not that every backend
behaves identically. It is that a **named, checked set of guarantees** holds
across them, and that where a backend cannot honor one, the gap is explicit
rather than silently absorbed.

## Run the real path

The command above is the shortest gate demonstration. To execute accepted
input-dependent plans locally, start with
[`examples/workflow_creation/generated_plan.py`](examples/workflow_creation/generated_plan.py)
and its [copy-pasteable PostgreSQL setup](examples/workflow_creation/README.md#run-the-examples-locally).
The same guide covers the three other shipped examples: canonical serialized
plan data, durable approval, and an external trust boundary. Every one is
executed offline by the test suite.

Static `Workflow.build()` authoring remains a convenience for application-owned
graphs. The generated-plan path is the primary example because runtime plans
are the gap this package closes.

Use `maida-workflows --help` for compile, database, fixture, diff, replay, and
verification commands. Fixture export is explicit and writes private local
files; production payloads are never uploaded automatically.

## Application backend

The same durable run can back a Python service or a frontend without exposing
worker and lease mechanics. Register workflow factories, create the ASGI
adapter, and mount it in the server your application already uses:

```python
from maida.workflows import WorkflowCatalog, create_userplane_app

catalog = WorkflowCatalog([SupportWorkflow])
app = create_userplane_app(store, catalog)
```

The adapter exposes run creation, status, typed commands, cursor-paginated
events, and server-sent events. Starting a run returns immediately and never
executes module handlers in the web process. Tenant scope comes from a trusted
host callback; request payloads cannot select their own tenant. See
[`examples/userplane_quickstart.py`](examples/userplane_quickstart.py) for a
deterministic, credential-free starting point.

## Identity and replay contract

Every executable occurrence has three independent identities:

- required, self-declared `module_id` aligns the semantic component regardless
  of which static or generated plan uses it;
- `logical_step` aligns its stable workflow position;
- `module_digest` changes when implementation, immutable configuration, schema,
  or effect classification changes.

Definitions align by `(module_id, logical_step)`. A mapped or nested execution
adds a deterministic `step_instance_id`; mapped values therefore require a
stable field or callback key. Reusing a module without `.at("stable-step")`, or
creating a duplicate replay key, is a compile error. Modules without a
non-empty `module_id` are also compile errors; identity is never inferred from
a workflow class, attribute name, or enclosing workflow.

This identity inversion is an intentional wire break. Pre-inversion Workflow
IR, specs, bundles, and replay fixtures are rejected; recompile plans and
re-export fixtures instead of comparing artifacts whose identity semantics
changed.

## Resource envelopes

Modules can declare immutable resource limits that travel with every durable
task and participate in structural comparison:

```python
from datetime import timedelta

from maida.workflows import Budget, ExecutionContext, Module


class Research(Module[str, str]):
    module_id = "research.execute"
    input_type = str
    output_type = str
    budget = Budget(
        wall_time=timedelta(minutes=2),
        model_tokens=20_000,
        tool_calls=10,
        cost_usd=0.50,
    )

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value
```

`Budget` is a behavior-bearing declaration, not a usage counter. Runtime and
provider brokers enforce wall-time limits and durably reserve and commit model,
tool, token, and cost usage across retries. Measured usage remains separate from
the compiled definition and task declaration, and a task cannot commit with an
uncommitted reservation.

Replay fixtures are projections of successful native runs: a
canonical manifest plus SHA-256-addressed blobs. Failed, cancelled, paused,
incomplete, redacted, truncated, missing, or corrupt histories fail closed.
Ordinary Maida/OTel/Langfuse/export traces are not accepted because their spans
do not prove replay-complete module boundaries. Static and generated fixture
formats both carry the graph-independent identity contract; generated histories
add authenticated plan lineage and concrete generated instances.

## Explainable authoring and serialization

Native `Workflow.build()` is the shortest authoring path for Python
applications and emits canonical `PlanIR`. Generated planners emit only a
restricted graph-choice mapping, which a trusted `ModuleRegistry` resolves
directly into that same `PlanIR`. `WorkflowSpec` remains only as a temporary
compatibility front-end for existing callers; it has no independent plan
compiler or extra runtime representation.

```python
compilation = compile_workflow_spec(spec, registry)
print(compilation.issues)
print(compilation.explanation)
workflow = compilation.raise_for_errors()
```

`WorkflowBundle.from_plan()` saves canonical `PlanIR` as `.maida-workflow`
data with integrity digests and restrictive permissions. Compatibility specs
may still accompany older bundles until their follow-on deletion task.
Loading parses no executable code. Rebinding through the trusted registry or an
exact factory catalog recomputes all module, schema, execution, access, model,
and budget identities before the definition can run. Pickle, bytecode, SDK
clients, credentials, runtime values, and execution history are never stored in
a workflow bundle.

## Interactions, generated graphs, and integrations

`Approval`, `Input`, and `WaitForSignal` park logical tasks in PostgreSQL and
release their worker lease. Typed idempotent commands later resume the task on
any compatible worker. API and trigger callers can also supply a tenant-scoped
start idempotency key; exact retries reuse one run and task graph.

Generated planners return a minimal mapping of node keys, allowlisted aliases,
dependencies, and outputs. A planner module declares a trusted `PlanBoundary`
containing its `ModuleRegistry`, structural limits, root output contract, and
maximum child grant. Trusted validation resolves that mapping directly into
the same canonical `PlanIR` used by static workflows. The common path is one
call:

```python
result = await WorkflowRunner(store, connectors=connectors).run_generated(planner, request)
```

The runtime executes the planner as a durable bootstrap, validates its accepted
output, adopts the resolved generated plan as the run definition, atomically
inserts the child DAG, and completes the run from the generated outputs. One
`ModuleRegistry` supplies both validation and exact executable resolution; the
caller never computes schema, module, or execution digests. Ordinary workers
claim child tasks independently, and generated histories retain the bootstrap
boundary as provenance without treating the static shell as the result.

Pass the core policy returned by `maida.policy.load_policy()` and, when the
task has an accepted recurring-plan population, its core baseline data as
`policy=` and `plan_baseline=`. The runner evaluates the canonical core
`PlanArtifact` before definition adoption or child insertion. A refusal raises
`maida.workflows.guardrail.PlanGuardrailError` with a stable code and readable
reason; accepted runs retain `PLAN_APPROVED` and `PLAN_EXECUTION_VERIFIED`
evidence in durable history.

The `maida demo --plan` command lazy-loads this package as an optional backend.
Core Maida keeps working when the package is absent; only the generated-plan
demo and runtime-plan gate require it. The core command uses
`.maida/policy.yaml` when present, identifies the selected source in every
result, and otherwise falls back to this package's bundled refusal policy.

`ExternalWorkflow` represents a whole external flow at one typed
capability/effect boundary. Deployment-owned adapters hold provider sessions
and credentials outside serialized plans; changing providers does not add
runtime state or bypass effect replay denial.

## Durable runtime

Submitting and scheduling a workflow never executes module handlers. A
`WorkflowScheduler` creates blocked tasks, makes them ready only after durable
dependencies resolve, and can be reconstructed in another control-plane
process with `WorkflowScheduler.resume()`. Executors claim only compatible
ready work through `TaskEnvelope`; downstream tasks are released from accepted
durable results rather than direct module-to-module calls.

`WorkflowRunner` is the local-development convenience: it hosts the scheduler
and a process executor together while preserving the same durable claim, lease,
and compare-and-swap completion protocol used by remote workers.

Start the local PostgreSQL service and migrate it:

```text
docker compose up -d --wait postgres
export MAIDA_WORKFLOWS_DSN=postgresql://maida_workflows:local-only@127.0.0.1:55432/maida_workflows
uv run maida-workflows db upgrade
```

Expired task attempts can be retried from persisted input. Attempt history is
retained for diagnostics, while only the accepted logical result is eligible
for fixture projection.

Key commands are:

```text
maida-workflows compile --workflow package.module:workflow
maida-workflows run --workflow package.module:workflow --input '{"request":"value"}'
maida-workflows submit --workflow package.module:workflow --input '{"request":"value"}'
maida-workflows schedule RUN_ID --workflow package.module:workflow
maida-workflows worker --workflow package.module:workflow --worker-id worker-1
maida-workflows trace export RUN_ID --output replay-fixtures/case
maida-workflows diff replay-fixtures/case --workflow package.module:workflow
maida-workflows replay replay-fixtures/case --workflow package.module:workflow --mode full-stub
maida-workflows replay replay-fixtures/case --workflow package.module:workflow --live module:application.component
maida-workflows replay replay-fixtures/case --workflow package.module:workflow --live step:logical-step
```

Full-stub replay validates integrity/alignment and injects every accepted output;
it invokes no module, model, tool, read, or effect path and creates no new usage.
Selective replay runs only exact selected boundaries against recorded input,
wraps those attempts in ordinary local Maida traces, compares behavior and
usage, validates the current handoff contract, then continues with historical
downstream outputs. Supported effect paths always go through `ReplayBroker`;
any effect attempt is a hard `REPLAY_EFFECT_VIOLATION`, while an explicitly
selected `effectful` module remains stubbed.

`REPLAY_DIVERGENCE` remains diagnostic in the replay detail. Invalid fixtures,
contract-invalid injection, selective execution failures, budget failures, and
effect violations block. Product-level policy and report decisions use Maida's
core schemas rather than a second verification container in this package.
Replay fixtures keep private payloads in local content-addressed storage; the
plan guardrail consumes Maida's core payload-free baseline population rather
than defining a second baseline format here.

## Native replay demo

The offline demo captures a native run, exports it, changes one same-identity
module, prints the structural/content diff, performs full-stub and selective
replay, and reports both source and replay effect-sentinel counts:

```text
uv run python -m examples.native_replay_demo \
  --dsn "$MAIDA_WORKFLOWS_DSN" \
  --artifacts /tmp/maida-workflows-demo-artifacts \
  --fixture /tmp/maida-workflows-demo-fixture
```

For a clean recording, choose new artifact/fixture paths, start from a healthy
local Compose service, and show the final JSON. Expected evidence includes one
`MODULE_DIGEST_CHANGED`, `full_stub_live_calls: 0`,
`selective_prepare_calls: 0`, `selective_decide_calls: 1`, and
`replay_effect_calls: 0`. The demo is deterministic and needs no cloud account,
API key, or production credential.

## Safety and compatibility

Replay workers prevent runtime-managed effect paths from reaching registered
production adapters. They do not sandbox arbitrary Python syscalls, so untrusted
selective modules still require appropriate process or container isolation.

Import the package as `maida.workflows` (or `from maida import workflows`). It
requires a `maida-ai` release that enables the `maida` namespace via
`pkgutil.extend_path`.

## Development

```text
uv sync --locked
uv run pytest --cov --cov-branch
uv run ruff check .
uv run ruff format --check .
uv run mypy .
uv build
```

PostgreSQL integration tests use `MAIDA_WORKFLOWS_TEST_DSN`. A local service is
provided in `compose.yaml`.

Test and package metadata target Python 3.12 and 3.13.
