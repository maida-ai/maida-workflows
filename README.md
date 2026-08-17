# Maida Workflows

Maida Workflows is the optional generated-plan backend for Maida, the
behavioral regression gate for AI agents. It turns runtime planner output into
a canonical plan, validates it against application-owned module contracts and
the core Maida policy before execution, and proves what actually ran. Static
workflow authoring remains a convenience path onto the same durable module
machinery.

See the pre-execution gate refuse a simulated generated plan in one local,
credential-free command:

```bash
maida demo --plan
```

The package supports:

- typed modules, branches, stable-key maps, parallel joins, and nested workflows;
- portable data-authored specs, explainable diagnostics, and safe workflow bundles;
- durable approval, typed input, and external-signal boundaries;
- durable PostgreSQL scheduling, tasks, attempts, events, definitions, and leases;
- process, container, VM, and microVM execution requirements with executor matching;
- content-addressed artifacts and replay-complete accepted boundary records;
- structural diff, zero-live-call full-stub replay, and isolated selective replay;
- allowlisted generated DAG materialization and replay;
- provider-neutral external-flow, connector, trigger, and import contracts;
- install as the `maida.workflows` namespace subpackage (`from maida import workflows`).

Python 3.12 and 3.13 are supported.

## Quick start

```python
from maida.workflows import ExecutionContext, Module, Workflow, compile_workflow


class Upper(Module[str, str]):
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value.upper()


class Greeting(Workflow[str, str]):
    workflow_id = "greeting"
    input_type = str
    output_type = str
    upper = Upper()

    def build(self, value):
        return self.upper(value)


plan = compile_workflow(Greeting())
print(plan.canonical_json())
```

For a PyTorch-style progression from one module through sequential, branching,
parallel, keyed-map, nested, data-authored, serialized, interactive, generated,
external, and replay-ready composition styles, see the
[workflow creation examples](examples/workflow_creation/README.md).

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

- `module_id` aligns the semantic component (by default
  `<workflow_id>.<module_attribute_path>`);
- `logical_step` aligns its stable workflow position;
- `module_digest` changes when implementation, immutable configuration, schema,
  or effect classification changes.

Definitions align by `(module_id, logical_step)`. A mapped or nested execution
adds a deterministic `step_instance_id`; mapped values therefore require a
stable field or callback key. Reusing a module without `.at("stable-step")`, or
creating a duplicate replay key, is a compile error.

## Resource envelopes

Modules can declare immutable resource limits that travel with every durable
task and participate in structural comparison:

```python
from datetime import timedelta

from maida.workflows import Budget, ExecutionContext, Module


class Research(Module[str, str]):
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
do not prove replay-complete module boundaries. Static histories retain the
original fixture contract; generated histories use the next compatible bundle
version to include authenticated plan lineage and concrete generated instances.

## Explainable authoring and serialization

Native `Workflow.build()` remains the shortest authoring path for Python
applications. `WorkflowSpec` provides the same reliability surface as canonical
data for humans, authoring agents, visual builders, and import adapters. A
trusted `ModuleRegistry` publishes aliases and configuration schemas; generated
specs may select those aliases but cannot supply code, imports, credentials,
grants, or execution providers.

```python
compilation = compile_workflow_spec(spec, registry)
print(compilation.issues)
print(compilation.explanation)
workflow = compilation.raise_for_errors()
```

`WorkflowBundle` saves specs and compiled contracts as canonical
`.maida-workflow` data with integrity digests and restrictive permissions.
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

Generated planners return a minimal `PlanFragmentIR`. A planner module declares
a trusted `PlanBoundary` containing its `ModuleRegistry`, structural limits,
root output contract, and maximum child grant. The common path is one call:

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
maida-workflows replay replay-fixtures/case --workflow package.module:workflow --live module:workflow.module_id
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
