# Maida Workflows

Maida Workflows is a local-first Python runtime for statically composed,
durable, replayable AI workflows. It compiles workflow definitions into a
canonical replay-addressable IR, records accepted module-boundary history in
PostgreSQL, and projects successful native runs into portable replay fixtures.

The first implementation supports:

- typed modules, branches, stable-key maps, parallel joins, and nested workflows;
- durable PostgreSQL runs, tasks, attempts, events, definitions, and leases;
- content-addressed artifacts and replay-complete accepted boundary records;
- structural diff, zero-live-call full-stub replay, and isolated selective replay;
- a canonical `maida_workflows` package and `maida.workflows` wheel shim.

Python 3.12 and 3.13 are supported. The temporary `<3.14` cap follows the
required `maida-ai` integration dependency and can be removed when that package
widens its own compatibility range.

## Quick start

```python
from maida_workflows import ExecutionContext, Module, Workflow, compile_workflow


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

Use `maida-workflows --help` for compile, database, fixture, diff, replay, and
verification commands. Fixture export is explicit and writes private local
files; production payloads are never uploaded automatically.

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

The `ReplayFixture 0.1.0` format is a projection of a successful native run: a
canonical manifest plus SHA-256-addressed blobs. Failed, cancelled, paused,
incomplete, redacted, truncated, missing, or corrupt histories fail closed.
Ordinary Maida/OTel/Langfuse/export traces are not accepted because their spans
do not prove replay-complete module boundaries.

## Durable runtime

Start the local PostgreSQL service and migrate it:

```text
docker compose up -d --wait postgres
export MAIDA_WORKFLOWS_DSN=postgresql://maida_workflows:local-only@127.0.0.1:55432/maida_workflows
uv run maida-workflows db upgrade
```

Runtime recovery uses persisted task input, leases, compare-and-swap completion,
and accepted checkpoints only. Replay resolution is a separate module and is
not imported by the recovery worker. Expired attempts remain diagnostic
history; only one accepted logical boundary is eligible for fixture projection.

Key commands are:

```text
maida-workflows compile --workflow package.module:workflow
maida-workflows run --workflow package.module:workflow --input '{"request":"value"}'
maida-workflows trace export RUN_ID --output replay-fixtures/case
maida-workflows diff replay-fixtures/case --workflow package.module:workflow
maida-workflows replay replay-fixtures/case --workflow package.module:workflow --mode full-stub
maida-workflows replay replay-fixtures/case --workflow package.module:workflow --live module:workflow.module_id
maida-workflows replay replay-fixtures/case --workflow package.module:workflow --live step:logical-step
maida-workflows verify replay-fixtures/case --workflow package.module:workflow
```

Full-stub replay validates integrity/alignment and injects every accepted output;
it invokes no module, model, tool, read, or effect path and creates no new usage.
Selective replay runs only exact selected boundaries against recorded input,
wraps those attempts in ordinary local Maida traces, compares behavior and
usage, validates the current handoff contract, then continues with historical
downstream outputs. Supported effect paths always go through `ReplayBroker`;
any effect attempt is a hard `REPLAY_EFFECT_VIOLATION`, while an explicitly
selected `effectful` module remains stubbed.

`REPLAY_DIVERGENCE` is diagnostic by default and can be promoted with
`verify --block-divergence`. Invalid fixtures, contract-invalid injection,
selective execution failures, budget failures, and effect violations block.
Baselines contain fixture/population digests and provenance, never fixture
payloads.

## Deterministic Phase 2 exit demo

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

CI should order checks as: compile, structural diff, full-stub replay, selective
replay of changed modules, representative live reruns, then the broader
behavioral gate.

## Current boundaries

Deferred work includes the HTTP/SSE userplane, durable approval commands,
production capability/effect brokers and idempotency, OS-level syscall
sandboxing, dynamic Plan IR execution/replay, hosted retention, generic-trace
heuristics, and Path A instrumentation. Replay workers currently guarantee that
runtime-managed effect paths cannot reach production adapters; arbitrary Python
syscall isolation needs the future OS/container sandbox.

`maida_workflows` is canonical. Built wheels also install `maida.workflows` as a
compatibility shim. Editable overlay behavior remains experimental because the
installed `maida` parent is a regular, non-cooperative namespace package.

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

Python 3.12 and 3.13 are both test targets. Removing the temporary `<3.14` cap
remains follow-up work after the required `maida-ai` dependency supports 3.14.
