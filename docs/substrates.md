# Execution substrates

Maida Workflows does not own distributed execution. It carries a set of
guarantees onto whatever runtime you already operate — or are choosing.

If you are evaluating substrates right now, this page is the honest state of
things: what works today, what does not, and what adopting each would cost.

## What is portable

The point of the seam is that **guarantees** move, not code:

- plan identity and content-addressed artifacts;
- the pre-execution gate — a plan refused here is refused everywhere;
- idempotency and dependency contracts at module boundaries;
- replay-complete evidence of what actually ran.

Compute backends agree on what a matrix multiply is; workflow engines disagree
about determinism, retry semantics and failure boundaries. So the promise is not
that every substrate behaves identically. It is that a named, checked set of
guarantees holds across them, and where a substrate cannot honor one, the gap is
explicit rather than silently absorbed.

## Status

| Substrate | Status | Notes |
| --- | --- | --- |
| Bundled local runner | **Available** | `LocalExecutor` / `TaskWorker`. A reference fixture for development and tests — deliberately not a production runtime. |
| Celery | **Available** | `CeleryBackend`. Shipped adapter, offline-testable, [worked example](#celery). |
| Temporal | **Not shipped** | The seam exists and is documented [below](#temporal). No adapter, no timeline commitment. |
| Prefect, Airflow, others | **Not shipped** | Same seam. No adapter. |

## The seam

Two types. Target these to write an adapter.

**`ExecutionRequest`** is the transport envelope: five identity strings
(`run_id`, `tenant_id`, `workflow_id`, `definition_digest`, `task_id`) and
nothing else. No queue, credential, retry policy or compute target — by
construction, not convention. It carries a stable `execution_id` used as the
dispatch idempotency key.

**`BoundaryHarness`** is the substrate-neutral trust boundary. It validates
exact run/tenant/workflow/definition/task identity, resolves trusted modules,
enforces schemas, budgets, grants, effects and interactions, and records the
accepted boundary. It explicitly does **not** claim tasks, lease deliveries,
heartbeat, match compute capabilities, or choose retries.

That division is the contract:

| Yours (the substrate) | Ours (the harness) |
| --- | --- |
| Delivery, acknowledgement, redelivery | Occurrence identity |
| Retry timing and backoff | Idempotency contracts |
| Worker pools and scaling | Dependency ordering |
| Routing and compute placement | Typed boundaries, grants, budgets |
| Queue topology | Accepted-boundary evidence and proof |

An adapter that enters the local claim, lease, heartbeat, capability-matching or
retry lifecycle has not drawn the seam — it has wrapped our engine in your
transport. The schema enforces the split: an attempt has *either* a local lease
token *or* an external execution id, never both.

## Celery

**Status: available.** Ships as `maida.workflows.CeleryBackend`. No Celery
production dependency is required to use the rest of the package; the shipped
example and tests run offline against a fake task.

Worker side — trusted application code returns a harness for the delivered
request:

```python
from maida.workflows import CeleryBackend

handler = CeleryBackend.task_handler(
    lambda request: harness_for(store, request)
)
```

Controller side:

```python
from maida.workflows import CeleryBackend, WorkflowRunner

result = await WorkflowRunner(
    store,
    backend=CeleryBackend(celery_task),
).run_generated(planner, request)
```

Celery owns delivery, redelivery, retry timing, worker pools and routing. A
failed external delivery leaves the task `READY` and emits no retry transition —
your Celery configuration decides whether to try again. Redelivery of the same
`execution_id` is accepted exactly once, fenced by a non-expiring reservation
rather than a second scheduler lease.

Working example: `examples/workflow_creation/celery_backend.py`.

## Temporal

**Status: not shipped.** There is no Temporal adapter and no committed date.
This section exists so you can size the work rather than guess.

A Temporal adapter would:

1. accept an `ExecutionRequest` as activity input — it is already a small,
   strictly-decoded, JSON-safe mapping;
2. resolve a `BoundaryHarness` from trusted application code in the worker;
3. call `harness.run_request(request)` and return acceptance;
4. let Temporal own activity retries, timeouts, heartbeating and task queues,
   and let Maida own identity, idempotency, boundaries and proof.

`maida/workflows/celery.py` is short and is the reference implementation of that
shape.

Two things to weigh before committing:

- **Determinism.** Temporal workflows are replay-deterministic; activities are
  not. The harness belongs in an *activity*, never in workflow code. Plan
  generation and gating are ordinary work, not workflow-deterministic code.
- **Store access** — see below. This is the sharper constraint for Temporal
  Cloud specifically.

## Known constraint: workers need store access

Every external worker requires **direct write access to the authoritative Maida
store**. The small `ExecutionRequest` is only the transport envelope: the
harness reads trusted task state and writes effect reservations, budget usage,
the accepted boundary, and post-execution proof evidence in that store itself.

There is currently **no pluggable evidence sink**.

Practically:

- Celery workers on your own network or VPC: fine.
- Self-hosted Temporal workers alongside your database: fine.
- Temporal Cloud, serverless functions, or any worker that cannot reach your
  database: this is a real obstacle today.

Whether the harness should emit evidence through the transport instead of
writing directly is an open design question, deliberately not answered before a
second real adapter exists to inform it.

## Choosing while this is in flux

If you are selecting a substrate now and want the Maida guarantees:

- **Celery** is the only path that works end to end today, with a shipped
  adapter and an offline example.
- **Temporal** is a supported target in principle and an unwritten adapter in
  practice. If you pick it, the adapter is yours to write until one ships, and
  the store-access constraint applies to your deployment topology.
- **The bundled runner** is fine for development and tests, and is not intended
  to run your production workload.

The plan verification loop — generate, resolve, gate, refuse or approve — is
independent of all of this. It runs before any substrate is involved, so
choosing later does not block adopting the gate now.
