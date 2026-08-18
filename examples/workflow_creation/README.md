# Plan examples

Start with [`generated_plan.py`](generated_plan.py). It demonstrates the reason
Maida Workflows exists: a planner creates a plan after receiving a request, the
plan is resolved against application-owned contracts and checked by Maida
policy, and only an accepted plan can execute.

Examples are organized by reliability boundary, not by authoring difficulty.
Static composition depth is ordinary Python authoring, not a product
capability. The five examples each exercise one distinct boundary:

| Example | What actually runs |
| --- | --- |
| `generated_plan.py` | Two input-dependent plans through `WorkflowRunner.run_generated()` |
| `celery_backend.py` | The brief generated plan through the real Celery adapter and an offline transport stub |
| `serialized_plan.py` | Registry-bound canonical plan data through the ordinary runner |
| `approval_boundary.py` | A task that parks, receives a durable approval, and resumes |
| `external_boundary.py` | An external effect through a deterministic deployment-owned adapter |

Every module is offline and deterministic. Each exports `EXAMPLE_INPUT`,
`EXPECTED_OUTPUT`, and an async `run_example(store, value)` function. The test
suite discovers this directory, executes every example against PostgreSQL, and
fails if a new example has no execution coverage.

## Generated plan first

The planner in `generated_plan.py` returns only node keys, allowlisted module
aliases, dependencies, and outputs. It branches on the request: `"brief
request"` creates a two-node normalize/draft plan, while `"thorough request"`
creates a five-node plan with fan-out, a read boundary, and an effect boundary.
The tests execute both requests and require different root definition digests,
different materialized node counts, and their documented outputs. Returning a
module-level constant from the planner therefore fails the documentation test.

Trusted application code supplies the `PlanBoundary`, `ModuleRegistry`, output
contract, structural limits, maximum capability grant, module identities,
schemas, budgets, and execution requirements. Generated data cannot select any
of those values.

The closed loop is one call:

```python
result = await WorkflowRunner(store, connectors=connectors).run_generated(
    planner,
    request,
    policy=policy,
)
```

Before any generated child is inserted, the runner:

1. executes the planner as a durable bootstrap boundary;
2. strictly parses and resolves its output against the trusted registry;
3. evaluates the canonical Maida `PlanArtifact` under core policy;
4. adopts the accepted generated plan as the run's root definition; and
5. materializes and executes its children through ordinary durable workers.

The bootstrap remains in history as provenance. It is not a static workflow
shell and does not provide the run result.

For the credential-free refusal story without PostgreSQL, run:

```bash
maida demo --plan
```

The command uses `.maida/policy.yaml` when present and otherwise uses the
package's bundled refusal policy. A refusal has a stable machine code and a
readable reason, and no generated child executes.

## Run the examples locally

Start and migrate the repository's local PostgreSQL service:

```bash
docker compose up -d --wait postgres
export MAIDA_WORKFLOWS_DSN=postgresql://maida_workflows:local-only@127.0.0.1:55432/maida_workflows
uv run maida-workflows db upgrade \
  --artifacts /tmp/maida-workflows-example-artifacts
```

Then pass a store to the example you want to run:

```python
import asyncio
import json
import os
from pathlib import Path

from examples.workflow_creation.generated_plan import run_example
from maida.workflows.artifacts import ArtifactStore, ValueCodec
from maida.workflows.persistence import PostgresStore


async def main() -> None:
    store = PostgresStore(
        os.environ["MAIDA_WORKFLOWS_DSN"],
        ValueCodec(ArtifactStore(Path("/tmp/maida-workflows-example-artifacts"))),
    )
    result = await run_example(store, "thorough request")
    history = store.load_run_history(result.run_id, tenant_id="local")
    proof = next(event for event in history.events if event.event_type == "PLAN_EXECUTION_VERIFIED")
    print(f"output={result.output}")
    print(f"status={history.run.status.value}")
    print(f"plan_proof={proof.event_type} {json.dumps(proof.payload, sort_keys=True)}")
    print(
        "accepted_boundary_instances="
        + ",".join(boundary.instance_key for boundary in history.accepted_boundaries)
    )


asyncio.run(main())
```

The output shows the result, terminal status, post-execution plan proof, and
the exact module-boundary instances whose typed results were accepted:

```text
output=delivered:draft:THOROUGH REQUEST | context:thorough request
status=SUCCEEDED
plan_proof=PLAN_EXECUTION_VERIFIED {"artifact_id": "...", "region_instance_id": "request-plan-root"}
accepted_boundary_instances=demo.planner@bootstrap/planner#...,demo.normalize@dynamic/request-plan/nodes/normalize#...,...
```

The abbreviated values above keep the guide readable; the command prints the
real content-addressed artifact ID and every accepted boundary instance. A
successful return without `PLAN_EXECUTION_VERIFIED` is not verified execution.

Change the import to `celery_backend`, `serialized_plan`, `approval_boundary`,
or `external_boundary` to execute the other examples with the same store.

## Celery execution backend

`celery_backend.py` uses the same `CeleryBackend` API as a deployment. Its
eager task replaces only Celery's broker and worker pool so the example remains
offline; the adapter still JSON-round-trips the strict execution request and
the registered handler still resolves trusted modules and commits real durable
boundaries. Queue routing, retry timing, worker identity, and compute placement
are intentionally not part of the Maida plan or the cross-backend comparison.

## Canonical serialized plan

`serialized_plan.py` serializes the canonical `PlanIR` emitted by ordinary
workflow authoring. `WorkflowBundle.from_plan()` stores that survivor directly
with an integrity digest and restrictive permissions. Loading parses no
executable code, and rebinding through the same trusted `ModuleRegistry`
recomputes exact module identity before the ordinary runner can execute it.
There is no separate serialized graph representation.

## Durable approval boundary

`approval_boundary.py` executes a real `Approval` task. The first worker parks
the task and releases its lease, the example sends one typed idempotent
`ApproveCommand`, and the same durable run resumes to its documented result.
No process blocks waiting for a person.

## External boundary

`external_boundary.py` wraps one provider-owned flow as a typed effect boundary.
The serialized workflow contains only neutral identity and contracts. Its local
adapter is deliberately deterministic and credential-free; a deployment owns
the real provider session and registers it behind the same connector boundary.
The example does not claim that Maida owns provider execution or ships a
provider-specific adapter.

## Other executable examples

The top-level examples are integration fixtures rather than an authoring
progression:

- `examples/native_replay_demo.py` captures, exports, diffs, and replays a real
  native run while proving that replay invokes no effect adapter.
- `examples/userplane_quickstart.py` mounts a deterministic workflow behind the
  ASGI adapter; the ASGI tests submit and complete its run.
- `examples/adversarial_workflows.py` supplies compact branch, map, nested,
  parallel, and effect histories for compiler and replay integration tests.

All three are included in the repository-wide shipped-example inventory test
and have PostgreSQL-backed execution coverage.
