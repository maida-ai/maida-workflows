# Human boundaries

Approvals and inputs are durable steps, not callbacks. A run parks and resumes
rather than blocking a worker, so the process can restart and the run continues.

| Module | Behavior |
| --- | --- |
| `Approval` | Parks the run and records `APPROVAL_REQUIRED`. Resumed by `ApproveCommand` or `RejectCommand`; yields an `ApprovalDecision` with attributed evidence. |
| `Input` | Requests a typed value from a person. Resumed by `InputCommand`. |
| `WaitForSignal` | Waits for an external event. Resumed by `SignalCommand`. |

Working example: `examples/workflow_creation/approval_boundary.py`, which parks,
records the event, sends an `ApproveCommand`, and resumes through the durable
worker protocol.

## Driving runs from an application

Commands arrive through `WorkflowClient`, or over HTTP if you mount the ASGI
adapter in the server you already run:

```python
from maida.workflows import WorkflowCatalog, create_userplane_app

catalog = WorkflowCatalog([SupportWorkflow])
app = create_userplane_app(store, catalog)
```

The adapter exposes run creation, status, typed commands, cursor-paginated
events and server-sent events. Starting a run returns immediately and never
executes module handlers in the web process.

Tenant scope comes from a trusted host callback — request payloads cannot select
their own tenant.

## Approvals as policy

An effect can require approval. `approval_requirements` appears in the plan
signature, so `plan_grants` policy can demand that a named effect carry an
approval before the plan is allowed to run — checked before execution, not
during it.
