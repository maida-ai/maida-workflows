# API reference

All 105 exported names, grouped by purpose. Import everything from
`maida.workflows`. Docstrings in `maida/workflows/` are the authority for
signatures; where this page and the code disagree, the code is right.

## Authoring

| Names | Purpose |
| --- | --- |
| `Module`, `ExecutionContext`, `RuntimeValue`, `Workflow`, `BoundModuleCall` | The typed unit of work, its runtime context, and static graph authoring. |
| `literal`, `map_over`, `parallel`, `when` | Control constructs for static authoring. Not available to generated plans. |
| `compile_workflow`, `CompileError`, `SymbolicValueError` | Compile a static workflow into `PlanIR`. |
| `bind_workflow`, `BoundWorkflow` | Bind a definition to concrete module instances. |

## Plan representation and identity

| Names | Purpose |
| --- | --- |
| `PlanIR`, `StepIR`, `BindingIR`, `ReplayKey`, `module_digest` | The single canonical plan type and the identities addressing occurrences within it. |
| `PlanSignature`, `PlanTaskProvenance`, `GeneratedPlanRecord` | Resolved evidence derived from a validated plan. |

## Generated plans and the trust boundary

| Names | Purpose |
| --- | --- |
| `PlanBoundary`, `PlanLimits`, `PlanValidator`, `PlanValidationError` | What a planner may do, and the validator resolving generated choices against trusted contracts. |
| `ModuleRegistry`, `ModuleTemplate` | The trusted allowlist — validation metadata and exact executable resolution from one registration. |

## Access: connectors, effects, grants

| Names | Purpose |
| --- | --- |
| `Connector`, `ConnectorAdapter`, `ConnectorRegistry`, `Capability` | Grant-checked reads from external systems. |
| `Effect`, `EffectAdapter`, `EffectSpec`, `Idempotency` | Idempotent side effects with declared keys. |
| `AccessBroker`, `AccessPolicy`, `AccessContractError`, `CapabilityGrant`, `PolicyDecision`, `ApprovalEvidence` | Grant enforcement, denial as an observable event, attributed approval records. |

## Budgets and models

| Names | Purpose |
| --- | --- |
| `Budget`, `BudgetUsage`, `BudgetExceededError` | Immutable per-occurrence resource envelopes, durably reserved and committed. |
| `ModelBroker`, `ModelSpec`, `ModelAdapter`, `ModelAdapterRegistry`, `ModelCallResult` | Budgeted model calls. Adapters are yours; no provider ships here. |

## Execution

| Names | Purpose |
| --- | --- |
| `WorkflowRunner`, `RunResult`, `RunStatus`, `RunHistory` | The one-call entry point and what a run produces. |
| `ExecutionRequest`, `ExecutionBackend`, `BoundaryHarness` | The substrate seam. Target these to write an adapter. |
| `CeleryBackend` | The one shipped external adapter. |
| `LocalExecutor`, `TaskWorker`, `WorkflowScheduler`, `WorkflowCoordinator`, `TaskEnvelope`, `ScheduleProgress`, `CoordinatorProgress` | Reference fixtures for offline development and tests — not a production runtime. |
| `ExecutionSpec`, `ExecutionMode`, `ExecutorCapabilities`, `TaskStatus`, `StoredValue`, `BoundaryRecord` | Execution requirements and durable records. |

## Human interaction

| Names | Purpose |
| --- | --- |
| `Approval`, `ApprovalDecision`, `Input`, `WaitForSignal` | Durable steps that park a run until answered. |
| `WorkflowClient`, `WorkflowRun`, `RunSnapshot`, `RunEvent`, `EventPage`, `CommandReceipt`, `CommandType`, `parse_command` | Driving and observing runs from an application. |
| `ApproveCommand`, `RejectCommand`, `InputCommand`, `SignalCommand`, `RunCommand`, `CancelCommand`, `PauseCommand`, `ResumeCommand`, `RetryCommand` | The closed typed command set. |
| `UserplaneASGI`, `create_userplane_app`, `WorkflowCatalog` | Mount runs behind the web server you already have. |

## Replay and portability

| Names | Purpose |
| --- | --- |
| `ReplayFixture`, `ReplayFixtureError`, `ReplayCase`, `ReplayMode`, `ReplayResult`, `ReplayDivergence` | Projection of a successful run, and deterministic re-execution against it. |
| `WorkflowBundle`, `WorkflowBundleError`, `WorkflowPortability` | Save a canonical plan and rebind it against a trusted registry or exact factory. |
| `ExternalWorkflow` | Wrap a provider-owned flow as one typed, opaque boundary. |

## Shipped examples

Every example is executed offline by the test suite; one that stops running
fails CI.

| Example | Shows |
| --- | --- |
| `workflow_creation/generated_plan.py` | **Start here.** Input-dependent planner, verified and executed end to end, with a connector read and an idempotent effect. |
| `workflow_creation/serialized_plan.py` | Save a canonical plan, load, rebind against a trusted registry, execute. |
| `workflow_creation/approval_boundary.py` | A run that parks on `APPROVAL_REQUIRED`, receives an `ApproveCommand`, resumes. |
| `workflow_creation/external_boundary.py` | The typed external-workflow boundary with a deterministic local adapter. |
| `workflow_creation/celery_backend.py` | The same plan executed through the Celery seam. |
| `userplane_quickstart.py` | Credential-free starting point for backing an application. |
| `native_replay_demo.py` | Export a fixture from a successful run and replay it with no live calls. |
| `adversarial_workflows.py` | Branch, stable-map, nested, parallel and effect boundaries under stress. |
