# Workflow creation examples

These examples introduce Maida Workflows the way a PyTorch tutorial introduces
models: create small reusable components, register them on a parent object, and
compose them in one readable method.

The important semantic difference is that `Workflow.build()` is pure
graph-construction code. It receives symbolic `RuntimeValue` objects and
describes static Workflow IR; it does not perform the work. Actual work happens
later in each module's asynchronous `execute()` method.

| PyTorch | Maida Workflows |
| --- | --- |
| `nn.Module` | `Module[Input, Output]` |
| Submodules assigned in `__init__` | Modules or child workflows assigned in `__init__` |
| `forward()` composition | `Workflow.build()` composition |
| Runtime tensor | Symbolic `RuntimeValue` during graph construction |
| Module computation | `Module.execute()` |

Do not branch on, iterate over, or measure a `RuntimeValue` with ordinary
Python. Use `when()`, `map_over()`, and `parallel()` so the complete graph
exists before execution.

`parallel()` expresses independent branches. `map_over()` currently guarantees
stable item identity, but does not promise concurrent item execution.

## Progression

| Level | Example | What it teaches |
| --- | --- | --- |
| Easy | `easy_first_workflow.py` | One typed module and one workflow |
| Easy | `easy_sequential.py` | Passing a symbolic module output to the next module |
| Intermediate | `intermediate_branching.py` | Runtime branching with `when()` |
| Intermediate | `intermediate_parallel.py` | Parallel work and a typed join |
| Advanced | `advanced_stable_map.py` | Runtime mapping with stable item keys |
| Advanced | `advanced_nested.py` | A child workflow composed inside a parent |
| Advanced | `advanced_portable_workflow.py` | Data-authored workflow specs and safe serialization |
| Advanced | `advanced_interactive.py` | Durable approval and explicit decision branches |
| Expert | `expert_replay_ready.py` | Map, parallel, branch, nesting, and explicit replay identity |
| Expert | `expert_generated_workflow.py` | Allowlisted generated fan-out/fan-in plans |
| Expert | `expert_external_workflow.py` | An honest typed boundary around an external flow |

Deterministic static examples export these names:

```python
workflow  # importable workflow instance
EXAMPLE_INPUT  # deterministic JSON-safe input
EXPECTED_OUTPUT  # exact expected result
```

All handlers are offline and deterministic. They need no model, network call,
credential, or production connector.

The generated example instead exports `planner`, `connectors`, and
`run_example()`, because its graph does not exist until the planner sees a
runtime input.

The interactive example is compilable offline and pauses only when executed.
The external example is compilable offline and requires a deployment adapter
for live execution. This distinction is intentional: workflow definitions
contain behavioral contracts, never provider clients or credentials.

## Native Python or portable data

Use native Python composition when the graph belongs in application code. Use
`WorkflowSpec` when a human, AI agent, visual builder, or external authoring
system needs to create the graph as explainable data:

```python
spec = WorkflowSpec(
    workflow_id="onboarding-portable",
    input_schema=type_schema(str),
    output_schema=type_schema(str),
    nodes=(
        NodeSpec.task("title", "text.title", BindingSpec.root()),
        NodeSpec.task("prefix", "text.prefix", BindingSpec.node("title")),
    ),
    output=BindingSpec.node("prefix"),
)

compilation = compile_workflow_spec(spec, registry)
print(compilation.explanation)
bound = compilation.raise_for_errors()
```

The recommended authoring loop is deliberately mechanical:

```text
inspect registry schemas
  → emit or edit WorkflowSpec data
  → compile to stable diagnostics and an explanation
  → review the exact graph and access surface
  → serialize a canonical bundle
  → bind through the trusted registry
  → execute, diff, and replay normally
```

An authoring agent selects allowlisted aliases and supplies schema-valid
configuration. It cannot add import paths, executable code, credentials,
capability grants, or unregistered modules. Compilation returns location-aware
diagnostics instead of guessing how to repair an ambiguous graph.

## Save and load workflows safely

`WorkflowBundle` is the workflow-definition equivalent of a model checkpoint,
with an important trust distinction: it stores canonical data, not pickle or
arbitrary Python bytecode.

```python
bundle = WorkflowBundle.from_spec(spec, registry)
bundle.save(Path("greeting.maida-workflow"))

loaded = WorkflowBundle.load(Path("greeting.maida-workflow"))  # parses no code
workflow = loaded.bind(module_registry=registry)  # recomputes every trusted pin
```

The file includes editable authoring data, compiled Workflow IR, exact module
requirements, and integrity digests. SDK clients, credentials, run payloads,
and execution history are excluded. Native Python workflows can also be saved
as factory-bound bundles, but loading them requires an exact digest-pinned
application factory because arbitrary graph-building code is not reconstructable
from safe data alone.

## Durable interaction boundaries

`Approval`, `Input`, and `WaitForSignal` are typed modules. A worker reaching
one records a request and gives up its lease; no process waits. A later typed,
idempotent command makes that logical task claimable by any compatible worker.
The interaction's accepted value becomes an ordinary replayable boundary.

## Generated graphs

The generated example keeps planner authority narrow. `PlanFragmentIR` contains
only stable node keys, allowlisted aliases, dependencies, and outputs. The
planner branches on its input: a brief request produces two nodes, while a
thorough request produces a five-node graph with fan-out, depth, a read grant,
and an effect boundary.

`PlanBoundary` is the trusted application-code marker that supplies the
registry, output contract, limits, and maximum grant. `WorkflowRunner` closes
the entire loop without manual digest or materialization calls:

```python
result = await WorkflowRunner(store, connectors=connectors).run_generated(planner, request)
```

The generated plan becomes the run definition. The durable bootstrap planner
task remains only as the parent provenance for its generated children; it is
not a static workflow shell and does not provide the run result.

## External systems

`ExternalWorkflow` wraps a provider-owned flow as one typed, opaque boundary.
Deployment-owned adapters register behind the normal connector/effect broker;
the workflow retains only a neutral connector operation and immutable version.
Replay stops at the Maida effect boundary before a provider session can run.

## Compile first

Compilation needs no database:

```bash
uv run maida-workflows compile \
  --workflow examples.workflow_creation.easy_first_workflow:workflow
```

The output is canonical IR. Notice that the module is addressable by a stable
`module_id`, `logical_step`, and `module_digest` before it runs.

## Run an example

Start the repository's local PostgreSQL service once:

```bash
docker compose up -d --wait postgres
export MAIDA_WORKFLOWS_DSN=postgresql://maida_workflows:local-only@127.0.0.1:55432/maida_workflows
uv run maida-workflows db upgrade \
  --artifacts /tmp/maida-workflows-onboarding-artifacts
```

Then run the first workflow:

```bash
uv run maida-workflows run \
  --workflow examples.workflow_creation.easy_first_workflow:workflow \
  --input '"Ada"' \
  --artifacts /tmp/maida-workflows-onboarding-artifacts
```

The result contains a generated run ID and definition digest. Its output is:

```json
"Hello, Ada!"
```

Try the remaining examples by changing the workflow reference and input:

```text
easy_sequential
  input:    "  ADA LOVELACE "
  output:   "Hello, Ada Lovelace!"

intermediate_branching
  input:    {"priority":"urgent","text":"Login broken"}
  output:   "human-review"

intermediate_parallel
  input:    "Maida workflows"
  output:   {"characters":15,"uppercase":"MAIDA WORKFLOWS","words":2}

advanced_stable_map
  input:    [{"id":"doc-b","text":"  Beta "},{"id":"doc-a","text":" ALPHA "}]
  output:   "beta | alpha"

advanced_nested
  input:    "Reliable workflows\nmake changes reviewable"
  output:   "Reliable workflows (5 words)"

expert_replay_ready
  input:    [{"id":"doc-refund","text":"Payment refund requested"},{"id":"doc-login","text":"Account login question"}]
  output:   "READY: ESCALATE (1 flag): flag:payment refund requested | ok:account login question"
```

Use the corresponding full module reference, for example:

```text
examples.workflow_creation.advanced_stable_map:workflow
```

## Replay-addressable identity

Assigning a module to a workflow attribute gives one occurrence a default
`module_id`. The expert example also reuses one `PolishReport` instance. Reuse
requires an explicit stable position for every occurrence:

```python
draft = self.polish.at("draft")(routed)
return self.polish.at("final")(ready)
```

The shared module aligns by its explicit `module_id`; `draft` and `final`
remain distinct `logical_step` values. For `map_over()`, a domain key such as a
document ID supplies stable execution identity. List position alone is never a
replay identity.
