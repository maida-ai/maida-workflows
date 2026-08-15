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
| Expert | `expert_replay_ready.py` | Map, parallel, branch, nesting, and explicit replay identity |

Every file exports three names:

```python
workflow  # importable workflow instance
EXAMPLE_INPUT  # deterministic JSON-safe input
EXPECTED_OUTPUT  # exact expected result
```

All handlers are offline and deterministic. They need no model, network call,
credential, or production connector.

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
