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
