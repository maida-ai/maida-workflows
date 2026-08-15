from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest

from maida.workflows import (
    BindingSpec,
    ExecutionContext,
    Module,
    ModuleRegistry,
    NodeSpec,
    RuntimeValue,
    Workflow,
    WorkflowBundle,
    WorkflowBundleError,
    WorkflowCatalog,
    WorkflowRunner,
    WorkflowSpec,
)
from maida.workflows._canonical import type_schema
from maida.workflows.persistence import PostgresStore


class Upper(Module[str, str]):
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value.upper()


class UpperWorkflow(Workflow[str, str]):
    workflow_id = "bundle-upper"
    input_type = str
    output_type = str
    upper = Upper()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.upper(value)


def registry() -> ModuleRegistry:
    return ModuleRegistry(modules={"text.upper": Upper})


def portable_spec() -> WorkflowSpec:
    return WorkflowSpec(
        workflow_id="bundle-spec-upper",
        input_schema=type_schema(str),
        output_schema=type_schema(str),
        nodes=(NodeSpec.task("upper", "text.upper", BindingSpec.root()),),
        output=BindingSpec.node("upper"),
    )


def test_spec_bundle_is_deterministic_private_and_exactly_rebindable(tmp_path: Path) -> None:
    first = WorkflowBundle.from_spec(portable_spec(), registry())
    second = WorkflowBundle.from_spec(portable_spec(), registry())
    path = tmp_path / "greeting.maida-workflow"

    first.save(path)
    loaded = WorkflowBundle.load(path)
    rebound = loaded.bind(module_registry=registry())

    assert first.digest == second.digest == loaded.digest
    assert first.canonical_json() == second.canonical_json() == path.read_text()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert rebound.plan.canonical_json() == first.plan.canonical_json()
    assert loaded.portability.value == "reconstructable"


def test_loading_rejects_tampering_unknown_fields_duplicates_and_noncanonical_data(
    tmp_path: Path,
) -> None:
    bundle = WorkflowBundle.from_spec(portable_spec(), registry())
    data = bundle.to_dict()

    tampered = dict(data)
    tampered["definition_digest"] = "0" * 64
    tampered_path = tmp_path / "tampered.maida-workflow"
    tampered_path.write_text(json.dumps(tampered, sort_keys=True, separators=(",", ":")))
    with pytest.raises(WorkflowBundleError, match="digest"):
        WorkflowBundle.load(tampered_path)

    unknown = dict(data)
    unknown["python_import"] = "unsafe.module:object"
    unknown_path = tmp_path / "unknown.maida-workflow"
    unknown_path.write_text(json.dumps(unknown, sort_keys=True, separators=(",", ":")))
    with pytest.raises(WorkflowBundleError, match="fields"):
        WorkflowBundle.load(unknown_path)

    duplicate_path = tmp_path / "duplicate.maida-workflow"
    duplicate_path.write_text('{"format":"maida-workflow","format":"other"}')
    with pytest.raises(WorkflowBundleError, match="duplicate"):
        WorkflowBundle.load(duplicate_path)

    pretty_path = tmp_path / "pretty.maida-workflow"
    pretty_path.write_text(json.dumps(data, indent=2))
    with pytest.raises(WorkflowBundleError, match="canonical"):
        WorkflowBundle.load(pretty_path)


def test_loading_enforces_a_size_limit_before_parsing(tmp_path: Path) -> None:
    path = tmp_path / "large.maida-workflow"
    path.write_bytes(b"{}" * 100)

    with pytest.raises(WorkflowBundleError, match="maximum"):
        WorkflowBundle.load(path, max_bytes=32)


def test_factory_bound_python_workflow_requires_its_exact_catalog() -> None:
    bundle = WorkflowBundle.from_workflow(UpperWorkflow())

    assert bundle.spec is None
    assert bundle.portability.value == "factory-bound"
    with pytest.raises(WorkflowBundleError, match="catalog"):
        bundle.bind(module_registry=ModuleRegistry())

    rebound = bundle.bind(workflow_catalog=WorkflowCatalog([UpperWorkflow]))
    assert rebound.plan.digest == bundle.plan.digest


def test_reconstructable_bundle_rejects_registry_behavior_drift() -> None:
    bundle = WorkflowBundle.from_spec(portable_spec(), registry())

    class ChangedUpper(Upper):
        async def execute(self, value: str, ctx: ExecutionContext) -> str:
            return value.lower()

    wrong = ModuleRegistry(modules={"text.upper": ChangedUpper})

    with pytest.raises(WorkflowBundleError, match="rebind"):
        bundle.bind(module_registry=wrong)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_loaded_bundle_runs_on_the_durable_runtime(
    postgres_store: PostgresStore,
    tmp_path: Path,
) -> None:
    path = tmp_path / "run.maida-workflow"
    WorkflowBundle.from_spec(portable_spec(), registry()).save(path)
    bound = WorkflowBundle.load(path).bind(module_registry=registry())

    result = await WorkflowRunner(postgres_store).run(bound, "Margaret")

    assert result.output == "MARGARET"
