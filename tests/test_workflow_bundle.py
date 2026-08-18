from __future__ import annotations

import json
import stat
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

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
    WorkflowPortability,
    WorkflowRunner,
    WorkflowSpec,
    compile_workflow,
)
from maida.workflows._canonical import canonical_json, digest_data, type_schema
from maida.workflows.bundle import _fixed_aliases_by_digest
from maida.workflows.persistence import PostgresStore


class Upper(Module[str, str]):
    module_id = "text.upper"
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


def write_bundle(path: Path, data: dict[str, Any]) -> None:
    payload = dict(data)
    payload.pop("bundle_digest", None)
    payload["bundle_digest"] = digest_data(payload)
    path.write_text(canonical_json(payload))


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


def test_multistep_bundle_rebind_ignores_requirement_storage_order() -> None:
    spec = WorkflowSpec(
        workflow_id="bundle-multistep",
        input_schema=type_schema(str),
        output_schema=type_schema(str),
        nodes=(
            NodeSpec.task("first", "text.upper", BindingSpec.root()),
            NodeSpec.task("second", "text.upper", BindingSpec.node("first")),
        ),
        output=BindingSpec.node("second"),
    )
    bundle = WorkflowBundle.from_spec(spec, registry())

    rebound = bundle.bind(module_registry=registry())

    assert rebound.plan.digest == bundle.plan.digest


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


@pytest.mark.parametrize("limit", (0, True, 1.5))
def test_loading_rejects_invalid_size_limits(tmp_path: Path, limit: Any) -> None:
    path = tmp_path / "bundle.maida-workflow"
    path.write_text("{}")

    with pytest.raises(ValueError, match="positive integer"):
        WorkflowBundle.load(path, max_bytes=limit)


def test_loading_rejects_missing_non_json_and_non_object_files(tmp_path: Path) -> None:
    with pytest.raises(WorkflowBundleError, match="cannot read"):
        WorkflowBundle.load(tmp_path / "missing.maida-workflow")

    invalid_utf8 = tmp_path / "invalid-utf8.maida-workflow"
    invalid_utf8.write_bytes(b"\xff")
    with pytest.raises(WorkflowBundleError, match="UTF-8 JSON"):
        WorkflowBundle.load(invalid_utf8)

    root_array = tmp_path / "array.maida-workflow"
    root_array.write_text("[]")
    with pytest.raises(WorkflowBundleError, match="root"):
        WorkflowBundle.load(root_array)


def test_bundle_constructor_rejects_incoherent_portability_and_identity() -> None:
    valid = WorkflowBundle.from_spec(portable_spec(), registry())

    with pytest.raises(WorkflowBundleError, match="version"):
        replace(valid, version="9.0.0")
    with pytest.raises(WorkflowBundleError, match="editable spec"):
        replace(valid, spec=None)
    with pytest.raises(WorkflowBundleError, match="cannot claim"):
        replace(valid, portability=WorkflowPortability.FACTORY_BOUND)
    with pytest.raises(WorkflowBundleError, match="identities"):
        replace(valid, spec=replace(portable_spec(), workflow_id="different"))


def test_from_spec_reports_compilation_diagnostics() -> None:
    invalid = WorkflowSpec(
        workflow_id="bundle-invalid",
        input_schema=type_schema(str),
        output_schema=type_schema(str),
        nodes=(NodeSpec.task("missing", "unknown.module", BindingSpec.root()),),
        output=BindingSpec.node("missing"),
    )

    with pytest.raises(WorkflowBundleError, match="UNKNOWN_MODULE"):
        WorkflowBundle.from_spec(invalid, registry())


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("format", "other", "format or version"),
        ("plan", [], "plan must be an object"),
        ("spec", [], "spec must be an object"),
        ("portability", "unknown", "contract is invalid"),
        ("bindings", {}, "bindings must be an array"),
        ("bindings", [1], "bindings must be an array"),
        ("provenance", [], "provenance must be an object"),
        ("workflow_id", "other", "workflow identity"),
        ("definition_digest", "0" * 64, "definition digest"),
        ("spec_digest", "0" * 64, "spec digest"),
    ),
)
def test_loading_rejects_each_authenticated_bundle_contract(
    tmp_path: Path,
    field: str,
    value: Any,
    message: str,
) -> None:
    data = WorkflowBundle.from_spec(portable_spec(), registry()).to_dict()
    data[field] = value
    path = tmp_path / f"invalid-{field}.maida-workflow"
    write_bundle(path, data)

    with pytest.raises(WorkflowBundleError, match=message):
        WorkflowBundle.load(path)


def test_loading_rejects_noncanonical_binding_order(tmp_path: Path) -> None:
    spec = WorkflowSpec(
        workflow_id="bundle-order",
        input_schema=type_schema(str),
        output_schema=type_schema(str),
        nodes=(
            NodeSpec.task("first", "text.upper", BindingSpec.root()),
            NodeSpec.task("second", "text.upper", BindingSpec.node("first")),
        ),
        output=BindingSpec.node("second"),
    )
    data = WorkflowBundle.from_spec(spec, registry()).to_dict()
    data["bindings"] = list(reversed(data["bindings"]))
    path = tmp_path / "unordered-bindings.maida-workflow"
    write_bundle(path, data)

    with pytest.raises(WorkflowBundleError, match="canonical contract"):
        WorkflowBundle.load(path)


def test_factory_bound_python_workflow_requires_its_exact_catalog() -> None:
    bundle = WorkflowBundle.from_workflow(UpperWorkflow())

    assert bundle.spec is None
    assert bundle.portability.value == "factory-bound"
    with pytest.raises(WorkflowBundleError, match="catalog"):
        bundle.bind(module_registry=ModuleRegistry())

    rebound = bundle.bind(workflow_catalog=WorkflowCatalog([UpperWorkflow]))
    assert rebound.plan.digest == bundle.plan.digest

    with pytest.raises(WorkflowBundleError, match="cannot rebind"):
        bundle.bind(workflow_catalog=WorkflowCatalog())


def test_canonical_plan_bundle_requires_complete_trusted_module_identity() -> None:
    plan = compile_workflow(UpperWorkflow())
    bundle = WorkflowBundle.from_plan(plan, registry())

    assert bundle.bind(module_registry=registry()).plan.canonical_json() == plan.canonical_json()
    with pytest.raises(TypeError, match="PlanIR"):
        WorkflowBundle.from_plan(cast(Any, {}), registry())
    with pytest.raises(WorkflowBundleError, match="cannot bind"):
        WorkflowBundle.from_plan(plan, ModuleRegistry())
    with pytest.raises(WorkflowBundleError, match="trusted module registry"):
        bundle.bind()

    step = plan.executable_steps[0]
    incomplete = replace(plan, steps=(replace(step, module_digest=None),))
    with pytest.raises(WorkflowBundleError, match="incomplete module identity"):
        WorkflowBundle.from_plan(incomplete, registry())


def test_factory_bound_bundle_rejects_catalog_definition_drift() -> None:
    bundle = WorkflowBundle.from_workflow(UpperWorkflow())

    class WrongCatalog:
        def resolve(self, definition_digest: str) -> Workflow[str, str]:
            del definition_digest

            class LowerWorkflow(UpperWorkflow):
                workflow_id = "bundle-lower"

            return LowerWorkflow()

    with pytest.raises(WorkflowBundleError, match="changed the exact definition"):
        bundle.bind(workflow_catalog=cast(WorkflowCatalog, WrongCatalog()))


def test_reconstructable_bundle_rejects_registry_behavior_drift() -> None:
    bundle = WorkflowBundle.from_spec(portable_spec(), registry())

    class ChangedUpper(Upper):
        async def execute(self, value: str, ctx: ExecutionContext) -> str:
            return value.lower()

    wrong = ModuleRegistry(modules={"text.upper": ChangedUpper})

    with pytest.raises(WorkflowBundleError, match="rebind"):
        bundle.bind(module_registry=wrong)

    with pytest.raises(WorkflowBundleError, match="trusted module registry"):
        bundle.bind()


def test_factory_bundle_alias_annotations_are_unique_and_optional() -> None:
    one = registry()
    aliases = _fixed_aliases_by_digest(one)
    bundle = WorkflowBundle.from_workflow(UpperWorkflow(), one)

    assert aliases
    assert bundle.binding_requirements[0]["alias"] == "text.upper"

    ambiguous = ModuleRegistry(modules={"text.upper": Upper, "text.loud": Upper})
    assert _fixed_aliases_by_digest(ambiguous) == {}


def test_atomic_save_removes_temporary_file_after_replace_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = WorkflowBundle.from_spec(portable_spec(), registry())
    destination = tmp_path / "nested" / "bundle.maida-workflow"

    def fail_replace(source: Path, target: Path) -> None:
        del source, target
        raise OSError("replace failed")

    monkeypatch.setattr("maida.workflows.bundle.os.replace", fail_replace)
    with pytest.raises(OSError, match="replace failed"):
        bundle.save(destination)

    assert not destination.exists()
    assert list(destination.parent.iterdir()) == []


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
