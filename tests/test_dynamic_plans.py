from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from typing import Any

import pytest

from maida.workflows import (
    Capability,
    Connector,
    ExecutionContext,
    Module,
    ModuleCatalog,
    PlanFragmentIR,
    PlanLimits,
    PlanNode,
    PlanSignature,
    PlanValidationError,
    PlanValidator,
    RuntimeValue,
    Workflow,
    compile_workflow,
)
from maida.workflows._canonical import schema_digest

TEXT = schema_digest(str)
NUMBER = schema_digest(int)
FLAG = schema_digest(bool)
PROCESS_EXECUTION: dict[str, Any] = {
    "capabilities": [],
    "cpu": None,
    "dependency_lock": None,
    "image": None,
    "isolation": "process",
    "memory": None,
}
_EXPECTED_SUPERSEDES_UNSET = object()


class TextModule(Module[str, str]):
    module_id = "modules.text"
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value


class TextWorkflow(Workflow[str, str]):
    workflow_id = "catalog-source"
    input_type = str
    output_type = str
    text = TextModule()

    def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
        return self.text.at("text")(value)


def _allow(
    catalog: ModuleCatalog,
    alias: str,
    module_id: str,
    digest_character: str,
    input_schemas: tuple[str, ...],
    output_schema: str,
) -> ModuleCatalog:
    return catalog.allow(
        alias,
        module_id=module_id,
        module_digest=digest_character * 64,
        input_schema_digests=input_schemas,
        output_schema_digest=output_schema,
        execution=PROCESS_EXECUTION,
        capabilities=(),
        effects=(),
    )


def _catalog() -> ModuleCatalog:
    catalog = _allow(ModuleCatalog(), "source", "modules.source", "1", (TEXT,), TEXT)
    catalog = _allow(catalog, "left", "modules.left", "2", (TEXT,), TEXT)
    catalog = _allow(catalog, "right", "modules.right", "3", (TEXT,), NUMBER)
    return _allow(catalog, "join", "modules.join", "4", (TEXT, NUMBER), FLAG)


def _fragment(
    *,
    fragment_id: str = "research-plan",
    revision: int = 1,
    supersedes: str | None = None,
) -> PlanFragmentIR:
    # Generated data contains only planner-controlled graph choices. Catalog
    # pins and type contracts are deliberately absent.
    return PlanFragmentIR(
        fragment_id=fragment_id,
        revision=revision,
        supersedes=supersedes,
        nodes=(
            PlanNode("join", "join", ("left", "right")),
            PlanNode("right", "right", ("source",)),
            PlanNode("source", "source", ("$input",)),
            PlanNode("left", "left", ("source",)),
        ),
        outputs=("join",),
    )


def _validator(
    *,
    limits: PlanLimits | None = None,
    budget_check: Any = None,
) -> PlanValidator:
    return PlanValidator(
        _catalog(),
        limits or PlanLimits(max_nodes=8, max_depth=5, max_fanout=3, max_replans=2),
        budget_check=budget_check or (lambda _signature: None),
    )


def _validate(
    validator: PlanValidator,
    fragment: PlanFragmentIR,
    *,
    input_schema: str = TEXT,
    output_schemas: tuple[str, ...] = (FLAG,),
    expected_revision: int | None = None,
    expected_supersedes: object | str | None = _EXPECTED_SUPERSEDES_UNSET,
) -> PlanSignature:
    revision = fragment.revision if expected_revision is None else expected_revision
    supersedes = (
        fragment.supersedes
        if expected_supersedes is _EXPECTED_SUPERSEDES_UNSET
        else expected_supersedes
    )
    return validator.validate(
        fragment,
        region_input_schema_digest=input_schema,
        expected_output_schema_digests=output_schemas,
        expected_revision=revision,
        expected_supersedes=supersedes,  # type: ignore[arg-type]
    )


def test_valid_fragment_is_minimal_canonical_and_resolved_by_trusted_validation() -> None:
    fragment = _fragment()
    signature = _validate(_validator(), fragment)

    assert fragment.version == "0.1.0"
    assert [node.key for node in fragment.nodes] == ["join", "left", "right", "source"]
    assert (
        fragment.canonical_json() == PlanFragmentIR.from_dict(fragment.to_dict()).canonical_json()
    )
    assert fragment.digest == PlanFragmentIR.from_dict(fragment.to_dict()).digest
    assert signature == PlanSignature.from_dict(signature.to_dict())
    assert signature.node_count == 4
    assert signature.max_depth == 3
    assert signature.max_fanout == 2
    assert signature.module_composition == (
        ("join", 1),
        ("left", 1),
        ("right", 1),
        ("source", 1),
    )
    assert signature.outputs == ("join",)
    assert len(signature.topology_digest) == 64
    assert len(signature.digest) == 64
    resolved_source = next(node for node in signature.resolved_nodes if node["key"] == "source")
    assert resolved_source["module_id"] == "modules.source"
    assert resolved_source["module_digest"] == "1" * 64
    assert resolved_source["input_schema_digests"] == (TEXT,)
    assert resolved_source["output_schema_digest"] == TEXT

    generated = fragment.to_dict()
    assert set(generated) == {
        "fragment_id",
        "nodes",
        "outputs",
        "revision",
        "supersedes",
        "version",
    }
    assert set(generated["nodes"][0]) == {"dependencies", "key", "module_alias"}


def test_behavioral_signature_ignores_fragment_label_but_tracks_resolved_behavior() -> None:
    original = _fragment(fragment_id="planner-output-1")
    relabeled = _fragment(fragment_id="planner-output-2")
    validator = _validator()

    original_signature = _validate(validator, original)
    relabeled_signature = _validate(validator, relabeled)
    assert original.digest != relabeled.digest
    assert original_signature == relabeled_signature

    changed_catalog = _allow(_catalog(), "changed-left", "modules.left", "9", (TEXT,), TEXT)
    changed = replace(
        original,
        nodes=tuple(
            replace(node, module_alias="changed-left") if node.key == "left" else node
            for node in original.nodes
        ),
    )
    changed_validator = PlanValidator(
        changed_catalog,
        PlanLimits(max_nodes=8, max_depth=5, max_fanout=3, max_replans=2),
        budget_check=lambda _signature: None,
    )
    assert _validate(changed_validator, changed) != original_signature


def test_resolved_signature_is_deeply_immutable() -> None:
    signature = _validate(_validator(), _fragment())
    original_digest = signature.digest
    resolved = signature.resolved_nodes[0]

    with pytest.raises(TypeError):
        resolved["module_id"] = "changed"  # type: ignore[index]
    with pytest.raises(TypeError):
        resolved["execution"]["isolation"] = "vm"
    with pytest.raises(AttributeError):
        resolved["input_schema_digests"].append(TEXT)
    assert signature.digest == original_digest


def test_catalog_is_the_only_source_of_module_pins_schemas_and_execution_metadata() -> None:
    catalog = ModuleCatalog()
    allowed = _allow(catalog, "text", "modules.text", "a", (TEXT,), TEXT)

    assert catalog.aliases == ()
    assert allowed.aliases == ("text",)
    assert allowed.resolve("text") == {
        "capabilities": [],
        "effects": [],
        "execution": PROCESS_EXECUTION,
        "input_schema_digests": [TEXT],
        "module_digest": "a" * 64,
        "module_id": "modules.text",
        "output_schema_digest": TEXT,
    }
    with pytest.raises(ValueError, match="already registered"):
        _allow(allowed, "text", "modules.text", "a", (TEXT,), TEXT)
    with pytest.raises(KeyError, match="not allowlisted"):
        allowed.resolve("missing")
    with pytest.raises(ValueError, match="alias"):
        _allow(catalog, "$unsafe", "modules.text", "a", (TEXT,), TEXT)


def test_catalog_defensively_copies_and_freezes_trusted_descriptors() -> None:
    execution = dict(PROCESS_EXECUTION)
    capabilities = ["local"]
    execution["capabilities"] = capabilities
    catalog = ModuleCatalog().allow(
        "text",
        module_id="modules.text",
        module_digest="a" * 64,
        input_schema_digests=(TEXT,),
        output_schema_digest=TEXT,
        execution=execution,
    )
    original_digest = catalog.digest

    execution["isolation"] = "vm"
    capabilities[0] = "changed"
    descriptor = catalog.resolve("text")
    descriptor["execution"]["isolation"] = "vm"

    assert catalog.resolve("text")["execution"]["isolation"] == "process"
    assert catalog.digest == original_digest


@pytest.mark.parametrize(
    ("kwargs", "message"),
    (
        ({"module_id": ""}, "module_id"),
        ({"input_schema_digests": TEXT}, "ordered sequence"),
        ({"execution": "process"}, "ExecutionSpec mapping"),
        ({"execution": {**PROCESS_EXECUTION, "unknown": True}}, "ExecutionSpec contract"),
        ({"execution": {**PROCESS_EXECUTION, "cpu": True}}, "ExecutionSpec contract"),
    ),
)
def test_catalog_rejects_invalid_trusted_descriptors(
    kwargs: dict[str, Any],
    message: str,
) -> None:
    descriptor: dict[str, Any] = {
        "module_id": "modules.text",
        "module_digest": "a" * 64,
        "input_schema_digests": (TEXT,),
        "output_schema_digest": TEXT,
        "execution": PROCESS_EXECUTION,
    }
    descriptor.update(kwargs)

    with pytest.raises(ValueError, match=message):
        ModuleCatalog().allow("text", **descriptor)


def test_catalog_projects_static_steps_and_retains_credential_free_access_contracts() -> None:
    lookup = Capability(
        "records.lookup",
        connector="records",
        operation="lookup",
        input_type=str,
        output_type=str,
        connector_version="adapter-v1",
    )

    class AccessWorkflow(Workflow[str, str]):
        workflow_id = "catalog-access"
        input_type = str
        output_type = str
        read = Connector(lookup, module_id="modules.records")

        def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
            return self.read.at("lookup")(value)

    plan = compile_workflow(AccessWorkflow())
    replay_key = plan.executable_steps[0].replay_key
    assert replay_key is not None
    catalog = ModuleCatalog.from_plan(plan, {"lookup": replay_key})
    descriptor = catalog.resolve("lookup")

    assert descriptor["module_id"] == "modules.records"
    assert descriptor["capabilities"] == [lookup.to_data()]
    assert descriptor["effects"] == []
    assert descriptor["execution"] == PROCESS_EXECUTION
    assert "credentials" not in descriptor
    assert "grants" not in descriptor
    with pytest.raises(ValueError, match="replay key"):
        ModuleCatalog.from_plan(plan, {"missing": None})  # type: ignore[dict-item]


def test_validator_rejects_unknown_alias_and_trusted_contract_mismatches() -> None:
    fragment = _fragment()
    source = next(node for node in fragment.nodes if node.key == "source")
    unknown = replace(
        fragment,
        nodes=tuple(
            replace(source, module_alias="unknown") if node.key == "source" else node
            for node in fragment.nodes
        ),
    )
    with pytest.raises(PlanValidationError, match="not allowlisted"):
        _validate(_validator(), unknown)
    with pytest.raises(PlanValidationError, match="region input schema"):
        _validate(_validator(), fragment, input_schema=NUMBER)
    with pytest.raises(PlanValidationError, match="output schema"):
        _validate(_validator(), fragment, output_schemas=(TEXT,))
    with pytest.raises(PlanValidationError, match="output contract count"):
        _validate(_validator(), fragment, output_schemas=(FLAG, FLAG))


def test_topology_requires_existing_unique_ordered_typed_dependencies() -> None:
    fragment = _fragment()
    left = next(node for node in fragment.nodes if node.key == "left")

    for changed, message in (
        (replace(left, dependencies=("missing",)), "does not exist"),
        (replace(left, dependencies=("source", "source")), "duplicate dependency"),
        (replace(left, dependencies=()), "input count"),
    ):
        malformed = replace(
            fragment,
            nodes=tuple(changed if node.key == "left" else node for node in fragment.nodes),
        )
        with pytest.raises(PlanValidationError, match=message):
            _validate(_validator(), malformed)

    join = next(node for node in fragment.nodes if node.key == "join")
    reversed_dependencies = replace(join, dependencies=("right", "left"))
    malformed = replace(
        fragment,
        nodes=tuple(
            reversed_dependencies if node.key == "join" else node for node in fragment.nodes
        ),
    )
    with pytest.raises(PlanValidationError, match="edge schema"):
        _validate(_validator(), malformed)


def test_topology_rejects_cycles_duplicate_keys_and_invalid_outputs() -> None:
    fragment = _fragment()
    source = next(node for node in fragment.nodes if node.key == "source")
    cycle_source = replace(source, dependencies=("left",))
    cyclic = replace(
        fragment,
        nodes=tuple(cycle_source if node.key == "source" else node for node in fragment.nodes),
    )
    with pytest.raises(PlanValidationError, match="acyclic"):
        _validate(_validator(), cyclic)

    duplicate = replace(fragment, nodes=(*fragment.nodes, fragment.nodes[0]))
    with pytest.raises(PlanValidationError, match="duplicate node key"):
        _validate(_validator(), duplicate)
    with pytest.raises(PlanValidationError, match=r"output.*does not exist"):
        _validate(_validator(), replace(fragment, outputs=("missing",)))
    with pytest.raises(PlanValidationError, match="duplicate output"):
        _validate(_validator(), replace(fragment, outputs=("join", "join")))
    with pytest.raises(PlanValidationError, match="at least one output"):
        _validate(_validator(), replace(fragment, outputs=()))
    with pytest.raises(PlanValidationError, match="at least one node"):
        _validate(_validator(), replace(fragment, nodes=()))


@pytest.mark.parametrize(
    ("limits", "revision", "supersedes", "message"),
    (
        (PlanLimits(max_nodes=3, max_depth=5, max_fanout=3, max_replans=2), 1, None, "node"),
        (PlanLimits(max_nodes=8, max_depth=2, max_fanout=3, max_replans=2), 1, None, "depth"),
        (PlanLimits(max_nodes=8, max_depth=5, max_fanout=1, max_replans=2), 1, None, "fanout"),
        (PlanLimits(max_nodes=8, max_depth=5, max_fanout=3, max_replans=0), 2, "a" * 64, "replan"),
    ),
)
def test_plan_limits_fail_closed(
    limits: PlanLimits, revision: int, supersedes: str | None, message: str
) -> None:
    fragment = _fragment(revision=revision, supersedes=supersedes)
    with pytest.raises(PlanValidationError, match=message):
        _validate(_validator(limits=limits), fragment)


def test_revision_lineage_is_explicit_and_checked_against_trusted_state() -> None:
    initial = _fragment()
    revised = _fragment(revision=2, supersedes=initial.digest)
    signature = _validate(_validator(), revised)
    assert signature.revision == 2
    assert signature.supersedes == initial.digest

    with pytest.raises(PlanValidationError, match="revision"):
        _validate(_validator(), revised, expected_revision=3)
    with pytest.raises(PlanValidationError, match="supersedes"):
        _validate(_validator(), revised, expected_supersedes="f" * 64)
    with pytest.raises(PlanValidationError, match=r"initial.*supersedes"):
        replace(initial, supersedes="a" * 64)
    with pytest.raises(PlanValidationError, match=r"revised.*supersedes"):
        replace(revised, supersedes=None)
    with pytest.raises(PlanValidationError, match="at least one"):
        _fragment(revision=0)


def test_plan_limits_reject_invalid_configuration() -> None:
    with pytest.raises(ValueError, match="max_nodes"):
        PlanLimits(max_nodes=0, max_depth=1, max_fanout=1, max_replans=0)
    with pytest.raises(ValueError, match="max_depth"):
        PlanLimits(max_nodes=1, max_depth=0, max_fanout=1, max_replans=0)
    with pytest.raises(ValueError, match="max_fanout"):
        PlanLimits(max_nodes=1, max_depth=1, max_fanout=-1, max_replans=0)
    with pytest.raises(ValueError, match="max_replans"):
        PlanLimits(max_nodes=1, max_depth=1, max_fanout=1, max_replans=-1)
    with pytest.raises(ValueError, match="integer"):
        PlanLimits(max_nodes=True, max_depth=1, max_fanout=1, max_replans=0)


def test_budget_validation_is_a_required_external_seam_over_resolved_behavior() -> None:
    checked: list[str] = []

    def accept(signature: PlanSignature) -> None:
        checked.append(signature.topology_digest)

    signature = _validate(_validator(budget_check=accept), _fragment())
    assert checked == [signature.topology_digest]

    def reject(_signature: PlanSignature) -> None:
        raise PlanValidationError("BUDGET_EXCEEDED", "trusted live budget policy rejected plan")

    with pytest.raises(PlanValidationError, match="budget policy") as rejected:
        _validate(_validator(budget_check=reject), _fragment())
    assert rejected.value.code == "BUDGET_EXCEEDED"

    def broken(_signature: PlanSignature) -> None:
        raise RuntimeError("policy unavailable")

    with pytest.raises(PlanValidationError, match="budget validation failed"):
        _validate(_validator(budget_check=broken), _fragment())

    def invalid_return(_signature: PlanSignature) -> bool:
        return True

    with pytest.raises(PlanValidationError, match="must return None"):
        _validate(_validator(budget_check=invalid_return), _fragment())
    with pytest.raises(TypeError, match="budget_check"):
        PlanValidator(
            _catalog(),
            PlanLimits(max_nodes=8, max_depth=5, max_fanout=3, max_replans=2),
            budget_check=None,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "forbidden_field",
    (
        "module_id",
        "module_digest",
        "input_schema_digest",
        "output_schema_digest",
        "execution",
        "code",
        "import_path",
        "credentials",
        "capability_grant",
        "effects",
        "budget",
    ),
)
def test_strict_import_rejects_trusted_or_executable_node_fields(forbidden_field: str) -> None:
    encoded = _fragment().to_dict()
    encoded["nodes"][0][forbidden_field] = "forbidden"

    with pytest.raises(PlanValidationError, match="fields"):
        PlanFragmentIR.from_dict(encoded)


def test_strict_import_rejects_versions_shapes_and_noncanonical_node_order() -> None:
    encoded = _fragment().to_dict()
    with pytest.raises(PlanValidationError, match="unsupported"):
        PlanFragmentIR.from_dict(dict(encoded, version="9.0.0"))
    with pytest.raises(PlanValidationError, match="fields"):
        PlanFragmentIR.from_dict(dict(encoded, runtime={}))
    with pytest.raises(PlanValidationError, match="nodes must be an array"):
        PlanFragmentIR.from_dict(dict(encoded, nodes="not-an-array"))

    reordered = _fragment().to_dict()
    reordered["nodes"] = list(reversed(reordered["nodes"]))
    with pytest.raises(PlanValidationError, match="canonical key order"):
        PlanFragmentIR.from_dict(reordered)

    malformed_lineage = _fragment(revision=2, supersedes="a" * 64).to_dict()
    malformed_lineage["supersedes"] = "not-a-digest"
    with pytest.raises(PlanValidationError, match="sha256"):
        PlanFragmentIR.from_dict(malformed_lineage)

    with pytest.raises(PlanValidationError, match="unsupported"):
        replace(_fragment(), version="9.0.0")
    with pytest.raises(PlanValidationError, match="PlanNode"):
        replace(_fragment(), nodes=("not-a-node",))  # type: ignore[arg-type]
    with pytest.raises(PlanValidationError, match="ordered sequence"):
        PlanNode("unsafe", "source", "source")  # type: ignore[arg-type]


def test_strict_fragment_import_rejects_malformed_generated_values() -> None:
    encoded = _fragment().to_dict()
    with pytest.raises(PlanValidationError, match="PlanFragmentIR must be an object"):
        PlanFragmentIR.from_dict([])  # type: ignore[arg-type]
    with pytest.raises(PlanValidationError, match="PlanNode must be an object"):
        PlanNode.from_dict([])  # type: ignore[arg-type]
    with pytest.raises(PlanValidationError, match="dependencies must be an array"):
        PlanNode.from_dict({"key": "node", "module_alias": "source", "dependencies": "$input"})
    with pytest.raises(PlanValidationError, match="outputs must be an array"):
        PlanFragmentIR.from_dict(dict(encoded, outputs="join"))
    with pytest.raises(PlanValidationError, match="duplicate node key"):
        PlanFragmentIR.from_dict(dict(encoded, nodes=[*encoded["nodes"], encoded["nodes"][-1]]))
    with pytest.raises(PlanValidationError, match="integer"):
        PlanFragmentIR.from_dict(dict(encoded, revision="one"))


def test_signature_import_is_strict() -> None:
    signature = _validate(_validator(), _fragment())
    invalid = signature.to_dict()
    invalid["unknown"] = True
    with pytest.raises(PlanValidationError, match="fields"):
        PlanSignature.from_dict(invalid)

    noncanonical = signature.to_dict()
    noncanonical["module_composition"] = list(reversed(noncanonical["module_composition"]))
    with pytest.raises(PlanValidationError, match="canonical"):
        PlanSignature.from_dict(noncanonical)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("node_count", 99, "node count"),
        ("max_depth", 99, "depth"),
        ("max_fanout", 99, "fanout"),
        ("module_composition", [{"alias": "source", "count": 4}], "composition"),
        ("topology_digest", "f" * 64, "topology digest"),
        ("output_schema_digests", [TEXT], "output schemas"),
        ("outputs", ["source"], "output schemas"),
    ),
)
def test_signature_import_rejects_internally_inconsistent_behavior(
    field: str,
    value: Any,
    message: str,
) -> None:
    encoded = _validate(_validator(), _fragment()).to_dict()
    encoded[field] = value

    with pytest.raises(PlanValidationError, match=message):
        PlanSignature.from_dict(encoded)


def test_signature_import_rejects_invalid_shapes_lineage_and_descriptors() -> None:
    valid = _validate(_validator(), _fragment()).to_dict()

    cases: tuple[tuple[str, Any, str], ...] = (
        ("version", "9.0.0", "unsupported"),
        ("revision", 0, "at least one"),
        ("supersedes", "a" * 64, "initial signature supersedes"),
        ("module_composition", "source", "composition must be an array"),
        ("resolved_nodes", "source", "resolved_nodes must be an array"),
        ("outputs", "join", "outputs must be an array"),
        ("output_schema_digests", FLAG, "output schemas must be an array"),
    )
    for field, value, message in cases:
        encoded = deepcopy(valid)
        encoded[field] = value
        with pytest.raises(PlanValidationError, match=message) as rejected:
            PlanSignature.from_dict(encoded)
        assert rejected.value.code in {
            "PLAN_SIGNATURE_INVALID",
            "PLAN_SIGNATURE_VERSION_UNSUPPORTED",
        }

    revised_without_lineage = deepcopy(valid)
    revised_without_lineage["revision"] = 2
    with pytest.raises(PlanValidationError, match="requires supersedes"):
        PlanSignature.from_dict(revised_without_lineage)

    malformed_composition = deepcopy(valid)
    malformed_composition["module_composition"] = [{"alias": "source"}]
    with pytest.raises(PlanValidationError, match="composition fields"):
        PlanSignature.from_dict(malformed_composition)

    invalid_count = deepcopy(valid)
    invalid_count["module_composition"][0]["count"] = 0
    with pytest.raises(PlanValidationError, match="positive integers"):
        PlanSignature.from_dict(invalid_count)

    duplicate_composition = deepcopy(valid)
    duplicate_composition["module_composition"] = [
        duplicate_composition["module_composition"][0],
        duplicate_composition["module_composition"][0],
    ]
    with pytest.raises(PlanValidationError, match="duplicate aliases"):
        PlanSignature.from_dict(duplicate_composition)

    malformed_node = deepcopy(valid)
    malformed_node["resolved_nodes"][0] = "join"
    with pytest.raises(PlanValidationError, match="string keys"):
        PlanSignature.from_dict(malformed_node)

    duplicate_node = deepcopy(valid)
    duplicate_node["resolved_nodes"].insert(0, deepcopy(duplicate_node["resolved_nodes"][0]))
    with pytest.raises(PlanValidationError, match="duplicate keys"):
        PlanSignature.from_dict(duplicate_node)

    unknown_descriptor_field = deepcopy(valid)
    unknown_descriptor_field["resolved_nodes"][0]["credentials"] = {}
    with pytest.raises(PlanValidationError, match="fields"):
        PlanSignature.from_dict(unknown_descriptor_field)

    invalid_descriptor = deepcopy(valid)
    invalid_descriptor["resolved_nodes"][0]["module_digest"] = "invalid"
    with pytest.raises(PlanValidationError, match="sha256"):
        PlanSignature.from_dict(invalid_descriptor)


def test_signature_import_revalidates_resolved_graph_topology_and_schemas() -> None:
    valid = _validate(_validator(), _fragment()).to_dict()

    no_nodes = deepcopy(valid)
    no_nodes["resolved_nodes"] = []
    no_nodes["node_count"] = 0
    no_nodes["module_composition"] = []
    with pytest.raises(PlanValidationError, match="at least one node"):
        PlanSignature.from_dict(no_nodes)

    for outputs, message in (
        ([], "at least one output"),
        (["join", "join"], "duplicate keys"),
        (["missing"], "does not exist"),
    ):
        encoded = deepcopy(valid)
        encoded["outputs"] = outputs
        with pytest.raises(PlanValidationError, match=message):
            PlanSignature.from_dict(encoded)

    def node(encoded: dict[str, Any], key: str) -> dict[str, Any]:
        return next(item for item in encoded["resolved_nodes"] if item["key"] == key)

    duplicate_dependency = deepcopy(valid)
    node(duplicate_dependency, "left")["dependencies"] = ["source", "source"]
    with pytest.raises(PlanValidationError, match="duplicate dependency"):
        PlanSignature.from_dict(duplicate_dependency)

    wrong_input_count = deepcopy(valid)
    node(wrong_input_count, "left")["dependencies"] = []
    with pytest.raises(PlanValidationError, match="input count"):
        PlanSignature.from_dict(wrong_input_count)

    missing_dependency = deepcopy(valid)
    node(missing_dependency, "left")["dependencies"] = ["missing"]
    with pytest.raises(PlanValidationError, match="does not exist"):
        PlanSignature.from_dict(missing_dependency)

    wrong_edge_order = deepcopy(valid)
    node(wrong_edge_order, "join")["dependencies"] = ["right", "left"]
    with pytest.raises(PlanValidationError, match="edge schema"):
        PlanSignature.from_dict(wrong_edge_order)

    wrong_region_input = deepcopy(valid)
    wrong_region_input["region_input_schema_digest"] = NUMBER
    with pytest.raises(PlanValidationError, match="edge schema"):
        PlanSignature.from_dict(wrong_region_input)

    cycle = deepcopy(valid)
    node(cycle, "source")["dependencies"] = ["left"]
    with pytest.raises(PlanValidationError, match="acyclic"):
        PlanSignature.from_dict(cycle)
