from __future__ import annotations

import traceback
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest

from maida.workflows import (
    Budget,
    Capability,
    CapabilityGrant,
    Connector,
    EffectSpec,
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
from maida.workflows._canonical import digest_data, schema_digest

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
UNBOUNDED = Budget()
NO_ACCESS = CapabilityGrant()
NODE_BUDGET = Budget(
    wall_time=timedelta(seconds=5),
    model_tokens=10,
    tool_calls=1,
    cost_usd=0.10,
)
REGION_BUDGET = Budget(
    wall_time=timedelta(seconds=30),
    model_tokens=1_000,
    tool_calls=100,
    cost_usd=10.0,
)
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
    *,
    budget: Budget = NODE_BUDGET,
    capabilities: tuple[dict[str, Any], ...] = (),
    effects: tuple[dict[str, Any], ...] = (),
) -> ModuleCatalog:
    return catalog.allow(
        alias,
        module_id=module_id,
        module_digest=digest_character * 64,
        input_schema_digests=input_schemas,
        output_schema_digest=output_schema,
        execution=PROCESS_EXECUTION,
        budget=budget,
        capabilities=capabilities,
        effects=effects,
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
    catalog: ModuleCatalog | None = None,
    region_grant: CapabilityGrant = NO_ACCESS,
    approval_check: Any = None,
) -> PlanValidator:
    return PlanValidator(
        catalog or _catalog(),
        limits
        or PlanLimits(
            max_nodes=8,
            max_depth=5,
            max_fanout=3,
            max_replans=2,
            budget=REGION_BUDGET,
        ),
        region_id="workflow.dynamic",
        region_grant=region_grant,
        approval_check=approval_check,
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
        ("modules.join", "4" * 64, 1),
        ("modules.left", "2" * 64, 1),
        ("modules.right", "3" * 64, 1),
        ("modules.source", "1" * 64, 1),
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
        PlanLimits(
            max_nodes=8,
            max_depth=5,
            max_fanout=3,
            max_replans=2,
            budget=REGION_BUDGET,
        ),
        region_id="workflow.dynamic",
        region_grant=CapabilityGrant(),
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
        "budget": NODE_BUDGET.to_data(),
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
        budget=NODE_BUDGET,
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
        "budget": NODE_BUDGET,
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
    assert descriptor["budget"] == Budget().to_data()
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
        (
            PlanLimits(max_nodes=3, max_depth=5, max_fanout=3, max_replans=2, budget=REGION_BUDGET),
            1,
            None,
            "node",
        ),
        (
            PlanLimits(max_nodes=8, max_depth=2, max_fanout=3, max_replans=2, budget=REGION_BUDGET),
            1,
            None,
            "depth",
        ),
        (
            PlanLimits(max_nodes=8, max_depth=5, max_fanout=1, max_replans=2, budget=REGION_BUDGET),
            1,
            None,
            "fanout",
        ),
        (
            PlanLimits(max_nodes=8, max_depth=5, max_fanout=3, max_replans=0, budget=REGION_BUDGET),
            2,
            "a" * 64,
            "replan",
        ),
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


def test_lineage_changes_preserve_behavioral_equality_and_digest() -> None:
    initial = _fragment()
    revised = _fragment(revision=2, supersedes=initial.digest)
    initial_signature = _validate(_validator(), initial)
    revised_signature = _validate(_validator(), revised)

    assert initial_signature == revised_signature
    assert initial_signature.topology_digest == revised_signature.topology_digest
    assert initial_signature.digest == revised_signature.digest
    assert initial_signature.canonical_json() != revised_signature.canonical_json()


def test_plan_limits_reject_invalid_configuration() -> None:
    with pytest.raises(ValueError, match="max_nodes"):
        PlanLimits(max_nodes=0, max_depth=1, max_fanout=1, max_replans=0, budget=UNBOUNDED)
    with pytest.raises(ValueError, match="max_depth"):
        PlanLimits(max_nodes=1, max_depth=0, max_fanout=1, max_replans=0, budget=UNBOUNDED)
    with pytest.raises(ValueError, match="max_fanout"):
        PlanLimits(max_nodes=1, max_depth=1, max_fanout=-1, max_replans=0, budget=UNBOUNDED)
    with pytest.raises(ValueError, match="max_replans"):
        PlanLimits(max_nodes=1, max_depth=1, max_fanout=1, max_replans=-1, budget=UNBOUNDED)
    with pytest.raises(ValueError, match="integer"):
        PlanLimits(max_nodes=True, max_depth=1, max_fanout=1, max_replans=0, budget=UNBOUNDED)


def test_budget_and_region_authority_are_explicit_trusted_inputs() -> None:
    with pytest.raises(TypeError, match="budget"):
        PlanLimits(  # type: ignore[call-arg]
            max_nodes=8,
            max_depth=5,
            max_fanout=3,
            max_replans=2,
        )
    with pytest.raises(TypeError, match="region_grant"):
        PlanValidator(
            _catalog(),
            PlanLimits(
                max_nodes=8,
                max_depth=5,
                max_fanout=3,
                max_replans=2,
                budget=REGION_BUDGET,
            ),
            region_id="workflow.dynamic",
            region_grant=None,  # type: ignore[arg-type]
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
        (
            "module_composition",
            [{"module_id": "modules.source", "module_digest": "1" * 64, "count": 4}],
            "composition",
        ),
        ("topology_digest", "f" * 64, "topology digest"),
        ("output_schema_digests", [TEXT], "output schemas"),
        ("outputs", ["source"], "unreachable"),
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
        ("output_schema_digests", FLAG, "output_schema_digests must be an array"),
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
    malformed_composition["module_composition"] = [{"module_id": "modules.source"}]
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
    with pytest.raises(PlanValidationError, match="duplicate pins"):
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


def _single_fragment(alias: str = "unit") -> PlanFragmentIR:
    return PlanFragmentIR(
        fragment_id="single-plan",
        revision=1,
        supersedes=None,
        nodes=(PlanNode("unit", alias, ("$input",)),),
        outputs=("unit",),
    )


def _single_catalog(
    *,
    alias: str = "unit",
    digest_character: str = "a",
    budget: Budget = NODE_BUDGET,
    capabilities: tuple[dict[str, Any], ...] = (),
    effects: tuple[dict[str, Any], ...] = (),
) -> ModuleCatalog:
    return _allow(
        ModuleCatalog(),
        alias,
        "modules.unit",
        digest_character,
        (TEXT,),
        TEXT,
        budget=budget,
        capabilities=capabilities,
        effects=effects,
    )


def test_every_node_must_be_reverse_reachable_from_an_output() -> None:
    hidden_effect = EffectSpec(
        "hidden.write",
        connector="hidden",
        operation="write",
        input_type=str,
        output_type=str,
    ).to_data()
    catalog = _allow(
        _catalog(),
        "hidden",
        "modules.hidden",
        "5",
        (TEXT,),
        TEXT,
        effects=(hidden_effect,),
    )
    fragment = replace(
        _fragment(),
        nodes=(*_fragment().nodes, PlanNode("orphan", "hidden", ("$input",))),
    )
    with pytest.raises(PlanValidationError, match="unreachable from outputs") as rejected:
        _validate(
            _validator(
                catalog=catalog,
                region_grant=CapabilityGrant(effects=("hidden.write",)),
            ),
            fragment,
        )
    assert rejected.value.code == "PLAN_TOPOLOGY_INVALID"

    encoded = _validate(_validator(), _fragment()).to_dict()
    source = deepcopy(next(node for node in encoded["resolved_nodes"] if node["key"] == "source"))
    source["key"] = "orphan"
    encoded["resolved_nodes"].append(source)
    encoded["resolved_nodes"].sort(key=lambda node: node["key"])
    encoded["alias_provenance"].append({"alias": "source", "node_key": "orphan"})
    encoded["alias_provenance"].sort(key=lambda item: item["node_key"])
    encoded["node_count"] = 5
    source_composition = next(
        item for item in encoded["module_composition"] if item["module_id"] == "modules.source"
    )
    source_composition["count"] = 2
    encoded["aggregate_budget"] = Budget(
        wall_time=timedelta(seconds=15),
        model_tokens=50,
        tool_calls=5,
        cost_usd=0.5,
    ).to_data()
    encoded["topology_digest"] = digest_data(
        {"nodes": encoded["resolved_nodes"], "outputs": encoded["outputs"]}
    )
    with pytest.raises(PlanValidationError, match="unreachable from outputs"):
        PlanSignature.from_dict(encoded)


def test_alias_renames_preserve_resolved_behavior_and_digest() -> None:
    aliases = {
        "source": "fetch",
        "left": "normalize",
        "right": "count",
        "join": "decide",
    }
    original_catalog = _catalog()
    renamed_catalog = ModuleCatalog()
    for original_alias, renamed_alias in sorted(aliases.items()):
        descriptor = original_catalog.resolve(original_alias)
        renamed_catalog = renamed_catalog.allow(
            renamed_alias,
            module_id=descriptor["module_id"],
            module_digest=descriptor["module_digest"],
            input_schema_digests=tuple(descriptor["input_schema_digests"]),
            output_schema_digest=descriptor["output_schema_digest"],
            execution=descriptor["execution"],
            budget=Budget.from_data(descriptor["budget"]),
            capabilities=tuple(descriptor["capabilities"]),
            effects=tuple(descriptor["effects"]),
        )
    renamed_fragment = replace(
        _fragment(),
        nodes=tuple(
            replace(node, module_alias=aliases[node.module_alias]) for node in _fragment().nodes
        ),
    )
    original = _validate(_validator(catalog=original_catalog), _fragment())
    renamed = _validate(_validator(catalog=renamed_catalog), renamed_fragment)

    assert original == renamed
    assert original.topology_digest == renamed.topology_digest
    assert original.digest == renamed.digest
    assert original.alias_provenance != renamed.alias_provenance
    assert original.catalog_digest != renamed.catalog_digest
    assert original.source_fragment_digest != renamed.source_fragment_digest
    assert all("module_alias" not in node for node in original.resolved_nodes)


def test_budget_aggregation_counts_occurrences_and_uses_dag_critical_path() -> None:
    budgets = {
        "source": Budget(
            wall_time=timedelta(seconds=1), model_tokens=10, tool_calls=1, cost_usd=0.1
        ),
        "left": Budget(wall_time=timedelta(seconds=2), model_tokens=20, tool_calls=2, cost_usd=0.2),
        "right": Budget(
            wall_time=timedelta(seconds=4), model_tokens=30, tool_calls=3, cost_usd=0.0
        ),
        "join": Budget(wall_time=timedelta(seconds=8), model_tokens=40, tool_calls=4, cost_usd=0.0),
    }
    catalog = ModuleCatalog()
    specifications = (
        ("source", "modules.source", "1", (TEXT,), TEXT),
        ("left", "modules.left", "2", (TEXT,), TEXT),
        ("right", "modules.right", "3", (TEXT,), NUMBER),
        ("join", "modules.join", "4", (TEXT, NUMBER), FLAG),
    )
    for alias, module_id, digest, inputs, output in specifications:
        catalog = _allow(
            catalog,
            alias,
            module_id,
            digest,
            inputs,
            output,
            budget=budgets[alias],
        )
    limits = PlanLimits(
        max_nodes=8,
        max_depth=5,
        max_fanout=3,
        max_replans=2,
        budget=Budget(
            wall_time=timedelta(seconds=13),
            model_tokens=100,
            tool_calls=10,
            cost_usd=0.3,
        ),
    )
    signature = _validate(_validator(catalog=catalog, limits=limits), _fragment())

    assert signature.aggregate_budget == limits.budget
    assert signature.aggregate_budget.cost_usd == 0.3

    repeated = replace(
        _fragment(),
        nodes=tuple(
            replace(node, module_alias="source") if node.key == "left" else node
            for node in _fragment().nodes
        ),
    )
    repeated_signature = _validate(
        _validator(catalog=catalog, limits=replace(limits, budget=REGION_BUDGET)),
        repeated,
    )
    assert ("modules.source", "1" * 64, 2) in repeated_signature.module_composition
    assert repeated_signature.aggregate_budget.model_tokens == 90


@pytest.mark.parametrize(
    ("child", "limit", "dimension"),
    (
        (
            Budget(wall_time=None, model_tokens=0, tool_calls=0, cost_usd=0.0),
            Budget(wall_time=timedelta(seconds=1)),
            "wall_time",
        ),
        (
            Budget(wall_time=timedelta(0), model_tokens=None, tool_calls=0, cost_usd=0.0),
            Budget(model_tokens=1),
            "model_tokens",
        ),
        (
            Budget(wall_time=timedelta(0), model_tokens=0, tool_calls=None, cost_usd=0.0),
            Budget(tool_calls=1),
            "tool_calls",
        ),
        (
            Budget(wall_time=timedelta(0), model_tokens=0, tool_calls=0, cost_usd=None),
            Budget(cost_usd=1.0),
            "cost_usd",
        ),
    ),
)
def test_finite_region_budget_rejects_unbounded_child_dimension(
    child: Budget,
    limit: Budget,
    dimension: str,
) -> None:
    validator = _validator(
        catalog=_single_catalog(budget=child),
        limits=PlanLimits(
            max_nodes=1,
            max_depth=1,
            max_fanout=1,
            max_replans=0,
            budget=limit,
        ),
    )
    with pytest.raises(PlanValidationError, match=f"unbounded child {dimension}") as rejected:
        _validate(validator, _single_fragment(), output_schemas=(TEXT,))
    assert rejected.value.code == "PLAN_BUDGET_EXCEEDED"


@pytest.mark.parametrize(
    ("limit", "dimension"),
    (
        (Budget(wall_time=timedelta(seconds=14)), "wall_time"),
        (Budget(model_tokens=39), "model_tokens"),
        (Budget(tool_calls=3), "tool_calls"),
        (Budget(cost_usd=0.39), "cost_usd"),
    ),
)
def test_aggregate_budget_cannot_exceed_a_finite_region_limit(
    limit: Budget,
    dimension: str,
) -> None:
    limits = PlanLimits(
        max_nodes=8,
        max_depth=5,
        max_fanout=3,
        max_replans=2,
        budget=limit,
    )
    with pytest.raises(PlanValidationError, match=f"aggregate {dimension}") as rejected:
        _validate(_validator(limits=limits), _fragment())
    assert rejected.value.code == "PLAN_BUDGET_EXCEEDED"


def test_budget_aggregation_overflow_is_a_typed_validation_failure() -> None:
    catalog = _single_catalog(
        budget=Budget(
            wall_time=timedelta(0),
            model_tokens=0,
            tool_calls=0,
            cost_usd=1e308,
        )
    )
    fragment = PlanFragmentIR(
        fragment_id="overflow-plan",
        revision=1,
        supersedes=None,
        nodes=(
            PlanNode("first", "unit", ("$input",)),
            PlanNode("second", "unit", ("first",)),
        ),
        outputs=("second",),
    )
    with pytest.raises(PlanValidationError, match="supported numeric range") as rejected:
        _validate(
            _validator(
                catalog=catalog,
                limits=PlanLimits(
                    max_nodes=2,
                    max_depth=2,
                    max_fanout=1,
                    max_replans=0,
                    budget=UNBOUNDED,
                ),
            ),
            fragment,
            output_schemas=(TEXT,),
        )
    assert rejected.value.code == "PLAN_BUDGET_INVALID"


def test_exact_child_grants_are_derived_and_region_escalation_is_rejected() -> None:
    read = Capability(
        "records.read",
        connector="records",
        operation="read",
        input_type=str,
        output_type=str,
    ).to_data()
    write = EffectSpec(
        "messages.send",
        connector="messages",
        operation="send",
        input_type=str,
        output_type=str,
    ).to_data()
    catalog = ModuleCatalog().allow(
        "unit",
        module_id="modules.unit",
        module_digest="a" * 64,
        input_schema_digests=(TEXT,),
        output_schema_digest=TEXT,
        execution={**PROCESS_EXECUTION, "capabilities": ["gpu-placement"]},
        budget=NODE_BUDGET,
        capabilities=(read,),
        effects=(write,),
    )
    grant = CapabilityGrant(capabilities=("records.read",), effects=("messages.send",))
    signature = _validate(
        _validator(catalog=catalog, region_grant=grant),
        _single_fragment(),
        output_schemas=(TEXT,),
    )

    assert signature.region_grant == grant
    assert signature.required_grant == grant
    assert signature.resolved_nodes[0]["capability_grant"] == {
        "capabilities": ("records.read",),
        "effects": ("messages.send",),
    }
    assert "gpu-placement" not in signature.required_grant.capabilities
    with pytest.raises(PlanValidationError, match="outside the trusted region grant") as rejected:
        _validate(
            _validator(catalog=catalog, region_grant=CapabilityGrant()),
            _single_fragment(),
            output_schemas=(TEXT,),
        )
    assert rejected.value.code == "PLAN_CAPABILITY_ESCALATION"


def test_approval_required_effects_need_trusted_policy_eligibility_check() -> None:
    effect = EffectSpec(
        "messages.send",
        connector="messages",
        operation="send",
        input_type=str,
        output_type=str,
        approval_required=True,
    ).to_data()
    catalog = _single_catalog(effects=(effect,))
    grant = CapabilityGrant(effects=("messages.send",))
    with pytest.raises(PlanValidationError, match="trusted approval policy") as rejected:
        _validate(
            _validator(catalog=catalog, region_grant=grant),
            _single_fragment(),
            output_schemas=(TEXT,),
        )
    assert rejected.value.code == "PLAN_APPROVAL_REQUIRED"

    checked: list[tuple[str, str, str]] = []

    def approve(region_id: str, node_key: str, effect_name: str) -> None:
        checked.append((region_id, node_key, effect_name))

    signature = _validate(
        _validator(
            catalog=catalog,
            region_grant=grant,
            approval_check=approve,
        ),
        _single_fragment(),
        output_schemas=(TEXT,),
    )
    assert checked == [("workflow.dynamic", "unit", "messages.send")]
    assert signature.approval_requirements == (("unit", "messages.send"),)

    def broken(_region_id: str, _node_key: str, _effect_name: str) -> None:
        raise RuntimeError("private-provider-payload")

    with pytest.raises(PlanValidationError) as failed:
        _validate(
            _validator(catalog=catalog, region_grant=grant, approval_check=broken),
            _single_fragment(),
            output_schemas=(TEXT,),
        )
    assert failed.value.code == "PLAN_APPROVAL_VALIDATION_FAILED"
    assert "private-provider-payload" not in str(failed.value)
    assert "private-provider-payload" not in "".join(
        traceback.format_exception(failed.type, failed.value, failed.tb)
    )

    with pytest.raises(PlanValidationError, match="must return None"):
        _validate(
            _validator(
                catalog=catalog,
                region_grant=grant,
                approval_check=lambda _region, _node, _effect: True,
            ),
            _single_fragment(),
            output_schemas=(TEXT,),
        )


def test_signature_revalidation_rebuilds_pins_instead_of_trusting_imported_data() -> None:
    fragment = _single_fragment()
    trusted_validator = _validator(catalog=_single_catalog())
    trusted = _validate(trusted_validator, fragment, output_schemas=(TEXT,))
    forged_validator = _validator(catalog=_single_catalog(digest_character="f"))
    forged = _validate(forged_validator, fragment, output_schemas=(TEXT,))
    encoded = forged.to_dict()
    encoded["catalog_digest"] = trusted.catalog_digest
    imported = PlanSignature.from_dict(encoded)

    assert imported.source_fragment_digest == trusted.source_fragment_digest
    assert imported.catalog_digest == trusted.catalog_digest
    assert imported.resolved_nodes[0]["module_digest"] == "f" * 64
    assert (
        trusted_validator.revalidate(
            trusted,
            fragment,
            region_input_schema_digest=TEXT,
            expected_output_schema_digests=(TEXT,),
            expected_revision=1,
            expected_supersedes=None,
        )
        == trusted
    )
    with pytest.raises(PlanValidationError, match="trusted validation context") as rejected:
        trusted_validator.revalidate(
            imported,
            fragment,
            region_input_schema_digest=TEXT,
            expected_output_schema_digests=(TEXT,),
            expected_revision=1,
            expected_supersedes=None,
        )
    assert rejected.value.code == "PLAN_SIGNATURE_UNTRUSTED"


def test_signature_import_requires_canonical_nested_access_and_grant_data() -> None:
    capabilities = tuple(
        Capability(
            f"records.{name}",
            connector="records",
            operation=name,
            input_type=str,
            output_type=str,
        ).to_data()
        for name in ("alpha", "zeta")
    )
    effects = tuple(
        EffectSpec(
            f"messages.{name}",
            connector="messages",
            operation=name,
            input_type=str,
            output_type=str,
        ).to_data()
        for name in ("archive", "send")
    )
    grant = CapabilityGrant(
        capabilities=("records.alpha", "records.zeta"),
        effects=("messages.archive", "messages.send"),
    )
    signature = _validate(
        _validator(
            catalog=_single_catalog(capabilities=capabilities, effects=effects),
            region_grant=grant,
        ),
        _single_fragment(),
        output_schemas=(TEXT,),
    )
    for field in ("capabilities", "effects"):
        encoded = signature.to_dict()
        encoded["resolved_nodes"][0][field] = list(reversed(encoded["resolved_nodes"][0][field]))
        with pytest.raises(PlanValidationError, match="canonical order"):
            PlanSignature.from_dict(encoded)

    encoded = signature.to_dict()
    encoded["region_grant"] = []
    with pytest.raises(PlanValidationError, match="region_grant must be an object"):
        PlanSignature.from_dict(encoded)

    encoded = signature.to_dict()
    encoded["resolved_nodes"][0]["capability_grant"] = []
    with pytest.raises(PlanValidationError, match="node capability_grant must be an object"):
        PlanSignature.from_dict(encoded)

    encoded = signature.to_dict()
    encoded["resolved_nodes"][0]["capability_grant"]["capabilities"] = list(
        reversed(encoded["resolved_nodes"][0]["capability_grant"]["capabilities"])
    )
    with pytest.raises(PlanValidationError, match="canonical"):
        PlanSignature.from_dict(encoded)


def test_public_catalog_rejects_wire_budget_in_place_of_budget_object() -> None:
    descriptor = {
        "module_id": "modules.unit",
        "module_digest": "a" * 64,
        "input_schema_digests": (TEXT,),
        "output_schema_digest": TEXT,
        "execution": PROCESS_EXECUTION,
        "budget": NODE_BUDGET.to_data(),
    }
    with pytest.raises(TypeError, match="budget must be a Budget"):
        ModuleCatalog().allow("unit", **descriptor)  # type: ignore[arg-type]


def test_region_identity_and_limit_budget_types_fail_closed() -> None:
    with pytest.raises(ValueError, match="region_id"):
        PlanValidator(
            _single_catalog(),
            PlanLimits(
                max_nodes=1,
                max_depth=1,
                max_fanout=1,
                max_replans=0,
                budget=REGION_BUDGET,
            ),
            region_id="not a stable identity",
            region_grant=CapabilityGrant(),
        )
    with pytest.raises(TypeError, match="budget"):
        PlanLimits(
            max_nodes=1,
            max_depth=1,
            max_fanout=1,
            max_replans=0,
            budget=None,  # type: ignore[arg-type]
        )


def test_deep_generated_chain_uses_iterative_graph_validation() -> None:
    node_count = 1_100
    zero_budget = Budget(
        wall_time=timedelta(0),
        model_tokens=0,
        tool_calls=0,
        cost_usd=0.0,
    )
    catalog = _single_catalog(budget=zero_budget)
    nodes = tuple(
        PlanNode(
            f"n{index:04d}",
            "unit",
            ("$input",) if index == 0 else (f"n{index - 1:04d}",),
        )
        for index in range(node_count)
    )
    fragment = PlanFragmentIR(
        fragment_id="deep-chain",
        revision=1,
        supersedes=None,
        nodes=nodes,
        outputs=(f"n{node_count - 1:04d}",),
    )
    with pytest.raises(PlanValidationError, match="plan depth") as rejected:
        _validate(
            _validator(
                catalog=catalog,
                limits=PlanLimits(
                    max_nodes=node_count,
                    max_depth=10,
                    max_fanout=1,
                    max_replans=0,
                    budget=zero_budget,
                ),
            ),
            fragment,
            output_schemas=(TEXT,),
        )
    assert rejected.value.code == "PLAN_LIMIT_EXCEEDED"

    signature = _validate(
        _validator(
            catalog=catalog,
            limits=PlanLimits(
                max_nodes=node_count,
                max_depth=node_count,
                max_fanout=1,
                max_replans=0,
                budget=zero_budget,
            ),
        ),
        fragment,
        output_schemas=(TEXT,),
    )
    assert signature.max_depth == node_count
    assert PlanSignature.from_dict(signature.to_dict()) == signature
