from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from maida.workflows import (
    Budget,
    CapabilityGrant,
    ExecutionContext,
    ModelSpec,
    Module,
    ModuleRegistry,
    PlanIR,
    PlanLimits,
    PlanSignature,
    PlanValidationError,
    PlanValidator,
    RuntimeValue,
    Workflow,
    compile_workflow,
    map_over,
    when,
)
from maida.workflows._canonical import schema_digest


@dataclass(frozen=True)
class Prompt:
    text: str


@dataclass(frozen=True)
class Answer:
    text: str


MODEL = ModelSpec(
    name="writer",
    provider="test",
    model="writer-v1",
    input_type=Prompt,
    output_type=Answer,
)


class GeneratedModule(Module[str, str]):
    module_id = "generated.text"
    input_type = str
    output_type = str
    models = (MODEL,)

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value


def generated_plan(**node_fields: Any) -> dict[str, Any]:
    node = {
        "dependencies": ["$input"],
        "key": "write",
        "module_alias": "text.write",
        **node_fields,
    }
    return {
        "fragment_id": "generated-request",
        "nodes": [node],
        "outputs": ["write"],
        "version": "0.2.0",
    }


def validator() -> PlanValidator:
    return PlanValidator(
        ModuleRegistry(modules={"text.write": GeneratedModule}),
        PlanLimits(4, 4, 4, Budget()),
        region_id="generated-region",
        region_grant=CapabilityGrant(),
    )


def test_generated_bytes_resolve_directly_to_the_canonical_plan() -> None:
    signature = validator().validate(
        generated_plan(),
        region_input_schema_digest=schema_digest(str),
        expected_output_schema_digests=(schema_digest(str),),
    )

    assert isinstance(signature.plan, PlanIR)
    restored_plan = PlanIR.from_dict(signature.plan.to_dict())
    assert signature.plan == restored_plan
    assert signature.plan.canonical_json().encode() == restored_plan.canonical_json().encode()
    assert signature.plan.executable_steps[0].models == (MODEL.to_data(),)
    restored = type(signature).from_dict(signature.to_dict())
    assert signature == restored
    assert signature.canonical_json() == restored.canonical_json()


@pytest.mark.parametrize("control", [{"kind": "when"}, {"control": {"region": "map"}}])
def test_generated_data_cannot_hide_control_regions(control: dict[str, Any]) -> None:
    with pytest.raises(PlanValidationError) as raised:
        validator().validate(
            generated_plan(**control),
            region_input_schema_digest=schema_digest(str),
            expected_output_schema_digests=(schema_digest(str),),
        )

    assert raised.value.code == "PLAN_FRAGMENT_INVALID"


class IsNonempty(Module[str, bool]):
    module_id = "text.is-nonempty"
    input_type = str
    output_type = bool

    async def execute(self, value: str, ctx: ExecutionContext) -> bool:
        return bool(value)


def test_surviving_plan_retains_static_control_regions() -> None:
    # Use a small explicit graph because generated plans intentionally cannot
    # express either control region.
    class Controls(Workflow[str, bool]):
        workflow_id = "controls"
        input_type = str
        output_type = bool
        check = IsNonempty()

        def build(self, value: RuntimeValue[str]) -> RuntimeValue[bool]:
            result = self.check.at("result")(value)
            return when(result, result, result)

    branch = compile_workflow(Controls())

    class Mapped(Workflow[list[str], list[bool]]):
        workflow_id = "mapped"
        input_type = list[str]
        output_type = list[bool]
        check = IsNonempty()

        def build(self, value: RuntimeValue[list[str]]) -> RuntimeValue[list[bool]]:
            return map_over(value, self.check.at("items"), item_key=lambda item: item)

    mapped = compile_workflow(Mapped())
    restored_branch = PlanIR.from_dict(branch.to_dict())

    assert type(branch) is type(
        validator()
        .validate(
            generated_plan(),
            region_input_schema_digest=schema_digest(str),
            expected_output_schema_digests=(schema_digest(str),),
        )
        .plan
    )
    assert branch.canonical_json().encode() == restored_branch.canonical_json().encode()
    assert any(step.kind == "when" for step in branch.steps)
    assert any(step.kind == "map_module" for step in mapped.steps)


def test_imported_signature_rejects_plan_tampering_with_a_stable_code() -> None:
    signature = validator().validate(
        generated_plan(),
        region_input_schema_digest=schema_digest(str),
        expected_output_schema_digests=(schema_digest(str),),
    )
    encoded = signature.to_dict()
    encoded["plan"]["steps"][0]["definition_digest"] = "0" * 64

    with pytest.raises(PlanValidationError) as raised:
        PlanSignature.from_dict(encoded)

    assert raised.value.code == "PLAN_SIGNATURE_INVALID"
