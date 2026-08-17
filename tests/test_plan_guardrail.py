from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from maida.baseline_sample import create_baseline_from_report  # type: ignore[import-untyped]
from maida.plan_contract import (  # type: ignore[import-untyped]
    PlanEvidence,
    PlanValidationIssue,
)
from maida.policy import load_policy  # type: ignore[import-untyped]
from maida.schema_versions import REPORT_SCHEMA_VERSION  # type: ignore[import-untyped]

from maida.workflows import (
    Budget,
    CapabilityGrant,
    Effect,
    EffectSpec,
    ExecutionSpec,
    Idempotency,
    Module,
    ModuleRegistry,
    PlanFragmentIR,
    PlanLimits,
    PlanNode,
    PlanValidator,
    TaskStatus,
)
from maida.workflows._canonical import schema_digest
from maida.workflows.guardrail import PlanGuardrail, PlanGuardrailError
from maida.workflows.models import CapabilityGrant as StoredCapabilityGrant

BASE_BUDGET = Budget(
    wall_time=timedelta(seconds=1),
    model_tokens=0,
    tool_calls=0,
    cost_usd=1.0,
)
EFFECT_BUDGET = Budget(
    wall_time=timedelta(seconds=1),
    model_tokens=0,
    tool_calls=1,
    cost_usd=3.0,
)
SEND = EffectSpec(
    "messages.send",
    "local-messages",
    "send",
    str,
    str,
    connector_version="1",
    idempotency=Idempotency.REQUIRED,
)


class Normalize(Module[str, str]):
    module_id = "demo.normalize"
    input_type = str
    output_type = str
    budget = BASE_BUDGET

    async def execute(self, value: str, ctx: Any) -> str:
        return value.upper()


def send_module() -> Module[Any, Any]:
    module = Effect(SEND, module_id="demo.send")
    module.budget = EFFECT_BUDGET
    return module


REGISTRY = ModuleRegistry(
    modules={
        "text.clean": Normalize,
        "text.normalize": Normalize,
        "messages.send": send_module,
    }
)
LIMITS = PlanLimits(
    max_nodes=4,
    max_depth=4,
    max_fanout=3,
    budget=Budget(
        wall_time=timedelta(seconds=5),
        model_tokens=0,
        tool_calls=2,
        cost_usd=10.0,
    ),
)
REGION_GRANT = CapabilityGrant(effects=(SEND.name,))


def _signature(*, alias: str = "text.normalize", with_effect: bool = False) -> Any:
    nodes = [PlanNode("normalize", alias, ("$input",))]
    outputs = ("normalize",)
    if with_effect:
        nodes.append(PlanNode("send", "messages.send", ("normalize",)))
        outputs = ("send",)
    return PlanValidator(
        REGISTRY,
        LIMITS,
        region_id="request-plan",
        region_grant=REGION_GRANT,
    ).validate(
        PlanFragmentIR("request", tuple(nodes), outputs),
        region_input_schema_digest=schema_digest(str),
        expected_output_schema_digests=(schema_digest(str),),
    )


def _baseline(artifact: Any) -> dict[str, Any]:
    trace_signature = {
        "tool_path": [],
        "tool_call_sequence": [],
        "tool_call_counts": {},
        "llm_models_used": [],
        "event_type_sequence": ["RUN_START", "RUN_END"],
        "final_status": "ok",
    }
    return cast(
        dict[str, Any],
        create_baseline_from_report(
            {
                "report_version": REPORT_SCHEMA_VERSION,
                "metadata": {
                    "trials_used": 1,
                    "trials_budgeted": 1,
                    "environment_fingerprint": {},
                },
                "trials": [
                    {
                        "trace_id": "0" * 32,
                        "run_name": "planned-task",
                        "metric_values": {},
                        "invariant_outcomes": {},
                        "structural_signature": trace_signature,
                    }
                ],
                "plan_evidence": [PlanEvidence(artifact=artifact, valid=True).to_dict()],
            }
        ),
    )


def _policy(tmp_path: Path, metrics: str) -> Any:
    path = tmp_path / "policy.yaml"
    path.write_text(f"version: 2.1\nmetrics:\n{metrics}", encoding="utf-8")
    return load_policy(path)


def _execution_history(signature: Any) -> tuple[list[Any], Any]:
    tasks = []
    for descriptor in signature.resolved_nodes:
        tasks.append(
            SimpleNamespace(
                module_id=descriptor["module_id"],
                module_digest=descriptor["module_digest"],
                dependency_node_ids=tuple(descriptor["dependencies"]),
                execution=ExecutionSpec.from_data(dict(descriptor["execution"])),
                budget=Budget.from_data(descriptor["budget"]),
                capability_grant=StoredCapabilityGrant.from_data(
                    {
                        "capabilities": list(descriptor["capability_grant"]["capabilities"]),
                        "effects": list(descriptor["capability_grant"]["effects"]),
                    }
                ),
                status=TaskStatus.SUCCEEDED,
                accepted_boundary=SimpleNamespace(
                    output_schema_digest=descriptor["output_schema_digest"]
                ),
                plan_provenance=SimpleNamespace(
                    region_instance_id="request-plan-root",
                    node_key=descriptor["key"],
                ),
            )
        )
    history = SimpleNamespace(
        tasks=tuple(tasks),
        events=(
            SimpleNamespace(
                event_type="PLAN_MATERIALIZED",
                payload={
                    "region_instance_id": "request-plan-root",
                    "signature": signature.to_dict(),
                },
            ),
        ),
    )
    return tasks, history


def test_core_plan_signature_ignores_alias_noise_but_tracks_real_change() -> None:
    guardrail = PlanGuardrail()

    original = guardrail.evaluate(_signature(alias="text.normalize")).artifact
    renamed = guardrail.evaluate(_signature(alias="text.clean")).artifact
    changed = guardrail.evaluate(_signature(with_effect=True)).artifact

    assert original is not None
    assert renamed is not None
    assert changed is not None
    assert original.artifact_id == renamed.artifact_id
    assert original.alias_provenance != renamed.alias_provenance
    assert original.artifact_id != changed.artifact_id


def test_plan_policy_refusal_has_stable_code_and_actionable_explanation(
    tmp_path: Path,
) -> None:
    policy = _policy(
        tmp_path,
        "  plan_fanout: {kind: measured, direction: upper, limit: 0}\n",
    )
    guardrail = PlanGuardrail(policy=policy)

    evidence = guardrail.evaluate(_signature(with_effect=True))

    assert evidence.valid is False
    assert evidence.issues[0].code == "PLAN_FANOUT_EXCEEDED"
    assert evidence.issues[0].location == "signature.max_fanout"
    assert guardrail.format(evidence) == (
        "PLAN REFUSED: PLAN_FANOUT_EXCEEDED\n"
        "Plan fan-out is 1; policy allows at most 0 (plan_fanout)."
    )


def test_plan_policy_restricts_modules_and_requires_effect_approval(
    tmp_path: Path,
) -> None:
    module_policy = _policy(
        tmp_path,
        "  plan_modules: {kind: invariant, allowed: [demo.normalize]}\n",
    )
    module_evidence = PlanGuardrail(policy=module_policy).evaluate(_signature(with_effect=True))
    assert module_evidence.valid is False
    assert module_evidence.issues[0].code == "PLAN_MODULE_FORBIDDEN"
    assert "demo.send" in module_evidence.issues[0].message

    approval_policy = _policy(
        tmp_path,
        "  plan_grants:\n    kind: invariant\n    approval_required_for: [messages.send]\n",
    )
    approval_evidence = PlanGuardrail(policy=approval_policy).evaluate(_signature(with_effect=True))
    assert approval_evidence.valid is False
    assert approval_evidence.issues[0].code == "PLAN_GRANT_FORBIDDEN"
    assert "missing its required approval" in approval_evidence.issues[0].message

    forbidden_policy = _policy(
        tmp_path,
        "  plan_effectful_modules: {kind: invariant, none_of: [demo.send]}\n",
    )
    forbidden = PlanGuardrail(policy=forbidden_policy).evaluate(_signature(with_effect=True))
    assert forbidden.issues[0].code == "PLAN_EFFECTFUL_MODULE_FORBIDDEN"
    assert "forbidden effectful modules: demo.send" in forbidden.issues[0].message

    lower_policy = _policy(
        tmp_path,
        "  plan_depth: {kind: measured, direction: lower, limit: 2}\n",
    )
    too_shallow = PlanGuardrail(policy=lower_policy).evaluate(_signature())
    assert too_shallow.issues[0].code == "PLAN_DEPTH_BELOW_MINIMUM"
    assert "requires at least 2" in too_shallow.issues[0].message


def test_plan_diff_names_effect_budget_and_shape_changes_for_engineers() -> None:
    guardrail = PlanGuardrail()
    before = guardrail.evaluate(_signature()).artifact
    after = guardrail.evaluate(_signature(with_effect=True)).artifact
    assert before is not None
    assert after is not None

    changes = guardrail.diff(before, after)
    evidence = guardrail.evaluate(_signature(with_effect=True), accepted=before)
    rendered = guardrail.format(evidence)

    assert changes == evidence.graph_changes
    assert "New effectful module: demo.send (1 occurrence)." in rendered
    assert "Budget cost USD grew 4x: 1 -> 4." in rendered
    assert "Plan depth increased: 1 -> 2." in rendered
    removed = guardrail.diff(after, before)
    removed_evidence = PlanEvidence(artifact=before, valid=True, graph_changes=removed)
    assert "Removed module: demo.send." in guardrail.format(removed_evidence)


def test_recurring_plan_baseline_can_allow_seen_and_refuse_unseen_shape(
    tmp_path: Path,
) -> None:
    policy = _policy(
        tmp_path,
        "  plan_shape_seen: {kind: invariant, require: true}\n",
    )
    seed = PlanGuardrail().evaluate(_signature()).artifact
    assert seed is not None
    guardrail = PlanGuardrail(policy=policy, baseline=_baseline(seed))

    assert guardrail.evaluate(_signature()).valid is True
    unseen = guardrail.evaluate(_signature(with_effect=True))

    assert unseen.valid is False
    assert unseen.issues[0].code == "PLAN_SHAPE_UNSEEN"
    assert "has not appeared in the accepted baseline" in unseen.issues[0].message
    assert "Changes from accepted plan:" in guardrail.format(unseen)


def test_guardrail_rejects_invalid_inputs_and_core_baseline_shapes(tmp_path: Path) -> None:
    trace_policy = _policy(
        tmp_path,
        "  no_loops: {kind: invariant, require: true}\n",
    )
    assert PlanGuardrail(policy=trace_policy).evaluate(_signature()).valid is True

    guardrail = PlanGuardrail()
    evidence = guardrail.evaluate(_signature())
    with pytest.raises(TypeError, match="PlanSignature"):
        guardrail.evaluate(object())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="requires refused plan evidence"):
        PlanGuardrailError(evidence, "not refused")
    with pytest.raises(ValueError, match="at least one issue"):
        guardrail.execution_error(())

    assert evidence.artifact is not None
    baseline = _baseline(evidence.artifact)
    trace_only = dict(baseline)
    trace_only.pop("plan_sample")
    assert PlanGuardrail(baseline=trace_only).evaluate(_signature()).valid is True

    other_plan = _baseline(evidence.artifact)
    other_plan["plan_sample"]["plan_id"] = "different-plan"
    assert PlanGuardrail(baseline=other_plan).evaluate(_signature()).valid is True

    empty_population = _baseline(evidence.artifact)
    empty_population["plan_sample"]["artifacts"] = {}
    assert PlanGuardrail(baseline=empty_population).evaluate(_signature()).valid is True

    malformed = _baseline(evidence.artifact)
    malformed["plan_sample"] = []
    with pytest.raises(ValueError, match="plan_sample must be an object"):
        PlanGuardrail(baseline=malformed).evaluate(_signature())

    wrong_key = _baseline(evidence.artifact)
    artifact_data = wrong_key["plan_sample"]["artifacts"].pop(evidence.artifact.artifact_id)
    wrong_key["plan_sample"]["artifacts"]["f" * 64] = artifact_data
    with pytest.raises(ValueError, match="does not match its digest"):
        PlanGuardrail(baseline=wrong_key).evaluate(_signature())


def test_post_execution_proof_detects_structure_divergence() -> None:
    signature = _signature(with_effect=True)
    guardrail = PlanGuardrail()
    evidence = guardrail.evaluate(signature)
    assert evidence.valid is True
    tasks, history = _execution_history(signature)

    assert (
        guardrail.verify_execution(
            history,
            evidence,
            region_instance_id="request-plan-root",
        )
        == ()
    )

    changed = SimpleNamespace(**(vars(tasks[0]) | {"module_id": "demo.substituted"}))
    divergent_history = SimpleNamespace(
        tasks=(changed, *tasks[1:]),
        events=history.events,
    )
    issues = guardrail.verify_execution(
        divergent_history,
        evidence,
        region_instance_id="request-plan-root",
    )

    assert issues[0].code == "PLAN_EXECUTION_DIVERGENCE"
    assert issues[0].location == "execution.nodes.normalize"
    assert "approved module demo.normalize" in issues[0].message


def test_post_execution_proof_fails_closed_for_missing_or_tampered_history() -> None:
    signature = _signature(with_effect=True)
    guardrail = PlanGuardrail()
    evidence = guardrail.evaluate(signature)
    assert evidence.artifact is not None
    tasks, history = _execution_history(signature)

    refused = PlanEvidence(
        artifact=evidence.artifact,
        valid=False,
        issues=(PlanValidationIssue("PLAN_REFUSED", "Refused."),),
    )
    with pytest.raises(ValueError, match="requires approved"):
        guardrail.verify_execution(history, refused, region_instance_id="request-plan-root")

    missing_event = SimpleNamespace(tasks=history.tasks, events=())
    assert (
        guardrail.verify_execution(missing_event, evidence, region_instance_id="request-plan-root")[
            0
        ].location
        == "execution"
    )

    invalid_event = SimpleNamespace(
        tasks=history.tasks,
        events=(
            SimpleNamespace(
                event_type="PLAN_MATERIALIZED",
                payload={"region_instance_id": "request-plan-root", "signature": {}},
            ),
        ),
    )
    assert (
        guardrail.verify_execution(invalid_event, evidence, region_instance_id="request-plan-root")[
            0
        ].location
        == "execution.materialized_signature"
    )

    other_evidence = guardrail.evaluate(_signature())
    assert (
        guardrail.verify_execution(history, other_evidence, region_instance_id="request-plan-root")[
            0
        ].message
        == "Materialized plan does not match the artifact approved before execution."
    )

    duplicate = SimpleNamespace(tasks=(tasks[0], *tasks), events=history.events)
    assert (
        "more than once"
        in guardrail.verify_execution(duplicate, evidence, region_instance_id="request-plan-root")[
            0
        ].message
    )

    incomplete = SimpleNamespace(tasks=tuple(tasks[:-1]), events=history.events)
    assert (
        "missing=['send']"
        in guardrail.verify_execution(incomplete, evidence, region_instance_id="request-plan-root")[
            0
        ].message
    )
