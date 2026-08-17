"""Judge trusted generated plans before execution using Maida's core contract."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

from maida.baseline_bind import validate_policy_against_baseline  # type: ignore[import-untyped]
from maida.baseline_sample import validate_baseline_version  # type: ignore[import-untyped]
from maida.gate import aggregate_metrics  # type: ignore[import-untyped]
from maida.plan_contract import (  # type: ignore[import-untyped]
    PlanArtifact,
    PlanDiffKind,
    PlanEvidence,
    PlanGraphChange,
    PlanValidationIssue,
    plan_artifact_from_resolved_signature,
    plan_invariant_outcomes,
    plan_metric_values,
)
from maida.policy_types import PLAN_METRIC_NAMES  # type: ignore[import-untyped]
from maida.statistics import GateVerdict  # type: ignore[import-untyped]

from ._canonical import canonical_data
from .dynamic import PlanSignature
from .models import TaskStatus

if TYPE_CHECKING:
    from maida.assertions import AssertionPolicy  # type: ignore[import-untyped]


_NUMERIC_RULES = {
    "plan_depth": ("PLAN_DEPTH_EXCEEDED", "Plan depth", "signature.max_depth"),
    "plan_fanout": (
        "PLAN_FANOUT_EXCEEDED",
        "Plan fan-out",
        "signature.max_fanout",
    ),
    "plan_budget_cost_usd": (
        "PLAN_BUDGET_COST_EXCEEDED",
        "Plan cost budget (USD)",
        "signature.aggregate_budget.cost_usd",
    ),
    "plan_budget_model_tokens": (
        "PLAN_BUDGET_MODEL_TOKENS_EXCEEDED",
        "Plan model-token budget",
        "signature.aggregate_budget.model_tokens",
    ),
    "plan_budget_tool_calls": (
        "PLAN_BUDGET_TOOL_CALLS_EXCEEDED",
        "Plan tool-call budget",
        "signature.aggregate_budget.tool_calls",
    ),
    "plan_budget_wall_time_ms": (
        "PLAN_BUDGET_WALL_TIME_EXCEEDED",
        "Plan wall-time budget (ms)",
        "signature.aggregate_budget.wall_time_ms",
    ),
}


def _number(value: object) -> str:
    number = float(cast(int | float, value))
    return str(int(number)) if number.is_integer() else f"{number:g}"


def _plan_policy(policy: AssertionPolicy | None) -> AssertionPolicy | None:
    if policy is None:
        return None
    metrics = {name: metric for name, metric in policy.metrics.items() if name in PLAN_METRIC_NAMES}
    if not metrics:
        return None
    return replace(policy, trials=1, fail_fast=True, metrics=metrics)


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return cast(Mapping[str, Any], value)


class PlanGuardrailError(RuntimeError):
    """A core plan-evidence refusal raised before generated children exist."""

    def __init__(self, evidence: PlanEvidence, message: str) -> None:
        if evidence.valid or not evidence.issues:
            raise ValueError("PlanGuardrailError requires refused plan evidence")
        self.evidence = evidence
        self.code = evidence.issues[0].code
        super().__init__(message)


class _PlanExecutionDivergenceError(RuntimeError):
    def __init__(self, issues: tuple[PlanValidationIssue, ...]) -> None:
        self.issues = issues
        self.code = issues[0].code
        super().__init__(issues[0].message)


class PlanGuardrail:
    """Evaluate, compare, explain, and prove generated plans in core vocabulary."""

    def __init__(
        self,
        *,
        policy: AssertionPolicy | None = None,
        baseline: Mapping[str, Any] | None = None,
    ) -> None:
        self.policy = _plan_policy(policy)
        self.baseline = dict(baseline) if baseline is not None else None
        if self.baseline is not None:
            validate_baseline_version(self.baseline)
        if self.policy is not None:
            validate_policy_against_baseline(self.policy, self.baseline)

    def evaluate(
        self,
        signature: PlanSignature,
        *,
        accepted: PlanArtifact | None = None,
    ) -> PlanEvidence:
        """Return typed core evidence for one trusted resolved signature."""
        if not isinstance(signature, PlanSignature):
            raise TypeError("signature must be PlanSignature")
        artifact = plan_artifact_from_resolved_signature(signature.to_dict())
        reference = accepted or self._accepted_reference(artifact)
        changes = self.diff(reference, artifact) if reference is not None else ()
        issues = self._policy_issues(artifact)
        return PlanEvidence(
            artifact=artifact,
            valid=not issues,
            issues=issues,
            graph_changes=changes,
        )

    def diff(self, before: PlanArtifact, after: PlanArtifact) -> tuple[PlanGraphChange, ...]:
        """Return ordered core graph changes between two plan artifacts."""
        before_data = before.to_dict()["signature"]
        after_data = after.to_dict()["signature"]
        changes: list[PlanGraphChange] = []

        before_modules = _modules_by_id(before_data["module_composition"])
        after_modules = _modules_by_id(after_data["module_composition"])
        for module_id in sorted(before_modules.keys() - after_modules.keys()):
            changes.append(
                _change(
                    PlanDiffKind.DELETION,
                    f"signature.module_composition.{module_id}",
                    before_modules[module_id],
                    None,
                    resolvable=False,
                )
            )
        for module_id in sorted(after_modules.keys() - before_modules.keys()):
            inserted = dict(after_modules[module_id])
            inserted["effectful"] = module_id in after.effectful_modules
            changes.append(
                _change(
                    PlanDiffKind.INSERTION,
                    f"signature.module_composition.{module_id}",
                    None,
                    inserted,
                    resolvable=False,
                )
            )
        for module_id in sorted(before_modules.keys() & after_modules.keys()):
            old = before_modules[module_id]
            new = after_modules[module_id]
            if old["module_digest"] != new["module_digest"]:
                changes.append(
                    _change(
                        PlanDiffKind.MODULE_DIGEST_CHANGED,
                        f"signature.module_composition.{module_id}.module_digest",
                        old["module_digest"],
                        new["module_digest"],
                        resolvable=False,
                    )
                )
            if old["count"] != new["count"]:
                changes.append(
                    _change(
                        PlanDiffKind.TOPOLOGY_CHANGED,
                        f"signature.module_composition.{module_id}.count",
                        old["count"],
                        new["count"],
                        resolvable=False,
                    )
                )

        for key in ("cost_usd", "model_tokens", "tool_calls", "wall_time_ms"):
            old = before_data["aggregate_budget"][key]
            new = after_data["aggregate_budget"][key]
            if old != new:
                changes.append(
                    _change(
                        PlanDiffKind.BUDGET_CHANGED,
                        f"signature.aggregate_budget.{key}",
                        old,
                        new,
                        resolvable=True,
                    )
                )

        for key, label in (("capabilities", "capabilities"), ("effects", "effects")):
            old = before_data["required_grant"][key]
            new = after_data["required_grant"][key]
            if old != new:
                kind = (
                    PlanDiffKind.CAPABILITY_CHANGED
                    if key == "capabilities"
                    else PlanDiffKind.EFFECT_CHANGED
                )
                changes.append(
                    _change(
                        kind,
                        f"signature.required_grant.{label}",
                        old,
                        new,
                        resolvable=False,
                    )
                )

        for key in ("node_count", "max_depth", "max_fanout"):
            old = before_data[key]
            new = after_data[key]
            if old != new:
                changes.append(
                    _change(
                        PlanDiffKind.TOPOLOGY_CHANGED,
                        f"signature.{key}",
                        old,
                        new,
                        resolvable=False,
                    )
                )
        for key, kind in (
            ("effectful_modules", PlanDiffKind.EFFECT_CHANGED),
            ("approval_requirements", PlanDiffKind.POLICY_CHANGED),
            ("output_schema_digests", PlanDiffKind.SCHEMA_CHANGED),
            ("topology_digest", PlanDiffKind.TOPOLOGY_CHANGED),
        ):
            old = before_data[key]
            new = after_data[key]
            if old != new:
                changes.append(
                    _change(
                        kind,
                        f"signature.{key}",
                        old,
                        new,
                        resolvable=False,
                    )
                )
        return tuple(changes)

    def format(self, evidence: PlanEvidence) -> str:
        """Render a compact engineer-facing approval, refusal, or plan diff."""
        if evidence.artifact is None:  # pragma: no cover - core evidence enforces this here
            return "PLAN REFUSED: no canonical plan artifact was produced."
        if not evidence.valid:
            lines = [f"PLAN REFUSED: {evidence.issues[0].code}"]
            lines.extend(issue.message for issue in evidence.issues)
            if evidence.graph_changes:
                lines.extend(["", "Changes from accepted plan:"])
                lines.extend(
                    f"- {self._format_change(change)}" for change in evidence.graph_changes
                )
            return "\n".join(lines)
        if not evidence.graph_changes:
            return f"PLAN APPROVED: {evidence.artifact.artifact_id}"
        lines = ["PLAN APPROVED WITH CHANGES:"]
        lines.extend(f"- {self._format_change(change)}" for change in evidence.graph_changes)
        return "\n".join(lines)

    def verify_execution(
        self,
        history: Any,
        evidence: PlanEvidence,
        *,
        region_instance_id: str,
    ) -> tuple[PlanValidationIssue, ...]:
        """Check persisted generated tasks against the exact approved signature."""
        if not evidence.valid or evidence.artifact is None:
            raise ValueError("execution proof requires approved plan evidence")
        event = next(
            (
                item
                for item in history.events
                if item.event_type == "PLAN_MATERIALIZED"
                and item.payload.get("region_instance_id") == region_instance_id
            ),
            None,
        )
        if event is None:
            return (_divergence("execution", "Approved plan has no materialization event."),)
        try:
            signature = PlanSignature.from_dict(
                _mapping(event.payload.get("signature"), "signature")
            )
            materialized = plan_artifact_from_resolved_signature(signature.to_dict())
        except (TypeError, ValueError) as exc:
            return (
                _divergence(
                    "execution.materialized_signature",
                    f"Materialized plan signature is invalid: {exc}",
                ),
            )
        if materialized.artifact_id != evidence.artifact.artifact_id:
            return (
                _divergence(
                    "execution.materialized_signature",
                    "Materialized plan does not match the artifact approved before execution.",
                ),
            )

        expected = {cast(str, item["key"]): item for item in signature.resolved_nodes}
        generated = tuple(
            task
            for task in history.tasks
            if task.plan_provenance is not None
            and task.plan_provenance.region_instance_id == region_instance_id
        )
        actual: dict[str, Any] = {}
        for task in generated:
            key = task.plan_provenance.node_key
            if key in actual:
                return (
                    _divergence(
                        f"execution.nodes.{key}",
                        f"Generated node {key} was materialized more than once.",
                    ),
                )
            actual[key] = task
        if set(actual) != set(expected):
            missing = sorted(set(expected) - set(actual))
            unexpected = sorted(set(actual) - set(expected))
            return (
                _divergence(
                    "execution.nodes",
                    "Executed node set diverged from the approved plan "
                    f"(missing={missing}, unexpected={unexpected}).",
                ),
            )

        issues: list[PlanValidationIssue] = []
        for key in sorted(expected):
            descriptor = expected[key]
            task = actual[key]
            mismatch = _task_mismatch(task, descriptor)
            if mismatch is not None:
                issues.append(_divergence(f"execution.nodes.{key}", mismatch))
        return tuple(issues)

    def execution_error(
        self, issues: tuple[PlanValidationIssue, ...]
    ) -> _PlanExecutionDivergenceError:
        """Build the runtime error for a failed post-execution proof."""
        if not issues:
            raise ValueError("execution divergence requires at least one issue")
        return _PlanExecutionDivergenceError(issues)

    def _accepted_reference(self, artifact: PlanArtifact) -> PlanArtifact | None:
        if self.baseline is None:
            return None
        raw_sample = self.baseline.get("plan_sample")
        if raw_sample is None:
            return None
        sample = _mapping(raw_sample, "baseline.plan_sample")
        if sample.get("plan_id") != artifact.plan_id:
            return None
        raw_artifacts = _mapping(sample.get("artifacts"), "baseline.plan_sample.artifacts")
        accepted: dict[str, PlanArtifact] = {}
        for artifact_id, value in raw_artifacts.items():
            restored = PlanArtifact.from_dict(_mapping(value, f"baseline artifact {artifact_id}"))
            if artifact_id != restored.artifact_id:
                raise ValueError(f"baseline artifact key {artifact_id} does not match its digest")
            accepted[artifact_id] = restored
        if artifact.artifact_id in accepted:
            return accepted[artifact.artifact_id]
        if not accepted:
            return None
        raw_counts = sample.get("artifact_counts")
        counts = raw_counts if isinstance(raw_counts, Mapping) else {}
        return min(
            accepted.values(),
            key=lambda item: (-int(counts.get(item.artifact_id, 0)), item.artifact_id),
        )

    def _policy_issues(self, artifact: PlanArtifact) -> tuple[PlanValidationIssue, ...]:
        if self.policy is None:
            return ()
        results = aggregate_metrics(
            policy=self.policy,
            trial_values=[plan_metric_values(artifact)],
            trial_invariants=[
                plan_invariant_outcomes(artifact, self.policy, baseline=self.baseline)
            ],
            process_outcomes=[True],
            baseline=self.baseline,
            trials_budgeted=1,
            stopping_rule="fixed_n",
        )
        return tuple(
            self._policy_issue(artifact, result)
            for result in results
            if result.check_name != "agent_process" and result.verdict is GateVerdict.FAIL
        )

    def _policy_issue(self, artifact: PlanArtifact, result: Any) -> PlanValidationIssue:
        name = cast(str, result.check_name)
        if name in _NUMERIC_RULES:
            code, label, location = _NUMERIC_RULES[name]
            observed = result.evidence["observed"]
            allowed = result.evidence["allowed"]
            upper = allowed.get("upper")
            if upper is not None and observed > upper:
                message = (
                    f"{label} is {_number(observed)}; policy allows at most "
                    f"{_number(upper)} ({name})."
                )
                return PlanValidationIssue(code=code, message=message, location=location)
            lower = allowed.get("lower")
            message = (
                f"{label} is {_number(observed)}; policy requires at least "
                f"{_number(lower)} ({name})."
            )
            return PlanValidationIssue(
                code=code.replace("EXCEEDED", "BELOW_MINIMUM"),
                message=message,
                location=location,
            )
        if name == "plan_shape_seen":
            return PlanValidationIssue(
                code="PLAN_SHAPE_UNSEEN",
                message=(
                    f"Plan topology {artifact.topology_digest[:12]} has not appeared in "
                    f"the accepted baseline for {artifact.plan_id}; policy requires a "
                    "previously seen shape."
                ),
                location="signature.topology_digest",
            )
        assert self.policy is not None  # narrowed by _policy_issues
        values = _invariant_values(artifact, name)
        metric = self.policy.metrics[name]
        forbidden = sorted(values & set(metric.none_of))
        outside = sorted(values - set(metric.allowed)) if metric.allowed is not None else []
        missing = sorted(set(metric.all_of) - values)
        if name == "plan_modules":
            code, label, location = (
                "PLAN_MODULE_FORBIDDEN",
                "modules",
                "signature.module_composition",
            )
        elif name == "plan_effectful_modules":
            code, label, location = (
                "PLAN_EFFECTFUL_MODULE_FORBIDDEN",
                "effectful modules",
                "signature.effectful_modules",
            )
        else:
            code, label, location = (
                "PLAN_GRANT_FORBIDDEN",
                "grants",
                "signature.required_grant",
            )
        details = []
        if forbidden:
            details.append(f"forbidden {label}: {', '.join(forbidden)}")
        if outside:
            details.append(f"{label} outside the allowlist: {', '.join(outside)}")
        if missing:
            details.append(f"required {label} missing: {', '.join(missing)}")
        if name == "plan_grants" and not details:
            details.append("a requested effect is missing its required approval")
        return PlanValidationIssue(
            code=code,
            message=f"Plan violates {name}: {'; '.join(details)}.",
            location=location,
        )

    @staticmethod
    def _format_change(change: PlanGraphChange) -> str:
        location = change.location
        if change.kind is PlanDiffKind.INSERTION:
            after = cast(Mapping[str, Any], change.after)
            module_id = cast(str, after["module_id"])
            qualifier = "effectful " if after.get("effectful") else ""
            count = int(after["count"])
            noun = "occurrence" if count == 1 else "occurrences"
            return f"New {qualifier}module: {module_id} ({count} {noun})."
        if change.kind is PlanDiffKind.DELETION:
            before = cast(Mapping[str, Any], change.before)
            return f"Removed module: {before['module_id']}."
        if location.startswith("signature.aggregate_budget."):
            key = location.rsplit(".", 1)[-1]
            labels = {
                "cost_usd": "cost USD",
                "model_tokens": "model tokens",
                "tool_calls": "tool calls",
                "wall_time_ms": "wall time ms",
            }
            before_value = float(change.before)
            after_value = float(change.after)
            ratio = (
                f" grew {after_value / before_value:g}x"
                if before_value > 0 and after_value > before_value
                else " changed"
            )
            return (
                f"Budget {labels[key]}{ratio}: {_number(before_value)} -> {_number(after_value)}."
            )
        if location == "signature.max_depth":
            direction = _direction(change.before, change.after)
            return f"Plan depth {direction}: {change.before} -> {change.after}."
        if location == "signature.max_fanout":
            direction = _direction(change.before, change.after)
            return f"Plan fan-out {direction}: {change.before} -> {change.after}."
        if location == "signature.node_count":
            return f"Plan node count changed: {change.before} -> {change.after}."
        if location == "signature.topology_digest":
            return "Dependency topology changed."
        labels = {
            "signature.required_grant.capabilities": "Required capabilities changed",
            "signature.required_grant.effects": "Required effects changed",
            "signature.effectful_modules": "Effectful module set changed",
            "signature.approval_requirements": "Approval requirements changed",
            "signature.output_schema_digests": "Output contract changed",
        }
        if location in labels:
            return f"{labels[location]}: {change.before} -> {change.after}."
        return f"{change.kind.value} at {location}: {change.before} -> {change.after}."


def _change(
    kind: PlanDiffKind,
    location: str,
    before: Any,
    after: Any,
    *,
    resolvable: bool,
) -> PlanGraphChange:
    return PlanGraphChange(kind, location, before, after, resolvable)


def _modules_by_id(value: object) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in cast(list[dict[str, Any]], value):
        module_id = item["module_id"]
        grouped.setdefault(module_id, []).append(dict(item))
    modules = {}
    for module_id, entries in grouped.items():
        entries.sort(key=lambda item: item["module_digest"])
        modules[module_id] = {
            "count": sum(int(item["count"]) for item in entries),
            "module_digest": (
                entries[0]["module_digest"]
                if len(entries) == 1
                else [item["module_digest"] for item in entries]
            ),
            "module_id": module_id,
        }
    return modules


def _invariant_values(artifact: PlanArtifact, name: str) -> set[str]:
    if name == "plan_modules":
        return {module.module_id for module in artifact.module_composition}
    if name == "plan_effectful_modules":
        return set(artifact.effectful_modules)
    return set(artifact.required_grant.names)


def _direction(before: object, after: object) -> str:
    grew = float(cast(int | float, after)) > float(cast(int | float, before))
    return "increased" if grew else "decreased"


def _divergence(location: str, message: str) -> PlanValidationIssue:
    return PlanValidationIssue(
        code="PLAN_EXECUTION_DIVERGENCE",
        message=message,
        location=location,
    )


def _task_mismatch(task: Any, descriptor: Mapping[str, Any]) -> str | None:
    expected_module = cast(str, descriptor["module_id"])
    if task.module_id != expected_module or task.module_digest != descriptor["module_digest"]:
        return (
            f"Generated node {descriptor['key']} executed {task.module_id} at "
            f"{task.module_digest}; approved module {expected_module} at "
            f"{descriptor['module_digest']} was required."
        )
    expected_fields = (
        (tuple(task.dependency_node_ids), tuple(descriptor["dependencies"]), "dependencies"),
        (task.execution.to_data(), descriptor["execution"], "execution requirements"),
        (task.budget.to_data(), descriptor["budget"], "budget"),
        (task.capability_grant.to_data(), descriptor["capability_grant"], "capability grant"),
    )
    for actual, expected, label in expected_fields:
        if canonical_data(actual) != canonical_data(expected):
            return f"Generated node {descriptor['key']} {label} diverged from the approved plan."
    if task.status is not TaskStatus.SUCCEEDED or task.accepted_boundary is None:
        return f"Generated node {descriptor['key']} has no accepted successful execution."
    if task.accepted_boundary.output_schema_digest != descriptor["output_schema_digest"]:
        return (
            f"Generated node {descriptor['key']} output contract diverged from the approved plan."
        )
    return None


__all__ = ["PlanGuardrail", "PlanGuardrailError"]
