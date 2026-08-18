from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from maida.policy import load_policy  # type: ignore[import-untyped]

from examples import adversarial_workflows, userplane_quickstart
from examples.workflow_creation import (
    approval_boundary,
    celery_backend,
    external_boundary,
    generated_plan,
    serialized_plan,
)
from maida.workflows import Workflow, WorkflowRunner
from maida.workflows.guardrail import PlanGuardrailError
from maida.workflows.persistence import PostgresStore

SHIPPED_EXAMPLE_MODULES = {
    "examples.adversarial_workflows",
    "examples.native_replay_demo",
    "examples.userplane_quickstart",
    "examples.workflow_creation.approval_boundary",
    "examples.workflow_creation.celery_backend",
    "examples.workflow_creation.external_boundary",
    "examples.workflow_creation.generated_plan",
    "examples.workflow_creation.serialized_plan",
}


def test_shipped_example_inventory_has_execution_coverage() -> None:
    repository = Path(__file__).parents[1]
    discovered = {
        ".".join(path.relative_to(repository).with_suffix("").parts)
        for path in (repository / "examples").rglob("*.py")
        if path.name != "__init__.py"
    }

    assert discovered == SHIPPED_EXAMPLE_MODULES


@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "module",
    (approval_boundary, celery_backend, external_boundary, serialized_plan),
    ids=lambda module: module.__name__.rsplit(".", maxsplit=1)[-1],
)
async def test_specialized_examples_execute_offline(
    module: ModuleType,
    postgres_store: PostgresStore,
) -> None:
    result = await module.run_example(postgres_store, deepcopy(module.EXAMPLE_INPUT))

    assert result.output == module.EXPECTED_OUTPUT


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_generated_example_varies_the_executed_plan_with_input(
    postgres_store: PostgresStore,
) -> None:
    cases = (
        (
            generated_plan.BRIEF_INPUT,
            generated_plan.BRIEF_EXPECTED_OUTPUT,
            2,
        ),
        (generated_plan.EXAMPLE_INPUT, generated_plan.EXPECTED_OUTPUT, 5),
    )
    definition_digests: list[str] = []

    for value, expected_output, expected_nodes in cases:
        result = await generated_plan.run_example(postgres_store, value)
        history = postgres_store.load_run_history(result.run_id, tenant_id="local")
        materialized = next(
            event for event in history.events if event.event_type == "PLAN_MATERIALIZED"
        )

        assert result.output == expected_output
        assert history.definition.workflow_id == "dynamic:request-plan"
        assert materialized.payload["signature"]["node_count"] == expected_nodes
        definition_digests.append(result.definition_digest)

    assert len(set(definition_digests)) == len(cases)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_generated_example_accepts_core_policy_and_refuses_the_plan(
    postgres_store: PostgresStore,
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        "version: 2.1\nmetrics:\n  plan_fanout: {kind: measured, direction: upper, limit: 1}\n",
        encoding="utf-8",
    )

    with pytest.raises(PlanGuardrailError, match="PLAN REFUSED") as excinfo:
        await generated_plan.run_example(
            postgres_store,
            policy=load_policy(policy_path),
        )

    assert excinfo.value.code == "PLAN_FANOUT_EXCEEDED"


def test_serialized_plan_round_trips_canonical_data(tmp_path: Path) -> None:
    path = tmp_path / "onboarding.maida-workflow"
    restored = serialized_plan.save_and_restore(path)

    assert restored.digest == serialized_plan.bundle.digest
    assert path.stat().st_mode & 0o777 == 0o600


@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("workflow", "value", "expected"),
    (
        (
            adversarial_workflows.AdversarialBranchWorkflow(),
            {"escalated": True},
            "urgent",
        ),
        (
            adversarial_workflows.AdversarialMapWorkflow(),
            [
                adversarial_workflows.BatchItem("b", " B "),
                adversarial_workflows.BatchItem("a", " A "),
            ],
            ["b", "a"],
        ),
        (
            adversarial_workflows.AdversarialNestedEffectWorkflow(),
            "case",
            ("reviewed:case", "reviewed:case"),
        ),
        (userplane_quickstart.GreetingWorkflow(), "Ada", "Hello, Ada!"),
    ),
)
async def test_support_examples_execute_offline(
    workflow: Workflow[Any, Any],
    value: Any,
    expected: Any,
    postgres_store: PostgresStore,
) -> None:
    result = await WorkflowRunner(postgres_store).run(workflow, value)

    assert result.output == expected
