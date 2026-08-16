from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest

from examples.workflow_creation import (
    advanced_interactive,
    advanced_nested,
    advanced_portable_workflow,
    advanced_stable_map,
    easy_first_workflow,
    easy_sequential,
    expert_external_workflow,
    expert_generated_workflow,
    expert_replay_ready,
    intermediate_branching,
    intermediate_parallel,
)
from maida.workflows import BoundWorkflow, Workflow, compile_workflow
from maida.workflows.persistence import PostgresStore
from maida.workflows.runtime import WorkflowRunner


@dataclass(frozen=True)
class ExampleCase:
    module: ModuleType
    expected_step_kinds: frozenset[str]

    @property
    def id(self) -> str:
        return self.module.__name__.rsplit(".", maxsplit=1)[-1]

    @property
    def workflow(self) -> Workflow[Any, Any] | BoundWorkflow:
        return cast(Workflow[Any, Any] | BoundWorkflow, self.module.workflow)


EXAMPLES = (
    ExampleCase(easy_first_workflow, frozenset({"module"})),
    ExampleCase(easy_sequential, frozenset({"module"})),
    ExampleCase(intermediate_branching, frozenset({"module", "when"})),
    ExampleCase(intermediate_parallel, frozenset({"module", "parallel"})),
    ExampleCase(advanced_stable_map, frozenset({"module", "map_module"})),
    ExampleCase(advanced_nested, frozenset({"module", "parallel"})),
    ExampleCase(advanced_portable_workflow, frozenset({"module"})),
    ExampleCase(expert_generated_workflow, frozenset({"module"})),
    ExampleCase(
        expert_replay_ready,
        frozenset({"module", "map_module", "parallel", "when"}),
    ),
)


@pytest.mark.parametrize("case", EXAMPLES, ids=lambda case: case.id)
def test_workflow_creation_examples_compile_canonically(case: ExampleCase) -> None:
    first = (
        case.workflow.plan
        if isinstance(case.workflow, BoundWorkflow)
        else compile_workflow(case.workflow)
    )
    second = (
        case.workflow.plan
        if isinstance(case.workflow, BoundWorkflow)
        else compile_workflow(case.workflow)
    )

    assert first.canonical_json() == second.canonical_json()
    assert first.digest == second.digest
    assert case.expected_step_kinds <= {step.kind for step in first.steps}
    assert first.executable_steps
    assert all(step.replay_key is not None for step in first.executable_steps)


@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize("case", EXAMPLES, ids=lambda case: case.id)
async def test_workflow_creation_examples_produce_documented_output(
    case: ExampleCase,
    postgres_store: PostgresStore,
) -> None:
    result = await WorkflowRunner(postgres_store).run(
        case.workflow,
        deepcopy(case.module.EXAMPLE_INPUT),
    )

    assert result.output == case.module.EXPECTED_OUTPUT


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_stable_map_uses_item_keys_instead_of_list_positions(
    postgres_store: PostgresStore,
) -> None:
    runner = WorkflowRunner(postgres_store)
    first = await runner.run(
        advanced_stable_map.workflow,
        deepcopy(advanced_stable_map.EXAMPLE_INPUT),
    )
    second = await runner.run(
        advanced_stable_map.workflow,
        list(reversed(deepcopy(advanced_stable_map.EXAMPLE_INPUT))),
    )

    def instances_by_document(run_id: str) -> dict[str, str]:
        history = postgres_store.load_run_history(run_id, tenant_id="local")
        return {
            str(postgres_store.values.decode(boundary.input_value)["id"]): (
                boundary.step_instance_id
            )
            for boundary in history.accepted_boundaries
            if boundary.module_id == "onboarding-stable-map.normalize"
        }

    assert first.output == "beta | alpha"
    assert second.output == "alpha | beta"
    assert instances_by_document(first.run_id) == instances_by_document(second.run_id)


def test_interactive_and_external_examples_compile_honest_boundaries() -> None:
    interactive = compile_workflow(advanced_interactive.workflow)
    external = compile_workflow(expert_external_workflow.workflow)

    assert {step.kind for step in interactive.steps} == {"module", "when"}
    assert any(step.effects for step in external.executable_steps)
    assert external.executable_steps[0].effects[0]["approval_required"] is True


def test_generated_example_validates_and_portable_bundle_round_trips(tmp_path: Path) -> None:
    expert_generated_workflow.validate_fragment()
    path = tmp_path / "onboarding.maida-workflow"
    restored = advanced_portable_workflow.save_and_restore(path)

    assert restored.digest == advanced_portable_workflow.bundle.digest
    assert path.stat().st_mode & 0o777 == 0o600
