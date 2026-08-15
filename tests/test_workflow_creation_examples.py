from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from types import ModuleType
from typing import Any, cast

import pytest

from examples.workflow_creation import (
    advanced_nested,
    advanced_stable_map,
    easy_first_workflow,
    easy_sequential,
    expert_replay_ready,
    intermediate_branching,
    intermediate_parallel,
)
from maida_workflows import Workflow, compile_workflow
from maida_workflows.persistence import PostgresStore
from maida_workflows.runtime import WorkflowRunner


@dataclass(frozen=True)
class ExampleCase:
    module: ModuleType
    expected_step_kinds: frozenset[str]

    @property
    def id(self) -> str:
        return self.module.__name__.rsplit(".", maxsplit=1)[-1]

    @property
    def workflow(self) -> Workflow[Any, Any]:
        return cast(Workflow[Any, Any], self.module.workflow)


EXAMPLES = (
    ExampleCase(easy_first_workflow, frozenset({"module"})),
    ExampleCase(easy_sequential, frozenset({"module"})),
    ExampleCase(intermediate_branching, frozenset({"module", "when"})),
    ExampleCase(intermediate_parallel, frozenset({"module", "parallel"})),
    ExampleCase(advanced_stable_map, frozenset({"module", "map_module"})),
    ExampleCase(advanced_nested, frozenset({"module", "parallel"})),
    ExampleCase(
        expert_replay_ready,
        frozenset({"module", "map_module", "parallel", "when"}),
    ),
)


@pytest.mark.parametrize("case", EXAMPLES, ids=lambda case: case.id)
def test_workflow_creation_examples_compile_canonically(case: ExampleCase) -> None:
    first = compile_workflow(case.workflow)
    second = compile_workflow(case.workflow)

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
