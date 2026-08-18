"""Advance durable runs independently from API processes and task workers.

The coordinator resolves each active run through its pinned workflow-definition
digest and performs non-executing scheduler passes. It never calls module
handlers. A catalog is an infrastructure registry of Python factories, not a
new durable workflow noun, so deployments may replace it with their own
definition loader while keeping scheduling semantics unchanged.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Protocol

from .authoring import Workflow
from .ir import compile_workflow
from .models import RunStatus
from .runtime import DurableRuntimeStore, WorkflowScheduler

WorkflowFactory = Callable[[], Workflow[Any, Any]]


@dataclass(frozen=True)
class _CatalogDefinition:
    workflow_id: str
    definition_digest: str


class WorkflowCatalog:
    """Resolve immutable workflow definitions from application-owned factories.

    Parameters
    ----------
    factories
        Zero-argument callables, commonly workflow classes, that return fresh
        :class:`~maida.workflows.Workflow` instances.

    Notes
    -----
    The catalog stores no credentials and performs no deployment discovery.
    Each resolution recompiles a fresh instance and verifies the digest, which
    prevents a changed factory from silently executing work pinned to an older
    definition.

    Examples
    --------
    >>> catalog = WorkflowCatalog([SupportWorkflow])  # doctest: +SKIP
    >>> catalog.definitions[0].workflow_id  # doctest: +SKIP
    'support'
    """

    def __init__(self, factories: Iterable[WorkflowFactory] = ()) -> None:
        self._factories: dict[str, WorkflowFactory] = {}
        self._definitions: dict[str, _CatalogDefinition] = {}
        for factory in factories:
            self.register(factory)

    @property
    def definitions(self) -> tuple[_CatalogDefinition, ...]:
        """Return registered immutable definitions ordered by digest."""
        return tuple(self._definitions[key] for key in sorted(self._definitions))

    def register(self, factory: WorkflowFactory) -> _CatalogDefinition:
        """Compile and register a workflow factory by exact definition digest.

        Re-registering the same definition is idempotent. Multiple versions of
        the same workflow ID may coexist because active runs resolve by digest.
        """
        workflow = factory()
        if not isinstance(workflow, Workflow):
            raise TypeError("workflow factory must return a Workflow instance")
        plan = compile_workflow(workflow)
        definition = _CatalogDefinition(plan.workflow_id, plan.digest)
        self._factories[plan.digest] = factory
        self._definitions[plan.digest] = definition
        return definition

    def resolve(self, definition_digest: str) -> Workflow[Any, Any]:
        """Return a fresh workflow matching an exact persisted definition.

        Raises
        ------
        ValueError
            If no factory is registered or its current output no longer
            compiles to the requested digest.
        """
        factory = self._factories.get(definition_digest)
        if factory is None:
            raise ValueError(f"definition {definition_digest!r} is not registered")
        workflow = factory()
        if compile_workflow(workflow).digest != definition_digest:
            raise ValueError("registered workflow factory no longer matches its pinned digest")
        return workflow

    def resolve_workflow(self, workflow_id: str) -> Workflow[Any, Any]:
        """Return the single registered definition for a workflow ID.

        Raises
        ------
        ValueError
            If no definition is registered or multiple versions make the
            unversioned application address ambiguous.
        """
        matches = [
            definition
            for definition in self._definitions.values()
            if definition.workflow_id == workflow_id
        ]
        if not matches:
            raise ValueError(f"workflow {workflow_id!r} is not registered")
        if len(matches) > 1:
            raise ValueError(
                f"workflow {workflow_id!r} has multiple registered definitions; "
                "select a deployment-pinned catalog"
            )
        return self.resolve(matches[0].definition_digest)


class _CoordinatorStore(DurableRuntimeStore, Protocol):
    def list_active_runs(
        self,
        *,
        limit: int,
    ) -> tuple[tuple[str, str, str], ...]: ...


@dataclass(frozen=True)
class CoordinatorProgress:
    """Aggregate outcome of one non-blocking control-plane scheduling pass."""

    scanned_runs: int
    advanced_runs: int
    unavailable_runs: int
    ready_tasks: int
    completed_runs: int


class WorkflowCoordinator:
    """Advance active runs using exact catalog-pinned workflow definitions.

    Parameters
    ----------
    store
        Durable runtime store used by schedulers and workers.
    catalog
        Factory catalog able to resolve persisted definition digests.

    Notes
    -----
    A coordinator may restart or run in multiple replicas. Scheduler passes and
    task insertion are idempotent, and no module handler is ever invoked here.
    """

    def __init__(self, store: _CoordinatorStore, catalog: WorkflowCatalog) -> None:
        self.store = store
        self.catalog = catalog

    def run_once(self, *, limit: int = 100) -> CoordinatorProgress:
        """Advance at most ``limit`` active runs and return aggregate progress.

        Definitions not available in this coordinator's catalog remain durable
        and running; they are counted as unavailable rather than failed.
        """
        if limit < 1 or limit > 10_000:
            raise ValueError("limit must be between 1 and 10000")
        active = self.store.list_active_runs(limit=limit)
        advanced = unavailable = ready = completed = 0
        for run_id, tenant_id, definition_digest in active:
            try:
                workflow = self.catalog.resolve(definition_digest)
            except ValueError:
                unavailable += 1
                continue
            progress = WorkflowScheduler.resume(
                self.store,
                workflow,
                run_id,
                tenant_id=tenant_id,
            ).advance()
            advanced += 1
            ready += progress.ready_tasks
            completed += int(progress.status is RunStatus.SUCCEEDED)
        return CoordinatorProgress(len(active), advanced, unavailable, ready, completed)
