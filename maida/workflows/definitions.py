"""Bind compiled workflow definitions to trusted executable modules.

The classes in this module separate a workflow's portable, inspectable graph
from the application-owned Python objects that may execute its module
boundaries.  Loading a definition never imports code.  A definition becomes
executable only after :class:`BoundWorkflow` verifies exact module identities,
digests, and type contracts.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from ._canonical import canonical_data, schema_digest, value_matches_type
from ._schema import value_matches_schema
from .authoring import Module, RuntimeValue, Workflow
from .ir import PlanIR, ReplayKey, _compile_workflow_graph, module_digest
from .model import _model_contract


@dataclass(frozen=True)
class BoundWorkflow:
    """Executable binding of canonical Workflow IR to trusted Python modules.

    Parameters
    ----------
    plan
        Canonical workflow graph whose identities and contracts are
        authoritative for scheduling, replay, and verification.
    input_type, output_type
        Concrete Python root types used to validate and rehydrate durable
        values.  Their schema digests must exactly match ``plan``.
    modules
        Trusted module instances keyed by every executable replay address in
        ``plan``.  Each instance is re-digested during construction.

    Raises
    ------
    TypeError
        If a module binding is not a :class:`Module` instance.
    ValueError
        If root schemas, module keys, module digests, or module schemas do not
        match the compiled definition.

    Notes
    -----
    ``BoundWorkflow`` contains live Python objects and is intentionally not a
    serialization format.  Portable workflow bundles contain only canonical
    data and must pass through a trusted registry before producing this type.
    Physical executor placement is not part of the binding identity.

    Examples
    --------
    >>> bound = bind_workflow(MyWorkflow())  # doctest: +SKIP
    >>> bound.plan.workflow_id
    'my-workflow'
    """

    plan: PlanIR
    input_type: Any
    output_type: Any
    modules: Mapping[ReplayKey, Module[Any, Any]]
    map_item_keys: Mapping[str, str | Callable[[Any], str]] | None = None
    _authoring_output: RuntimeValue[Any] | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        """Validate every executable contract and freeze the module mapping."""
        if self.input_type is not Any and schema_digest(self.input_type) != _schema_data_digest(
            self.plan.input_schema
        ):
            raise ValueError("bound input type does not match the workflow input schema")
        if self.output_type is not Any and schema_digest(self.output_type) != _schema_data_digest(
            self.plan.output_schema
        ):
            raise ValueError("bound output type does not match the workflow output schema")

        supplied = dict(self.modules)
        expected = {
            step.replay_key: step
            for step in self.plan.executable_steps
            if step.replay_key is not None
        }
        if set(supplied) != set(expected):
            missing = sorted(key.as_string() for key in set(expected) - set(supplied))
            extra = sorted(key.as_string() for key in set(supplied) - set(expected))
            raise ValueError(
                f"module bindings do not match the plan; missing={missing}, extra={extra}"
            )
        for key, module in supplied.items():
            if not isinstance(module, Module):
                raise TypeError(f"binding {key.as_string()} must be a Module instance")
            step = expected[key]
            if module_digest(module) != step.module_digest:
                raise ValueError(f"module digest does not match {key.as_string()}")
            if step.input_binding is None:
                raise ValueError(f"executable step {key.as_string()} has no input binding")
            if schema_digest(module.input_type) != step.input_binding.schema_digest:
                raise ValueError(f"module input schema does not match {key.as_string()}")
            if schema_digest(module.output_type) != step.output_schema_digest:
                raise ValueError(f"module output schema does not match {key.as_string()}")
            if _model_contract(module) != step.models:
                raise ValueError(f"module model declarations do not match {key.as_string()}")
        object.__setattr__(self, "modules", MappingProxyType(supplied))
        map_item_keys = dict(self.map_item_keys or {})
        expected_maps = {step.node_id for step in self.plan.steps if step.kind == "map_module"}
        if not set(map_item_keys).issubset(expected_maps):
            raise ValueError("map item-key bindings contain nodes absent from the plan")
        object.__setattr__(self, "map_item_keys", MappingProxyType(map_item_keys))

    def accepts_input(self, value: Any) -> bool:
        """Return whether a root value satisfies the compiled input schema."""
        if self.input_type is not Any:
            return value_matches_type(value, self.input_type)
        return value_matches_schema(canonical_data(value), self.plan.input_schema)

    def accepts_output(self, value: Any) -> bool:
        """Return whether a terminal value satisfies the compiled output schema."""
        if self.output_type is not Any:
            return value_matches_type(value, self.output_type)
        return value_matches_schema(canonical_data(value), self.plan.output_schema)

    @classmethod
    def from_workflow(cls, workflow: Workflow[Any, Any]) -> BoundWorkflow:
        """Compile and bind a native Python workflow exactly once.

        Parameters
        ----------
        workflow
            Native workflow instance whose pure ``build`` method describes the
            graph and whose module objects provide trusted handlers.

        Returns
        -------
        BoundWorkflow
            Validated executable definition using the compiled graph as its
            scheduling authority.
        """
        compiled = _compile_workflow_graph(workflow)
        return cls(
            plan=compiled.plan,
            input_type=workflow.input_type,
            output_type=workflow.output_type,
            modules=compiled.modules,
            map_item_keys=compiled.map_item_keys,
            _authoring_output=compiled.output,
        )


def bind_workflow(workflow: Workflow[Any, Any]) -> BoundWorkflow:
    """Compile a native workflow into an exact trusted executable binding.

    Parameters
    ----------
    workflow
        Native workflow definition to compile and bind.

    Returns
    -------
    BoundWorkflow
        Definition that can be submitted, scheduled, or executed without
        rebuilding the Python authoring graph.
    """
    return BoundWorkflow.from_workflow(workflow)


def _schema_data_digest(schema: Mapping[str, Any]) -> str:
    from ._canonical import digest_data

    return digest_data(schema)
