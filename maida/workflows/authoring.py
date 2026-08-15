"""Define typed modules and compose them into static workflows.

Workflow construction uses symbolic :class:`RuntimeValue` objects. A
``Workflow.build`` method describes dependencies and control flow; actual I/O
and computation belong in ``Module.execute``. Use :func:`when`,
:func:`parallel`, and :func:`map_over` instead of ordinary Python control flow
over symbolic values.
"""

from __future__ import annotations

import types
import typing
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Any, cast

from ._canonical import schema_digest
from .models import ExecutionSpec


class SymbolicValueError(TypeError):
    """Ordinary Python tried to observe a value that exists only at runtime."""


@dataclass(frozen=True)
class ExecutionContext:
    """Runtime metadata supplied to one module execution.

    Attributes
    ----------
    run_id
        Identifier of the workflow run that owns the task.
    task_id
        Durable identifier of the logical task being attempted.
    step_instance_id
        Deterministic identity of this execution instance.
    replay
        Whether the module is running as a selective replay target.
    broker
        Runtime-managed broker for supported reads and effects.
    metadata
        Mutable collection for trajectories, usage, and other boundary data.
    """

    run_id: str
    task_id: str
    step_instance_id: str
    replay: bool = False
    broker: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class _Expression:
    kind: str
    dependencies: tuple[RuntimeValue[Any], ...] = ()
    payload: Any = None


@dataclass(frozen=True)
class RuntimeValue[OutputT]:
    """Symbolic reference to a value that will exist during execution.

    ``RuntimeValue`` objects are created while a workflow graph is being built.
    They cannot be inspected, iterated, or used as Python booleans because no
    runtime value exists at construction time.

    Parameters
    ----------
    value_type
        Declared Python type of the future value.

    Notes
    -----
    Workflow authors normally receive these objects from ``Workflow.build`` or
    from module calls rather than constructing them directly.
    """

    value_type: Any
    _expression: _Expression

    @classmethod
    def input[InputT](cls, value_type: type[InputT]) -> RuntimeValue[InputT]:
        """Create the symbolic root input for a workflow definition.

        Parameters
        ----------
        value_type
            Python type accepted by the workflow.

        Returns
        -------
        RuntimeValue
            Symbolic input carrying the requested type contract.
        """
        return RuntimeValue(value_type=value_type, _expression=_Expression(kind="input"))

    def __bool__(self) -> bool:
        """Reject ordinary Python truth testing of a symbolic value."""
        raise SymbolicValueError(
            "RuntimeValue is symbolic; use when(...) instead of Python if, and, or, or not"
        )

    def __iter__(self) -> typing.Never:
        """Reject ordinary Python iteration over a symbolic collection."""
        raise SymbolicValueError(
            "RuntimeValue is symbolic; use map_over(...) instead of Python iteration"
        )

    def __len__(self) -> int:
        """Reject measuring a collection that exists only during execution."""
        raise SymbolicValueError(
            "RuntimeValue is symbolic; use map_over(...) instead of len(...) or Python iteration"
        )


class Module[InputT, OutputT](ABC):
    """Typed unit of workflow execution.

    Subclasses declare ``input_type`` and ``output_type`` and implement
    :meth:`execute`. Assign module instances to workflow attributes so the
    compiler can derive stable identities. Reused instances must use
    :meth:`at` for every occurrence.

    Attributes
    ----------
    module_id
        Optional stable component identity. If omitted, the workflow attribute
        path supplies a default.
    input_type
        Python type accepted by the module.
    output_type
        Python type returned by the module.
    effectful
        Whether executing the module may commit an external effect.
    execution
        Immutable environment requirements used to match durable tasks to
        capable executors.
    capabilities, effects
        Typed external access declarations compiled into the module boundary.
        Explicit :class:`~maida.workflows.Connector` and
        :class:`~maida.workflows.Effect` modules provide the easiest path.
    """

    module_id: str | None = None
    input_type: type[InputT]
    output_type: type[OutputT]
    effectful: bool = False
    execution: ExecutionSpec = ExecutionSpec()
    capabilities: tuple[Any, ...] = ()
    effects: tuple[Any, ...] = ()

    def __call__(self, value: RuntimeValue[InputT]) -> RuntimeValue[OutputT]:
        """Create a symbolic output connected to this module occurrence.

        The handler is not executed. Calling a module inside ``Workflow.build``
        only records a typed dependency in the workflow definition.
        """
        return self._bind(value=value, logical_step=None, explicit=False)

    def at(self, logical_step: str) -> BoundModuleCall[InputT, OutputT]:
        """Bind this module occurrence to an explicit logical step.

        Parameters
        ----------
        logical_step
            Non-empty stable identity for the occurrence within the workflow.

        Returns
        -------
        BoundModuleCall
            Callable symbolic binding that retains the supplied identity.

        Raises
        ------
        ValueError
            If ``logical_step`` is empty or contains only whitespace.
        """
        if not logical_step or not logical_step.strip():
            raise ValueError("logical_step must be a non-empty stable identifier")
        return BoundModuleCall(module=self, logical_step=logical_step)

    def _bind(
        self,
        *,
        value: RuntimeValue[InputT],
        logical_step: str | None,
        explicit: bool,
    ) -> RuntimeValue[OutputT]:
        _require_type_handoff(
            value.value_type,
            self.input_type,
            boundary=f"module {type(self).__qualname__} input contract",
        )
        return RuntimeValue(
            value_type=self.output_type,
            _expression=_Expression(
                kind="module",
                dependencies=(cast(RuntimeValue[Any], value),),
                payload=_ModuleBinding(self, logical_step, explicit),
            ),
        )

    @abstractmethod
    async def execute(self, value: InputT, ctx: ExecutionContext) -> OutputT:
        """Execute one typed module boundary.

        Parameters
        ----------
        value
            Concrete runtime input matching :attr:`input_type`.
        ctx
            Execution metadata and runtime-managed broker access.

        Returns
        -------
        OutputT
            Concrete output matching :attr:`output_type`.
        """


@dataclass(frozen=True)
class _ModuleBinding:
    module: Module[Any, Any]
    logical_step: str | None
    explicit: bool


@dataclass(frozen=True)
class BoundModuleCall[InputT, OutputT]:
    """Module occurrence carrying an explicit replay-stable logical step.

    Instances are returned by :meth:`Module.at` and are called with a symbolic
    input during workflow construction.
    """

    module: Module[InputT, OutputT]
    logical_step: str

    def __call__(self, value: RuntimeValue[InputT]) -> RuntimeValue[OutputT]:
        """Create a symbolic output at the previously bound logical step."""
        return self.module._bind(value=value, logical_step=self.logical_step, explicit=True)


@dataclass(frozen=True)
class _WorkflowBinding:
    workflow: Workflow[Any, Any]
    output: RuntimeValue[Any]


class Workflow[InputT, OutputT](ABC):
    """Static composition of typed modules and child workflows.

    Subclasses declare a stable ``workflow_id`` and root input/output types.
    Implement :meth:`build` using only symbolic composition; runtime work must
    remain inside module handlers.

    Attributes
    ----------
    workflow_id
        Stable identity of the workflow definition.
    input_type
        Python type accepted as the root input.
    output_type
        Python type produced as the terminal output.
    """

    workflow_id: str
    input_type: type[InputT]
    output_type: type[OutputT]

    @abstractmethod
    def build(self, value: RuntimeValue[InputT]) -> RuntimeValue[OutputT]:
        """Describe the workflow graph with symbolic values.

        Parameters
        ----------
        value
            Symbolic root input matching :attr:`input_type`.

        Returns
        -------
        RuntimeValue
            Symbolic terminal value matching :attr:`output_type`.

        Notes
        -----
        This method must be deterministic and free of runtime side effects.
        """

    def __call__(self, value: RuntimeValue[InputT]) -> RuntimeValue[OutputT]:
        """Compose this workflow as a typed child of another workflow.

        The child graph is constructed symbolically and its declared root
        contracts are validated before composition succeeds.
        """
        _require_type_handoff(
            value.value_type,
            self.input_type,
            boundary=f"workflow {self.workflow_id!r} input contract",
        )
        nested_input = RuntimeValue.input(self.input_type)
        nested_output = self.build(nested_input)
        _require_type_handoff(
            nested_output.value_type,
            self.output_type,
            boundary=f"workflow {self.workflow_id!r} output contract",
        )
        return RuntimeValue(
            value_type=self.output_type,
            _expression=_Expression(
                kind="workflow",
                dependencies=(cast(RuntimeValue[Any], value),),
                payload=_WorkflowBinding(self, cast(RuntimeValue[Any], nested_output)),
            ),
        )


def when[OutputT](
    condition: RuntimeValue[bool],
    then: RuntimeValue[OutputT],
    otherwise: RuntimeValue[OutputT],
) -> RuntimeValue[OutputT]:
    """Select one of two symbolic values from a runtime condition.

    Parameters
    ----------
    condition
        Symbolic boolean evaluated during workflow execution.
    then
        Value selected when ``condition`` is true.
    otherwise
        Value selected when ``condition`` is false.

    Returns
    -------
    RuntimeValue
        Symbolic value with the common branch output type.

    Raises
    ------
    TypeError
        If the condition is not boolean or the branch schemas differ.
    """
    _require_type_handoff(condition.value_type, bool, boundary="when condition")
    if schema_digest(then.value_type) != schema_digest(otherwise.value_type):
        raise TypeError("when branches must have the same output type")
    return RuntimeValue(
        value_type=then.value_type,
        _expression=_Expression(
            kind="when",
            dependencies=(
                cast(RuntimeValue[Any], condition),
                cast(RuntimeValue[Any], then),
                cast(RuntimeValue[Any], otherwise),
            ),
        ),
    )


def parallel(*values: RuntimeValue[Any]) -> RuntimeValue[tuple[Any, ...]]:
    """Combine independent symbolic values into a typed tuple.

    Parameters
    ----------
    *values
        One or more symbolic computations that do not depend on each other.

    Returns
    -------
    RuntimeValue
        Symbolic tuple preserving the argument order and element types.

    Raises
    ------
    ValueError
        If no values are supplied.
    """
    if not values:
        raise ValueError("parallel requires at least one value")
    output_type = types.GenericAlias(tuple, tuple(value.value_type for value in values))
    return RuntimeValue(
        value_type=output_type,
        _expression=_Expression(kind="parallel", dependencies=tuple(values)),
    )


@dataclass(frozen=True)
class _MapBinding:
    module: Module[Any, Any]
    logical_step: str | None
    explicit: bool
    item_key: str | Callable[[Any], str]


def map_over[ItemT, OutputT](
    values: RuntimeValue[Sequence[ItemT]],
    module: Module[ItemT, OutputT] | BoundModuleCall[ItemT, OutputT],
    *,
    item_key: str | Callable[[ItemT], str],
) -> RuntimeValue[list[OutputT]]:
    """Apply a module to each runtime collection item using stable identity.

    Parameters
    ----------
    values
        Symbolic sequence whose items match the module input contract.
    module
        Module, or explicitly bound module occurrence, applied to each item.
    item_key
        Field name or callback returning a stable, non-empty key for each item.
        Collection position is intentionally not used as replay identity.

    Returns
    -------
    RuntimeValue
        Symbolic list of module outputs in input order.

    Raises
    ------
    TypeError
        If the item-key strategy is invalid or item/module types are
        incompatible.
    ValueError
        If the field name is empty or absent from a declared dataclass item.
    """
    if isinstance(item_key, str) and not item_key.strip():
        raise ValueError("map_over item_key must be a non-empty field name")
    if not isinstance(item_key, str) and not callable(item_key):
        raise TypeError("map_over item_key must be a field name or callback")
    if isinstance(module, BoundModuleCall):
        binding = _MapBinding(module.module, module.logical_step, True, item_key)
        output_type = module.module.output_type
        module_input_type = module.module.input_type
    else:
        binding = _MapBinding(module, None, False, item_key)
        output_type = module.output_type
        module_input_type = module.input_type
    sequence_arguments = typing.get_args(values.value_type)
    if sequence_arguments:
        item_type = sequence_arguments[0]
        if isinstance(item_key, str) and is_dataclass(item_type):
            available_fields = {item.name for item in fields(item_type)}
            if item_key not in available_fields:
                raise ValueError(f"map_over item_key {item_key!r} is not a field on {item_type!r}")
        _require_type_handoff(
            item_type,
            module_input_type,
            boundary=f"map module {type(binding.module).__qualname__} input contract",
        )
    return RuntimeValue(
        value_type=list[output_type],  # type: ignore[valid-type]
        _expression=_Expression(
            kind="map",
            dependencies=(cast(RuntimeValue[Any], values),),
            payload=binding,
        ),
    )


def _require_type_handoff(source: Any, target: Any, *, boundary: str) -> None:
    if target in {Any, typing.Any} or source in {Any, typing.Any}:
        return
    if schema_digest(source) != schema_digest(target):
        raise TypeError(f"{boundary} expects {target!r}, received symbolic {source!r}")
