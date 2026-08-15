"""Define typed modules and compose them into static workflows.

Workflow construction uses symbolic :class:`RuntimeValue` objects. A
``Workflow.build`` method describes dependencies and control flow; actual I/O
and computation belong in ``Module.execute``. Use :func:`when`,
:func:`parallel`, and :func:`map_over` instead of ordinary Python control flow
over symbolic values.
"""

from __future__ import annotations

import dataclasses
import types
import typing
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Any, cast

from ._canonical import canonical_data, schema_digest, value_matches_type
from .budget import Budget
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
class _FieldBinding:
    path: tuple[str, ...]


@dataclass(frozen=True)
class _StructuredBinding:
    kind: str
    names: tuple[str, ...] = ()


_MISSING = object()


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

    def field(self, path: str) -> RuntimeValue[Any]:
        """Project a typed field from a symbolic object.

        Parameters
        ----------
        path
            Dot-separated field path. Each component must exist in the
            declared dataclass, typed dictionary, or mapping contract.

        Returns
        -------
        RuntimeValue
            Symbolic reference carrying the projected field's Python type.

        Raises
        ------
        ValueError
            If the path is empty or absent from the declared object contract.

        Examples
        --------
        >>> request = RuntimeValue.input(Request)  # doctest: +SKIP
        >>> email = request.field("customer.email")  # doctest: +SKIP
        """
        parts = tuple(part for part in path.split(".") if part)
        if not parts or ".".join(parts) != path:
            raise ValueError("field path must contain non-empty dot-separated names")
        projected = _projected_type(self.value_type, parts)
        return RuntimeValue(
            value_type=projected,
            _expression=_Expression(
                kind="field",
                dependencies=(cast(RuntimeValue[Any], self),),
                payload=_FieldBinding(parts),
            ),
        )

    def __getattr__(self, name: str) -> RuntimeValue[Any]:
        """Provide ``value.name`` sugar for :meth:`field` projections."""
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            return self.field(name)
        except ValueError as exc:
            raise AttributeError(name) from exc


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
    budget
        Immutable resource-limit declaration included in the module content
        identity and durable task envelope. Runtime integrations meter and
        enforce consumption separately.
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
    budget: Budget = Budget()
    capabilities: tuple[Any, ...] = ()
    effects: tuple[Any, ...] = ()

    @typing.overload
    def __call__(self, value: RuntimeValue[InputT], /) -> RuntimeValue[OutputT]: ...

    @typing.overload
    def __call__(self, /, **fields: Any) -> RuntimeValue[OutputT]: ...

    def __call__(
        self,
        value: RuntimeValue[InputT] | object = _MISSING,
        /,
        **fields: Any,
    ) -> RuntimeValue[OutputT]:
        """Create a symbolic output connected to this module occurrence.

        The handler is not executed. Calling a module inside ``Workflow.build``
        only records a typed dependency in the workflow definition.
        """
        return self._bind(
            value=_module_input(self.input_type, value, fields),
            logical_step=None,
            explicit=False,
        )

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
                payload=_ModuleBinding(self, logical_step, explicit, value),
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
    input_value: RuntimeValue[Any]


@dataclass(frozen=True)
class BoundModuleCall[InputT, OutputT]:
    """Module occurrence carrying an explicit replay-stable logical step.

    Instances are returned by :meth:`Module.at` and are called with a symbolic
    input during workflow construction.
    """

    module: Module[InputT, OutputT]
    logical_step: str

    @typing.overload
    def __call__(self, value: RuntimeValue[InputT], /) -> RuntimeValue[OutputT]: ...

    @typing.overload
    def __call__(self, /, **fields: Any) -> RuntimeValue[OutputT]: ...

    def __call__(
        self,
        value: RuntimeValue[InputT] | object = _MISSING,
        /,
        **fields: Any,
    ) -> RuntimeValue[OutputT]:
        """Create a symbolic output at the previously bound logical step."""
        return self.module._bind(
            value=_module_input(self.module.input_type, value, fields),
            logical_step=self.logical_step,
            explicit=True,
        )


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


def literal[ValueT](value: ValueT, value_type: Any | None = None) -> RuntimeValue[ValueT]:
    """Create a durable symbolic literal for a workflow binding.

    Parameters
    ----------
    value
        JSON-compatible immutable value embedded in the workflow definition.
    value_type
        Optional declared Python contract. By default ``type(value)`` is used.

    Returns
    -------
    RuntimeValue
        Symbolic literal that can participate in structured module inputs.

    Raises
    ------
    TypeError
        If the value violates the requested type or is not canonically
        serializable.

    Notes
    -----
    Literals become part of the definition digest. Secrets and credentials
    must never be embedded; resolve them through a runtime provider instead.
    """
    annotation = type(value) if value_type is None else value_type
    if not value_matches_type(value, annotation):
        raise TypeError(f"literal value does not match {annotation!r}")
    encoded = canonical_data(value)
    return RuntimeValue(
        value_type=annotation,
        _expression=_Expression(kind="literal", payload=encoded),
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


def _module_input(
    input_type: Any,
    value: object,
    fields_by_name: Mapping[str, Any],
) -> RuntimeValue[Any]:
    if value is not _MISSING and fields_by_name:
        raise TypeError("module calls accept either one symbolic value or keyword bindings")
    if value is not _MISSING:
        if not isinstance(value, RuntimeValue):
            raise TypeError("positional module input must be a RuntimeValue")
        return value
    if not fields_by_name:
        raise TypeError("module call requires a symbolic value or keyword bindings")
    return _structured_value(input_type, fields_by_name)


def _structured_value(input_type: Any, supplied: Mapping[str, Any]) -> RuntimeValue[Any]:
    hints: Mapping[str, Any]
    required: set[str]
    if is_dataclass(input_type):
        hints = typing.get_type_hints(input_type)
        declared_fields = {item.name: item for item in fields(input_type)}
        required = {
            name
            for name, item in declared_fields.items()
            if item.default is dataclasses.MISSING and item.default_factory is dataclasses.MISSING
        }
    elif typing.is_typeddict(input_type):
        hints = typing.get_type_hints(input_type)
        required = set(getattr(input_type, "__required_keys__", hints))
    else:
        origin = typing.get_origin(input_type)
        arguments = typing.get_args(input_type)
        if origin not in (dict, Mapping):
            raise TypeError(
                "keyword module bindings require a dataclass, TypedDict, or mapping input type"
            )
        value_type = arguments[1] if len(arguments) > 1 else Any
        hints = {name: value_type for name in supplied}
        required = set(supplied)
    unknown = set(supplied) - set(hints)
    if unknown:
        raise TypeError(f"module keyword bindings contain unknown fields: {sorted(unknown)}")
    missing = required - set(supplied)
    if missing:
        raise TypeError(f"module keyword bindings are missing required fields: {sorted(missing)}")
    names = tuple(sorted(supplied))
    dependencies = tuple(
        _binding_value(supplied[name], hints.get(name, Any), name) for name in names
    )
    return RuntimeValue(
        value_type=input_type,
        _expression=_Expression(
            kind="object",
            dependencies=dependencies,
            payload=_StructuredBinding("object", names),
        ),
    )


def _binding_value(value: Any, expected: Any, name: str) -> RuntimeValue[Any]:
    try:
        symbolic = value if isinstance(value, RuntimeValue) else literal(value, expected)
    except TypeError as exc:
        raise TypeError(f"module keyword binding {name!r} violates {expected!r}") from exc
    _require_type_handoff(
        symbolic.value_type,
        expected,
        boundary=f"module keyword binding {name!r}",
    )
    return symbolic


def _projected_type(annotation: Any, path: tuple[str, ...]) -> Any:
    current = annotation
    traversed: list[str] = []
    for part in path:
        traversed.append(part)
        if is_dataclass(current) or typing.is_typeddict(current):
            hints = typing.get_type_hints(current)
            if part not in hints:
                raise ValueError(f"{current!r} has no field {'.'.join(traversed)!r}")
            current = hints[part]
            continue
        origin = typing.get_origin(current)
        arguments = typing.get_args(current)
        if origin in (dict, Mapping):
            current = arguments[1] if len(arguments) > 1 else Any
            continue
        raise ValueError(f"{current!r} has no field {'.'.join(traversed)!r}")
    return current
