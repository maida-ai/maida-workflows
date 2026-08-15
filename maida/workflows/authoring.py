from __future__ import annotations

import types
import typing
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Any, cast

from ._canonical import schema_digest


class SymbolicValueError(TypeError):
    """Ordinary Python tried to observe a value that exists only at runtime."""


@dataclass(frozen=True)
class ExecutionContext:
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
    value_type: Any
    _expression: _Expression

    @classmethod
    def input[InputT](cls, value_type: type[InputT]) -> RuntimeValue[InputT]:
        return RuntimeValue(value_type=value_type, _expression=_Expression(kind="input"))

    def __bool__(self) -> bool:
        raise SymbolicValueError(
            "RuntimeValue is symbolic; use when(...) instead of Python if, and, or, or not"
        )

    def __iter__(self) -> typing.Never:
        raise SymbolicValueError(
            "RuntimeValue is symbolic; use map_over(...) instead of Python iteration"
        )

    def __len__(self) -> int:
        raise SymbolicValueError(
            "RuntimeValue is symbolic; use map_over(...) instead of len(...) or Python iteration"
        )


class Module[InputT, OutputT](ABC):
    module_id: str | None = None
    input_type: type[InputT]
    output_type: type[OutputT]
    effectful: bool = False

    def __call__(self, value: RuntimeValue[InputT]) -> RuntimeValue[OutputT]:
        return self._bind(value=value, logical_step=None, explicit=False)

    def at(self, logical_step: str) -> BoundModuleCall[InputT, OutputT]:
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
        """Execute one module boundary."""


@dataclass(frozen=True)
class _ModuleBinding:
    module: Module[Any, Any]
    logical_step: str | None
    explicit: bool


@dataclass(frozen=True)
class BoundModuleCall[InputT, OutputT]:
    module: Module[InputT, OutputT]
    logical_step: str

    def __call__(self, value: RuntimeValue[InputT]) -> RuntimeValue[OutputT]:
        return self.module._bind(value=value, logical_step=self.logical_step, explicit=True)


@dataclass(frozen=True)
class _WorkflowBinding:
    workflow: Workflow[Any, Any]
    output: RuntimeValue[Any]


class Workflow[InputT, OutputT](ABC):
    workflow_id: str
    input_type: type[InputT]
    output_type: type[OutputT]

    @abstractmethod
    def build(self, value: RuntimeValue[InputT]) -> RuntimeValue[OutputT]:
        """Build the static workflow graph."""

    def __call__(self, value: RuntimeValue[InputT]) -> RuntimeValue[OutputT]:
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
