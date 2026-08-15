from __future__ import annotations

import types
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, cast


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


class Workflow[InputT, OutputT](ABC):
    workflow_id: str
    input_type: type[InputT]
    output_type: type[OutputT]

    @abstractmethod
    def build(self, value: RuntimeValue[InputT]) -> RuntimeValue[OutputT]:
        """Build the static workflow graph."""

    def __call__(self, value: RuntimeValue[InputT]) -> RuntimeValue[OutputT]:
        return RuntimeValue(
            value_type=self.output_type,
            _expression=_Expression(
                kind="workflow",
                dependencies=(cast(RuntimeValue[Any], value),),
                payload=self,
            ),
        )


def when[OutputT](
    condition: RuntimeValue[bool],
    if_true: RuntimeValue[OutputT],
    if_false: RuntimeValue[OutputT],
) -> RuntimeValue[OutputT]:
    if if_true.value_type != if_false.value_type:
        raise TypeError("when branches must have the same output type")
    return RuntimeValue(
        value_type=if_true.value_type,
        _expression=_Expression(
            kind="when",
            dependencies=(
                cast(RuntimeValue[Any], condition),
                cast(RuntimeValue[Any], if_true),
                cast(RuntimeValue[Any], if_false),
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
    else:
        binding = _MapBinding(module, None, False, item_key)
        output_type = module.output_type
    return RuntimeValue(
        value_type=list[output_type],  # type: ignore[valid-type]
        _expression=_Expression(
            kind="map",
            dependencies=(cast(RuntimeValue[Any], values),),
            payload=binding,
        ),
    )
