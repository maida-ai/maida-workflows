from __future__ import annotations

import inspect
import marshal
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from typing import Any, cast

from ._canonical import (
    canonical_data,
    canonical_json,
    digest_bytes,
    digest_data,
    qualified_name,
    schema_digest,
    type_schema,
)
from .authoring import (
    Module,
    RuntimeValue,
    Workflow,
    _MapBinding,
    _ModuleBinding,
)

IR_VERSION = "0.1.0"


class CompileError(ValueError):
    """A workflow cannot be assigned unambiguous replay identities."""


@dataclass(frozen=True, order=True)
class ReplayKey:
    module_id: str
    logical_step: str

    def as_string(self) -> str:
        return f"{self.module_id}@{self.logical_step}"


@dataclass(frozen=True)
class BindingIR:
    source: str
    schema_digest: str


@dataclass(frozen=True)
class StepIR:
    node_id: str
    kind: str
    dependencies: tuple[str, ...]
    output_schema_digest: str
    module_id: str | None = None
    logical_step: str | None = None
    module_digest: str | None = None
    definition_digest: str | None = None
    input_binding: BindingIR | None = None
    control: Mapping[str, Any] | None = None

    @property
    def replay_key(self) -> ReplayKey | None:
        if self.module_id is None or self.logical_step is None:
            return None
        return ReplayKey(self.module_id, self.logical_step)


@dataclass(frozen=True)
class PlanIR:
    version: str
    workflow_id: str
    input_schema: Mapping[str, Any]
    output_schema: Mapping[str, Any]
    steps: tuple[StepIR, ...]
    output_node: str

    def to_dict(self) -> dict[str, Any]:
        return cast(dict[str, Any], canonical_data(asdict(self)))

    def canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return digest_data(self.to_dict())

    @property
    def executable_steps(self) -> tuple[StepIR, ...]:
        return tuple(step for step in self.steps if step.replay_key is not None)


def _behavior_bytes(module: Module[Any, Any]) -> bytes:
    function = module.__class__.execute
    try:
        return inspect.getsource(function).encode()
    except (OSError, TypeError):
        code = getattr(function, "__code__", None)
        if code is None:
            return qualified_name(function).encode()
        return marshal.dumps(code)


def module_digest(module: Module[Any, Any]) -> str:
    config = {
        key: value
        for key, value in vars(module).items()
        if key != "module_id" and not key.startswith("_")
    }
    payload = b"\0".join(
        (
            qualified_name(module.__class__).encode(),
            _behavior_bytes(module),
            canonical_json(config).encode(),
            schema_digest(module.input_type).encode(),
            schema_digest(module.output_type).encode(),
            str(module.effectful).encode(),
        )
    )
    return digest_bytes(payload)


def _callback_identity(callback: Callable[[Any], str]) -> str:
    try:
        content = inspect.getsource(callback).encode()
    except (OSError, TypeError):
        code = getattr(callback, "__code__", None)
        content = marshal.dumps(code) if code is not None else qualified_name(callback).encode()
    return f"{qualified_name(callback)}:{digest_bytes(content)}"


def _module_paths(workflow: Workflow[Any, Any]) -> dict[int, str]:
    found: dict[int, list[str]] = {}
    for name in sorted(dir(workflow)):
        if name.startswith("_"):
            continue
        try:
            value = getattr(workflow, name)
        except Exception as exc:  # pragma: no cover - hostile descriptors are invalid definitions
            raise CompileError(f"cannot inspect workflow attribute {name!r}: {exc}") from exc
        if isinstance(value, Module):
            found.setdefault(id(value), []).append(name)
    ambiguous = [paths for paths in found.values() if len(paths) > 1]
    if ambiguous:
        joined = ", ".join("/".join(paths) for paths in ambiguous)
        raise CompileError(f"the same module is assigned to multiple workflow attributes: {joined}")
    return {module_id: paths[0] for module_id, paths in found.items()}


class _Compiler:
    def __init__(self, workflow: Workflow[Any, Any]) -> None:
        self.root_workflow = workflow
        self.steps: list[StepIR] = []
        self.node_ids: dict[int, str] = {}
        self.occurrences: Counter[int] = Counter()
        self.keys: set[ReplayKey] = set()
        self.module_paths_by_workflow: dict[int, dict[int, str]] = {}

    def compile(self) -> PlanIR:
        self._validate_workflow(self.root_workflow)
        root_input = RuntimeValue.input(self.root_workflow.input_type)
        output = self.root_workflow.build(root_input)
        if output.value_type != self.root_workflow.output_type:
            raise CompileError(
                f"workflow {self.root_workflow.workflow_id!r} declares output "
                f"{self.root_workflow.output_type!r} but builds {output.value_type!r}"
            )
        output_node = self._visit(
            output,
            path="root",
            workflow=self.root_workflow,
            external_input="input",
        )
        return PlanIR(
            version=IR_VERSION,
            workflow_id=self.root_workflow.workflow_id,
            input_schema=type_schema(self.root_workflow.input_type),
            output_schema=type_schema(self.root_workflow.output_type),
            steps=tuple(self.steps),
            output_node=output_node,
        )

    def _validate_workflow(self, workflow: Workflow[Any, Any]) -> None:
        if not getattr(workflow, "workflow_id", ""):
            raise CompileError("workflow_id must be a non-empty stable identifier")
        self.module_paths_by_workflow[id(workflow)] = _module_paths(workflow)

    def _visit(
        self,
        value: RuntimeValue[Any],
        *,
        path: str,
        workflow: Workflow[Any, Any],
        external_input: str,
    ) -> str:
        expression = value._expression
        if expression.kind == "input":
            return external_input
        existing = self.node_ids.get(id(value))
        if existing is not None:
            return existing
        if expression.kind == "workflow":
            nested = expression.payload
            if not isinstance(nested, Workflow):
                raise CompileError("invalid nested workflow expression")
            self._validate_workflow(nested)
            source = self._visit(
                expression.dependencies[0],
                path=f"{path}.input",
                workflow=workflow,
                external_input=external_input,
            )
            nested_input = RuntimeValue.input(nested.input_type)
            nested_output = nested.build(nested_input)
            node_id = self._visit(
                nested_output,
                path=f"{path}.nested[{nested.workflow_id}]",
                workflow=nested,
                external_input=source,
            )
            self.node_ids[id(value)] = node_id
            return node_id
        dependencies = tuple(
            self._visit(
                dependency,
                path=f"{path}.dep{index}",
                workflow=workflow,
                external_input=external_input,
            )
            for index, dependency in enumerate(expression.dependencies)
        )
        node_id = path
        if expression.kind == "module":
            binding = expression.payload
            if not isinstance(binding, _ModuleBinding):
                raise CompileError("invalid module expression")
            step = self._module_step(
                binding.module,
                binding.logical_step,
                binding.explicit,
                path,
                workflow,
                dependencies,
                value,
                control=None,
            )
        elif expression.kind == "map":
            binding = expression.payload
            if not isinstance(binding, _MapBinding):
                raise CompileError("invalid map expression")
            item_key = (
                {"field": binding.item_key}
                if isinstance(binding.item_key, str)
                else {"callback": _callback_identity(binding.item_key)}
            )
            step = self._module_step(
                binding.module,
                binding.logical_step,
                binding.explicit,
                path,
                workflow,
                dependencies,
                value,
                control={"region": "map", "item_key": item_key},
            )
        elif expression.kind in {"when", "parallel"}:
            step = StepIR(
                node_id=node_id,
                kind=expression.kind,
                dependencies=dependencies,
                output_schema_digest=schema_digest(value.value_type),
                control={"region": expression.kind},
            )
        else:
            raise CompileError(f"unsupported Plan IR expression {expression.kind!r}")
        self.steps.append(step)
        self.node_ids[id(value)] = node_id
        return node_id

    def _module_step(
        self,
        module: Module[Any, Any],
        explicit_logical_step: str | None,
        explicit: bool,
        path: str,
        workflow: Workflow[Any, Any],
        dependencies: tuple[str, ...],
        value: RuntimeValue[Any],
        control: Mapping[str, Any] | None,
    ) -> StepIR:
        self.occurrences[id(module)] += 1
        if self.occurrences[id(module)] > 1 and not explicit:
            raise CompileError(
                "a reused module requires an explicit .at(logical_step) identity "
                "for every occurrence"
            )
        attr_path = self.module_paths_by_workflow[id(workflow)].get(id(module))
        module_id = module.module_id
        if module_id is None:
            if attr_path is None:
                raise CompileError(
                    "a module without module_id must be assigned to a named workflow attribute"
                )
            module_id = f"{workflow.workflow_id}.{attr_path}"
        logical_step = explicit_logical_step if explicit else path
        if logical_step is None:  # defensive: explicit bindings always carry a step
            raise CompileError("module occurrence has no logical_step")
        key = ReplayKey(module_id, logical_step)
        if key in self.keys:
            raise CompileError(f"duplicate replay key {key.as_string()}")
        self.keys.add(key)
        behavior_digest = module_digest(module)
        input_digest = schema_digest(module.input_type)
        output_digest = schema_digest(module.output_type)
        definition_digest = digest_data(
            {
                "module_id": module_id,
                "logical_step": logical_step,
                "module_digest": behavior_digest,
                "input_schema_digest": input_digest,
                "output_schema_digest": output_digest,
                "control": control,
            }
        )
        return StepIR(
            node_id=path,
            kind="map_module" if control and control.get("region") == "map" else "module",
            dependencies=dependencies,
            output_schema_digest=output_digest,
            module_id=module_id,
            logical_step=logical_step,
            module_digest=behavior_digest,
            definition_digest=definition_digest,
            input_binding=BindingIR(source=dependencies[0], schema_digest=input_digest),
            control=control,
        )


def compile_workflow(workflow: Workflow[Any, Any]) -> PlanIR:
    return _Compiler(workflow).compile()
