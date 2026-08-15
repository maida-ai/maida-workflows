"""Compile workflow definitions into canonical, replay-addressable IR.

The public objects in this module describe workflow structure without runtime
payloads. :func:`compile_workflow` is the usual entry point; :class:`PlanIR`
and :class:`StepIR` support inspection, deterministic serialization, and
structural comparison.
"""

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
    _WorkflowBinding,
)

IR_VERSION = "0.2.0"
SUPPORTED_IR_VERSIONS = frozenset({"0.1.0", IR_VERSION})


class CompileError(ValueError):
    """Raised when a workflow cannot compile to a valid static definition."""


@dataclass(frozen=True, order=True)
class ReplayKey:
    """Stable address of one executable module occurrence.

    Attributes
    ----------
    module_id
        Stable identity of the semantic component.
    logical_step
        Stable position of this occurrence in the workflow definition.
    """

    module_id: str
    logical_step: str

    def as_string(self) -> str:
        """Return the canonical ``module_id@logical_step`` representation."""
        return f"{self.module_id}@{self.logical_step}"


@dataclass(frozen=True)
class BindingIR:
    """Typed dependency binding for an executable IR step.

    Attributes
    ----------
    source
        Node identifier that supplies the step input.
    schema_digest
        Digest of the input type contract expected by the step.
    """

    source: str
    schema_digest: str


@dataclass(frozen=True)
class StepIR:
    """One executable or control node in a compiled workflow.

    Executable nodes carry module identity and definition digests. Control
    nodes, such as branches and parallel joins, retain dependency topology and
    their output schema but have no :attr:`replay_key`.

    Attributes
    ----------
    node_id
        Deterministic hierarchical node identifier.
    kind
        Node kind, such as ``module``, ``map_module``, ``when``, or ``parallel``.
    dependencies
        Node identifiers that must produce values before this node.
    output_schema_digest
        Digest of the node output contract.
    module_id, logical_step, module_digest, definition_digest
        Replay and content identities for executable nodes.
    input_binding
        Typed source binding for an executable node.
    execution
        Immutable executor requirements for executable nodes.
    capabilities, effects
        Canonical external read and write declarations.
    control
        Canonical control-region metadata when applicable.
    """

    node_id: str
    kind: str
    dependencies: tuple[str, ...]
    output_schema_digest: str
    module_id: str | None = None
    logical_step: str | None = None
    module_digest: str | None = None
    definition_digest: str | None = None
    input_binding: BindingIR | None = None
    execution: Mapping[str, Any] | None = None
    capabilities: tuple[Mapping[str, Any], ...] = ()
    effects: tuple[Mapping[str, Any], ...] = ()
    control: Mapping[str, Any] | None = None

    @property
    def replay_key(self) -> ReplayKey | None:
        """Return the replay address, or ``None`` for a control node."""
        if self.module_id is None or self.logical_step is None:
            return None
        return ReplayKey(self.module_id, self.logical_step)


@dataclass(frozen=True)
class PlanIR:
    """Canonical static definition of a workflow.

    ``PlanIR`` contains schemas, ordered steps, dependency topology, stable
    replay identities, and a terminal node. Its canonical JSON and digest are
    deterministic for the same workflow definition.

    Attributes
    ----------
    version
        Workflow IR schema version.
    workflow_id
        Stable workflow identity supplied by the author.
    input_schema, output_schema
        Canonical root value schemas.
    steps
        Ordered executable and control nodes.
    output_node
        Node identifier that supplies the workflow result.
    """

    version: str
    workflow_id: str
    input_schema: Mapping[str, Any]
    output_schema: Mapping[str, Any]
    steps: tuple[StepIR, ...]
    output_node: str

    def to_dict(self) -> dict[str, Any]:
        """Return a deterministic JSON-compatible representation of the plan."""
        return cast(dict[str, Any], canonical_data(asdict(self)))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PlanIR:
        """Validate and construct a plan from serialized data.

        Parameters
        ----------
        data
            Mapping containing a supported IR version and complete step graph.

        Returns
        -------
        PlanIR
            Validated immutable plan.

        Raises
        ------
        ValueError
            If the version, replay identities, topology, or output node is
            invalid.
        """
        if data.get("version") not in SUPPORTED_IR_VERSIONS:
            raise ValueError(
                f"unsupported Workflow IR version {data.get('version')!r}; "
                f"expected one of {sorted(SUPPORTED_IR_VERSIONS)}"
            )
        steps = []
        for raw in data["steps"]:
            binding = raw.get("input_binding")
            steps.append(
                StepIR(
                    node_id=raw["node_id"],
                    kind=raw["kind"],
                    dependencies=tuple(raw["dependencies"]),
                    output_schema_digest=raw["output_schema_digest"],
                    module_id=raw.get("module_id"),
                    logical_step=raw.get("logical_step"),
                    module_digest=raw.get("module_digest"),
                    definition_digest=raw.get("definition_digest"),
                    input_binding=BindingIR(**binding) if binding else None,
                    execution=raw.get("execution"),
                    capabilities=tuple(raw.get("capabilities", ())),
                    effects=tuple(raw.get("effects", ())),
                    control=raw.get("control"),
                )
            )
        plan = cls(
            version=str(data["version"]),
            workflow_id=str(data["workflow_id"]),
            input_schema=cast(Mapping[str, Any], data["input_schema"]),
            output_schema=cast(Mapping[str, Any], data["output_schema"]),
            steps=tuple(steps),
            output_node=str(data["output_node"]),
        )
        _validate_imported_plan(plan)
        return plan

    def canonical_json(self) -> str:
        """Serialize the plan as deterministic canonical JSON."""
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        """Return the SHA-256 content digest of the canonical plan."""
        return digest_data(self.to_dict())

    @property
    def executable_steps(self) -> tuple[StepIR, ...]:
        """Return only steps that have replay-addressable module identities."""
        return tuple(step for step in self.steps if step.replay_key is not None)


@dataclass(frozen=True)
class _CompiledWorkflowGraph:
    plan: PlanIR
    output: RuntimeValue[Any]


def _behavior_bytes(module: Module[Any, Any]) -> bytes:
    artifacts: list[bytes] = []
    for module_class in reversed(module.__class__.mro()):
        if module_class in {object, Module}:
            continue
        try:
            artifacts.append(inspect.getsource(module_class).encode())
            continue
        except (OSError, TypeError):
            pass
        artifacts.append(qualified_name(module_class).encode())
        for name, member in sorted(vars(module_class).items()):
            function = (
                member.__func__ if isinstance(member, (classmethod, staticmethod)) else member
            )
            code = getattr(function, "__code__", None)
            if code is not None:
                artifacts.extend((name.encode(), marshal.dumps(code)))
    return b"\0".join(artifacts)


_MODULE_CONTRACT_FIELDS = {
    "capabilities",
    "effectful",
    "effects",
    "execution",
    "input_type",
    "module_id",
    "output_type",
}


def _module_configuration(module: Module[Any, Any]) -> dict[str, Any]:
    declared: dict[str, Any] = {}
    for module_class in reversed(module.__class__.mro()):
        if module_class in {object, Module}:
            continue
        for name, value in sorted(vars(module_class).items()):
            if (
                name.startswith("_")
                or name in _MODULE_CONTRACT_FIELDS
                or isinstance(value, (classmethod, property, staticmethod))
                or callable(value)
            ):
                continue
            declared[name] = value
    configured = {
        name: value
        for name, value in sorted(vars(module).items())
        if name not in _MODULE_CONTRACT_FIELDS and not name.startswith("_")
    }
    return {"class": declared, "instance": configured}


def _access_contract(module: Module[Any, Any]) -> dict[str, tuple[dict[str, Any], ...]]:
    def declarations(attribute: str) -> tuple[dict[str, Any], ...]:
        encoded: list[dict[str, Any]] = []
        for declaration in getattr(module, attribute, ()):
            to_data = getattr(declaration, "to_data", None)
            if not callable(to_data):
                raise CompileError(f"module {attribute} declarations must provide to_data()")
            data = to_data()
            if not isinstance(data, dict):
                raise CompileError(f"module {attribute} declaration must encode as an object")
            encoded.append(cast(dict[str, Any], canonical_data(data)))
        return tuple(sorted(encoded, key=canonical_json))

    return {
        "capabilities": declarations("capabilities"),
        "effects": declarations("effects"),
    }


def module_digest(module: Module[Any, Any]) -> str:
    """Compute the behavior-bearing content digest for a module definition.

    The digest covers the module class artifact, public configuration, declared
    input/output schemas, and effect classification. Stable component and step
    identities are intentionally excluded.

    Parameters
    ----------
    module
        Module instance whose definition should be identified.

    Returns
    -------
    str
        Lowercase SHA-256 hexadecimal digest.
    """
    payload = b"\0".join(
        (
            qualified_name(module.__class__).encode(),
            _behavior_bytes(module),
            canonical_json(_module_configuration(module)).encode(),
            schema_digest(module.input_type).encode(),
            schema_digest(module.output_type).encode(),
            str(module.effectful).encode(),
            canonical_json(module.execution.to_data()).encode(),
            canonical_json(_access_contract(module)).encode(),
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
        self.implicit_occurrences: set[int] = set()
        self.keys: set[ReplayKey] = set()
        self.module_paths_by_workflow: dict[int, dict[int, str]] = {}

    def compile(self) -> PlanIR:
        return self.compile_graph().plan

    def compile_graph(self) -> _CompiledWorkflowGraph:
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
        return _CompiledWorkflowGraph(
            PlanIR(
                version=IR_VERSION,
                workflow_id=self.root_workflow.workflow_id,
                input_schema=type_schema(self.root_workflow.input_type),
                output_schema=type_schema(self.root_workflow.output_type),
                steps=tuple(self.steps),
                output_node=output_node,
            ),
            output,
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
            binding = expression.payload
            if not isinstance(binding, _WorkflowBinding):
                raise CompileError("invalid nested workflow expression")
            nested = binding.workflow
            self._validate_workflow(nested)
            source = self._visit(
                expression.dependencies[0],
                path=f"{path}.input",
                workflow=workflow,
                external_input=external_input,
            )
            node_id = self._visit(
                binding.output,
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
        if not explicit:
            self.implicit_occurrences.add(id(module))
        if self.occurrences[id(module)] > 1 and (
            not explicit or id(module) in self.implicit_occurrences
        ):
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
        access = _access_contract(module)
        input_digest = schema_digest(module.input_type)
        output_digest = schema_digest(module.output_type)
        definition_digest = digest_data(
            {
                "module_id": module_id,
                "logical_step": logical_step,
                "module_digest": behavior_digest,
                "input_schema_digest": input_digest,
                "output_schema_digest": output_digest,
                "execution": module.execution.to_data(),
                "capabilities": access["capabilities"],
                "effects": access["effects"],
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
            execution=module.execution.to_data(),
            capabilities=access["capabilities"],
            effects=access["effects"],
            control=control,
        )


def compile_workflow(workflow: Workflow[Any, Any]) -> PlanIR:
    """Compile a workflow into deterministic static IR.

    Parameters
    ----------
    workflow
        Workflow instance with registered modules and a pure ``build`` method.

    Returns
    -------
    PlanIR
        Canonical replay-addressable workflow definition.

    Raises
    ------
    CompileError
        If module identities, topology, or the root output contract are invalid.
    TypeError
        If a symbolic handoff violates a declared type contract.

    Examples
    --------
    >>> plan = compile_workflow(MyWorkflow())  # doctest: +SKIP
    >>> plan.workflow_id
    'my-workflow'
    """
    return _Compiler(workflow).compile()


def _compile_workflow_graph(workflow: Workflow[Any, Any]) -> _CompiledWorkflowGraph:
    return _Compiler(workflow).compile_graph()


def _validate_imported_plan(plan: PlanIR) -> None:
    known_nodes = {"input"}
    replay_keys: set[ReplayKey] = set()
    for step in plan.steps:
        if step.node_id in known_nodes:
            raise ValueError(f"duplicate Workflow IR node {step.node_id!r}")
        missing_dependencies = set(step.dependencies) - known_nodes
        if missing_dependencies:
            raise ValueError(
                f"Workflow IR node {step.node_id!r} has unknown or forward dependencies: "
                f"{sorted(missing_dependencies)}"
            )
        identity = (step.module_id, step.logical_step)
        if (identity[0] is None) != (identity[1] is None):
            raise ValueError(f"Workflow IR node {step.node_id!r} has a partial replay identity")
        if step.replay_key is not None:
            if step.replay_key in replay_keys:
                raise ValueError(f"duplicate replay key {step.replay_key.as_string()}")
            if (
                step.module_digest is None
                or step.definition_digest is None
                or step.input_binding is None
            ):
                raise ValueError(
                    f"executable Workflow IR node {step.node_id!r} has an incomplete definition"
                )
            replay_keys.add(step.replay_key)
        known_nodes.add(step.node_id)
    if plan.output_node not in known_nodes:
        raise ValueError(f"Workflow IR output node {plan.output_node!r} does not exist")
