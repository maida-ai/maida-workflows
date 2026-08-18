"""Compile workflow definitions into canonical, replay-addressable IR.

The public objects in this module describe workflow structure without runtime
payloads. :func:`compile_workflow` is the usual entry point; :class:`PlanIR`
and :class:`StepIR` support inspection, deterministic serialization, and
structural comparison.
"""

from __future__ import annotations

import inspect
import marshal
import re
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
    _declared_module_id,
    _FieldBinding,
    _MapBinding,
    _ModuleBinding,
    _StructuredBinding,
    _WorkflowBinding,
)
from .budget import Budget
from .interactions import _InteractionModule
from .model import _model_contract, _validated_model_contract

IR_VERSION = "0.6.0"
SUPPORTED_IR_VERSIONS = frozenset({IR_VERSION})
_GRAPH_DERIVED_IDENTITY_IR_VERSIONS = frozenset({"0.1.0", "0.2.0", "0.3.0", "0.4.0", "0.5.0"})


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
        Stable position of this occurrence in the plan instance.
    """

    module_id: str
    logical_step: str

    def as_string(self) -> str:
        """Return the versioned canonical replay-address representation."""
        return f"replay-v1:{self.module_id}@{self.logical_step}"


@dataclass(frozen=True)
class BindingIR:
    """Typed value expression supplying one executable module input.

    Attributes
    ----------
    schema_digest
        Digest of the value type produced by this binding.
    kind
        ``source``, ``field``, ``literal``, ``object``, ``list``, or ``tuple``.
    source
        Node identifier used by source and field bindings.
    path
        Stable field path used by a field projection.
    value
        Canonical JSON value used by a literal binding.
    fields, items
        Recursively typed children for structured bindings.
    """

    schema_digest: str
    kind: str = "source"
    source: str | None = None
    path: tuple[str, ...] = ()
    value: Any = None
    fields: tuple[tuple[str, BindingIR], ...] = ()
    items: tuple[BindingIR, ...] = ()

    def to_data(self) -> dict[str, Any]:
        """Return the canonical recursive wire representation."""
        data: dict[str, Any] = {"kind": self.kind, "schema_digest": self.schema_digest}
        if self.kind in {"source", "field"}:
            data["source"] = self.source
        if self.kind == "field":
            data["path"] = list(self.path)
        elif self.kind == "literal":
            data["value"] = canonical_data(self.value)
        elif self.kind == "object":
            data["fields"] = [
                {"name": name, "binding": binding.to_data()} for name, binding in self.fields
            ]
        elif self.kind in {"list", "tuple"}:
            data["items"] = [binding.to_data() for binding in self.items]
        elif self.kind != "source":
            raise ValueError(f"unsupported binding kind {self.kind!r}")
        return data

    @classmethod
    def from_data(cls, data: Any) -> BindingIR:
        """Validate and restore a recursive binding from canonical data."""
        if not isinstance(data, Mapping):
            raise ValueError("Workflow IR input binding must be an object")
        kind = data.get("kind")
        expected_fields = {
            "source": {"kind", "schema_digest", "source"},
            "field": {"kind", "schema_digest", "source", "path"},
            "literal": {"kind", "schema_digest", "value"},
            "object": {"kind", "schema_digest", "fields"},
            "list": {"kind", "schema_digest", "items"},
            "tuple": {"kind", "schema_digest", "items"},
        }
        if not isinstance(kind, str) or kind not in expected_fields:
            raise ValueError("Workflow IR input binding kind is invalid")
        if set(data) != expected_fields[kind]:
            raise ValueError("Workflow IR input binding fields are invalid")
        schema = data["schema_digest"]
        if not isinstance(schema, str):
            raise ValueError("Workflow IR binding schema digest must be a string")
        if kind in {"source", "field"}:
            source = data["source"]
            if not isinstance(source, str) or not source:
                raise ValueError("Workflow IR binding source must be a node identifier")
            path: tuple[str, ...] = ()
            if kind == "field":
                raw_path = data["path"]
                if (
                    not isinstance(raw_path, list)
                    or not raw_path
                    or any(not isinstance(item, str) or not item for item in raw_path)
                ):
                    raise ValueError("Workflow IR field binding path is invalid")
                path = tuple(raw_path)
            return cls(schema_digest=schema, kind=str(kind), source=source, path=path)
        if kind == "literal":
            return cls(schema_digest=schema, kind="literal", value=canonical_data(data["value"]))
        if kind == "object":
            raw_fields = data["fields"]
            if not isinstance(raw_fields, list):
                raise ValueError("Workflow IR object binding fields must be an array")
            fields: list[tuple[str, BindingIR]] = []
            for item in raw_fields:
                if not isinstance(item, Mapping) or set(item) != {"name", "binding"}:
                    raise ValueError("Workflow IR object binding field is invalid")
                name = item["name"]
                if not isinstance(name, str) or not name:
                    raise ValueError("Workflow IR object binding field name is invalid")
                fields.append((name, cls.from_data(item["binding"])))
            if [name for name, _ in fields] != sorted({name for name, _ in fields}):
                raise ValueError("Workflow IR object binding fields must be canonical")
            return cls(schema_digest=schema, kind="object", fields=tuple(fields))
        raw_items = data["items"]
        if not isinstance(raw_items, list):
            raise ValueError("Workflow IR sequence binding items must be an array")
        return cls(
            schema_digest=schema,
            kind=str(kind),
            items=tuple(cls.from_data(item) for item in raw_items),
        )

    @property
    def source_nodes(self) -> tuple[str, ...]:
        """Return ordered unique graph nodes referenced by this binding."""
        if self.kind in {"source", "field"}:
            return (self.source,) if self.source is not None else ()
        children = tuple(binding for _, binding in self.fields) or self.items
        return tuple(dict.fromkeys(node for child in children for node in child.source_nodes))


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
    models
        Credential-free typed model declarations resolved by the live broker.
    budget
        Canonical resource-limit declaration for executable nodes. Measured
        usage is deliberately not part of Workflow IR.
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
    models: tuple[Mapping[str, Any], ...] = ()
    budget: Mapping[str, int | float | None] | None = None
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
        if self.version != IR_VERSION:
            raise ValueError(f"cannot serialize unsupported Workflow IR version {self.version!r}")
        encoded = cast(dict[str, Any], canonical_data(asdict(self)))
        for raw_step, step in zip(encoded["steps"], self.steps, strict=True):
            if step.input_binding is not None:
                raw_step["input_binding"] = step.input_binding.to_data()
        return encoded

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
        version = data.get("version")
        if version in _GRAPH_DERIVED_IDENTITY_IR_VERSIONS:
            raise ValueError(
                f"Workflow IR {version} predates graph-independent module identity; "
                "recompile the plan"
            )
        if version not in SUPPORTED_IR_VERSIONS:
            raise ValueError(
                f"unsupported Workflow IR version {data.get('version')!r}; "
                f"expected one of {sorted(SUPPORTED_IR_VERSIONS)}"
            )
        steps = []
        for raw in data["steps"]:
            binding = raw.get("input_binding")
            if "capabilities" not in raw or "effects" not in raw:
                raise ValueError(f"Workflow IR {version} steps require external access fields")
            if "budget" not in raw:
                raise ValueError(f"Workflow IR {version} steps require a budget field")
            if "models" not in raw:
                raise ValueError(f"Workflow IR {version} steps require a models field")
            budget: dict[str, int | float | None] | None = None
            raw_budget = raw["budget"]
            if raw.get("module_id") is None:
                if raw_budget is not None:
                    raise ValueError(
                        f"control Workflow IR node {raw.get('node_id')!r} cannot declare a budget"
                    )
            else:
                if not isinstance(raw_budget, Mapping):
                    raise ValueError(
                        f"Workflow IR node {raw.get('node_id')!r} budget must be an object"
                    )
                try:
                    restored_budget = Budget.from_data(raw_budget)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        f"Workflow IR node {raw.get('node_id')!r} budget is invalid: {exc}"
                    ) from exc
                budget = restored_budget.to_data()
                if canonical_json(budget) != canonical_json(raw_budget):
                    raise ValueError(
                        f"Workflow IR node {raw.get('node_id')!r} budget is not canonical"
                    )
            restored_binding = BindingIR.from_data(binding) if binding else None
            if restored_binding is not None and canonical_json(
                restored_binding.to_data()
            ) != canonical_json(binding):
                raise ValueError("Workflow IR input binding is not canonical")
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
                    input_binding=restored_binding,
                    execution=raw.get("execution"),
                    capabilities=_validated_access_declarations(
                        raw.get("capabilities", ()),
                        expected_kind="capability",
                        location=f"Workflow IR node {raw.get('node_id')!r} capabilities",
                        require_canonical=True,
                        error_type=ValueError,
                    ),
                    effects=_validated_access_declarations(
                        raw.get("effects", ()),
                        expected_kind="effect",
                        location=f"Workflow IR node {raw.get('node_id')!r} effects",
                        require_canonical=True,
                        error_type=ValueError,
                    ),
                    models=_validated_model_contract(
                        raw.get("models", ()),
                        require_canonical=True,
                    ),
                    budget=budget,
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
    modules: Mapping[ReplayKey, Module[Any, Any]]
    map_item_keys: Mapping[str, str | Callable[[Any], str]]


def _finalize_plan(
    *,
    workflow_id: str,
    input_schema: Mapping[str, Any],
    output_schema: Mapping[str, Any],
    steps: tuple[StepIR, ...],
    output_node: str,
) -> PlanIR:
    """Build and strictly import the one canonical plan representation."""
    plan = PlanIR(
        version=IR_VERSION,
        workflow_id=workflow_id,
        input_schema=input_schema,
        output_schema=output_schema,
        steps=steps,
        output_node=output_node,
    )
    return PlanIR.from_dict(plan.to_dict())


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
    "budget",
    "capabilities",
    "effectful",
    "effects",
    "execution",
    "input_type",
    "module_id",
    "models",
    "output_type",
    "plan_boundary",
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
        if name
        not in {
            "budget",
            "capabilities",
            "effects",
            "input_type",
            "module_id",
            "models",
            "output_type",
            "plan_boundary",
        }
        and not name.startswith("_")
    }
    return {"class": declared, "instance": configured}


_ACCESS_COMMON_FIELDS = frozenset(
    {
        "connector",
        "connector_version",
        "input_schema_digest",
        "kind",
        "name",
        "operation",
        "output_schema_digest",
        "policy_tags",
    }
)
_ACCESS_EFFECT_FIELDS = frozenset({"approval_required", "idempotency"})
_ACCESS_IDENTITY_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")


def _validated_access_declarations(
    raw: Any,
    *,
    expected_kind: str,
    location: str,
    require_canonical: bool,
    error_type: type[ValueError],
) -> tuple[dict[str, Any], ...]:
    if not isinstance(raw, (list, tuple)):
        raise error_type(f"{location} must be an array")
    declarations: list[dict[str, Any]] = []
    names: set[str] = set()
    expected_fields = _ACCESS_COMMON_FIELDS | (
        _ACCESS_EFFECT_FIELDS if expected_kind == "effect" else frozenset()
    )
    for index, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise error_type(f"{location}[{index}] must be an object")
        declaration = cast(dict[str, Any], canonical_data(dict(item)))
        if declaration.get("kind") != expected_kind:
            raise error_type(f"{location}[{index}] must have kind {expected_kind!r}")
        if set(declaration) != expected_fields:
            raise error_type(
                f"{location}[{index}] fields do not match the {expected_kind} contract"
            )
        for field in ("name", "connector", "operation"):
            value = declaration[field]
            if not isinstance(value, str) or _ACCESS_IDENTITY_PATTERN.fullmatch(value) is None:
                raise error_type(f"{location}[{index}].{field} must be a stable name")
        version = declaration["connector_version"]
        if version is not None and (not isinstance(version, str) or not version.strip()):
            raise error_type(
                f"{location}[{index}].connector_version must be null or a non-empty string"
            )
        for field in ("input_schema_digest", "output_schema_digest"):
            value = declaration[field]
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise error_type(f"{location}[{index}].{field} must be a sha256 digest")
        tags = declaration["policy_tags"]
        if (
            not isinstance(tags, list)
            or any(not isinstance(tag, str) or not tag.strip() for tag in tags)
            or tags != sorted(set(tags))
        ):
            raise error_type(
                f"{location}[{index}].policy_tags must be sorted unique non-empty strings"
            )
        if expected_kind == "effect":
            if declaration["idempotency"] not in {"none", "optional", "required"}:
                raise error_type(f"{location}[{index}].idempotency is invalid")
            if type(declaration["approval_required"]) is not bool:
                raise error_type(f"{location}[{index}].approval_required must be boolean")
        name = cast(str, declaration["name"])
        if name in names:
            raise error_type(f"{location} contains duplicate name {name!r}")
        names.add(name)
        declarations.append(declaration)
    canonical = tuple(sorted(declarations, key=canonical_json))
    if require_canonical and tuple(declarations) != canonical:
        raise error_type(f"{location} must be in canonical order")
    return canonical


def _access_contract(module: Module[Any, Any]) -> dict[str, tuple[dict[str, Any], ...]]:
    def declarations(attribute: str, expected_kind: str) -> tuple[dict[str, Any], ...]:
        encoded: list[dict[str, Any]] = []
        for declaration in getattr(module, attribute, ()):
            to_data = getattr(declaration, "to_data", None)
            if not callable(to_data):
                raise CompileError(f"module {attribute} declarations must provide to_data()")
            data = to_data()
            if not isinstance(data, dict):
                raise CompileError(f"module {attribute} declaration must encode as an object")
            encoded.append(cast(dict[str, Any], canonical_data(data)))
        return _validated_access_declarations(
            encoded,
            expected_kind=expected_kind,
            location=f"module {attribute}",
            require_canonical=False,
            error_type=CompileError,
        )

    access = {
        "capabilities": declarations("capabilities", "capability"),
        "effects": declarations("effects", "effect"),
    }
    if access["effects"] and not module.effectful:
        raise CompileError("a module declaring effects must set effectful = True")
    return access


def _budget_contract(module: Module[Any, Any]) -> dict[str, int | float | None]:
    budget = getattr(module, "budget", None)
    if not isinstance(budget, Budget):
        raise CompileError("module budget must be a Budget declaration")
    return budget.to_data()


def _definition_digest(
    *,
    module_id: str,
    logical_step: str,
    module_digest: str,
    input_schema_digest: str,
    output_schema_digest: str,
    execution: Mapping[str, Any],
    capabilities: tuple[Mapping[str, Any], ...],
    effects: tuple[Mapping[str, Any], ...],
    models: tuple[Mapping[str, Any], ...],
    budget: Mapping[str, int | float | None],
    control: Mapping[str, Any] | None,
) -> str:
    """Return the one definition identity used by every plan authoring path."""
    contract: dict[str, Any] = {
        "module_id": module_id,
        "logical_step": logical_step,
        "module_digest": module_digest,
        "input_schema_digest": input_schema_digest,
        "output_schema_digest": output_schema_digest,
        "execution": execution,
        "capabilities": capabilities,
        "effects": effects,
        "control": control,
    }
    if models:
        contract["models"] = models
    if budget != Budget().to_data():
        contract["budget"] = budget
    return digest_data(contract)


def _plan_boundary_contract(module: Module[Any, Any]) -> dict[str, Any] | None:
    marker = getattr(module, "plan_boundary", None)
    if marker is None:
        return None
    to_data = getattr(marker, "to_data", None)
    if not callable(to_data):
        raise CompileError("module plan_boundary declarations must provide to_data()")
    data = to_data()
    if not isinstance(data, dict):
        raise CompileError("module plan_boundary declaration must encode as an object")
    return cast(dict[str, Any], canonical_data(data))


def module_digest(module: Module[Any, Any]) -> str:
    """Compute the behavior-bearing content digest for a module definition.

    The digest covers the module class artifact, public configuration, declared
    input/output schemas, execution environment, external access, effect
    classification, and resource budget. Stable component and step identities
    are intentionally excluded.

    Parameters
    ----------
    module
        Module instance whose definition should be identified.

    Returns
    -------
    str
        Lowercase SHA-256 hexadecimal digest.
    """
    budget = _budget_contract(module)
    parts = [
        qualified_name(module.__class__).encode(),
        _behavior_bytes(module),
        canonical_json(_module_configuration(module)).encode(),
        schema_digest(module.input_type).encode(),
        schema_digest(module.output_type).encode(),
        str(module.effectful).encode(),
        canonical_json(module.execution.to_data()).encode(),
    ]
    access = _access_contract(module)
    if access["capabilities"] or access["effects"]:
        parts.append(canonical_json(access).encode())
    models = _model_contract(module)
    if models:
        parts.append(canonical_json({"models": models}).encode())
    plan_boundary = _plan_boundary_contract(module)
    if plan_boundary is not None:
        parts.append(canonical_json({"plan_boundary": plan_boundary}).encode())
    if budget != Budget().to_data():
        parts.append(canonical_json({"budget": budget}).encode())
    payload = b"\0".join(parts)
    return digest_bytes(payload)


def _callback_identity(callback: Callable[[Any], str]) -> str:
    try:
        content = inspect.getsource(callback).encode()
    except (OSError, TypeError):
        code = getattr(callback, "__code__", None)
        content = marshal.dumps(code) if code is not None else qualified_name(callback).encode()
    return f"{qualified_name(callback)}:{digest_bytes(content)}"


class _Compiler:
    def __init__(self, workflow: Workflow[Any, Any]) -> None:
        self.root_workflow = workflow
        self.steps: list[StepIR] = []
        self.node_ids: dict[int, str] = {}
        self.occurrences: Counter[int] = Counter()
        self.implicit_occurrences: set[int] = set()
        self.keys: set[ReplayKey] = set()
        self.modules: dict[ReplayKey, Module[Any, Any]] = {}
        self.map_item_keys: dict[str, str | Callable[[Any], str]] = {}

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
        steps = tuple(self.steps)
        return _CompiledWorkflowGraph(
            _finalize_plan(
                workflow_id=self.root_workflow.workflow_id,
                input_schema=type_schema(self.root_workflow.input_type),
                output_schema=type_schema(self.root_workflow.output_type),
                steps=steps,
                output_node=output_node,
            ),
            output,
            dict(self.modules),
            dict(self.map_item_keys),
        )

    def _validate_workflow(self, workflow: Workflow[Any, Any]) -> None:
        if not getattr(workflow, "workflow_id", ""):
            raise CompileError("workflow_id must be a non-empty stable identifier")

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
                path=f"{path}.nested",
                workflow=nested,
                external_input=source,
            )
            self.node_ids[id(value)] = node_id
            return node_id
        node_id = path
        if expression.kind == "module":
            binding = expression.payload
            if not isinstance(binding, _ModuleBinding):
                raise CompileError("invalid module expression")
            input_binding = self._compile_input_binding(
                binding.input_value,
                path=f"{path}.dep0",
                workflow=workflow,
                external_input=external_input,
            )
            dependencies = input_binding.source_nodes
            control: dict[str, Any] | None = None
            if isinstance(binding.module, _InteractionModule):
                control = {"interaction": binding.module.interaction_kind}
                signal_name = getattr(binding.module, "signal_name", None)
                if signal_name is not None:
                    control["signal_name"] = signal_name
            step = self._module_step(
                binding.module,
                binding.logical_step,
                binding.explicit,
                path,
                dependencies,
                input_binding=input_binding,
                control=control,
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
            self.map_item_keys[path] = binding.item_key
            dependencies = tuple(
                self._visit(
                    dependency,
                    path=f"{path}.dep{index}",
                    workflow=workflow,
                    external_input=external_input,
                )
                for index, dependency in enumerate(expression.dependencies)
            )
            step = self._module_step(
                binding.module,
                binding.logical_step,
                binding.explicit,
                path,
                dependencies,
                input_binding=BindingIR(
                    source=dependencies[0],
                    schema_digest=schema_digest(binding.module.input_type),
                ),
                control={"region": "map", "item_key": item_key},
            )
        elif expression.kind == "when":
            condition_binding = self._compile_input_binding(
                expression.dependencies[0],
                path=f"{path}.dep0",
                workflow=workflow,
                external_input=external_input,
            )
            condition_sources = condition_binding.source_nodes
            if len(condition_sources) > 1:
                raise CompileError("a when condition must resolve from one symbolic value")
            condition_node = condition_sources[0] if condition_sources else "input"
            branches = tuple(
                self._visit(
                    dependency,
                    path=f"{path}.dep{index}",
                    workflow=workflow,
                    external_input=external_input,
                )
                for index, dependency in enumerate(expression.dependencies[1:], start=1)
            )
            dependencies = (condition_node, *branches)
            step = StepIR(
                node_id=node_id,
                kind="when",
                dependencies=dependencies,
                output_schema_digest=schema_digest(value.value_type),
                input_binding=condition_binding,
                control={"region": "when"},
            )
        elif expression.kind == "parallel":
            dependencies = tuple(
                self._visit(
                    dependency,
                    path=f"{path}.dep{index}",
                    workflow=workflow,
                    external_input=external_input,
                )
                for index, dependency in enumerate(expression.dependencies)
            )
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

    def _compile_input_binding(
        self,
        value: RuntimeValue[Any],
        *,
        path: str,
        workflow: Workflow[Any, Any],
        external_input: str,
    ) -> BindingIR:
        expression = value._expression
        value_digest = schema_digest(value.value_type)
        if expression.kind == "literal":
            return BindingIR(
                schema_digest=value_digest,
                kind="literal",
                value=canonical_data(expression.payload),
            )
        if expression.kind == "field":
            root, projected_path = self._field_root(value)
            source = self._visit(
                root,
                path=path,
                workflow=workflow,
                external_input=external_input,
            )
            return BindingIR(
                schema_digest=value_digest,
                kind="field",
                source=source,
                path=projected_path,
            )
        if expression.kind == "object":
            structured = expression.payload
            if not isinstance(structured, _StructuredBinding) or structured.kind != "object":
                raise CompileError("invalid structured module input")
            fields = tuple(
                (
                    name,
                    self._compile_input_binding(
                        dependency,
                        path=f"{path}.field[{name}]",
                        workflow=workflow,
                        external_input=external_input,
                    ),
                )
                for name, dependency in zip(structured.names, expression.dependencies, strict=True)
            )
            return BindingIR(schema_digest=value_digest, kind="object", fields=fields)
        source = self._visit(
            value,
            path=path,
            workflow=workflow,
            external_input=external_input,
        )
        return BindingIR(source=source, schema_digest=value_digest)

    @staticmethod
    def _field_root(value: RuntimeValue[Any]) -> tuple[RuntimeValue[Any], tuple[str, ...]]:
        path: tuple[str, ...] = ()
        current = value
        while current._expression.kind == "field":
            binding = current._expression.payload
            if not isinstance(binding, _FieldBinding):
                raise CompileError("invalid field projection")
            path = (*binding.path, *path)
            current = current._expression.dependencies[0]
        return current, path

    def _module_step(
        self,
        module: Module[Any, Any],
        explicit_logical_step: str | None,
        explicit: bool,
        path: str,
        dependencies: tuple[str, ...],
        input_binding: BindingIR,
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
        try:
            module_id = _declared_module_id(module)
        except ValueError as exc:
            raise CompileError(str(exc)) from exc
        logical_step = explicit_logical_step if explicit else path
        if logical_step is None:  # defensive: explicit bindings always carry a step
            raise CompileError("module occurrence has no logical_step")
        key = ReplayKey(module_id, logical_step)
        if key in self.keys:
            raise CompileError(f"duplicate replay key {key.as_string()}")
        self.keys.add(key)
        self.modules[key] = module
        behavior_digest = module_digest(module)
        access = _access_contract(module)
        models = _model_contract(module)
        budget = _budget_contract(module)
        input_digest = schema_digest(module.input_type)
        output_digest = schema_digest(module.output_type)
        definition_digest = _definition_digest(
            module_id=module_id,
            logical_step=logical_step,
            module_digest=behavior_digest,
            input_schema_digest=input_digest,
            output_schema_digest=output_digest,
            execution=module.execution.to_data(),
            capabilities=access["capabilities"],
            effects=access["effects"],
            models=models,
            budget=budget,
            control=control,
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
            input_binding=input_binding,
            execution=module.execution.to_data(),
            capabilities=access["capabilities"],
            effects=access["effects"],
            models=models,
            budget=budget,
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

    Notes
    -----
    All workflows emit the current IR format. Older graph-derived identity
    formats must be recompiled rather than imported under changed semantics.

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
                or step.budget is None
            ):
                raise ValueError(
                    f"executable Workflow IR node {step.node_id!r} has an incomplete definition"
                )
            replay_keys.add(step.replay_key)
            if not set(step.input_binding.source_nodes).issubset(step.dependencies):
                raise ValueError(f"Workflow IR node {step.node_id!r} omits a binding dependency")
        elif step.capabilities or step.effects or step.models or step.budget is not None:
            raise ValueError(
                f"control Workflow IR node {step.node_id!r} cannot declare module contracts"
            )
        elif step.input_binding is not None and (
            step.kind != "when"
            or not set(step.input_binding.source_nodes).issubset(step.dependencies)
        ):
            raise ValueError(
                f"control Workflow IR node {step.node_id!r} has an invalid condition binding"
            )
        known_nodes.add(step.node_id)
    if plan.output_node not in known_nodes:
        raise ValueError(f"Workflow IR output node {plan.output_node!r} does not exist")
