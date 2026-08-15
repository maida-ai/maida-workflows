"""Author portable workflows with deterministic, explainable data contracts.

``WorkflowSpec`` is the common authoring surface for humans, AI agents, and
external builders.  It contains aliases, typed bindings, and topology only.
Trusted application registries resolve aliases and the compiler emits the same
canonical Workflow IR consumed by durable execution, diff, and replay.
"""

from __future__ import annotations

import builtins
import re
from collections import defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, cast

from ._canonical import canonical_data, canonical_json, digest_data, type_schema
from ._schema import schema_at_path, schemas_compatible, value_matches_schema
from .authoring import Module
from .budget import Budget
from .definitions import BoundWorkflow
from .interactions import Approval, Input, WaitForSignal, _SchemaAnnotation
from .ir import (
    BindingIR,
    PlanIR,
    ReplayKey,
    StepIR,
    _access_contract,
    module_digest,
)
from .registry import ModuleRegistry

SPEC_VERSION = "0.1.0"
_KEY_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")
_SENSITIVE_PARTS = frozenset(
    {"api_key", "credential", "credentials", "password", "secret", "token"}
)


@dataclass(frozen=True)
class BindingSpec:
    """Portable value expression used by workflow nodes.

    Parameters
    ----------
    kind
        ``root``, ``node``, ``literal``, ``object``, ``list``, or ``tuple``.
    source
        Node key for a node reference. Root references use no source.
    path
        Optional field path projected from the root or referenced node.
    value
        Canonical value for a literal expression.
    fields, items
        Recursive object or sequence children.

    Notes
    -----
    Bindings are pure data. They cannot contain callables, imports, credentials,
    or connector sessions. Literal values participate in definition identity.
    """

    kind: str
    source: str | None = None
    path: tuple[str, ...] = ()
    value: Any = None
    fields: tuple[tuple[str, BindingSpec], ...] = ()
    items: tuple[BindingSpec, ...] = ()

    def __post_init__(self) -> None:
        """Validate and canonicalize this recursive binding expression."""
        if self.kind not in {"root", "node", "literal", "object", "list", "tuple"}:
            raise ValueError(f"unsupported binding kind {self.kind!r}")
        if self.kind == "node":
            _require_key("binding source", self.source)
        elif self.source is not None:
            raise ValueError(f"{self.kind} binding cannot declare a source")
        if any(not isinstance(part, str) or not part for part in self.path):
            raise ValueError("binding field paths require non-empty names")
        if self.kind not in {"root", "node"} and self.path:
            raise ValueError(f"{self.kind} binding cannot project a field path")
        if self.kind == "literal":
            object.__setattr__(self, "value", canonical_data(self.value))
        elif self.value is not None:
            raise ValueError(f"{self.kind} binding cannot contain a literal value")
        if self.kind == "object":
            names = [name for name, _ in self.fields]
            if any(not isinstance(name, str) or not name for name in names):
                raise ValueError("object binding field names must be non-empty strings")
            if len(names) != len(set(names)):
                raise ValueError("object binding field names must be unique")
            object.__setattr__(self, "fields", tuple(sorted(self.fields)))
        elif self.fields:
            raise ValueError(f"{self.kind} binding cannot contain object fields")
        if self.kind not in {"list", "tuple"} and self.items:
            raise ValueError(f"{self.kind} binding cannot contain sequence items")

    @classmethod
    def root(cls, path: str | None = None) -> BindingSpec:
        """Reference the workflow root input, optionally projecting a field."""
        return cls("root", path=_path(path))

    @classmethod
    def node(cls, key: str, path: str | None = None) -> BindingSpec:
        """Reference a prior node output, optionally projecting a field."""
        return cls("node", source=key, path=_path(path))

    @classmethod
    def literal(cls, value: Any) -> BindingSpec:
        """Embed a canonical non-secret literal in the workflow definition."""
        return cls("literal", value=value)

    @classmethod
    def object(cls, **fields: BindingSpec) -> BindingSpec:
        """Build a named object from recursive workflow bindings."""
        return cls("object", fields=tuple(fields.items()))

    @classmethod
    def list(cls, *items: BindingSpec) -> BindingSpec:
        """Build an ordered list from recursive workflow bindings."""
        return cls("list", items=tuple(items))

    @classmethod
    def tuple(cls, *items: BindingSpec) -> BindingSpec:
        """Build an ordered tuple from recursive workflow bindings."""
        return cls("tuple", items=tuple(items))

    @property
    def node_sources(self) -> builtins.tuple[str, ...]:
        """Return ordered unique referenced node keys."""
        if self.kind == "node":
            return (cast(str, self.source),)
        children = builtins.tuple(binding for _, binding in self.fields) or self.items
        return builtins.tuple(
            dict.fromkeys(key for child in children for key in child.node_sources)
        )

    def to_dict(self) -> dict[str, Any]:
        """Return strict canonical JSON data for this binding."""
        data: dict[str, Any] = {"kind": self.kind}
        if self.kind == "node":
            data["source"] = self.source
        if self.kind in {"root", "node"}:
            data["path"] = list(self.path)
        elif self.kind == "literal":
            data["value"] = canonical_data(self.value)
        elif self.kind == "object":
            data["fields"] = [
                {"name": name, "binding": binding.to_dict()} for name, binding in self.fields
            ]
        else:
            data["items"] = [item.to_dict() for item in self.items]
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BindingSpec:
        """Restore a binding while rejecting unknown or noncanonical fields."""
        if not isinstance(data, Mapping):
            raise ValueError("binding must be an object")
        kind = data.get("kind")
        fields_by_kind = {
            "root": {"kind", "path"},
            "node": {"kind", "source", "path"},
            "literal": {"kind", "value"},
            "object": {"kind", "fields"},
            "list": {"kind", "items"},
            "tuple": {"kind", "items"},
        }
        if not isinstance(kind, str) or kind not in fields_by_kind:
            raise ValueError("binding kind is invalid")
        if set(data) != fields_by_kind[kind]:
            raise ValueError("binding fields do not match its kind")
        if kind in {"root", "node"}:
            raw_path = data["path"]
            if not isinstance(raw_path, list) or any(
                not isinstance(part, str) for part in raw_path
            ):
                raise ValueError("binding path must be an array of strings")
            result = cls(kind, source=data.get("source"), path=builtins.tuple(raw_path))
        elif kind == "literal":
            result = cls.literal(data["value"])
        elif kind == "object":
            raw_fields = data["fields"]
            if not isinstance(raw_fields, list):
                raise ValueError("object binding fields must be an array")
            restored_fields: list[tuple[str, BindingSpec]] = []
            for item in raw_fields:
                if not isinstance(item, Mapping) or set(item) != {"name", "binding"}:
                    raise ValueError("object binding field is invalid")
                name = item["name"]
                if not isinstance(name, str):
                    raise ValueError("object binding field name must be a string")
                child = item["binding"]
                if not isinstance(child, Mapping):
                    raise ValueError("object binding child must be an object")
                restored_fields.append((name, cls.from_dict(child)))
            result = cls("object", fields=builtins.tuple(restored_fields))
        else:
            raw_items = data["items"]
            if not isinstance(raw_items, list) or any(
                not isinstance(item, Mapping) for item in raw_items
            ):
                raise ValueError("sequence binding items must be objects")
            result = cls(
                kind,
                items=builtins.tuple(
                    cls.from_dict(cast(Mapping[str, Any], item)) for item in raw_items
                ),
            )
        if canonical_json(result.to_dict()) != canonical_json(data):
            raise ValueError("binding is not canonical")
        return result


@dataclass(frozen=True)
class NodeSpec:
    """One authoring node in a portable workflow specification.

    Use :meth:`task`, :meth:`map`, or :meth:`branch` rather than constructing
    kind-specific fields manually. Node keys are stable definition identity;
    declaration position is not identity.
    """

    key: str
    kind: str
    module: str | None = None
    input: BindingSpec | None = None
    config: Mapping[str, Any] = field(default_factory=dict)
    after: tuple[str, ...] = ()
    module_id: str | None = None
    logical_step: str | None = None
    item_key: str | None = None
    condition: str | None = None
    then: str | None = None
    otherwise: str | None = None
    workflow: WorkflowSpec | None = None
    prompt: str | None = None
    response_schema: Mapping[str, Any] | None = None
    signal_name: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate stable identity and canonicalize configuration/topology."""
        _require_key("node key", self.key)
        if self.kind not in {
            "module",
            "map",
            "branch",
            "nested",
            "approval",
            "input",
            "signal",
        }:
            raise ValueError(f"unsupported node kind {self.kind!r}")
        object.__setattr__(self, "config", MappingProxyType(canonical_data(dict(self.config))))
        object.__setattr__(self, "metadata", MappingProxyType(canonical_data(dict(self.metadata))))
        if self.response_schema is not None:
            object.__setattr__(
                self,
                "response_schema",
                MappingProxyType(canonical_data(self.response_schema)),
            )
        for dependency in self.after:
            _require_key("order-only dependency", dependency)
        object.__setattr__(self, "after", tuple(sorted(set(self.after))))
        if self.module_id is not None:
            _require_key("module_id", self.module_id)
        if self.logical_step is not None and not self.logical_step.strip():
            raise ValueError("logical_step must be non-empty")
        if self.kind in {"module", "map"}:
            _require_key("module alias", self.module)
            if not isinstance(self.input, BindingSpec):
                raise ValueError(f"{self.kind} node requires an input binding")
            if any(value is not None for value in (self.condition, self.then, self.otherwise)):
                raise ValueError(f"{self.kind} node cannot declare branch targets")
            if self.kind == "map" and (not self.item_key or not self.item_key.strip()):
                raise ValueError("map node requires a stable item_key field")
            if self.kind == "module" and self.item_key is not None:
                raise ValueError("ordinary module node cannot declare item_key")
            if self.workflow is not None:
                raise ValueError(f"{self.kind} node cannot declare a nested workflow")
            if (
                any(
                    value is not None
                    for value in (self.prompt, self.response_schema, self.signal_name)
                )
                or self.metadata
            ):
                raise ValueError(f"{self.kind} node cannot declare interaction fields")
        elif self.kind == "branch":
            if any(value is not None for value in (self.module, self.input, self.item_key)):
                raise ValueError("branch node cannot declare module fields")
            if self.config or self.after or self.module_id or self.logical_step:
                raise ValueError("branch node cannot declare module configuration")
            for label, value in (
                ("condition", self.condition),
                ("then", self.then),
                ("otherwise", self.otherwise),
            ):
                _require_key(f"branch {label}", value)
            if self.workflow is not None:
                raise ValueError("branch node cannot declare a nested workflow")
            if (
                any(
                    value is not None
                    for value in (self.prompt, self.response_schema, self.signal_name)
                )
                or self.metadata
            ):
                raise ValueError("branch node cannot declare interaction fields")
        elif self.kind == "nested":
            if not isinstance(self.workflow, WorkflowSpec):
                raise ValueError("nested node requires a WorkflowSpec")
            if not isinstance(self.input, BindingSpec):
                raise ValueError("nested node requires an input binding")
            if any(
                value is not None
                for value in (
                    self.module,
                    self.item_key,
                    self.condition,
                    self.then,
                    self.otherwise,
                    self.module_id,
                    self.logical_step,
                )
            ):
                raise ValueError("nested node cannot declare module or branch fields")
            if self.config:
                raise ValueError("nested node cannot declare module config")
            if (
                any(
                    value is not None
                    for value in (self.prompt, self.response_schema, self.signal_name)
                )
                or self.metadata
            ):
                raise ValueError("nested node cannot declare interaction fields")
        else:
            if not isinstance(self.input, BindingSpec):
                raise ValueError(f"{self.kind} node requires an input binding")
            if not isinstance(self.prompt, str) or not self.prompt.strip():
                raise ValueError(f"{self.kind} node requires a non-empty prompt")
            if (
                any(
                    value is not None
                    for value in (
                        self.module,
                        self.item_key,
                        self.condition,
                        self.then,
                        self.otherwise,
                        self.workflow,
                    )
                )
                or self.config
            ):
                raise ValueError(f"{self.kind} node cannot declare module or control fields")
            if self.kind == "approval":
                if self.response_schema is not None or self.signal_name is not None:
                    raise ValueError("approval node has a fixed decision schema")
            elif not isinstance(self.response_schema, Mapping):
                raise ValueError(f"{self.kind} node requires a response schema")
            if self.kind == "signal":
                if not isinstance(self.signal_name, str) or not self.signal_name.strip():
                    raise ValueError("signal node requires a non-empty signal name")
            elif self.signal_name is not None:
                raise ValueError(f"{self.kind} node cannot declare a signal name")

    @classmethod
    def task(
        cls,
        key: str,
        module: str,
        input: BindingSpec,
        *,
        config: Mapping[str, Any] | None = None,
        after: Sequence[str] = (),
        module_id: str | None = None,
        logical_step: str | None = None,
    ) -> NodeSpec:
        """Create one ordinary distributed module task node."""
        return cls(
            key,
            "module",
            module=module,
            input=input,
            config=config or {},
            after=tuple(after),
            module_id=module_id,
            logical_step=logical_step,
        )

    @classmethod
    def map(
        cls,
        key: str,
        module: str,
        input: BindingSpec,
        *,
        item_key: str,
        config: Mapping[str, Any] | None = None,
        after: Sequence[str] = (),
        module_id: str | None = None,
        logical_step: str | None = None,
    ) -> NodeSpec:
        """Create stable-key distributed fan-out over a runtime sequence."""
        return cls(
            key,
            "map",
            module=module,
            input=input,
            config=config or {},
            after=tuple(after),
            module_id=module_id,
            logical_step=logical_step,
            item_key=item_key,
        )

    @classmethod
    def branch(cls, key: str, condition: str, then: str, otherwise: str) -> NodeSpec:
        """Create a lazy branch selecting one of two node outputs."""
        return cls(
            key,
            "branch",
            condition=condition,
            then=then,
            otherwise=otherwise,
        )

    @classmethod
    def nested(
        cls,
        key: str,
        workflow: WorkflowSpec,
        input: BindingSpec,
        *,
        after: Sequence[str] = (),
    ) -> NodeSpec:
        """Compose a portable child workflow at one stable graph position."""
        return cls(
            key,
            "nested",
            input=input,
            after=tuple(after),
            workflow=workflow,
        )

    @classmethod
    def approval(
        cls,
        key: str,
        input: BindingSpec,
        *,
        prompt: str,
        metadata: Mapping[str, Any] | None = None,
        after: Sequence[str] = (),
        module_id: str | None = None,
        logical_step: str | None = None,
    ) -> NodeSpec:
        """Create a durable approval boundary with an explicit graph result."""
        return cls(
            key,
            "approval",
            input=input,
            prompt=prompt,
            metadata=metadata or {},
            after=tuple(after),
            module_id=module_id,
            logical_step=logical_step,
        )

    @classmethod
    def request_input(
        cls,
        key: str,
        input: BindingSpec,
        *,
        response_schema: Mapping[str, Any],
        prompt: str,
        metadata: Mapping[str, Any] | None = None,
        after: Sequence[str] = (),
        module_id: str | None = None,
        logical_step: str | None = None,
    ) -> NodeSpec:
        """Create a durable schema-validated application input boundary."""
        return cls(
            key,
            "input",
            input=input,
            prompt=prompt,
            response_schema=response_schema,
            metadata=metadata or {},
            after=tuple(after),
            module_id=module_id,
            logical_step=logical_step,
        )

    @classmethod
    def wait_for_signal(
        cls,
        key: str,
        input: BindingSpec,
        *,
        payload_schema: Mapping[str, Any],
        name: str,
        prompt: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        after: Sequence[str] = (),
        module_id: str | None = None,
        logical_step: str | None = None,
    ) -> NodeSpec:
        """Create a durable named external-signal boundary."""
        return cls(
            key,
            "signal",
            input=input,
            prompt=prompt or f"Wait for signal {name}",
            response_schema=payload_schema,
            signal_name=name,
            metadata=metadata or {},
            after=tuple(after),
            module_id=module_id,
            logical_step=logical_step,
        )

    @property
    def dependencies(self) -> tuple[str, ...]:
        """Return canonical data and order dependencies by authoring key."""
        if self.kind == "branch":
            return cast(tuple[str, ...], (self.condition, self.then, self.otherwise))
        if self.input is None:
            return self.after
        return tuple(dict.fromkeys((*self.input.node_sources, *self.after)))

    def to_dict(self) -> dict[str, Any]:
        """Return strict canonical JSON data for this node."""
        return {
            "key": self.key,
            "kind": self.kind,
            "module": self.module,
            "input": self.input.to_dict() if self.input is not None else None,
            "config": canonical_data(dict(self.config)),
            "after": list(self.after),
            "module_id": self.module_id,
            "logical_step": self.logical_step,
            "item_key": self.item_key,
            "condition": self.condition,
            "then": self.then,
            "otherwise": self.otherwise,
            "workflow": self.workflow.to_dict() if self.workflow is not None else None,
            "prompt": self.prompt,
            "response_schema": (
                canonical_data(self.response_schema) if self.response_schema is not None else None
            ),
            "signal_name": self.signal_name,
            "metadata": canonical_data(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NodeSpec:
        """Restore a node while rejecting unknown and noncanonical data."""
        expected = {
            "key",
            "kind",
            "module",
            "input",
            "config",
            "after",
            "module_id",
            "logical_step",
            "item_key",
            "condition",
            "then",
            "otherwise",
            "workflow",
            "prompt",
            "response_schema",
            "signal_name",
            "metadata",
        }
        if not isinstance(data, Mapping) or set(data) != expected:
            raise ValueError("workflow node fields are invalid")
        raw_input = data["input"]
        raw_after = data["after"]
        raw_config = data["config"]
        raw_workflow = data["workflow"]
        raw_response_schema = data["response_schema"]
        raw_metadata = data["metadata"]
        if raw_input is not None and not isinstance(raw_input, Mapping):
            raise ValueError("workflow node input must be an object or null")
        if not isinstance(raw_after, list) or any(not isinstance(item, str) for item in raw_after):
            raise ValueError("workflow node after must be an array of strings")
        if not isinstance(raw_config, Mapping):
            raise ValueError("workflow node config must be an object")
        if raw_workflow is not None and not isinstance(raw_workflow, Mapping):
            raise ValueError("nested workflow must be an object or null")
        if raw_response_schema is not None and not isinstance(raw_response_schema, Mapping):
            raise ValueError("interaction response schema must be an object or null")
        if not isinstance(raw_metadata, Mapping):
            raise ValueError("interaction metadata must be an object")
        result = cls(
            key=str(data["key"]),
            kind=str(data["kind"]),
            module=data["module"],
            input=(BindingSpec.from_dict(raw_input) if raw_input is not None else None),
            config=raw_config,
            after=tuple(raw_after),
            module_id=data["module_id"],
            logical_step=data["logical_step"],
            item_key=data["item_key"],
            condition=data["condition"],
            then=data["then"],
            otherwise=data["otherwise"],
            workflow=(WorkflowSpec.from_dict(raw_workflow) if raw_workflow is not None else None),
            prompt=data["prompt"],
            response_schema=raw_response_schema,
            signal_name=data["signal_name"],
            metadata=raw_metadata,
        )
        if canonical_json(result.to_dict()) != canonical_json(data):
            raise ValueError("workflow node is not canonical")
        return result


@dataclass(frozen=True)
class WorkflowSpec:
    """Versioned, portable source definition for a workflow graph.

    Parameters
    ----------
    workflow_id
        Stable workflow identity.
    input_schema, output_schema
        Canonical JSON schemas for root values.
    nodes
        Module and control nodes. Declaration order is canonicalized by key and
        never acts as graph identity.
    output
        Direct node reference supplying the root workflow result.
    capabilities, effects
        Maximum stable access names the workflow author permits. Resolved
        module declarations must be subsets.
    version
        Authoring contract version, currently ``0.1.0``.

    Notes
    -----
    ``WorkflowSpec`` is safe data, not executable code. Compilation requires a
    trusted :class:`ModuleRegistry` and produces canonical Workflow IR.
    """

    workflow_id: str
    input_schema: Mapping[str, Any]
    output_schema: Mapping[str, Any]
    nodes: tuple[NodeSpec, ...]
    output: BindingSpec
    capabilities: tuple[str, ...] = ()
    effects: tuple[str, ...] = ()
    version: str = SPEC_VERSION

    def __post_init__(self) -> None:
        """Validate canonical root data and stable node identities."""
        _require_key("workflow_id", self.workflow_id)
        if self.version != SPEC_VERSION:
            raise ValueError(f"unsupported WorkflowSpec version {self.version!r}")
        if (
            not isinstance(self.output, BindingSpec)
            or self.output.kind != "node"
            or self.output.path
        ):
            raise ValueError("workflow output must directly reference one node")
        keys = [node.key for node in self.nodes]
        if len(keys) != len(set(keys)):
            raise ValueError("workflow node keys must be unique")
        object.__setattr__(self, "nodes", tuple(sorted(self.nodes, key=lambda item: item.key)))
        object.__setattr__(
            self, "input_schema", MappingProxyType(canonical_data(self.input_schema))
        )
        object.__setattr__(
            self, "output_schema", MappingProxyType(canonical_data(self.output_schema))
        )
        for name in (*self.capabilities, *self.effects):
            _require_key("access name", name)
        object.__setattr__(self, "capabilities", tuple(sorted(set(self.capabilities))))
        object.__setattr__(self, "effects", tuple(sorted(set(self.effects))))

    def to_dict(self) -> dict[str, Any]:
        """Return the canonical portable authoring representation."""
        return {
            "format": "maida-workflow-spec",
            "version": self.version,
            "workflow_id": self.workflow_id,
            "input_schema": canonical_data(self.input_schema),
            "output_schema": canonical_data(self.output_schema),
            "nodes": [node.to_dict() for node in self.nodes],
            "output": self.output.to_dict(),
            "capabilities": list(self.capabilities),
            "effects": list(self.effects),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> WorkflowSpec:
        """Load strict canonical authoring data without importing code."""
        expected = {
            "format",
            "version",
            "workflow_id",
            "input_schema",
            "output_schema",
            "nodes",
            "output",
            "capabilities",
            "effects",
        }
        if not isinstance(data, Mapping) or set(data) != expected:
            raise ValueError("WorkflowSpec fields are invalid")
        if data["format"] != "maida-workflow-spec":
            raise ValueError("WorkflowSpec format is invalid")
        raw_nodes = data["nodes"]
        raw_output = data["output"]
        if not isinstance(raw_nodes, list) or any(
            not isinstance(node, Mapping) for node in raw_nodes
        ):
            raise ValueError("WorkflowSpec nodes must be an array of objects")
        if not isinstance(raw_output, Mapping):
            raise ValueError("WorkflowSpec output must be an object")
        capabilities = data["capabilities"]
        effects = data["effects"]
        if not isinstance(capabilities, list) or not isinstance(effects, list):
            raise ValueError("WorkflowSpec access declarations must be arrays")
        result = cls(
            workflow_id=str(data["workflow_id"]),
            input_schema=cast(Mapping[str, Any], data["input_schema"]),
            output_schema=cast(Mapping[str, Any], data["output_schema"]),
            nodes=tuple(NodeSpec.from_dict(cast(Mapping[str, Any], node)) for node in raw_nodes),
            output=BindingSpec.from_dict(raw_output),
            capabilities=tuple(capabilities),
            effects=tuple(effects),
            version=str(data["version"]),
        )
        if canonical_json(result.to_dict()) != canonical_json(data):
            raise ValueError("WorkflowSpec is not canonical")
        return result

    def canonical_json(self) -> str:
        """Serialize this specification as deterministic canonical JSON."""
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        """Return the SHA-256 identity of canonical authoring data."""
        return digest_data(self.to_dict())

    @staticmethod
    def json_schema() -> dict[str, Any]:
        """Return JSON Schema suitable for humans, agents, and visual builders."""
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Maida WorkflowSpec",
            "type": "object",
            "additionalProperties": False,
            "required": [
                "format",
                "version",
                "workflow_id",
                "input_schema",
                "output_schema",
                "nodes",
                "output",
                "capabilities",
                "effects",
            ],
            "properties": {
                "format": {"const": "maida-workflow-spec"},
                "version": {"const": SPEC_VERSION},
                "workflow_id": {"type": "string", "pattern": _KEY_PATTERN.pattern},
                "input_schema": {"type": "object"},
                "output_schema": {"type": "object"},
                "nodes": {"type": "array", "items": {"type": "object"}},
                "output": {"type": "object"},
                "capabilities": {"type": "array", "items": {"type": "string"}},
                "effects": {"type": "array", "items": {"type": "string"}},
            },
        }


@dataclass(frozen=True)
class ValidationIssue:
    """Stable compiler diagnostic understandable without Python internals."""

    code: str
    location: str
    message: str

    def to_dict(self) -> dict[str, str]:
        """Return machine-readable deterministic diagnostic data."""
        return {"code": self.code, "location": self.location, "message": self.message}


@dataclass(frozen=True)
class WorkflowExplanation:
    """Human- and agent-readable summary of resolved workflow behavior."""

    workflow_id: str
    node_count: int
    nodes: tuple[dict[str, Any], ...]
    edges: tuple[tuple[str, str], ...]
    capabilities: tuple[str, ...]
    effects: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return deterministic structured explanation data."""
        return {
            "workflow_id": self.workflow_id,
            "node_count": self.node_count,
            "nodes": list(self.nodes),
            "edges": [list(edge) for edge in self.edges],
            "capabilities": list(self.capabilities),
            "effects": list(self.effects),
        }

    def render_text(self) -> str:
        """Render a concise stable explanation for terminals and review."""
        lines = [f"Workflow {self.workflow_id}: {self.node_count} nodes"]
        for node in self.nodes:
            label = node.get("module", node["kind"])
            lines.append(f"- {node['key']} [{node['kind']}]: {label}")
        for source, target in self.edges:
            lines.append(f"  {source} -> {target}")
        return "\n".join(lines)


class WorkflowSpecError(ValueError):
    """Raised when invalid authoring diagnostics are promoted to an exception."""


@dataclass(frozen=True)
class WorkflowCompilation:
    """Result of validating, explaining, and compiling a WorkflowSpec.

    Invalid input returns diagnostics with ``plan`` and ``bound`` set to
    ``None``. Call :meth:`raise_for_errors` at an execution boundary after
    presenting diagnostics to a human or authoring agent.
    """

    spec: WorkflowSpec
    issues: tuple[ValidationIssue, ...]
    explanation: WorkflowExplanation
    plan: PlanIR | None = None
    bound: BoundWorkflow | None = field(default=None, repr=False)
    binding_requirements: tuple[dict[str, Any], ...] = ()

    @property
    def ok(self) -> bool:
        """Return whether compilation produced an executable exact binding."""
        return not self.issues and self.plan is not None and self.bound is not None

    def raise_for_errors(self) -> BoundWorkflow:
        """Return the executable binding or raise all stable diagnostics.

        Raises
        ------
        WorkflowSpecError
            If validation or trusted module resolution failed.
        """
        if not self.ok or self.bound is None:
            details = "; ".join(
                f"{issue.code} at {issue.location}: {issue.message}" for issue in self.issues
            )
            raise WorkflowSpecError(details or "workflow compilation failed")
        return self.bound


def compile_workflow_spec(
    spec: WorkflowSpec,
    registry: ModuleRegistry,
) -> WorkflowCompilation:
    """Validate and compile portable authoring data into exact Workflow IR.

    Parameters
    ----------
    spec
        Canonical authoring specification from Python, JSON, an AI agent, or an
        external builder.
    registry
        Trusted application registry that resolves every module alias and
        recomputes immutable contracts.

    Returns
    -------
    WorkflowCompilation
        Structured diagnostics and explanation, plus a bound executable graph
        only when every identity, schema, topology, and access check succeeds.

    Notes
    -----
    Compilation never invokes module handlers and performs no connector access.
    """
    compiler = _SpecCompiler(spec, registry)
    return compiler.compile()


@dataclass(frozen=True)
class _ResolvedNode:
    spec: NodeSpec
    module: Module[Any, Any] | None = None
    requirement: dict[str, Any] | None = None


class _SpecCompiler:
    def __init__(self, spec: WorkflowSpec, registry: ModuleRegistry) -> None:
        self.spec = spec
        self.registry = registry
        self.issues: list[ValidationIssue] = []
        self.nodes = {node.key: node for node in spec.nodes}
        self.resolved: dict[str, _ResolvedNode] = {}
        self.schemas: dict[str, Mapping[str, Any]] = {"input": spec.input_schema}
        self.steps: list[StepIR] = []
        self.modules: dict[ReplayKey, Module[Any, Any]] = {}
        self.requirements: list[dict[str, Any]] = []
        self.node_ids: dict[str, str] = {}

    def compile(self) -> WorkflowCompilation:
        self._resolve_modules()
        self._validate_references()
        order = self._topological_order()
        if not self.issues:
            for key in order:
                self._compile_node(self.resolved[key])
        output_key = cast(str, self.spec.output.source)
        if (
            not self.issues
            and output_key in self.schemas
            and not schemas_compatible(self.schemas[output_key], self.spec.output_schema)
        ):
            self._issue(
                "SCHEMA_MISMATCH",
                "output",
                "declared output schema does not match the selected node",
            )
        explanation = self._explanation()
        if self.issues:
            return WorkflowCompilation(
                self.spec,
                tuple(sorted(self.issues, key=lambda item: (item.location, item.code))),
                explanation,
            )
        plan = PlanIR(
            version="0.4.0",
            workflow_id=self.spec.workflow_id,
            input_schema=canonical_data(self.spec.input_schema),
            output_schema=canonical_data(self.spec.output_schema),
            steps=tuple(self.steps),
            output_node=self.node_ids[output_key],
        )
        # Exercise the same strict importer used for untrusted bundles.
        plan = PlanIR.from_dict(plan.to_dict())
        bound = BoundWorkflow(
            plan=plan,
            input_type=Any,
            output_type=Any,
            modules=self.modules,
        )
        return WorkflowCompilation(
            self.spec,
            (),
            explanation,
            plan,
            bound,
            tuple(self.requirements),
        )

    def _resolve_modules(self) -> None:
        for node in self.spec.nodes:
            if (
                _contains_sensitive_key(node.config)
                or _contains_sensitive_key(node.metadata)
                or (node.input is not None and _binding_contains_sensitive_literal(node.input))
            ):
                self._issue(
                    "SECRET_LITERAL",
                    f"nodes.{node.key}.config",
                    "credentials and secrets must be resolved by runtime providers",
                )
                continue
            if node.kind in {"branch", "nested", "approval", "input", "signal"}:
                self.resolved[node.key] = _ResolvedNode(node)
                continue
            try:
                module = self.registry.resolve(cast(str, node.module), node.config)
                requirement = self.registry.requirement(cast(str, node.module), node.config)
            except (KeyError, TypeError, ValueError) as exc:
                self._issue("UNKNOWN_MODULE", f"nodes.{node.key}.module", str(exc))
                continue
            access = _access_contract(module)
            capabilities = {cast(str, item["name"]) for item in access["capabilities"]}
            effects = {cast(str, item["name"]) for item in access["effects"]}
            if not capabilities.issubset(self.spec.capabilities):
                self._issue(
                    "CAPABILITY_DENIED",
                    f"nodes.{node.key}",
                    "module read capabilities exceed the workflow declaration",
                )
            if not effects.issubset(self.spec.effects):
                self._issue(
                    "EFFECT_DENIED",
                    f"nodes.{node.key}",
                    "module effects exceed the workflow declaration",
                )
            self.resolved[node.key] = _ResolvedNode(node, module, requirement)

    def _validate_references(self) -> None:
        known = set(self.nodes)
        for node in self.spec.nodes:
            for dependency in node.dependencies:
                if dependency not in known:
                    self._issue(
                        "UNKNOWN_DEPENDENCY",
                        f"nodes.{node.key}",
                        f"dependency {dependency!r} is not declared",
                    )
        output = self.spec.output.source
        if output not in known:
            self._issue("INVALID_OUTPUT", "output", "selected output node is not declared")
            return
        reachable: set[str] = set()
        pending = [output]
        while pending:
            key = pending.pop()
            if key in reachable or key not in self.nodes:
                continue
            reachable.add(key)
            pending.extend(self.nodes[key].dependencies)
        for key in sorted(known - reachable):
            self._issue(
                "UNREACHABLE_NODE",
                f"nodes.{key}",
                "node is not part of the workflow output graph",
            )

    def _topological_order(self) -> tuple[str, ...]:
        incoming = {
            key: set(node.dependencies) & set(self.nodes) for key, node in self.nodes.items()
        }
        followers: dict[str, set[str]] = defaultdict(set)
        for key, dependencies in incoming.items():
            for dependency in dependencies:
                followers[dependency].add(key)
        ready = deque(sorted(key for key, dependencies in incoming.items() if not dependencies))
        order: list[str] = []
        while ready:
            key = ready.popleft()
            order.append(key)
            for follower in sorted(followers[key]):
                incoming[follower].discard(key)
                if not incoming[follower]:
                    ready.append(follower)
            ready = deque(sorted(ready))
        if len(order) != len(self.nodes):
            self._issue("CYCLE", "nodes", "workflow dependencies contain a cycle")
        return tuple(order)

    def _compile_node(self, resolved: _ResolvedNode) -> None:
        node = resolved.spec
        if node.kind == "branch":
            self._compile_branch(node)
            return
        if node.kind == "nested":
            self._compile_nested(node)
            return
        module = resolved.module
        requirement = resolved.requirement
        if node.kind in {"approval", "input", "signal"}:
            module, requirement = self._interaction_module(node)
        if module is None or node.input is None or requirement is None:
            return
        input_schema = type_schema(module.input_type)
        if node.kind == "map":
            actual = self._binding_schema(node.input)
            item_schema = actual.get("items") if actual.get("type") == "array" else None
            if not isinstance(item_schema, Mapping) or not _structured_compatible(
                item_schema, input_schema
            ):
                self._issue(
                    "SCHEMA_MISMATCH",
                    f"nodes.{node.key}.input",
                    "map items do not match the module input schema",
                )
                return
            input_binding = self._compile_binding(node.input, input_schema)
            output_schema: Mapping[str, Any] = {
                "type": "array",
                "items": type_schema(module.output_type),
            }
        else:
            actual = self._binding_schema(node.input)
            if not _structured_compatible(actual, input_schema):
                self._issue(
                    "SCHEMA_MISMATCH",
                    f"nodes.{node.key}.input",
                    "resolved binding does not match the module input schema",
                )
                return
            input_binding = self._compile_binding(node.input, input_schema)
            output_schema = type_schema(module.output_type)
        dependency_keys = tuple(
            dict.fromkeys(
                (
                    *(("input",) if _binding_uses_root(node.input) else ()),
                    *node.input.node_sources,
                    *node.after,
                )
            )
        )
        dependencies = tuple(
            key if key == "input" else self.node_ids[key] for key in dependency_keys
        )
        module_id = node.module_id or f"{self.spec.workflow_id}.{node.key}"
        logical_step = node.logical_step or f"nodes/{node.key}"
        key = ReplayKey(module_id, logical_step)
        if key in self.modules:
            self._issue(
                "DUPLICATE_REPLAY_KEY",
                f"nodes.{node.key}",
                f"replay identity {key.as_string()!r} is already used",
            )
            return
        access = _access_contract(module)
        budget = module.budget.to_data()
        behavior_digest = module_digest(module)
        control: dict[str, Any] | None = None
        if node.kind == "map":
            control = {"region": "map", "item_key": {"field": node.item_key}}
        elif node.kind in {"approval", "input", "signal"}:
            control = {"interaction": node.kind}
            if node.signal_name is not None:
                control["signal_name"] = node.signal_name
        definition_contract: dict[str, Any] = {
            "module_id": module_id,
            "logical_step": logical_step,
            "module_digest": behavior_digest,
            "input_schema_digest": digest_data(input_schema),
            "output_schema_digest": digest_data(type_schema(module.output_type)),
            "execution": module.execution.to_data(),
            "capabilities": access["capabilities"],
            "effects": access["effects"],
            "control": control,
        }
        if budget != Budget().to_data():
            definition_contract["budget"] = budget
        step = StepIR(
            node_id=f"nodes/{node.key}",
            kind="map_module" if node.kind == "map" else "module",
            dependencies=dependencies,
            output_schema_digest=digest_data(type_schema(module.output_type)),
            module_id=module_id,
            logical_step=logical_step,
            module_digest=behavior_digest,
            definition_digest=digest_data(definition_contract),
            input_binding=input_binding,
            execution=module.execution.to_data(),
            capabilities=access["capabilities"],
            effects=access["effects"],
            budget=budget,
            control=control,
        )
        self.steps.append(step)
        self.node_ids[node.key] = step.node_id
        self.modules[key] = module
        self.schemas[node.key] = output_schema
        bound_requirement = dict(requirement)
        bound_requirement.update(
            {"node": node.key, "module_id": module_id, "logical_step": logical_step}
        )
        self.requirements.append(bound_requirement)

    def _interaction_module(self, node: NodeSpec) -> tuple[Module[Any, Any], dict[str, Any]]:
        if node.input is None or node.prompt is None:
            raise ValueError("interaction node is incomplete")
        input_annotation = cast(type[Any], _SchemaAnnotation(self._binding_schema(node.input)))
        if node.kind == "approval":
            module: Module[Any, Any] = Approval(
                input_annotation,
                prompt=node.prompt,
                metadata=node.metadata,
                module_id=node.module_id,
            )
        else:
            output_annotation = cast(
                type[Any], _SchemaAnnotation(cast(Mapping[str, Any], node.response_schema))
            )
            if node.kind == "input":
                module = Input(
                    input_annotation,
                    output_annotation,
                    prompt=node.prompt,
                    metadata=node.metadata,
                    module_id=node.module_id,
                )
            else:
                module = WaitForSignal(
                    input_annotation,
                    output_annotation,
                    name=cast(str, node.signal_name),
                    prompt=node.prompt,
                    metadata=node.metadata,
                    module_id=node.module_id,
                )
        return module, {
            "kind": "builtin",
            "builtin": node.kind,
            "configuration": {
                "prompt": node.prompt,
                "response_schema": node.response_schema,
                "signal_name": node.signal_name,
                "metadata": node.metadata,
            },
        }

    def _compile_branch(self, node: NodeSpec) -> None:
        condition = cast(str, node.condition)
        then = cast(str, node.then)
        otherwise = cast(str, node.otherwise)
        if any(key not in self.schemas for key in (condition, then, otherwise)):
            return
        if self.schemas[condition] != type_schema(bool):
            self._issue(
                "SCHEMA_MISMATCH",
                f"nodes.{node.key}.condition",
                "branch condition must produce boolean",
            )
            return
        if not schemas_compatible(
            self.schemas[then], self.schemas[otherwise]
        ) or not schemas_compatible(self.schemas[otherwise], self.schemas[then]):
            self._issue(
                "SCHEMA_MISMATCH",
                f"nodes.{node.key}",
                "branch outputs must have the same schema",
            )
            return
        schema = self.schemas[then]
        self.steps.append(
            StepIR(
                node_id=f"nodes/{node.key}",
                kind="when",
                dependencies=(
                    self.node_ids[condition],
                    self.node_ids[then],
                    self.node_ids[otherwise],
                ),
                output_schema_digest=digest_data(schema),
                control={"region": "when"},
            )
        )
        self.schemas[node.key] = schema
        self.node_ids[node.key] = f"nodes/{node.key}"

    def _compile_nested(self, node: NodeSpec) -> None:
        child_spec = node.workflow
        if child_spec is None or node.input is None:
            return
        if not set(child_spec.capabilities).issubset(self.spec.capabilities) or not set(
            child_spec.effects
        ).issubset(self.spec.effects):
            self._issue(
                "ACCESS_DENIED",
                f"nodes.{node.key}",
                "nested workflow access exceeds its parent declaration",
            )
            return
        actual_input = self._binding_schema(node.input)
        if not _structured_compatible(actual_input, child_spec.input_schema):
            self._issue(
                "SCHEMA_MISMATCH",
                f"nodes.{node.key}.input",
                "nested workflow input does not match the child root schema",
            )
            return
        child = compile_workflow_spec(child_spec, self.registry)
        if not child.ok or child.plan is None or child.bound is None:
            for issue in child.issues:
                self._issue(
                    issue.code,
                    f"nodes.{node.key}.workflow.{issue.location}",
                    issue.message,
                )
            return
        parent_binding = self._compile_binding(node.input, child_spec.input_schema)
        prefix = f"nodes/{node.key}/"
        id_map = {step.node_id: f"{prefix}{step.node_id}" for step in child.plan.steps}
        id_map["input"] = "input"
        parent_after = tuple(self.node_ids[key] for key in node.after)
        key_map: dict[ReplayKey, ReplayKey] = {}
        rewritten_steps: list[StepIR] = []
        for child_step in child.plan.steps:
            rewritten_binding = (
                _rewrite_nested_binding(child_step.input_binding, parent_binding, id_map)
                if child_step.input_binding is not None
                else None
            )
            dependencies = tuple(
                dict.fromkeys(
                    (
                        *(
                            source
                            for dependency in child_step.dependencies
                            for source in (
                                parent_binding.source_nodes
                                if dependency == "input"
                                else (id_map[dependency],)
                            )
                        ),
                        *(parent_after if "input" in child_step.dependencies else ()),
                    )
                )
            )
            module_id = child_step.module_id
            logical_step = (
                f"nodes/{node.key}/{child_step.logical_step}"
                if child_step.logical_step is not None
                else None
            )
            definition_digest = child_step.definition_digest
            if child_step.replay_key is not None:
                if module_id is None or logical_step is None:
                    self._issue(
                        "INVALID_NESTED_DEFINITION",
                        f"nodes.{node.key}",
                        "nested executable identity is incomplete",
                    )
                    return
                new_key = ReplayKey(module_id, logical_step)
                if new_key in self.modules:
                    self._issue(
                        "DUPLICATE_REPLAY_KEY",
                        f"nodes.{node.key}",
                        f"nested replay identity {new_key.as_string()!r} is already used",
                    )
                    return
                key_map[child_step.replay_key] = new_key
                definition_digest = _rewritten_definition_digest(
                    child_step, module_id=module_id, logical_step=logical_step
                )
            rewritten_steps.append(
                StepIR(
                    node_id=id_map[child_step.node_id],
                    kind=child_step.kind,
                    dependencies=dependencies,
                    output_schema_digest=child_step.output_schema_digest,
                    module_id=module_id,
                    logical_step=logical_step,
                    module_digest=child_step.module_digest,
                    definition_digest=definition_digest,
                    input_binding=rewritten_binding,
                    execution=child_step.execution,
                    capabilities=child_step.capabilities,
                    effects=child_step.effects,
                    budget=child_step.budget,
                    control=child_step.control,
                )
            )
        self.steps.extend(rewritten_steps)
        for old_key, new_key in key_map.items():
            self.modules[new_key] = child.bound.modules[old_key]
        for requirement in child.binding_requirements:
            rewritten_requirement = dict(requirement)
            rewritten_requirement["node"] = f"{node.key}.{requirement['node']}"
            rewritten_requirement["logical_step"] = (
                f"nodes/{node.key}/{requirement['logical_step']}"
            )
            self.requirements.append(rewritten_requirement)
        self.schemas[node.key] = child_spec.output_schema
        self.node_ids[node.key] = id_map[child.plan.output_node]

    def _binding_schema(self, binding: BindingSpec) -> Mapping[str, Any]:
        if binding.kind in {"root", "node"}:
            source_schema = (
                self.spec.input_schema
                if binding.kind == "root"
                else self.schemas.get(cast(str, binding.source), {})
            )
            try:
                return schema_at_path(source_schema, binding.path)
            except ValueError as exc:
                self._issue("UNKNOWN_FIELD", "binding", str(exc))
                return {}
        if binding.kind == "literal":
            return type_schema(type(binding.value))
        if binding.kind == "object":
            return {
                "type": "object",
                "properties": {name: self._binding_schema(child) for name, child in binding.fields},
                "required": [name for name, _ in binding.fields],
                "additionalProperties": False,
            }
        children = [self._binding_schema(child) for child in binding.items]
        if binding.kind == "tuple":
            return {"type": "array", "prefixItems": children}
        if not children:
            return {"type": "array", "items": {}}
        first = children[0]
        if any(child != first for child in children[1:]):
            return {"type": "array", "prefixItems": children}
        return {"type": "array", "items": first}

    def _compile_binding(
        self,
        binding: BindingSpec,
        expected_schema: Mapping[str, Any],
    ) -> BindingIR:
        if binding.kind in {"root", "node"}:
            source = "input" if binding.kind == "root" else self.node_ids[cast(str, binding.source)]
            actual = self._binding_schema(binding)
            return BindingIR(
                schema_digest=digest_data(expected_schema or actual),
                kind="field" if binding.path else "source",
                source=source,
                path=binding.path,
            )
        if binding.kind == "literal":
            if not value_matches_schema(binding.value, expected_schema):
                raise ValueError("literal does not match expected schema")
            return BindingIR(
                schema_digest=digest_data(expected_schema),
                kind="literal",
                value=binding.value,
            )
        if binding.kind == "object":
            properties = expected_schema.get("properties", {})
            additional = expected_schema.get("additionalProperties", {})
            fields = tuple(
                (
                    name,
                    self._compile_binding(
                        child,
                        cast(
                            Mapping[str, Any],
                            properties.get(name, additional)
                            if isinstance(properties, Mapping)
                            else {},
                        ),
                    ),
                )
                for name, child in binding.fields
            )
            return BindingIR(
                schema_digest=digest_data(expected_schema), kind="object", fields=fields
            )
        item_schemas = expected_schema.get("prefixItems")
        common = expected_schema.get("items", {})
        children = tuple(
            self._compile_binding(
                child,
                cast(
                    Mapping[str, Any],
                    item_schemas[index]
                    if isinstance(item_schemas, Sequence) and index < len(item_schemas)
                    else common,
                ),
            )
            for index, child in enumerate(binding.items)
        )
        return BindingIR(
            schema_digest=digest_data(expected_schema), kind=binding.kind, items=children
        )

    def _explanation(self) -> WorkflowExplanation:
        nodes = tuple(
            {
                "key": node.key,
                "kind": node.kind,
                "module": node.module,
            }
            for node in self.spec.nodes
        )
        edges = tuple(
            sorted(
                (
                    "input" if source is None else source,
                    node.key,
                )
                for node in self.spec.nodes
                for source in _explanation_sources(node)
            )
        )
        return WorkflowExplanation(
            self.spec.workflow_id,
            len(nodes),
            nodes,
            edges,
            self.spec.capabilities,
            self.spec.effects,
        )

    def _issue(self, code: str, location: str, message: str) -> None:
        self.issues.append(ValidationIssue(code, location, message))


def _path(value: str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    parts = tuple(value.split("."))
    if any(not part for part in parts):
        raise ValueError("binding path must contain non-empty dot-separated names")
    return parts


def _rewrite_nested_binding(
    binding: BindingIR,
    parent: BindingIR,
    node_ids: Mapping[str, str],
) -> BindingIR:
    if binding.kind in {"source", "field"}:
        if binding.source == "input":
            if binding.kind == "source":
                return replace(parent, schema_digest=binding.schema_digest)
            return _project_nested_binding(parent, binding.path, binding.schema_digest)
        if binding.source is None:
            raise ValueError("nested binding source is missing")
        return replace(binding, source=node_ids[binding.source])
    if binding.kind == "object":
        return replace(
            binding,
            fields=tuple(
                (name, _rewrite_nested_binding(child, parent, node_ids))
                for name, child in binding.fields
            ),
        )
    if binding.kind in {"list", "tuple"}:
        return replace(
            binding,
            items=tuple(
                _rewrite_nested_binding(child, parent, node_ids) for child in binding.items
            ),
        )
    return binding


def _project_nested_binding(
    binding: BindingIR,
    path: tuple[str, ...],
    schema: str,
) -> BindingIR:
    if not path:
        return replace(binding, schema_digest=schema)
    if binding.kind in {"source", "field"}:
        return replace(
            binding,
            kind="field",
            path=(*binding.path, *path),
            schema_digest=schema,
        )
    if binding.kind == "object":
        name, *remaining = path
        fields = dict(binding.fields)
        if name not in fields:
            raise ValueError(f"nested input binding has no field {name!r}")
        return _project_nested_binding(fields[name], tuple(remaining), schema)
    if binding.kind == "literal":
        value = binding.value
        for name in path:
            if not isinstance(value, Mapping) or name not in value:
                raise ValueError(f"nested literal input has no field {name!r}")
            value = value[name]
        return BindingIR(schema_digest=schema, kind="literal", value=value)
    raise ValueError("nested input field projection is not supported for this binding")


def _rewritten_definition_digest(
    step: StepIR,
    *,
    module_id: str,
    logical_step: str,
) -> str:
    contract: dict[str, Any] = {
        "module_id": module_id,
        "logical_step": logical_step,
        "module_digest": step.module_digest,
        "input_schema_digest": (
            step.input_binding.schema_digest if step.input_binding is not None else None
        ),
        "output_schema_digest": step.output_schema_digest,
        "execution": step.execution,
        "capabilities": step.capabilities,
        "effects": step.effects,
        "control": step.control,
    }
    if step.budget is not None and step.budget != Budget().to_data():
        contract["budget"] = step.budget
    return digest_data(contract)


def _require_key(label: str, value: Any) -> None:
    if not isinstance(value, str) or _KEY_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{label} must be a stable identifier")


def _contains_sensitive_key(value: Any) -> bool:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in _SENSITIVE_PARTS or _contains_sensitive_key(child):
                return True
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return any(_contains_sensitive_key(item) for item in value)
    return False


def _binding_contains_sensitive_literal(binding: BindingSpec) -> bool:
    if binding.kind == "literal":
        return _contains_sensitive_key(binding.value)
    children = tuple(child for _, child in binding.fields) or binding.items
    return any(_binding_contains_sensitive_literal(child) for child in children)


def _structured_compatible(
    source: Mapping[str, Any],
    target: Mapping[str, Any],
) -> bool:
    if source.get("type") != "object" or target.get("type") != "object":
        return schemas_compatible(source, target)
    source_properties = source.get("properties", {})
    target_properties = target.get("properties", {})
    if not isinstance(source_properties, Mapping) or not isinstance(target_properties, Mapping):
        return False
    required = target.get("required", [])
    if not isinstance(required, Sequence) or any(
        name not in source_properties for name in required
    ):
        return False
    if target.get("additionalProperties") is False and any(
        name not in target_properties for name in source_properties
    ):
        return False
    additional = target.get("additionalProperties", {})
    for name, source_schema in source_properties.items():
        target_schema = target_properties.get(name, additional)
        if (
            not isinstance(source_schema, Mapping)
            or not isinstance(target_schema, Mapping)
            or not schemas_compatible(source_schema, target_schema)
        ):
            return False
    return True


def _explanation_sources(node: NodeSpec) -> tuple[str | None, ...]:
    if node.kind == "branch":
        return cast(tuple[str | None, ...], node.dependencies)
    sources: list[str | None] = list(node.input.node_sources if node.input else ())
    if node.input is not None and _binding_uses_root(node.input):
        sources.append(None)
    sources.extend(node.after)
    return tuple(dict.fromkeys(sources))


def _binding_uses_root(binding: BindingSpec) -> bool:
    if binding.kind == "root":
        return True
    children = tuple(child for _, child in binding.fields) or binding.items
    return any(_binding_uses_root(child) for child in children)
