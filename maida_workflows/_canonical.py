from __future__ import annotations

import dataclasses
import hashlib
import json
import types
import typing
from collections.abc import Mapping, Sequence
from enum import Enum
from pathlib import Path
from typing import Any


class CanonicalValueError(TypeError):
    """Raised when a value cannot be represented in a durable contract."""


def canonical_data(value: Any) -> Any:
    """Convert supported values to deterministic, JSON-safe data."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: canonical_data(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    if isinstance(value, Enum):
        return canonical_data(value.value)
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise CanonicalValueError("non-finite floats are not canonical JSON values")
        return value
    if isinstance(value, bytes):
        return {"$bytes": value.hex()}
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise CanonicalValueError("canonical object keys must be strings")
        return {key: canonical_data(value[key]) for key in sorted(value)}
    if isinstance(value, (set, frozenset)):
        encoded = [canonical_data(item) for item in value]
        return sorted(encoded, key=canonical_json)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [canonical_data(item) for item in value]
    raise CanonicalValueError(f"unsupported canonical value: {type(value).__qualname__}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        canonical_data(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_data(value: Any) -> str:
    return digest_bytes(canonical_json(value).encode())


def qualified_name(value: Any) -> str:
    module = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)
    if module and qualname:
        return f"{module}.{qualname}"
    return repr(value)


def type_schema(annotation: Any) -> dict[str, Any]:
    """Return a small deterministic schema for runtime boundary validation."""
    if annotation is Any or annotation is typing.Any:
        return {}
    if annotation is None or annotation is type(None):
        return {"type": "null"}
    if annotation in (str, int, float, bool):
        primitives = {str: "string", int: "integer", float: "number", bool: "boolean"}
        return {"type": primitives[annotation]}
    if annotation is bytes:
        return {"type": "string", "contentEncoding": "hex"}
    if dataclasses.is_dataclass(annotation):
        hints = typing.get_type_hints(annotation)
        required: list[str] = []
        properties: dict[str, Any] = {}
        for field in dataclasses.fields(annotation):
            properties[field.name] = type_schema(hints.get(field.name, Any))
            if (
                field.default is dataclasses.MISSING
                and field.default_factory is dataclasses.MISSING
            ):
                required.append(field.name)
        schema: dict[str, Any] = {
            "type": "object",
            "title": qualified_name(annotation),
            "properties": properties,
            "additionalProperties": False,
        }
        if required:
            schema["required"] = required
        return schema
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin in (list, Sequence):
        return {"type": "array", "items": type_schema(args[0] if args else Any)}
    if origin is tuple:
        return {"type": "array", "prefixItems": [type_schema(item) for item in args]}
    if origin in (dict, Mapping):
        return {"type": "object", "additionalProperties": type_schema(args[1] if args else Any)}
    if origin in (typing.Union, types.UnionType):
        return {"anyOf": [type_schema(item) for item in args]}
    return {"pythonType": qualified_name(annotation)}


def schema_digest(annotation: Any) -> str:
    return digest_data(type_schema(annotation))


def value_matches_type(value: Any, annotation: Any) -> bool:
    if annotation is Any or annotation is typing.Any:
        return True
    if value is None:
        origin = typing.get_origin(annotation)
        return (
            annotation is None
            or annotation is type(None)
            or (
                origin in (typing.Union, types.UnionType)
                and type(None) in typing.get_args(annotation)
            )
        )
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin in (typing.Union, types.UnionType):
        return any(value_matches_type(value, item) for item in args)
    if origin in (list, Sequence):
        return isinstance(value, list) and all(
            value_matches_type(item, args[0] if args else Any) for item in value
        )
    if origin is tuple:
        return (
            isinstance(value, tuple)
            and len(value) == len(args)
            and all(
                value_matches_type(item, expected)
                for item, expected in zip(value, args, strict=True)
            )
        )
    if origin in (dict, Mapping):
        return isinstance(value, dict) and all(isinstance(key, str) for key in value)
    try:
        return isinstance(value, annotation)
    except TypeError:
        return True
