from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def value_matches_schema(value: Any, schema: Mapping[str, Any]) -> bool:
    if not schema:
        return True
    alternatives = schema.get("anyOf")
    if isinstance(alternatives, Sequence):
        return any(
            isinstance(item, Mapping) and value_matches_schema(value, item) for item in alternatives
        )
    expected = schema.get("type")
    if expected == "null":
        return value is None
    if expected == "boolean":
        return type(value) is bool
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "string":
        return isinstance(value, str)
    if expected == "array":
        if not isinstance(value, (list, tuple)):
            return False
        prefix = schema.get("prefixItems")
        if isinstance(prefix, Sequence):
            return len(value) == len(prefix) and all(
                isinstance(item_schema, Mapping) and value_matches_schema(item, item_schema)
                for item, item_schema in zip(value, prefix, strict=True)
            )
        item_schema = schema.get("items", {})
        return isinstance(item_schema, Mapping) and all(
            value_matches_schema(item, item_schema) for item in value
        )
    if expected == "object":
        if not isinstance(value, Mapping):
            return False
        properties = schema.get("properties", {})
        if not isinstance(properties, Mapping):
            return False
        required = schema.get("required", [])
        if not isinstance(required, Sequence) or isinstance(required, (str, bytes)):
            return False
        if any(name not in value for name in required):
            return False
        if schema.get("additionalProperties") is False and any(
            name not in properties for name in value
        ):
            return False
        additional = schema.get("additionalProperties", {})
        for name, item in value.items():
            child_schema = properties.get(name, additional)
            if isinstance(child_schema, bool):
                if not child_schema:
                    return False
                continue
            if not isinstance(child_schema, Mapping) or not value_matches_schema(
                item, child_schema
            ):
                return False
        return True
    return "pythonType" not in schema


def schema_at_path(schema: Mapping[str, Any], path: tuple[str, ...]) -> Mapping[str, Any]:
    current: Mapping[str, Any] = schema
    for part in path:
        if current.get("type") != "object":
            raise ValueError(f"field path component {part!r} does not select an object")
        properties = current.get("properties", {})
        if isinstance(properties, Mapping) and part in properties:
            child = properties[part]
        else:
            child = current.get("additionalProperties")
        if not isinstance(child, Mapping):
            raise ValueError(f"field path component {part!r} is not declared")
        current = child
    return current


def schemas_compatible(source: Mapping[str, Any], target: Mapping[str, Any]) -> bool:
    if not target or not source:
        return True
    if source == target:
        return True
    if target.get("pythonType") in {"builtins.object", "typing.Any"}:
        return True
    if target.get("type") == "number" and source.get("type") == "integer":
        return True
    target_alternatives = target.get("anyOf")
    if isinstance(target_alternatives, Sequence):
        return any(
            isinstance(candidate, Mapping) and schemas_compatible(source, candidate)
            for candidate in target_alternatives
        )
    return False
