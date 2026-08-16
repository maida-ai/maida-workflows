from __future__ import annotations

from typing import Any

import pytest

from maida.workflows._schema import schema_at_path, schemas_compatible, value_matches_schema


@pytest.mark.parametrize(
    ("value", "schema", "expected"),
    (
        (object(), {}, True),
        ("value", {"anyOf": [1, {"type": "string"}]}, True),
        (1, {"anyOf": [{"type": "string"}]}, False),
        (None, {"type": "null"}, True),
        (False, {"type": "boolean"}, True),
        (1, {"type": "boolean"}, False),
        (1, {"type": "integer"}, True),
        (True, {"type": "integer"}, False),
        (1.5, {"type": "number"}, True),
        (False, {"type": "number"}, False),
        ("value", {"type": "string"}, True),
        (1, {"type": "string"}, False),
        ("value", {"type": "array"}, False),
        (
            (1, "a"),
            {"type": "array", "prefixItems": [{"type": "integer"}, {"type": "string"}]},
            True,
        ),
        ((1,), {"type": "array", "prefixItems": [{"type": "integer"}, {"type": "string"}]}, False),
        ((1,), {"type": "array", "prefixItems": [1]}, False),
        ([1, 2], {"type": "array", "items": {"type": "integer"}}, True),
        ([1], {"type": "array", "items": []}, False),
        ("value", {"type": "object"}, False),
        ({}, {"type": "object", "properties": []}, False),
        ({}, {"type": "object", "required": "name"}, False),
        ({}, {"type": "object", "required": ["name"]}, False),
        ({"extra": 1}, {"type": "object", "properties": {}, "additionalProperties": False}, False),
        ({"extra": 1}, {"type": "object", "additionalProperties": True}, True),
        ({"extra": 1}, {"type": "object", "additionalProperties": False}, False),
        ({"extra": 1}, {"type": "object", "additionalProperties": []}, False),
        (
            {"name": "Ada"},
            {
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
                "additionalProperties": False,
            },
            True,
        ),
        ({"name": 1}, {"type": "object", "properties": {"name": {"type": "string"}}}, False),
        (object(), {"description": "unconstrained"}, True),
        (object(), {"pythonType": "custom.Type"}, False),
    ),
)
def test_value_schema_matching_is_recursive_and_strict(
    value: Any, schema: dict[str, Any], expected: bool
) -> None:
    assert value_matches_schema(value, schema) is expected


def test_schema_path_supports_properties_and_typed_additional_fields() -> None:
    schema = {
        "type": "object",
        "properties": {
            "known": {
                "type": "object",
                "properties": {"value": {"type": "integer"}},
            }
        },
        "additionalProperties": {"type": "string"},
    }

    assert schema_at_path(schema, ("known", "value")) == {"type": "integer"}
    assert schema_at_path(schema, ("dynamic",)) == {"type": "string"}
    with pytest.raises(ValueError, match="does not select an object"):
        schema_at_path(schema, ("dynamic", "child"))
    with pytest.raises(ValueError, match="not declared"):
        schema_at_path({"type": "object", "properties": {}}, ("missing",))


@pytest.mark.parametrize(
    ("source", "target", "expected"),
    (
        ({}, {"type": "string"}, True),
        ({"type": "string"}, {}, True),
        ({"type": "string"}, {"type": "string"}, True),
        ({"type": "string"}, {"pythonType": "typing.Any"}, True),
        ({"type": "string"}, {"pythonType": "builtins.object"}, True),
        ({"type": "integer"}, {"type": "number"}, True),
        (
            {"type": "string"},
            {"anyOf": [1, {"type": "null"}, {"type": "string"}]},
            True,
        ),
        ({"type": "integer"}, {"anyOf": [{"type": "string"}]}, False),
        ({"type": "string"}, {"type": "integer"}, False),
    ),
)
def test_schema_compatibility_supports_safe_widening(
    source: dict[str, Any], target: dict[str, Any], expected: bool
) -> None:
    assert schemas_compatible(source, target) is expected
