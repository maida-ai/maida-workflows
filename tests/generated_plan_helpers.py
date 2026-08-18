from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from maida.workflows._canonical import canonical_json, digest_data


def plan_node(key: str, module_alias: str, dependencies: Sequence[str]) -> dict[str, Any]:
    return {
        "dependencies": list(dependencies),
        "key": key,
        "module_alias": module_alias,
    }


def generated_plan(
    fragment_id: str,
    nodes: Iterable[Mapping[str, Any]],
    outputs: Sequence[str],
    version: str = "0.2.0",
) -> dict[str, Any]:
    return {
        "fragment_id": fragment_id,
        "nodes": sorted((dict(node) for node in nodes), key=lambda node: str(node["key"])),
        "outputs": list(outputs),
        "version": version,
    }


def plan_digest(plan: Mapping[str, Any]) -> str:
    return digest_data(plan)


def plan_json(plan: Mapping[str, Any]) -> str:
    return canonical_json(plan)
