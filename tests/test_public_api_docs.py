from __future__ import annotations

import ast
import importlib
from pathlib import Path
from types import ModuleType

import maida.workflows as workflows

PUBLIC_MODULES = (
    "maida.workflows",
    "maida.workflows.access",
    "maida.workflows.alignment",
    "maida.workflows.asgi",
    "maida.workflows.artifacts",
    "maida.workflows.authoring",
    "maida.workflows.baseline",
    "maida.workflows.budget",
    "maida.workflows.cli",
    "maida.workflows.coordination",
    "maida.workflows.dynamic",
    "maida.workflows.fixture",
    "maida.workflows.ir",
    "maida.workflows.interactions",
    "maida.workflows.interop",
    "maida.workflows.models",
    "maida.workflows.model",
    "maida.workflows.materialization",
    "maida.workflows.persistence",
    "maida.workflows.replay",
    "maida.workflows.runtime",
    "maida.workflows.userplane",
    "maida.workflows.verification",
    "examples",
    "examples.adversarial_workflows",
    "examples.native_replay_demo",
    "examples.userplane_quickstart",
    "examples.workflow_creation",
    "examples.workflow_creation.easy_first_workflow",
    "examples.workflow_creation.easy_sequential",
    "examples.workflow_creation.intermediate_branching",
    "examples.workflow_creation.intermediate_parallel",
    "examples.workflow_creation.advanced_stable_map",
    "examples.workflow_creation.advanced_nested",
    "examples.workflow_creation.expert_replay_ready",
)


def _tree(module: ModuleType) -> ast.Module:
    assert module.__file__ is not None
    return ast.parse(Path(module.__file__).read_text())


def _definition(
    module: ModuleType, name: str
) -> ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef:
    for node in _tree(module).body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and (
            node.name == name
        ):
            return node
    raise AssertionError(f"{module.__name__}.{name} has no source definition")


def _assert_descriptive(docstring: str | None, label: str, *, minimum: int = 20) -> None:
    assert docstring is not None, f"{label} has no docstring"
    assert len(docstring.strip()) >= minimum, f"{label} has only a placeholder docstring"


def test_user_facing_modules_have_descriptive_docstrings() -> None:
    for module_name in PUBLIC_MODULES:
        module = importlib.import_module(module_name)
        _assert_descriptive(ast.get_docstring(_tree(module)), module_name, minimum=40)


def test_root_exports_have_explicit_docstrings() -> None:
    for name in workflows.__all__:
        value = getattr(workflows, name)
        module = importlib.import_module(value.__module__)
        definition = _definition(module, value.__name__)
        _assert_descriptive(ast.get_docstring(definition), f"{value.__module__}.{value.__name__}")


def test_public_submodule_objects_have_explicit_docstrings() -> None:
    for module_name in PUBLIC_MODULES:
        if module_name in {"maida.workflows", "maida.workflows.cli"}:
            continue
        module = importlib.import_module(module_name)
        for node in _tree(module).body:
            if not isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name.startswith("_"):
                continue
            _assert_descriptive(ast.get_docstring(node), f"{module_name}.{node.name}")


def test_public_methods_on_exported_classes_have_explicit_docstrings() -> None:
    documented_dunders = {"__bool__", "__call__", "__iter__", "__len__"}
    for name in workflows.__all__:
        value = getattr(workflows, name)
        if not isinstance(value, type):
            continue
        module = importlib.import_module(value.__module__)
        definition = _definition(module, value.__name__)
        assert isinstance(definition, ast.ClassDef)
        for member in definition.body:
            if not isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if member.name.startswith("_") and member.name not in documented_dunders:
                continue
            _assert_descriptive(
                ast.get_docstring(member),
                f"{value.__module__}.{value.__name__}.{member.name}",
            )
