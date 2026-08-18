from __future__ import annotations

import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.version import Version


def test_core_dependency_floor_requires_plan_demo_release() -> None:
    project = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]
    requirements = [Requirement(value) for value in project["dependencies"]]
    core = next(requirement for requirement in requirements if requirement.name == "maida-ai")

    assert Version("0.5.2") not in core.specifier
    assert Version("0.5.2.post1") in core.specifier
