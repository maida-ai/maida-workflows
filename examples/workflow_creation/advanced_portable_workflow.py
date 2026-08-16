"""Author and serialize an editable workflow without Python import strings.

This example is the data-first counterpart to the native ``Workflow.build``
examples. A human, an AI agent, or a visual builder can produce the same
``WorkflowSpec``. A trusted application registry resolves its aliases, and a
``WorkflowBundle`` stores canonical source data plus exact compiled contracts.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from maida.workflows import (
    BindingSpec,
    ExecutionContext,
    Module,
    ModuleRegistry,
    ModuleTemplate,
    NodeSpec,
    WorkflowBundle,
    WorkflowSpec,
)
from maida.workflows._canonical import type_schema


class _Title(Module[str, str]):
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value.strip().title()


@dataclass(frozen=True)
class _PrefixConfig:
    text: str


class _Prefix(Module[str, str]):
    input_type = str
    output_type = str

    def __init__(self, config: _PrefixConfig) -> None:
        self.text = config.text

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return f"{self.text}{value}"


registry = ModuleRegistry(
    modules={"text.title": _Title},
    templates={
        "text.prefix": ModuleTemplate(
            "example.text.prefix",
            "1",
            _PrefixConfig,
            _Prefix,
        )
    },
)

spec = WorkflowSpec(
    workflow_id="onboarding-portable",
    input_schema=type_schema(str),
    output_schema=type_schema(str),
    nodes=(
        NodeSpec.task("title", "text.title", BindingSpec.root()),
        NodeSpec.task(
            "prefix",
            "text.prefix",
            BindingSpec.node("title"),
            config={"text": "[portable] "},
        ),
    ),
    output=BindingSpec.node("prefix"),
)

bundle = WorkflowBundle.from_spec(spec, registry)
workflow = bundle.bind(module_registry=registry)

EXAMPLE_INPUT = "  hello ada  "
EXPECTED_OUTPUT = "[portable] Hello Ada"


def save_and_restore(path: Path) -> WorkflowBundle:
    """Save canonical data privately, reload it, and verify trusted rebinding."""
    bundle.save(path)
    restored = WorkflowBundle.load(path)
    restored.bind(module_registry=registry)
    return restored
