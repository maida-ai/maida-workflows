from __future__ import annotations

from dataclasses import dataclass

import pytest

from maida.workflows import (
    ExecutionContext,
    Module,
    ModuleRegistry,
    ModuleTemplate,
    module_digest,
)


class Upper(Module[str, str]):
    input_type = str
    output_type = str

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return value.upper()


@dataclass(frozen=True)
class PrefixConfig:
    prefix: str
    repeat: int = 1


class Prefix(Module[str, str]):
    input_type = str
    output_type = str

    def __init__(self, config: PrefixConfig) -> None:
        self.prefix = config.prefix
        self.repeat = config.repeat

    async def execute(self, value: str, ctx: ExecutionContext) -> str:
        return self.prefix * self.repeat + value


def registry() -> ModuleRegistry:
    return ModuleRegistry(
        modules={"text.upper": Upper},
        templates={
            "text.prefix": ModuleTemplate(
                template_id="maida.examples.prefix",
                version="1",
                config_type=PrefixConfig,
                factory=Prefix,
            )
        },
    )


def test_fixed_registration_resolves_fresh_exact_modules() -> None:
    catalog = registry()

    first = catalog.resolve("text.upper")
    second = catalog.resolve("text.upper", expected_digest=module_digest(first))

    assert isinstance(first, Upper)
    assert isinstance(second, Upper)
    assert first is not second


def test_parameterized_template_validates_and_canonicalizes_config() -> None:
    catalog = registry()

    module = catalog.resolve("text.prefix", {"repeat": 2, "prefix": ">"})
    requirement = catalog.requirement("text.prefix", {"prefix": ">", "repeat": 2})

    assert isinstance(module, Prefix)
    assert module.prefix == ">"
    assert module.repeat == 2
    assert requirement["alias"] == "text.prefix"
    assert requirement["template"] == {
        "id": "maida.examples.prefix",
        "version": "1",
    }
    assert requirement["config"] == {"prefix": ">", "repeat": 2}
    assert requirement["module_digest"] == module_digest(module)


def test_registry_fails_closed_for_invalid_or_changed_bindings() -> None:
    catalog = registry()

    with pytest.raises(ValueError, match="config"):
        catalog.resolve("text.prefix", {"prefix": 3})
    with pytest.raises(ValueError, match="does not accept config"):
        catalog.resolve("text.upper", {"unexpected": True})
    with pytest.raises(ValueError, match="digest"):
        catalog.resolve("text.upper", expected_digest="0" * 64)
    with pytest.raises(KeyError, match="not registered"):
        catalog.resolve("missing")


def test_registry_description_is_credential_free_data_not_python_imports() -> None:
    description = registry().describe()

    assert description[0]["alias"] == "text.prefix"
    assert description[1]["alias"] == "text.upper"
    assert "factory" not in repr(description)
    assert "__main__" not in repr(description)


def test_registry_rejects_ambiguous_or_unstable_aliases() -> None:
    with pytest.raises(ValueError, match="stable"):
        ModuleRegistry(modules={"bad alias": Upper})
    with pytest.raises(ValueError, match="registered twice"):
        ModuleRegistry(
            modules={"same": Upper},
            templates={"same": ModuleTemplate("example.same", "1", PrefixConfig, Prefix)},
        )
