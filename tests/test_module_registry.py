from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from maida.workflows import (
    Budget,
    ExecutionContext,
    ExecutionSpec,
    Module,
    ModuleRegistry,
    ModuleTemplate,
    module_digest,
)
from maida.workflows._canonical import schema_digest
from maida.workflows.registry import _catalog_entry


class Upper(Module[str, str]):
    module_id = "text.upper"
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
    assert catalog.descriptor("text.upper")["module_id"] == "text.upper"
    assert catalog.resolve_exact("text.upper", module_digest(first)).module_id == "text.upper"


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


def test_merged_registry_rejects_nonexecutable_and_unidentified_generated_bindings() -> None:
    descriptor_only = ModuleRegistry().allow(
        "trusted.only",
        module_id="trusted.only",
        module_digest="a" * 64,
        input_schema_digests=(schema_digest(str),),
        output_schema_digest=schema_digest(str),
        execution=ExecutionSpec().to_data(),
        budget=Budget(),
    )
    assert descriptor_only.describe()[0]["kind"] == "trusted"
    with pytest.raises(TypeError, match="no executable factory"):
        descriptor_only.resolve("trusted.only")

    unidentified = ModuleRegistry(modules={"text.prefix": lambda: Prefix(PrefixConfig(prefix=">"))})
    with pytest.raises(ValueError, match="self-declared module_id"):
        unidentified.descriptor("text.prefix")
    with pytest.raises(TypeError, match="return a Module"):
        ModuleRegistry(modules={"broken": lambda: object()}).resolve("broken")  # type: ignore[dict-item, return-value]


def test_template_and_descriptor_failures_are_explicit() -> None:
    with pytest.raises(ValueError, match="version"):
        ModuleTemplate("example.prefix", "", PrefixConfig, Prefix)
    with pytest.raises(TypeError, match="callable"):
        ModuleTemplate("example.prefix", "1", PrefixConfig, None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="ModuleTemplate"):
        ModuleRegistry(templates={"bad": object()})  # type: ignore[dict-item]

    template = ModuleTemplate("example.prefix", "1", PrefixConfig, Prefix)
    with pytest.raises(ValueError, match="object"):
        template.create([])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="config is invalid"):
        template.create({"prefix": object()})
    invalid_factory = ModuleTemplate(
        "example.invalid",
        "1",
        PrefixConfig,
        lambda config: object(),  # type: ignore[arg-type, return-value]
    )
    with pytest.raises(TypeError, match="return a Module"):
        invalid_factory.create({"prefix": ">"})
    with pytest.raises(ValueError, match="author-supplied config"):
        ModuleRegistry(templates={"text.prefix": template}).descriptor("text.prefix")

    descriptor: dict[str, Any] = {
        "module_id": "modules.text",
        "module_digest": "a" * 64,
        "input_schema_digests": (schema_digest(str),),
        "output_schema_digest": schema_digest(str),
        "capabilities": (),
        "effects": (),
    }
    with pytest.raises(ValueError, match="execution is invalid"):
        _catalog_entry(**descriptor, execution={"isolation": "invalid"}, budget=Budget())
    with pytest.raises(ValueError, match="budget must be"):
        _catalog_entry(**descriptor, execution=ExecutionSpec().to_data(), budget="invalid")
    with pytest.raises(ValueError, match="budget is invalid"):
        _catalog_entry(**descriptor, execution=ExecutionSpec().to_data(), budget={})
