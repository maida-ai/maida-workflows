from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

import pytest

from maida.workflows import (
    BudgetUsage,
    ModelAdapterRegistry,
    ModelBroker,
    ModelCallResult,
    ModelSpec,
)
from maida.workflows.model import _model_contract, _validated_model_contract


@dataclass(frozen=True)
class Request:
    text: str


@dataclass(frozen=True)
class Response:
    text: str


SPEC = ModelSpec(
    name="writer",
    provider="test",
    model="writer-v1",
    input_type=Request,
    output_type=Response,
    pinned_version="2026-08",
    configuration={"temperature": 0.0},
)


class Meter:
    def __init__(self) -> None:
        self.reserved: list[tuple[str, BudgetUsage]] = []
        self.committed: list[tuple[str, BudgetUsage]] = []

    def reserve_model(self, name: str, usage: BudgetUsage) -> str:
        self.reserved.append((name, usage))
        return f"reservation:{name}"

    def commit_model(self, reservation: str, usage: BudgetUsage) -> None:
        self.committed.append((reservation, usage))


class Adapter:
    def __init__(self, *, estimate: Any = None, result: Any = None) -> None:
        self.estimate = BudgetUsage(model_tokens=2) if estimate is None else estimate
        self.result = (
            ModelCallResult(
                Response("ok"),
                "writer-served",
                BudgetUsage(model_tokens=2, cost_usd=0.01),
                input_tokens=1,
                output_tokens=1,
                metadata={"region": "local"},
            )
            if result is None
            else result
        )

    def estimate_call(self, model: ModelSpec[Any, Any], request: Any) -> Any:
        return self.estimate

    async def call(self, model: ModelSpec[Any, Any], request: Any) -> Any:
        return self.result


def broker(adapter: Adapter) -> tuple[ModelBroker, Meter, dict[str, Any], list[tuple[str, Any]]]:
    meter = Meter()
    metadata: dict[str, Any] = {}
    audit: list[tuple[str, Any]] = []
    return (
        ModelBroker(
            ModelAdapterRegistry({"test": adapter}),
            (SPEC,),
            meter=meter,
            metadata=metadata,
            audit=lambda name, payload: audit.append((name, payload)),
        ),
        meter,
        metadata,
        audit,
    )


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"name": "not a name"}, "stable identifier"),
        ({"provider": "not a provider"}, "stable identifier"),
        ({"model": "  "}, "non-empty"),
        ({"pinned_version": ""}, "non-empty"),
        ({"configuration": {"nested": [{"api-key": "secret"}]}}, "credentials"),
    ],
)
def test_model_spec_rejects_unstable_or_sensitive_declarations(
    changes: dict[str, Any], message: str
) -> None:
    values: dict[str, Any] = {
        "name": "writer",
        "provider": "test",
        "model": "writer-v1",
        "input_type": Request,
        "output_type": Response,
    }
    values.update(changes)

    with pytest.raises(ValueError, match=message):
        ModelSpec(**values)


def test_model_call_result_validates_token_evidence_and_freezes_metadata() -> None:
    result = ModelCallResult(
        Response("ok"),
        "served",
        BudgetUsage(model_tokens=3),
        metadata={"nested": {"value": 1}},
    )

    assert result.input_tokens == 3
    assert result.output_tokens == 0
    assert result.metadata == {"nested": {"value": 1}}
    with pytest.raises(ValueError, match="served_model"):
        ModelCallResult(Response("ok"), "", BudgetUsage())
    with pytest.raises(TypeError, match="BudgetUsage"):
        ModelCallResult(Response("ok"), "served", object())  # type: ignore[arg-type]
    for input_tokens, output_tokens in ((None, 1), (-1, 4), (1, 1)):
        with pytest.raises(ValueError, match="token breakdown"):
            ModelCallResult(
                Response("ok"),
                "served",
                BudgetUsage(model_tokens=3),
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )


def test_model_adapter_registry_and_broker_declarations_fail_closed() -> None:
    with pytest.raises(ValueError, match="stable identifiers"):
        ModelAdapterRegistry({"not valid": Adapter()})
    with pytest.raises(TypeError, match="implement"):
        ModelAdapterRegistry({"test": object()})  # type: ignore[dict-item]
    with pytest.raises(LookupError, match="not registered"):
        ModelAdapterRegistry().resolve("missing")
    with pytest.raises(TypeError, match="ModelSpec"):
        ModelBroker(
            ModelAdapterRegistry(),
            (object(),),  # type: ignore[arg-type]
            meter=Meter(),
            metadata={},
            audit=lambda *_: None,
        )
    with pytest.raises(ValueError, match="unique"):
        ModelBroker(
            ModelAdapterRegistry(),
            (SPEC, SPEC),
            meter=Meter(),
            metadata={},
            audit=lambda *_: None,
        )


@pytest.mark.asyncio
async def test_model_broker_records_typed_success() -> None:
    selected, meter, metadata, audit = broker(Adapter())

    output: Response = await selected.call("writer", Request("hello"))

    assert output == Response("ok")
    assert meter.reserved == [("writer", BudgetUsage(model_tokens=2))]
    assert meter.committed[0][0] == "reservation:writer"
    assert [name for name, _ in audit] == ["MODEL_RESOLVED", "MODEL_CALLED"]
    assert metadata["usage"] == {
        "input_tokens": 1,
        "output_tokens": 1,
        "cost_usd": 0.01,
    }
    assert metadata["trajectories"][0]["metadata"]["region"] == "local"


@pytest.mark.asyncio
async def test_model_broker_rejects_each_untrusted_adapter_boundary() -> None:
    selected, _, _, _ = broker(Adapter())
    with pytest.raises(LookupError, match="not declared"):
        await selected.call("missing", Request("hello"))
    with pytest.raises(TypeError, match="request"):
        await selected.call("writer", "wrong")

    selected, _, _, _ = broker(Adapter(estimate=object()))
    with pytest.raises(TypeError, match="estimate"):
        await selected.call("writer", Request("hello"))

    selected, _, _, _ = broker(Adapter(result=object()))
    with pytest.raises(TypeError, match="ModelCallResult"):
        await selected.call("writer", Request("hello"))

    selected, _, _, _ = broker(
        Adapter(
            result=ModelCallResult(
                "wrong",
                "served",
                BudgetUsage(model_tokens=2),
            )
        )
    )
    with pytest.raises(TypeError, match="response"):
        await selected.call("writer", Request("hello"))


def test_module_model_contract_rejects_mutable_or_duplicate_declarations() -> None:
    class Invalid:
        models: Any

    target = Invalid()
    target.models = [SPEC]
    with pytest.raises(TypeError, match="tuple"):
        _model_contract(target)
    target.models = (object(),)
    with pytest.raises(TypeError, match="tuple"):
        _model_contract(target)
    target.models = (SPEC, SPEC)
    with pytest.raises(ValueError, match="unique"):
        _model_contract(target)


def test_imported_model_contract_is_strict_and_canonical() -> None:
    valid = SPEC.to_data()
    second = ModelSpec(
        "analyst",
        "test",
        "analyst-v1",
        Request,
        Response,
    ).to_data()
    assert _validated_model_contract([second, valid], require_canonical=False) == (
        second,
        valid,
    )
    with pytest.raises(ValueError, match="canonical order"):
        _validated_model_contract([valid, second])
    with pytest.raises(ValueError, match="array"):
        _validated_model_contract({})
    with pytest.raises(ValueError, match="fields"):
        _validated_model_contract([{"name": "writer"}])

    mutations: tuple[tuple[str, Any, str], ...] = (
        ("name", "not valid", "name"),
        ("provider", "not valid", "provider"),
        ("model", "", "model"),
        ("pinned_version", 1, "pinned_version"),
        ("configuration", [], "configuration"),
        ("configuration", {"password": "secret"}, "sensitive"),
        ("input_schema_digest", "bad", "input_schema_digest"),
        ("output_schema_digest", "z" * 64, "output_schema_digest"),
    )
    for field, value, message in mutations:
        changed = deepcopy(valid)
        changed[field] = value
        with pytest.raises(ValueError, match=message):
            _validated_model_contract([changed])

    with pytest.raises(ValueError, match="unique"):
        _validated_model_contract([valid, valid])
