from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

from ._canonical import canonical_data, canonical_json, digest_data
from .fixture import ReplayFixture

BASELINE_VERSION = "0.1.0"


@dataclass(frozen=True)
class BaselineSource:
    fixture_digest: str
    source_kind: str
    source_run_id: str
    source_completed_at: str


@dataclass(frozen=True)
class ReplayBaseline:
    version: str
    workflow_id: str
    population_digest: str
    sources: tuple[BaselineSource, ...]
    provenance: dict[str, Any]

    def to_data(self) -> dict[str, Any]:
        return cast(dict[str, Any], canonical_data(asdict(self)))

    @property
    def digest(self) -> str:
        return digest_data(self.to_data())


def create_baseline(
    fixtures: Sequence[ReplayFixture],
    *,
    provenance: dict[str, Any] | None = None,
) -> ReplayBaseline:
    if not fixtures:
        raise ValueError("a replay baseline requires at least one fixture")
    workflow_ids = {fixture.workflow_ir.workflow_id for fixture in fixtures}
    if len(workflow_ids) != 1:
        raise ValueError("all baseline fixtures must describe the same workflow")
    sources = tuple(
        BaselineSource(
            fixture.digest,
            fixture.source.kind,
            fixture.source.run_id,
            fixture.source.completed_at,
        )
        for fixture in sorted(fixtures, key=lambda item: (item.source.completed_at, item.digest))
    )
    return ReplayBaseline(
        BASELINE_VERSION,
        next(iter(workflow_ids)),
        digest_data([source.fixture_digest for source in sources]),
        sources,
        dict(provenance or {}),
    )


def write_baseline(baseline: ReplayBaseline, output: Path) -> None:
    output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(canonical_json(baseline.to_data()).encode())
