"""Create payload-free baselines from one or more replay fixtures.

Baselines retain fixture digests, source provenance, and an optional acceptance
record. They intentionally do not copy workflow inputs, outputs, or artifact
payloads, making them suitable for version control and verification policy.
"""

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
    """Digest and native-run provenance for one baseline fixture."""

    fixture_digest: str
    source_kind: str
    source_run_id: str
    source_completed_at: str


@dataclass(frozen=True)
class ReplayBaseline:
    """Deterministic population of fixture digests for one workflow.

    Attributes
    ----------
    version
        Baseline schema version.
    workflow_id
        Workflow shared by every source fixture.
    population_digest
        Digest of the ordered fixture-digest population.
    sources
        Fixture digests and their native source provenance.
    provenance
        User-supplied acceptance or creation metadata without payloads.
    """

    version: str
    workflow_id: str
    population_digest: str
    sources: tuple[BaselineSource, ...]
    provenance: dict[str, Any]

    def to_data(self) -> dict[str, Any]:
        """Return a canonical JSON-compatible baseline representation."""
        return cast(dict[str, Any], canonical_data(asdict(self)))

    @property
    def digest(self) -> str:
        """Return the SHA-256 digest of the canonical baseline data."""
        return digest_data(self.to_data())


def create_baseline(
    fixtures: Sequence[ReplayFixture],
    *,
    provenance: dict[str, Any] | None = None,
) -> ReplayBaseline:
    """Create a deterministic payload-free baseline from replay fixtures.

    Parameters
    ----------
    fixtures
        One or more fixtures for the same workflow.
    provenance
        Optional JSON-compatible acceptance or creator metadata.

    Returns
    -------
    ReplayBaseline
        Sources sorted by completion time and digest.

    Raises
    ------
    ValueError
        If no fixtures are supplied or they describe different workflows.
    """
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
    """Write canonical baseline JSON to a new private local file.

    The parent directory is created with restrictive permissions. Existing
    output files are never overwritten.
    """
    output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(canonical_json(baseline.to_data()).encode())
