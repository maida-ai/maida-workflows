from __future__ import annotations

import os
from pathlib import Path

import pytest

from maida_workflows._canonical import digest_bytes
from maida_workflows.artifacts import (
    ArtifactStore,
    CorruptArtifactError,
    MissingArtifactError,
    UnavailableValueError,
    ValueCodec,
)


def test_inline_and_artifact_values_round_trip_identically(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "artifacts")
    inline = ValueCodec(store, inline_limit=1_000).encode({"answer": [1, 2]}, schema_digest="s")
    artifact = ValueCodec(store, inline_limit=0).encode({"answer": [1, 2]}, schema_digest="s")

    assert inline.digest == artifact.digest
    assert ValueCodec(store).decode(inline) == ValueCodec(store).decode(artifact)
    assert os.stat(store.root).st_mode & 0o777 == 0o700
    artifact_path = store.root / store.relative_path(artifact.artifact_digest or "")
    assert os.stat(artifact_path).st_mode & 0o777 == 0o600


def test_missing_corrupt_and_unavailable_values_fail_closed(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "artifacts")
    codec = ValueCodec(store, inline_limit=0)
    encoded = codec.encode("important", schema_digest="s")
    artifact_path = store.root / store.relative_path(encoded.artifact_digest or "")
    artifact_path.unlink()
    with pytest.raises(MissingArtifactError, match="missing"):
        codec.decode(encoded)

    digest = store.put(b"original")
    path = store.root / store.relative_path(digest)
    path.write_bytes(b"tampered")
    with pytest.raises(CorruptArtifactError, match="integrity"):
        store.get(digest)

    unavailable = codec.unavailable(
        schema_digest="s", digest=digest_bytes(b"secret"), reason="redacted"
    )
    with pytest.raises(UnavailableValueError, match="redacted"):
        codec.decode(unavailable)


def test_artifact_digest_paths_are_validated(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path)
    with pytest.raises(ValueError, match="SHA-256"):
        store.get("../not-a-digest")
    with pytest.raises(ValueError, match="inline_limit"):
        ValueCodec(store, inline_limit=-1)
