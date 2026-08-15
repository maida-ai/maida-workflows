from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from ._canonical import canonical_json, digest_bytes
from .models import StoredValue, ValueStorage


class ArtifactError(RuntimeError):
    """Base class for immutable artifact failures."""


class MissingArtifactError(ArtifactError):
    pass


class CorruptArtifactError(ArtifactError):
    pass


class UnavailableValueError(ArtifactError):
    pass


class ArtifactStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)

    def relative_path(self, digest: str) -> Path:
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError("artifact digest must be a lowercase SHA-256 hex value")
        return Path(digest[:2]) / digest[2:]

    def put(self, content: bytes) -> str:
        digest = digest_bytes(content)
        relative = self.relative_path(digest)
        directory = self.root / relative.parent
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(directory, 0o700)
        path = self.root / relative
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            if self.get(digest) != content:  # pragma: no cover - get raises on hash mismatch
                raise CorruptArtifactError(f"artifact {digest} has conflicting content") from None
            return digest
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        return digest

    def get(self, digest: str) -> bytes:
        path = self.root / self.relative_path(digest)
        try:
            content = path.read_bytes()
        except FileNotFoundError as exc:
            raise MissingArtifactError(f"required artifact {digest} is missing") from exc
        actual = digest_bytes(content)
        if actual != digest:
            raise CorruptArtifactError(f"artifact {digest} failed integrity check (found {actual})")
        return content


class ValueCodec:
    def __init__(self, artifacts: ArtifactStore, *, inline_limit: int = 16_384) -> None:
        if inline_limit < 0:
            raise ValueError("inline_limit must not be negative")
        self.artifacts = artifacts
        self.inline_limit = inline_limit

    def encode(self, value: Any, *, schema_digest: str) -> StoredValue:
        data = canonical_json(value).encode()
        digest = digest_bytes(data)
        if len(data) <= self.inline_limit:
            return StoredValue(
                schema_digest=schema_digest,
                digest=digest,
                storage=ValueStorage.INLINE,
                inline=json.loads(data),
            )
        artifact_digest = self.artifacts.put(data)
        return StoredValue(
            schema_digest=schema_digest,
            digest=digest,
            storage=ValueStorage.ARTIFACT,
            artifact_digest=artifact_digest,
        )

    def unavailable(self, *, schema_digest: str, digest: str, reason: str) -> StoredValue:
        return StoredValue(
            schema_digest=schema_digest,
            digest=digest,
            storage=ValueStorage.UNAVAILABLE,
            unavailable_reason=reason,
        )

    def bytes(self, value: StoredValue) -> bytes:
        if value.storage is ValueStorage.UNAVAILABLE:
            reason = value.unavailable_reason or "unknown reason"
            raise UnavailableValueError(f"required value is unavailable: {reason}")
        if value.storage is ValueStorage.INLINE:
            content = canonical_json(value.inline).encode()
        else:
            if value.artifact_digest is None:
                raise MissingArtifactError("artifact-backed value has no artifact digest")
            content = self.artifacts.get(value.artifact_digest)
        actual = digest_bytes(content)
        if actual != value.digest:
            raise CorruptArtifactError(
                f"value {value.digest} failed integrity check (found {actual})"
            )
        return content

    def decode(self, value: StoredValue) -> Any:
        return json.loads(self.bytes(value))
