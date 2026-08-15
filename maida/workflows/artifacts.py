"""Store immutable value payloads by content digest on the local filesystem.

Small canonical values can remain inline in :class:`StoredValue`; larger values
are written through :class:`ArtifactStore`. :class:`ValueCodec` presents the
same encode/decode interface for both representations and verifies every digest
before returning bytes.
"""

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
    """Raised when a referenced content-addressed blob does not exist."""


class CorruptArtifactError(ArtifactError):
    """Raised when stored bytes do not match their expected digest."""


class UnavailableValueError(ArtifactError):
    """Raised when a required value was recorded as unavailable."""


class ArtifactStore:
    """Private filesystem store for immutable SHA-256-addressed bytes.

    Parameters
    ----------
    root
        Directory containing digest-partitioned blob paths.
    create
        Create ``root`` with restrictive permissions when true. Set to false
        when opening an existing read-only bundle.
    """

    def __init__(self, root: Path, *, create: bool = True) -> None:
        self.root = root
        if create:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
            os.chmod(self.root, 0o700)

    def relative_path(self, digest: str) -> Path:
        """Return the two-level relative path for a validated SHA-256 digest.

        Raises
        ------
        ValueError
            If ``digest`` is not lowercase 64-character hexadecimal text.
        """
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError("artifact digest must be a lowercase SHA-256 hex value")
        return Path(digest[:2]) / digest[2:]

    def put(self, content: bytes) -> str:
        """Store immutable bytes and return their SHA-256 content address.

        Existing identical content is reused without mutation.
        """
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
        """Load bytes after verifying their path and content digest.

        Raises
        ------
        MissingArtifactError
            If the digest path does not exist.
        CorruptArtifactError
            If the stored bytes hash to a different digest.
        """
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
    """Encode typed canonical values inline or through an artifact store.

    Parameters
    ----------
    artifacts
        Content-addressed store for values larger than ``inline_limit``.
    inline_limit
        Maximum canonical JSON byte length retained inline. Zero forces all
        non-empty values into the artifact store.
    """

    def __init__(self, artifacts: ArtifactStore, *, inline_limit: int = 16_384) -> None:
        if inline_limit < 0:
            raise ValueError("inline_limit must not be negative")
        self.artifacts = artifacts
        self.inline_limit = inline_limit

    def encode(self, value: Any, *, schema_digest: str) -> StoredValue:
        """Encode a value and choose inline or artifact-backed storage.

        Parameters
        ----------
        value
            Canonically serializable typed value.
        schema_digest
            Digest of the declared type contract for the value.

        Returns
        -------
        StoredValue
            Immutable value reference containing content and schema digests.
        """
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
        """Create a non-replayable reference for a missing or redacted value."""
        return StoredValue(
            schema_digest=schema_digest,
            digest=digest,
            storage=ValueStorage.UNAVAILABLE,
            unavailable_reason=reason,
        )

    def bytes(self, value: StoredValue) -> bytes:
        """Resolve canonical bytes and verify their recorded content digest.

        Raises
        ------
        UnavailableValueError
            If the reference is explicitly unavailable.
        MissingArtifactError, CorruptArtifactError
            If artifact content cannot satisfy its integrity contract.
        """
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
        """Resolve, verify, and decode a stored canonical JSON value."""
        return json.loads(self.bytes(value))
