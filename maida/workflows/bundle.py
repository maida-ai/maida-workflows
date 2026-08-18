"""Serialize workflow definitions as safe, canonical, data-only bundles.

A ``.maida-workflow`` file contains canonical Workflow IR, exact trusted
binding requirements, and credential-free provenance. Loading validates bytes
and digests but never imports or executes Python. Explicit registry or catalog
binding is required before execution.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

from ._canonical import canonical_data, canonical_json, digest_data
from .authoring import Workflow
from .coordination import WorkflowCatalog
from .definitions import BoundWorkflow, bind_workflow
from .ir import PlanIR, ReplayKey
from .registry import ModuleRegistry

BUNDLE_VERSION = "0.3.0"
DEFAULT_MAX_BUNDLE_BYTES = 16 * 1024 * 1024


class WorkflowPortability(StrEnum):
    """How canonical plan data can be rebound to trusted executable modules."""

    RECONSTRUCTABLE = "reconstructable"
    FACTORY_BOUND = "factory-bound"


class WorkflowBundleError(ValueError):
    """Raised when workflow bundle bytes, identity, or binding are invalid."""


@dataclass(frozen=True)
class WorkflowBundle:
    """Canonical portable workflow definition with explicit trust requirements.

    Parameters
    ----------
    plan
        Canonical compiled Workflow IR used by scheduling and verification.
    binding_requirements
        Credential-free module identities and digests required to bind the
        plan.
    portability
        Trusted rebinding strategy.
    provenance
        Small credential-free source metadata. Runtime payloads and identities
        never belong here.
    version
        Bundle format version.

    Notes
    -----
    This class deliberately does not serialize Python bytecode, pickle data,
    import paths, connector sessions, credentials, or run history. Use a
    :class:`~maida.workflows.ReplayFixture` for accepted execution history.

    Examples
    --------
    >>> bundle = WorkflowBundle.from_plan(plan, registry)  # doctest: +SKIP
    >>> bundle.save(Path("review.maida-workflow"))  # doctest: +SKIP
    >>> bound = WorkflowBundle.load(Path("review.maida-workflow")).bind(  # doctest: +SKIP
    ...     module_registry=registry
    ... )
    """

    plan: PlanIR
    binding_requirements: tuple[Mapping[str, Any], ...]
    portability: WorkflowPortability
    provenance: Mapping[str, Any]
    version: str = BUNDLE_VERSION

    def __post_init__(self) -> None:
        """Validate cross-object identity and freeze canonical metadata."""
        if self.version != BUNDLE_VERSION:
            raise WorkflowBundleError(f"unsupported workflow bundle version {self.version!r}")
        if not isinstance(self.portability, WorkflowPortability):
            raise WorkflowBundleError("workflow bundle portability is invalid")
        requirements = tuple(
            MappingProxyType(cast(dict[str, Any], canonical_data(dict(requirement))))
            for requirement in self.binding_requirements
        )
        requirements = tuple(sorted(requirements, key=canonical_json))
        object.__setattr__(self, "binding_requirements", requirements)
        provenance = cast(dict[str, Any], canonical_data(dict(self.provenance)))
        object.__setattr__(self, "provenance", MappingProxyType(provenance))

    @classmethod
    def from_workflow(
        cls,
        workflow: Workflow[Any, Any],
        registry: ModuleRegistry | None = None,
    ) -> WorkflowBundle:
        """Capture an arbitrary Python workflow as an exact factory-bound file.

        Parameters
        ----------
        workflow
            Native Python workflow to compile exactly once.
        registry
            Optional registry used only to annotate uniquely matching fixed
            module aliases. It does not make arbitrary Python graph code
            reconstructable.

        Returns
        -------
        WorkflowBundle
            Data-only plan that requires a digest-pinned ``WorkflowCatalog``
            factory before execution.
        """
        bound = bind_workflow(workflow)
        aliases = _fixed_aliases_by_digest(registry) if registry is not None else {}
        requirements = tuple(
            {
                "module_id": key.module_id,
                "logical_step": key.logical_step,
                "module_digest": step.module_digest,
                "alias": aliases.get(cast(str, step.module_digest)),
            }
            for key, step in (
                (cast(Any, step.replay_key), step)
                for step in bound.plan.executable_steps
                if step.replay_key is not None
            )
        )
        return cls(
            plan=bound.plan,
            binding_requirements=requirements,
            portability=WorkflowPortability.FACTORY_BOUND,
            provenance={"source": "python-workflow"},
        )

    @classmethod
    def from_plan(cls, plan: PlanIR, registry: ModuleRegistry) -> WorkflowBundle:
        """Bundle canonical PlanIR with exact trusted module requirements."""
        if not isinstance(plan, PlanIR):
            raise TypeError("plan must be PlanIR")
        try:
            requirements, _modules = _reconstruct_plan(plan, registry)
        except (LookupError, TypeError, ValueError) as exc:
            raise WorkflowBundleError(
                "canonical plan cannot bind through this module registry"
            ) from exc
        return cls(
            plan=PlanIR.from_dict(plan.to_dict()),
            binding_requirements=requirements,
            portability=WorkflowPortability.RECONSTRUCTABLE,
            provenance={"source": "canonical-plan"},
        )

    @classmethod
    def load(
        cls,
        path: Path,
        *,
        max_bytes: int = DEFAULT_MAX_BUNDLE_BYTES,
    ) -> WorkflowBundle:
        """Load and integrity-check canonical bundle bytes without executing code.

        Parameters
        ----------
        path
            Local ``.maida-workflow`` file.
        max_bytes
            Positive byte limit checked before parsing. The default is 16 MiB.

        Returns
        -------
        WorkflowBundle
            Strictly validated data-only definition.

        Raises
        ------
        WorkflowBundleError
            If size, UTF-8, JSON, canonical encoding, schema, or a digest is
            invalid.
        """
        if type(max_bytes) is not int or max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        try:
            size = path.stat().st_size
        except OSError as exc:
            raise WorkflowBundleError(f"cannot read workflow bundle: {exc}") from exc
        if size > max_bytes:
            raise WorkflowBundleError("workflow bundle exceeds the configured maximum size")
        try:
            raw = path.read_bytes()
            text = raw.decode("utf-8")
            data = json.loads(text, object_pairs_hook=_unique_object)
        except WorkflowBundleError:
            raise
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WorkflowBundleError("workflow bundle is not valid UTF-8 JSON") from exc
        if not isinstance(data, Mapping):
            raise WorkflowBundleError("workflow bundle root must be an object")
        expected = {
            "format",
            "version",
            "workflow_id",
            "definition_digest",
            "portability",
            "plan",
            "bindings",
            "provenance",
            "bundle_digest",
        }
        if set(data) != expected:
            raise WorkflowBundleError("workflow bundle fields are invalid")
        if canonical_json(data) != text:
            raise WorkflowBundleError("workflow bundle bytes are not canonical JSON")
        payload = dict(data)
        claimed_bundle_digest = payload.pop("bundle_digest")
        if claimed_bundle_digest != digest_data(payload):
            raise WorkflowBundleError("workflow bundle digest is invalid")
        if data["format"] != "maida-workflow" or data["version"] != BUNDLE_VERSION:
            raise WorkflowBundleError("workflow bundle format or version is unsupported")
        try:
            plan_data = data["plan"]
            if not isinstance(plan_data, Mapping):
                raise ValueError("plan must be an object")
            plan = PlanIR.from_dict(plan_data)
            portability = WorkflowPortability(data["portability"])
            bindings = data["bindings"]
            provenance = data["provenance"]
            if not isinstance(bindings, list) or any(
                not isinstance(item, Mapping) for item in bindings
            ):
                raise ValueError("bindings must be an array of objects")
            if not isinstance(provenance, Mapping):
                raise ValueError("provenance must be an object")
            bundle = cls(
                plan=plan,
                binding_requirements=tuple(cast(Mapping[str, Any], item) for item in bindings),
                portability=portability,
                provenance=provenance,
                version=str(data["version"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkflowBundleError(f"workflow bundle contract is invalid: {exc}") from exc
        if data["workflow_id"] != bundle.plan.workflow_id:
            raise WorkflowBundleError("workflow bundle workflow identity is invalid")
        if data["definition_digest"] != bundle.plan.digest:
            raise WorkflowBundleError("workflow bundle definition digest is invalid")
        if bundle.to_dict() != dict(data):
            raise WorkflowBundleError("workflow bundle is not in canonical contract form")
        return bundle

    def bind(
        self,
        *,
        module_registry: ModuleRegistry | None = None,
        workflow_catalog: WorkflowCatalog | None = None,
    ) -> BoundWorkflow:
        """Bind safe data to exact application-owned Python implementations.

        Parameters
        ----------
        module_registry
            Required for reconstructable plans. Every module identity and
            digest is recomputed from trusted code.
        workflow_catalog
            Required for factory-bound Python workflows. Resolution uses the
            exact persisted definition digest.

        Returns
        -------
        BoundWorkflow
            Executable graph whose canonical plan exactly matches this bundle.

        Raises
        ------
        WorkflowBundleError
            If the appropriate trust registry is missing or current behavior
            no longer matches the serialized definition.
        """
        if self.portability is WorkflowPortability.RECONSTRUCTABLE:
            if module_registry is None:
                raise WorkflowBundleError(
                    "canonical plan bundle requires a trusted module registry"
                )
            try:
                requirements, modules = _reconstruct_plan(self.plan, module_registry)
                if canonical_json(requirements) != canonical_json(self.binding_requirements):
                    raise ValueError("canonical plan binding requirements changed")
                return BoundWorkflow(
                    plan=self.plan,
                    input_type=Any,
                    output_type=Any,
                    modules=modules,
                )
            except (LookupError, TypeError, ValueError) as exc:
                raise WorkflowBundleError(
                    "canonical plan bundle cannot rebind through this registry"
                ) from exc
        if workflow_catalog is None:
            raise WorkflowBundleError("factory-bound bundle requires a trusted workflow catalog")
        try:
            workflow = workflow_catalog.resolve(self.plan.digest)
            bound = bind_workflow(workflow)
        except (TypeError, ValueError) as exc:
            raise WorkflowBundleError("workflow catalog cannot rebind this definition") from exc
        if bound.plan.canonical_json() != self.plan.canonical_json():
            raise WorkflowBundleError("workflow catalog rebind changed the exact definition")
        return bound

    def to_dict(self) -> dict[str, Any]:
        """Return canonical JSON-compatible bundle data including integrity digest."""
        payload = {
            "format": "maida-workflow",
            "version": self.version,
            "workflow_id": self.plan.workflow_id,
            "definition_digest": self.plan.digest,
            "portability": self.portability.value,
            "plan": self.plan.to_dict(),
            "bindings": [canonical_data(item) for item in self.binding_requirements],
            "provenance": canonical_data(self.provenance),
        }
        return {**payload, "bundle_digest": digest_data(payload)}

    def canonical_json(self) -> str:
        """Serialize this bundle as deterministic canonical JSON bytes text."""
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        """Return the integrity digest covering every non-digest bundle field."""
        return cast(str, self.to_dict()["bundle_digest"])

    def save(self, path: Path) -> None:
        """Atomically write canonical bytes with owner-only local permissions.

        Parameters
        ----------
        path
            Destination file. Parent directories are created with owner-only
            permissions when absent. Existing files are atomically replaced.

        Notes
        -----
        Saving is explicit and local. This method never uploads, publishes, or
        adds the file to version control.
        """
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        content = self.canonical_json().encode("utf-8")
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        temporary_path = Path(temporary)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, path)
            path.chmod(0o600)
        except BaseException:
            try:
                temporary_path.unlink(missing_ok=True)
            finally:
                raise


def _unique_object(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise WorkflowBundleError(f"workflow bundle contains duplicate field {key!r}")
        result[key] = value
    return result


def _reconstruct_plan(
    plan: PlanIR,
    registry: ModuleRegistry,
) -> tuple[
    tuple[Mapping[str, Any], ...],
    dict[ReplayKey, Any],
]:
    """Resolve a canonical plan through exact trusted fixed-module identities."""
    requirements: list[Mapping[str, Any]] = []
    modules: dict[ReplayKey, Any] = {}
    for step in plan.executable_steps:
        key = step.replay_key
        if key is None or step.module_digest is None:
            raise ValueError("canonical plan module identity is incomplete")
        modules[key] = registry.resolve_exact(key.module_id, step.module_digest)
        requirements.append(
            {
                "logical_step": key.logical_step,
                "module_digest": step.module_digest,
                "module_id": key.module_id,
            }
        )
        if step.kind == "map_module":
            control = step.control or {}
            item_key = control.get("item_key")
            if (
                not isinstance(item_key, Mapping)
                or set(item_key) != {"field"}
                or not isinstance(item_key["field"], str)
                or not item_key["field"].strip()
            ):
                raise ValueError("serialized map plans require a field item-key contract")
    return tuple(sorted(requirements, key=canonical_json)), modules


def _fixed_aliases_by_digest(registry: ModuleRegistry) -> dict[str, str]:
    matches: dict[str, list[str]] = {}
    for description in registry.describe():
        if description.get("kind") != "fixed":
            continue
        digest = description.get("module_digest")
        alias = description.get("alias")
        if isinstance(digest, str) and isinstance(alias, str):
            matches.setdefault(digest, []).append(alias)
    return {digest: aliases[0] for digest, aliases in matches.items() if len(aliases) == 1}
