"""Represent an externally executed workflow at Maida's typed trust boundary.

Provider sessions, credentials, and invocation adapters remain deployment
concerns. This module stores only stable identity, typed ports, and declared
access so external work can participate in validation and replay boundaries.
"""

from __future__ import annotations

import re
from typing import Any, cast

from .access import Capability, EffectSpec, Idempotency
from .authoring import ExecutionContext, Module

_IDENTITY = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")


class ExternalWorkflow[InputT, OutputT](Module[InputT, OutputT]):
    """Represent a whole external flow as one typed distributed module.

    Parameters
    ----------
    module_id
        Stable semantic identity used for diff and replay alignment.
    workflow
        Provider-owned stable flow name. It becomes the broker operation, not
        an import path or runtime state noun.
    provider
        Deployment connector registry key.
    input_type, output_type
        Contracts validated immediately before and after provider invocation.
    provider_version
        Optional immutable adapter/configuration pin. Credentials must never be
        placed here.
    effectful
        ``False`` for a genuinely read-only flow; ``True`` for any flow that
        can change external state.
    idempotency
        Destination guarantee required from a consequential provider adapter.
    approval_required, policy_tags
        Existing Maida effect-policy declarations applied before invocation.

    Notes
    -----
    The module records only provider-neutral identity and contracts. It cannot
    access a provider directly: live execution must supply the ordinary Maida
    broker, and replay stubs or denies that boundary before provider code runs.
    """

    def __init__(
        self,
        *,
        module_id: str,
        workflow: str,
        provider: str,
        input_type: type[InputT],
        output_type: type[OutputT],
        provider_version: str | None = None,
        effectful: bool,
        idempotency: Idempotency = Idempotency.REQUIRED,
        approval_required: bool = False,
        policy_tags: tuple[str, ...] = (),
    ) -> None:
        _stable("module_id", module_id)
        _stable("workflow", workflow)
        _stable("provider", provider)
        if provider_version is not None and not provider_version.strip():
            raise ValueError("provider_version must be non-empty when supplied")
        if not isinstance(effectful, bool):
            raise TypeError("effectful must be a boolean")
        if not isinstance(idempotency, Idempotency):
            raise TypeError("idempotency must be an Idempotency value")
        self.module_id = module_id
        self.workflow = workflow
        self.provider = provider
        self.provider_version = provider_version
        self.input_type = input_type
        self.output_type = output_type
        self.effectful = effectful
        self.idempotency = idempotency
        if effectful:
            self.capabilities = ()
            self.effects = (
                EffectSpec(
                    name=f"external.{workflow}.invoke",
                    connector=provider,
                    operation=workflow,
                    input_type=input_type,
                    output_type=output_type,
                    connector_version=provider_version,
                    idempotency=idempotency,
                    approval_required=approval_required,
                    policy_tags=policy_tags,
                ),
            )
        else:
            self.capabilities = (
                Capability(
                    name=f"external.{workflow}.read",
                    connector=provider,
                    operation=workflow,
                    input_type=input_type,
                    output_type=output_type,
                    connector_version=provider_version,
                    policy_tags=policy_tags,
                ),
            )
            self.effects = ()

    async def execute(self, value: InputT, ctx: ExecutionContext) -> OutputT:
        """Invoke the external flow through the runtime-managed broker.

        Raises
        ------
        RuntimeError
            If code attempts to execute outside a configured Maida worker.
        AccessContractError
            If declarations, grants, policy, provider identity, typed values,
            approval, or idempotency requirements fail.
        """
        if ctx.broker is None:
            raise RuntimeError("ExternalWorkflow requires a runtime access broker")
        if self.effectful:
            return cast(
                OutputT,
                await ctx.broker.effect(
                    self.provider,
                    self.workflow,
                    value,
                    connector_version=self.provider_version,
                ),
            )
        return cast(
            OutputT,
            await ctx.broker.read(
                self.provider,
                self.workflow,
                value,
                connector_version=self.provider_version,
            ),
        )


def _stable(label: str, value: Any) -> str:
    if not isinstance(value, str) or _IDENTITY.fullmatch(value) is None:
        raise ValueError(f"{label} must be a stable identifier")
    return value
