"""Author durable approval, input, and signal boundaries in workflow graphs.

Interaction modules park a logical task through the durable worker protocol.
No worker, process, or VM waits while a command is outstanding. A later worker
reclaims the same task, validates the accepted command, and commits one normal
typed module boundary for replay and verification.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, cast

from ._canonical import (
    _rehydrate_value,
    canonical_data,
    digest_data,
    schema_digest,
    type_schema,
    value_matches_type,
)
from .authoring import ExecutionContext, Module


@dataclass(frozen=True)
class ApprovalDecision:
    """Typed result of one durable approval request.

    Parameters
    ----------
    approved
        ``True`` for an accepted request and ``False`` for a rejection.
    comment
        Optional approver comment supplied with acceptance.
    reason
        Optional rejection reason.
    command_id
        Durable command idempotency key that produced this decision.

    Notes
    -----
    Rejection is data, not a workflow failure. Authors can branch explicitly on
    :attr:`approved`, which keeps policy visible in the graph.
    """

    approved: bool
    comment: str | None = None
    reason: str | None = None
    command_id: str | None = None


class _InteractionModule[InputT, OutputT](Module[InputT, OutputT]):
    interaction_kind: str
    prompt: str
    metadata: Mapping[str, Any]

    def _request_data(
        self,
        *,
        run_id: str,
        task_id: str,
        step_instance_id: str,
    ) -> dict[str, Any]:
        request_id = (
            "interaction-"
            + digest_data(
                {
                    "run_id": run_id,
                    "task_id": task_id,
                    "step_instance_id": step_instance_id,
                    "kind": self.interaction_kind,
                }
            )[:32]
        )
        payload: dict[str, Any] = {
            "request_id": request_id,
            "kind": self.interaction_kind,
            "prompt": self.prompt,
            "schema_digest": schema_digest(self.output_type),
            "schema": type_schema(self.output_type),
            "metadata": canonical_data(self.metadata),
        }
        signal_name = getattr(self, "signal_name", None)
        if signal_name is not None:
            payload["signal_name"] = signal_name
        return payload

    async def execute(self, value: InputT, ctx: ExecutionContext) -> OutputT:
        """Reject direct execution because workers own durable parking."""
        raise RuntimeError("interaction modules require the durable TaskWorker protocol")

    def _resolve_data(self, resolution: Mapping[str, Any]) -> OutputT:
        raise NotImplementedError


class Approval[InputT](_InteractionModule[InputT, ApprovalDecision]):
    """Pause a logical task until a specific request is approved or rejected.

    Parameters
    ----------
    input_type
        Type of the upstream value that causes this approval occurrence.
    prompt
        Human-readable question shown by the application.
    metadata
        Optional canonical, non-sensitive presentation data.
    Returns
    -------
    ApprovalDecision
        Durable typed decision that can feed a normal :func:`when` branch.

    Notes
    -----
    The upstream value is not exposed in the request payload. Applications can
    present separately authorized run data while the workflow records only
    prompt metadata and command provenance.

    Examples
    --------
    >>> approval = Approval(Change, prompt="Deploy this change?")  # doctest: +SKIP
    """

    interaction_kind = "approval"
    module_id = "maida.interaction.approval"
    output_type = ApprovalDecision

    def __init__(
        self,
        input_type: type[InputT],
        *,
        prompt: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        if not prompt.strip():
            raise ValueError("approval prompt must be non-empty")
        self.input_type = input_type
        self.prompt = prompt
        self.metadata = MappingProxyType(cast(dict[str, Any], canonical_data(dict(metadata or {}))))

    def _resolve_data(self, resolution: Mapping[str, Any]) -> ApprovalDecision:
        decision = resolution.get("decision")
        if decision not in {"approve", "reject"}:
            raise ValueError("approval resolution has no valid decision")
        return ApprovalDecision(
            approved=decision == "approve",
            comment=cast(str | None, resolution.get("comment")),
            reason=cast(str | None, resolution.get("reason")),
            command_id=cast(str | None, resolution.get("command_id")),
        )


class Input[InputT, ResponseT](_InteractionModule[InputT, ResponseT]):
    """Pause a logical task until schema-valid typed input is supplied.

    Parameters
    ----------
    input_type
        Type of the upstream value that creates this interaction occurrence.
    response_type
        Required command payload type and module output contract.
    prompt
        Human-readable request shown by the application.
    metadata
        Optional canonical, non-sensitive presentation data.
    Notes
    -----
    Payload schema is validated before a command makes the task ready and is
    validated again by the worker before accepting the boundary.
    """

    interaction_kind = "input"
    module_id = "maida.interaction.input"

    def __init__(
        self,
        input_type: type[InputT],
        response_type: type[ResponseT],
        *,
        prompt: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        if not prompt.strip():
            raise ValueError("input prompt must be non-empty")
        self.input_type = input_type
        self.output_type = response_type
        self.prompt = prompt
        self.metadata = MappingProxyType(cast(dict[str, Any], canonical_data(dict(metadata or {}))))

    def _resolve_data(self, resolution: Mapping[str, Any]) -> ResponseT:
        value = _rehydrate_value(resolution.get("value"), self.output_type)
        if not value_matches_type(value, self.output_type):
            raise ValueError("input resolution violates its output contract")
        return cast(ResponseT, value)


class WaitForSignal[InputT, PayloadT](_InteractionModule[InputT, PayloadT]):
    """Pause until a named, schema-valid external signal is delivered.

    Parameters
    ----------
    input_type
        Type of the upstream value that creates this wait occurrence.
    payload_type
        Required signal payload type and module output contract.
    name
        Stable external signal name, such as ``payment.settled``.
    prompt
        Optional application message. A deterministic message is derived from
        ``name`` when omitted.
    metadata
        Optional canonical, non-sensitive presentation data.
    Notes
    -----
    Provider webhook state remains outside the workflow runtime. Integrations
    translate verified events into idempotent ``SignalCommand`` values.
    """

    interaction_kind = "signal"
    module_id = "maida.interaction.signal"

    def __init__(
        self,
        input_type: type[InputT],
        payload_type: type[PayloadT],
        *,
        name: str,
        prompt: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        if not name.strip():
            raise ValueError("signal name must be non-empty")
        self.input_type = input_type
        self.output_type = payload_type
        self.signal_name = name
        self.prompt = prompt or f"Wait for signal {name}"
        self.metadata = MappingProxyType(cast(dict[str, Any], canonical_data(dict(metadata or {}))))

    def _resolve_data(self, resolution: Mapping[str, Any]) -> PayloadT:
        if resolution.get("name") != self.signal_name:
            raise ValueError("signal resolution name does not match the request")
        value = _rehydrate_value(resolution.get("value"), self.output_type)
        if not value_matches_type(value, self.output_type):
            raise ValueError("signal resolution violates its output contract")
        return cast(PayloadT, value)
