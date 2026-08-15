"""Declare immutable resource envelopes for workflow modules.

Budgets are behavior-bearing limits compiled into module definitions and sent
to workers with durable tasks. They describe the maximum consumption a runtime
may permit; they are not usage counters. Runtime and provider integrations are
responsible for metering actual work and enforcing the declared limits.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Self

_WIRE_FIELDS = frozenset({"cost_usd", "model_tokens", "tool_calls", "wall_time_ms"})


def _optional_count(name: str, value: int | None) -> None:
    if value is None:
        return
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer or None")
    if value < 0:
        raise ValueError(f"{name} must be nonnegative")


@dataclass(frozen=True)
class Budget:
    """Immutable resource limits declared for one module execution.

    ``None`` leaves a dimension unbounded. Zero is a valid limit and means the
    module may consume none of that resource. Wall time uses
    :class:`datetime.timedelta` in Python and exact integer milliseconds in
    canonical IR, persistence, and worker envelopes.

    Parameters
    ----------
    wall_time
        Maximum elapsed execution time. Values must be nonnegative and use
        millisecond precision.
    model_tokens
        Maximum combined model input and output tokens.
    tool_calls
        Maximum number of runtime-metered tool calls.
    cost_usd
        Maximum runtime-metered cost in US dollars. The value must be finite
        and nonnegative. Integer inputs must be exactly representable by the
        canonical floating-point wire value; lossy conversion is rejected.

    Notes
    -----
    A ``Budget`` is a declaration, not an enforcement loop or usage record.
    Executors, model clients, and tool brokers must meter consumption and stop
    work when a declared limit is reached. Persisted usage remains separate.

    Examples
    --------
    >>> from datetime import timedelta
    >>> budget = Budget(
    ...     wall_time=timedelta(minutes=2),
    ...     model_tokens=20_000,
    ...     tool_calls=10,
    ...     cost_usd=0.50,
    ... )
    >>> budget.to_data()["wall_time_ms"]
    120000
    """

    wall_time: timedelta | None = None
    model_tokens: int | None = None
    tool_calls: int | None = None
    cost_usd: float | None = None

    def __post_init__(self) -> None:
        if self.wall_time is not None:
            if not isinstance(self.wall_time, timedelta):
                raise TypeError("wall_time must be a timedelta or None")
            total_microseconds = (
                self.wall_time.days * 86_400 + self.wall_time.seconds
            ) * 1_000_000 + self.wall_time.microseconds
            if total_microseconds < 0:
                raise ValueError("wall_time must be nonnegative")
            if total_microseconds % 1_000:
                raise ValueError("wall_time must use millisecond precision")
        _optional_count("model_tokens", self.model_tokens)
        _optional_count("tool_calls", self.tool_calls)
        if self.cost_usd is not None:
            if isinstance(self.cost_usd, bool) or not isinstance(self.cost_usd, (int, float)):
                raise TypeError("cost_usd must be a number or None")
            try:
                normalized = float(self.cost_usd)
            except OverflowError:
                raise ValueError("cost_usd must be finite") from None
            if not math.isfinite(normalized):
                raise ValueError("cost_usd must be finite")
            if isinstance(self.cost_usd, int) and normalized != self.cost_usd:
                raise ValueError("integer cost_usd must be exactly representable as a float")
            if normalized < 0:
                raise ValueError("cost_usd must be nonnegative")
            object.__setattr__(self, "cost_usd", 0.0 if normalized == 0 else normalized)

    def to_data(self) -> dict[str, int | float | None]:
        """Return the canonical declaration used by IR and task envelopes.

        Returns
        -------
        dict
            A JSON-compatible mapping with exact ``wall_time_ms`` and the
            remaining declared limits. The mapping never contains measured
            usage.
        """
        wall_time_ms = None
        if self.wall_time is not None:
            wall_time_ms = (
                self.wall_time.days * 86_400 + self.wall_time.seconds
            ) * 1_000 + self.wall_time.microseconds // 1_000
        return {
            "cost_usd": self.cost_usd,
            "model_tokens": self.model_tokens,
            "tool_calls": self.tool_calls,
            "wall_time_ms": wall_time_ms,
        }

    @classmethod
    def from_data(cls, data: Mapping[str, Any]) -> Self:
        """Validate and restore a budget from canonical wire data.

        Parameters
        ----------
        data
            Mapping containing exactly ``wall_time_ms``, ``model_tokens``,
            ``tool_calls``, and ``cost_usd``.

        Returns
        -------
        Budget
            Immutable declaration equivalent to the canonical mapping.

        Raises
        ------
        TypeError
            If a value has an ambiguous or unsupported type.
        ValueError
            If fields are missing or unknown, or a limit is negative,
            non-finite, or outside ``timedelta`` range.
        """
        if set(data) != _WIRE_FIELDS:
            raise ValueError("budget fields must exactly match the canonical wire contract")
        wall_time_ms = data["wall_time_ms"]
        if wall_time_ms is not None:
            if type(wall_time_ms) is not int:
                raise TypeError("wall_time_ms must be an integer or None")
            if wall_time_ms < 0:
                raise ValueError("wall_time_ms must be nonnegative")
        try:
            wall_time = timedelta(milliseconds=wall_time_ms) if wall_time_ms is not None else None
        except OverflowError as exc:
            raise ValueError("wall_time_ms is outside the supported timedelta range") from exc
        return cls(
            wall_time=wall_time,
            model_tokens=data["model_tokens"],
            tool_calls=data["tool_calls"],
            cost_usd=data["cost_usd"],
        )
