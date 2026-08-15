"""Evaluate replay cases under configurable verification policies.

Verification converts replay outcomes and fixture/contract errors into one
stable pass/fail result. Divergence and changed behavior are diagnostic by
default and can be promoted to blocking outcomes through
:class:`VerificationPolicy`.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any

from .authoring import Workflow
from .fixture import ReplayFixtureError
from .replay import (
    ReplayCase,
    ReplayContractError,
    ReplayEngine,
    ReplayResult,
    ReplaySelectorError,
    ReplayStatus,
)


class VerificationVerdict(StrEnum):
    """Aggregate pass or fail verdict for a verification suite."""

    PASS = "PASS"
    FAIL = "FAIL"


@dataclass(frozen=True)
class VerificationPolicy:
    """Rules that promote diagnostic replay outcomes to blocking failures.

    Attributes
    ----------
    replay_divergence_blocking
        Fail when historical and current graphs cannot be aligned exactly.
    behavior_change_blocking
        Fail when a selectively executed module changes output or trajectory.
    """

    replay_divergence_blocking: bool = False
    behavior_change_blocking: bool = False


@dataclass(frozen=True)
class VerificationSuite:
    """Collection of replay cases evaluated as one behavioral check.

    Attributes
    ----------
    replay_tests
        Ordered full-stub and selective replay cases to evaluate.
    """

    replay_tests: tuple[ReplayCase, ...] = ()


@dataclass(frozen=True)
class VerificationCaseResult:
    """Normalized result or contract error for one replay case."""

    result: ReplayResult | None
    error_code: str | None = None
    message: str = ""
    blocking: bool = False


@dataclass(frozen=True)
class VerificationResult:
    """Aggregate verdict with ordered per-case replay evidence."""

    verdict: VerificationVerdict
    replay_results: tuple[VerificationCaseResult, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


async def verify_workflow(
    workflow: Workflow[Any, Any],
    suite: VerificationSuite,
    *,
    policy: VerificationPolicy | None = None,
    engine: ReplayEngine | None = None,
) -> VerificationResult:
    """Run a verification suite against the current workflow.

    Parameters
    ----------
    workflow
        Current workflow definition and module implementations.
    suite
        Ordered replay cases to validate and compare.
    policy
        Optional rules for treating divergence or changed behavior as blocking.
    engine
        Optional configured replay engine, useful for custom tracing or tests.

    Returns
    -------
    VerificationResult
        Aggregate verdict and one normalized result per replay case.

    Notes
    -----
    Invalid fixtures and contract or selector errors always block. Graph
    divergence and changed behavior follow the supplied policy.
    """
    selected_policy = policy or VerificationPolicy()
    selected_engine = engine or ReplayEngine()
    results: list[VerificationCaseResult] = []
    for case in suite.replay_tests:
        try:
            replay_result = await selected_engine.replay(workflow, case)
        except ReplayFixtureError as exc:
            results.append(
                VerificationCaseResult(
                    None,
                    error_code=exc.code.value,
                    message=str(exc),
                    blocking=True,
                )
            )
            continue
        except (ReplayContractError, ReplaySelectorError) as exc:
            results.append(
                VerificationCaseResult(
                    None,
                    error_code="REPLAY_CONTRACT_INVALID",
                    message=str(exc),
                    blocking=True,
                )
            )
            continue
        blocking = replay_result.blocking
        if replay_result.status is ReplayStatus.REPLAY_DIVERGENCE:
            blocking = selected_policy.replay_divergence_blocking
        elif replay_result.status is ReplayStatus.CHANGED:
            blocking = selected_policy.behavior_change_blocking
        if blocking != replay_result.blocking:
            replay_result = replace(replay_result, blocking=blocking)
        results.append(
            VerificationCaseResult(
                replay_result,
                message=replay_result.message,
                blocking=blocking,
            )
        )
    verdict = (
        VerificationVerdict.FAIL
        if any(result.blocking for result in results)
        else VerificationVerdict.PASS
    )
    return VerificationResult(verdict, tuple(results))
