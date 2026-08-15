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
    PASS = "PASS"
    FAIL = "FAIL"


@dataclass(frozen=True)
class VerificationPolicy:
    replay_divergence_blocking: bool = False
    behavior_change_blocking: bool = False


@dataclass(frozen=True)
class VerificationSuite:
    replay_tests: tuple[ReplayCase, ...] = ()


@dataclass(frozen=True)
class VerificationCaseResult:
    result: ReplayResult | None
    error_code: str | None = None
    message: str = ""
    blocking: bool = False


@dataclass(frozen=True)
class VerificationResult:
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
