"""Build, run, and replay typed Maida workflows.

The package-level namespace contains the stable authoring and execution API.
Start with :class:`Module` and :class:`Workflow`, compile definitions with
:func:`compile_workflow`, and use :class:`WorkflowRunner` for local durable
execution.

Examples
--------
>>> from maida.workflows import ExecutionContext, Module, RuntimeValue, Workflow
>>> class Upper(Module[str, str]):
...     input_type = str
...     output_type = str
...     async def execute(self, value: str, ctx: ExecutionContext) -> str:
...         return value.upper()
>>> class UpperWorkflow(Workflow[str, str]):
...     workflow_id = "upper"
...     input_type = str
...     output_type = str
...     upper = Upper()
...     def build(self, value: RuntimeValue[str]) -> RuntimeValue[str]:
...         return self.upper(value)
"""

from .authoring import (
    BoundModuleCall,
    ExecutionContext,
    Module,
    RuntimeValue,
    SymbolicValueError,
    Workflow,
    map_over,
    parallel,
    when,
)
from .fixture import ReplayFixture, ReplayFixtureError, ReplayFixtureImporter
from .ir import CompileError, PlanIR, ReplayKey, StepIR, compile_workflow
from .models import (
    BoundaryRecord,
    ExecutionMode,
    RunHistory,
    RunStatus,
    StoredValue,
)
from .replay import (
    ReplayCase,
    ReplayDivergence,
    ReplayMode,
    ReplayResult,
)
from .runtime import RunResult, TaskWorker, WorkflowRunner
from .verification import VerificationSuite

__all__ = [
    "BoundModuleCall",
    "BoundaryRecord",
    "CompileError",
    "ExecutionContext",
    "ExecutionMode",
    "Module",
    "PlanIR",
    "ReplayCase",
    "ReplayDivergence",
    "ReplayFixture",
    "ReplayFixtureError",
    "ReplayFixtureImporter",
    "ReplayKey",
    "ReplayMode",
    "ReplayResult",
    "RunHistory",
    "RunResult",
    "RunStatus",
    "RuntimeValue",
    "StepIR",
    "StoredValue",
    "SymbolicValueError",
    "TaskWorker",
    "VerificationSuite",
    "Workflow",
    "WorkflowRunner",
    "compile_workflow",
    "map_over",
    "parallel",
    "when",
]
