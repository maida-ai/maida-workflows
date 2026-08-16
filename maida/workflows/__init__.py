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

from .access import (
    AccessBroker,
    AccessContractError,
    AccessPolicy,
    ApprovalEvidence,
    Capability,
    CapabilityGrant,
    Connector,
    ConnectorAdapter,
    ConnectorRegistry,
    Effect,
    EffectAdapter,
    EffectSpec,
    Idempotency,
    PolicyDecision,
)
from .asgi import UserplaneASGI, create_userplane_app
from .authoring import (
    BoundModuleCall,
    ExecutionContext,
    Module,
    RuntimeValue,
    SymbolicValueError,
    Workflow,
    literal,
    map_over,
    parallel,
    when,
)
from .budget import Budget, BudgetExceededError, BudgetUsage
from .bundle import (
    WorkflowBundle,
    WorkflowBundleError,
    WorkflowPortability,
)
from .coordination import CoordinatorProgress, WorkflowCatalog, WorkflowCoordinator
from .definitions import BoundWorkflow, bind_workflow
from .dynamic import (
    PlanBoundary,
    PlanFragmentIR,
    PlanLimits,
    PlanNode,
    PlanSignature,
    PlanValidationError,
    PlanValidator,
)
from .fixture import GeneratedPlanRecord, ReplayFixture, ReplayFixtureError
from .interactions import Approval, ApprovalDecision, Input, WaitForSignal
from .interop import ExternalWorkflow
from .ir import (
    BindingIR,
    CompileError,
    PlanIR,
    ReplayKey,
    StepIR,
    compile_workflow,
    module_digest,
)
from .model import (
    ModelAdapter,
    ModelAdapterRegistry,
    ModelBroker,
    ModelCallResult,
    ModelSpec,
)
from .models import (
    BoundaryRecord,
    ExecutionMode,
    ExecutionSpec,
    ExecutorCapabilities,
    PlanTaskProvenance,
    RunHistory,
    RunStatus,
    StoredValue,
    TaskStatus,
)
from .registry import ModuleRegistry, ModuleTemplate
from .replay import (
    ReplayCase,
    ReplayDivergence,
    ReplayMode,
    ReplayResult,
)
from .runtime import (
    Executor,
    LocalExecutor,
    RunResult,
    ScheduleProgress,
    TaskEnvelope,
    TaskWorker,
    WorkflowRunner,
    WorkflowScheduler,
)
from .spec import (
    BindingSpec,
    NodeSpec,
    ValidationIssue,
    WorkflowCompilation,
    WorkflowExplanation,
    WorkflowSpec,
    WorkflowSpecError,
    compile_workflow_spec,
)
from .userplane import (
    ApproveCommand,
    CancelCommand,
    CommandReceipt,
    CommandType,
    EventPage,
    InputCommand,
    PauseCommand,
    RejectCommand,
    ResumeCommand,
    RetryCommand,
    RunCommand,
    RunEvent,
    RunSnapshot,
    SignalCommand,
    WorkflowClient,
    WorkflowRun,
    parse_command,
)
from .verification import VerificationSuite

__all__ = [
    "AccessBroker",
    "AccessContractError",
    "AccessPolicy",
    "Approval",
    "ApprovalDecision",
    "ApprovalEvidence",
    "ApproveCommand",
    "BindingIR",
    "BindingSpec",
    "BoundModuleCall",
    "BoundWorkflow",
    "BoundaryRecord",
    "Budget",
    "BudgetExceededError",
    "BudgetUsage",
    "CancelCommand",
    "Capability",
    "CapabilityGrant",
    "CommandReceipt",
    "CommandType",
    "CompileError",
    "Connector",
    "ConnectorAdapter",
    "ConnectorRegistry",
    "CoordinatorProgress",
    "Effect",
    "EffectAdapter",
    "EffectSpec",
    "EventPage",
    "ExecutionContext",
    "ExecutionMode",
    "ExecutionSpec",
    "Executor",
    "ExecutorCapabilities",
    "ExternalWorkflow",
    "GeneratedPlanRecord",
    "Idempotency",
    "Input",
    "InputCommand",
    "LocalExecutor",
    "ModelAdapter",
    "ModelAdapterRegistry",
    "ModelBroker",
    "ModelCallResult",
    "ModelSpec",
    "Module",
    "ModuleRegistry",
    "ModuleTemplate",
    "NodeSpec",
    "PauseCommand",
    "PlanBoundary",
    "PlanFragmentIR",
    "PlanIR",
    "PlanLimits",
    "PlanNode",
    "PlanSignature",
    "PlanTaskProvenance",
    "PlanValidationError",
    "PlanValidator",
    "PolicyDecision",
    "RejectCommand",
    "ReplayCase",
    "ReplayDivergence",
    "ReplayFixture",
    "ReplayFixtureError",
    "ReplayKey",
    "ReplayMode",
    "ReplayResult",
    "ResumeCommand",
    "RetryCommand",
    "RunCommand",
    "RunEvent",
    "RunHistory",
    "RunResult",
    "RunSnapshot",
    "RunStatus",
    "RuntimeValue",
    "ScheduleProgress",
    "SignalCommand",
    "StepIR",
    "StoredValue",
    "SymbolicValueError",
    "TaskEnvelope",
    "TaskStatus",
    "TaskWorker",
    "UserplaneASGI",
    "ValidationIssue",
    "VerificationSuite",
    "WaitForSignal",
    "Workflow",
    "WorkflowBundle",
    "WorkflowBundleError",
    "WorkflowCatalog",
    "WorkflowClient",
    "WorkflowCompilation",
    "WorkflowCoordinator",
    "WorkflowExplanation",
    "WorkflowPortability",
    "WorkflowRun",
    "WorkflowRunner",
    "WorkflowScheduler",
    "WorkflowSpec",
    "WorkflowSpecError",
    "bind_workflow",
    "compile_workflow",
    "compile_workflow_spec",
    "create_userplane_app",
    "literal",
    "map_over",
    "module_digest",
    "parallel",
    "parse_command",
    "when",
]
