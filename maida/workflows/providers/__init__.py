"""Optional deployment adapters for third-party integration providers.

Provider packages translate external SDK clients into Maida's stable connector,
effect, trigger, and interoperability contracts. They do not add provider state
to workflow IR or the durable runtime and remain optional at installation time.
"""

from .composio import (
    ComposioSession,
    ComposioToolAdapter,
    ComposioToolBinding,
    ComposioTriggerEvent,
)

__all__ = [
    "ComposioSession",
    "ComposioToolAdapter",
    "ComposioToolBinding",
    "ComposioTriggerEvent",
]
