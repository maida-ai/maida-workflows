# Composio

**Status: no adapter ships.** Maida Workflows does not vendor provider
integrations, credentials, or auth. What it gives you is the contract to put
*around* your existing Composio client so its reads and side effects become
grant-checked, budgeted, idempotent and provable.

This page is a recipe you complete, not a package you install. The shapes below
are the real `ConnectorAdapter` and `EffectAdapter` protocols; the Composio call
inside them is yours and is **not** exercised by this repository's tests.

## Why there is no adapter

Providers, connectors and credentials are outside this package's boundary on
purpose. Shipping a Composio adapter would mean shipping auth handling, session
management and version-tracking for someone else's API — and then doing it again
for the next provider. Your Composio client already works. Maida's job starts at
the point where a plan wants to *use* it.

The practical upside: nothing about your Composio setup has to change.

## The two categories

Maida distinguishes reading from causing, and the distinction is load-bearing:

- a **Connector** reads from an external system under a capability grant;
- an **Effect** causes something outside and can require an idempotency key.

Most Composio tools are one or the other. Sort them before you start — a tool
that sends, posts, creates or pays is an Effect, not a Connector.

## 1. Declare the capability and effect

These are Maida-side declarations. They name a grant, a connector, an operation,
and the request/response types.

```python
from maida.workflows import Capability, EffectSpec, Idempotency

GITHUB_ISSUES = Capability(
    "github.issues.read",   # grant name, referenced by PlanBoundary
    "composio-github",      # connector registry identity
    "list_issues",          # operation name
    str, str,               # request / response types
    connector_version="1",
)

SLACK_POST = EffectSpec(
    "slack.message.send",
    "composio-slack",
    "post_message",
    str, str,
    connector_version="1",
    idempotency=Idempotency.REQUIRED,
)
```

## 2. Wrap your Composio client

An adapter holds your client and credentials privately. Adapter instances are
never serialized into plans, task envelopes or audit records.

```python
from typing import Any

class ComposioReadAdapter:
    """Read-only Composio operations behind the Maida connector contract."""

    def __init__(self, client: Any) -> None:
        self._client = client   # your configured Composio client

    @property
    def connector(self) -> str:
        return "composio-github"

    @property
    def connector_version(self) -> str | None:
        return "1"

    @property
    def operations(self) -> frozenset[str]:
        return frozenset({"list_issues"})

    async def read(self, operation: str, request: Any) -> Any:
        # dispatch to Composio; return a provider-neutral value
        return await self._client.execute(operation, request)
```

For effects, the adapter receives a stable idempotency key and **must** forward
it to a destination that honors it:

```python
class ComposioEffectAdapter:
    """Consequential Composio operations with idempotency forwarding."""

    def __init__(self, client: Any) -> None:
        self._client = client

    @property
    def connector(self) -> str:
        return "composio-slack"

    @property
    def connector_version(self) -> str | None:
        return "1"

    @property
    def effect_operations(self) -> frozenset[str]:
        return frozenset({"post_message"})

    @property
    def idempotent_effects(self) -> frozenset[str]:
        # only list operations whose destination actually honors the key
        return frozenset({"post_message"})

    async def effect(
        self,
        operation: str,
        request: Any,
        *,
        idempotency_key: str,
    ) -> Any:
        return await self._client.execute(
            operation, request, idempotency_key=idempotency_key
        )
```

Declaring an operation in `idempotent_effects` is a promise. If Composio's
destination does not deduplicate on the key you forward, leave it out — an
honest non-idempotent effect is safer than a false claim, because retries and
redeliveries are decided by your substrate.

## 3. Expose them as modules

```python
from maida.workflows import Connector, Effect, ModuleRegistry

def issues_module():
    module = Connector(GITHUB_ISSUES)
    module.budget = TOOL_BUDGET
    return module

def slack_module():
    module = Effect(SLACK_POST)
    module.budget = TOOL_BUDGET
    return module

registry = ModuleRegistry(modules={
    "github.issues": issues_module,
    "slack.post": slack_module,
    "text.summarize": Summarize,
})
```

## 4. Grant them on the boundary

The boundary sets the ceiling. A generated plan can select these aliases; it can
never name a grant, connector, credential or effect it was not given.

```python
from maida.workflows import CapabilityGrant, PlanBoundary

boundary = PlanBoundary(
    registry,
    limits,
    region_id="triage-plan",
    output_type=str,
    region_grant=CapabilityGrant(
        capabilities=(GITHUB_ISSUES.name,),
        effects=(SLACK_POST.name,),
    ),
)
```

## What you get for the wrapping

Once Composio tools sit behind these contracts:

- **Grant enforcement.** A plan requesting a capability outside its grant is
  refused before execution, as a typed denial rather than a runtime surprise.
- **Budgets.** Tool calls, cost and wall time are declared per occurrence and
  durably reserved.
- **Idempotency.** Effects carry a stable key derived from the task and step.
- **Policy visibility.** `effectful_modules` and `required_grant` appear in the
  plan signature, so "this plan gained a Slack send it never had" is a
  detectable structural change, not something you find in production.
- **Proof.** Every touch is recorded at a typed boundary and checked against the
  approved plan after execution.

## Caveats

- The Composio calls above are illustrative. Adapt them to your client's actual
  API; the protocol shapes are what matter.
- No part of this path is covered by this repository's test suite. Test your
  adapter against your own Composio account.
- Credentials stay in your adapter. Do not put them in a module, a plan, a
  registry entry, or anything a planner can see.

## Related

- [Connectors and effects](../guides/connectors-and-effects.md) — the general form
- [Execution substrates](../substrates.md) — where the work actually runs
