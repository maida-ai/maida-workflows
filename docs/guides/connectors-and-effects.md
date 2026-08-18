# Connectors and effects

This is what "tool use" is called here. The distinction between reading and
causing is load-bearing, not cosmetic.

- a **Connector** reads from an external system under a capability grant;
- an **Effect** causes something outside and can require idempotency.

Both go through the access broker, so every touch is grant-checked and recorded.
That is what makes "this plan reads a data surface it never read before" a
detectable structural change.

## Declare what may be reached

```python
from maida.workflows import Capability, EffectSpec, Idempotency

CONTEXT = Capability(
    "records.context.read",   # grant name
    "demo-records",           # connector identity
    "context",                # operation
    str, str,                 # request / response types
    connector_version="1",
)

DELIVER = EffectSpec(
    "messages.deliver",
    "demo-records",
    "deliver",
    str, str,
    connector_version="1",
    idempotency=Idempotency.REQUIRED,
)
```

## Expose them as modules

```python
from maida.workflows import Connector, Effect

def context_module():
    module = Connector(CONTEXT)
    module.budget = TOOL_BUDGET
    return module

def deliver_module():
    module = Effect(DELIVER)
    module.budget = TOOL_BUDGET
    return module

registry = ModuleRegistry(modules={
    "records.context": context_module,
    "messages.deliver": deliver_module,
    "text.normalize": Normalize,
})
```

## Grant them on the boundary

```python
from maida.workflows import CapabilityGrant

boundary = PlanBoundary(
    registry, limits,
    region_id="request-plan",
    output_type=str,
    region_grant=CapabilityGrant(
        capabilities=(CONTEXT.name,),
        effects=(DELIVER.name,),
    ),
)
```

The grant is a ceiling. A plan requesting anything outside it is refused before
execution, as a typed denial.

## Supplying the implementation

Maida ships no provider adapter, no auth and no session handling. You implement
`ConnectorAdapter` for reads and `EffectAdapter` for effects, holding your own
client and credentials privately. Adapter instances are never serialized into
plans, task envelopes or audit records.

A read adapter needs `connector`, `connector_version`, `operations`, and
`async read(operation, request)`.

An effect adapter needs `connector`, `connector_version`, `effect_operations`,
`idempotent_effects`, and `async effect(operation, request, *, idempotency_key)`.

Listing an operation in `idempotent_effects` is a promise that the destination
deduplicates on the key you forward. If it does not, leave it out.

See [Composio](../integrations/composio.md) for a worked recipe against a real
provider client.

## What policy sees

Connectors and effects surface in the plan signature as `required_grant` and
`effectful_modules`, so a plan that gains a new side effect is visible to policy
before it runs — not discovered in production.
