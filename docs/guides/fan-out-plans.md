# Fan-out plans

What you might call a multi-step or multi-agent flow. Here it is one plan with
several nodes, checked as a whole.

## The wire format

A planner emits topology; the dependency graph does the composition.

```json
{"fragment_id": "thorough-plan", "nodes": [
  {"key": "normalize", "module_alias": "text.normalize",  "dependencies": ["$input"]},
  {"key": "context",   "module_alias": "records.context", "dependencies": ["$input"]},
  {"key": "draft",     "module_alias": "text.draft",      "dependencies": ["normalize"]},
  {"key": "audit",     "module_alias": "text.audit",      "dependencies": ["draft", "context"]},
  {"key": "deliver",   "module_alias": "messages.deliver","dependencies": ["audit"]}
], "outputs": ["deliver"]}
```

`$input` is the region input. Two nodes depend on it, so they fan out; `audit`
joins them; `deliver` crosses an effect boundary. Five nodes, depth 4, fan-out 2.

## What policy sees

Resolution produces a signature carrying the facts policy reasons about:

| Field | Used for |
| --- | --- |
| `max_depth`, `max_fanout`, `node_count` | Structural limits |
| `module_composition` | Which modules, how many times, at what digest |
| `effectful_modules` | "This plan gained a send it never had" |
| `required_grant` | Capabilities and effects the plan needs |
| `aggregate_budget` | Cost, tokens, tool calls, wall time for the whole plan |
| `topology_digest` | Shape comparison across runs |
| `approval_requirements` | Effects requiring human approval |

A policy can cap any of the numeric fields and constrain the set-valued ones:

```yaml
version: 2.1
metrics:
  plan_fanout:
    kind: measured
    direction: upper
    limit: 2
  plan_effectful_modules:
    kind: invariant
    none_of: ["billing.charge"]
```

## Structural comparison

Two plans diff into readable changes rather than field-level noise:

```text
PLAN APPROVED WITH CHANGES:
- New effectful module: demo.send (1 occurrence).
- Budget cost USD grew 4x: 1 -> 4.
- Required effects changed: [] -> ['messages.send'].
- Plan depth increased: 1 -> 2.
- Dependency topology changed.
```

## Recurring plans

The same task planned repeatedly is the ideal population for drift detection.
With a baseline bound, `plan_shape_seen` refuses a topology that has never been
accepted for that plan id — and omitting the rule means novelty alone is not a
refusal.

## Generated plans are plain DAGs

Static authoring supports `when` and `map_over` control regions. Generated data
cannot express them, deliberately: hiding a twelve-way fan-out inside one node
would make it invisible to `max_fanout`. Same representation, different
permitted subset by provenance. An attempt to inject a control region is refused
with `PLAN_FRAGMENT_INVALID`.
