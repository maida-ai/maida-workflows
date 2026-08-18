# Maida Workflows -- coding-agent instructions

This file governs the `maida-workflows` repository. It inherits the Maida.AI
organization instructions in `../AGENTS.md`; read that file first. If it is not
available, say so instead of treating this file as the complete rulebook.

The inherited rules most likely to matter here are:

- Keep local workflows local: no account, license key, telemetry, trace upload,
  credential, or outbound runtime call. Examples are deterministic and offline.
- Use `uv` for dependency management and commands. Version comes from Git tags;
  do not edit a version string by hand.
- Work on a feature branch. Make atomic commits that include behavior, tests,
  and affected docs. Do not push, release, or publish unless asked.
- Prefer the standard library or an existing dependency. Justify every new
  dependency and ask before adding a production dependency unless the task
  explicitly requires it.
- A change is not done until relevant tests actually ran, coverage was
  assessed, behavior-changing docs were updated, and the final handoff lists
  the exact commands and any unverified checks.

This file ships in a public repository. Keep it about engineering and product
behavior; do not add internal strategy or business framing.

## Purpose and product boundary

Maida is the product surface. This package is its optional backend for
runtime-generated plans: it resolves minimal planner output against trusted
application contracts, refuses policy-breaking plans before generated work is
inserted, and records proof of what ran afterwards.

Optimize for these outcomes, in order:

1. A generated plan is resolved and checked before execution, with a stable,
   actionable refusal when it violates policy.
2. An accepted run retains enough typed boundary evidence to prove that the
   approved plan is what executed.
3. The same plan identity, dependency contract, idempotency contract, and proof
   survive when another runtime executes the work.
4. Plans remain canonical, addressable data that can be diffed, reviewed,
   replayed, and compared across runs.

### What belongs here

- Generated-plan decoding, trusted validation, identity, canonical `PlanIR`,
  structural diff, replay, and pre/post-execution proof.
- Guarantees that must survive distribution: occurrence identity, idempotency,
  dependency ordering, typed boundaries, grants, budgets, and effect evidence.
- Small adapters that carry those guarantees onto an execution substrate a
  user already owns.
- The minimal local runner needed for deterministic development and tests.

### What does not belong here

- Provider or connector products, credentials, authentication, or provider
  sessions.
- Prompts, memory systems, tool loops, built-in agent roles, or domain-specific
  module behavior.
- A hosted runtime, scheduler, control plane, worker fleet, queue, autoscaler,
  retry product, or general distributed execution engine.
- Dashboards, deployment infrastructure, or execution-target configuration.

Use this boundary test before adding a feature: does it exist so a plan can be
verified, or so work can be run? Verification is ours. Scheduling, delivery,
leases, retry timing, routing, and compute placement belong to the selected
runtime. Adapt to Temporal, Celery, LangGraph, Prefect, Airflow, or another
substrate; do not rebuild it here.

The bundled `LocalExecutor` and `TaskWorker` are reference fixtures. They may
demonstrate the guarantees but must not acquire competitive runtime features.
An external adapter must use the substrate-neutral `ExecutionRequest` and
`BoundaryHarness` seam; it must not enter the local claim, heartbeat,
capability-matching, or retry lifecycle. The current external harness requires
direct access to the authoritative Maida store. Do not hide that deployment
constraint or add a speculative evidence-sink abstraction without a second
real adapter or user need.

## Law 1: generated data chooses logic, never authority or execution

A planner may emit only the exact graph-choice mapping accepted by
`maida.workflows.dynamic._decode_generated_plan`: fragment identity, ordered
node keys, allowlisted module aliases, dependencies, and outputs.

Generated data must never be able to express:

- code, imports, module paths, or anything evaluated;
- credentials, secrets, connection details, or provider sessions;
- capability grants, scopes, permissions, or approvals;
- budgets, limits, or resource envelopes;
- execution targets, queues, providers, schedules, or retry policy;
- module identities, digests, schemas, models, or execution requirements.

Those values come from the application-owned `PlanBoundary` and
`ModuleRegistry` and are recomputed before policy evaluation or execution.
Unknown aliases and malformed or extra fields fail closed before generated
child insertion. A model must never participate in scheduling, dependency
resolution, dispatch, retries, leases, identity, idempotency, or grant
decisions.

Ship contracts and guardrails, not domain behavior. If behavior can be a
user-defined `Module`, it does not belong as a built-in library role.

## Law 2: the Module is the unit of identity

Every concrete `Module` has a non-empty, self-declared `module_id`. Never derive
module identity from graph position, attribute path, class name, or enclosing
workflow. The design test is simple: can trusted code name, validate, budget,
and execute this module from a plan that did not exist when the module was
imported? If not, the design is wrong.

Occurrence identity remains plan-relative and deterministic, but it is not
module identity. Reused, mapped, and nested occurrences must keep stable
logical-step and instance identities without changing the module's semantic
identity.

Static `Workflow.build()` authoring and generated authoring both terminate in
the same canonical `PlanIR`. Do not add another graph or serialization type for
an exception case. Serialized identity changes are wire changes: version them,
reject unsupported artifacts loudly, document regeneration or migration, and
never silently reinterpret old data.

## Law 3: simple and closed beats broad and configurable

- **One mechanism per concept.** `PlanIR` is the plan representation and
  `ModuleRegistry` is the trusted source for both validation metadata and exact
  executable resolution. Extend the owner; do not add a shadow registry,
  resolver, plan type, policy, baseline, or report.
- **One complete default path.** The common case should be one import and one
  call. If users assemble digests, schemas, validators, or orchestration by
  hand, the API is unfinished.
- **Public names pay rent.** Every exported name needs a one-sentence purpose,
  a non-test caller, and a justification when the public surface grows.
- **No speculative generality.** Add an abstraction only when a second real
  implementation or user requirement proves the seam.
- **Configuration needs a verified default.** Add an option only for a distinct
  user decision. Define precedence, name the selected source, and test both the
  default and override paths.
- **Delete dead paths.** A capability that is only validated, stored, exported,
  or tested but never initiated by a real path is not shipped.

Current reference point (2026-08-18): 18,875 physical lines across 29 Python
modules under `maida/workflows`, with 105 names in the root `__all__`. These are
directional measurements, not ceilings. Growth is allowed when it closes a real
user path and the change explains the cost. After deletion-heavy work, compare
absolute missing statements and branches as well as the coverage percentage so
denominator changes are not mistaken for regressions or improvements.

## Core Maida owns the product vocabulary

Core `maida` owns versioned policy, plan artifact, baseline, report, and trace
contracts under its `maida/contracts/` mechanism. This repository consumes
those contracts. It must not create a private verdict, policy dialect, baseline
stream, durable report envelope, or competing plan-change taxonomy.

When a plan-level concept does not fit:

1. propose the additive change in core first;
2. update the Python implementation, schemas, conformance vectors, and version
   source together;
3. propagate the authoritative snapshot through the cross-repository sync
   mechanism; and
4. consume the released core API here without `PYTHONPATH` or sibling-source
   workarounds.

For values that move on a release schedule, tests assert compatibility or
agreement with the authoritative source, not an exact duplicate literal. Exact
artifact versions may still be required at a strict decoding boundary; do not
confuse that wire check with a copied release-channel allowlist.

## Documentation and examples are executable product surfaces

- A shipped example must execute the real path it claims to demonstrate. A
  generated-plan example must vary the generated plan from runtime input; a
  module-level constant is not generation.
- Keep examples deterministic, offline, credential-free, and covered by the
  shipped-example inventory test. A public capability needs a non-test caller;
  a user-visible capability also needs a real example or product demo.
- Documentation must describe the code that exists now. Do not document a
  workaround as a feature, claim a release that is not published, or print a
  recovery command that the product does not follow.
- Test copy-paste installation and recovery instructions in an isolated
  environment. For changes that cross the `maida`/`maida-workflows` package
  boundary, verify the built or published packages together and state clearly
  when release coordination remains.
- A gate refusal must say what failed, why, which policy source was used, and
  what exact next step will affect the next identical command.

## Current phase: adoption proof

The structural realignment is over. The next phase is to prove that a working
engineer can adopt the feature without reading source or understanding this
repository's history.

Prioritize:

1. a fresh documented install followed by `maida demo --plan` in under 60
   seconds, using the packages and version constraints users actually receive;
2. one documented offline loop that generates, checks, executes, and displays
   post-execution verification evidence;
3. lower module-registration friction without weakening Laws 1 or 2; and
4. evidence from a real second execution substrate before generalizing the
   backend or evidence-store seam.

Until those paths are honest and repeatable, do not add providers, connectors,
domain modules, another execution backend, scheduler features, a new plan or
registry representation, a local Maida schema dialect, or a speculative
evidence sink. Idea notes are not tasks; require a scoped brief and acceptance
criteria before promoting one.

## Verification expectations

Use TDD when practical: add the smallest failing behavioral test, implement the
minimal change, rerun the focused test, then run the broader gates appropriate
to the risk.

The authoritative full-system check uses the repository's local PostgreSQL
service and must report zero database skips:

```text
docker compose up -d --wait postgres
MAIDA_WORKFLOWS_TEST_DSN=<local-compose-dsn> uv run pytest --cov --cov-branch -q
uv run ruff check .
uv run ruff format --check .
uv run mypy .
uv build
git diff --check
```

Additional risk-based checks:

- Changes to `PlanIR` import or serialization cover the successful round trip,
  exact fields, current version, binding dependencies, topology, identity, and
  malformed-input rejection paths.
- Changes to generated planning prove unknown aliases and authority-bearing
  fields are refused before child insertion, then exercise approval and
  post-execution proof.
- Backend changes run the same chain and fan-out plans locally and externally,
  compare the portable verified history, and prove the external path never
  enters the reference claim/lease/retry lifecycle.
- Core-contract changes run the local consumer test and core's
  `scripts/check_cross_repo_sync.py --workspace ..`; disclose when the remote
  scheduled check has not run.
- User-visible install/demo changes get a fresh isolated smoke test using only
  the documented commands. Record elapsed time and every point where source
  reading was required.

Do not report a network-limited build, a database-skipped suite, or a
source-overridden dependency as a pass for the intended packaged path.

## Definition of done

In addition to the parent checklist, a change here is done only when:

- it passes the verification-versus-execution boundary test;
- generated data still cannot express authority, identity, or execution
  control, and no model is in a deterministic control path;
- module identity remains graph-independent and all authoring paths still
  produce canonical `PlanIR`;
- core Maida remains the sole owner of shared policy, plan artifact, baseline,
  report, trace, and diff vocabulary;
- public API growth and new configuration are justified, and no parallel
  mechanism or dead capability was introduced;
- public capabilities have non-test callers and user-visible capabilities have
  real offline examples;
- coverage is maintained or a deletion-denominator analysis proves the
  remaining absolute uncovered surface did not grow;
- docs, installation instructions, recovery text, and the README opening match
  verified behavior; and
- exact test, lint, type, build, contract-sync, smoke, and portability results
  are reported, including failures and unrun checks.
