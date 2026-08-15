CREATE TABLE workflow_schema_migrations (
    version TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE workflow_definitions (
    digest TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    ir_version TEXT NOT NULL,
    canonical_ir JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TYPE workflow_execution_mode AS ENUM (
    'LIVE',
    'REPLAY_FULL_STUB',
    'REPLAY_SELECTIVE',
    'VERIFY_LIVE'
);

CREATE TYPE workflow_run_status AS ENUM (
    'PENDING',
    'RUNNING',
    'SUCCEEDED',
    'FAILED',
    'CANCELLED',
    'PAUSED'
);

CREATE TYPE workflow_task_status AS ENUM (
    'PENDING',
    'LEASED',
    'SUCCEEDED',
    'FAILED'
);

CREATE TABLE workflow_runs (
    run_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    definition_digest TEXT NOT NULL REFERENCES workflow_definitions(digest),
    execution_mode workflow_execution_mode NOT NULL DEFAULT 'LIVE',
    status workflow_run_status NOT NULL DEFAULT 'PENDING',
    root_input JSONB NOT NULL,
    root_input_schema_digest TEXT NOT NULL,
    root_output JSONB,
    root_output_schema_digest TEXT,
    replayable_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE workflow_tasks (
    task_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
    module_id TEXT NOT NULL,
    logical_step TEXT NOT NULL,
    step_instance_id TEXT NOT NULL,
    module_digest TEXT NOT NULL,
    dependency_instance_keys JSONB NOT NULL DEFAULT '[]'::jsonb,
    task_input JSONB NOT NULL,
    status workflow_task_status NOT NULL DEFAULT 'PENDING',
    lease_owner TEXT,
    lease_token UUID,
    lease_expires_at TIMESTAMPTZ,
    accepted_attempt_id UUID,
    accepted_boundary JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    UNIQUE (run_id, module_id, logical_step, step_instance_id)
);

CREATE TABLE workflow_attempts (
    attempt_id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES workflow_tasks(task_id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    lease_token UUID NOT NULL,
    status TEXT NOT NULL,
    diagnostic JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    UNIQUE (task_id, attempt_number)
);

ALTER TABLE workflow_tasks
    ADD CONSTRAINT workflow_tasks_accepted_attempt_fk
    FOREIGN KEY (accepted_attempt_id) REFERENCES workflow_attempts(attempt_id);

CREATE TABLE workflow_events (
    event_id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
    task_id UUID REFERENCES workflow_tasks(task_id) ON DELETE CASCADE,
    attempt_id UUID REFERENCES workflow_attempts(attempt_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE workflow_artifacts (
    digest TEXT PRIMARY KEY,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    media_type TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX workflow_tasks_claim_idx
    ON workflow_tasks (status, lease_expires_at, created_at);
CREATE INDEX workflow_events_run_order_idx
    ON workflow_events (run_id, event_id);
