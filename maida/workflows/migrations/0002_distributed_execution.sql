ALTER TABLE workflow_tasks
    ALTER COLUMN status DROP DEFAULT,
    ALTER COLUMN status TYPE TEXT USING status::text;

UPDATE workflow_tasks SET status = 'READY' WHERE status = 'PENDING';

DROP TYPE workflow_task_status;

CREATE TYPE workflow_task_status AS ENUM (
    'BLOCKED',
    'READY',
    'LEASED',
    'RUNNING',
    'SUCCEEDED',
    'FAILED'
);

ALTER TABLE workflow_tasks
    ALTER COLUMN status TYPE workflow_task_status USING status::workflow_task_status,
    ALTER COLUMN status SET DEFAULT 'BLOCKED',
    ALTER COLUMN task_input DROP NOT NULL,
    ADD COLUMN node_id TEXT,
    ADD COLUMN dependency_node_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN execution_requirements JSONB NOT NULL DEFAULT
        '{"capabilities":[],"cpu":null,"dependency_lock":null,"image":null,"isolation":"process","memory":null}'::jsonb,
    ADD COLUMN execution_isolation TEXT NOT NULL DEFAULT 'process',
    ADD COLUMN execution_image TEXT,
    ADD COLUMN execution_cpu INTEGER CHECK (execution_cpu IS NULL OR execution_cpu > 0),
    ADD COLUMN execution_memory_bytes BIGINT CHECK (
        execution_memory_bytes IS NULL OR execution_memory_bytes > 0
    ),
    ADD COLUMN required_executor_capabilities JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN capability_grant JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN branch_decisions JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN map_decisions JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN ready_at TIMESTAMPTZ;

UPDATE workflow_tasks SET node_id = logical_step WHERE node_id IS NULL;

ALTER TABLE workflow_tasks ALTER COLUMN node_id SET NOT NULL;

ALTER TABLE workflow_attempts
    ADD COLUMN worker_id TEXT,
    ADD COLUMN claimed_at TIMESTAMPTZ,
    ADD COLUMN checkpoint_ref JSONB,
    ALTER COLUMN started_at DROP DEFAULT,
    ALTER COLUMN started_at DROP NOT NULL;

UPDATE workflow_attempts
SET worker_id = COALESCE(
        (
            SELECT accepted_boundary->'accepted_attempt'->>'worker_id'
            FROM workflow_tasks
            WHERE workflow_tasks.task_id = workflow_attempts.task_id
        ),
        'legacy-worker'
    ),
    claimed_at = COALESCE(started_at, now());

ALTER TABLE workflow_attempts
    ALTER COLUMN worker_id SET NOT NULL,
    ALTER COLUMN claimed_at SET NOT NULL,
    ALTER COLUMN claimed_at SET DEFAULT now();

DROP INDEX workflow_tasks_claim_idx;

CREATE INDEX workflow_tasks_claim_idx
    ON workflow_tasks (
        status,
        execution_isolation,
        ready_at,
        lease_expires_at,
        created_at
    );
