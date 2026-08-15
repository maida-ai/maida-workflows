ALTER TABLE workflow_tasks
    ALTER COLUMN status DROP DEFAULT,
    ALTER COLUMN status TYPE TEXT USING status::text;

DROP TYPE workflow_task_status;

CREATE TYPE workflow_task_status AS ENUM (
    'BLOCKED',
    'READY',
    'LEASED',
    'RUNNING',
    'NEEDS_INPUT',
    'NEEDS_APPROVAL',
    'WAITING_SIGNAL',
    'SUCCEEDED',
    'FAILED',
    'CANCELLED'
);

ALTER TABLE workflow_tasks
    ALTER COLUMN status TYPE workflow_task_status USING status::workflow_task_status,
    ALTER COLUMN status SET DEFAULT 'BLOCKED';

CREATE UNIQUE INDEX workflow_events_command_id_idx
    ON workflow_events (run_id, (payload->>'command_id'))
    WHERE event_type = 'COMMAND_RECEIVED';
