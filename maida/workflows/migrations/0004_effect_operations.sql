CREATE TABLE workflow_effect_operations (
    task_id UUID NOT NULL REFERENCES workflow_tasks(task_id) ON DELETE CASCADE,
    effect_name TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    reservation_order INTEGER NOT NULL CHECK (reservation_order >= 0),
    connector TEXT NOT NULL,
    operation TEXT NOT NULL,
    connector_version TEXT,
    idempotency_requirement TEXT NOT NULL CHECK (
        idempotency_requirement IN ('none', 'optional', 'required')
    ),
    adapter_idempotent BOOLEAN NOT NULL,
    request_digest TEXT NOT NULL,
    result_schema_digest TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'RESERVED' CHECK (
        status IN ('RESERVED', 'ATTEMPTED', 'COMMITTED')
    ),
    result_ref JSONB,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_attempt_id UUID REFERENCES workflow_attempts(attempt_id),
    approval_request_id TEXT,
    approval_command_id TEXT,
    approval_event_id BIGINT REFERENCES workflow_events(event_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempted_at TIMESTAMPTZ,
    committed_at TIMESTAMPTZ,
    PRIMARY KEY (task_id, effect_name, ordinal),
    UNIQUE (task_id, reservation_order),
    CHECK ((status = 'COMMITTED') = (result_ref IS NOT NULL)),
    CHECK (
        (approval_request_id IS NULL) = (approval_command_id IS NULL)
        AND (approval_command_id IS NULL) = (approval_event_id IS NULL)
    )
);

CREATE INDEX workflow_effect_operations_status_idx
    ON workflow_effect_operations (status, created_at);
