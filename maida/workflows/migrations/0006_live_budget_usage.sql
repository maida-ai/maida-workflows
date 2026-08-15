CREATE TABLE workflow_budget_usage (
    task_id UUID NOT NULL REFERENCES workflow_tasks(task_id) ON DELETE CASCADE,
    charge_key TEXT NOT NULL,
    attempt_id UUID NOT NULL REFERENCES workflow_attempts(attempt_id),
    kind TEXT NOT NULL CHECK (kind IN ('model', 'tool')),
    reservation JSONB NOT NULL,
    actual JSONB,
    status TEXT NOT NULL DEFAULT 'RESERVED' CHECK (
        status IN ('RESERVED', 'COMMITTED')
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    committed_at TIMESTAMPTZ,
    PRIMARY KEY (task_id, charge_key),
    CHECK ((status = 'COMMITTED') = (actual IS NOT NULL))
);

CREATE INDEX workflow_budget_usage_attempt_idx
    ON workflow_budget_usage (attempt_id, created_at);
