ALTER TABLE workflow_runs
    ADD COLUMN start_idempotency_key TEXT,
    ADD COLUMN start_request_digest TEXT,
    ADD CONSTRAINT workflow_runs_start_idempotency_pair CHECK (
        (start_idempotency_key IS NULL) = (start_request_digest IS NULL)
    );

CREATE UNIQUE INDEX workflow_runs_tenant_start_idempotency_idx
    ON workflow_runs (tenant_id, start_idempotency_key)
    WHERE start_idempotency_key IS NOT NULL;
