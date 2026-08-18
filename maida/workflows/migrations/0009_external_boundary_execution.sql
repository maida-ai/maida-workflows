ALTER TABLE workflow_attempts
    ALTER COLUMN lease_token DROP NOT NULL,
    ADD COLUMN execution_id TEXT,
    ADD CONSTRAINT workflow_attempts_execution_fence CHECK (
        (lease_token IS NULL) <> (execution_id IS NULL)
    );

CREATE UNIQUE INDEX workflow_attempts_execution_id_idx
    ON workflow_attempts (execution_id)
    WHERE execution_id IS NOT NULL;
