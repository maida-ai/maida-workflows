ALTER TABLE workflow_tasks
    ADD COLUMN parent_task_id UUID REFERENCES workflow_tasks(task_id),
    ADD COLUMN plan_region_id TEXT,
    ADD COLUMN plan_region_instance_id TEXT,
    ADD COLUMN plan_node_key TEXT,
    ADD COLUMN plan_digest TEXT,
    ADD CONSTRAINT workflow_tasks_plan_provenance_complete CHECK (
        (parent_task_id IS NULL AND plan_region_id IS NULL
         AND plan_region_instance_id IS NULL AND plan_node_key IS NULL
         AND plan_digest IS NULL)
        OR
        (parent_task_id IS NOT NULL AND plan_region_id IS NOT NULL
         AND plan_region_instance_id IS NOT NULL AND plan_node_key IS NOT NULL
         AND plan_digest IS NOT NULL)
    );

CREATE UNIQUE INDEX workflow_tasks_dynamic_node_idx
    ON workflow_tasks (run_id, plan_region_instance_id, plan_node_key)
    WHERE plan_region_instance_id IS NOT NULL;

CREATE INDEX workflow_tasks_dynamic_region_idx
    ON workflow_tasks (run_id, plan_region_instance_id, status)
    WHERE plan_region_instance_id IS NOT NULL;

CREATE UNIQUE INDEX workflow_events_plan_materialized_idx
    ON workflow_events (
        run_id,
        (payload->>'region_instance_id')
    )
    WHERE event_type = 'PLAN_MATERIALIZED';
