ALTER TABLE workflow_tasks
    ADD COLUMN budget_declaration JSONB NOT NULL DEFAULT
        '{"cost_usd":null,"model_tokens":null,"tool_calls":null,"wall_time_ms":null}'::jsonb;
