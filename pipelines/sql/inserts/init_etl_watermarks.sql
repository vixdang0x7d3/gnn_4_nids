INSERT INTO etl_watermarks VALUES
    ('unified_flows', 0.0, CURRENT_TIMESTAMP),
    ('og_features', 0.0, CURRENT_TIMESTAMP),
    ('nf_features', 0.0, CURRENT_TIMESTAMP)
ON CONFLICT (pipeline_stage) DO NOTHING;
