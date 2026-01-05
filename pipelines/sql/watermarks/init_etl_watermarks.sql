INSERT INTO etl_watermarks (pipeline_stage, last_processed_ts, last_updated)
VALUES
    ('compute_unified_flows', 0.0, CURRENT_TIMESTAMP),
    ('compute_og_features', 0.0, CURRENT_TIMESTAMP),
    ('compute_nf_features', 0.0, CURRENT_TIMESTAMP),
    ('archive_og_features', 0.0, CURRENT_TIMESTAMP),
    ('archive_nf_features', 0.0, CURRENT_TIMESTAMP),
ON CONFLICT (pipeline_stage) DO NOTHING;
