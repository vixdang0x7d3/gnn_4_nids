-- Watermarks for ETL processing steps
CREATE TABLE IF NOT EXISTS etl_watermarks (
    pipeline_stage VARCHAR PRIMARY KEY,  -- 'unified_flows', 'og_features', 'nf_features'
    last_processed_ts DOUBLE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
