-- Watermarks for ETL processing steps
CREATE TABLE IF NOT EXISTS etl_watermarks (
    pipeline_stage VARCHAR PRIMARY KEY,
    last_processed_ts DOUBLE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
