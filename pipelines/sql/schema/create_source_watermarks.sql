-- Watermark table for raw logs.
-- Keep track of latest timestamps,
-- used for calculating safe horizon for feature computation
CREATE TABLE IF NOT EXISTS source_watermarks (
    source VARCHAR PRIMARY KEY,
    max_event_ts DOUBLE,
    max_ingestion_ts TIMESTAMP
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
