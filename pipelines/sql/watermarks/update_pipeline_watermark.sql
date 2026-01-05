-- Update pipeline watermark to new timestamp
-- params: :stage, :last_processed_ts
INSERT INTO etl_watermarks (pipeline_stage, last_processed_ts, last_updated)
VALUES
    (:stage, :last_processed_ts, CURRENT_TIMESTAMP)
ON CONFLICT (pipeline_stage) DO UPDATE SET
    last_processed_ts = EXCLUDED.last_processed_ts,
    last_updated = EXCLUDED.last_updated;
