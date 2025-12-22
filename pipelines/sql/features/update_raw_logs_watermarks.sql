-- Update pipeline watermark for raw logs
-- parameter :safe_horizon
UPDATE etl_watermarks
SET last_processed_ts = :safe_horizon,
    updated_at = CURRENT_TIMESTAMP
WHERE pipeline_stage = 'raw_logs';
