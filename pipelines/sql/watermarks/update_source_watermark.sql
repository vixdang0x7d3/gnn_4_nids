-- Update source watermarks based on newly arrived data
-- parameter: ${source}
INSERT INTO source_watermarks (source, max_event_ts)
SELECT
    '${source}',
    COALESCE(MAX(ts), 0.0),
    COALESCE(MAX(ingested_at), CURRENT_TIMESTAMP),
    CURRENT_TIMESTAMP
FROM ${source}
ON CONFLICT (source) DO UPDATE SET
    max_event_ts = GREATEST(
        EXCLUDED.max_event_ts,
        source_watermarks.max_event_ts
    ),
    max_ingestion_ts = GREATEST(
        EXCLUDED.max_ingestion_ts,
        source_watermarks.max_ingestion_ts
    ),
    last_updated = EXCLUDED.last_updated;
