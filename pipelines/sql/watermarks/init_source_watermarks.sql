-- Initialize for each raw table
INSERT INTO source_watermarks (source, max_event_ts, max_ingestion_ts, last_updated)
VALUES
    ('raw_conn', 0.0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('raw_dns', 0.0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('raw_http', 0.0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('raw_pkt_stats', 0.0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('raw_ssl', 0.0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('raw_ftp', 0.0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (source) DO NOTHING;
