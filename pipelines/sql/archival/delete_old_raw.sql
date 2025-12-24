DELETE FROM ${source} WHERE ingested_at < :cutoff_ts
RETURNING 1;
