DELETE FROM ${source} WHERE computed_at < :cutoff_ts
RETURNING 1;
