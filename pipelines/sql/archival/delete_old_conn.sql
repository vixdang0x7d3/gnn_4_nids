DELETE FROM raw_conn WHERE ts < :cutoff_ts
RETURNING 1
