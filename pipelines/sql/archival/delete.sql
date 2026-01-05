DELETE FROM ${source} WHERE ts < :cutoff_ts
RETURNING 1;
