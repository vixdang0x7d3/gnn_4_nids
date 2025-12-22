DELETE FROM raw_unsw_extra WHERE ts < :cutoff_ts
RETURNING 1
