DELETE FROM og_features WHERE stime < :cutoff_ts
RETURNING 1
