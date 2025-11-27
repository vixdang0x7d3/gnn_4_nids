-- Performance indices for delta queries
CREATE INDEX IF NOT EXISTS idx_raw_conn_ts ON raw_conn(ts);
CREATE INDEX IF NOT EXISTS idx_raw_conn_uid ON raw_conn(uid);
CREATE INDEX IF NOT EXISTS idx_raw_unsw_uid ON raw_unsw_extra(uid);
CREATE INDEX IF NOT EXISTS idx_features_stime ON og_features(stime);
