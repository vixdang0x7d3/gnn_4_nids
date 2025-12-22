-- Performance indices for delta queries
CREATE INDEX IF NOT EXISTS idx_raw_conn_ts ON raw_conn(ts);
CREATE INDEX IF NOT EXISTS idx_raw_conn_uid ON raw_conn(uid);

CREATE INDEX IF NOT EXISTS idx_raw_pkt_stats ON raw_pkt_stats(uid);
CREATE INDEX IF NOT EXISTS idx_raw_dns ON raw_dns(uid);
CREATE INDEX IF NOT EXISTS idx_raw_http ON raw_http(uid);
CREATE INDEX IF NOT EXISTS idx_raw_ftp ON raw_ftp(uid);

-- Primary index for delta processing
CREATE INDEX IF NOT EXISTS IF idx_unified_flows_ts
ON unified_flows(ts);

-- Indices for time-window lookups
CREATE INDEX IF NOT EXISTS idx_unified_flows_id_orig_h_ts
ON unified_flows("id.orig_h", ts);

CREATE INDEX IF NOT EXISTS idx_unified_flows_id_resp_h_ts
ON unified_flows("id.resp_h", ts);

CREATE INDEX IF NOT EXISTS idx_unified_flows_id_orig_h_p_ts
ON unified_flows("id.orig_h", "id.orig_p", ts)


CREATE INDEX IF NOT EXISTS idx_unified_flows_id_resp_h_p_ts
ON unified_flows("id.resp_h", "id.orig_p", ts)

CREATE INDEX IF NOT EXISTS idx_unified_flows_service_ts
ON unified_flows(service, ts)

CREATE INDEX IF NOT EXISTS idx_unified_flows_proto_ts
ON unified_flows(proto, ts)

-- Composite index for common time-window queries
CREATE INDEX IF NOT EXISTS idx_unified_flows_service_ts
ON unified_flows(service, ts);

CREATE INDEX IF NOT EXISTS idx_unified_flows_proto_ts
ON unified_flows(proto, ts);

CREATE INDEX IF NOT EXISTS idx_og_features_stime ON og_features(stime);
CREATE INDEX IF NOT EXISTS idx_nf_features_flow_start ON nf_features(flow_start_milliseconds);
