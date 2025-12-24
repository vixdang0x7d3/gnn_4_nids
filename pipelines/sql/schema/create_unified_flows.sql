-- Unified network flow table.
-- Intermediate table joined from log sources,
-- used to compute target features sets (og_features & nf_features)
-- and forensic/analysis purposes
CREATE TABLE IF NOT EXISTS unified_flows (
    ts DOUBLE,
    uid VARCHAR PRIMARY KEY,
    "id.orig_h" VARCHAR,
    "id.orig_p" BIGINT,
    "id.resp_h" VARCHAR,
    "id.resp_p" BIGINT,
    proto VARCHAR,
    service VARCHAR,
    duration DOUBLE,
    orig_bytes BIGINT,
    resp_bytes BIGINT,
    conn_state VARCHAR,
    local_orig BOOLEAN,
    local_resp BOOLEAN,
    orig_pkts BIGINT,
    resp_pkts BIGINT,
    tcp_rtt DOUBLE,
    tcp_flags_orig BIGINT,
    tcp_flags_resp BIGINT,
    tcp_win_max_orig BIGINT,
    tcp_win_max_resp BIGINT,
    retrans_orig_pkts BIGINT,
    retrans_resp_pkts BIGINT,
    orig_pkt_times DOUBLE[],
    resp_pkt_times DOUBLE[],
    orig_pkt_sizes BIGINT[],
    resp_pkt_sizes BIGINT[],
    orig_ttls BIGINT[],
    resp_ttls BIGINT[],

    -- HTTP enrichment
    http_trans_depth BIGINT,
    http_res_bdy_len BIGINT,

    -- DNS enrichment (for NetFlow features)
    dns_query_id BIGINT,
    dns_query_type BIGINT,
    dns_ttl_answer BIGINT,

    -- FTP enrichment
    ftp_is_login BOOLEAN,
    ftp_command VARCHAR,
    ftp_reply_code BIGINT,

    -- Debug flags
    has_pkt_stats BOOLEAN,
    has_http BOOLEAN,
    has_dns BOOLEAN,
    has_ftp BOOLEAN,

    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
