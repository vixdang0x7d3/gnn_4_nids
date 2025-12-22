-- conn.log staging table
CREATE TABLE IF NOT EXISTS raw_conn (
    ts DOUBLE,
    uid VARCHAR,
    id.orig_h VARCHAR,
    id.orig_p BIGINT,
    id.resp_h VARCHAR,
    id.resp_p BIGINT,
    proto VARCHAR,
    service VARCHAR,
    duration DOUBLE,
    orig_bytes BIGINT,
    resp_bytes BIGINT,
    conn_state VARCHAR,
    local_orig BOOLEAN,
    local_resp BOOLEAN,
    missed_bytes BIGINT,
    history VARCHAR,
    orig_pkts BIGINT,
    orig_ip_bytes BIGINT,
    resp_pkts BIGINT,
    resp_ip_bytes BIGINT,
    ip_proto BIGINT,
    tunnel_parent STRING
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
