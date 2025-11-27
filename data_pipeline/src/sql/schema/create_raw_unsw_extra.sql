-- Raw UNSW-NB15 extra features staging table
CREATE TABLE IF NOT EXISTS raw_unsw_extra (
    ts DOUBLE,
    uid VARCHAR,
    id_orig_h VARCHAR,
    id_orig_p BIGINT,
    id_resp_h VARCHAR,
    id_resp_p BIGINT,
    tcp_rtt DOUBLE,
    src_pkt_times DOUBLE[],
    dst_pkt_times DOUBLE[],
    src_ttl BIGINT,
    dst_ttl BIGINT,
    src_pkt_sizes DOUBLE[],
    dst_pkt_sizes DOUBLE[],
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
