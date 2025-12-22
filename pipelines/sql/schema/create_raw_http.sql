-- http.log staging table
CREATE TABLE IF NOT EXISTS raw_http (
    ts DOUBLE,
    uid VARCHAR,
    id.orig_h VARCHAR,
    id.orig_p BIGINT,
    id.resp_h VARCHAR,
    id.resp_p BIGINT,
    trans_depth BIGINT
    method VARCHAR,
    host VARCHAR,
    uri VARCHAR,
    referrer VARCHAR,
    version VARCHAR,
    user_agent VARCHAR,
    origin VARCHAR,
    request_body_len BIGINT,
    response_body_len BIGINT,
    status_code BIGINT,
    status_msg VARCHAR,
    tags VARCHAR[],
    resp_fuids VARCHAR[],
    resp_mime_types VARCHAR[],
    proxied VARCHAR[],
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
