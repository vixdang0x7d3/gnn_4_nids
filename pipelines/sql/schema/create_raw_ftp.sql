-- ftp.log staging table
CREATE TABLE IF NOT EXISTS raw_ftp (
    ts DOUBLE,
    uid VARCHAR,
    "id.orig_h" VARCHAR,
    "id.orig_p" BIGINT,
    "id.resp_h" VARCHAR,
    "id.resp_p" BIGINT,
    user VARCHAR,
    password VARCHAR,
    command VARCHAR,
    reply_code BIGINT,
    reply_msg VARCHAR,
    data_channel_passive BOOL,
    data_channel_orig_h VARCHAR,
    data_channel_resp_h VARCHAR,
    data_channel_resp_p BIGINT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
