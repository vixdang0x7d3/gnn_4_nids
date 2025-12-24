-- delta-based aggregation for generating unified view of network flows
-- base table for feature sets (og_features, nf_features)
-- parameter: :last_raw_log_ts

MERGE INTO unified_flows as dest
USING (
    -- Deduplicate enrichment tables (pick first record per uid)
    WITH dedup_dns AS (
        SELECT * FROM raw_dns
        QUALIFY ROW_NUMBER() OVER (PARTITION BY uid ORDER BY ts) = 1
    ),
    dedup_http AS (
        SELECT * FROM raw_http
        QUALIFY ROW_NUMBER() OVER (PARTITION BY uid ORDER BY ts) = 1
    )

    SELECT
        c.ts,
        c.uid,
        c."id.orig_h",
        c."id.orig_p",
        c."id.resp_h",
        c."id.resp_p",
        c.proto,
        COALESCE(c.service, 'unknown') AS service,
        COALESCE(c.duration, 0.0) AS dur,
        s.orig_bytes,
        s.resp_bytes,
        c.conn_state,
        c.local_orig,
        c.local_resp,
        c.orig_pkts,
        c.resp_pkts,

        COALESCE(s.tcp_rtt, 0) AS tcp_rtt,
        COALESCE(s.tcp_flags_orig, 0) AS tcp_flags_orig,
        COALESCE(s.tcp_flags_resp, 0) AS tcp_flags_resp,
        COALESCE(s.tcp_win_max_orig, 0) AS tcp_win_max_orig,
        COALESCE(s.tcp_win_max_resp, 0) AS tcp_win_max_resp,
        COALESCE(s.retrans_orig_pkts, 0) AS retrans_orig_pkts,
        COALESCE(s.retrans_resp_pkts, 0) AS retrans_resp_pkts,
        COALESCE(s.orig_pkt_times, []) AS orig_pkt_times,
        COALESCE(s.resp_pkt_times, []) AS resp_pkt_times,
        COALESCE(s.orig_pkt_sizes, []) AS orig_pkt_sizes,
        COALESCE(s.resp_pkt_sizes, []) AS resp_pkt_sizes,

        COALESCE(s.orig_ttls, []) AS orig_ttls,
        COALESCE(s.resp_ttls, []) AS resp_ttls,

        COALESCE(h.trans_depth, 0) AS http_trans_depth,
        COALESCE(h.response_body_len, 0) AS http_res_bdy_len,

        COALESCE(d.trans_id, 0) AS dns_query_id,
        COALESCE(d.qtype, 0) AS dns_query_type,

        CASE
           WHEN len(COALESCE(d."TTLs", [])) > 0 THEN d."TTLs"[1]
           ELSE 0
        END AS dns_ttl_answer,

        -- FTP enrichment
        CASE WHEN f.user IS NOT NULL AND f.password IS NOT NULL THEN true ELSE false END AS ftp_is_login,
        f.command AS ftp_command,
        COALESCE(f.reply_code, 0) AS ftp_reply_code,

        CASE WHEN s.uid is not null THEN 1 ELSE 0 END AS has_pkt_stats,
        CASE WHEN h.uid is not null THEN 1 ELSE 0 END AS has_http,
        CASE WHEN d.uid is not null THEN 1 ELSE 0 END AS has_dns,
        CASE WHEN f.uid is not null THEN 1 ELSE 0 END AS has_ftp,

        CURRENT_TIMESTAMP AS computed_at


    FROM raw_conn c
    LEFT JOIN raw_pkt_stats s ON c.uid = s.uid
    LEFT JOIN dedup_dns d ON c.uid = d.uid
    LEFT JOIN dedup_http h ON c.uid = h.uid
    LEFT JOIN raw_ftp f ON c.uid = f.uid
    WHERE c.ts > :last_raw_log_ts -- DELTA FILTER
) AS src
ON dest.uid = src.uid
WHEN MATCHED THEN UPDATE SET computed_at = src.computed_at
WHEN NOT MATCHED THEN INSERT *
RETURNING 1;
