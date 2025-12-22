MERGE INTO og_features as dest
USING (
    WITH new_flows AS (
        -- Only process new flows
        SELECT * FROM unified_flows
        WHERE ts > :last_unified_flow_ts -- DELTA FILTER
    ),
    base_features AS (
    -- Compute per-flow features
        SELECT
            uid,
            ts,
            "id.orig_h" AS srcip,
            "id.orig_p" AS sport,
            "id.resp_h" AS dstip,
            "id.resp_p" AS dsport,

            CASE
                WHEN proto = 'tcp' THEN 'tcp'
                WHEN proto = 'udp' THEN 'udp'
                ELSE 'other'
            END AS proto,

            CASE
                WHEN conn_state = 'S1' THEN 'fin'
                WHEN conn_state IN ('S1', 'S2', 'S3') THEN 'con'
                ELSE 'other'
            END AS state,

            duration AS dur,

            COALESCE(orig_ttls[1], 0) AS sttl,
            COALESCE(resp_ttls[1], 0) AS dttl,

            retrans_orig_pkts AS sloss,
            retrans_resp_pkts AS dloss,

            CASE
                WHEN service = 'http' THEN 'http'
                WHEN service = 'ssh' THEN 'ssh'
                ELSE 'other'
            END AS service,


            -- Derived features
            CASE WHEN dur > 0 THEN (orig_bytes * 8.0) / dur ELSE 0 END AS sload,
            CASE WHEN dur > 0 THEN (resp_bytes * 8.0) / dur ELSE 0 END AS dload,

            orig_pkts AS spkts,
            resp_pkts AS dpkts,

            tcp_win_max_orig AS swin,
            tcp_win_max_resp AS dwin,

            CASE WHEN orig_pkts > 0 THEN orig_bytes / orig_pkts else 0 END AS smeansz,
            CASE WHEN resp_pkts > 0 THEN resp_bytes / resp_pkts else 0 END AS dmeansz,

            http_trans_detph AS trans_depth,
            http_resp_bdy_len AS resp_bdy_len,

            CASE WHEN LEN(orig_pkt_times) > 1
                THEN (
                    SELECT STDDEV_SAMP(x)
                    FROM UNNEST([orig_pkt_times[i+1] - orig_pkt_times[i] FOR i IN RANGE(LEN(orig_pkt_times) - 1)])
                    AS t(x)
                )
                ELSE 0
            END AS sjit,

            CASE WHEN LEN(resp_pkt_times) > 1
                THEN (
                    SELECT STDDEV_SAMP(x)
                    FROM UNNEST([resp_pkt_times[i+1] - resp_pkt_times[i] FOR i IN RANGE(LEN(resp_pkt_times) - 1)])
                    AS t(x)
                )
                ELSE 0
            END AS djit


            ts AS stime,
            (ts + dur) AS ltime,

            CASE WHEN LEN(orig_pkt_times) > 1
                THEN LIST_AVG([
                    orig_pkt_times[i+1] - orig_pkt_times[i]
                    for i IN RANGE(LEN(orig_pkt_times) - 1)
                ])
                ELSE 0
            END AS sinpkt,

            CASE WHEN LEN(dst_pkt_times) > 1
                THEN LIST_AVG([
                    dst_pkt_times[i+1] - dst_pkt_times[i]
                    FOR i IN RANGE(LEN(orig_pkt_times) - 1)
                ])
                ELSE 0
            END AS dinpkt,

            tcp_rtt AS tcprtt,

            CASE WHEN history LIKE '%S%' AND history LIKE '%H%' AND tcp_rtt > 0 THEN tcp_rtt /  2.0 ELSE 0 END AS synack,
            CASE WHEN history LIKE '%S%' AND history LIKE '%H%' AND tcp_rtt > 0 THEN tcp_rtt /  2.0 ELSE 0 END AS ackdat,

            CASE WHEN srcip = dstip AND sport = dsport THEN 1 ELSE 0 END AS is_sm_ips_ports

            fitp_is_login AS is_ftp_login,

            ftp_command

        FROM new_flows
    ),
    tw_dst AS (
        SELECT
            b1.uid,
            COUNT(*) FILTER (WHERE b1.uid != b2.uid) AS ct_dst_ltm,
            COUNT(*) FILTER (WHERE b1.uid != b2.uid AND b1.service = 'http') AS ct_flw_http_mthd,
            COUNT(*) FILTER (WHERE b1.uid != b2.uid AND b1.service = b2.service) AS ct_srv_dst,
            COUNT(*) FILTER (WHERE b1.uid != b2.uid AND b1.state = b2.state AND b1.sttl = b2.sttl) AS ct_state_ttl,
            COUNT(*) FILTER (WHERE b1.uid != b2.uid AND b1.sport = b2.sport) AS ct_dst_sport_ltm
        FROM base_features b1
        LEFT JOIN base_features b2
            ON b1.dstip = b2.dstip
            AND b2.ts BETWEEN (b1.ts - 100) AND b1.ts
        GROUP BY b1.uid
    )
    tw_src_dst AS (
        SELECT
            b1.uid,
            COUNT(*) FILTER (WHERE b1.uid != b2.uid) AS ct_dst_src_ltm
        FROM base_features b1
        LEFT JOIN base_features b2
            ON b1.srcip = b2.srcip
            AND b1.dstip = b2.dstip
            AND b2.ts BETWEEN (b1.ts - 100) AND b1.ts
        GROUP BY b1.uid
    ),
    SELECT
        f.srcip, f.sport, f.dstip, f.dsport, f.proto, f.state, f.dur,
        f.sbytes, f.dbytes, f.sttl, f.dttl, f.sloss, f.dloss, f.service,
        f.sload, f.dload, f.spkts, f.dpkts, f.swin, f.dwin,
        0 AS stcpb, 0 AS dtcpb,
        f.smeansz, f.dmeansz, f.trans_depth, f.res_bdy_len, f.sjit, f.djit,
        f.stime, f.ltime, f.sintpkt, f.dintpkt, f.tcprtt, f.synack, f.ackdat,
        f.is_sm_ips_ports,
        f.is_ftp_login,
        0 AS ct_ftp_cmd,
        COALESCE(td.ct_state_ttl, 0) AS ct_state_ttl,
        COALESCE(td.ct_flw_http_mthd, 0) AS ct_flw_http_mthd,
        COALESCE(ts.ct_srv_src, 0) AS ct_srv_src,
        COALESCE(td.ct_srv_dst, 0) AS ct_srv_dst,
        COALESCE(td.ct_dst_ltm, 0) AS ct_dst_ltm,
        COALESCE(ts.ct_src_ltm, 0) AS ct_src_ltm,
        COALESCE(ts.ct_src_dport_ltm, 0) AS ct_src_dport_ltm,
        COALESCE(td.ct_dst_sport_ltm, 0) AS ct_dst_sport_ltm,
        COALESCE(tsd.ct_dst_src_ltm, 0) AS ct_dst_src_ltm

    FROM base_features f
    LEFT JOIN tw_src ts ON f.uid = ts.uid
    LEFT JOIN tw_dst td ON f.uid = td.uid
    LEFT JOIN tw_src_dst tsd ON f.uid = tsd.uid
) AS src
ON dest.uid = src.uid
WHEN MATCHED THEN UPDATE SET computed_at = src.computed_at
WHEN NOT MATCHED THEN INSERT *
RETURNING 1;
