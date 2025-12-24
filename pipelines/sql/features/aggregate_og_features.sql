-- delta-based aggregation for generating UNSW-NB15 (original) feature set
-- from base table (unified_flows)
-- parameter: :last_unified_flow_ts

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
                WHEN conn_state IN ('SF', 'S1', 'S2', 'S3') THEN 'FIN'
                WHEN conn_state IN ('S0', 'REJ') THEN 'CON'
                ELSE 'INT'
            END AS state,

            duration AS dur,
            orig_bytes AS sbytes,
            resp_bytes AS dbytes,

            COALESCE(orig_ttls[1], 0) AS sttl,
            COALESCE(resp_ttls[1], 0) AS dttl,

            retrans_orig_pkts AS sloss,
            retrans_resp_pkts AS dloss,

            CASE
                WHEN service = 'http' THEN 'http'
                WHEN service = 'ssh' THEN 'ssh'
                WHEN service = 'dns' THEN 'dns'
                ELSE 'other'
            END AS service,

            -- Derived features
            CASE WHEN duration > 0 THEN (orig_bytes * 8.0) / duration ELSE 0 END AS sload,
            CASE WHEN duration > 0 THEN (resp_bytes * 8.0) / duration ELSE 0 END AS dload,

            orig_pkts AS spkts,
            resp_pkts AS dpkts,

            tcp_win_max_orig AS swin,
            tcp_win_max_resp AS dwin,

            CASE WHEN orig_pkts > 0 THEN orig_bytes / orig_pkts ELSE 0 END AS smeansz,
            CASE WHEN resp_pkts > 0 THEN resp_bytes / resp_pkts ELSE 0 END AS dmeansz,

            http_trans_depth AS trans_depth,
            http_res_bdy_len AS resp_bdy_len,

            CASE WHEN LEN(orig_pkt_times) > 1
                THEN (
                    SELECT STDDEV_SAMP(x)
                    FROM UNNEST([orig_pkt_times[i+1] - orig_pkt_times[i] FOR i IN RANGE(LEN(orig_pkt_times) - 1)]) AS t(x)
                )
                ELSE 0
            END AS sjit,

            CASE WHEN LEN(resp_pkt_times) > 1
                THEN (
                    SELECT STDDEV_SAMP(x)
                    FROM UNNEST([resp_pkt_times[i+1] - resp_pkt_times[i] FOR i IN RANGE(LEN(resp_pkt_times) - 1)]) AS t(x)
                )
                ELSE 0
            END AS djit,

            ts AS stime,
            (ts + duration) AS ltime,

            CASE WHEN LEN(orig_pkt_times) > 1
                THEN LIST_AVG([orig_pkt_times[i+1] - orig_pkt_times[i] FOR i IN RANGE(LEN(orig_pkt_times) - 1)])
                ELSE 0
            END AS sintpkt,

            CASE WHEN LEN(resp_pkt_times) > 1
                THEN LIST_AVG([resp_pkt_times[i+1] - resp_pkt_times[i] FOR i IN RANGE(LEN(resp_pkt_times) - 1)])
                ELSE 0
            END AS dintpkt,

            tcp_rtt AS tcprtt,

            CASE WHEN tcp_rtt > 0 THEN tcp_rtt / 2.0 ELSE 0 END AS synack,
            CASE WHEN tcp_rtt > 0 THEN tcp_rtt / 2.0 ELSE 0 END AS ackdat,

            CASE WHEN srcip = dstip AND sport = dsport THEN 1 ELSE 0 END AS is_sm_ips_ports,

            COALESCE(ftp_is_login, 0) AS is_ftp_login,
            ftp_command

        FROM new_flows
    ),
    tw_src AS (
        SELECT
            b1.uid,
            COUNT(*) FILTER (WHERE b1.uid != b2.uid) AS ct_src_ltm,
            COUNT(*) FILTER (WHERE b1.uid != b2.uid AND b1.service = b2.service) AS ct_srv_src,
            COUNT(*) FILTER (WHERE b1.uid != b2.uid AND b1.dsport = b2.dsport) AS ct_src_dport_ltm
        FROM base_features b1
        LEFT JOIN base_features b2
            ON b1.srcip = b2.srcip
            AND b2.ts BETWEEN (b1.ts - 100) AND b1.ts
        GROUP BY b1.uid
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
    ),
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
    )
    SELECT
        f.stime AS ts,  -- Normalize to 'ts' column name
        f.srcip, f.sport, f.dstip, f.dsport, f.proto, f.state, f.dur,
        f.sbytes, f.dbytes, f.sttl, f.dttl, f.sloss, f.dloss, f.service,
        f.sload, f.dload, f.spkts, f.dpkts, f.swin, f.dwin,
        0 AS stcpb, 0 AS dtcpb,
        f.smeansz, f.dmeansz, f.trans_depth, f.resp_bdy_len, f.sjit, f.djit,
        f.stime, f.ltime, f.sintpkt, f.dintpkt, f.tcprtt, f.synack, f.ackdat,
        f.is_sm_ips_ports,
        COALESCE(td.ct_state_ttl, 0) AS ct_state_ttl,
        COALESCE(td.ct_flw_http_mthd, 0) AS ct_flw_http_mthd,
        f.is_ftp_login,
        0 AS ct_ftp_cmd,
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
ON dest.srcip = src.srcip
    AND dest.sport = src.sport
    AND dest.dstip = src.dstip
    AND dest.dsport = src.dsport
    AND dest.stime = src.stime
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
RETURNING 1;
