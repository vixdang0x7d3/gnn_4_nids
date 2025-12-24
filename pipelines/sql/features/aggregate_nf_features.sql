-- delta-based aggregation for generating NetFlow feature set
-- from base table (unified_flows)
-- parameter: :last_unified_flow_ts

MERGE INTO nf_features AS dest
USING (
    SELECT
        uid,
        ts,  -- Flow start timestamp
        "id.orig_h" AS ipv4_src_addr,
        "id.orig_p" AS l4_src_port,
        "id.resp_h" AS ipv4_dst_addr,
        "id.resp_p" AS l4_dst_port,

        -- Protocol encoding
        CASE
            WHEN proto = 'tcp' THEN 6
            WHEN proto = 'udp' THEN 17
            WHEN proto = 'icmp' THEN 1
            ELSE 0
        END AS protocol,

        service AS l7_proto,

        -- Byte/packet stats
        orig_bytes AS in_bytes,
        resp_bytes AS out_bytes,
        orig_pkts AS in_pkts,
        resp_pkts AS out_pkts,

        CASE WHEN duration > 0
            THEN (orig_bytes * 1.0) / duration
            ELSE 0
        END AS src_to_dst_second_bytes,

        CASE WHEN duration > 0
            THEN (resp_bytes * 1.0) / duration
            ELSE 0
        END AS dst_to_src_second_bytes,

        CASE WHEN orig_pkts > 0
            THEN (orig_bytes::DOUBLE / orig_pkts) * retrans_orig_pkts
            ELSE 0
        END AS retransmitted_in_bytes,

        CASE WHEN resp_pkts > 0
            THEN (resp_bytes::DOUBLE / resp_pkts) * retrans_resp_pkts
            ELSE 0
        END AS retransmitted_out_bytes,

        retrans_orig_pkts AS retransmitted_in_pkts,
        retrans_resp_pkts AS retransmitted_out_pkts,

        -- Packet size stats
        CASE
            WHEN LEN(orig_pkt_sizes) > 0 AND LEN(resp_pkt_sizes) > 0
                THEN LEAST(LIST_MIN(orig_pkt_sizes), LIST_MIN(resp_pkt_sizes))
            WHEN LEN(orig_pkt_sizes) > 0
                THEN LIST_MIN(orig_pkt_sizes)
            WHEN LEN(resp_pkt_sizes) > 0
                THEN LIST_MIN(resp_pkt_sizes)
            ELSE 0
        END AS shortest_flow_pkt,

        CASE
            WHEN LEN(orig_pkt_sizes) > 0 AND LEN(resp_pkt_sizes) > 0
                THEN GREATEST(LIST_MAX(orig_pkt_sizes), LIST_MAX(resp_pkt_sizes))
            WHEN LEN(orig_pkt_sizes) > 0
                THEN LIST_MAX(orig_pkt_sizes)
            WHEN LEN(resp_pkt_sizes) > 0
                THEN LIST_MAX(resp_pkt_sizes)
            ELSE 0
        END AS logest_flow_pkt,

        CASE
            WHEN LEN(orig_pkt_sizes) > 0 AND LEN(resp_pkt_sizes) > 0
                THEN LEAST(LIST_MIN(orig_pkt_sizes), LIST_MIN(resp_pkt_sizes))
            WHEN LEN(orig_pkt_sizes) > 0
                THEN LIST_MIN(orig_pkt_sizes)
            WHEN LEN(resp_pkt_sizes) > 0
                THEN LIST_MIN(resp_pkt_sizes)
            ELSE 0
        END AS min_ip_pkt_len,

        CASE
            WHEN LEN(orig_pkt_sizes) > 0 AND LEN(resp_pkt_sizes) > 0
                THEN GREATEST(LIST_MAX(orig_pkt_sizes), LIST_MAX(resp_pkt_sizes))
            WHEN LEN(orig_pkt_sizes) > 0
                THEN LIST_MAX(orig_pkt_sizes)
            WHEN LEN(resp_pkt_sizes) > 0
                THEN LIST_MAX(resp_pkt_sizes)
            ELSE 0
        END AS max_ip_pkt_len,

        -- Throughput
        CASE WHEN duration > 0
            THEN (orig_bytes * 8.0) / duration
            ELSE 0
        END AS src_to_dst_avg_throughput,

        CASE WHEN duration > 0
            THEN (resp_bytes * 8.0) / duration
            ELSE 0
        END AS dst_to_src_avg_throughput,

        -- Packet size bucket histograms
        LIST_COUNT([x FOR x IN orig_pkt_sizes IF x <= 128])
        + LIST_COUNT([x FOR x IN resp_pkt_sizes IF x <= 128])
        AS num_pkts_up_to_128_bytes,

        LIST_COUNT([x FOR x IN orig_pkt_sizes IF x > 128 AND x <= 256])
        + LIST_COUNT([x FOR x IN resp_pkt_sizes IF x > 128 AND x <= 256])
        AS num_pkts_128_to_256_bytes,

        LIST_COUNT([x FOR x IN orig_pkt_sizes IF x > 256 AND x <= 512])
        + LIST_COUNT([x FOR x IN resp_pkt_sizes IF x > 256 AND x <= 512])
        AS num_pkts_256_to_512_bytes,

        LIST_COUNT([x FOR x IN orig_pkt_sizes IF x > 512 AND x <= 1024])
        + LIST_COUNT([x FOR x IN resp_pkt_sizes IF x > 512 AND x <= 1024])
        AS num_pkts_512_to_1024_bytes,

        LIST_COUNT([x FOR x IN orig_pkt_sizes IF x > 1024 AND x <= 1514])
        + LIST_COUNT([x FOR x IN resp_pkt_sizes IF x > 1024 AND x <= 1514])
        AS num_pkts_1024_to_1514_bytes,

        -- Time + Duration
        duration * 1000 AS flow_duration_milliseconds,
        duration * 1000 AS duration_in,
        duration * 1000 AS duration_out,
        ts * 1000 AS flow_start_milliseconds,
        (ts + duration) * 1000 AS flow_end_milliseconds,

        -- IAT stats
        CASE WHEN LEN(orig_pkt_times) > 1
            THEN LIST_MIN([orig_pkt_times[i+1] - orig_pkt_times[i] FOR i IN RANGE(LEN(orig_pkt_times) - 1)])
            ELSE 0
        END AS src_to_dst_iat_min,

        CASE WHEN LEN(orig_pkt_times) > 1
            THEN LIST_MAX([orig_pkt_times[i+1] - orig_pkt_times[i] FOR i IN RANGE(LEN(orig_pkt_times) - 1)])
            ELSE 0
        END AS src_to_dst_iat_max,

        CASE WHEN LEN(orig_pkt_times) > 1
            THEN LIST_AVG([orig_pkt_times[i+1] - orig_pkt_times[i] FOR i IN RANGE(LEN(orig_pkt_times) - 1)])
            ELSE 0
        END AS src_to_dst_iat_avg,

        CASE WHEN LEN(orig_pkt_times) > 1
            THEN (SELECT STDDEV_SAMP(x) FROM UNNEST([orig_pkt_times[i+1] - orig_pkt_times[i] FOR i IN RANGE(LEN(orig_pkt_times) - 1)]) AS t(x))
            ELSE 0
        END AS src_to_dst_iat_stddev,

        CASE WHEN LEN(resp_pkt_times) > 1
            THEN LIST_MIN([resp_pkt_times[i+1] - resp_pkt_times[i] FOR i IN RANGE(LEN(resp_pkt_times) - 1)])
            ELSE 0
        END AS dst_to_src_iat_min,

        CASE WHEN LEN(resp_pkt_times) > 1
            THEN LIST_MAX([resp_pkt_times[i+1] - resp_pkt_times[i] FOR i IN RANGE(LEN(resp_pkt_times) - 1)])
            ELSE 0
        END AS dst_to_src_iat_max,

        CASE WHEN LEN(resp_pkt_times) > 1
            THEN LIST_AVG([resp_pkt_times[i+1] - resp_pkt_times[i] FOR i IN RANGE(LEN(resp_pkt_times) - 1)])
            ELSE 0
        END AS dst_to_src_iat_avg,

        CASE WHEN LEN(resp_pkt_times) > 1
            THEN (SELECT STDDEV_SAMP(x) FROM UNNEST([resp_pkt_times[i+1] - resp_pkt_times[i] FOR i IN RANGE(LEN(resp_pkt_times) - 1)]) AS t(x))
            ELSE 0
        END AS dst_to_src_iat_stddev,

        -- TCP flags
        tcp_flags_orig + tcp_flags_resp AS tcp_flags,
        tcp_flags_orig AS client_tcp_flags,
        tcp_flags_resp AS server_tcp_flags,

        -- TTL calculations
        CASE
            WHEN LEN(orig_ttls) > 0 AND LEN(resp_ttls) > 0
                THEN LEAST(LIST_MIN(orig_ttls), LIST_MIN(resp_ttls))
            WHEN LEN(orig_ttls) > 0
                THEN LIST_MIN(orig_ttls)
            WHEN LEN(resp_ttls) > 0
                THEN LIST_MIN(resp_ttls)
            ELSE 0
        END AS min_ttl,

        CASE
            WHEN LEN(orig_ttls) > 0 AND LEN(resp_ttls) > 0
                THEN GREATEST(LIST_MAX(orig_ttls), LIST_MAX(resp_ttls))
            WHEN LEN(orig_ttls) > 0
                THEN LIST_MAX(orig_ttls)
            WHEN LEN(resp_ttls) > 0
                THEN LIST_MAX(resp_ttls)
            ELSE 0
        END AS max_ttl,

        -- TCP window
        tcp_win_max_orig AS tcp_win_max_in,
        tcp_win_max_resp AS tcp_win_max_out,

        -- Dummy ICMP features
        0 AS icmp_type,
        0 AS icmp_ipv4_type,

        -- DNS features
        dns_query_id,
        dns_query_type,
        dns_ttl_answer,

        -- FTP features
        ftp_reply_code AS ftp_command_ret_code,

        CURRENT_TIMESTAMP AS computed_at

    FROM unified_flows
    WHERE ts > :last_unified_flow_ts -- DELTA FILTER
) AS src
ON dest.uid = src.uid
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
RETURNING 1;
