import pyarrow as pa

# =====================================================
# CONN Schema
# =====================================================
CONN_ARROW_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.float64()),
        pa.field("uid", pa.string()),
        pa.field("id.orig_h", pa.string(), nullable=True),
        pa.field("id.orig_p", pa.int64(), nullable=True),
        pa.field("id.resp_h", pa.string(), nullable=True),
        pa.field("id.resp_p", pa.int64(), nullable=True),
        pa.field("proto", pa.string(), nullable=True),
        pa.field("service", pa.string(), nullable=True),
        pa.field("duration", pa.float64(), nullable=True),
        pa.field("orig_bytes", pa.int64(), nullable=True),
        pa.field("resp_bytes", pa.int64(), nullable=True),
        pa.field("conn_state", pa.string(), nullable=True),
        pa.field("local_orig", pa.bool_(), nullable=True),
        pa.field("local_resp", pa.bool_(), nullable=True),
        pa.field("missed_bytes", pa.int64(), nullable=True),
        pa.field("history", pa.string(), nullable=True),
        pa.field("orig_pkts", pa.int64(), nullable=True),
        pa.field("orig_ip_bytes", pa.int64(), nullable=True),
        pa.field("resp_pkts", pa.int64(), nullable=True),
        pa.field("resp_ip_bytes", pa.int64(), nullable=True),
        pa.field("ip_proto", pa.int64(), nullable=True),
        pa.field("tunnel_parent", pa.string(), nullable=True),
        pa.field("ingested_at", pa.timestamp("us"), nullable=False),
    ]
)

# =====================================================
# PKT_STATS Schema
# =====================================================
PKT_STATS_ARROW_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.float64()),
        pa.field("uid", pa.string()),
        pa.field("id.orig_h", pa.string(), nullable=True),
        pa.field("id.orig_p", pa.int64(), nullable=True),
        pa.field("id.resp_h", pa.string(), nullable=True),
        pa.field("id.resp_p", pa.int64(), nullable=True),
        pa.field("proto", pa.string(), nullable=True),
        pa.field("service", pa.string(), nullable=True),
        pa.field("orig_bytes", pa.int64(), nullable=True),
        pa.field("resp_bytes", pa.int64(), nullable=True),
        pa.field("tcp_rtt", pa.float64(), nullable=True),
        pa.field("tcp_flags_orig", pa.int64(), nullable=True),
        pa.field("tcp_flags_resp", pa.int64(), nullable=True),
        pa.field("tcp_win_max_orig", pa.int64(), nullable=True),
        pa.field("tcp_win_max_resp", pa.int64(), nullable=True),
        pa.field("retrans_orig_pkts", pa.int64(), nullable=True),
        pa.field("retrans_resp_pkts", pa.int64(), nullable=True),
        # Nested arrays - nullable
        pa.field("orig_pkt_times", pa.list_(pa.float64()), nullable=True),
        pa.field("resp_pkt_times", pa.list_(pa.float64()), nullable=True),
        pa.field("orig_pkt_sizes", pa.list_(pa.int64()), nullable=True),
        pa.field("resp_pkt_sizes", pa.list_(pa.int64()), nullable=True),
        pa.field("orig_ttls", pa.list_(pa.int64()), nullable=True),
        pa.field("resp_ttls", pa.list_(pa.int64()), nullable=True),
        pa.field("ingested_at", pa.timestamp("us"), nullable=False),
    ]
)

# =====================================================
# DNS Schema -
# =====================================================
DNS_ARROW_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.float64()),
        pa.field("uid", pa.string()),
        pa.field("id.orig_h", pa.string(), nullable=True),
        pa.field("id.orig_p", pa.int64(), nullable=True),
        pa.field("id.resp_h", pa.string(), nullable=True),
        pa.field("id.resp_p", pa.int64(), nullable=True),
        pa.field("proto", pa.string(), nullable=True),
        pa.field("trans_id", pa.int64(), nullable=True),
        pa.field("rtt", pa.float64(), nullable=True),
        pa.field("query", pa.string(), nullable=True),
        pa.field("qclass", pa.int64(), nullable=True),
        pa.field("qclass_name", pa.string(), nullable=True),
        pa.field("qtype", pa.int64(), nullable=True),
        pa.field("qtype_name", pa.string(), nullable=True),
        pa.field("rcode", pa.int64(), nullable=True),
        pa.field("rcode_name", pa.string(), nullable=True),
        pa.field("AA", pa.bool_(), nullable=True),
        pa.field("TC", pa.bool_(), nullable=True),
        pa.field("RD", pa.bool_(), nullable=True),
        pa.field("RA", pa.bool_(), nullable=True),
        pa.field("Z", pa.int64(), nullable=True),
        pa.field("answer", pa.list_(pa.string()), nullable=True),
        pa.field("TTLs", pa.list_(pa.int64()), nullable=True),
        pa.field("rejected", pa.bool_(), nullable=True),
        pa.field("ingested_at", pa.timestamp("us"), nullable=False),
    ]
)

# =====================================================
# HTTP Schema
# =====================================================
HTTP_ARROW_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.float64()),
        pa.field("uid", pa.string()),
        pa.field("id.orig_h", pa.string(), nullable=True),
        pa.field("id.orig_p", pa.int64(), nullable=True),
        pa.field("id.resp_h", pa.string(), nullable=True),
        pa.field("id.resp_p", pa.int64(), nullable=True),
        pa.field("trans_depth", pa.int64(), nullable=True),
        pa.field("method", pa.string(), nullable=True),
        pa.field("host", pa.string(), nullable=True),
        pa.field("uri", pa.string(), nullable=True),
        pa.field("referrer", pa.string(), nullable=True),
        pa.field("version", pa.string(), nullable=True),
        pa.field("user_agent", pa.string(), nullable=True),
        pa.field("origin", pa.string(), nullable=True),
        pa.field("request_body_len", pa.int64(), nullable=True),
        pa.field("response_body_len", pa.int64(), nullable=True),
        pa.field("status_code", pa.int64(), nullable=True),
        pa.field("status_msg", pa.string(), nullable=True),
        pa.field("tags", pa.list_(pa.string()), nullable=True),
        pa.field("resp_fuids", pa.list_(pa.string()), nullable=True),
        pa.field("resp_mime_types", pa.list_(pa.string()), nullable=True),
        pa.field("proxied", pa.list_(pa.string()), nullable=True),
        pa.field("ingested_at", pa.timestamp("us"), nullable=False),
    ]
)

# =====================================================
# SSL Schema
# =====================================================
SSL_ARROW_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.float64()),
        pa.field("uid", pa.string()),
        pa.field("id.orig_h", pa.string(), nullable=True),
        pa.field("id.orig_p", pa.int64(), nullable=True),
        pa.field("id.resp_h", pa.string(), nullable=True),
        pa.field("id.resp_p", pa.int64(), nullable=True),
        pa.field("version", pa.string(), nullable=True),
        pa.field("cipher", pa.string(), nullable=True),
        pa.field("curve", pa.string(), nullable=True),
        pa.field("server_name", pa.string(), nullable=True),
        pa.field("resumed", pa.bool_(), nullable=True),
        pa.field("next_protocol", pa.string(), nullable=True),
        pa.field("established", pa.bool_(), nullable=True),
        pa.field("last_alert", pa.string(), nullable=True),
        pa.field("cert_chain_fps", pa.list_(pa.string()), nullable=True),
        pa.field("client_cert_chain_fps", pa.list_(pa.string()), nullable=True),
        pa.field("subject", pa.string(), nullable=True),
        pa.field("issuer", pa.string(), nullable=True),
        pa.field("fingerprint", pa.string(), nullable=True),
        pa.field("validation_status", pa.string(), nullable=True),
        pa.field("ssl_history", pa.string(), nullable=True),
        pa.field("sni_matches_cert", pa.bool_(), nullable=True),
        pa.field("ingested_at", pa.timestamp("us"), nullable=False),
    ]
)

# =====================================================
# FTP Schema
# =====================================================
FTP_ARROW_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.float64()),
        pa.field("uid", pa.string()),
        pa.field("id.orig_h", pa.string(), nullable=True),
        pa.field("id.orig_p", pa.int64(), nullable=True),
        pa.field("id.resp_h", pa.string(), nullable=True),
        pa.field("id.resp_p", pa.int64(), nullable=True),
        pa.field("user", pa.string(), nullable=True),
        pa.field("password", pa.string(), nullable=True),
        pa.field("command", pa.string(), nullable=True),
        pa.field("reply_code", pa.int64(), nullable=True),
        pa.field("reply_msg", pa.string(), nullable=True),
        pa.field("data_channel_passive", pa.bool_(), nullable=True),
        pa.field("data_channel_orig_h", pa.string(), nullable=True),
        pa.field("data_channel_resp_h", pa.string(), nullable=True),
        pa.field("data_channel_resp_p", pa.int64(), nullable=True),
        pa.field("ingested_at", pa.timestamp("us"), nullable=False),
    ]
)
