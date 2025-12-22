# pkt_stats.zeek
# Custom packet statistics logger for DPI features
# Captures packet-level metrics for ETL feature engineering

module PKT_STATS;

export {
    redef enum Log::ID += { LOG };

    # Limit packet vector sizes to prevent Kafka message size issues
    const MAX_PKT_VECTOR_SIZE = 1000 &redef;

    type Info: record {
        # Identifiers
        ts: time &log;
        uid: string &log;
        id: conn_id &log;

        # Protocol metadata
        proto: string &log &optional;
        service: string &log &optional;
        duration: interval &log &optional;

        # TCP RTT estimation
        tcp_rtt: interval &log &optional;

        # Byte/packet counters
        orig_bytes: count &log &default=0;
        resp_bytes: count &log &default=0;
        orig_pkts: count &log &default=0;
        resp_pkts: count &log &default=0;

        # TCP flags (cumulative OR per direction)
        tcp_flags_orig: count &log &default=0;
        tcp_flags_resp: count &log &default=0;

        # TCP window sizes (track maximum)
        tcp_win_max_orig: count &log &default=0;
        tcp_win_max_resp: count &log &default=0;

        # Retransmission counts (from history)
        retrans_orig_pkts: count &log &default=0;
        retrans_resp_pkts: count &log &default=0;

        # Raw packet vectors for ETL processing
        orig_pkt_times: vector of time &log &optional;
        resp_pkt_times: vector of time &log &optional;
        orig_pkt_sizes: vector of count &log &optional;
        resp_pkt_sizes: vector of count &log &optional;
        orig_ttls: vector of count &log &optional;
        resp_ttls: vector of count &log &optional;
    };

    global log_pkt_stats: event(rec: Info);
}

# Connection-indexed state
global conn_state: table[string] of Info;

event zeek_init()
{
    Log::create_stream(LOG, [$columns=Info, $ev=log_pkt_stats, $path="pkt-stats"]);
}

# Initialize tracking for new connections
event new_connection(c: connection)
{
    local info: Info;
    info$ts = network_time();
    info$uid = c$uid;
    info$id = c$id;

    # Initialize vectors
    info$orig_pkt_times = vector();
    info$resp_pkt_times = vector();
    info$orig_pkt_sizes = vector();
    info$resp_pkt_sizes = vector();
    info$orig_ttls = vector();
    info$resp_ttls = vector();

    conn_state[c$uid] = info;
}

# HOT PATH: Per-packet data capture
event new_packet(c: connection, p: pkt_hdr)
{
    # Early return for untracked connections
    if (c$uid !in conn_state)
        return;

    local info = conn_state[c$uid];

    # IP-level processing
    if (p?$ip)
    {
        local is_orig = p$ip$src == c$id$orig_h;
        local pkt_len = p$ip$len;

        if (is_orig)
        {
            # Only add to vectors if under size limit
            if (|info$orig_pkt_times| < MAX_PKT_VECTOR_SIZE)
            {
                info$orig_pkt_times += network_time();
                info$orig_pkt_sizes += pkt_len;
                info$orig_ttls += p$ip$ttl;
            }
            ++info$orig_pkts;
            info$orig_bytes += pkt_len;
        }
        else
        {
            # Only add to vectors if under size limit
            if (|info$resp_pkt_times| < MAX_PKT_VECTOR_SIZE)
            {
                info$resp_pkt_times += network_time();
                info$resp_pkt_sizes += pkt_len;
                info$resp_ttls += p$ip$ttl;
            }
            ++info$resp_pkts;
            info$resp_bytes += pkt_len;
        }
    }

    # TCP-specific processing
    if (p?$tcp && p?$ip)
    {
        local flags = p$tcp$flags;
        local tcp_is_orig = p$ip$src == c$id$orig_h;

        if (tcp_is_orig)
        {
            info$tcp_flags_orig = info$tcp_flags_orig | flags;
            if (p$tcp$win > info$tcp_win_max_orig)
                info$tcp_win_max_orig = p$tcp$win;
        }
        else
        {
            info$tcp_flags_resp = info$tcp_flags_resp | flags;
            if (p$tcp$win > info$tcp_win_max_resp)
                info$tcp_win_max_resp = p$tcp$win;
        }
    }
}

# Estimate TCP RTT from SYN-SYNACK timing
# Use low priority to ensure new_packet events have fired first
event connection_established(c: connection) &priority=-5
{
    if (c$uid !in conn_state)
        return;

    local info = conn_state[c$uid];

    # Simple RTT estimation: if we have both orig and resp packets
    if (|info$orig_pkt_times| > 0 && |info$resp_pkt_times| > 0)
    {
        # RTT approximates time between first SYN and first SYNACK
        info$tcp_rtt = info$resp_pkt_times[0] - info$orig_pkt_times[0];
    }
}

# Estimate retransmissions from Zeek connection history string
function estimate_retransmissions(c: connection, info: Info)
{
    if (!c?$history)
        return;

    local history = c$history;
    local len = |history|;
    local idx = 0;

    while (idx < len)
    {
        local char = sub_bytes(history, idx, 1);

        # 'H' = packet seen multiple times (retransmission)
        # Uppercase = originator direction
        if (char == "H")
            ++info$retrans_orig_pkts;
        # Lowercase = responder direction
        else if (char == "h")
            ++info$retrans_resp_pkts;

        ++idx;
    }
}

# Finalize and log when connection closes
event connection_state_remove(c: connection)
{
    if (c$uid !in conn_state)
        return;

    local info = conn_state[c$uid];

    # Populate protocol and service fields
    if (c?$conn)
    {
        if (c$conn?$proto)
            info$proto = cat(c$conn$proto);
        if (c$conn?$service)
            info$service = c$conn$service;
        if (c$conn?$duration)
            info$duration = c$conn$duration;
    }

    # Estimate retransmissions
    estimate_retransmissions(c, info);

    # Ensure tcp_rtt is always set (even if to 0.0 for non-TCP or unestablished connections)
    if (!info?$tcp_rtt)
        info$tcp_rtt = 0.0 secs;

    # Write log
    Log::write(LOG, info);

    # Cleanup
    delete conn_state[c$uid];
}
