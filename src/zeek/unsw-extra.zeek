@load base/protocols/conn

module UNSW;

export {
    redef enum Log::ID += { LOG };

    type Info: record {
        ts: time &log;
        uid: string &log;
        id: conn_id &log;

        # TCP RTT estimation
        tcp_rtt: interval &log &optional;

        # Packet timestampt for inter-packet time
        src_pkt_times: vector of time &log &optional;
        dst_pkt_times: vector of time &log &optional;

        # TTL values
        src_ttl: count &log &optional;
        dst_ttl: count &log &optional;

        # Packet size for mean calculation
        src_pkt_sizes: vector of count &log &optional;
        dst_pkt_sizes: vector of count &log &optional;
    };

    global unsw_extra: event(rec: Info);
}

# Track connection state
global conn_state: table[string] of Info;

event zeek_init() {
    Log::create_stream(UNSW::LOG, [$columns=Info, $ev=unsw_extra, $path="unsw-extra"]);
}

event new_connection(c: connection) {
    local info: Info;
    info$ts = network_time();
    info$uid = c$uid;
    info$id = c$id;
    info$src_pkt_times = vector();
    info$dst_pkt_times = vector();
    info$src_pkt_sizes = vector();
    info$dst_pkt_sizes = vector();

    conn_state[c$uid] = info;
}

event new_packet(c: connection, p: pkt_hdr) {
    if (c$uid !in conn_state)
        return;

    local info = conn_state[c$uid];

    # Capture TTL
    if (p?$ip) {
        if (p$ip$src == c$id$orig_h && !info?$src_ttl)
            info$src_ttl  = p$ip$ttl;
        if (p$ip$src == c$id$orig_h && !info?$dst_ttl)
            info$dst_ttl = p$ip$ttl;

        # Track packet timestamps and sizes
        if (p$ip$src == c$id$orig_h) {
            info$src_pkt_times += network_time();
            info$src_pkt_sizes += p$ip$len;
        } else { 
            info$dst_pkt_times += network_time();
            info$dst_pkt_sizes += p$ip$len;
        }
    }
}

# Estimate TCP RTT from SYN-SYNACK timing
event connection_established(c: connection) {
    if (c$uid !in conn_state) {
        return;
    }

    local info = conn_state[c$uid];

    # Simple RTT estimation: if we have both src and dst packets
    if (|info$src_pkt_times| > 0 && |info$dst_pkt_times| > 0) {
        # RTT approximates time between first SYN and first SYNACK
        info$tcp_rtt = info$dst_pkt_times[0] - info$src_pkt_times[0];
    }
}

event connection_state_remove(c: connection) {
    if (c$uid !in conn_state)
        return;
    
    Log::write(UNSW::LOG, conn_state[c$uid]);
    delete conn_state[c$uid];
}

