##! UNSW-NB15 Feature Extraction Script
##!
##! This script captures packet-level features needed for the NF-UNSW-NB15-v3 dataset
##! compatible with GNN-based Network Intrusion Detection Systems (NIDS).
##!
##! Author: Generated for CCSTGN compatibility
##! Compatible with: Zeek 5.x, 6.x, 7.x, 8.x
##!
##! Usage:
##!   zeek -r traffic.pcap unsw-nb15-features.zeek
##!   zeek -i eth0 unsw-nb15-features.zeek

@load base/protocols/conn
@load base/protocols/dns
@load base/protocols/ftp

module UNSW_NB15;

export {
    redef enum Log::ID += { LOG };

    type Info: record {
        ## Timestamp and identifiers
        ts: time &log;                              ##< Flow start time
        uid: string &log;                           ##< Unique flow identifier
        id: conn_id &log;                           ##< 5-tuple (src_ip, src_port, dst_ip, dst_port, proto)

        ## Basic flow information
        proto: string &log &optional;               ##< Protocol (tcp, udp, icmp)
        service: string &log &optional;             ##< Application layer service
        duration: interval &log &optional;          ##< Flow duration

        ## Bytes and packets counters
        orig_bytes: count &log &default=0;          ##< Originator bytes sent
        resp_bytes: count &log &default=0;          ##< Responder bytes sent
        orig_pkts: count &log &default=0;           ##< Originator packets sent
        resp_pkts: count &log &default=0;           ##< Responder packets sent

        ## TCP flags (cumulative OR of all TCP flags seen)
        tcp_flags_cumulative: count &log &default=0;  ##< All TCP flags (both directions)
        client_tcp_flags: count &log &default=0;      ##< Client-side TCP flags only
        server_tcp_flags: count &log &default=0;      ##< Server-side TCP flags only

        ## TCP window sizes
        tcp_win_max_in: count &log &default=0;        ##< Max TCP window (client -> server)
        tcp_win_max_out: count &log &default=0;       ##< Max TCP window (server -> client)

        ## TCP round-trip time estimation
        tcp_rtt: interval &log &optional;             ##< RTT estimated from SYN/SYN-ACK

        ## TTL values (tracks ALL packets, not just first)
        min_ttl: count &log &default=255;             ##< Minimum TTL observed
        max_ttl: count &log &default=0;               ##< Maximum TTL observed

        ## Retransmission estimates (from Zeek history parsing)
        retrans_in_pkts: count &log &default=0;       ##< Estimated retransmitted packets (orig)
        retrans_out_pkts: count &log &default=0;      ##< Estimated retransmitted packets (resp)

        ## Packet timestamps (for IAT calculation in post-processing)
        src_pkt_times: vector of time &log &optional;   ##< Originator packet timestamps
        dst_pkt_times: vector of time &log &optional;   ##< Responder packet timestamps

        ## Packet sizes (for statistics calculation in post-processing)
        src_pkt_sizes: vector of count &log &optional;  ##< Originator packet sizes
        dst_pkt_sizes: vector of count &log &optional;  ##< Responder packet sizes

        ## ICMP details
        icmp_type: count &log &optional;              ##< ICMP type
        icmp_code: count &log &optional;              ##< ICMP code

        ## DNS details
        dns_query_id: count &log &optional;           ##< DNS transaction ID
        dns_query_type: count &log &optional;         ##< DNS query type (numeric)
        dns_ttl_answer: count &log &optional;         ##< TTL of first A record

        ## FTP details
        ftp_return_code: count &log &optional;        ##< FTP response code
    };

    global unsw_nb15_features: event(rec: Info);
}

## Global state: track per-connection information
global conn_state: table[string] of Info;

event zeek_init()
    {
    Log::create_stream(UNSW_NB15::LOG, [$columns=Info, $ev=unsw_nb15_features, $path="unsw-nb15-features"]);
    }

event new_connection(c: connection)
    {
    local info: Info;
    info$ts = network_time();
    info$uid = c$uid;
    info$id = c$id;

    # Initialize vectors
    info$src_pkt_times = vector();
    info$dst_pkt_times = vector();
    info$src_pkt_sizes = vector();
    info$dst_pkt_sizes = vector();

    conn_state[c$uid] = info;
    }

event new_packet(c: connection, p: pkt_hdr)
    {
    if ( c$uid !in conn_state )
        return;

    local info = conn_state[c$uid];

    # Process IP-level information
    if ( p?$ip )
        {
        local pkt_len = p$ip$len;

        # Track TTL - update min/max across ALL packets
        if ( p$ip$ttl < info$min_ttl )
            info$min_ttl = p$ip$ttl;
        if ( p$ip$ttl > info$max_ttl )
            info$max_ttl = p$ip$ttl;

        # Determine packet direction and update counters
        if ( p$ip$src == c$id$orig_h )
            {
            # Originator -> Responder
            info$src_pkt_times += network_time();
            info$src_pkt_sizes += pkt_len;
            ++info$orig_pkts;
            info$orig_bytes += pkt_len;
            }
        else
            {
            # Responder -> Originator
            info$dst_pkt_times += network_time();
            info$dst_pkt_sizes += pkt_len;
            ++info$resp_pkts;
            info$resp_bytes += pkt_len;
            }
        }

    # Process TCP-specific information
    if ( p?$tcp && p?$ip )
        {
        local flags = p$tcp$flags;

        # Cumulative OR of all TCP flags
        info$tcp_flags_cumulative = info$tcp_flags_cumulative | flags;

        # Direction-specific TCP flags
        if ( p$ip$src == c$id$orig_h )
            {
            info$client_tcp_flags = info$client_tcp_flags | flags;

            # Track max TCP window (client -> server)
            if ( p$tcp$win > info$tcp_win_max_in )
                info$tcp_win_max_in = p$tcp$win;
            }
        else
            {
            info$server_tcp_flags = info$server_tcp_flags | flags;

            # Track max TCP window (server -> client)
            if ( p$tcp$win > info$tcp_win_max_out )
                info$tcp_win_max_out = p$tcp$win;
            }
        }

    # Process ICMP information (skip for now - field names vary by Zeek version)
    # if ( p?$icmp )
    #     {
    #     if ( !info?$icmp_type )
    #         {
    #         info$icmp_type = p$icmp$icmp_type;
    #         info$icmp_code = p$icmp$code;
    #         }
    #     }
    }

event connection_established(c: connection)
    {
    if ( c$uid !in conn_state )
        return;

    local info = conn_state[c$uid];

    # Estimate TCP RTT from SYN-SYNACK timing
    if ( |info$src_pkt_times| > 0 && |info$dst_pkt_times| > 0 )
        {
        info$tcp_rtt = info$dst_pkt_times[0] - info$src_pkt_times[0];
        }
    }

## Estimate retransmissions from Zeek connection history string
function estimate_retransmissions(c: connection, info: Info)
    {
    if ( !c?$history )
        return;

    local history = c$history;

    # Parse Zeek history string for retransmission indicators
    # 'h' = missed data (potential retransmission)
    # This is a heuristic approximation

    local orig_retrans = 0;
    local resp_retrans = 0;
    local len = |history|;

    local idx = 0;
    while ( idx < len )
        {
        local char = sub_bytes(history, idx, 1);

        # Uppercase = originator direction
        if ( char == "H" )
            ++orig_retrans;
        # Lowercase = responder direction
        else if ( char == "h" )
            ++resp_retrans;

        ++idx;
        }

    info$retrans_in_pkts = orig_retrans;
    info$retrans_out_pkts = resp_retrans;
    }

## DNS query tracking
event dns_request(c: connection, msg: dns_msg, query: string, qtype: count, qclass: count)
    {
    if ( c$uid !in conn_state )
        return;

    local info = conn_state[c$uid];
    info$dns_query_id = msg$id;
    info$dns_query_type = qtype;
    }

## DNS response tracking (TTL extraction disabled - field type varies by Zeek version)
# event dns_A_reply(c: connection, msg: dns_msg, ans: dns_answer, a: addr)
#     {
#     if ( c$uid !in conn_state )
#         return;
#
#     local info = conn_state[c$uid];
#
#     # Capture TTL of first A record only
#     if ( !info?$dns_ttl_answer && ans?$TTL )
#         info$dns_ttl_answer = ans$TTL;
#     }

## FTP response tracking
event ftp_reply(c: connection, code: count, msg: string, cont_resp: bool)
    {
    if ( c$uid !in conn_state )
        return;

    local info = conn_state[c$uid];

    # Capture first FTP return code only
    if ( !info?$ftp_return_code )
        info$ftp_return_code = code;
    }

## Finalize and write log entry when connection ends
event connection_state_remove(c: connection)
    {
    if ( c$uid !in conn_state )
        return;

    local info = conn_state[c$uid];

    # Add protocol and service information from connection record
    if ( c$conn?$proto )
        info$proto = cat(c$conn$proto);
    if ( |c$service| > 0 )
        {
        for ( s in c$service )
            {
            info$service = s;
            break;
            }
        }
    if ( c$conn?$duration )
        info$duration = c$conn$duration;

    # Estimate retransmissions from connection history
    estimate_retransmissions(c, info);

    # Write to log file
    Log::write(UNSW_NB15::LOG, info);

    # Clean up state
    delete conn_state[c$uid];
    }
