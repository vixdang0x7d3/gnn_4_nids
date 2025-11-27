-- delta-based feature aggregation with merge
-- parameter: :last_stime
merge into og_features as dest
using (
    with base as (
        select
            c.uid, c.ts, c.id_orig_h, c.id_orig_p, c.id_resp_h, c.id_resp_p,
            c.proto, coalesce(c.service, 'unknown') as service,
            coalesce(c.duration, 0) as duration,
            coalesce(c.orig_bytes, 0) as orig_bytes,
            coalesce(c.resp_bytes, 0) as resp_bytes,
            c.conn_state, coalesce(c.orig_pkts, 0) as orig_pkts,
            coalesce(c.resp_pkts, 0) as resp_pkts,
            coalesce(u.tcp_rtt, 0) as tcp_rtt,
            coalesce(u.src_ttl, 0) as src_ttl,
            coalesce(u.dst_ttl, 0) as dst_ttl,
            coalesce(u.src_pkt_times, []) as src_pkt_times,
            coalesce(u.dst_pkt_times, []) as dst_pkt_times
        from raw_conn c
        left join raw_unsw_extra u on c.uid = u.uid
        where c.ts > :last_stime
    ),
    inter_packet_times as (
        select uid,
            case when len(src_pkt_times) > 1 then
                list_avg([src_pkt_times[i+1] - src_pkt_times[i] for i in range(len(src_pkt_times) - 1)])
            else 0 end as sintpkt,
            case when len(dst_pkt_times) > 1 then
                list_avg([dst_pkt_times[i+1] - dst_pkt_times[i] for i in range(len(dst_pkt_times) - 1)])
            else 0 end as dintpkt
        from base
    ),
    time_window as (
        select b1.uid, count(distinct b2.uid) as ct_dst_sport_ltm
        from base b1
        left join raw_conn b2 on
            b1.id_resp_h = b2.id_resp_h and b1.id_orig_p = b2.id_orig_p
            and b2.ts between (b1.ts - 100) and b1.ts and b1.uid != b2.uid
        group by b1.uid
    )
    select b.uid, b.id_orig_h as src_ip, b.id_orig_p as src_port,
        b.id_resp_h as dst_ip, b.id_resp_p as dst_port,
        case when b.proto not in ('tcp', 'udp') then 'other' else b.proto end as proto,
        case when b.service = 'dns' then b.service when b.service is null then 'unknown' else 'other' end as service,
        case when b.conn_state = 'sf' then 'fin'
             when b.conn_state in ('s1', 's2', 's3') then 'con'
             when b.conn_state in ('s0', 'oth') then 'int' else 'other' end as state,
        b.src_ttl as sttl, b.duration as dur,
        coalesce(ipt.sintpkt, 0) as sintpkt, coalesce(ipt.dintpkt, 0) as dintpkt,
        coalesce(tw.ct_dst_sport_ltm, 0) as ct_dst_sport_ltm, b.tcp_rtt as tcprtt,
        b.orig_bytes as sbytes, b.resp_bytes as dbytes,
        case when b.orig_pkts > 0 then b.orig_bytes::double / b.orig_pkts else 0 end as smeanz,
        case when b.resp_pkts > 0 then b.resp_bytes::double / b.resp_pkts else 0 end as dmeanz,
        case when b.duration > 0 then (b.orig_bytes * 8.0) / b.duration else 0 end as sload,
        case when b.duration > 0 then (b.resp_bytes * 8.0) / b.duration else 0 end as dload,
        b.orig_pkts as spkts, b.resp_pkts as dpkts,
        b.ts as stime, (b.ts + b.duration) as dtime,
        current_timestamp as computed_at
    from base b
    left join inter_packet_times ipt on b.uid = ipt.uid
    left join time_window tw on b.uid = tw.uid
) as src
on dest.uid = src.uid
when matched then update set computed_at = src.computed_at
when not matched then insert (
    uid, src_ip, src_port, dst_ip, dst_port, proto, service, state, sttl, dur,
    sintpkt, dintpkt, ct_dst_sport_ltm, tcprtt, sbytes, dbytes, smeanz, dmeanz,
    sload, dload, spkts, dpkts, stime, dtime, computed_at
) values (
    src.uid, src.src_ip, src.src_port, src.dst_ip, src.dst_port, src.proto,
    src.service, src.state, src.sttl, src.dur, src.sintpkt, src.dintpkt,
    src.ct_dst_sport_ltm, src.tcprtt, src.sbytes, src.dbytes, src.smeanz,
    src.dmeanz, src.sload, src.dload, src.spkts, src.dpkts, src.stime, src.dtime, src.computed_at
)
