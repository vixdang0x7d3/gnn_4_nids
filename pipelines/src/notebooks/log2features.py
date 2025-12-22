import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import json
    import os
    from pathlib import Path

    import duckdb
    import marimo as mo
    import pyarrow as pa


@app.cell
def _():
    data_path = Path("/home/v/works/gnn_4_nids/data_pipeline/data/message_dump")
    all_messages_file = data_path / "all_messages.json"
    duckdb_conn = duckdb.connect("/tmp/log2features.db")

    _ = os.system(f"ls -l {data_path}")
    return all_messages_file, duckdb_conn


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Data Ingestion
    """)
    return


@app.cell
def _(all_messages_file, duckdb_conn):
    # Parse all log types in a single pass through the file
    # Initialize column dictionaries for all log types

    conn_columns = {
        "ts": [],
        "uid": [],
        "id.orig_h": [],
        "id.orig_p": [],
        "id.resp_h": [],
        "id.resp_p": [],
        "proto": [],
        "service": [],
        "duration": [],
        "orig_bytes": [],
        "resp_bytes": [],
        "conn_state": [],
        "local_orig": [],
        "local_resp": [],
        "missed_bytes": [],
        "history": [],
        "orig_pkts": [],
        "orig_ip_bytes": [],
        "resp_pkts": [],
        "resp_ip_bytes": [],
        "ip_proto": [],
    }

    pkt_stats_columns = {
        "ts": [],
        "uid": [],
        "id.orig_h": [],
        "id.orig_p": [],
        "id.resp_h": [],
        "id.resp_p": [],
        "proto": [],
        "service": [],
        "orig_bytes": [],
        "resp_bytes": [],
        "orig_pkts": [],
        "resp_pkts": [],
        "tcp_rtt": [],
        "tcp_flags_orig": [],
        "tcp_flags_resp": [],
        "tcp_win_max_orig": [],
        "tcp_win_max_resp": [],
        "retrans_orig_pkts": [],
        "retrans_resp_pkts": [],
        "orig_pkt_times": [],
        "resp_pkt_times": [],
        "orig_pkt_sizes": [],
        "resp_pkt_sizes": [],
        "orig_ttls": [],
        "resp_ttls": [],
    }

    dns_columns = {
        "ts": [],
        "uid": [],
        "id.orig_h": [],
        "id.orig_p": [],
        "id.resp_h": [],
        "id.resp_p": [],
        "proto": [],
        "trans_id": [],
        "rtt": [],
        "query": [],
        "qclass": [],
        "qclass_name": [],
        "qtype": [],
        "qtype_name": [],
        "rcode": [],
        "rcode_name": [],
        "AA": [],
        "TC": [],
        "RD": [],
        "RA": [],
        "Z": [],
        "answers": [],
        "TTLs": [],
        "rejected": [],
    }

    http_columns = {
        "ts": [],
        "uid": [],
        "id.orig_h": [],
        "id.orig_p": [],
        "id.resp_h": [],
        "id.resp_p": [],
        "trans_depth": [],
        "method": [],
        "host": [],
        "uri": [],
        "referrer": [],
        "version": [],
        "user_agent": [],
        "origin": [],
        "request_body_len": [],
        "response_body_len": [],
        "status_code": [],
        "status_msg": [],
        "tags": [],
        "resp_fuids": [],
        "resp_mime_types": [],
    }

    ssl_columns = {
        "ts": [],
        "uid": [],
        "id.orig_h": [],
        "id.orig_p": [],
        "id.resp_h": [],
        "id.resp_p": [],
        "version": [],
        "cipher": [],
        "curve": [],
        "server_name": [],
        "resumed": [],
        "next_protocol": [],
        "established": [],
        "cert_chain_fuids": [],
        "client_cert_chain_fuids": [],
        "subject": [],
        "issuer": [],
        "validation_status": [],
    }

    ftp_columns = {
        "ts": [],
        "uid": [],
        "id.orig_h": [],
        "id.orig_p": [],
        "id.resp_h": [],
        "id.resp_p": [],
        "user": [],
        "password": [],
        "command": [],
        "reply_code": [],
        "reply_msg": [],
        "data_channel_passive": [],
        "data_channel_orig_h": [],
        "data_channel_resp_h": [],
        "data_channel_resp_p": [],
    }

    # Single pass through the file
    with open(all_messages_file, "r") as _f:
        for _line in _f:  # Memory-efficient iterator instead of readlines()
            _msg = json.loads(_line)

            if "conn" in _msg:
                c = _msg["conn"]
                conn_columns["ts"].append(c.get("ts"))
                conn_columns["uid"].append(c.get("uid"))
                conn_columns["id.orig_h"].append(c.get("id.orig_h"))
                conn_columns["id.orig_p"].append(c.get("id.orig_p"))
                conn_columns["id.resp_h"].append(c.get("id.resp_h"))
                conn_columns["id.resp_p"].append(c.get("id.resp_p"))
                conn_columns["proto"].append(c.get("proto"))
                conn_columns["service"].append(c.get("service"))
                conn_columns["duration"].append(c.get("duration"))
                conn_columns["orig_bytes"].append(c.get("orig_bytes"))
                conn_columns["resp_bytes"].append(c.get("resp_bytes"))
                conn_columns["conn_state"].append(c.get("conn_state"))
                conn_columns["local_orig"].append(c.get("local_orig"))
                conn_columns["local_resp"].append(c.get("local_resp"))
                conn_columns["missed_bytes"].append(c.get("missed_bytes"))
                conn_columns["history"].append(c.get("history"))
                conn_columns["orig_pkts"].append(c.get("orig_pkts"))
                conn_columns["orig_ip_bytes"].append(c.get("orig_ip_bytes"))
                conn_columns["resp_pkts"].append(c.get("resp_pkts"))
                conn_columns["resp_ip_bytes"].append(c.get("resp_ip_bytes"))
                conn_columns["ip_proto"].append(c.get("ip_proto"))

            elif "pkt-stats" in _msg:
                p = _msg["pkt-stats"]
                pkt_stats_columns["ts"].append(p.get("ts"))
                pkt_stats_columns["uid"].append(p.get("uid"))
                pkt_stats_columns["id.orig_h"].append(p.get("id.orig_h"))
                pkt_stats_columns["id.orig_p"].append(p.get("id.orig_p"))
                pkt_stats_columns["id.resp_h"].append(p.get("id.resp_h"))
                pkt_stats_columns["id.resp_p"].append(p.get("id.resp_p"))
                pkt_stats_columns["proto"].append(p.get("proto"))
                pkt_stats_columns["service"].append(p.get("service"))
                pkt_stats_columns["orig_bytes"].append(p.get("orig_bytes"))
                pkt_stats_columns["resp_bytes"].append(p.get("resp_bytes"))
                pkt_stats_columns["orig_pkts"].append(p.get("orig_pkts"))
                pkt_stats_columns["resp_pkts"].append(p.get("resp_pkts"))
                pkt_stats_columns["tcp_rtt"].append(p.get("tcp_rtt"))
                pkt_stats_columns["tcp_flags_orig"].append(p.get("tcp_flags_orig"))
                pkt_stats_columns["tcp_flags_resp"].append(p.get("tcp_flags_resp"))
                pkt_stats_columns["tcp_win_max_orig"].append(p.get("tcp_win_max_orig"))
                pkt_stats_columns["tcp_win_max_resp"].append(p.get("tcp_win_max_resp"))
                pkt_stats_columns["retrans_orig_pkts"].append(
                    p.get("retrans_orig_pkts")
                )
                pkt_stats_columns["retrans_resp_pkts"].append(
                    p.get("retrans_resp_pkts")
                )
                pkt_stats_columns["orig_pkt_times"].append(p.get("orig_pkt_times"))
                pkt_stats_columns["resp_pkt_times"].append(p.get("resp_pkt_times"))
                pkt_stats_columns["orig_pkt_sizes"].append(p.get("orig_pkt_sizes"))
                pkt_stats_columns["resp_pkt_sizes"].append(p.get("resp_pkt_sizes"))
                pkt_stats_columns["orig_ttls"].append(p.get("orig_ttls"))
                pkt_stats_columns["resp_ttls"].append(p.get("resp_ttls"))

            elif "dns" in _msg:
                d = _msg["dns"]
                dns_columns["ts"].append(d.get("ts"))
                dns_columns["uid"].append(d.get("uid"))
                dns_columns["id.orig_h"].append(d.get("id.orig_h"))
                dns_columns["id.orig_p"].append(d.get("id.orig_p"))
                dns_columns["id.resp_h"].append(d.get("id.resp_h"))
                dns_columns["id.resp_p"].append(d.get("id.resp_p"))
                dns_columns["proto"].append(d.get("proto"))
                dns_columns["trans_id"].append(d.get("trans_id"))
                dns_columns["rtt"].append(d.get("rtt"))
                dns_columns["query"].append(d.get("query"))
                dns_columns["qclass"].append(d.get("qclass"))
                dns_columns["qclass_name"].append(d.get("qclass_name"))
                dns_columns["qtype"].append(d.get("qtype"))
                dns_columns["qtype_name"].append(d.get("qtype_name"))
                dns_columns["rcode"].append(d.get("rcode"))
                dns_columns["rcode_name"].append(d.get("rcode_name"))
                dns_columns["AA"].append(d.get("AA"))
                dns_columns["TC"].append(d.get("TC"))
                dns_columns["RD"].append(d.get("RD"))
                dns_columns["RA"].append(d.get("RA"))
                dns_columns["Z"].append(d.get("Z"))
                dns_columns["answers"].append(d.get("answers"))
                dns_columns["TTLs"].append(d.get("TTLs"))
                dns_columns["rejected"].append(d.get("rejected"))

            elif "http" in _msg:
                h = _msg["http"]
                http_columns["ts"].append(h.get("ts"))
                http_columns["uid"].append(h.get("uid"))
                http_columns["id.orig_h"].append(h.get("id.orig_h"))
                http_columns["id.orig_p"].append(h.get("id.orig_p"))
                http_columns["id.resp_h"].append(h.get("id.resp_h"))
                http_columns["id.resp_p"].append(h.get("id.resp_p"))
                http_columns["trans_depth"].append(h.get("trans_depth"))
                http_columns["method"].append(h.get("method"))
                http_columns["host"].append(h.get("host"))
                http_columns["uri"].append(h.get("uri"))
                http_columns["referrer"].append(h.get("referrer"))
                http_columns["version"].append(h.get("version"))
                http_columns["user_agent"].append(h.get("user_agent"))
                http_columns["origin"].append(h.get("origin"))
                http_columns["request_body_len"].append(h.get("request_body_len"))
                http_columns["response_body_len"].append(h.get("response_body_len"))
                http_columns["status_code"].append(h.get("status_code"))
                http_columns["status_msg"].append(h.get("status_msg"))
                http_columns["tags"].append(h.get("tags", []))
                http_columns["resp_fuids"].append(h.get("resp_fuids", []))
                http_columns["resp_mime_types"].append(h.get("resp_mime_types", []))

            elif "ssl" in _msg:
                s = _msg["ssl"]
                ssl_columns["ts"].append(s.get("ts"))
                ssl_columns["uid"].append(s.get("uid"))
                ssl_columns["id.orig_h"].append(s.get("id.orig_h"))
                ssl_columns["id.orig_p"].append(s.get("id.orig_p"))
                ssl_columns["id.resp_h"].append(s.get("id.resp_h"))
                ssl_columns["id.resp_p"].append(s.get("id.resp_p"))
                ssl_columns["version"].append(s.get("version"))
                ssl_columns["cipher"].append(s.get("cipher"))
                ssl_columns["curve"].append(s.get("curve"))
                ssl_columns["server_name"].append(s.get("server_name"))
                ssl_columns["resumed"].append(s.get("resumed"))
                ssl_columns["next_protocol"].append(s.get("next_protocol"))
                ssl_columns["established"].append(s.get("established"))
                ssl_columns["cert_chain_fuids"].append(s.get("cert_chain_fuids", []))
                ssl_columns["client_cert_chain_fuids"].append(
                    s.get("client_cert_chain_fuids", [])
                )
                ssl_columns["subject"].append(s.get("subject"))
                ssl_columns["issuer"].append(s.get("issuer"))
                ssl_columns["validation_status"].append(s.get("validation_status"))

            elif "ftp" in _msg:
                f = _msg["ftp"]
                ftp_columns["ts"].append(f.get("ts"))
                ftp_columns["uid"].append(f.get("uid"))
                ftp_columns["id.orig_h"].append(f.get("id.orig_h"))
                ftp_columns["id.orig_p"].append(f.get("id.orig_p"))
                ftp_columns["id.resp_h"].append(f.get("id.resp_h"))
                ftp_columns["id.resp_p"].append(f.get("id.resp_p"))
                ftp_columns["user"].append(f.get("user"))
                ftp_columns["password"].append(f.get("password"))
                ftp_columns["command"].append(f.get("command"))
                ftp_columns["reply_code"].append(f.get("reply_code"))
                ftp_columns["reply_msg"].append(f.get("reply_msg"))
                ftp_columns["data_channel_passive"].append(
                    f.get("data_channel.passive")
                )
                ftp_columns["data_channel_orig_h"].append(f.get("data_channel.orig_h"))
                ftp_columns["data_channel_resp_h"].append(f.get("data_channel.resp_h"))
                ftp_columns["data_channel_resp_p"].append(f.get("data_channel.resp_p"))

    # Create all tables
    duckdb_conn.sql("drop table if exists raw_conn")
    duckdb_conn.sql("drop table if exists raw_pkt_stats")
    duckdb_conn.sql("drop table if exists raw_dns")
    duckdb_conn.sql("drop table if exists raw_http")
    duckdb_conn.sql("drop table if exists raw_ssl")
    duckdb_conn.sql("drop table if exists raw_ftp")

    duckdb_conn.from_arrow(pa.RecordBatch.from_pydict(conn_columns)).to_table(
        "raw_conn"
    )
    duckdb_conn.from_arrow(pa.RecordBatch.from_pydict(pkt_stats_columns)).to_table(
        "raw_pkt_stats"
    )
    duckdb_conn.from_arrow(pa.RecordBatch.from_pydict(dns_columns)).to_table("raw_dns")
    duckdb_conn.from_arrow(pa.RecordBatch.from_pydict(http_columns)).to_table(
        "raw_http"
    )
    duckdb_conn.from_arrow(pa.RecordBatch.from_pydict(ssl_columns)).to_table("raw_ssl")
    duckdb_conn.from_arrow(pa.RecordBatch.from_pydict(ftp_columns)).to_table("raw_ftp")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## This Block Is For Exploration Queries :-)
    """)
    return


@app.cell
def _(duckdb_conn, raw_dns):
    _df = mo.sql(
        f"""
        SUMMARIZE raw_dns
        """,
        engine=duckdb_conn
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Data Cleaning & Feature Aggregation
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    In this step, we aggregate logs and perform cleaning, transformation so we can get a unified feature view for down-stream model inferences.

    Main tables are: `raw_conn`, `raw_pkt_stats`.

    Expected output: Unified flows.
    - 5-tuples (`src_ip`, `dst_ip`, `src_port`, `dst_port`, `proto`)
    - timestamps (`start`, `end`)
    - bidirectional byte/packet counts
    - bidirectional packet lists $\rightarrow$ for down-stream stats
    - TCP state/flags
    - TTL values (min, max, per-direction)
    - retransmission counts
    - service/L7 protocol

    Data quality check:
    - `raw_conn` is pretty clean since it's the standard connection log.
    - `raw_pkt_stats` contains some empty lists, easy to handle

    Protocol Enrichment: Application logs (dns, http, ssl) are useful for analysis and post-detection enrichment. They are full of `null` and weird values so it'll be painful to process all three of them. So i'm gonna leave them as they are and handle them later.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Aggregate unified flows
    """)
    return


@app.cell
def _(duckdb_conn, raw_conn, raw_dns, raw_ftp, raw_http, raw_pkt_stats):
    _df = mo.sql(
        f"""
        create or replace table unified_flows as
        select
            -- Unique ID and timestamps
            c.ts,
            c.uid,

            -- 5-tuple
            c."id.orig_h" as srcip,
            c."id.orig_p" as sport,
            c."id.resp_h" as dstip,
            c."id.resp_p" as dsport,
            c.proto,

            -- Connection metadata
            c.conn_state,
            c.history,  -- TCP state history (e.g., 'ShADadFf' for normal connection)
            coalesce(c.service, 'unknown') as service,
            coalesce(c.duration, 0) as dur,
            c.local_orig,
            c.local_resp,

            -- Byte/packet counts from conn
            coalesce(c.orig_bytes, 0) as orig_bytes,
            coalesce(c.resp_bytes, 0) as resp_bytes,
            coalesce(c.orig_pkts, 0) as orig_pkts,
            coalesce(c.resp_pkts, 0) as resp_pkts,

            -- DPI packet statistics from raw_pkt_stats
            coalesce(s.tcp_rtt, 0) as tcp_rtt,
            coalesce(s.tcp_flags_orig, 0) as tcp_flags_orig,
            coalesce(s.tcp_flags_resp, 0) as tcp_flags_resp,
            coalesce(s.tcp_win_max_orig, 0) as tcp_win_max_orig,
            coalesce(s.tcp_win_max_resp, 0) as tcp_win_max_resp,
            coalesce(s.retrans_orig_pkts, 0) as retrans_orig_pkts,
            coalesce(s.retrans_resp_pkts, 0) as retrans_resp_pkts,
            coalesce(s.orig_pkt_times, []) as orig_pkt_times,
            coalesce(s.resp_pkt_times, []) as resp_pkt_times,
            coalesce(s.orig_pkt_sizes, []) as orig_pkt_sizes,
            coalesce(s.resp_pkt_sizes, []) as resp_pkt_sizes,
            coalesce(s.orig_ttls, []) as orig_ttls,
            coalesce(s.resp_ttls, []) as resp_ttls,

            -- HTTP enrichment (minimal - only fields needed for features)
            coalesce(h.trans_depth, 0) as http_trans_depth,
            coalesce(h.response_body_len, 0) as http_res_bdy_len,

            -- DNS enrichment (for NetFlow features)
            coalesce(d.trans_id, 0) as dns_query_id,
            coalesce(d.qtype, 0) as dns_query_type,
            case when len(coalesce(d."TTLs", [])) > 0 then d."TTLs"[1] else 0 end as dns_ttl_answer,

            -- FTP enrichment
            case when ftp.user is not null and ftp.password is not null then 1 else 0 end as ftp_is_login,
            ftp.command as ftp_command,
            coalesce(ftp.reply_code, 0) as ftp_reply_code,

            -- Debug flags (track which logs were joined)
            case when s.uid is not null then 1 else 0 end as has_pkt_stats,
            case when h.uid is not null then 1 else 0 end as has_http,
            case when d.uid is not null then 1 else 0 end as has_dns,
            case when ftp.uid is not null then 1 else 0 end as has_ftp

        from raw_conn c
        left join raw_pkt_stats s on c.uid = s.uid
        left join raw_http h on c.uid = h.uid
        left join raw_dns d on c.uid = d.uid
        left join raw_ftp ftp on c.uid = ftp.uid
        """,
        engine=duckdb_conn
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Generate Original UNSW-NB15 Features (47 features)
    """)
    return


@app.cell
def _(duckdb_conn):
    _df = mo.sql(
        f"""
        set threads to 1
        """,
        engine=duckdb_conn
    )
    return


@app.cell(disabled=True)
def _(duckdb_conn, unified_flows):
    # Original UNSW-NB15 Feature Set - Built from unified_flows
    _df = mo.sql(
        f"""
        create or replace table og_features as
        with features as (
            select
                uid, srcip, sport, dstip, dsport, proto,

                -- State normalization
                case
                    when conn_state = 'SF' then 'fin'
                    when conn_state in ('S1', 'S2', 'S3') then 'con'
                    when conn_state in ('S0', 'REJ') then 'int'
                    when conn_state in ('RSTO', 'RSTOS0', 'RSTR', 'RSTRH') then 'RST'
                    else 'other'
                end as state,
                dur,
                orig_bytes as sbytes,
                resp_bytes as dbytes,

            	-- first packet TTL value
                case when len(orig_ttls) > 0 then orig_ttls[1] else 0 end as sttl,
                case when len(resp_ttls) > 0 then resp_ttls[1] else 0 end as dttl,

                retrans_orig_pkts as sloss,
                retrans_resp_pkts as dloss,

            	-- Service normalization
                case
            		when service is null then 'other'
            		when service in ('http', 'dns') then service
            		else 'other'
            	end as service,


                case when dur > 0 then (orig_bytes * 8.0) / dur else 0 end as sload,
                case when dur > 0 then (resp_bytes * 8.0) / dur else 0 end as dload,

                orig_pkts as spkts,
                resp_pkts as dpkts,

                tcp_win_max_orig as swin,
                tcp_win_max_resp as dwin,

                0 as stcpb,  -- TCP base seq number not available in Zeek logs
                0 as dtcpb,  -- TCP base seq number not available in Zeek logs

                case when orig_pkts > 0 then orig_bytes / orig_pkts else 0 end as smeansz,
                case when resp_pkts > 0 then resp_bytes / resp_pkts else 0 end as dmeansz,

                http_trans_depth as trans_depth,

                http_res_bdy_len as res_bdy_len,

                case
            		when len(orig_pkt_times) > 1
            			then (
            				select stddev_samp(x)
            				from unnest([orig_pkt_times[i+1] - orig_pkt_times[i] for i in range(len(orig_pkt_times) - 1)])
            				as t(x)
            			)
            		else 0
            	end as sjit,

                case
            		when len(resp_pkt_times) > 1
            			then (
            				select stddev_samp(x)
            				from unnest([resp_pkt_times[i+1] - resp_pkt_times[i] for i in range(len(resp_pkt_times) - 1)])
            				as t(x)
            			)
            		else 0
            	end as djit,

                ts as stime,
                ts + dur as ltime,

                case
            		when len(orig_pkt_times) > 1
            			then list_avg([orig_pkt_times[i+1] - orig_pkt_times[i] for i in range(len(orig_pkt_times) - 1)])
            		else 0
            	end as sintpkt,

                case
            		when len(resp_pkt_times) > 1
            			then list_avg([resp_pkt_times[i+1] - resp_pkt_times[i] for i in range(len(resp_pkt_times) - 1)])
            		else 0
            	end as dintpkt,
                tcp_rtt as tcprtt,
                -- synack: time from SYN to SYN-ACK (approximated as half RTT if handshake completed)
                case when history like '%S%' and history like '%H%' and tcp_rtt > 0 then tcp_rtt / 2.0 else 0 end as synack,
                -- ackdat: time from SYN-ACK to ACK (approximated as half RTT if handshake completed)
                case when history like '%S%' and history like '%H%' and tcp_rtt > 0 then tcp_rtt / 2.0 else 0 end as ackdat,
                case when srcip = dstip and sport = dsport then 1 else 0 end as is_sm_ips_ports,
                ftp_is_login as is_ftp_login,
                ftp_command,
                conn_state
            from unified_flows using sample 10%
        ),
        time_window_features as (
            select f1.uid,
                count(distinct case when f2.dstip = f1.dstip and f2.state = f1.state and f2.sttl = f1.sttl then f2.uid end) as ct_state_ttl,
                count(distinct case when f2.dstip = f1.dstip and f2.service = 'http' then f2.uid end) as ct_flw_http_mthd,
                count(distinct case when f2.ftp_command is not null then f2.uid end) as ct_ftp_cmd,
                count(distinct case when f2.srcip = f1.srcip and f2.service = f1.service then f2.uid end) as ct_srv_src,
                count(distinct case when f2.dstip = f1.dstip and f2.service = f1.service then f2.uid end) as ct_srv_dst,
                count(distinct case when f2.dstip = f1.dstip then f2.uid end) as ct_dst_ltm,
                count(distinct case when f2.srcip = f1.srcip then f2.uid end) as ct_src_ltm,
                count(distinct case when f2.srcip = f1.srcip and f2.dsport = f1.dsport then f2.uid end) as ct_src_dport_ltm,
                count(distinct case when f2.dstip = f1.dstip and f2.sport = f1.sport then f2.uid end) as ct_dst_sport_ltm,
                count(distinct case when f2.srcip = f1.srcip and f2.dstip = f1.dstip then f2.uid end) as ct_dst_src_ltm
            from features f1
            left join features f2 on f2.stime between (f1.stime - 100) and f1.stime and f1.uid != f2.uid
            group by f1.uid
        )
        select f.srcip, f.sport, f.dstip, f.dsport, f.proto, f.state, f.dur,
            f.sbytes, f.dbytes, f.sttl, f.dttl, f.sloss, f.dloss, f.service,
            f.sload, f.dload, f.spkts, f.dpkts, f.swin, f.dwin, f.stcpb, f.dtcpb,
            f.smeansz, f.dmeansz, f.trans_depth, f.res_bdy_len, f.sjit, f.djit,
            f.stime, f.ltime, f.sintpkt, f.dintpkt, f.tcprtt, f.synack, f.ackdat,
            f.is_sm_ips_ports,
            coalesce(t.ct_state_ttl, 0) as ct_state_ttl,
            coalesce(t.ct_flw_http_mthd, 0) as ct_flw_http_mthd,
            f.is_ftp_login,
            coalesce(t.ct_ftp_cmd, 0) as ct_ftp_cmd,
            coalesce(t.ct_srv_src, 0) as ct_srv_src,
            coalesce(t.ct_srv_dst, 0) as ct_srv_dst,
            coalesce(t.ct_dst_ltm, 0) as ct_dst_ltm,
            coalesce(t.ct_src_ltm, 0) as ct_src_ltm,
            coalesce(t.ct_src_dport_ltm, 0) as ct_src_dport_ltm,
            coalesce(t.ct_dst_sport_ltm, 0) as ct_dst_sport_ltm,
            coalesce(t.ct_dst_src_ltm, 0) as ct_dst_src_ltm
        from features f
        left join time_window_features t on f.uid = t.uid
        """,
        engine=duckdb_conn
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Generate NetFlow UNSW-NB15-v3 Features (53 features)
    """)
    return


@app.cell
def _(duckdb_conn, unified_flows):
    # NetFlow-based UNSW-NB15-v3 Feature Set - Built from unified_flows
    _df = mo.sql(
        f"""
        create or replace table nf_v3_features as
        select
            srcip as ipv4_src_addr,
            sport as l4_src_port,
            dstip as ipv4_dst_addr,
            dsport as l4_dst_port,
            case when proto = 'tcp' then 6 when proto = 'udp' then 17 when proto = 'icmp' then 1 else 0 end as protocol,
            case when service = 'dns' then 53 when service = 'http' then 80 when service = 'ftp' then 21 else 0 end as l7_proto,
            orig_bytes as in_bytes,
            resp_bytes as out_bytes,
            orig_pkts as in_pkts,
            resp_pkts as out_pkts,
            dur * 1000 as flow_duration_milliseconds,
            tcp_flags_orig + tcp_flags_resp as tcp_flags,  -- Note: counts not bitmasks
            tcp_flags_orig as client_tcp_flags,  -- Note: count not bitmask
            tcp_flags_resp as server_tcp_flags,  -- Note: count not bitmask
            dur * 1000 as duration_in,
            dur * 1000 as duration_out,
            case
            	when len(orig_ttls) > 0 and len(resp_ttls) > 0 then least(list_min(orig_ttls), list_min(resp_ttls))
            	when len(orig_ttls) > 0 then list_min(orig_ttls)
            	when len(resp_ttls) > 0 then list_min(resp_ttls)
            	else 0
            end as min_ttl,

            case
            	when len(orig_ttls) > 0 and len(resp_ttls) > 0
            	then greatest(list_max(orig_ttls), list_max(resp_ttls))
            	when len(orig_ttls) > 0 then list_max(orig_ttls)
            	when len(resp_ttls) > 0 then list_max(resp_ttls)
            	else 0
            end as max_ttl,

            case
            	when len(orig_pkt_sizes) > 0 and len(resp_pkt_sizes) > 0 then greatest(list_max(orig_pkt_sizes), list_max(resp_pkt_sizes))
            	when len(orig_pkt_sizes) > 0 then list_max(orig_pkt_sizes)
            	when len(resp_pkt_sizes) > 0 then list_max(resp_pkt_sizes)
            	else 0
            end as longest_flow_pkt,

            case
            	when len(orig_pkt_sizes) > 0 and len(resp_pkt_sizes) > 0 then least(list_min(orig_pkt_sizes), list_min(resp_pkt_sizes))
            	when len(orig_pkt_sizes) > 0 then list_min(orig_pkt_sizes)
            	when len(resp_pkt_sizes) > 0 then list_min(resp_pkt_sizes)
            	else 0
            end as shortest_flow_pkt,

            case
            	when len(orig_pkt_sizes) > 0 and len(resp_pkt_sizes) > 0 then least(list_min(orig_pkt_sizes), list_min(resp_pkt_sizes))
            	when len(orig_pkt_sizes) > 0 then list_min(orig_pkt_sizes)
            	when len(resp_pkt_sizes) > 0 then list_min(resp_pkt_sizes)
            	else 0
            end as min_ip_pkt_len,

            case
            	when len(orig_pkt_sizes) > 0 and len(resp_pkt_sizes) > 0 then greatest(list_max(orig_pkt_sizes), list_max(resp_pkt_sizes))
            	when len(orig_pkt_sizes) > 0 then list_max(orig_pkt_sizes)
            	when len(resp_pkt_sizes) > 0 then list_max(resp_pkt_sizes)
            	else 0
            end as max_ip_pkt_len,

            case when dur > 0 then (orig_bytes * 1.0) / dur else 0 end as src_to_dst_second_bytes,
            case when dur > 0 then (resp_bytes * 1.0) / dur else 0 end as dst_to_src_second_bytes,

            case when orig_pkts > 0 then (orig_bytes::double / orig_pkts) * retrans_orig_pkts else 0 end as retransmitted_in_bytes,
            case when resp_pkts > 0 then (resp_bytes::double / resp_pkts) * retrans_resp_pkts else 0 end as retransmitted_out_bytes,

            retrans_orig_pkts as retransmitted_in_pkts,
            retrans_resp_pkts as retransmitted_out_pkts,

            case when dur > 0 then (orig_bytes * 8.0) / dur else 0 end as src_to_dst_avg_throughput,
            case when dur > 0 then (resp_bytes * 8.0) / dur else 0 end as dst_to_src_avg_throughput,

            list_count([x for x in orig_pkt_sizes if x <= 128])
            + list_count([x for x in resp_pkt_sizes if x <= 128]) as num_pkts_up_to_128_bytes,

            list_count([x for x in orig_pkt_sizes if x > 128 and x <= 256])
            + list_count([x for x in resp_pkt_sizes if x > 128 and x <= 256]) as num_pkts_128_to_256_bytes,

            list_count([x for x in orig_pkt_sizes if x > 256 and x <= 512])
            + list_count([x for x in resp_pkt_sizes if x > 256 and x <= 512]) as num_pkts_256_to_512_bytes,

            list_count([x for x in orig_pkt_sizes if x > 512 and x <= 1024])
            + list_count([x for x in resp_pkt_sizes if x > 512 and x <= 1024]) as num_pkts_512_to_1024_bytes,

            list_count([x for x in orig_pkt_sizes if x > 1024 and x <= 1514])
            + list_count([x for x in resp_pkt_sizes if x > 1024 and x <= 1514]) as num_pkts_1024_to_1514_bytes,

            tcp_win_max_orig as tcp_win_max_in,
            tcp_win_max_resp as tcp_win_max_out,

            0 as icmp_type,
            0 as icmp_ipv4_type,

            dns_query_id,
            dns_query_type,
            dns_ttl_answer,

            ftp_reply_code as ftp_command_ret_code,

            ts * 1000 as flow_start_milliseconds,
            (ts + dur) * 1000 as flow_end_milliseconds,

            case
            	when len(orig_pkt_times) > 1 then list_min([orig_pkt_times[i+1] - orig_pkt_times[i] for i in range(len(orig_pkt_times) - 1)])
            	else 0
            end as src_to_dst_iat_min,

            case
            	when len(orig_pkt_times) > 1 then list_max([orig_pkt_times[i+1] - orig_pkt_times[i] for i in range(len(orig_pkt_times) - 1)])
            	else 0
            end as src_to_dst_iat_max,

            case
            	when len(orig_pkt_times) > 1 then list_avg([orig_pkt_times[i+1] - orig_pkt_times[i] for i in range(len(orig_pkt_times) - 1)])
            	else 0
            end as src_to_dst_iat_avg,

            case
            	when len(orig_pkt_times) > 1 then (select stddev_samp(x) from unnest([orig_pkt_times[i+1] - orig_pkt_times[i] for i in range(len(orig_pkt_times) - 1)]) as t(x))
            	else 0
            end as src_to_dst_iat_stddev,
            case
            	when len(resp_pkt_times) > 1 then list_min([resp_pkt_times[i+1] - resp_pkt_times[i] for i in range(len(resp_pkt_times) - 1)])
            	else 0
            end as dst_to_src_iat_min,

            case
            	when len(resp_pkt_times) > 1 then list_max([resp_pkt_times[i+1] - resp_pkt_times[i] for i in range(len(resp_pkt_times) - 1)])
            	else 0
            end as dst_to_src_iat_max,

            case
            	when len(resp_pkt_times) > 1 then list_avg([resp_pkt_times[i+1] - resp_pkt_times[i] for i in range(len(resp_pkt_times) - 1)])
            	else 0
            end as dst_to_src_iat_avg,

            case
            	when len(resp_pkt_times) > 1 then (select stddev_samp(x) from unnest([resp_pkt_times[i+1] - resp_pkt_times[i] for i in range(len(resp_pkt_times) - 1)]) as t(x))
            	else 0 end as dst_to_src_iat_stddev
        from unified_flows
        """,
        engine=duckdb_conn
    )
    return


@app.cell
def _(duckdb_conn, nf_v3_features):
    _df = mo.sql(
        f"""
        COPY  (SELECT * FROM nf_v3_features) to './real_data.parquet' (FORMAT parquet)
        """,
        engine=duckdb_conn
    )
    return


if __name__ == "__main__":
    app.run()
