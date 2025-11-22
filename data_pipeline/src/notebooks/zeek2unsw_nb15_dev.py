import marimo

__generated_with = "0.18.0"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import marimo as mo

    import duckdb
    import pyarrow as pa


@app.cell
def _():
    duckdb_conn = duckdb.connect()
    return (duckdb_conn,)


@app.cell
def _(duckdb_conn):
    _df = mo.sql(
        f"""
        create or replace table conn as
        SELECT * FROM read_csv(
            'data/zeek_logs/test_logs/conn.log',
            delim='\t',
            comment='#',
            encoding='utf-8',
            skip=8,
            header=false,
            auto_detect=false,
            nullstr='-',
            columns={{
            	'ts' : 'DOUBLE',
            	'uid' : 'VARCHAR',
            	'id_orig_h' : 'VARCHAR',
            	'id_orig_p' : 'BIGINT',
            	'id_resp_h' : 'VARCHAR',
            	'id_resp_p' : 'BIGINT',
            	'proto' : 'VARCHAR',
            	'service' : 'VARCHAR',
            	'duration' : 'DOUBLE',
            	'orig_bytes' : 'BIGINT',
            	'resp_bytes' : 'BIGINT',
        		'conn_state' : 'VARCHAR',
            	'local_orig' : 'BOOLEAN',
           		'local_resp' : 'BOOLEAN', 
        		'missed_bytes' : 'BIGINT',    
            	'history' : 'VARCHAR',
            	'orig_pkts' : 'BIGINT',
        		'orig_ip_bytes' : 'BIGINT',
            	'resp_pkts' : 'BIGINT',
           		'resp_ip_bytes' : 'BIGINT', 
            	'tunnel_parents' : 'VARCHAR',
            }}
        )
        """,
        engine=duckdb_conn
    )
    return


@app.cell
def _(conn, duckdb_conn):
    _df = mo.sql(
        f"""
        create or replace table stg_conn as
        select
        	ts,
        	uid,
        	id_orig_h,
        	id_orig_p,
        	id_resp_h,
        	id_resp_p,
        	proto,
        	service: coalesce(service, 'unknown'),
        	duration: coalesce(duration, 0),
        	orig_bytes: coalesce(orig_bytes, 0),
            resp_bytes: coalesce(resp_bytes, 0),
            conn_state,
            local_orig,
            local_resp,
            missed_bytes,
        	history: coalesce(history, ''),
        	orig_pkts,
            orig_ip_bytes,
            resp_pkts,
            resp_ip_bytes,
            tunnel_parents: coalesce(tunnel_parents, 'unknown')
        from conn
        """,
        engine=duckdb_conn
    )
    return


@app.cell
def _(duckdb_conn):
    _df = mo.sql(
        f"""
        create or replace table unsw_extra as
        select * from read_csv(
            'data/zeek_logs/test_logs/unsw-extra.log',
            delim='\t',
            auto_detect=false,
            comment='#',
            encoding='utf-8',
            header=false,
            nullstr='-',
            skip=8,
            columns={{
           		'ts' : 'DOUBLE',
            	'uid' : 'VARCHAR',
            	'id_orig_h' : 'VARCHAR',
            	'id_orig_p' : 'BIGINT',
            	'id_resp_h' : 'VARCHAR',
            	'id_resp_p' : 'BIGINT',
            	'tcp_rtt' : 'DOUBLE',
            	'src_pkt_times' : 'VARCHAR',
            	'dst_pkt_times' : 'VARCHAR',
            	'src_ttl' : 'BIGINT',
            	'dst_ttl' : 'BIGINT',
            	'src_pkt_sizes' : 'VARCHAR',
            	'dst_pkt_sizes' : 'VARCHAR'
            }}
        )
        """,
        engine=duckdb_conn
    )
    return


@app.cell
def _(duckdb_conn, unsw_extra):
    _df = mo.sql(
        f"""
        create or replace table stg_unsw_extra as
        select
            ts,
            uid,
            id_orig_h,
            id_orig_p,
            id_resp_h,
            id_resp_p,
            tcp_rtt: coalesce(tcp_rtt, 0),
            src_pkt_times: case
            	when src_pkt_times = '(empty)' then []
            	else list_transform(
            		str_split(src_pkt_times, ','),
            		x -> cast(x as double)
            	)
            end,
            dst_pkt_times: case
            	when dst_pkt_times = '(empty)' then []
            	else list_transform(
           			str_split(dst_pkt_times, ','),
            		x -> cast(x as double)
                )
            end,
           	src_ttl: coalesce(src_ttl, 0),
            dst_ttl: coalesce(dst_ttl, 0),
        	src_pkt_sizes: case
            	when src_pkt_sizes = '(empty)' then []
            	else list_transform(
           			str_split(src_pkt_times, ','),
            		x -> cast(x as double)
                )
            end,
            dst_pkt_sizes: case
            	when dst_pkt_sizes = '(empty)' then []
            	else list_transform(
           			str_split(dst_pkt_sizes, ','),
            		x -> cast(x as double)
                )
            end
        from unsw_extra
        """,
        engine=duckdb_conn
    )
    return


@app.cell
def _(duckdb_conn, stg_conn):
    _df = mo.sql(
        f"""
        from stg_conn
        """,
        engine=duckdb_conn
    )
    return


@app.cell
def _(duckdb_conn, stg_unsw_extra):
    _df = mo.sql(
        f"""
        from stg_unsw_extra
        """,
        engine=duckdb_conn
    )
    return


@app.cell
def _(duckdb_conn, stg_conn, stg_unsw_extra):
    _df = mo.sql(
        f"""
        with base as (
            select
            	c.*,
            	u.tcp_rtt,
            	u.src_ttl,
            	u.dst_ttl,
            	u.src_pkt_times,
            	u.dst_pkt_times,
            	u.src_pkt_sizes,
            	u.dst_pkt_sizes,
            from stg_conn c
            left join stg_unsw_extra u on c.uid = u.uid
        ),
        inter_packet_times as (
            select
        		uid,

            	-- Source inter-packet time (mean)
        		case
            		when len(src_pkt_times) > 1 then
            			list_avg([
           					src_pkt_times[i+1] - src_pkt_times[i]
        					for i In range(len(src_pkt_times) - 1)
                    	])
            		else 0
        		end as sintpkt,

                -- Destination inter-packet time (mean)
                case
                	when len(dst_pkt_times) > 1 then
                		list_avg([
                			dst_pkt_times[i+1] - dst_pkt_times[i]
                			for i in range(len(dst_pkt_times) - 1)
                        ])
                	else 0
            	end as dintpkt
        	from base 
        ),
        time_window as (
            select
            	b1.uid,
            	count(distinct b2.uid) as ct_dst_sport_ltm
            from base b1
            left join base b2 on
            	b1.id_resp_h = b2.id_resp_h
            	and b1.id_orig_p = b2.id_orig_p
            	and b2.ts between(b1.ts - 100) and b1.ts
            	and b1.uid != b2.uid
            group by b1.uid
        )
        select
        	b.uid,
        	b.id_orig_h as src_ip,
        	b.id_orig_p as src_port,
        	b.id_resp_h as dst_ip,
        	b.id_resp_p as dst_port,


            -- Edge features
        	case 
            	when b.proto not in ['tcp', 'udp'] then 'other'
            	else b.proto
            end as proto,
        	case 
            	when b.service = 'dns' then b.service
            	when b.service is NULL then 'unknown'
            	else 'other'
        	end as service,
            case
            	when b.conn_state = 'SF' then 'fin'
            	when b.conn_state in ['S1', 'S2', 'S3'] then 'con'
            	when b.conn_state in ['S0', 'OTH'] then 'int'
            	else 'other'
            end as state,

        	-- Basic features
        	coalesce(b.src_ttl, 0) as sttl,
        	coalesce(b.duration, 0) as dur,
        	coalesce(ipt.sintpkt, 0) as sintpkt,
        	coalesce(ipt.dintpkt, 0) as dintpkt,
        	coalesce(tw.ct_dst_sport_ltm, 0) as ct_dst_sport_ltm,
        	coalesce(b.tcp_rtt, 0) as tcprtt,

        	-- Byte counts
        	coalesce(b.orig_bytes, 0) as sbytes,
        	coalesce(b.resp_bytes, 0) as dbytes,

        	-- Mean packet sizes
        	case 
        		when b.orig_pkts > 0 then b.orig_bytes::double / b.orig_pkts
        		else 0
        	end as smeanz,
        	case
        		when b.resp_pkts > 0 then b.resp_bytes::double / b.resp_pkts
        		else 0
        	end as dmeanz,

        	-- Load (bits per second)
        	case
        		when b.duration > 0 then (b.orig_bytes * 8.0) / b.duration
        		else 0
        	end as sload,
        	case
        		when b.duration > 0 then (b.resp_bytes * 8.0) / b.duration
        		else 0
        	end as dload,

        	-- Packet counts
        	coalesce(b.orig_pkts, 0) as spkts,
        	coalesce(b.resp_pkts, 0) as dpkts,

        	b.ts as stime,
        	(b.ts+b.duration) as dtime,
        from base b
        left join inter_packet_times ipt on b.uid = ipt.uid
        left join time_window tw on b.uid = tw.uid
        """,
        engine=duckdb_conn
    )
    return


if __name__ == "__main__":
    app.run()
