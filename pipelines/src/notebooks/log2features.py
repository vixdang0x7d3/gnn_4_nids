import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")

with app.setup:
    import json
    import os
    import sys
    from pathlib import Path

    import duckdb
    import marimo as mo
    import pyarrow as pa

    # Add parent directory to path for sqlutils
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from sqlutils import SQL


@app.cell
def _():
    data_path = Path("/home/v/works/gnn_4_nids/pipelines/data/message_dump")
    sql_dir = Path("/home/v/works/gnn_4_nids/pipelines/sql")
    all_messages_file = data_path / "all_messages.json"
    duckdb_conn = duckdb.connect("/tmp/log2features.db")

    _ = os.system(f"ls -l {data_path}")
    return all_messages_file, data_path, duckdb_conn, sql_dir


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Data Ingestion
    Load test data from message_dump
    """)
    return


@app.cell
def _(all_messages_file, duckdb_conn, pa):
    # Parse all log types
    conn_cols = {
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

    pkt_cols = {
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

    dns_cols = {
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

    http_cols = {
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

    ftp_cols = {
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

    with open(all_messages_file, "r") as file_handle:
        for line in file_handle:
            msg = json.loads(line)
            if "conn" in msg:
                for k in conn_cols:
                    conn_cols[k].append(msg["conn"].get(k))
            elif "pkt-stats" in msg:
                for k in pkt_cols:
                    pkt_cols[k].append(msg["pkt-stats"].get(k))
            elif "dns" in msg:
                for k in dns_cols:
                    dns_cols[k].append(msg["dns"].get(k))
            elif "http" in msg:
                for k in http_cols:
                    http_cols[k].append(
                        msg["http"].get(
                            k,
                            []
                            if k in ["tags", "resp_fuids", "resp_mime_types"]
                            else None,
                        )
                    )
            elif "ftp" in msg:
                for k in ftp_cols:
                    ftp_cols[k].append(
                        msg["ftp"].get(
                            k.replace("_", ".") if "data_channel" in k else k
                        )
                    )

    for tbl in ["raw_conn", "raw_pkt_stats", "raw_dns", "raw_http", "raw_ftp"]:
        duckdb_conn.sql(f"DROP TABLE IF EXISTS {tbl}")

    duckdb_conn.from_arrow(pa.RecordBatch.from_pydict(conn_cols)).to_table("raw_conn")
    duckdb_conn.from_arrow(pa.RecordBatch.from_pydict(pkt_cols)).to_table(
        "raw_pkt_stats"
    )
    duckdb_conn.from_arrow(pa.RecordBatch.from_pydict(dns_cols)).to_table("raw_dns")
    duckdb_conn.from_arrow(pa.RecordBatch.from_pydict(http_cols)).to_table("raw_http")
    duckdb_conn.from_arrow(pa.RecordBatch.from_pydict(ftp_cols)).to_table("raw_ftp")

    print(
        f"✓ Loaded {len(conn_cols['uid'])} conn, {len(pkt_cols['uid'])} pkt-stats, {len(dns_cols['uid'])} dns, {len(http_cols['uid'])} http, {len(ftp_cols['uid'])} ftp logs"
    )
    return conn_cols, dns_cols, ftp_cols, http_cols, pkt_cols


@app.cell(hide_code=True)
def _():
    mo.md("# Test SQL Queries from Files")
    return


@app.cell
def _(SQL, sql_dir):
    # Load SQL using sqlutils
    create_unified_sql = SQL.from_file(sql_dir / "schema" / "create_unified_flows.sql")
    aggregate_unified_sql = SQL.from_file(
        sql_dir / "features" / "aggregate_unified_flows.sql"
    )
    aggregate_og_sql = SQL.from_file(sql_dir / "features" / "aggregate_og_features.sql")
    aggregate_nf_sql = SQL.from_file(sql_dir / "features" / "aggregate_nf_features.sql")
    create_og_sql = SQL.from_file(sql_dir / "schema" / "create_og_nb15_features.sql")
    create_nf_sql = SQL.from_file(sql_dir / "schema" / "create_nf_nb15_v3_features.sql")
    print("✓ Loaded all SQL files")
    return (
        aggregate_nf_sql,
        aggregate_og_sql,
        aggregate_unified_sql,
        create_nf_sql,
        create_og_sql,
        create_unified_sql,
    )


@app.cell(hide_code=True)
def _():
    mo.md("## Test 1: Create unified_flows Schema")
    return


@app.cell
def _(create_unified_sql, duckdb_conn):
    duckdb_conn.execute("DROP TABLE IF EXISTS unified_flows")
    duckdb_conn.execute(*create_unified_sql.duck)
    print("✓ unified_flows schema created")
    return


@app.cell(hide_code=True)
def _():
    mo.md("## Test 2: Aggregate Unified Flows")
    return


@app.cell
def _(aggregate_unified_sql, duckdb_conn):
    result_unified = duckdb_conn.execute(
        *aggregate_unified_sql(last_raw_log_ts=0).duck
    ).fetchall()
    inserted_unified = (
        result_unified[0][0] if result_unified and result_unified[0] else 0
    )
    unified_count = duckdb_conn.execute(
        "SELECT COUNT(*) FROM unified_flows"
    ).fetchone()[0]
    print(f"✓ Inserted {inserted_unified} rows, total: {unified_count}")

    sample_unified = duckdb_conn.execute("""
        SELECT uid, "id.orig_h", "id.orig_p", "id.resp_h", proto, service, duration
        FROM unified_flows LIMIT 5
    """).fetchall()
    print("Sample unified_flows:")
    for r1 in sample_unified:
        print(r1)
    return inserted_unified, result_unified, sample_unified, unified_count


@app.cell(hide_code=True)
def _():
    mo.md("## Test 3: Create og_features Schema")
    return


@app.cell
def _(create_og_sql, duckdb_conn):
    duckdb_conn.execute("DROP TABLE IF EXISTS og_features")
    duckdb_conn.execute(*create_og_sql.duck)
    print("✓ og_features schema created")
    return


@app.cell(hide_code=True)
def _():
    mo.md("## Test 4: Aggregate OG Features")
    return


@app.cell
def _(aggregate_og_sql, duckdb_conn):
    result_og = duckdb_conn.execute(
        *aggregate_og_sql(last_unified_flow_ts=0).duck
    ).fetchall()
    inserted_og = sum(r[0] for r in result_og) if result_og else 0
    og_count = duckdb_conn.execute("SELECT COUNT(*) FROM og_features").fetchone()[0]
    print(f"✓ Inserted {inserted_og} rows, total: {og_count}")

    sample_og = duckdb_conn.execute(
        "SELECT srcip, dstip, proto, state, dur, sbytes, dbytes FROM og_features LIMIT 5"
    ).fetchall()
    print("Sample og_features:")
    for r2 in sample_og:
        print(r2)
    return inserted_og, og_count, result_og, sample_og


@app.cell(hide_code=True)
def _():
    mo.md("## Test 5: Create nf_features Schema")
    return


@app.cell
def _(create_nf_sql, duckdb_conn):
    duckdb_conn.execute("DROP TABLE IF EXISTS nf_features")
    duckdb_conn.execute(*create_nf_sql.duck)
    print("✓ nf_features schema created")
    return


@app.cell(hide_code=True)
def _():
    mo.md("## Test 6: Aggregate NF Features")
    return


@app.cell
def _(aggregate_nf_sql, duckdb_conn):
    result_nf = duckdb_conn.execute(
        *aggregate_nf_sql(last_unified_flow_ts=0).duck
    ).fetchall()
    inserted_nf = sum(r[0] for r in result_nf) if result_nf else 0
    nf_count = duckdb_conn.execute("SELECT COUNT(*) FROM nf_features").fetchone()[0]
    print(f"✓ Inserted {inserted_nf} rows, total: {nf_count}")

    sample_nf = duckdb_conn.execute(
        "SELECT ipv4_src_addr, ipv4_dst_addr, protocol, in_bytes, out_bytes FROM nf_features LIMIT 5"
    ).fetchall()
    print("Sample nf_features:")
    for row in sample_nf:
        print(row)
    return inserted_nf, nf_count, result_nf, sample_nf


@app.cell(hide_code=True)
def _():
    mo.md("# Summary")
    return


@app.cell
def _(nf_count, og_count, unified_count):
    print("=" * 60)
    print(f"Unified flows:     {unified_count:,}")
    print(f"OG features:       {og_count:,}")
    print(f"NF features:       {nf_count:,}")
    print("=" * 60)
    print("✓ All SQL queries validated!")
    return


if __name__ == "__main__":
    app.run()
