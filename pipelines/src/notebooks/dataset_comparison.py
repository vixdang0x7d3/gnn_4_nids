import marimo

__generated_with = "0.18.0"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import duckdb
    import pyarrow as pa


@app.cell
def _():
    mo.md("""
    # Dataset Comparison: Zeek Logs vs UNSW-NB15

    Compare statistics to determine if more Zeek data collection is needed.
    """)
    return


@app.cell
def _():
    conn = duckdb.connect()
    return (conn,)


@app.cell
def _(conn):
    # Load UNSW-NB15 dataset
    conn.execute("""
        CREATE OR REPLACE TABLE nb15 AS
        SELECT * FROM 'data/nb15_sample/processed.parquet'
    """)
    return


@app.cell
def _(conn):
    # Load Zeek conn.log
    conn.execute("""
        CREATE OR REPLACE TABLE zeek_conn AS
        SELECT * FROM read_csv(
            'data/zeek_logs/test_logs/conn.log',
            delim='\t',
            comment='#',
            skip=8,
            header=false,
            auto_detect=false,
            nullstr='-',
            columns={
                'ts': 'DOUBLE',
                'uid': 'VARCHAR',
                'id_orig_h': 'VARCHAR',
                'id_orig_p': 'BIGINT',
                'id_resp_h': 'VARCHAR',
                'id_resp_p': 'BIGINT',
                'proto': 'VARCHAR',
                'service': 'VARCHAR',
                'duration': 'DOUBLE',
                'orig_bytes': 'BIGINT',
                'resp_bytes': 'BIGINT',
                'conn_state': 'VARCHAR',
                'local_orig': 'BOOLEAN',
                'local_resp': 'BOOLEAN',
                'missed_bytes': 'BIGINT',
                'history': 'VARCHAR',
                'orig_pkts': 'BIGINT',
                'orig_ip_bytes': 'BIGINT',
                'resp_pkts': 'BIGINT',
                'resp_ip_bytes': 'BIGINT',
                'tunnel_parents': 'VARCHAR'
            }
        )
    """)
    return


@app.cell
def _():
    mo.md("""
    ## Record Counts
    """)
    return


@app.cell
def _(conn, nb15, zeek_conn):
    _df = mo.sql(
        f"""
        SELECT
            'UNSW-NB15' as dataset,
            COUNT(*) as record_count
        FROM nb15
        UNION ALL
        SELECT
            'Zeek Logs' as dataset,
            COUNT(*) as record_count
        FROM zeek_conn
        """,
        engine=conn
    )
    return


@app.cell
def _():
    mo.md("""
    ## Protocol Distribution
    """)
    return


@app.cell
def _(conn, nb15, zeek_conn):
    _df = mo.sql(
        f"""
        WITH nb15_proto AS (
            SELECT proto, COUNT(*) as cnt FROM nb15 GROUP BY proto
        ),
        zeek_proto AS (
            SELECT proto, COUNT(*) as cnt FROM zeek_conn GROUP BY proto
        )
        SELECT
            COALESCE(n.proto, z.proto) as protocol,
            n.cnt as nb15_count,
            z.cnt as zeek_count,
            ROUND(n.cnt * 100.0 / SUM(n.cnt) OVER(), 2) as nb15_pct,
            ROUND(z.cnt * 100.0 / SUM(z.cnt) OVER(), 2) as zeek_pct
        FROM nb15_proto n
        FULL OUTER JOIN zeek_proto z ON n.proto = z.proto
        ORDER BY nb15_count DESC NULLS LAST
        """,
        engine=conn
    )
    return


@app.cell
def _():
    mo.md("""
    ## Service Distribution
    """)
    return


@app.cell
def _(conn, nb15, zeek_conn):
    _df = mo.sql(
        f"""
        WITH nb15_svc AS (
            SELECT service, COUNT(*) as cnt FROM nb15 GROUP BY service
        ),
        zeek_svc AS (
            SELECT COALESCE(service, 'unknown') as service, COUNT(*) as cnt
            FROM zeek_conn GROUP BY service
        )
        SELECT
            COALESCE(n.service, z.service) as service,
            n.cnt as nb15_count,
            z.cnt as zeek_count
        FROM nb15_svc n
        FULL OUTER JOIN zeek_svc z ON n.service = z.service
        ORDER BY nb15_count DESC NULLS LAST
        LIMIT 15
        """,
        engine=conn
    )
    return


@app.cell
def _():
    mo.md("""
    ## Numeric Feature Statistics - NB15
    """)
    return


@app.cell
def _(conn, nb15):
    _df = mo.sql(
        f"""
        SELECT
            'dur' as feature,
            MIN(dur) as min_val,
            ROUND(AVG(dur), 4) as mean_val,
            ROUND(MEDIAN(dur), 4) as median_val,
            MAX(dur) as max_val,
            ROUND(STDDEV(dur), 4) as std_val
        FROM nb15
        UNION ALL
        SELECT 'sbytes', MIN(sbytes), ROUND(AVG(sbytes), 4), ROUND(MEDIAN(sbytes), 4), MAX(sbytes), ROUND(STDDEV(sbytes), 4) FROM nb15
        UNION ALL
        SELECT 'dbytes', MIN(dbytes), ROUND(AVG(dbytes), 4), ROUND(MEDIAN(dbytes), 4), MAX(dbytes), ROUND(STDDEV(dbytes), 4) FROM nb15
        UNION ALL
        SELECT 'sttl', MIN(sttl), ROUND(AVG(sttl), 4), ROUND(MEDIAN(sttl), 4), MAX(sttl), ROUND(STDDEV(sttl), 4) FROM nb15
        UNION ALL
        SELECT 'spkts', MIN(spkts), ROUND(AVG(spkts), 4), ROUND(MEDIAN(spkts), 4), MAX(spkts), ROUND(STDDEV(spkts), 4) FROM nb15
        UNION ALL
        SELECT 'dpkts', MIN(dpkts), ROUND(AVG(dpkts), 4), ROUND(MEDIAN(dpkts), 4), MAX(dpkts), ROUND(STDDEV(dpkts), 4) FROM nb15
        UNION ALL
        SELECT 'sload', MIN(sload), ROUND(AVG(sload), 4), ROUND(MEDIAN(sload), 4), MAX(sload), ROUND(STDDEV(sload), 4) FROM nb15
        UNION ALL
        SELECT 'dload', MIN(dload), ROUND(AVG(dload), 4), ROUND(MEDIAN(dload), 4), MAX(dload), ROUND(STDDEV(dload), 4) FROM nb15
        """,
        engine=conn
    )
    return


@app.cell
def _():
    mo.md("""
    ## Numeric Feature Statistics - Zeek
    """)
    return


@app.cell
def _(conn, zeek_conn):
    _df = mo.sql(
        f"""
        SELECT
            'dur' as feature,
            MIN(duration) as min_val,
            ROUND(AVG(duration), 4) as mean_val,
            ROUND(MEDIAN(duration), 4) as median_val,
            MAX(duration) as max_val,
            ROUND(STDDEV(duration), 4) as std_val
        FROM zeek_conn
        UNION ALL
        SELECT 'sbytes', MIN(orig_bytes), ROUND(AVG(orig_bytes), 4), ROUND(MEDIAN(orig_bytes), 4), MAX(orig_bytes), ROUND(STDDEV(orig_bytes), 4) FROM zeek_conn
        UNION ALL
        SELECT 'dbytes', MIN(resp_bytes), ROUND(AVG(resp_bytes), 4), ROUND(MEDIAN(resp_bytes), 4), MAX(resp_bytes), ROUND(STDDEV(resp_bytes), 4) FROM zeek_conn
        UNION ALL
        SELECT 'spkts', MIN(orig_pkts), ROUND(AVG(orig_pkts), 4), ROUND(MEDIAN(orig_pkts), 4), MAX(orig_pkts), ROUND(STDDEV(orig_pkts), 4) FROM zeek_conn
        UNION ALL
        SELECT 'dpkts', MIN(resp_pkts), ROUND(AVG(resp_pkts), 4), ROUND(MEDIAN(resp_pkts), 4), MAX(resp_pkts), ROUND(STDDEV(resp_pkts), 4) FROM zeek_conn
        """,
        engine=conn
    )
    return


@app.cell
def _():
    mo.md("""
    ## NB15 Attack Distribution
    """)
    return


@app.cell
def _(conn, nb15):
    _df = mo.sql(
        f"""
        SELECT
            attack_cat,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct
        FROM nb15
        GROUP BY attack_cat
        ORDER BY count DESC
        """,
        engine=conn
    )
    return


@app.cell
def _():
    mo.md("""
    ## Data Collection Assessment

    Based on the comparison above, consider:

    1. **Volume**: How does Zeek record count compare to NB15?
    2. **Protocol coverage**: Are all NB15 protocols represented in Zeek data?
    3. **Service diversity**: Does Zeek capture similar service distribution?
    4. **Feature ranges**: Are numeric features in similar ranges?

    **Recommendation**: If Zeek data is significantly smaller or lacks diversity,
    you need to collect more data covering different attack scenarios.
    """)
    return


if __name__ == "__main__":
    app.run()
