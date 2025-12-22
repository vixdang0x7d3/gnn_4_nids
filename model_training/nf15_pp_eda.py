import marimo

__generated_with = "0.18.0"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import marimo as mo
    import duckdb

    conn = duckdb.connect(":memory:")

    columns = {
        "FLOW_START_MILLISECONDS": "BIGINT",
        "FLOW_END_MILLISECONDS": "BIGINT",
        "IPV4_SRC_ADDR": "VARCHAR",
        "L4_SRC_PORT": "BIGINT",
        "IPV4_DST_ADDR": "VARCHAR",
        "L4_DST_PORT": "BIGINT",
        "PROTOCOL": "BIGINT",
        "L7_PROTO": "DOUBLE",
        "IN_BYTES": "BIGINT",
        "IN_PKTS": "BIGINT",
        "OUT_BYTES": "BIGINT",
        "OUT_PKTS": "BIGINT",
        "TCP_FLAGS": "BIGINT",
        "CLIENT_TCP_FLAGS": "BIGINT",
        "SERVER_TCP_FLAGS": "BIGINT",
        "FLOW_DURATION_MILLISECONDS": "BIGINT",
        "DURATION_IN": "BIGINT",
        "DURATION_OUT": "BIGINT",
        "MIN_TTL": "BIGINT",
        "MAX_TTL": "BIGINT",
        "LONGEST_FLOW_PKT": "BIGINT",
        "SHORTEST_FLOW_PKT": "BIGINT",
        "MIN_IP_PKT_LEN": "BIGINT",
        "MAX_IP_PKT_LEN": "BIGINT",
        "SRC_TO_DST_SECOND_BYTES": "DOUBLE",
        "DST_TO_SRC_SECOND_BYTES": "DOUBLE",
        "RETRANSMITTED_IN_BYTES": "BIGINT",
        "RETRANSMITTED_IN_PKTS": "BIGINT",
        "RETRANSMITTED_OUT_BYTES": "BIGINT",
        "RETRANSMITTED_OUT_PKTS": "BIGINT",
        "SRC_TO_DST_AVG_THROUGHPUT": "BIGINT",
        "DST_TO_SRC_AVG_THROUGHPUT": "BIGINT",
        "NUM_PKTS_UP_TO_128_BYTES": "BIGINT",
        "NUM_PKTS_128_TO_256_BYTES": "BIGINT",
        "NUM_PKTS_256_TO_512_BYTES": "BIGINT",
        "NUM_PKTS_512_TO_1024_BYTES": "BIGINT",
        "NUM_PKTS_1024_TO_1514_BYTES": "BIGINT",
        "TCP_WIN_MAX_IN": "BIGINT",
        "TCP_WIN_MAX_OUT": "BIGINT",
        "ICMP_TYPE": "BIGINT",
        "ICMP_IPV4_TYPE": "BIGINT",
        "DNS_QUERY_ID": "BIGINT",
        "DNS_QUERY_TYPE": "BIGINT",
        "DNS_TTL_ANSWER": "BIGINT",
        "FTP_COMMAND_RET_CODE": "BIGINT",
        "SRC_TO_DST_IAT_MIN": "BIGINT",
        "SRC_TO_DST_IAT_MAX": "BIGINT",
        "SRC_TO_DST_IAT_AVG": "BIGINT",
        "SRC_TO_DST_IAT_STDDEV": "BIGINT",
        "DST_TO_SRC_IAT_MIN": "BIGINT",
        "DST_TO_SRC_IAT_MAX": "BIGINT",
        "DST_TO_SRC_IAT_AVG": "BIGINT",
        "DST_TO_SRC_IAT_STDDEV": "BIGINT",
        "Label": "BIGINT",
        "Attack": "VARCHAR",
    }


@app.cell
def _():
    conn.sql("DROP TABLE IF EXISTS data")
    conn.read_csv(
        '/home/v/works/gnn_4_nids/model_training/data/nf_nb15_v3_csv/NF-UNSW-NB15-v3.csv', 
        columns=columns, 
    ).to_table("data")
    return


@app.cell
def _(data):
    _df = mo.sql(
        f"""
        SUMMARIZE data
        """,
        engine=conn
    )
    return


@app.cell
def _(data):
    _df = mo.sql(
        f"""
        SELECT COUNT(*) FROM data
        WHERE 
            SRC_TO_DST_SECOND_BYTES IS NULL
        	OR DST_TO_SRC_SECOND_BYTES IS NULL 
        """,
        engine=conn
    )
    return


if __name__ == "__main__":
    app.run()
