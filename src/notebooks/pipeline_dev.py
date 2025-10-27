import marimo

__generated_with = "0.17.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    import duckdb

    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.compute as pc
    return duckdb, pq


@app.cell
def _(duckdb):
    conn = duckdb.connect()
    return (conn,)


@app.cell
def _(conn, pq):
    data = pq.read_table('data/NB15_sample/raw/nb15_train.parquet')
    data

    feature_attrs = [
        "min_max_sttl",
        "min_max_dur",
        "min_max_dintpkt",
        "min_max_sintpkt",
        "min_max_ct_dst_sport_ltm",
        "min_max_tcprtt",
        "min_max_sbytes",
        "min_max_dbytes",
        "min_max_smeansz",
        "min_max_dmeansz",
        "min_max_sload",
        "min_max_dload",
        "min_max_spkts",
        "min_max_dpkts",
    ]

    feature_attrs_str = ", ".join(feature_attrs)

    edge_attrs = ["proto", "state", "service"]
    edge_attrs_str = ", ".join(edge_attrs)

    rel = conn.execute(f"""
    select row_number() over () - 1 as row_id, ?, ?, 
    from data
    """)
    return (rel,)


@app.cell
def _(rel):
    def load_graph_nodes(rel):
        total_rows = rel.aggregate('count(*)').fetchone()[0]

        x_batches = [] 
        y_batches = [] 


        return total_rows


    load_graph_nodes(rel)
    return


if __name__ == "__main__":
    app.run()
