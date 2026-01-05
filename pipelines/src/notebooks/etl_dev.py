import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import marimo as mo
    import duckdb
    from data_pipeline.etl import ZeekDataPipeline


@app.cell
def _():
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin123"
    os.environ["DUCKDB_S3_ENDPOINT"] = "localhost:9000"
    return


@app.cell
def _():
    pipeline = ZeekDataPipeline(
        db_path="./data/etl_run_data/pipeline_data.db",
        archive_path="s3://pipeline-data",
        sql_dir="./sql",
        bootstrap_servers="localhost:9092",
        topic="zeek.dpi",
        group_id="zeek-consumer",
        feature_set="nf",
        loader_batch_size=122880,
        loader_max_age=5,
        aggregation_interval=5,
        slack=5,
        archive_interval=5,
        retention=3 * 3600,
        backup_interval=2*60,
        reserved=500_000,
    )
    return (pipeline,)


@app.cell
def _(pipeline):
    pipeline.run(
        duration=5 * 60,
    )
    return


@app.cell
def _():
    conn = duckdb.connect('./data/etl_run_data/pipeline_data.db')
    return (conn,)


@app.cell
def _(conn):
    _df = mo.sql(
        f"""
        show tables
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, nf_features):
    _df = mo.sql(
        f"""
        SELECT COUNT(*) FROM nf_features
        """,
        engine=conn
    )
    return


if __name__ == "__main__":
    app.run()
