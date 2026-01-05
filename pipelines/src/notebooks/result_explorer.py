import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # GNN-NIDS Inference Results Explorer

    Query predictions from S3 and join with original features for analysis.
    """)
    return


@app.cell
def _():
    import duckdb
    import polars as pl
    return duckdb, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## S3 Configuration
    """)
    return


@app.cell
def _():
    # S3/MinIO Configuration
    S3_ENDPOINT = "localhost:9000"
    S3_ACCESS_KEY = "admin"
    S3_SECRET_KEY = "minioadmin123"
    BUCKET = "pipeline-data"
    return BUCKET, S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY


@app.cell
def _(BUCKET, S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY, duckdb):
    def get_conn():
        """Create DuckDB connection with S3 configured."""
        conn = duckdb.connect()
        conn.execute(f"""
            INSTALL httpfs; LOAD httpfs;
            SET s3_endpoint='{S3_ENDPOINT}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
            SET s3_access_key_id='{S3_ACCESS_KEY}';
            SET s3_secret_access_key='{S3_SECRET_KEY}';
        """)
        return conn

    conn = get_conn()
    predictions_uri = f"s3://{BUCKET}/predictions/*.parquet"
    features_uri = f"s3://{BUCKET}/features/*.parquet"
    return conn, features_uri, get_conn, predictions_uri


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## List Available Files
    """)
    return


@app.cell
def _(BUCKET, S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY, mo, pl):
    # Use boto3 to list files with metadata
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{S3_ENDPOINT}",
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

    def list_s3_files(prefix: str) -> pl.DataFrame:
        """List files in S3 bucket with metadata."""
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        if "Contents" not in response:
            return pl.DataFrame()

        files = [
            {
                "key": obj["Key"],
                "size_mb": round(obj["Size"] / (1024 * 1024), 2),
                "last_modified": obj["LastModified"].isoformat(),
            }
            for obj in response["Contents"]
            if obj["Key"].endswith(".parquet")
        ]
        return pl.DataFrame(files).sort("last_modified", descending=True)

    pred_files = list_s3_files("predictions/")
    feature_files = list_s3_files("features/")

    mo.vstack(
        [
            mo.md("### Prediction Files"),
            mo.ui.table(pred_files, selection=None),
            mo.md("### Feature Files"),
            mo.ui.table(feature_files, selection=None),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Predictions Summary
    """)
    return


@app.cell
def _(conn, mo, predictions_uri):
    # Summary statistics using DuckDB
    summary = conn.execute(f"""
        SELECT
            COUNT(*) as total_records,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count,
            ROUND(AVG(CASE WHEN is_anomaly THEN 1.0 ELSE 0.0 END) * 100, 2) as anomaly_rate_pct,
            ROUND(AVG(anomaly_score), 4) as avg_score,
            ROUND(MAX(anomaly_score), 4) as max_score,
            SUM(CASE WHEN anomaly_score > 0.9 THEN 1 ELSE 0 END) as high_confidence,
            COUNT(DISTINCT source_file) as num_files
        FROM read_parquet('{predictions_uri}')
    """).pl()

    mo.ui.table(summary, selection=None)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Score Distribution
    """)
    return


@app.cell
def _(conn, mo, predictions_uri):
    # Score distribution
    score_dist = conn.execute(f"""
        SELECT
            CASE
                WHEN anomaly_score < 0.2 THEN '0.0-0.2'
                WHEN anomaly_score < 0.4 THEN '0.2-0.4'
                WHEN anomaly_score < 0.6 THEN '0.4-0.6'
                WHEN anomaly_score < 0.8 THEN '0.6-0.8'
                ELSE '0.8-1.0'
            END as score_bucket,
            COUNT(*) as count
        FROM read_parquet('{predictions_uri}')
        GROUP BY 1
        ORDER BY 1
    """).pl()

    # Simple bar chart with altair
    import altair as alt

    chart = (
        alt.Chart(score_dist)
        .mark_bar()
        .encode(
            x=alt.X("score_bucket:N", title="Score Bucket", sort=None),
            y=alt.Y("count:Q", title="Count"),
            color=alt.value("#4c78a8"),
        )
        .properties(width=500, height=300, title="Anomaly Score Distribution")
    )

    mo.vstack(
        [
            mo.ui.altair_chart(chart),
            mo.ui.table(score_dist, selection=None),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Top Anomalies
    """)
    return


@app.cell
def _(conn, mo, predictions_uri):
    # Top anomalies by score
    top_anomalies = conn.execute(f"""
        SELECT
            uid,
            ts,
            ROUND(anomaly_score, 4) as anomaly_score,
            prediction,
            source_file
        FROM read_parquet('{predictions_uri}')
        WHERE is_anomaly = true
        ORDER BY anomaly_score DESC
        LIMIT 50
    """).pl()

    mo.ui.table(top_anomalies, selection="multi", pagination=True)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Filter by Score Threshold
    """)
    return


@app.cell
def _(mo):
    threshold_slider = mo.ui.slider(
        start=0.0,
        stop=1.0,
        step=0.05,
        value=0.5,
        label="Minimum Anomaly Score",
        show_value=True,
    )
    threshold_slider
    return (threshold_slider,)


@app.cell
def _(get_conn, mo, predictions_uri, threshold_slider):
    # Filter by threshold (use fresh connection to avoid state issues)
    _conn = get_conn()
    filtered_df = _conn.execute(f"""
        SELECT uid, ts, ROUND(anomaly_score, 4) as anomaly_score, prediction, source_file
        FROM read_parquet('{predictions_uri}')
        WHERE anomaly_score >= {threshold_slider.value}
        ORDER BY anomaly_score DESC
        LIMIT 500
    """).pl()

    mo.vstack(
        [
            mo.md(
                f"**{len(filtered_df):,} records** with score ≥ {threshold_slider.value}"
            ),
            mo.ui.table(filtered_df, pagination=True, page_size=20),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Join Predictions with Features

    Join predictions back to original features for detailed investigation.
    """)
    return


@app.cell
def _(features_uri, get_conn, mo):
    # Join predictions with features on uid and ts
    _conn = get_conn()

    # First check what columns exist in features
    feature_cols = _conn.execute(f"""
        SELECT column_name
        FROM (DESCRIBE SELECT * FROM read_parquet('{features_uri}') LIMIT 1)
    """).fetchall()
    feature_col_names = [c[0] for c in feature_cols]

    mo.md(f"**Feature columns:** {len(feature_col_names)} columns available")
    return


@app.cell
def _(features_uri, get_conn, mo, predictions_uri):
    # Perform the join - get anomalies with their original features
    _conn = get_conn()

    joined_df = _conn.execute(f"""
        SELECT
            p.uid,
            p.ts,
            p.anomaly_score,
            p.is_anomaly,
            p.prediction,
            p.source_file as prediction_file,
            f.* EXCLUDE (uid, ts)
        FROM read_parquet('{predictions_uri}') p
        LEFT JOIN read_parquet('{features_uri}') f
            ON p.uid = f.uid AND p.ts = f.ts
        WHERE p.is_anomaly = true
        ORDER BY p.anomaly_score DESC
        LIMIT 100
    """).pl()

    mo.vstack(
        [
            mo.md(f"**Joined {len(joined_df):,} anomalies** with their features"),
            mo.ui.dataframe(joined_df),
        ]
    )
    return (joined_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Anomalies by Source File
    """)
    return


@app.cell
def _(conn, mo, predictions_uri):
    # Group by source file
    by_file = conn.execute(f"""
        SELECT
            source_file,
            COUNT(*) as total_records,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomalies,
            ROUND(AVG(anomaly_score), 4) as avg_score
        FROM read_parquet('{predictions_uri}')
        GROUP BY source_file
        ORDER BY anomalies DESC
    """).pl()

    mo.ui.table(by_file, selection=None)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Export Results
    """)
    return


@app.cell
def _(joined_df, mo):
    # Download anomalies with features as CSV
    csv_data = joined_df.write_csv()

    mo.download(
        csv_data.encode() if isinstance(csv_data, str) else csv_data,
        filename="anomalies_with_features.csv",
        label="📥 Download Anomalies CSV",
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Custom SQL Query

    Run your own query against predictions and features.
    """)
    return


@app.cell
def _(BUCKET, mo):
    default_query = f"""SELECT
    p.uid,
    p.ts,
    p.anomaly_score,
    p.is_anomaly
    FROM read_parquet('s3://{BUCKET}/predictions/*.parquet') p
    WHERE p.anomaly_score > 0.8
    LIMIT 20"""

    query_input = mo.ui.code_editor(
        value=default_query,
        language="sql",
        min_height=150,
    )
    query_input
    return (query_input,)


@app.cell
def _(get_conn, mo, query_input):
    # Execute custom query
    _conn = get_conn()
    try:
        custom_result = _conn.execute(query_input.value).pl()
        result_output = mo.vstack(
            [
                mo.md(f"**{len(custom_result):,} rows returned**"),
                mo.ui.table(custom_result, pagination=True),
            ]
        )
    except Exception as e:
        result_output = mo.callout(f"**Error:** {e}", kind="danger")

    result_output
    return


if __name__ == "__main__":
    app.run()
