"""
Alerting Pipeline - Asset Consumer

Triggered by PREDICTIONS_ASSET when inference completes.
Sends text email alerts if anomaly threshold exceeded.

Schedule: Asset-driven (triggered by PREDICTIONS_ASSET)
"""

import sys
from datetime import datetime, timedelta

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.sdk import DAG, task

sys.path.insert(0, "/opt/airflow/dags")

from assets import (
    ALERT_THRESHOLD,
    BUCKET_NAME,
    PREDICTION_ASSET,
    S3_CONFIG,
)

# Email Configuration
EMAIL_CONN_ID = "smtp_default"
DEFAULT_ALERT_RECIPIENTS = [
    "n21dcat065@student.ptithcm.edu.vn",
    "n21dccn091@student.ptithcm.edu.vn",
]


def get_alert_recipients() -> list[str]:
    """Get alert email recipients from Variable or default."""
    try:
        recipients = Variable.get("alert_email_recipients", default_var=None)
        if recipients:
            return [r.strip() for r in recipients.split(",")]
    except Exception:
        pass
    return DEFAULT_ALERT_RECIPIENTS


def build_alert_text(analysis: dict) -> str:
    """Build plain text email body."""
    severity = (
        "CRITICAL" if analysis["anomaly_count"] >= ALERT_THRESHOLD * 2 else "WARNING"
    )

    return f"""
GNN-NIDS {severity} ALERT
{"=" * 50}

SUMMARY
-------
Anomalies Detected: {analysis["anomaly_count"]:,}
Anomaly Rate: {analysis["anomaly_rate"]:.2%}
Total Records: {analysis["total_records"]:,}
High Confidence (>0.9): {analysis["high_confidence_count"]:,}

THRESHOLD
---------
Current: {analysis["anomaly_count"]} anomalies
Threshold: {ALERT_THRESHOLD} anomalies
Status: {severity}

SCORE STATISTICS
----------------
Mean: {analysis["score_mean"]:.4f}
Max: {analysis["score_max"]:.4f}

SOURCE
------
File: {analysis["source_file"]}
Time: {analysis["analysis_time"]}

{"=" * 50}
GNN-NIDS Alerting Pipeline
"""


default_args = {
    "owner": "gnn-nids",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
}

with DAG(
    dag_id="alert_anomaly",
    description="Analyze predictions and send email alerts",
    schedule=[PREDICTION_ASSET],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    @task()
    def analyze_and_alert(*, inlet_events):
        """Load predictions, analyze, and send alert if threshold exceeded."""
        import boto3
        import duckdb
        import numpy as np

        # Get trigger metadata
        try:
            events = list(inlet_events[PREDICTION_ASSET])
            if events:
                metadata = events[-1].extra or {}
                print(f"Triggered by event: {metadata}")
        except (KeyError, TypeError):
            print("No inlet events (manual trigger)")

        # Configure DuckDB for S3
        duckdb_s3_endpoint = S3_CONFIG["endpoint_url"].split("//")[1]
        conn = duckdb.connect()
        conn.install_extension("httpfs")
        conn.load_extension("httpfs")
        conn.execute(f"""
            SET s3_url_style='path';
            SET s3_use_ssl=false;
            SET s3_endpoint='{duckdb_s3_endpoint}';
            SET s3_access_key_id='{S3_CONFIG["aws_access_key_id"]}';
            SET s3_secret_access_key='{S3_CONFIG["aws_secret_access_key"]}';
        """)

        # Find latest predictions file
        s3_client = boto3.client("s3", **S3_CONFIG)  # type: ignore
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="predictions/")

        if "Contents" not in response:
            raise AirflowSkipException("No prediction files found")

        files = [obj for obj in response["Contents"] if obj["Key"].endswith(".parquet")]
        if not files:
            raise AirflowSkipException("No prediction parquet files found")

        files.sort(key=lambda x: x["LastModified"], reverse=True)
        latest_file = files[0]["Key"]
        s3_uri = f"s3://{BUCKET_NAME}/{latest_file}"

        print(f"Analyzing: {latest_file}")

        # Load and analyze via DuckDB
        df = conn.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count,
                AVG(anomaly_score) as score_mean,
                MAX(anomaly_score) as score_max,
                SUM(CASE WHEN anomaly_score > 0.9 THEN 1 ELSE 0 END) as high_confidence
            FROM read_parquet('{s3_uri}')
        """).fetchone()
        conn.close()

        total_records = int(df[0]) if df else 0
        anomaly_count = int(df[1]) if df else 0
        score_mean = float(df[2]) if df else 0.0
        score_max = float(df[3]) if df else 0.0
        high_confidence = int(df[4]) if df else 0

        # Sanitize NaN
        score_mean = 0.0 if np.isnan(score_mean) else score_mean
        score_max = 0.0 if np.isnan(score_max) else score_max

        anomaly_rate = anomaly_count / total_records if total_records > 0 else 0.0

        analysis = {
            "total_records": total_records,
            "anomaly_count": anomaly_count,
            "anomaly_rate": anomaly_rate,
            "score_mean": score_mean,
            "score_max": score_max,
            "high_confidence_count": high_confidence,
            "source_file": latest_file,
            "analysis_time": datetime.now().isoformat(),
        }

        print(f"\n{'=' * 60}")
        print("ANALYSIS RESULTS")
        print(f"{'=' * 60}")
        print(f"Total Records: {total_records:,}")
        print(f"Anomalies: {anomaly_count:,} ({anomaly_rate:.2%})")
        print(f"High Confidence: {high_confidence}")
        print(f"Alert Threshold: {ALERT_THRESHOLD}")
        print(f"{'=' * 60}\n")

        # Send alert if threshold exceeded
        if anomaly_count >= ALERT_THRESHOLD:
            severity = "CRITICAL" if anomaly_count >= ALERT_THRESHOLD * 2 else "WARNING"
            recipients = get_alert_recipients()
            subject = f"[{severity}] GNN-NIDS Alert: {anomaly_count} anomalies detected"
            body = build_alert_text(analysis)

            try:
                smtp_hook = SmtpHook(smtp_conn_id=EMAIL_CONN_ID)
                smtp_hook.get_conn()
                smtp_hook.send_email_smtp(
                    to=recipients,
                    subject=subject,
                    html_content=f"<pre>{body}</pre>",  # Wrap in pre for formatting
                )

                print(f"{'=' * 60}")
                print(f"{'📧 ALERT EMAIL SENT'}")
                print(f"{'=' * 60}")
                print(f"Recipients: {', '.join(recipients)}")
                print(f"Subject: {subject}")
                print(f"{'=' * 60}\n")

                analysis["email_sent"] = True
                analysis["email_recipients"] = recipients

            except Exception as e:
                print(f"Failed to send email: {e}")
                analysis["email_sent"] = False
                analysis["email_error"] = str(e)
        else:
            print(
                f"Below threshold ({anomaly_count} < {ALERT_THRESHOLD}), no alert sent"
            )
            analysis["email_sent"] = False
            analysis["email_reason"] = "below_threshold"

        return analysis

    # Single task DAG
    analyze_and_alert()
