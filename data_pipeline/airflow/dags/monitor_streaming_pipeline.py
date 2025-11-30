"""
Monitor the streaming ETL pipeline

Checks:
- Kafka consumer group status
- DuckDB table row counts
- Feature computation lag

Schedule: Every 5 minutes
"""

import sys
from datetime import datetime, timedelta

from airflow.sdk import dag, task

sys.path.insert(0, "/opt/airflow/data_pipeline/src")

import duckdb
from confluent_kafka.admin import AdminClient

default_args = {
    "owner": "gnn-nids",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
}


@dag(
    dag_id="monitor_streaming_pipeline",
    description="Monitor the streaming ETL pipeline",
    schedule=timedelta(minutes=5),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["monitoring", "etl", "health-check"],
    default_args=default_args,
)
def monitor_etl():
    """DAG for monitoring the streaming ETL pipeline"""

    @task()
    def check_kafka_consumer_lag():
        """Check Kafka consumer group health."""
        admin_client = AdminClient({"bootstrap.servers": "broker:29092"})

        groups = admin_client.list_consumer_groups().groups
        group_list = [g for g in groups.valid]

        metrics = {
            "consumer_groups_count": len(group_list),
            "topic": "zeek.logs",
            "timestamp": datetime.now().isoformat(),
            "status": "healthy" if len(group_list) > 0 else "warning",
        }

        print(f"Kafka Health: {metrics}")
        return metrics

    @task()
    def check_duckdb_health():
        """Check DuckDB table statistics and feature lag."""
        conn = duckdb.connect("/data/zeek_features.db", read_only=True)

        try:
            # Count rows in each table
            _result = conn.execute("SELECT COUNT(*) FROM raw_conn").fetchone()
            raw_conn_count = _result[0] if _result else 0

            _result = conn.execute("SELECT COUNT(*) FROM raw_unsw_extra").fetchone()
            raw_unsw_extra_count = _result[0] if _result else 0

            _result = conn.execute("SELECT COUNT(*) FROM og_nb15_features").fetchone()
            features_count = _result[0] if _result else 0

            # Get latest feature timestamp
            _result = conn.execute("SELECT MAX(stime) FROM og_nb15_features").fetchone()
            latest_feature_ts = _result[0] if _result else None

            # Calculate lag
            lag_seconds = None
            if latest_feature_ts:
                lag_seconds = datetime.now().timestamp() - latest_feature_ts

            metrics = {
                "raw_conn_count": raw_conn_count,
                "raw_unsw_extra_count": raw_unsw_extra_count,
                "features_count": features_count,
                "lag_seconds": lag_seconds,
            }

            # Alert if feature lag is too high
            if lag_seconds and lag_seconds > 600:  # 10 minutes
                print(f"WARNING: Feature computation lag is {lag_seconds:.0f} seconds")
                metrics["status"] = "warning"
            else:
                metrics["status"] = "healthy"

            print(f"DuckDB Health: {metrics}")
            return metrics

        finally:
            conn.close()

    @task()
    def aggregate_monitoring_metrics(kafka_metrics, duckdb_metrics):
        """Aggregate and report all monitoring metrics."""

        # Determining overall status
        statuses = [kafka_metrics.get("status"), duckdb_metrics.get("status")]
        overall_status = "warning" if "warning" in statuses else "healthy"

        report = {
            "kafka": kafka_metrics,
            "duckdb": duckdb_metrics,
            "overall_status": overall_status,
            "timestamp": datetime.now().isoformat(),
        }

        print("=" * 80)
        print("STREAMING PIPELINE HEALTH REPORT")
        print("=" * 80)
        print(f"Status: {overall_status.upper()}")
        print(f"Timestamp: {report['timestamp']}")
        print("")
        print("Kafka:")
        print(f"  Consumer: {kafka_metrics.get('consumer_groups_count', 'N/A')}")
        print(f"  Status: {kafka_metrics.get('status', 'unknown')}")
        print("")
        print("DuckDB:")
        print(f"  Raw Conn Rows: {duckdb_metrics['raw_conn_count']:,}")
        print(f"  Raw UNSW Rows: {duckdb_metrics['raw_unsw_count']:,}")
        print(f"  Features Rows: {duckdb_metrics['features_count']:,}")

        if duckdb_metrics["feature_lag_seconds"]:
            print(f"  Feature Lag: {duckdb_metrics['feature_lag_seconds']:.0f} seconds")
        else:
            print("  Feature Lag: N/A (no features yet)")

        print(f"  Status: {duckdb_metrics.get('status', 'unknown')}")
        print("=" * 80)

        return report

    kafka_metrics = check_kafka_consumer_lag()
    duckdb_metrics = check_duckdb_health()
    aggregate_monitoring_metrics(kafka_metrics, duckdb_metrics)


# Run DAG
monitor_etl()
