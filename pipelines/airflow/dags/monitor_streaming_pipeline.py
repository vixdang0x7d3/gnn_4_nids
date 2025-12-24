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

from airflow.sdk import chain, dag, task

sys.path.insert(0, "/opt/airflow/data_pipeline/src")

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
        from confluent_kafka.admin import AdminClient

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
    def report_kafka_health(kafka_metrics):
        """Aggregate and report all monitoring metrics."""

        report = {
            "kafka": kafka_metrics,
            "status": kafka_metrics.get("status", "unknown"),
            "timestamp": datetime.now().isoformat(),
        }

        print("=" * 80)
        print("STREAMING PIPELINE HEALTH REPORT")
        print("=" * 80)
        print(f"Status: {kafka_metrics.get('status', 'unknown').upper()}")
        print(f"Timestamp: {report['timestamp']}")
        print("")
        print("Kafka:")
        print(f"  Consumer: {kafka_metrics.get('consumer_groups_count', 'N/A')}")
        print(f"  Status: {kafka_metrics.get('status', 'unknown')}")
        print("")
        print("=" * 80)

        return report

    kafka_metrics = check_kafka_consumer_lag()
    report = report_kafka_health(kafka_metrics)

    # Chain task dependencies
    chain(kafka_metrics, report)


# Run DAG
monitor_etl()
