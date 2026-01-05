"""
S3 Feature File Sensor

Watches for new feature parquet files in S3 and emits FEATURES_ASSET
to trigger downstream inference pipeline.

Workflow:
- Custom S3 sensor that tracks processed files via S3 marker objects
- On detecting new files, emits FEATURES_ASSET with file metadata
- Downstream DAGs (inference) are scheduled on FEATURES_ASSET

Schedule: Every 10 seconds (configurable)
Idempotency: Uses S3 marker files
"""

import sys
from datetime import datetime, timedelta

from airflow.sdk import DAG, PokeReturnValue, task

sys.path.append("/opt/airflow/dags")

from assets import BUCKET_NAME, FEATURE_ASSET, S3_CONFIG

default_args = {
    "owner": "gnn-nids",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


with DAG(
    dag_id="new_feature_sensor",
    description="Detect new parquet files",
    schedule=timedelta(minutes=5),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    @task.sensor(
        task_id="detect_new_feature_files",
        poke_interval=10,
        timeout=240,
        mode="reschedule",
        soft_fail=True,
        outlets=[FEATURE_ASSET],
    )
    def detect_new_files() -> PokeReturnValue:
        """
        Sensor task that detects new parquets in S3.

        Returned XCom value:
            - files: list of new feature files
            - count: number of new files detected
            - latest: timestamp of latest file
            - detection_time: timestamp of detection
        """

        import boto3

        s3_client = boto3.client("s3", **S3_CONFIG)  # type: ignore

        prefix = FEATURE_ASSET.extra["prefix"]
        marker_key = f"{prefix}.last_processed_marker"

        # Get last processed timestamp
        try:
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=marker_key)
            last_processed = datetime.fromisoformat(
                response["Body"].read().decode("utf-8").strip()
            )

            print(f"Last processed: {last_processed}")
        except s3_client.exceptions.NoSuchKey:
            print("First run - no marker")
            last_processed = None

        except Exception as e:
            print(f"Marker error: {e}")
            last_processed = None

        try:
            response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        except Exception as e:
            print(f"S3 error: {e}")
            return PokeReturnValue(is_done=False)

        if "Contents" not in response:
            return PokeReturnValue(is_done=False)

        # Find new files
        new_files = []
        latest_time = last_processed or datetime.min

        for obj in response["Contents"]:
            key = obj["Key"]
            if not key.endswith(".parquet") or ".last_processed" in key:
                continue

            file_time = obj["LastModified"].replace(tzinfo=None)

            if last_processed is None or file_time > last_processed:
                new_files.append(
                    {
                        "key": key,
                        "size": obj["Size"],
                        "modified": file_time.isoformat(),
                    }
                )
                if file_time > latest_time:
                    latest_file = file_time

        if not new_files:
            print("No new files")
            return PokeReturnValue(is_done=False)

        # Found new file
        print(f"Found {len(new_files)} new file(s)")

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=marker_key,
            Body=latest_file.isoformat().encode("utf-8"),
        )

        return PokeReturnValue(
            is_done=True,
            xcom_value={
                "files": new_files,
                "count": len(new_files),
                "latest": latest_file,
                "detection_time": datetime.now(),
            },
        )

    @task()
    def log_and_summarize(detection_result: dict):
        """
        Log detection results.
        """
        if not detection_result:
            print("No detection result (sensor timed out)")
            return {"logged": False}

        print("\n" + "=" * 70)
        print("NEW PARQUET FILES DETECTED")
        print("=" * 70)
        print(f"Files Found: {detection_result['count']}")
        print(f"Detection Time: {detection_result['detection_time']}")
        print(f"Latest File Modified: {detection_result['latest']}")
        print("\nFiles:")
        for f in detection_result["files"][:10]:
            size_kb = f["size"] / 1024
            print(f"  {f['key']} ({size_kb:.1f} KB)")
        if detection_result["count"] > 10:
            print(f"  ... and {detection_result['count'] - 10} more")
        print("=" * 70)
        print("FEATURES_ASSET emitted")
        print("Downstream DAG (inference_pipeline) will be triggered")
        print("=" * 70 + "\n")

        return {
            "logged": True,
            "file_count": detection_result["count"],
        }

    sensor_result = detect_new_files()
    summary = log_and_summarize(sensor_result)
