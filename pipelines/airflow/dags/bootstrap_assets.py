"""
Bootstrap Assets for Asset-Driven Scheduling

This DAG initializes the asset-based scheduling system by:
1. Creating S3 buckets if they don't exist
2. Emitting initial asset events to prime the system
3. Validating connections and configurations

Schedule: Manual trigger only (run once during deployment)

This ensures that downstream DAGs waiting for assets can start working
even if no data has been produced yet.
"""

import sys
from datetime import datetime, timedelta

from airflow.exceptions import AirflowException
from airflow.sdk import chain, dag, task
from airflow.sdk.definitions.asset import Asset

sys.path.insert(0, "/opt/airflow/data_pipeline/src")


# Define all assets used in the pipeline
# Must match asset definitions in other DAGs (archive_to_s3, inference_anomaly_detection)
FEATURES_ASSET = Asset(
    uri="s3://datasets/archive/features/",
    name="feature_parquets",
)
PREDICTIONS_ASSET = Asset(
    uri="s3://datasets/predictions/",
    name="anomaly_predictions",
)

default_args = {
    "owner": "gnn-nids",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
}

# S3/MinIO configuration
S3_CONFIG = {
    "endpoint_url": "http://minio:9000",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "minioadmin123",
    "region_name": "us-east-1",
}

BUCKET_NAME = "datasets"


@dag(
    dag_id="bootstrap_assets",
    description="Initialize asset-based scheduling system (run once)",
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bootstrap", "setup", "assets"],
    default_args=default_args,
)
def bootstrap_assets():
    """Bootstrap the asset-driven pipeline infrastructure."""

    @task()
    def create_s3_buckets():
        """Create required S3 buckets if they don't exist."""
        import boto3

        s3_client = boto3.client("s3", **S3_CONFIG)  # ty: ignore

        buckets_to_create = [BUCKET_NAME]
        created_buckets = []
        existing_buckets = []

        for bucket in buckets_to_create:
            try:
                s3_client.head_bucket(Bucket=bucket)
                existing_buckets.append(bucket)
                print(f"Bucket already exists: {bucket}")
            except Exception:
                try:
                    s3_client.create_bucket(Bucket=bucket)
                    created_buckets.append(bucket)
                    print(f"Created bucket: {bucket}")
                except Exception as e:
                    print(f"Failed to create bucket {bucket}: {e}")
                    raise AirflowException(f"Failed to create bucket: {bucket}")

        return {
            "created_buckets": created_buckets,
            "existing_buckets": existing_buckets,
            "total_buckets": len(created_buckets) + len(existing_buckets),
        }

    @task()
    def create_s3_prefixes(bucket_info):
        """Create S3 prefixes (folders) for organizing data."""
        import boto3

        s3_client = boto3.client("s3", **S3_CONFIG)  # ty: ignore

        prefixes = [
            "archive/features/",
            "predictions/",
            "models/",
            "alerts/",
        ]

        created_prefixes = []

        for prefix in prefixes:
            # Create empty object to represent folder
            try:
                s3_client.put_object(Bucket=BUCKET_NAME, Key=prefix, Body=b"")
                created_prefixes.append(prefix)
                print(f"Created prefix: s3://{BUCKET_NAME}/{prefix}")
            except Exception as e:
                print(f"Warning: Could not create prefix {prefix}: {e}")

        return {
            "created_prefixes": created_prefixes,
            "prefix_count": len(created_prefixes),
        }

    @task(outlets=[FEATURES_ASSET])
    def register_existing_parquet_files():
        """Scan S3 and register existing parquet files as Assets."""
        import boto3

        s3_client = boto3.client("s3", **S3_CONFIG)  # ty: ignore

        try:
            response = s3_client.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix="archive/features/",
            )

            if "Contents" not in response:
                print("No existing parquet files found in S3")
                return {
                    "registered_files": [],
                    "count": 0,
                }

            files = [
                obj["Key"]
                for obj in response["Contents"]
                if obj["Key"].endswith(".parquet")
            ]

            file_details = [
                {
                    "key": obj["Key"],
                    "size_mb": obj["Size"] / (1024 * 1024),
                    "last_modified": obj["LastModified"].isoformat(),
                }
                for obj in response["Contents"]
                if obj["Key"].endswith(".parquet")
            ]

            total_size_mb = sum(f["size_mb"] for f in file_details)

            print(f"Registered {len(files)} existing parquet files as Assets")
            print(f"Total size: {total_size_mb:.2f} MB")

            for idx, file_info in enumerate(file_details[:5], 1):
                print(f"  {idx}. {file_info['key']} ({file_info['size_mb']:.2f} MB)")

            if len(files) > 5:
                print(f"  ... and {len(files) - 5} more files")

            return {
                "registered_files": files,
                "count": len(files),
                "total_size_mb": total_size_mb,
                "file_details": file_details,
            }

        except Exception as e:
            print(f"Failed to register existing files: {e}")
            return {
                "registered_files": [],
                "count": 0,
                "error": str(e),
            }

    @task()
    def print_bootstrap_summary(bucket_info, prefix_info, registered_files_info):
        """Print bootstrap summary report."""
        print("\n" + "=" * 80)
        print("ASSET PIPELINE BOOTSTRAP SUMMARY")
        print("=" * 80)
        print(f"Timestamp: {datetime.now().isoformat()}")
        print("")
        print("S3/MinIO Storage:")
        print(f"  Total Buckets: {bucket_info['total_buckets']}")
        print(f"  Created: {len(bucket_info['created_buckets'])}")
        print(f"  Existing: {len(bucket_info['existing_buckets'])}")
        print(f"  Prefixes Created: {prefix_info['prefix_count']}")
        print("")
        print("Assets Configured:")
        print(f"  FEATURES_ASSET: {FEATURES_ASSET.uri}")
        print(f"  PREDICTIONS_ASSET: {PREDICTIONS_ASSET.uri}")
        print("")
        print("Existing Files Registered:")
        print(f"  Parquet Files: {registered_files_info['count']}")
        if registered_files_info["count"] > 0:
            print(
                f"  Total Size: {registered_files_info.get('total_size_mb', 0):.2f} MB"
            )
            print("  Asset Event: FEATURES_ASSET emitted")
        else:
            print("  No existing files found (clean deployment)")
        print("")
        print("=" * 80)
        print("  Bootstrap completed successfully")
        print("=" * 80)

        return {
            "bootstrap_status": "complete",
            "timestamp": datetime.now().isoformat(),
        }

    # DAG workflow
    bucket_info = create_s3_buckets()
    prefix_info = create_s3_prefixes(bucket_info)
    registered_files_info = register_existing_parquet_files()
    summary = print_bootstrap_summary(bucket_info, prefix_info, registered_files_info)

    # Chain task dependencies
    chain(bucket_info, prefix_info, registered_files_info, summary)


# Run DAG
bootstrap_assets()
