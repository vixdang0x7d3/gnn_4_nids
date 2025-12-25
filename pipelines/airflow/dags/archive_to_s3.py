"""
Archive features to S3 using FileSensor-driven event detection.

Workflow:
1. FileSensor detects new parquet files in /data/archive
2. Scan and upload files to MinIO S3
3. Emit FEATURES_ASSET for downstream inference
4. Cleanup old local files

Schedule: Every 1 minute (near real-time, <1 min latency)
Retention: 2 hours (files kept locally before cleanup)

This is a near real-time pipeline - files are detected and processed
within 1 minute of being written by the archiver.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset, chain, dag, task

sys.path.insert(0, "/opt/airflow/data_pipeline/src")

# Asset definition
FEATURES_ASSET = Asset(
    uri="s3://datasets/archive/features/",
    name="feature_parquets",
    extra={
        "bucket": "datasets",
        "prefix": "archive/features/",
        "file_pattern": "*.parquet",
    },
)

default_args = {
    "owner": "gnn-nids",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

# S3/MinIO configuration
S3_CONFIG = {
    "endpoint_url": "http://minio:9000",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "minioadmin123",
    "region_name": "us-east-1",
}

BUCKET_NAME = "datasets"

# Archive directory where streaming ETL writes parquet files
ARCHIVE_DIR = Path("/data/archive")

# Keep local parquet files for N seconds before cleanup
# Retention must be longer than sensor timeout to prevent premature deletion
LOCAL_RETENTION_SEC = 7200  # 2 hours


@dag(
    dag_id="archive_to_s3",
    description="Asset-driven upload to S3 with Asset emission",
    schedule=timedelta(minutes=1),  # Check for new files every minute
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=[
        "archive",
        "s3",
        "storage",
        "etl",
        "asset-producer",
        "near-realtime",
    ],
    default_args=default_args,
    max_active_runs=1,  # Prevent concurrent runs
)
def archive_to_s3():
    """Asset-driven upload pipeline with Asset emission."""

    @task.sensor(poke_interval=30, timeout=300, mode="reschedule")
    def wait_for_new_parquet():
        """Check if new parquet files exist in archive directory.

        With 1-minute schedule, timeout is 5 minutes - if no files appear,
        the run fails but next scheduled run will retry.
        """
        archive_dir = Path(ARCHIVE_DIR)
        if not archive_dir.exists():
            return False

        # Check for any parquet files
        parquet_files = list(archive_dir.glob("*.parquet"))
        return len(parquet_files) > 0

    @task()
    def scan_parquet_files():
        """Scan archive directory for parquet files."""

        archive_dir = Path(ARCHIVE_DIR)
        if not archive_dir.exists():
            raise AirflowSkipException(
                f"Archive directory '{archive_dir}' does not exist"
            )

        # Find all parquet files
        parquet_files = list(archive_dir.glob("*.parquet"))

        if not parquet_files:
            raise AirflowSkipException("No parquet files found")

        files_info = []
        total_size = 0

        for file_path in parquet_files:
            file_size = file_path.stat().st_size
            total_size += file_size

            files_info.append(
                {
                    "local_path": str(file_path),
                    "file_name": file_path.name,
                    "size_bytes": file_size,
                    "modified_time": file_path.stat().st_mtime,
                }
            )

        print(f"Found {len(files_info)} parquet file(s)")
        print(f"Total size: {total_size / (1024**2):.2f} MB")

        return {
            "files": files_info,
            "total_files": len(files_info),
            "total_size_mb": total_size / (1024**2),
        }

    @task(outlets=[FEATURES_ASSET])
    def upload_to_s3(scan_info):
        """Upload parquet files to S3 and emit Asset event.

        Features files go to: archive/features/
        Unified flows go to: archive/unified_flows/
        Only feature files trigger the FEATURES_ASSET.
        """
        import boto3

        s3_client = boto3.client("s3", **S3_CONFIG)  # type: ignore

        bucket_name = BUCKET_NAME

        # Ensure bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' exists")
        except Exception:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Created bucket: {bucket_name}")

        uploaded_features = []
        uploaded_unified_flows = []

        for file_info in scan_info["files"]:
            local_path = file_info["local_path"]
            file_name = file_info["file_name"]

            # Determine S3 prefix based on file type
            if "_unified_flows_" in file_name:
                s3_key = f"archive/unified_flows/{file_name}"
                uploaded_unified_flows.append({
                    "s3_uri": f"s3://{bucket_name}/{s3_key}",
                    "file_name": file_name,
                    "upload_timestamp": datetime.now().isoformat(),
                })
            elif "_features_" in file_name:
                s3_key = f"archive/features/{file_name}"
                uploaded_features.append({
                    "s3_uri": f"s3://{bucket_name}/{s3_key}",
                    "file_name": file_name,
                    "upload_timestamp": datetime.now().isoformat(),
                })
            else:
                print(f"Skipping unknown file type: {file_name}")
                continue

            s3_client.upload_file(local_path, bucket_name, s3_key)

            file_size_mb = file_info["size_bytes"] / (1024 * 1024)
            print(f"Uploaded: s3://{bucket_name}/{s3_key}")
            print(f"  Size: {file_size_mb:.2f} MB")

        total_uploaded_mb = sum(f["size_bytes"] for f in scan_info["files"]) / (
            1024 * 1024
        )

        print("\nUpload summary:")
        print(f"  Feature files: {len(uploaded_features)}")
        print(f"  Unified flow files: {len(uploaded_unified_flows)}")
        print(f"  Total size: {total_uploaded_mb:.2f} MB")

        return {
            "bucket": bucket_name,
            "uploaded_files": uploaded_features + uploaded_unified_flows,
            "uploaded_features": uploaded_features,
            "uploaded_unified_flows": uploaded_unified_flows,
            "total_uploaded": len(uploaded_features) + len(uploaded_unified_flows),
            "total_uploaded_mb": total_uploaded_mb,
            "asset_metadata": uploaded_features,  # Only feature files for Asset
        }

    @task()
    def cleanup_old_parquets(upload_info):
        """Delete local parquet files older than retention period."""

        if not upload_info["uploaded_files"]:
            raise AirflowSkipException("No files were uploaded, skipping cleanup")

        cutoff_time = (
            datetime.now() - timedelta(seconds=LOCAL_RETENTION_SEC)
        ).timestamp()

        deleted_files = []
        kept_files = []

        for file_info in upload_info["asset_metadata"]:
            file_name = file_info["file_name"]
            local_path = Path(ARCHIVE_DIR) / file_name

            if not local_path.exists():
                print(f"File already deleted: {file_name}")
                continue

            file_mtime = local_path.stat().st_mtime

            if file_mtime < cutoff_time:
                # Delete old file
                file_size_mb = local_path.stat().st_size / (1024 * 1024)
                age_sec = datetime.now().timestamp() - file_mtime
                local_path.unlink()

                deleted_files.append(
                    {
                        "filename": file_name,
                        "size_mb": file_size_mb,
                        "age_sec": age_sec,
                    }
                )

                print(f"Deleted (>{LOCAL_RETENTION_SEC}s old): {file_name}")
            else:
                kept_files.append(file_name)

        if deleted_files:
            total_freed_mb = sum(f["size_mb"] for f in deleted_files)
            print("\nCleanup summary:")
            print(
                f"  Deleted: {len(deleted_files)} files ({total_freed_mb:.2f} MB freed)"
            )
            print(f"  Kept: {len(kept_files)} files (within retention period)")
        else:
            print(f"No files older than {LOCAL_RETENTION_SEC}s to delete")

        return {
            "deleted_count": len(deleted_files),
            "deleted_files": deleted_files,
            "kept_count": len(kept_files),
        }

    @task()
    def print_summary(scan_info, upload_info, cleanup_info):
        """Print comprehensive summary of the archive pipeline execution."""
        print("\n" + "=" * 100)
        print("ARCHIVE TO S3 PIPELINE EXECUTION SUMMARY")
        print("=" * 100)

        # Execution metadata
        print(f"\nExecution Time: {datetime.now().isoformat()}")
        print("DAG ID: archive_to_s3")
        print(f"Archive Directory: {ARCHIVE_DIR}")

        # Scan results
        print("\n--- LOCAL FILES SCANNED ---")
        print(f"Files Found: {scan_info['total_files']}")
        print(f"Total Size: {scan_info['total_size_mb']:.2f} MB")

        if scan_info["files"]:
            print("\nFile Details:")
            for idx, file_info in enumerate(scan_info["files"], 1):
                size_mb = file_info["size_bytes"] / (1024 * 1024)
                mod_time = datetime.fromtimestamp(
                    file_info["modified_time"]
                ).isoformat()
                print(f"  {idx}. {file_info['file_name']}")
                print(f"     Size: {size_mb:.2f} MB | Modified: {mod_time}")

        # Upload results
        print("\n--- S3 UPLOAD ---")
        print(f"Bucket: {upload_info['bucket']}")
        print(f"Files Uploaded: {upload_info['total_uploaded']}")
        print(f"Total Uploaded: {upload_info['total_uploaded_mb']:.2f} MB")

        if upload_info["uploaded_files"]:
            print("\nUploaded Files:")
            for idx, file_info in enumerate(upload_info["uploaded_files"], 1):
                print(f"  {idx}. {file_info['s3_uri']}")
                print(f"     Uploaded: {file_info['upload_timestamp']}")

        # Cleanup results
        print("\n--- LOCAL CLEANUP ---")
        print(
            f"Retention Period: {LOCAL_RETENTION_SEC} seconds ({LOCAL_RETENTION_SEC / 60:.1f} minutes)"
        )
        print(f"Files Deleted: {cleanup_info['deleted_count']}")
        print(f"Files Kept: {cleanup_info['kept_count']}")

        if cleanup_info["deleted_files"]:
            total_freed_mb = sum(f["size_mb"] for f in cleanup_info["deleted_files"])
            print(f"Total Space Freed: {total_freed_mb:.2f} MB")
            print("\nDeleted Files:")
            for idx, file_info in enumerate(cleanup_info["deleted_files"], 1):
                age_min = file_info["age_sec"] / 60
                print(f"  {idx}. {file_info['filename']}")
                print(
                    f"     Size: {file_info['size_mb']:.2f} MB | Age: {age_min:.1f} minutes"
                )

        # Asset emission
        print("\n--- ASSET EMISSION ---")
        print("FEATURES_ASSET emitted")
        print(f"URI: {FEATURES_ASSET.uri}")
        print(f"Files in Asset: {len(upload_info['asset_metadata'])}")

        # Pipeline statistics
        print("\n--- PIPELINE STATISTICS ---")
        print(
            f"Scan -> Upload: {upload_info['total_uploaded']} of {scan_info['total_files']} files"
        )
        print(
            f"Cleanup Rate: {cleanup_info['deleted_count']} deleted, {cleanup_info['kept_count']} retained"
        )
        print("=" * 100 + "\n")

        return {
            "summary_generated": True,
            "timestamp": datetime.now().isoformat(),
            "pipeline": "archive_to_s3",
            "files_processed": scan_info["total_files"],
            "files_uploaded": upload_info["total_uploaded"],
            "files_deleted": cleanup_info["deleted_count"],
        }

    # DAG workflow
    sensor = wait_for_new_parquet()
    scan_info = scan_parquet_files()
    upload_info = upload_to_s3(scan_info)
    cleanup_info = cleanup_old_parquets(upload_info)
    summary = print_summary(scan_info, upload_info, cleanup_info)

    # Chain task dependencies
    chain(sensor, scan_info, upload_info, cleanup_info, summary)


# Run DAG
archive_to_s3()
