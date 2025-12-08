"""
Archive old features from DuckDB to S3-compatible storage.

Workflow:
1. Find parquet files in local archival directory
2. Upload to S3-compatible storage (MinIO or AWS S3)
3. Delete old parquet files after N days

Schedule: Every 6 hours

Note:
Uses boto3 for S3 compatibility. For MinIO, specify endpoint_url.
For AWS S3, omit endpoint_url and configure AWS credentials via environment.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

sys.path.insert(0, "/opt/airflow/data_pipeline/src")

import boto3

default_args = {
    "owner": "gnn-nids",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# Archive directory where streaming ETL writes parquet files
ARCHIVE_DIR = Path("/data/archive")

# Keep local parquet files for N days before cleanup
LOCAL_RETENTION_DAYS = 7


@dag(
    dag_id="archive_to_s3",
    description="Upload parquet archives to S3 and cleanup old local files",
    schedule=timedelta(hours=6),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["archive", "s3", "storage", "etl"],
    default_args=default_args,
)
def archive_to_s3():
    """Upload archived parquet files to S3 and cleanup old local files."""

    @task()
    def scan_parquet_files():
        """Scan archive directory for parquet files"""
        if not ARCHIVE_DIR.exists():
            raise AirflowSkipException(
                f"Archive directory '{ARCHIVE_DIR}' does not exist"
            )

        # Find all parquet files
        parquet_files = list(ARCHIVE_DIR.glob("*.parquet"))

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

        print(f"Found {len(files_info)} parquet files")
        print(f"Total size: {total_size / (1024**2):.2f} MB")

        return {
            "files": files_info,
            "total_files": len(files_info),
            "total_size_mb": total_size / (1024**2),
        }

    @task()
    def upload_to_s3(scan_info):
        """Upload exported Parquet file to S3-compatible storage."""

        s3_client = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",  # For MinIO; omit for AWS S3
            aws_access_key_id="admin",
            aws_secret_access_key="minio123",
            region_name="us-east-1",
        )

        bucket_name = "datasets"

        # Ensure bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' exists")
        except Exception:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Created bucket: {bucket_name}")

        uploaded_files = []
        for si in scan_info["files"]:
            local_path = si["output_path"]
            file_name = si["file_name"]

            # archiver already timestamped the file.
            s3_key = f"archive/features/{file_name}"

            s3_client.upload_file(local_path, bucket_name, s3_key)

            file_size_mb = si["size_bytes"] / (1024 * 1024)

            print(f"Uploaded S3: s3://{bucket_name}/{s3_key}")
            print(f"File size: {file_size_mb:.2f} MB")

            uploaded_files.append(si)

        total_uploaded_mb = sum(f["size_bytes"] for f in uploaded_files) / (1024 * 1024)

        print("\nUploaded summary:")
        print(f"  Uploaded: {len(uploaded_files)} files ({total_uploaded_mb:.2f} MB)")

        return {
            "bucket": bucket_name,
            "uploaded_files": uploaded_files,
            "total_uploaded": len(uploaded_files),
            "total_uploaded_mb": total_uploaded_mb,
        }

    @task()
    def cleanup_old_parquets(upload_info):
        """Delete local parquet files older than retention period."""

        # Skip if nothing was uploaded
        if not upload_info["uploaded_files"]:
            raise AirflowSkipException("No files were uploaded, skipping cleanup")

        cutoff_time = (
            datetime.now() - timedelta(days=LOCAL_RETENTION_DAYS)
        ).timestamp()

        deleted_files = []
        kept_files = []

        for file_info in upload_info["uploaded_files"]:
            local_path = Path(file_info["local_path"])

            if not local_path.exists():
                print(f"File already deleted: {local_path}")
                continue

            file_mtime = file_info["modified_time"]

            if file_mtime < cutoff_time:
                # Delete old file
                file_size_mb = file_info["size_bytes"] / (1024 * 1024)
                local_path.unlink()
                deleted_files.append(
                    {
                        "filename": file_info["file_name"],
                        "size_mb": file_size_mb,
                        "age_days": (datetime.now().timestamp() - file_mtime) / 86400,
                    }
                )

                print(f"Deleted (>{LOCAL_RETENTION_DAYS}): {file_info['filename']}")
            else:
                kept_files.append(file_info["filename"])

        if deleted_files:
            total_freed_mb = sum(f["size_mb"] for f in deleted_files)
            print("\nCleanup summary: ")
            print(
                f"  Deleted: {len(deleted_files)} files ({total_freed_mb:.2f} MB freed)"
            )
            print(f"  Kept: {len(kept_files)} files (within retention period)")
        else:
            print(f"No files older than {LOCAL_RETENTION_DAYS} to delete")

        return {
            "deleted_count": len(deleted_files),
            "deleted_files": deleted_files,
            "kept_count": len(kept_files),
        }

    scan_info = scan_parquet_files()
    upload_info = upload_to_s3(scan_info)
    cleanup_old_parquets(upload_info)


# Run DAG
archive_to_s3()
