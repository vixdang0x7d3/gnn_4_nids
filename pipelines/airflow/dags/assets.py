"""
Shared asset definitions and configuration for all DAGs
"""

import os

from airflow.sdk import Asset

# =============================================================================
# S3/MinIO Configuration
# =============================================================================

BUCKET_NAME = "pipeline-data"

S3_CONFIG = {
    "endpoint_url": "http://minio:9000",
    "region_name": "us-east-1",
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
}


# =============================================================================
# Asset Definitions
# =============================================================================

FEATURE_ASSET = Asset(
    uri="s3://pipeline-data/features/",
    name="feature",
    extra={
        "descriptions": "Exported feature files for inference",
        "bucket": BUCKET_NAME,
        "prefix": "features/",
        "file_patterns": "*.parquet",
    },
)

PREDICTION_ASSET = Asset(
    uri="s3://pipeline-data/predictions",
    name="predictions",
    extra={
        "description": "Exported inference results",
        "bucket": BUCKET_NAME,
        "prefix": "predictions/",
        "file_patterns": "*.parquet",
    },
)

# =============================================================================
# Model Configuration
# =============================================================================

MLFLOW_TRACKING_URI = "http://mlflow:5000"
MODEL_NAME = "gnn_anomaly_detector"
MLFLOW_MODEL_ALIAS = "champion"
BINARY_MODE = True
GRAPH_N_NEIGHBORS = 2

# Detection thresholds
ANOMALY_THRESHOLD = 0.7  # Score threshold for binary classification
ALERT_THRESHOLD = 10  # Number of anomalies to trigger alert

# =============================================================================
# Feature Schema Definitions
# =============================================================================

# Original UNSW-NB15 features
OG_FEATURE_ATTRIBUTES = [
    "sttl",
    "dur",
    "dintpkt",
    "sintpkt",
    "ct_dst_sport_ltm",
    "tcprtt",
    "sbytes",
    "dbytes",
    "smeansz",
    "dmeansz",
    "sload",
    "dload",
    "spkts",
    "dpkts",
]
OG_EDGE_ATTRIBUTES = ["proto", "service", "state"]

# NetFlow-based features
NF_FEATURE_ATTRIBUTES = [
    "min_ttl",
    "flow_duration_milliseconds",
    "tcp_win_max_out",
    "server_tcp_flags",
    "dst_to_src_iat_max",
    "src_to_dst_iat_max",
    "in_bytes",
    "out_bytes",
    "num_pkts_up_to_128_bytes",
    "dst_to_src_avg_throughput",
    "src_to_dst_second_bytes",
    "dns_query_type",
    "ftp_command_ret_code",
]
NF_EDGE_ATTRIBUTES = ["protocol", "l7_proto"]
