"""
Real-time anomaly detection inference triggered by Asset events.

Workflow:
1. Triggered by FEATURES_ASSET (when archive_to_s3 uploads files)
2. Discover new feature files from S3
3. Load GNN model and scaler from MLflow
4. Build graph using GraphGCN
5. Run inference on graph data
6. Export predictions to S3
7. Emit PREDICTIONS_ASSET
8. Log alerts for anomalies

Schedule: Asset-driven (triggered automatically by archive_to_s3)

Near real-time: ~1-2 minute total latency from archive to predictions
"""

import sys
from datetime import datetime, timedelta

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.sdk import Asset, chain, dag, task

sys.path.insert(0, "/opt/airflow/data_pipeline/src")
sys.path.insert(0, "/opt/airflow/graph_building/src")
sys.path.insert(0, "/opt/airflow/model_training/src")

# Model constants (from gnn.const and gnn.const_nf)
BINARY_LABEL = "label"
MULTICLASS_LABEL = "multiclass_label"

# Original UNSW-NB15 features (lowercase - from og_features table)
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

# NetFlow-based features (lowercase - from nf_features table)
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

# Model configuration
MLFLOW_TRACKING_URI = "http://mlflow:5000"
MODEL_NAME = "gnn_anomaly_detector"
MLFLOW_MODEL_ALIAS = "champion"  # Use alias instead of deprecated stage
BINARY_MODE = True
GRAPH_N_NEIGHBORS = 2

# Assets
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
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),
}

# S3/MinIO configuration
S3_CONFIG = {
    "endpoint_url": "http://minio:9000",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "minioadmin123",
    "region_name": "us-east-1",
}

BUCKET_NAME = "datasets"
ANOMALY_THRESHOLD = 0.7  # Alert if anomaly score > 0.7
ALERT_THRESHOLD = 10  # Alert if more than 10 anomalies detected


@dag(
    dag_id="inference_anomaly_detection",
    description="GNN-based anomaly detection with MLflow (Asset-driven)",
    schedule=[FEATURES_ASSET],  # Triggered when FEATURES_ASSET updated
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=[
        "inference",
        "ml",
        "gnn",
        "anomaly-detection",
        "asset-consumer",
        "near-realtime",
    ],
    default_args=default_args,
)
def inference_anomaly_detection():
    """Asset-driven inference pipeline for network anomaly detection."""

    @task()
    def discover_new_files():
        """Discover new parquet files and implement idempotency."""
        import boto3

        # Get last processed file from Airflow Variable
        last_processed = Variable.get("last_processed_feature_file", default_var=None)

        # Initialize S3 client
        s3_client = boto3.client("s3", **S3_CONFIG)  # ty: ignore

        # List all parquet files in features directory
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix="archive/features/",
        )

        if "Contents" not in response:
            raise AirflowSkipException("No feature files found in S3")

        all_files = [
            {
                "key": obj["Key"],
                "size": obj["Size"],
                "last_modified": obj["LastModified"],
                "s3_uri": f"s3://{BUCKET_NAME}/{obj['Key']}",
            }
            for obj in response["Contents"]
            if obj["Key"].endswith(".parquet") and "features" in obj["Key"]
        ]

        if not all_files:
            raise AirflowSkipException("No feature parquet files found")

        # Sort by last modified (newest first)
        all_files.sort(key=lambda x: x["last_modified"], reverse=True)
        latest_file = all_files[0]

        # Check if already processed
        if last_processed and latest_file["key"] == last_processed:
            raise AirflowSkipException(f"File already processed: {latest_file['key']}")

        print(f"Processing new file: {latest_file['key']}")
        print(f"  Size: {latest_file['size'] / (1024**2):.2f} MB")
        print(f"  Modified: {latest_file['last_modified']}")

        return {
            "file_to_process": latest_file,
            "total_files_in_s3": len(all_files),
            "timestamp": datetime.now().isoformat(),
        }

    @task()
    def load_model_from_mlflow():
        """Load production GNN model and scaler from MLflow using alias."""
        import pickle

        import mlflow

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

        # Load model using alias (modern approach, replaces stages)
        model_uri = f"models:/{MODEL_NAME}@{MLFLOW_MODEL_ALIAS}"

        try:
            loaded_model = mlflow.pytorch.load_model(model_uri)

            # Get model version by alias
            client = mlflow.MlflowClient()
            model_version = client.get_model_version_by_alias(MODEL_NAME, MLFLOW_MODEL_ALIAS)

            if not model_version:
                raise ValueError(f"No model found for {MODEL_NAME}@{MLFLOW_MODEL_ALIAS}")

            run_id = model_version.run_id

            # Download scaler
            scaler_path = mlflow.artifacts.download_artifacts(
                run_id=run_id,
                artifact_path="preprocessor/fitted_mm_scaler.pkl",
                dst_path="/tmp",
            )

            with open(scaler_path, "rb") as f:
                scaler = pickle.load(f)

            print(f"Model loaded: {MODEL_NAME}@{MLFLOW_MODEL_ALIAS} (version {model_version.version})")
            print(f"Run ID: {run_id}")
            print(f"Scaler loaded from: {scaler_path}")

            return {
                "model": loaded_model,
                "scaler": scaler,
                "model_version": model_version.version,
                "model_alias": MLFLOW_MODEL_ALIAS,
                "run_id": run_id,
                "model_loaded": True,
            }

        except Exception as e:
            print(f"MLflow model loading failed: {e}")
            print("Using placeholder mode (no actual inference)")

            return {
                "model": None,
                "scaler": None,
                "model_version": "placeholder",
                "run_id": None,
                "model_loaded": False,
                "error": str(e),
            }

    @task()
    def run_inference(file_info, model_info):
        """Run inference: load data from S3, build graph, predict."""
        from pathlib import Path

        import duckdb
        import numpy as np
        import pandas as pd
        import torch
        from graph_building import GraphGCN
        from graph_building.transform import minmax_scale

        file_to_process = file_info["file_to_process"]
        s3_uri = file_to_process["s3_uri"]
        filename = Path(s3_uri).name

        print(f"\nProcessing: {filename}")

        # If model not loaded, use placeholder
        if not model_info["model_loaded"]:
            print("Model not available, generating placeholder predictions")
            return _run_placeholder_inference(file_to_process)

        # Configure DuckDB for S3 access
        conn = duckdb.connect(":memory:")
        conn.execute("""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_endpoint='minio:9000';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
            SET s3_access_key_id='admin';
            SET s3_secret_access_key='minioadmin123';
        """)

        # Detect schema by checking which columns exist
        schema_check_query = f"SELECT * FROM read_parquet('{s3_uri}') LIMIT 1"
        sample = conn.execute(schema_check_query).df()
        columns = set(sample.columns)

        # Determine schema type based on column names
        if "protocol" in columns and "l7_proto" in columns:
            schema_type = "nf"
            feature_attrs = NF_FEATURE_ATTRIBUTES
            edge_attrs = NF_EDGE_ATTRIBUTES
            print("Detected schema: NetFlow (NF)")
        elif "proto" in columns and "service" in columns and "state" in columns:
            schema_type = "og"
            feature_attrs = OG_FEATURE_ATTRIBUTES
            edge_attrs = OG_EDGE_ATTRIBUTES
            print("Detected schema: Original UNSW-NB15 (OG)")
        else:
            raise ValueError(
                f"Unknown schema type. Available columns: {sorted(columns)}"
            )

        # Load data with appropriate columns (both schemas now use 'ts')
        load_query = f"""
        SELECT
            ROW_NUMBER() OVER () - 1 as index,
            {", ".join(feature_attrs)},
            {", ".join(edge_attrs)}
        FROM read_parquet('{s3_uri}')
        ORDER BY {", ".join(edge_attrs)}, ts
        """

        data = conn.execute(load_query).arrow()
        conn.close()

        print(f"Loaded {len(data)} records from S3")
        print(f"Using {len(feature_attrs)} features and {len(edge_attrs)} edge attributes")

        # Apply scaling
        scaler = model_info["scaler"]
        scaled_data, _ = minmax_scale(data, feature_attrs, scaler)

        # Build graph
        graph = GraphGCN(
            table=scaled_data,
            node_attrs=feature_attrs,
            edge_attrs=edge_attrs,
            label=None,  # No labels for inference
            n_neighbors=GRAPH_N_NEIGHBORS,
        ).build(include_labels=False)

        print(f"Graph: {graph.num_nodes} nodes, {graph.num_edges} edges")

        # Run inference
        model = model_info["model"]
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model = model.to(device)
        model.eval()

        with torch.no_grad():
            graph = graph.to(device)
            output = model(graph.x, graph.edge_index)
            probs = torch.softmax(output, dim=1).cpu().numpy()
            preds = np.argmax(probs, axis=1)

        # Calculate anomalies
        if BINARY_MODE:
            anomaly_probs = probs[:, 1]
            anomalies = anomaly_probs > ANOMALY_THRESHOLD
            anomaly_count = anomalies.sum()
        else:
            # Normal class is index 0, all others are anomalies
            anomaly_probs = 1 - probs[:, 0]
            anomalies = preds != 0
            anomaly_count = anomalies.sum()

        print(f"Detected {anomaly_count} anomalies (threshold: {ANOMALY_THRESHOLD})")

        # Create predictions DataFrame
        pred_df = pd.DataFrame(
            {
                "timestamp": datetime.now(),
                "feature_file": filename,
                "anomaly_score": anomaly_probs,
                "is_anomaly": anomalies,
                "prediction": preds,
                "record_index": range(len(preds)),
            }
        )

        return {
            "predictions_df": pred_df.to_dict("records"),
            "total_records": len(data),
            "total_anomalies": int(anomaly_count),
            "anomaly_rate": float(anomaly_count / len(data)),
            "input_file": file_to_process["key"],
            "inference_timestamp": datetime.now().isoformat(),
            "model_version": model_info["model_version"],
            "schema_type": schema_type,
            "num_features": len(feature_attrs),
        }

    def _run_placeholder_inference(file_to_process):
        """Fallback: Generate placeholder predictions when model unavailable."""
        from pathlib import Path

        import boto3
        import numpy as np
        import pandas as pd

        s3_client = boto3.client("s3", **S3_CONFIG)  # ty: ignore

        # Download and count records
        local_path = f"/tmp/{Path(file_to_process['key']).name}"
        s3_client.download_file(BUCKET_NAME, file_to_process["key"], local_path)

        df = pd.read_parquet(local_path)
        num_records = len(df)
        Path(local_path).unlink()

        # Generate random predictions
        np.random.seed(42)
        anomaly_probs = np.random.random(num_records)
        anomalies = anomaly_probs > ANOMALY_THRESHOLD
        preds = anomalies.astype(int)

        pred_df = pd.DataFrame(
            {
                "timestamp": datetime.now(),
                "feature_file": Path(file_to_process["key"]).name,
                "anomaly_score": anomaly_probs,
                "is_anomaly": anomalies,
                "prediction": preds,
                "record_index": range(num_records),
            }
        )

        return {
            "predictions_df": pred_df.to_dict("records"),
            "total_records": num_records,
            "total_anomalies": int(anomalies.sum()),
            "anomaly_rate": float(anomalies.sum() / num_records),
            "input_file": file_to_process["key"],
            "inference_timestamp": datetime.now().isoformat(),
            "model_version": "placeholder",
            "schema_type": "unknown",
            "num_features": 0,
        }

    @task(outlets=[PREDICTIONS_ASSET])
    def export_predictions(inference_results):
        """Export predictions to parquet in S3."""
        from pathlib import Path

        import boto3
        import pandas as pd

        s3_client = boto3.client("s3", **S3_CONFIG)  # ty: ignore

        # Convert predictions back to DataFrame
        pred_df = pd.DataFrame(inference_results["predictions_df"])

        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        output_filename = f"predictions_{timestamp}.parquet"
        output_key = f"predictions/{output_filename}"

        # Save locally then upload
        local_path = f"/tmp/{output_filename}"
        pred_df.to_parquet(local_path, index=False)

        s3_client.upload_file(local_path, BUCKET_NAME, output_key)

        output_uri = f"s3://{BUCKET_NAME}/{output_key}"
        print(f"\nPredictions exported: {output_uri}")
        print(f"  Records: {len(pred_df)}")
        print(f"  Model: {inference_results['model_version']}")

        # Cleanup
        Path(local_path).unlink()

        return {
            "output_uri": output_uri,
            "output_key": output_key,
            "num_records": len(pred_df),
            "export_timestamp": datetime.now().isoformat(),
        }

    @task()
    def log_anomaly_alerts(inference_results, export_info):
        """Log alerts for detected anomalies."""
        total_anomalies = inference_results["total_anomalies"]
        anomaly_rate = inference_results["anomaly_rate"]
        model_version = inference_results["model_version"]

        print("\n" + "=" * 80)
        print("ANOMALY DETECTION SUMMARY")
        print("=" * 80)
        print(f"Timestamp: {datetime.now().isoformat()}")
        print(f"Input File: {inference_results['input_file']}")
        print(f"Model Version: {model_version}")
        print(f"Total Records: {inference_results['total_records']}")
        print(f"Anomalies Detected: {total_anomalies}")
        print(f"Anomaly Rate: {anomaly_rate:.2%}")
        print(f"Predictions: {export_info['output_uri']}")
        print("=" * 80)

        # Alert if anomaly count exceeds threshold
        if total_anomalies >= ALERT_THRESHOLD:
            alert_msg = (
                "⚠️  HIGH ANOMALY ALERT ⚠️"
                f"Detected {total_anomalies} anomalies ({anomaly_rate:.2%})"
                f"Threshold: {ALERT_THRESHOLD} anomalies"
                f"Model: {model_version}"
                f"Input: {inference_results['input_file']}"
                f"Predictions: {export_info['output_uri']}"
            )

            print(alert_msg)

            # Log as warning
            import logging

            logging.getLogger(__name__).warning(alert_msg)

            return {
                "alert_sent": True,
                "alert_type": "high_anomaly_count",
                "anomaly_count": total_anomalies,
                "anomaly_rate": anomaly_rate,
            }
        else:
            print(
                f"No alert: {total_anomalies} anomalies below threshold ({ALERT_THRESHOLD})"
            )
            return {"alert_sent": False, "reason": "below_threshold"}

    @task()
    def update_processed_tracker(file_info):
        """Update Airflow Variable to track last processed file."""
        file_key = file_info["file_to_process"]["key"]
        Variable.set("last_processed_feature_file", file_key)
        print(f"Updated tracker: {file_key}")
        return {"tracked_file": file_key}

    @task()
    def print_summary(
        file_info, inference_results, export_info, alert_result, tracker_update
    ):
        """Print comprehensive summary of the inference pipeline execution."""
        print("\n" + "=" * 100)
        print("INFERENCE PIPELINE EXECUTION SUMMARY")
        print("=" * 100)

        # Execution metadata
        print(f"\nExecution Time: {datetime.now().isoformat()}")
        print("DAG ID: inference_anomaly_detection")

        # Input file information
        print("\n--- INPUT ---")
        print(f"File Processed: {file_info['file_to_process']['key']}")
        print(f"File Size: {file_info['file_to_process']['size'] / (1024**2):.2f} MB")
        print(f"Last Modified: {file_info['file_to_process']['last_modified']}")
        print(f"Total Files in S3: {file_info['total_files_in_s3']}")

        # Model information
        print("\n--- MODEL ---")
        print(f"Model Name: {MODEL_NAME}")
        print(f"Model Alias: {MLFLOW_MODEL_ALIAS}")
        print(f"Model Version: {inference_results['model_version']}")
        print(f"Detection Mode: {'Binary' if BINARY_MODE else 'Multiclass'}")
        print(f"Feature Schema: {inference_results.get('schema_type', 'unknown').upper()}")
        print(f"Number of Features: {inference_results.get('num_features', 0)}")

        # Inference results
        print("\n--- INFERENCE RESULTS ---")
        print(f"Total Records Processed: {inference_results['total_records']:,}")
        print(f"Anomalies Detected: {inference_results['total_anomalies']:,}")
        print(f"Anomaly Rate: {inference_results['anomaly_rate']:.2%}")
        print(f"Anomaly Threshold: {ANOMALY_THRESHOLD}")
        print(f"Inference Timestamp: {inference_results['inference_timestamp']}")

        # Output information
        print("\n--- OUTPUT ---")
        print(f"Predictions File: {export_info['output_uri']}")
        print(f"Records Exported: {export_info['num_records']:,}")
        print(f"Export Timestamp: {export_info['export_timestamp']}")

        # Alert status
        print("\n--- ALERTS ---")
        if alert_result["alert_sent"]:
            print(f"ALERT SENT: {alert_result['alert_type']}")
            print(
                f"Alert Reason: {alert_result['anomaly_count']} anomalies detected (threshold: {ALERT_THRESHOLD})"
            )
            print(
                f"Alert Severity: HIGH ({alert_result['anomaly_rate']:.2%} anomaly rate)"
            )
        else:
            print(f"No alerts sent ({alert_result.get('reason', 'normal')})")

        # Tracking
        print("\n--- TRACKING ---")
        print(f"Last Processed File Updated: {tracker_update['tracked_file']}")

        print("\n" + "=" * 100)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 100 + "\n")

        return {
            "summary_generated": True,
            "timestamp": datetime.now().isoformat(),
            "pipeline": "inference_anomaly_detection",
        }

    # DAG workflow
    file_info = discover_new_files()
    model_info = load_model_from_mlflow()
    inference_results = run_inference(file_info, model_info)
    export_info = export_predictions(inference_results)
    alert_result = log_anomaly_alerts(inference_results, export_info)
    tracker_update = update_processed_tracker(file_info)
    summary = print_summary(
        file_info, inference_results, export_info, alert_result, tracker_update
    )

    # Chain task dependencies
    chain([file_info, model_info], inference_results, export_info, alert_result)
    chain(file_info, tracker_update, summary)
    chain(alert_result, summary)


# Run DAG
inference_anomaly_detection()
