"""
Inference Pipeline

Triggered by FEATURES_ASSET when new feature files are detected.
Runs GNN inference and emits PREDICTIONS_ASSET.

Workflow:
1. Discover new feature files from triggering asset event
2. Load GNN model from MLflow (with alias)
3. Build graph and run inference
4. Export predictions to S3
5. Emit PREDICTIONS_ASSET for downstream alerting

Schedule: Asset-driven (triggered by FEATURES_ASSET)
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import mlflow
from airflow.exceptions import AirflowSkipException
from airflow.sdk import DAG, Variable, task

sys.path.insert(0, "/opt/airflow/dags")

from assets import (
    ANOMALY_THRESHOLD,
    BINARY_MODE,
    BUCKET_NAME,
    FEATURE_ASSET,
    GRAPH_N_NEIGHBORS,
    MLFLOW_MODEL_ALIAS,
    MLFLOW_TRACKING_URI,
    MODEL_NAME,
    NF_EDGE_ATTRIBUTES,
    NF_FEATURE_ATTRIBUTES,
    OG_EDGE_ATTRIBUTES,
    OG_FEATURE_ATTRIBUTES,
    PREDICTION_ASSET,
    S3_CONFIG,
)

default_args = {
    "owner": "gnn-nids",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=1),
}

with DAG(
    dag_id="anomaly_detection",
    description="GNN anomaly detection inference",
    schedule=[FEATURE_ASSET],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    @task()
    def get_triggering_files(*, inlet_events):
        """
        Get file information from the triggering asset event.

        The inlet_events context provides metadata from the upstream
        asset emission, including which files triggered this run.
        """
        import boto3

        # inlet_events is InletEventsAccessors, not a dict
        # Access with bracket notation, not .get()
        try:
            events = inlet_events[FEATURE_ASSET]
            if events:
                latest_event = events[-1]
                extra = latest_event.extra or {}
                print(f"Triggered by asset event at {latest_event.timestamp}")
                print(f"Event metadata: {extra}")
        except (KeyError, IndexError):
            print("No asset events (manual trigger or empty)")

        s3_client = boto3.client("s3", **S3_CONFIG)  # type: ignore

        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME, Prefix=FEATURE_ASSET.extra["prefix"]
        )

        if "Contents" not in response:
            raise AirflowSkipException("No feature files found in S3")

        files = [
            {
                "key": obj["Key"],
                "size": obj["Size"],
                "last_modified": obj["LastModified"].isoformat(),
                "s3_uri": f"s3://{BUCKET_NAME}/{obj['Key']}",
            }
            for obj in response["Contents"]
            if obj["Key"].endswith(".parquet") and "_features_" in obj["Key"]
        ]

        if not files:
            raise AirflowSkipException("No feature files found")

        files.sort(key=lambda x: x["last_modified"], reverse=True)

        file_to_process = files[0]

        print(f"\nProcessing: {file_to_process['key']}")
        print(f"Size: {file_to_process['size'] / (1024**2):.2f} MB")
        print(f"Modified: {file_to_process['last_modified']}")

        return {
            "file": file_to_process,
            "total_available": len(files),
            "trigger_time": datetime.now().isoformat(),
        }

    @task()
    def load_model():
        """
        Load GNN model and scaler from MLflow.

        Uses model alias (champion) for production model selection.
        Saves artifacts to /tmp for use in inference task.
        """

        import pickle

        import torch

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

        model_uri = f"models:/{MODEL_NAME}@{MLFLOW_MODEL_ALIAS}"
        model_path = "/tmp/gnn_model.pt"
        scaler_path = "/tmp/scaler.pkl"

        try:
            # Load model
            model = mlflow.pytorch.load_model(
                model_uri, map_location=torch.device("cpu")
            )

            # Get version info
            client = mlflow.MlflowClient()
            model_version = client.get_model_version_by_alias(
                MODEL_NAME, MLFLOW_MODEL_ALIAS
            )

            if not model_version:
                raise ValueError(f"No model found: {MODEL_NAME}@{MLFLOW_MODEL_ALIAS}")

            run_id = model_version.run_id

            # Download scaler
            scaler_artifact = mlflow.artifacts.download_artifacts(
                run_id=run_id,
                artifact_path="preprocessor/fitted_mm_scaler.pkl",
                dst_path="/tmp/mlflow_artifacts",
            )

            # Save model
            # Artifacts can't be passed through XCom

            torch.save(model, model_path)

            # Copy scaler
            with open(scaler_artifact, "rb") as f:
                scaler = pickle.load(f)
            with open(scaler_path, "wb") as f:
                pickle.dump(scaler, f)

            print(f"Model loaded: {MODEL_NAME}@{MLFLOW_MODEL_ALIAS}")
            print(f"Version: {model_version.version}")
            print(f"Run ID: {run_id}")

            return {
                "model_path": model_path,
                "scaler_path": scaler_path,
                "model_version": str(model_version.version),
                "run_id": run_id,
                "loaded": True,
            }

        except Exception as e:
            print(f"MLflow model loading failed: {e}")
            print("Using placeholder mode")

            return {
                "model_path": None,
                "scaler_path": None,
                "model_version": "placeholder",
                "run_id": None,
                "loaded": False,
                "error": str(e),
            }

    @task()
    def run_inference(file_info, model_info):
        """
        Run GNN inference on featurkke data.

        1. Load data from S3 via DuckDB
        2. Detect feature schema from Airflow variable
        3. Build input graph
        4. Run model inference
        5. Return predictions
        """

        import pickle
        from pathlib import Path

        import duckdb
        import mlflow
        import numpy as np
        import pandas as pd
        import torch
        from graph_building import GraphGCN
        from graph_building.transform import minmax_scale

        file_to_process = file_info["file"]
        s3_uri = file_to_process["s3_uri"]
        filename = Path(file_to_process["key"]).name

        print("\n")
        print("=" * 60)
        print(f"INFERENCE: {filename}")
        print("=" * 60)

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

        schema_type = Variable.get("inference_schema", default="NF")

        if schema_type == "NF":
            feature_attrs = NF_FEATURE_ATTRIBUTES
            edge_attrs = NF_EDGE_ATTRIBUTES
        elif schema_type == "OG":
            feature_attrs = OG_FEATURE_ATTRIBUTES
            edge_attrs = OG_EDGE_ATTRIBUTES
        else:
            raise ValueError(
                f"Unknown schema type: {schema_type}, possible values: 'NF', 'OG'"
            )

        print(f"Schema type: {schema_type}")
        print(f"Feature attributes: {feature_attrs}")
        print(f"Edge attributes: {edge_attrs}")

        if not model_info["loaded"]:
            return _placeholder_inference(file_to_process, conn, schema_type)

        # Load data with uid and ts for indexing
        query = f"""
            SELECT
                uid,
                ts,
                {", ".join(feature_attrs)},
                {", ".join(edge_attrs)}
            FROM read_parquet('{s3_uri}')
            ORDER BY {", ".join(edge_attrs)}, ts
        """

        data = conn.execute(query).fetch_arrow_table()
        conn.close()

        num_records = data.num_rows

        print(f"Records: {num_records}")

        # Extract identifiers before processing
        uid_col = data.column("uid").to_pylist()
        ts_col = data.column("ts").to_pylist()

        # Load scaler and scale data
        with open(model_info["scaler_path"], "rb") as f:
            scaler = pickle.load(f)

        scaled_data, _ = minmax_scale(data, feature_attrs, scaler)

        # Build graph
        graph = GraphGCN(
            table=scaled_data,
            node_attrs=feature_attrs,
            edge_attrs=edge_attrs,
            label=None,
            n_neighbors=GRAPH_N_NEIGHBORS,
        ).build(include_labels=False)

        print(f"Graph: {graph.num_nodes} nodes, {graph.num_edges} edges")

        # Run inference
        model = torch.load(
            model_info["model_path"],
            weights_only=False,
            map_location=torch.device("cpu"),
        )
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model = model.to(device).eval()

        with torch.no_grad():
            graph = graph.to(device)
            output = model(graph.x, graph.edge_index)
            probs = torch.softmax(output, dim=1).cpu().numpy()
            preds = np.argmax(probs, axis=1)

        # Calculate anomalies
        if BINARY_MODE:
            anomaly_probs = probs[:, 1]
            anomalies = anomaly_probs > ANOMALY_THRESHOLD
        else:
            anomaly_probs = 1 - probs[:, 0]
            anomalies = preds != 0

        anomaly_probs = np.nan_to_num(anomaly_probs, nan=0.0, posinf=1.0, neginf=0.0)

        anomaly_count = int(anomalies.sum())
        anomaly_rate = float(anomaly_count / num_records) if num_records > 0 else 0.0

        print(f"Anomalies: {anomaly_count} ({anomaly_rate:.2%})")

        # Create predictions DataFrame indexed by uid and ts
        pred_df = pd.DataFrame(
            {
                "uid": uid_col,
                "ts": ts_col,
                "prediction": preds,
                "anomaly_score": anomaly_probs,
                "is_anomaly": anomalies,
                "inference_ts": datetime.now().isoformat(),
                "source_file": filename,
            }
        )

        return {
            "predictions": pred_df.to_dict("records"),
            "total_records": int(data.num_rows),
            "anomaly_count": anomaly_count,
            "anomaly_rate": anomaly_rate,
            "source_file": file_to_process["key"],
            "schema_type": schema_type,
            "model_version": model_info["model_version"],
            "inference_time": datetime.now().isoformat(),
        }

    def _placeholder_inference(
        file_to_process,
        conn,
        schema_type,
    ) -> dict:
        """Fallback when model unavailable."""

        import numpy as np
        import pandas as pd

        s3_uri = file_to_process["s3_uri"]
        filename = Path(file_to_process["key"]).name

        # Load data via DuckDB httpfs
        df = conn.execute(f"SELECT uid, ts FROM read_parquet('{s3_uri}')").df()
        conn.close()

        num_records = len(df)

        # Extract identifiers
        uid_col = df["uid"].tolist()
        ts_col = df["ts"].tolist()

        np.random.seed(42)
        anomaly_probs = np.random.random(num_records)
        anomalies = anomaly_probs > ANOMALY_THRESHOLD

        pred_df = pd.DataFrame(
            {
                "uid": uid_col,
                "ts": ts_col,
                "prediction": anomalies.astype(int),
                "anomaly_score": anomaly_probs,
                "is_anomaly": anomalies,
                "inference_ts": datetime.now().isoformat(),
                "source_file": filename,
            }
        )

        return {
            "predictions": pred_df.to_dict("records"),
            "total_records": num_records,
            "anomaly_count": int(anomalies.sum()),
            "anomaly_rate": float(anomalies.sum() / num_records),
            "source_file": file_to_process["key"],
            "schema_type": schema_type,
            "model_version": "placeholder",
            "inference_time": datetime.now().isoformat(),
        }

    @task(outlets=[PREDICTION_ASSET])
    def export_predictions(inference_results):
        """
        Export predictions to S3 and emit PREDICTIONS_ASSET.

        The outlet declaration ensures PREDICTIONS_ASSET is updated
        when this task completes successfully.

        Output schema:
        - uid: Flow primary key (from Zeek)
        - ts: Original event timestamp
        - prediction: Model prediction class
        - anomaly_score: Probability score (0-1)
        - is_anomaly: Binary anomaly flag
        - inference_ts: When inference was performed
        - source_file: Original feature file
        """
        import boto3
        import pandas as pd

        s3_client = boto3.client("s3", **S3_CONFIG)  # type: ignore

        # Convert to DataFrame
        pred_df = pd.DataFrame(inference_results["predictions"])

        # Generate output path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"predictions_{timestamp}.parquet"
        output_key = f"predictions/{output_filename}"

        # Save and upload
        local_path = f"/tmp/{output_filename}"
        pred_df.to_parquet(local_path, index=False)
        s3_client.upload_file(local_path, BUCKET_NAME, output_key)
        Path(local_path).unlink()

        output_uri = f"s3://{BUCKET_NAME}/{output_key}"

        print(f"\n{'=' * 60}")
        print("PREDICTIONS EXPORTED")
        print(f"{'=' * 60}")
        print(f"Output: {output_uri}")
        print(f"Records: {len(pred_df)}")
        print(f"Anomalies: {inference_results['anomaly_count']}")
        print(f"Schema: {inference_results['schema_type']}")
        print(f"Model: {inference_results['model_version']}")
        print(f"Columns: {list(pred_df.columns)}")
        print(f"{'=' * 60}")
        print("PREDICTIONS_ASSET emitted -> Alerting pipeline will trigger")
        print(f"{'=' * 60}\n")

        # Return metadata (also available to downstream via inlet_events)
        return {
            "output_uri": output_uri,
            "output_key": output_key,
            "record_count": len(pred_df),
            "anomaly_count": inference_results["anomaly_count"],
            "anomaly_rate": inference_results["anomaly_rate"],
            "source_file": inference_results["source_file"],
            "schema_type": inference_results["schema_type"],
            "model_version": inference_results["model_version"],
            "export_time": datetime.now().isoformat(),
        }

    # DAG workflow with parallel model loading
    file_info = get_triggering_files()
    model_info = load_model()
    inference_results = run_inference(file_info, model_info)
    export_info = export_predictions(inference_results)

    # Dependencies: file_info and model_info run in parallel
    [file_info, model_info] >> inference_results >> export_info
