# GNN-NIDS: Essential Orchestration & MLOps Concepts

Focus: Airflow, MLflow, Kafka, and pipeline patterns needed to operate this system.

---

## 1. Apache Airflow Core Concepts

### 1.1 DAG Execution Model

**Execution Context**:
```python
def my_task(**context):
    context['ti']              # TaskInstance - for XCom
    context['execution_date']  # Logical date (data being processed)
    context['ts_nodash']       # Timestamp without dashes (for IDs)
    context['dag_run']         # Current DAG run object
```

**Key Point**: `execution_date` ≠ actual run time. If scheduled daily at midnight, execution_date is yesterday's date.

---

**XCom (Cross-Communication)**:
- Pass small metadata between tasks (< 48KB)
- Stored in Airflow's PostgreSQL
```python
# Task 1: Return value auto-pushed to XCom
def extract(**context):
    return {'batch_id': '123', 'path': '/tmp/data.parquet'}

# Task 2: Pull from XCom
def transform(**context):
    ti = context['ti']
    metadata = ti.xcom_pull(task_ids='extract')
    path = metadata['path']
```

**Anti-pattern**: Don't pass large data through XCom. Pass file paths instead.

---

**Task Dependencies - Parallel vs Sequential**:
```python
# Sequential
task1 >> task2 >> task3

# Parallel then join
task1 >> [task2, task3] >> task4

# Fan-out then fan-in
[task1, task2] >> task3 >> [task4, task5] >> task6
```

**In Your Project**:
```python
# Inference DAG: Load model in parallel with preprocessing
query >> [load_model, preprocess] >> inference >> update
```

---

### 1.2 Scheduling & Triggering

**Schedule Interval Gotcha**:
```python
schedule_interval='*/5 * * * *'  # Every 5 minutes
start_date=datetime(2025, 1, 1)
catchup=False  # IMPORTANT: Don't backfill
```

**catchup=False**: Only run future schedules, don't process historical dates.
**catchup=True**: Backfill from start_date to now (rarely what you want).

---

**Trigger Rules** (when does task execute?):
```python
trigger_rule='all_success'      # Default: all parents succeeded
trigger_rule='all_done'         # All parents finished (any state)
trigger_rule='none_failed'      # No parent failed (success or skipped OK)
```

**Use Case**:
```python
# Cleanup runs even if alert task skipped
cleanup = PythonOperator(
    task_id='cleanup',
    trigger_rule='all_done'  # Run regardless
)
```

---

### 1.3 Error Handling

**AirflowSkipException** (not a failure):
```python
from airflow.exceptions import AirflowSkipException

def task(**context):
    if no_data_available:
        raise AirflowSkipException("No data to process")
    # ... normal logic
```

**Effect**: Task marked `skipped` (not `failed`), DAG run still succeeds.

**Use in your project**:
- Inference DAG: No unpredicted flows
- Lifecycle DAG: No old data to archive

---

**Retries**:
```python
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}
```

**When retries help**: Network glitches, transient database locks
**When retries don't help**: Code bugs, missing dependencies

---

### 1.4 Connections & Variables

**Airflow Connections** (credentials):
```bash
# Add via CLI
airflow connections add 'minio_s3' \
  --conn-type 'aws' \
  --conn-extra '{"endpoint_url": "http://minio:9000", ...}'
```

**Access in code**:
```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
s3 = S3Hook(aws_conn_id='minio_s3')
```

**Airflow Variables** (config):
```bash
airflow variables set KAFKA_BROKER "kafka:9092"
```

```python
from airflow.models import Variable
broker = Variable.get("KAFKA_BROKER")
```

---

### 1.5 LocalExecutor Behavior

**How it works**:
- Airflow scheduler spawns separate Python processes for tasks
- Each task runs in isolation (separate process)
- Parallelism limited by `AIRFLOW__CORE__PARALLELISM` setting

**Implications**:
- Can't share in-memory state between tasks (use XCom/files)
- Each task loads libraries fresh (model loading per task)
- Process overhead (~100MB per task)

**Config**:
```yaml
AIRFLOW__CORE__PARALLELISM=32        # Max tasks system-wide
AIRFLOW__CORE__DAG_CONCURRENCY=16    # Max tasks per DAG
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1  # Concurrent DAG runs
```

---

## 2. MLflow Core Concepts

### 2.1 Tracking Architecture

**Components**:
```
Training Code
    ↓ (HTTP)
MLflow Tracking Server
    ↓ (metadata)          ↓ (artifacts)
PostgreSQL              MinIO
(params, metrics)       (models, files)
```

**Setup**:
```python
mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("gnn-nids-binary-training")
```

---

### 2.2 Experiments & Runs

**Experiment** = logical grouping of related runs
**Run** = single training execution

```python
with mlflow.start_run():
    # Log hyperparameters (logged once)
    mlflow.log_params({
        'n_convs': 64,
        'n_hidden': 512,
        'learning_rate': 0.01
    })

    # Training loop
    for epoch in range(epochs):
        # Log metrics (logged per step)
        mlflow.log_metrics({
            'train_loss': loss,
            'val_loss': val_loss
        }, step=epoch)

    # Log artifacts (logged once)
    mlflow.pytorch.log_model(model, "model")
    mlflow.log_artifact("confusion_matrix.png")

    # Get run ID for later reference
    run_id = mlflow.active_run().info.run_id
```

**Key distinction**:
- **Params**: Inputs (hyperparameters, config)
- **Metrics**: Outputs (losses, accuracies) - can be time-series
- **Artifacts**: Files (models, plots, data)

---

### 2.3 Model Registry

**Lifecycle**: Train → Register → Stage → Promote

```python
from mlflow.tracking import MlflowClient
client = MlflowClient()

# 1. Register model from run
model_uri = f"runs:/{run_id}/model"
version = mlflow.register_model(model_uri, "gnn-nids-binary")

# 2. Transition to Staging
client.transition_model_version_stage(
    name="gnn-nids-binary",
    version=version.version,
    stage="Staging"
)

# 3. After validation, promote to Production
client.transition_model_version_stage(
    name="gnn-nids-binary",
    version=version.version,
    stage="Production",
    archive_existing_versions=True  # Old production → Archived
)
```

**Stages**:
- **None**: Just registered
- **Staging**: Being tested
- **Production**: Active in inference
- **Archived**: Deprecated

---

### 2.4 Loading Models

**Three ways**:

```python
# 1. By run ID
model = mlflow.pytorch.load_model(f"runs:/{run_id}/model")

# 2. By version number
model = mlflow.pytorch.load_model("models:/gnn-nids-binary/4")

# 3. By stage (BEST for production)
model = mlflow.pytorch.load_model("models:/gnn-nids-binary/Production")
```

**Your inference DAG uses**: Stage-based loading (`Production`)

**Why**: New model promoted → inference automatically uses it (no code change)

---

### 2.5 Artifacts

**Downloading artifacts**:
```python
# Download from run
artifact_path = mlflow.artifacts.download_artifacts(
    artifact_uri="runs:/{run_id}/scaler/fitted_scaler.pkl"
)

# Download from model version
scaler_path = mlflow.artifacts.download_artifacts(
    artifact_uri="models:/gnn-nids-binary/Production/scaler"
)
```

**Storage**: MinIO (S3-compatible) at `s3://mlflow-artifacts/{experiment_id}/{run_id}/`

---

### 2.6 Tags & Search

**Tags** (organize runs):
```python
mlflow.set_tag("model_type", "binary")
mlflow.set_tag("data_version", "2025-01-15")
```

**Search runs** (in UI or programmatically):
```python
runs = client.search_runs(
    experiment_ids=["1"],
    filter_string="metrics.test_accuracy > 0.95",
    order_by=["metrics.test_accuracy DESC"]
)
```

---

## 3. Apache Kafka Concepts

### 3.1 Consumer Groups

**Concept**: Coordinate multiple consumers for parallel processing

```
Topic: zeek-logs (3 partitions)

Consumer Group: airflow-etl
  Consumer 1: Reads partitions 0, 1
  Consumer 2: Reads partition 2

Consumer Group: backup-consumer  (independent)
  Consumer 1: Reads all partitions 0, 1, 2
```

**Key point**: Each consumer group tracks its own offsets independently.

---

### 3.2 Offsets & Commit Strategy

**Offset** = position in partition (which message you're on)

**At-Least-Once Delivery** (used in your project):
```python
consumer = Consumer({
    'enable.auto.commit': False  # Manual control
})

# 1. Read messages
messages = consumer.consume(batch_size, timeout)

# 2. Process messages
process_and_save(messages)

# 3. Commit offsets AFTER success
consumer.commit()
```

**If crash before commit**: Re-process messages (duplicate processing)
**Solution**: Idempotent writes (`ON CONFLICT DO NOTHING`)

---

**Auto-commit (DON'T use)**:
```python
'enable.auto.commit': True  # Commits before processing
```
**Problem**: If crash during processing, messages lost.

---

### 3.3 Batch Consumption

**Your pattern**:
```python
def consume_batch(max_messages=5000, timeout=60):
    messages = []
    start = time.time()

    while len(messages) < max_messages:
        if time.time() - start > timeout:
            break  # Return what we have

        msg = consumer.poll(timeout=1.0)
        if msg:
            messages.append(msg)

    return messages
```

**Trade-offs**:
- Larger batch: Better throughput, more memory
- Longer timeout: Wait for more data, higher latency
- Your config: 5000 messages, 60s timeout

---

### 3.4 Consumer Lag

**Lag** = How far behind real-time you are

```bash
kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group airflow-etl --describe
```

Output:
```
GROUP       TOPIC      PARTITION  CURRENT  LOG-END  LAG
airflow-etl zeek-logs  0          12345    12500    155
```

**Lag = 155**: 155 messages behind
**If lag growing**: Consumers can't keep up with producers

---

## 4. Pipeline Patterns

### 4.1 Hot/Cold Path Pattern

**Pattern**: Dual storage optimized for different access patterns

```
Kafka Stream → ETL DAG
                  ↓
        ┌─────────┴─────────┐
        ↓                   ↓
    Hot Path            Cold Path
    DuckDB              Parquet/MinIO
    (last 24h)          (all history)
    Fast queries        Batch processing
        ↓                   ↓
    Inference           Training
```

**Hot Path**: Recent data, mutable (updates), fast random access
**Cold Path**: Historical data, immutable (append-only), cheap storage

**Your implementation**:
- Hot: DuckDB with prediction columns (update in-place)
- Cold: Daily Parquet partitions (write once, read many)

---

### 4.2 Micro-batching Pattern

**Pattern**: Process streaming data in small batches

**vs Pure Streaming**: Process record-by-record (high overhead)
**vs Batch**: Process daily/hourly (high latency)
**Micro-batch**: Every 5 minutes (balance throughput & latency)

**Your DAGs**:
- ETL: Every 5 min, batch size 5000 messages
- Inference: Every 5 min, batch size 10000 flows

---

### 4.3 Dual Write Pattern

**Pattern**: Write to multiple destinations

```python
validate_task >> hot_path_task >> cold_path_task
```

**Challenge**: What if one fails?
**Solution**: Sequential writes (hot then cold), both must succeed

**Alternative** (not used): Parallel writes with distributed transaction

---

### 4.4 Idempotency Pattern

**Pattern**: Running multiple times produces same result

**Your implementations**:

```python
# Hot path: Prevent duplicate inserts
INSERT INTO flows (uid, ...)
VALUES (?, ...)
ON CONFLICT (uid) DO NOTHING

# Inference: Update by UID (overwrite OK)
UPDATE flows
SET prediction = ?, confidence = ?
WHERE uid = ?
```

**Why critical**: Airflow retries on failure, must be safe to re-run

---

### 4.5 Time-based Partitioning

**Pattern**: Organize by time windows

```
cold_path/
  ├── 2025-01-01/  ← Daily partition
  │   ├── batch_080000.parquet
  │   └── batch_080500.parquet
  ├── 2025-01-02/
  └── 2025-01-03/
```

**Query optimization**: Only read needed date range
```python
# Training DAG: Load last 21 days
table = storage.read_date_range(start_date, end_date)
```

---

### 4.6 Lambda Architecture

**Pattern**: Combine batch and streaming processing

```
Streaming Layer (Speed):  Kafka → ETL → Hot Path → Inference
Batch Layer:              Cold Path → Training → Model Registry
Serving Layer:            DuckDB (predictions) + MLflow (models)
```

**Your system**: Classic lambda architecture implementation

---

## 5. ML Pipeline Patterns

### 5.1 Early Stopping

**Pattern**: Stop training when validation stops improving

```python
best_val_loss = float('inf')
trigger_times = 0
patience = 10

for epoch in range(max_epochs):
    val_loss = validate()

    if val_loss < best_val_loss:
        best_val_loss = val_loss
        save_checkpoint()
        trigger_times = 0
    else:
        trigger_times += 1
        if trigger_times >= patience:
            break  # Stop early
```

**Logged to MLflow**: All epochs, not just final

---

### 5.2 Model Promotion Pipeline

**Pattern**: Automated quality gates

```
Train → Evaluate → Check Accuracy → Register → Staging
                         ↓ (if < 95%)
                     Fail DAG

Staging → Manual/Auto Test → Promote → Production
```

**Your training DAG**:
```python
if test_accuracy < 0.95:
    raise AirflowException("Model quality check failed")
```

---

### 5.3 Batch Inference Pattern

**Pattern**: Process accumulated data periodically

```
Every 5 minutes:
  1. Query unpredicted flows (WHERE prediction IS NULL)
  2. Load model from MLflow
  3. Batch predict
  4. Update database (SET prediction = ?, predicted_at = NOW())
```

**vs Real-time**: API responds to individual requests
**Trade-off**: Latency (0-5 min) for higher throughput

---

### 5.4 Model Caching (Not Implemented, But Should Consider)

**Problem**: Loading model every DAG run is slow

**Pattern**: Keep model in memory between runs

```python
# Global cache
_model_cache = {'version': None, 'model': None}

def load_model_cached():
    current_version = get_production_version()

    if _model_cache['version'] != current_version:
        _model_cache['model'] = mlflow.pytorch.load_model(...)
        _model_cache['version'] = current_version

    return _model_cache['model']
```

**Challenge**: Airflow LocalExecutor spawns new processes (cache doesn't persist)
**Solution**: Need persistent worker or API service

---

## 6. Operational Concepts

### 6.1 Airflow DAG Lifecycle

**States**:
1. **Paused**: DAG won't be scheduled (default for new DAGs)
2. **Active**: DAG scheduling enabled
3. **Running**: DAG run in progress
4. **Success**: DAG run completed successfully
5. **Failed**: At least one task failed

**DAG run vs Task instance**:
- DAG run: Single execution of entire workflow
- Task instance: Single execution of one task

---

### 6.2 Backpressure Handling

**Problem**: Producer faster than consumer (Kafka lag grows)

**Solutions**:
1. **Increase consumer throughput**: Larger batch size, faster processing
2. **Add more consumers**: Scale horizontally (multiple Airflow workers)
3. **Increase DAG frequency**: Every 2 min instead of 5 min
4. **Alert on lag**: Monitor and investigate

**Your monitoring**:
```bash
# Check consumer lag
kafka-consumer-groups --group airflow-etl --describe
```

---

### 6.3 Failure Recovery

**Scenarios**:

1. **Task fails**: Airflow retries (3x), then marks failed
2. **DAG fails**: Fix issue, clear failed task, re-run
3. **Data corruption**: Fix source, clear tasks, backfill
4. **Service down** (Kafka/MLflow): Task retries automatically

**Clear and re-run**:
```bash
# Clear task state
airflow tasks clear kafka_etl_pipeline -t transform_data -s 2025-01-15

# Re-run DAG
airflow dags trigger kafka_etl_pipeline
```

---

### 6.4 Docker Service Communication

**DNS resolution**:
```python
# Inside container, use service name (not localhost)
mlflow.set_tracking_uri("http://mlflow:5000")  # ✓ Correct
mlflow.set_tracking_uri("http://localhost:5000")  # ✗ Wrong (localhost = container itself)
```

**Docker network**: All services on `ml_platform` network can resolve each other's names

---

### 6.5 Volume Mounts

**Bind mounts** (code changes reflected immediately):
```yaml
volumes:
  - ../data_pipeline:/opt/airflow/data_pipeline
```

**Effect**: Edit code on host → available in container immediately
**Use for**: Development, DAG files, source code

**Named volumes** (persistent data):
```yaml
volumes:
  - postgres_data:/var/lib/postgresql/data
```

**Effect**: Data persists even if container deleted

---

## 7. Key Gotchas & Best Practices

### Airflow
- ❌ Don't use global variables in DAGs (loaded once, stale state)
- ✓ Use XCom for small metadata only (< 48KB)
- ✓ Always set `catchup=False` for real-time pipelines
- ✓ Use `provide_context=True` to access execution context

### MLflow
- ✓ Log params before training, metrics during, artifacts after
- ✓ Load by stage (`Production`) not version number in inference
- ❌ Don't commit large artifacts to git (use MLflow/MinIO)
- ✓ Tag runs with data version for reproducibility

### Kafka
- ✓ Manual commit after successful processing
- ✓ Use consumer groups for parallel processing
- ❌ Don't auto-commit (risk data loss)
- ✓ Monitor consumer lag regularly

### Data Pipeline
- ✓ Idempotent writes (safe to re-run)
- ✓ Validate data before and after transformation
- ✓ Use same scaler for training and inference
- ❌ Don't pass large data through XCom (use files)

---

## 8. Quick Reference

### Airflow CLI
```bash
# List DAGs
airflow dags list

# Trigger DAG
airflow dags trigger kafka_etl_pipeline

# View task logs
airflow tasks logs kafka_etl_pipeline transform_data 2025-01-15

# Clear failed task
airflow tasks clear kafka_etl_pipeline -t transform_data -s 2025-01-15
```

### MLflow CLI
```bash
# List experiments
mlflow experiments list

# View run
mlflow runs describe --run-id <run_id>

# Search runs
mlflow runs list --experiment-id 1
```

### Kafka CLI
```bash
# List topics
kafka-topics --bootstrap-server localhost:9092 --list

# Consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group airflow-etl --describe

# Read from topic
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic zeek-logs --from-beginning
```

### Docker Compose
```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f airflow-scheduler

# Restart service
docker-compose restart airflow-scheduler

# Stop all
docker-compose down
```

---

## Focus Areas for Learning

**Priority 1 (Must Know)**:
- Airflow: DAG structure, XCom, task dependencies, error handling
- MLflow: Tracking runs, model registry, loading models
- Kafka: Consumer groups, offsets, manual commit
- Hot/cold path pattern

**Priority 2 (Should Know)**:
- Airflow: Scheduling, trigger rules, connections
- MLflow: Artifacts, stages, versioning
- Kafka: Consumer lag, batch size tuning
- Idempotency, micro-batching

**Priority 3 (Nice to Know)**:
- Airflow: Advanced scheduling, pools, task groups
- MLflow: Tags, search, autologging
- Kafka: Partitions, replication
- Model monitoring, drift detection

---

**Next Steps**: Focus on understanding how these concepts interact in your specific DAGs. Read through the DAG implementations with this guide as reference.
