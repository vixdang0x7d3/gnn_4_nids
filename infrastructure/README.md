# GNN NIDS Infrastructure

Complete Docker-based infrastructure for the GNN Network Intrusion Detection System.

## Services

- **Kafka + Zookeeper**: Message streaming (port 9092)
- **MinIO**: S3-compatible object storage (ports 9000/9001)
- **PostgreSQL**: Metadata storage for MLflow and Airflow (port 5432)
- **MLflow**: Experiment tracking and model registry (port 5000)
- **Airflow**: Workflow orchestration (port 8080)

## Quick Start

```bash
cd infrastructure

# Build packages and Airflow image
make build

# Start all services
make up-all

# Check status
make status
make health
```

**Access Points:**
- Airflow UI: http://localhost:8080 (admin/admin)
- MLflow UI: http://localhost:5000
- MinIO Console: http://localhost:9001 (admin/minioadmin123)
- Kafka: localhost:9092

## Build System

### Local Python Packages

The system includes three local packages with dependencies:
```
graph_building (base)
    ↑
    ├── data_pipeline
    └── model_training
```

These packages are built as wheels and installed into Airflow containers.

### Build Process

```bash
# Build Python wheels (graph_building, data_pipeline, model_training)
make build-packages

# Build Airflow Docker image with packages
make build

# Both steps combined
make build
```

**What happens:**
1. `build_packages.sh` uses `uv` to build wheels for each package
2. Wheels are placed in `infrastructure/airflow/packages/`
3. Docker builds Airflow image and installs wheels in dependency order
4. Packages are verified during build

## Service Control

### Start Services

```bash
# Start base services only (Kafka, MinIO, MLflow, Postgres)
make up-base

# Start Airflow only (requires base services)
make up-airflow

# Start everything
make up-all
```

### Stop Services

```bash
# Stop Airflow
make down-airflow

# Stop all services
make down-all
```

### Restart

```bash
make restart  # Restarts everything
```

## Logs

```bash
# All logs
make logs

# Specific service logs
make logs-kafka
make logs-mlflow
make logs-minio
make logs-airflow
```

## Monitoring

```bash
# Show service status
make status

# Check health of all services
make health

# Verify services are working correctly
make verify
```

## Data Management

### Volumes

- `kafka_data`: Kafka message storage
- `zookeeper_data`: Zookeeper data
- `minio_data`: S3 object storage
- `postgres_data`: PostgreSQL databases (mlflow + airflow)
- `duckdb_data`: DuckDB database file (`/data/zeek_features.db`)
- `archive_data`: Parquet archive files (`/data/archive/`)

### Cleanup

```bash
# Remove Airflow data only
make clean-airflow

# Remove Kafka data only
make clean-kafka

# Remove MLflow data only
make clean-mlflow

# Remove everything (DESTRUCTIVE!)
make clean-all
```

## Airflow Configuration

### DAG Location
- DAGs are mounted from: `../data_pipeline/airflow/dags/`
- Edit DAGs directly in that directory (hot reload)

### Environment Variables
Set in `infrastructure/.env`:
```bash
# PostgreSQL
POSTGRES_USER=mlflow
POSTGRES_PASSWORD=mlflow123

# MinIO
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=minioadmin123

# Airflow
AIRFLOW__CORE__FERNET_KEY=<generated-key>
AIRFLOW__CORE__SECRET_KEY=<generated-key>
```

### Installed Packages

The following local packages are pre-installed in Airflow containers:
- `graph_building` - Graph construction utilities
- `data_pipeline` - ETL pipeline components
- `model_training` - GNN model training utilities

Plus all dependencies from `airflow/requirements.txt`:
- boto3, confluent-kafka, duckdb, mlflow, torch, torch-geometric, etc.

## Development Workflow

### Updating DAGs

DAGs are mounted as volumes, so changes are picked up automatically:

```bash
# Edit DAG
vim ../data_pipeline/airflow/dags/my_dag.py

# Airflow will detect changes (may take ~30 seconds)
# No rebuild needed!
```

### Updating Python Packages

If you modify `graph_building`, `data_pipeline`, or `model_training` code:

```bash
# Rebuild packages and Airflow image
make build

# Restart Airflow services
make down-airflow
make up-airflow
```

### Adding Dependencies

**For base dependencies** (boto3, torch, etc.):
1. Edit `infrastructure/airflow/requirements.txt`
2. Run `make build`

**For local package dependencies**:
1. Edit the package's `pyproject.toml`
2. Run `make build`

## Troubleshooting

### Packages Not Found

```bash
# Verify packages were built
ls -lh infrastructure/airflow/packages/

# Rebuild from scratch
make clean-airflow
make build
make up-airflow
```

### Database Connection Errors

```bash
# Check PostgreSQL is running
docker compose ps postgres

# Check databases exist
docker compose exec postgres psql -U mlflow -l

# Reinitialize Airflow DB
make down-airflow
docker compose run --rm airflow-init
make up-airflow
```

### DuckDB Lock Errors

The streaming ETL pipeline holds a write lock on DuckDB. Airflow DAGs handle this by:
- **Monitoring DAG**: Uses `read_only=True` connection
- **Archival DAG**: Only reads parquet files (no DuckDB access)
- **Inference DAG**: May need retry logic for write operations

See `docs/TODO_COMPLETE.md` for detailed DuckDB concurrency solutions.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Network: ml_platform              │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │  Kafka   │  │  MinIO   │  │ Postgres │  │  MLflow  │   │
│  │  :9092   │  │  :9000   │  │  :5432   │  │  :5000   │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
│       │             │              │             │          │
│       └─────────────┴──────────────┴─────────────┘          │
│                          │                                   │
│                   ┌──────▼──────┐                           │
│                   │   Airflow   │                           │
│                   │   :8080     │                           │
│                   │             │                           │
│                   │ Packages:   │                           │
│                   │ - graph_    │                           │
│                   │   building  │                           │
│                   │ - data_     │                           │
│                   │   pipeline  │                           │
│                   │ - model_    │                           │
│                   │   training  │                           │
│                   └─────────────┘                           │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Next Steps

After starting the infrastructure:

1. **Start Streaming ETL**: Run the data pipeline to consume Kafka messages
2. **Train Model**: Use MLflow to track experiments and save models
3. **Deploy DAGs**: Airflow will automatically pick up DAGs from `data_pipeline/airflow/dags/`

## Reference

- Airflow Docs: https://airflow.apache.org/docs/
- MLflow Docs: https://mlflow.org/docs/latest/
- MinIO Docs: https://min.io/docs/
- Kafka Docs: https://kafka.apache.org/documentation/
