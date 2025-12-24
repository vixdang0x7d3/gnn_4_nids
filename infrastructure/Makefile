.PHONY: help build start-base start-all stop-all clean-all

# ============================================
# HELP
# ============================================
help:
	@echo "GNN NIDS Infrastructure Management"
	@echo ""
	@echo "Commands:"
	@echo "  make build        - Build all packages and Docker images"
	@echo "  make start-base   - Start base services (Postgres, MinIO, Kafka, MLflow)"
	@echo "  make start-all    - Start all services (base + Airflow + data_pipeline)"
	@echo "  make stop-all     - Stop all services"
	@echo "  make clean-all    - Remove all volumes and containers (DESTRUCTIVE)"
	@echo ""

# ============================================
# BUILD
# ============================================
build-packages:
	@echo "==> Building Python packages..."
	@chmod +x build_packages.sh
	@./build_packages.sh

build: build-packages
	@echo "==> Building Docker images..."
	@docker compose build airflow-webserver airflow-scheduler airflow-dag-processor airflow-init data_pipeline mlflow

# ============================================
# START SERVICES
# ============================================
start-base:
	@echo "==> Starting base services (Postgres, MinIO, Kafka, MLflow)..."
	@docker compose up -d zookeeper broker minio minio-init postgres mlflow
	@echo ""
	@echo "   Base services started!"
	@echo "   Postgres: localhost:5432"
	@echo "   MinIO API: http://localhost:9000"
	@echo "   MinIO UI: http://localhost:9001 (admin/minioadmin123)"
	@echo "   MLflow: http://localhost:5000"
	@echo "   Kafka: localhost:9092"

start-all:
	@echo "==> Starting all services..."
	@docker compose up -d zookeeper broker minio minio-init postgres mlflow
	@echo "==> Initializing Airflow database..."
	@docker compose run --rm airflow-init
	@echo "==> Starting Airflow and data_pipeline..."
	@docker compose up -d airflow-webserver airflow-scheduler airflow-dag-processor data_pipeline
	@echo ""
	@echo "   All services started!"
	@echo "   Postgres: localhost:5432"
	@echo "   MinIO API: http://localhost:9000"
	@echo "   MinIO UI: http://localhost:9001 (admin/minioadmin123)"
	@echo "   MLflow: http://localhost:5000"
	@echo "   Kafka: localhost:9092"
	@echo "   Airflow: http://localhost:8080"

# ============================================
# STOP SERVICES
# ============================================
stop-all:
	@echo "==> Stopping all services..."
	@docker compose down

# ============================================
# CLEANUP
# ============================================
clean-all:
	@echo ""
	@echo "   WARNING: This will delete ALL data (volumes, containers)!"
	@echo ""
	@read -p "Are you sure? Type 'yes' to continue: " confirm && [ "$$confirm" = "yes" ] || (echo "Cancelled." && exit 1)
	@docker compose down -v
	@rm -rf airflow/packages/*.whl data_pipeline/packages/*.whl 2>/dev/null || true
	@echo "All volumes and data removed"
	@echo "Built packages removed"
