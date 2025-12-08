.PHONY: help build up down restart logs status health verify clean \
        build-packages \
        up-base up-airflow up-all \
        down-airflow down-all \
        logs-kafka logs-mlflow logs-minio logs-airflow \
        clean-kafka clean-mlflow clean-airflow clean-all

# ============================================
# HELP
# ============================================
help:
	@echo "GNN NIDS Infrastructure Management"
	@echo ""
	@echo "Quick Start:"
	@echo "  make build           - Build all images (pipeline + Airflow + MLflow + local packages)"
	@echo "  make up-all          - Start all services (base + MLflow + Airflow + pipeline)"
	@echo "  make down-all        - Stop all services"
	@echo "  make logs            - View all logs"
	@echo "  make status          - Show service status"
	@echo "  make health          - Check service health"
	@echo ""
	@echo "Build Commands:"
	@echo "  make build-packages  - Build Python wheels (graph_building, data_pipeline, model_training)"
	@echo "  make build           - Build MLflow; build Airflow image and pipeline image with packages"
	@echo ""
	@echo "Service Control:"
	@echo "  make up-base         - Start base services (Kafka, MinIO, Postgres)"
	@echo "  make up-mlflow       - Start MLflow services only"
	@echo "  make up-airflow      - Start Airflow services only"
	@echo "  make up-pipeline     - Start pipeline services only"
	@echo "  make up-all          - Start everything (base + Airflow + MLflow + pipeline)"
	@echo "  make down-mlflow     - Stop MLflow services"
	@echo "  make down-airflow    - Stop Airflow services"
	@echo "  make down-pipeline   - Stop pipeline services"
	@echo "  make down-all        - Stop all services"
	@echo "  make restart         - Restart all services"
	@echo ""
	@echo "Logs:"
	@echo "  make logs            - View all logs"
	@echo "  make logs-kafka      - View Kafka logs"
	@echo "  make logs-mlflow     - View MLflow logs"
	@echo "  make logs-minio      - View MinIO logs"
	@echo "  make logs-airflow    - View Airflow logs"
	@echo ""
	@echo "Maintenance:"
	@echo "  make clean-all       - Remove all volumes (DESTRUCTIVE)"

# ============================================
# BUILD
# ============================================
build-packages:
	@echo "==> Building Python packages..."
	@chmod +x build_packages.sh
	@./build_packages.sh

build: build-packages
	@echo "==> Building Airflow image with local packages..."
	@docker compose build airflow-webserver airflow-scheduler airflow-dag-processor airflow-init
	@echo "==> Building  pipeline image..."
	@docker compose build pipeline
	@echo "==> Building MLflow image..."
	@docker compose build mlflow

# ============================================
# START SERVICES
# ============================================
up-base:
	@echo "==> Starting base services (Kafka, MinIO, Postgres)..."
	@docker compose up -d zookeeper broker minio minio-init postgres
	@echo "==> Waiting for services to be healthy..."
	@sleep 10
	@make health-base

up-mlflow:
	@echo "==> Starting MLflow server"
	@docker compose up -d  minio minio-init postgres mlflow

up-airflow:
	@echo "==> Initializing Airflow database..."
	@docker compose run --rm airflow-init
	@echo "==> Starting Airflow services..."
	@docker compose up -d airflow-webserver airflow-scheduler airflow-dag-processor
	@echo ""
	@echo "   Airflow is ready!"

up-pipeline:
	@echo "==> Starting pipeline services..."
	@docker compose up -d pipeline

up-all: up-base up-airflow up-pipeline up-mlflow
	@echo ""
	@echo "   Full stack running!"
	@echo "   Kafka: localhost:9092"
	@echo "   MinIO: http://localhost:9001 (admin/minioadmin123)"
	@echo "   MLflow: http://localhost:5000"
	@echo "   Airflow: http://localhost:8080 (admin/admin)"
	@sleep 10
	@make health-all


# Backward compatibility - 'up' starts base services
up: up-all

# ============================================
# STOP SERVICES
# ============================================
down-airflow:
	@echo "==> Stopping Airflow services..."
	@docker compose stop airflow-webserver airflow-scheduler airflow-dag-processor

down-pipeline:
	@echo "==> Stopping pipeline services..."
	@docker compose stop pipeline

down-mlflow:
	@echo "==> Stopping MLflow services..."
	@docker compose stop mlflow postgres minio

down-all:
	@echo "==> Stopping all services..."
	@docker compose down

# Backward compatibility
down: down-all

# ============================================
# RESTART
# ============================================
restart: down-all up-all

# ============================================
# LOGS
# ============================================
logs:
	@docker compose logs -f

logs-kafka:
	@docker compose logs -f broker zookeeper

logs-mlflow:
	@docker compose logs -f mlflow postgres

logs-minio:
	@docker compose logs -f minio

logs-airflow:
	@docker compose logs -f airflow-webserver airflow-scheduler airflow-dag-processor

logs-pipeline:
	@docker compose logs -f pipeline

# ============================================
# STATUS & HEALTH
# ============================================
status:
	@docker compose ps

health-base:
	@echo "Checking service health..."
	@echo ""
	@echo "Kafka Broker (if running):"
	@docker compose exec -T broker kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 && echo "  Healthy :-)" || echo "  Unhealthy :-P"
	@echo ""
	@echo "MinIO (if running):"
	@curl -sf http://localhost:9000/minio/health/live > /dev/null && echo "  Healthy :-)" || echo "  Unhealthy :-P"
	@echo ""
	@echo "PostgreSQL (if running):"
	@docker compose exec -T postgres pg_isready -U mlflow > /dev/null && echo "  Healthy  :-)" || echo "  Unhealthy  :-P"

health-all:
	@echo "Checking service health..."
	@echo ""
	@echo "Kafka Broker (if running):"
	@docker compose exec -T broker kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 && echo "  Healthy :-)" || echo "  Unhealthy :-P"
	@echo ""
	@echo "MinIO (if running):"
	@curl -sf http://localhost:9000/minio/health/live > /dev/null && echo "  Healthy :-)" || echo "  Unhealthy :-P"
	@echo ""
	@echo "PostgreSQL (if running):"
	@docker compose exec -T postgres pg_isready -U mlflow > /dev/null && echo "  Healthy  :-)" || echo "  Unhealthy  :-P"
	@echo ""
	@echo "MLflow (if running):"
	@curl -sf http://localhost:5000/health > /dev/null && echo "  Healthy :-)" || echo "  Unhealthy :-P"
	@echo ""
	@echo "Airflow (if running):"
	@curl -sf http://localhost:8080/health > /dev/null && echo "  Healthy  :-)" || echo "  Not running or unhealthy :-P"

# ============================================
# VERIFY
# ============================================
# verify:
#	@echo "Verifying infrastructure..."
#	@echo ""
#	@echo "1. Kafka Topics:"
#	@docker compose exec -T broker kafka-topics --bootstrap-server localhost:9092 --list || echo "  Failed :-P"
#	@echo ""
#	@echo "2. MinIO Buckets:"
#	@docker compose exec -T minio mc ls minio || echo "   Failed :-P"
#	@echo ""
#	@echo "3. PostgreSQL:"
#	@docker compose exec -T postgres psql -U mlflow -d mlflow -c "SELECT 1;" > /dev/null && echo "   Connection successful :-)" || echo "   Failed :-P"
#	@docker compose exec -T postgres psql -U mlflow -d airflow -c "SELECT 1;" > /dev/null && echo "   Airflow DB exists :-)" || echo "   Airflow DB not initialized :-P"
#	@echo ""
#	@echo "4. MLflow API:"
#	@curl -s http://localhost:5000/api/2.0/mlflow/experiments/list | grep -q "experiments" && echo "   API working :-)" || echo "   Failed :-P"
#	@echo ""
#	@echo "5. Airflow (if running):"
#	@curl -s http://localhost:8080/health > /dev/null && echo "   Webserver running :-)" || echo "   Not running :-P"
#	@echo ""
#	@echo "Verification complete!"

# ============================================
# CLEANUP
# ============================================
#
# clean-airflow:
# 	@echo "   Removing Airflow data..."
# 	@docker compose down airflow-webserver airflow-scheduler airflow-dag-processor airflow-init
# 	@docker compose exec postgres psql -U mlflow -c "DROP DATABASE IF EXISTS airflow;" 2>/dev/null || true
# 	@echo "Airflow data removed"
#
# clean-mlflow:
# 	@echo "   Removing MLflow data..."
# 	@docker compose down mlflow
# 	@docker compose exec postgres psql -U mlflow -c "DROP DATABASE IF EXISTS mlflow;" 2>/dev/null || true
# 	@echo "MLflow data removed"
#
# clean-kafka:
# 	@echo "   Removing Kafka data..."
# 	@docker compose down broker zookeeper
# 	@docker volume rm kafka_data zookeeper_data zookeeper_logs 2>/dev/null || true
# 	@echo "Kafka volumes removed"
#
# clean-postgres-minio:
# 	@echo "   Removing Postgres and MinIO data..."
# 	@docker compose down postgres minio
# 	@docker volume rm postgres_data minio_data 2>/dev/null || true
# 	@echo "Postgres and MinIO volumes removed"

clean-all: down
	@echo ""
	@echo "   WARNING: This will delete ALL data (volumes, containers)!"
	@echo ""
	@read -p "Are you sure? Type 'yes' to continue: " confirm && [ "$$confirm" = "yes" ] || (echo "Cancelled." && exit 1)
	@docker compose down pipeline
	@docker volume rm archive_data duckdb_data 2>/dev/null || true
	@rm -rf airflow/packages/*.whl 2>/dev/null || true
	@rm -rf pipeline/packages/*.whl 2>/dev/null || true
	@echo "All volumes and data removed"
	@echo "Built packages removed"

# Backward compatibility
clean: clean-all
