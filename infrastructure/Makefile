.PHONY: help up down restart logs status clean \
        kafka-only mlflow-only minio-only \
        init verify health

# Default target
help:
	@echo "Infrastructure Management Commands:"
	@echo ""
	@echo "  make up              - Start all services"
	@echo "  make down            - Stop all services"
	@echo "  make restart         - Restart all services"
	@echo "  make logs            - View logs (all services)"
	@echo "  make logs-<service>  - View logs for specific service"
	@echo "  make status          - Show service status"
	@echo "  make health          - Check service health"
	@echo ""
	@echo "Partial Stacks:"
	@echo "  make kafka-only      - Start only Kafka stack"
	@echo "  make mlflow-only     - Start only MLflow stack"
	@echo "  make minio-only      - Start only MinIO"
	@echo ""
	@echo "Maintenance:"
	@echo "  make init            - Initialize buckets and databases"
	@echo "  make verify          - Verify all services are working"
	@echo "  make clean           - Remove all volumes (DESTRUCTIVE)"
	@echo "  make clean-kafka     - Remove only Kafka volumes"
	@echo "  make clean-mlflow    - Remove only MLflow volumes"

# Start all services
up:
	@echo "Starting all infrastructure services..."
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@make health

# Stop all services
down:
	@echo "Stopping all infrastructure services..."
	docker compose down

# Restart all services
restart: down up

# View logs for all services
logs:
	docker compose logs -f

# View logs for specific services
logs-kafka:
	docker compose logs -f broker zookeeper

logs-mlflow:
	docker compose logs -f mlflow postgres

logs-minio:
	docker compose logs -f minio

# Show service status
status:
	docker compose ps

# Check health of all services
health:
	@echo "Checking service health..."
	@echo ""
	@echo "Kafka Broker:"
	@docker compose exec -T broker kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 && echo "  Healthy :-)" || echo "  Unhealthy :-P"
	@echo ""
	@echo "MinIO:"
	@curl -sf http://localhost:9000/minio/health/live > /dev/null && echo "   Healthy :-)" || echo "  Unhealthy :-P"
	@echo ""
	@echo "PostgreSQL:"
	@docker compose exec -T postgres pg_isready -U mlflow > /dev/null && echo "  Healthy :-)" || echo "  Unhealthy :-P"
	@echo ""
	@echo "MLflow:"
	@curl -sf http://localhost:5000/health > /dev/null && echo "  Healthy :-)" || echo "  Unhealthy :-P"

# Start only Kafka stack
kafka-only:
	docker compose up -d zookeeper broker

# Start only MLflow stack (includes MinIO and PostgreSQL)
mlflow-only:
	docker compose up -d postgres minio minio-init mlflow

# Start only MinIO
minio-only:
	docker compose up -d minio minio-init

# Initialize buckets and databases
init:
	@echo "Initializing infrastructure..."
	docker compose up -d minio postgres
	@echo "Waiting for services..."
	@sleep 10
	docker compose up minio-init
	@echo "Initialization complete!"

# Verify all services are working
verify:
	@echo "Verifying infrastructure..."
	@echo ""
	@echo "1. Checking Kafka..."
	docker compose exec -T broker kafka-topics --bootstrap-server localhost:9092 --list || echo "Kafka verification failed"
	@echo ""
	@echo "2. Checking MinIO buckets..."
	docker compose exec -T minio mc ls myminio || echo "MinIO verification failed"
	@echo ""
	@echo "3. Checking PostgreSQL..."
	docker compose exec -T postgres psql -U mlflow -d mlflow -c "SELECT 1;" || echo "PostgreSQL verification failed"
	@echo ""
	@echo "4. Checking MLflow..."
	curl -s http://localhost:5000/api/2.0/mlflow/experiments/list | grep -q "experiments" && echo "MLflow API working" || echo "MLflow verification failed"
	@echo ""
	@echo "Verification complete!"

# Clean all volumes (DESTRUCTIVE)
clean:
	@echo "WARNING: This will delete all data!"
	@read -p "Are you sure? Type 'yes' to continue: " confirm && [ "$$confirm" = "yes" ] || exit 1
	docker compose down -v
	@echo "All volumes removed."

# Clean only Kafka volumes
clean-kafka:
	@echo "Removing Kafka volumes..."
	docker compose down
	docker volume rm kafka_data zookeeper_data zookeeper_logs 2>/dev/null || true
	@echo "Kafka volumes removed."

# Clean only MLflow volumes
clean-mlflow:
	@echo "Removing MLflow volumes..."
	docker compose down
	docker volume rm postgres_data minio_data 2>/dev/null || true
	@echo "MLflow volumes removed."
