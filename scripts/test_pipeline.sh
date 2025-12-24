#!/usr/bin/env bash
# Test script for Zeek ETL Pipeline with 4SICS PCAP

set -e

echo "=== Testing Zeek ETL Pipeline with 4SICS PCAP ==="

# Step 1: Start Kafka
echo ""
echo "Step 1: Starting Kafka services..."
cd "$(dirname "$0")/../infrastructure"

docker-compose up -d broker

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 10
docker-compose exec -T broker kafka-topics --bootstrap-server localhost:9092 --list || sleep 5

# Create topic if it doesn't exist
echo "Creating zeek.logs topic..."
docker-compose exec -T broker kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --topic zeek.logs \
    --if-not-exists \
    --partitions 1 \
    --replication-factor 1

echo "✓ Kafka is ready"

# Step 2: Start Zeek to process PCAP
echo ""
echo "Step 2: Processing PCAP with Zeek..."
cd ../zeek/dpi_impl

# Stop any existing Zeek container
./run.sh stop || true

# Process PCAP
./run.sh pcap ../test_pcaps/4SICS-GeekLounge-151022.pcap &
ZEEK_PID=$!

echo "✓ Zeek processing started (PID: $ZEEK_PID)"

# Step 3: Run ETL pipeline
echo ""
echo "Step 3: Starting ETL pipeline..."
cd ../../pipelines

# Create output directories
mkdir -p data/output/archive

# Run pipeline for 5 minutes (300 seconds)
echo "Running pipeline for 300 seconds..."
./run_etl.sh --duration 300

echo ""
echo "=== Pipeline Test Complete ==="
echo ""
echo "Results:"
echo "  Database: pipelines/data/output/nids_data.duckdb"
echo "  Archive:  pipelines/data/output/archive/"
echo ""
echo "To inspect the database:"
echo "  cd pipelines"
echo "  uv run duckdb data/output/nids_data.duckdb"
echo ""
echo "To check Parquet files:"
echo "  ls -lh data/output/archive/"
