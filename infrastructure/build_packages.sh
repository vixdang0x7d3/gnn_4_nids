#!/bin/bash
set -e

echo "Building local Python packages for Airflow and ETL pipeline..."
echo ""

# Get absolute paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Create packages directory
mkdir -p "$SCRIPT_DIR/airflow/packages"
rm -f "$SCRIPT_DIR/airflow/packages/*.whl"

mkdir -p "$SCRIPT_DIR/pipeline/packages"
rm -f "$SCRIPT_DIR/pipeline/packages/*.whl"

# Build graph_building (base dependency)
echo "Building graph_building..."
cd "$ROOT_DIR/graph_building"
uv build --wheel --out-dir "$SCRIPT_DIR/airflow/packages"
echo "graph_building built"
echo ""

# Build data_pipeline (depends on graph_building)
echo "Building data_pipeline..."
cd "$ROOT_DIR/data_pipeline"
uv build --wheel --out-dir "$SCRIPT_DIR/airflow/packages"
echo "data_pipeline built"
echo ""

# Build model_training (depends on graph_building)
echo "Building model_training..."
cd "$ROOT_DIR/model_training"
uv build --wheel --out-dir "$SCRIPT_DIR/airflow/packages"
echo "model_training built"
echo ""

echo "All packages built successfully!"
echo ""
echo "Packages created:"
ls -lh "$SCRIPT_DIR/airflow/packages/"*.whl
echo ""

# Copy data_pipeline wheel to pipeline directory
echo "Copying wheels to streaming-pipeline..."
cp "$SCRIPT_DIR/airflow/packages/graph_building-"*.whl "$SCRIPT_DIR/pipeline/packages/"
cp "$SCRIPT_DIR/airflow/packages/data_pipeline-"*.whl "$SCRIPT_DIR/pipeline/packages/"

echo "Packages are copied:"
ls -lh "$SCRIPT_DIR/pipeline/packages/"*.whl
echo ""

echo "Done!"
