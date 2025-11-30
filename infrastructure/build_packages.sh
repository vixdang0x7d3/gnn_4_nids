#!/bin/bash
set -e

echo "Building local Python packages for Airflow..."
echo ""

# Create packages directory
mkdir -p airflow/packages
rm -f airflow/packages/*.whl

# Get absolute paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

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

# Copy data_pipeline wheel to streaming-pipeline directory
echo "Copying data_pipeline wheel to streaming-pipeline..."
mkdir -p "$SCRIPT_DIR/streaming-pipeline/packages"
cp "$SCRIPT_DIR/airflow/packages/data_pipeline-"*.whl "$SCRIPT_DIR/streaming-pipeline/packages/"
echo "Done!"
