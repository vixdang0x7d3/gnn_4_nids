# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Network Intrusion Detection System (NIDS) that uses Graph Neural Networks (GNNs) to detect network anomalies using the UNSW-NB15 dataset. The project is built with PyTorch Geometric for graph processing and uses DuckDB for efficient data handling.

## Development Commands

- **Start development environment**: `uv run marimo edit --headless --watch` (or `make notebooks`)
- **Install dependencies**: `uv sync` (project uses uv for dependency management)
- **Run individual notebook**: `uv run marimo run src/notebooks/<notebook_name>.py`

## Architecture

### Core Components

- **`src/gnn/dataset.py`**: Contains `NB15Dataset` class that handles UNSW-NB15 dataset loading, preprocessing, and PyTorch Geometric integration. Supports binary/multiclass classification, data augmentation with SMOTE, and train/val/test splits.

- **`src/gnn/graph_builder.py`**: Contains `GraphBuilder` class responsible for constructing graph structures from tabular network data. Creates node embeddings from selected features and builds edges based on network flow similarities (protocol, service, state groupings).

### Data Flow

1. Raw UNSW-NB15 data is downloaded and stored in `dataset/NB15/raw/` as parquet files
2. `GraphBuilder` processes the data using DuckDB for memory-efficient operations
3. SMOTE augmentation is applied for training data in multiclass scenarios
4. Graph structures are created with nodes representing network flows and edges connecting similar flows
5. Processed graph data is saved as PyTorch tensors in `dataset/NB15/processed/`

### Key Features

- **Selected features**: 14 min-max normalized features defined in `SELECTED_COLS` (dataset.py:22-37)
- **Graph construction**: Uses k-nearest neighbors approach based on protocol/service/state similarity
- **Memory management**: Batched processing and explicit garbage collection for large datasets
- **Data augmentation**: SMOTE oversampling for imbalanced multiclass training data

### Notebooks

Located in `src/notebooks/`, these are Marimo notebooks for data exploration and experimentation:
- `preprocess.py`: Data preprocessing and feature engineering
- `feature_selection.py`: Feature analysis and selection
- `dataset_test.py`: Dataset loading and validation
- `learn_pyg.py`: PyTorch Geometric learning experiments

## Data Pipeline

The project expects the UNSW-NB15 dataset to be automatically downloaded via Google Drive link. Processed data is cached in `dataset/NB15/processed/` with filenames indicating configuration (binary/multiclass, k-neighbors, augmentation status).