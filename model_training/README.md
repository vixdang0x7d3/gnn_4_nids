# Model Training - GCN for Network Intrusion Detection

Graph Convolutional Network (GCN) training pipeline for network intrusion detection on UNSW-NB15 dataset.

## Overview

This module implements Graph Convolutional Networks for binary and multi-class intrusion detection. It handles data preprocessing, graph construction, model training, and evaluation using PyTorch Geometric.

### Key Features

- **Binary Classification**: Normal vs Attack traffic
- **Multi-class Classification**: Attack category classification (DoS, Exploits, Fuzzers, etc.)
- **Graph-based Learning**: Leverages network topology and traffic patterns
- **Imbalanced Data Handling**: SMOTE and class weighting strategies
- **Interactive Training**: Marimo notebooks for experimentation
- **Model Checkpointing**: Save/load trained models

## Quick Start

### 1. Install Dependencies

```bash
# Install all dependencies (including PyTorch Geometric)
uv sync

# Install dev dependencies (includes marimo, ipython)
uv sync --dev
```

### 2. Prepare Data

Option A: Use UNSW-NB15 sample data (included)
```bash
# Data already in data/nb15_sample/
ls data/nb15_sample/
```

Option B: Download full UNSW-NB15 dataset
```bash
# Use gdown to download from Google Drive
uv run python scripts/download_nb15.py
```

Option C: Use live Zeek data
```bash
# Ensure Zeek is running (see ../zeek/README.md)
# Ensure data pipeline is running (see ../data_pipeline/README.md)
# Graph data will be in ../data_pipeline/data/graphs/
```

### 3. Train Model

```bash
# Launch interactive training notebook
uv run marimo edit src/notebooks/train_gcn.py

# Or run as script
uv run python src/notebooks/train_gcn.py
```

### 4. Evaluate Model

```bash
# Launch evaluation notebook
uv run marimo edit src/notebooks/feature_selection.py
```

## Architecture

### Model Architecture

```
Input Graph (PyG Data)
    ├── Nodes: Connections/IPs
    ├── Edges: Communication relationships
    └── Features: UNSW-NB15 features (dur, sbytes, sttl, etc.)
    ↓
GCN Layer 1 (in_dim → 128)
    ├── Graph Convolution
    ├── ReLU Activation
    └── Dropout (0.5)
    ↓
GCN Layer 2 (128 → 64)
    ├── Graph Convolution
    ├── ReLU Activation
    └── Dropout (0.5)
    ↓
GCN Layer 3 (64 → 32)
    ├── Graph Convolution
    ├── ReLU Activation
    └── Dropout (0.5)
    ↓
Output Layer (32 → num_classes)
    ├── Linear
    └── Softmax (for classification)
    ↓
Predictions
    ├── Binary: Normal/Attack
    └── Multi-class: Attack categories
```

## Directory Structure

```
model_training/
├── src/
│   ├── gnn/
│   │   ├── nb15_gcn.py             # GCN model implementation
│   │   ├── helpers.py              # Training utilities
│   │   └── const.py                # Constants and configs
│   ├── dataprep/
│   │   ├── graph_base.py           # Base graph construction
│   │   ├── graph_gcn.py            # GCN-specific graph prep
│   │   └── transform.py            # Data transformations
│   ├── preprocessing/
│   │   └── preprocess_nb15.py      # UNSW-NB15 preprocessing
│   └── notebooks/
│       ├── train_gcn.py            # Main training notebook
│       ├── feature_engineering.py  # Feature analysis
│       ├── feature_selection.py    # Feature importance
│       └── preprocess.py           # Data preprocessing notebook
├── models/                          # Saved model checkpoints
├── data/                            # Training datasets
├── scripts/                         # Utility scripts
├── pyproject.toml                   # Python dependencies
└── README.md                        # This file
```

## Data Preparation

### UNSW-NB15 Dataset

The UNSW-NB15 dataset contains:
- **2,540,044 records** total
- **49 features** per record
- **10 attack categories**: DoS, Exploits, Fuzzers, Generic, Reconnaissance, Analysis, Backdoor, Shellcode, Worms
- **Binary labels**: Normal (0) vs Attack (1)
- **Imbalanced**: ~56% Attack, ~44% Normal

### Feature Engineering

```python
# Key features used for GCN
SELECTED_FEATURES = [
    'dur',          # Duration
    'sbytes',       # Source bytes
    'dbytes',       # Dest bytes
    'sttl',         # Source TTL
    'dttl',         # Dest TTL
    'sintpkt',      # Source inter-packet time
    'dintpkt',      # Dest inter-packet time
    'smean',        # Mean source packet size
    'dmean',        # Mean dest packet size
    'tcprtt',       # TCP RTT
]
```

### Graph Construction

#### Node Representation

Option 1: **Connection-based**
- Each node = network flow/connection
- Features = UNSW-NB15 features

Option 2: **IP-based**
- Each node = IP address
- Features = aggregated flow statistics

#### Edge Construction

Edges represent communication relationships:

```python
# Example: Connect source and destination IPs
edge_index = [[src_ip_1, src_ip_2, ...],
              [dst_ip_1, dst_ip_2, ...]]

# Or: Temporal connections (flows within time window)
# Or: Similarity-based (feature similarity)
```

### Handling Imbalanced Data

```python
# Strategy 1: SMOTE (Synthetic Minority Oversampling)
from imblearn.over_sampling import SMOTE
smote = SMOTE(random_state=42)
X_resampled, y_resampled = smote.fit_resample(X, y)

# Strategy 2: Class Weights
class_weights = torch.tensor([0.44, 0.56])  # Inverse of class distribution
criterion = nn.CrossEntropyLoss(weight=class_weights)

# Strategy 3: Stratified Sampling
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)
```

## Training

### Hyperparameters

```python
# Model architecture
hidden_channels = [128, 64, 32]
dropout = 0.5

# Training
learning_rate = 0.001
batch_size = 256
num_epochs = 100
early_stopping_patience = 10

# Optimization
optimizer = torch.optim.Adam
weight_decay = 5e-4
```

### Training Loop

```python
import torch
from torch_geometric.nn import GCNConv

# Initialize model
model = GCN(
    in_channels=num_features,
    hidden_channels=[128, 64, 32],
    out_channels=num_classes,
    dropout=0.5
)

# Training
for epoch in range(num_epochs):
    model.train()
    optimizer.zero_grad()

    # Forward pass
    out = model(data.x, data.edge_index)
    loss = criterion(out[train_mask], data.y[train_mask])

    # Backward pass
    loss.backward()
    optimizer.step()

    # Validation
    val_acc = evaluate(model, data, val_mask)

    # Early stopping
    if val_acc > best_val_acc:
        best_val_acc = val_acc
        torch.save(model.state_dict(), 'models/best_model.pt')
```

### Evaluation Metrics

```python
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    classification_report
)

# Binary classification
accuracy = accuracy_score(y_true, y_pred)
precision = precision_score(y_true, y_pred)
recall = recall_score(y_true, y_pred)
f1 = f1_score(y_true, y_pred)

# Multi-class classification
report = classification_report(y_true, y_pred,
                               target_names=attack_categories)
```

## Model Checkpointing

### Save Model

```python
# Save full model
torch.save({
    'epoch': epoch,
    'model_state_dict': model.state_dict(),
    'optimizer_state_dict': optimizer.state_dict(),
    'loss': loss,
    'accuracy': accuracy
}, 'models/checkpoint_epoch_{}.pt'.format(epoch))

# Save best model only
if val_acc > best_val_acc:
    torch.save(model.state_dict(), 'models/best_model.pt')
```

### Load Model

```python
# Load model
model = GCN(in_channels, hidden_channels, out_channels)
model.load_state_dict(torch.load('models/best_model.pt'))
model.eval()

# Load checkpoint
checkpoint = torch.load('models/checkpoint_epoch_50.pt')
model.load_state_dict(checkpoint['model_state_dict'])
optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
epoch = checkpoint['epoch']
loss = checkpoint['loss']
```

## Notebooks

### train_gcn.py
Main training pipeline:
- Load and preprocess data
- Construct graphs
- Train GCN model
- Evaluate performance
- Save model checkpoints

### feature_engineering.py
Feature analysis and engineering:
- Feature distributions
- Correlation analysis
- Feature importance
- Feature selection

### feature_selection.py
Advanced feature selection:
- Recursive feature elimination
- SHAP values
- Permutation importance
- Feature subset evaluation

### preprocess.py
Data preprocessing:
- Handle missing values
- Normalize/standardize features
- Encode categorical variables
- Train/test split

## Technologies

### Core Dependencies

- **PyTorch** (v2.8+): Deep learning framework
- **PyTorch Geometric** (v2.6+): Graph neural networks
- **torch-scatter, torch-sparse** (v2.1+): Sparse operations
- **NumPy** (v2.3+): Numerical computing
- **Pandas** (v2.3+): Data manipulation
- **Polars** (v1.32+): High-performance DataFrames

### Visualization & Analysis

- **Seaborn** (v0.13+): Statistical visualization
- **Altair** (v5.5+): Declarative visualization
- **NetworkX** (v3.5+): Graph analysis

### Data Handling

- **imbalanced-learn** (v0.0+): SMOTE and resampling
- **scikit-learn**: Metrics and utilities
- **DuckDB** (v1.4+): SQL analytics

### Development Tools

- **Marimo** (v0.17+): Reactive notebooks
- **IPython** (v9.5+): Interactive shell
- **uv**: Fast package manager

## Performance Optimization

### GPU Acceleration

```python
# Check GPU availability
import torch
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")

# Move model and data to GPU
model = model.to(device)
data = data.to(device)
```

### Batch Processing

```python
from torch_geometric.loader import DataLoader

# Create mini-batches
loader = DataLoader(dataset, batch_size=256, shuffle=True)

for batch in loader:
    batch = batch.to(device)
    # Training step
```

### Memory Management

```python
# Clear cache
torch.cuda.empty_cache()

# Gradient checkpointing for large graphs
model = torch.utils.checkpoint.checkpoint_sequential(model, segments=3)
```

## Troubleshooting

### CUDA Out of Memory

```python
# Reduce batch size
batch_size = 128  # Instead of 256

# Use gradient accumulation
for i, batch in enumerate(loader):
    loss = loss / accumulation_steps
    loss.backward()

    if (i + 1) % accumulation_steps == 0:
        optimizer.step()
        optimizer.zero_grad()
```

### Poor Model Performance

1. **Check data quality**: Missing values, outliers, feature scaling
2. **Feature engineering**: Try different feature combinations
3. **Graph construction**: Experiment with different edge definitions
4. **Hyperparameters**: Tune learning rate, dropout, hidden dimensions
5. **Imbalanced data**: Apply SMOTE, class weights, or stratified sampling

### Installation Issues

```bash
# PyTorch Geometric installation issues
pip install torch-scatter torch-sparse -f https://data.pyg.org/whl/torch-2.8.0+cpu.html

# Or use uv (recommended)
uv sync --reinstall
```

## Results

### Binary Classification (Normal vs Attack)

Expected performance on UNSW-NB15:
- **Accuracy**: ~95%
- **Precision**: ~93%
- **Recall**: ~96%
- **F1-Score**: ~94%

### Multi-class Classification (Attack Categories)

Expected performance:
- **Overall Accuracy**: ~90%
- **Macro F1-Score**: ~88%

| Attack Category | Precision | Recall | F1-Score |
|----------------|-----------|--------|----------|
| DoS | 0.95 | 0.93 | 0.94 |
| Exploits | 0.91 | 0.89 | 0.90 |
| Fuzzers | 0.88 | 0.92 | 0.90 |
| Generic | 0.87 | 0.85 | 0.86 |
| Reconnaissance | 0.90 | 0.88 | 0.89 |

*Note: Actual results may vary based on data preprocessing, graph construction, and hyperparameters*

## Related Components

- **Zeek Setup**: See `../zeek/README.md` for traffic capture
- **Data Pipeline**: See `../data_pipeline/README.md` for ETL processing
- **Root Project**: See `../README.md` for overall architecture

## References

### Papers

- Semi-Supervised Classification with Graph Convolutional Networks (Kipf & Welling, 2017)
- Graph Neural Networks for Intrusion Detection Systems
- UNSW-NB15: A comprehensive network intrusion detection dataset

### Documentation

- [PyTorch Geometric Documentation](https://pytorch-geometric.readthedocs.io/)
- [PyTorch Documentation](https://pytorch.org/docs/)
- [UNSW-NB15 Dataset](https://research.unsw.edu.au/projects/unsw-nb15-dataset)

### Tutorials

- [PyG Introduction](https://pytorch-geometric.readthedocs.io/en/latest/notes/introduction.html)
- [GCN Tutorial](https://pytorch-geometric.readthedocs.io/en/latest/notes/colabs.html)
