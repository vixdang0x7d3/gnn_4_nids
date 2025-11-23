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

## Data Preparation

### UNSW-NB15 Dataset

The UNSW-NB15 dataset contains:
- **2,540,044 records** total
- **49 features** per record
- **10 attack categories**: DoS, Exploits, Fuzzers, Generic, Reconnaissance, Analysis, Backdoor, Shellcode, Worms
- **Binary labels**: Normal (0) vs Attack (1)
- **Imbalanced**: ~80% Attack, ~20% Normal

### Feature selection

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

Methods used:
- Information gain
- Recursive feature elimination
- Correlation analysis

### Handling Imbalanced Data

- **Groupwise SMOTE** perform sampling in each (proto, state, service) tripplet group to retain graph connectivity (only applied in multi-class classification)
- **Class weights** Assign more weight to minority class address label imbalance

## Evaluation

### Metrics 
- precision
- recall
- f1
- weighted accuracy
- AUC score (for binary classification)
- confusion matrix

### Results:

**Detailed Binary Classification Results on Test Set**:

| **Class** | **Precision** | **Recall** | **F1-Score** | **Support** |
|---|---:|---:|---:|---:|
| Normal (0) | 0.9997 | 0.9811 | 0.9904 | 443,512 |
| Attack (1) | 0.8852 | **0.9983** | 0.9383 | 64,602 |
| **Accuracy** | | | **0.9833** | **508,114** |
| Macro Avg | 0.9425 | 0.9897 | 0.9643 | 508,114 |
| Weighted Avg | 0.9852 | 0.9833 | 0.9837 | 508,114 |


**Detailed Multiclass Classification Results on Test Set**:

| **Class (ID)** | **Precision** | **Recall** | **F1-Score** | **Support** |
|---|---:|---:|---:|---:|
| Normal (0) | 0.9997 | 0.9803 | 0.9899 | 443,512 |
| Generic (9) | 0.9828 | 0.9654 | 0.9741 | 43,259 |
| **Worms (1)** | 0.0087 | **0.8929** | 0.0172 | 28 |
| Shellcode (3) | 0.0264 | **0.6928** | 0.0509 | 306 |
| Analysis (4) | 0.0609 | **0.5882** | 0.1104 | 527 |
| Backdoors (2) | 0.0242 | 0.2381 | 0.0439 | 462 |
| DoS (6) | 0.0649 | 0.0404 | 0.0498 | 3,293 |
| Reconnaissance (5) | 0.1608 | 0.1468 | 0.1534 | 2,841 |
| Fuzzers (7) | 0.2824 | 0.1538 | 0.1992 | 4,934 |
| Exploits (8) | 0.7096 | 0.2271 | 0.3441 | 8,952 |
| **Accuracy** | | | **0.9457** | **508,114** |
| Macro Avg | 0.3320 | 0.4926 | 0.2933 | 508,114 |

The high recall but very low precision for minority attack classes (Worms, Shellcode, Analysis) despite SMOTE, and class weight techniques applied. Multiclass detection model is not ready for production.


