"""
Original UNSW-NB15 GNN Training with Enhanced MLflow Integration
Trains a Graph Convolutional Network on original UNSW-NB15 features with comprehensive tracking
"""

import marimo

__generated_with = "0.18.1"
app = marimo.App(width="medium", auto_download=["ipynb"])


@app.cell
def _():
    import marimo as mo
    import sys
    import os
    import os.path as osp
    import pickle
    from pathlib import Path

    import torch
    import torch.nn as nn
    from torch.optim import Adadelta, Adam
    from torch_geometric.loader import RandomNodeLoader

    import duckdb
    import numpy as np
    import pandas as pd

    import matplotlib.pyplot as plt
    import seaborn as sns

    from sklearn.metrics import (
        roc_curve, auc, precision_recall_curve, average_precision_score,
        accuracy_score, recall_score, classification_report,
        balanced_accuracy_score, confusion_matrix, f1_score
    )
    from sklearn.utils import class_weight

    import mlflow
    import mlflow.pytorch

    return (
        Adadelta, Adam, RandomNodeLoader,
        accuracy_score, auc, average_precision_score, balanced_accuracy_score,
        class_weight, classification_report, confusion_matrix, duckdb, f1_score,
        mlflow, mo, nn, np, os, osp, pd, pickle, plt,
        precision_recall_curve, recall_score, roc_curve, sns, sys, torch, Path
    )


@app.cell
def _(Path, os, sys):
    # Add project root to path
    project_root = Path(__file__).resolve().parents[3]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    # Add source directories
    gnn_src = project_root / "model_training" / "src"
    graph_src = project_root / "graph_building" / "src"

    for path in [gnn_src, graph_src]:
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))

    return (project_root, gnn_src, graph_src)


@app.cell
def _():
    from gnn.nb15_gcn import NB15_GCN
    from gnn.helpers import train, predict
    from gnn.const import (
        FEATURE_ATTRIBUTES, EDGE_ATTRIBUTES,
        BINARY_LABEL, MULTICLASS_LABEL
    )
    from graph_building.graph_gcn import GraphGCN
    from graph_building.transform import groupwise_smote, minmax_scale

    return (
        BINARY_LABEL, EDGE_ATTRIBUTES, FEATURE_ATTRIBUTES,
        GraphGCN, MULTICLASS_LABEL, NB15_GCN,
        groupwise_smote, minmax_scale, train, predict
    )


@app.cell
def _(mo):
    mo.md(r"""
    ## Configuration

    Configure model hyperparameters and paths for original UNSW-NB15 GNN training.
    """)
    return


@app.cell
def _(duckdb, osp, os, project_root):
    conn = duckdb.connect()

    # SQL Query Template
    LOAD_SQL = """
        SELECT
          ROW_NUMBER() OVER () as index,
          {feature_cols},
          {edge_cols},
          {label}
        FROM read_parquet('{parquet_path}')
        ORDER BY RANDOM()
    """

    # Training Configuration
    params = {
        # Paths
        "dataset_path": str(project_root / "model_training" / "data" / "NB15_preprocessed"),
        "artifacts_path": str(project_root / "model_training" / "models"),

        # Data Config
        "binary_mode": True,
        "use_smote": False,
        "smote_min_samples": 10,
        "smote_k_neighbors": 5,
        "graph_n_neighbors": 2,

        # Loader Config
        "num_parts": 256,
        "shuffle": True,
        "num_workers": 4,

        # Model Architecture
        "n_features": 14,  # Original UNSW-NB15 features
        "n_convs": 8,
        "n_hidden": 512,
        "alpha": 0.5,
        "theta": 0.7,
        "dropout": 0.5,

        # Training Config
        "epochs": 1,
        "optimizer": "Adadelta",
        "learning_rate": 1.0,
        "patience": 3,  # Early stopping patience
    }

    # Extract params
    dataset_path = params["dataset_path"]
    artifacts_path = params["artifacts_path"]
    os.makedirs(artifacts_path, exist_ok=True)

    # Dataset splits
    trainset_path = osp.join(dataset_path, "train.parquet")
    testset_path = osp.join(dataset_path, "test.parquet")
    valset_path = osp.join(dataset_path, "val.parquet")

    # MLflow experiment name
    EXPERIMENT_NAME = "Original_UNSW_NB15_GNN"

    # Model naming
    model_name = (
        f"gcn_unsw_nb15"
        f"_{'bin' if params['binary_mode'] else 'multi'}"
        f"_{'smote' if params['use_smote'] and not params['binary_mode'] else 'no_smote'}"
        f"_n{params['graph_n_neighbors']}"
        f"_conv{params['n_convs']}"
        f"_h{params['n_hidden']}"
    )

    model_save_path = osp.join(artifacts_path, f"{model_name}.pt")

    return (
        EXPERIMENT_NAME, LOAD_SQL, conn, model_name, model_save_path, params,
        testset_path, trainset_path, valset_path, artifacts_path
    )


@app.cell
def _(mo):
    mo.md(r"""
    ## Load Data

    Load train, validation, and test datasets from parquet files.
    """)
    return


@app.cell
def _(
    BINARY_LABEL, EDGE_ATTRIBUTES, FEATURE_ATTRIBUTES,
    LOAD_SQL, MULTICLASS_LABEL, conn, params,
    testset_path, trainset_path, valset_path
):
    # Load training data
    load_train_sql = LOAD_SQL.format(
        feature_cols=", ".join(FEATURE_ATTRIBUTES),
        edge_cols=", ".join(EDGE_ATTRIBUTES),
        label=BINARY_LABEL if params["binary_mode"] else MULTICLASS_LABEL,
        parquet_path=trainset_path
    )
    train_data = conn.sql(load_train_sql)

    # Load validation data
    load_val_sql = LOAD_SQL.format(
        feature_cols=", ".join(FEATURE_ATTRIBUTES),
        edge_cols=", ".join(EDGE_ATTRIBUTES),
        label=BINARY_LABEL if params["binary_mode"] else MULTICLASS_LABEL,
        parquet_path=valset_path
    )
    val_data = conn.sql(load_val_sql)

    # Load test data
    load_test_sql = LOAD_SQL.format(
        feature_cols=", ".join(FEATURE_ATTRIBUTES),
        edge_cols=", ".join(EDGE_ATTRIBUTES),
        label=BINARY_LABEL if params["binary_mode"] else MULTICLASS_LABEL,
        parquet_path=testset_path
    )
    test_data = conn.sql(load_test_sql)

    print(f"Train samples: {train_data.count('*').fetchone()[0]}")
    print(f"Val samples: {val_data.count('*').fetchone()[0]}")
    print(f"Test samples: {test_data.count('*').fetchone()[0]}")

    return train_data, val_data, test_data


@app.cell
def _(mo):
    mo.md(r"""
    ## Preprocessing

    Apply MinMax scaling and optional SMOTE for class balancing.
    """)
    return


@app.cell
def _(
    EDGE_ATTRIBUTES, FEATURE_ATTRIBUTES, artifacts_path,
    groupwise_smote, minmax_scale, osp, params, pickle,
    test_data, train_data, val_data
):
    # Convert to Arrow tables
    train_arrow = train_data.arrow().read_all()

    # Apply MinMax scaling
    train_arrow, fitted_mm_scaler = minmax_scale(train_arrow, FEATURE_ATTRIBUTES)

    # Apply SMOTE if needed (only for multi-class)
    if not params["binary_mode"] and params["use_smote"]:
        train_arrow = groupwise_smote(
            table=train_arrow,
            feature_attrs=FEATURE_ATTRIBUTES,
            group_attrs=EDGE_ATTRIBUTES,
            min_samples=params["smote_min_samples"],
            k_neighbors=params["smote_k_neighbors"]
        )

    # Save scaler
    scaler_save_path = osp.join(artifacts_path, "fitted_mm_scaler_unsw.pkl")
    with open(scaler_save_path, "wb") as f:
        pickle.dump(fitted_mm_scaler, f)

    # Scale validation and test sets
    val_arrow = val_data.arrow().read_all()
    val_arrow, _ = minmax_scale(val_arrow, FEATURE_ATTRIBUTES, fitted_mm_scaler)

    test_arrow = test_data.arrow().read_all()
    test_arrow, _ = minmax_scale(test_arrow, FEATURE_ATTRIBUTES, fitted_mm_scaler)

    print("✓ Preprocessing complete")

    return train_arrow, val_arrow, test_arrow, fitted_mm_scaler, scaler_save_path


@app.cell
def _(mo):
    mo.md(r"""
    ## Build Graphs

    Construct PyTorch Geometric graph data structures.
    """)
    return


@app.cell
def _(
    BINARY_LABEL, EDGE_ATTRIBUTES, FEATURE_ATTRIBUTES,
    GraphGCN, MULTICLASS_LABEL, RandomNodeLoader, params,
    test_arrow, train_arrow, val_arrow
):
    # Build training graph
    train_graph = GraphGCN(
        table=train_arrow,
        node_attrs=FEATURE_ATTRIBUTES,
        edge_attrs=EDGE_ATTRIBUTES,
        label=BINARY_LABEL if params["binary_mode"] else MULTICLASS_LABEL,
        n_neighbors=params["graph_n_neighbors"],
    ).build(include_labels=True)

    # Build validation graph
    val_graph = GraphGCN(
        table=val_arrow,
        node_attrs=FEATURE_ATTRIBUTES,
        edge_attrs=EDGE_ATTRIBUTES,
        label=BINARY_LABEL if params["binary_mode"] else MULTICLASS_LABEL,
        n_neighbors=params["graph_n_neighbors"],
    ).build(include_labels=True)

    # Build test graph
    test_graph = GraphGCN(
        table=test_arrow,
        node_attrs=FEATURE_ATTRIBUTES,
        edge_attrs=EDGE_ATTRIBUTES,
        label=BINARY_LABEL if params["binary_mode"] else MULTICLASS_LABEL,
        n_neighbors=params["graph_n_neighbors"],
    ).build(include_labels=True)

    # Create data loaders
    train_loader = RandomNodeLoader(
        data=train_graph,
        num_parts=params["num_parts"],
        shuffle=params["shuffle"]
    )
    val_loader = RandomNodeLoader(
        data=val_graph,
        num_parts=params["num_parts"],
        shuffle=False
    )
    test_loader = RandomNodeLoader(
        data=test_graph,
        num_parts=params["num_parts"],
        shuffle=False
    )

    print(f"✓ Graphs built")
    print(f"  Train: {train_graph.num_nodes} nodes, {train_graph.num_edges} edges")
    print(f"  Val:   {val_graph.num_nodes} nodes, {val_graph.num_edges} edges")
    print(f"  Test:  {test_graph.num_nodes} nodes, {test_graph.num_edges} edges")

    return train_graph, val_graph, test_graph, train_loader, val_loader, test_loader


@app.cell
def _(mo):
    mo.md(r"""
    ## Initialize Model

    Create GCN model with specified architecture.
    """)
    return


@app.cell
def _(NB15_GCN, params):
    model = NB15_GCN(
        n_features=params["n_features"],
        n_classes=2 if params["binary_mode"] else 10,
        n_convs=params["n_convs"],
        n_hidden=params["n_hidden"],
        alpha=params["alpha"],
        theta=params["theta"],
        dropout=params["dropout"],
    )

    print(f"✓ Model initialized")
    print(f"  Parameters: {sum(p.numel() for p in model.parameters()):,}")

    return (model,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Configure Training

    Setup optimizer, loss function with class weights, and device.
    """)
    return


@app.cell
def _(Adadelta, Adam, class_weight, model, nn, np, params, torch, train_graph, val_graph):
    # Setup device
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model.to(device)

    # Compute class weights for training set
    y_train = train_graph.y.numpy()
    train_class_weights = class_weight.compute_class_weight(
        'balanced',
        classes=np.unique(y_train),
        y=y_train
    )
    train_class_weights = torch.tensor(train_class_weights, dtype=torch.float)
    loss_fn = nn.CrossEntropyLoss(weight=train_class_weights.to(device))

    # Compute class weights for validation set
    y_val = val_graph.y.numpy()
    val_class_weights = class_weight.compute_class_weight(
        'balanced',
        classes=np.unique(y_val),
        y=y_val
    )
    val_class_weights = torch.tensor(val_class_weights, dtype=torch.float)
    loss_fn_val = nn.CrossEntropyLoss(weight=val_class_weights.to(device))

    # Setup optimizer
    if params["optimizer"] == "Adadelta":
        optimizer = Adadelta(model.parameters(), lr=params.get("learning_rate", 1.0))
    else:
        optimizer = Adam(model.parameters(), lr=params.get("learning_rate", 0.001))

    print(f"✓ Training configured")
    print(f"  Device: {device}")
    print(f"  Optimizer: {params['optimizer']}")
    print(f"  Class weights: {train_class_weights.numpy()}")

    return device, loss_fn, loss_fn_val, optimizer


@app.cell
def _(mo):
    mo.md(r"""
    ## Train Model

    Train the GCN model with MLflow tracking.
    """)
    return


@app.cell
def _(
    EXPERIMENT_NAME, artifacts_path, device, loss_fn, loss_fn_val,
    mlflow, model, model_name, model_save_path, optimizer, os, osp,
    params, plt, scaler_save_path, torch, train, train_loader, val_loader
):
    # Setup MLflow with S3/MinIO credentials
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"
    os.environ['AWS_ACCESS_KEY_ID'] = 'admin'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin123'

    mlflow.set_tracking_uri("http://localhost:5000")
    mlflow.set_experiment(EXPERIMENT_NAME)

    print(f"Starting MLflow Run: {model_name}")
    print(f"Tracking URI: {mlflow.get_tracking_uri()}")

    # Start MLflow run
    with mlflow.start_run(run_name=model_name) as run:
        # Log all parameters
        mlflow.log_params(params)

        # Check if model already exists
        if osp.exists(model_save_path):
            print(f"Loading existing model from {model_save_path}")
            model.load_state_dict(
                torch.load(model_save_path, map_location=device, weights_only=False)
            )
            history = None
        else:
            # Train model
            print("Training new model...")
            history = train(
                model=model,
                loss_fn=loss_fn,
                train_dataloader=train_loader,
                optimizer=optimizer,
                device=device,
                model_path=artifacts_path,
                model_name=model_name.replace('.pt', ''),
                eval=True,
                loss_fn_val=loss_fn_val,
                val_dataloader=val_loader,
                epochs=params["epochs"],
                patience=params.get("patience", 3)
            )

            # Log training history metrics
            if history:
                for epoch, (train_loss, val_loss, val_acc) in enumerate(
                    zip(history['train_epoch_loss'],
                        history['val_loss'],
                        history['val_acc'])
                ):
                    mlflow.log_metrics({
                        "train_loss": train_loss,
                        "val_loss": val_loss,
                        "val_accuracy": val_acc
                    }, step=epoch)

        # Log model to MLflow
        mlflow.pytorch.log_model(model, "model")

        # Log scaler artifact
        if osp.exists(scaler_save_path):
            mlflow.log_artifact(scaler_save_path, artifact_path="preprocessor")

        # Plot and log training history
        if history:
            _fig, (_ax1, _ax2) = plt.subplots(1, 2, figsize=(14, 5))

            # Loss plot
            _ax1.plot(history['train_epoch_loss'], label='Train Loss', marker='o', linewidth=2)
            _ax1.plot(history['val_loss'], label='Val Loss', marker='s', linewidth=2)
            _ax1.set_title('Training vs Validation Loss', fontsize=14, fontweight='bold')
            _ax1.set_xlabel('Epoch', fontsize=12)
            _ax1.set_ylabel('Loss', fontsize=12)
            _ax1.legend(fontsize=11)
            _ax1.grid(True, alpha=0.3)

            # Accuracy plot
            _ax2.plot(history['val_acc'], label='Val Accuracy',
                    color='green', marker='o', linewidth=2)
            _ax2.set_title('Validation Accuracy', fontsize=14, fontweight='bold')
            _ax2.set_xlabel('Epoch', fontsize=12)
            _ax2.set_ylabel('Accuracy', fontsize=12)
            _ax2.legend(fontsize=11)
            _ax2.grid(True, alpha=0.3)

            plt.tight_layout()

            # Save and log figure
            history_plot_path = osp.join(artifacts_path, "training_history_unsw.png")
            plt.savefig(history_plot_path, dpi=150, bbox_inches='tight')
            mlflow.log_artifact(history_plot_path, artifact_path="plots")
            plt.show()

        print(f"✓ Training complete. Run ID: {run.info.run_id}")
        run_id = run.info.run_id

    return history, run_id


@app.cell
def _(mo):
    mo.md(r"""
    ## Evaluate on Test Set

    Run inference and compute comprehensive evaluation metrics.
    """)
    return


@app.cell
def _(params):
    # Define class names
    if params["binary_mode"]:
        class_names = ["Normal", "Attack"]
    else:
        class_names = [
            "Normal", "Worms", "Backdoors", "Shellcode", "Analysis",
            "Reconnaissance", "DoS", "Fuzzers", "Exploits", "Generic"
        ]

    return (class_names,)


@app.cell
def _(device, model, params, test_loader, torch):
    # Run inference
    model.eval()
    y_true = []
    y_pred = []
    y_probs = []

    print("Running inference on test set...")
    with torch.no_grad():
        for batch in test_loader:
            batch = batch.to(device)
            out = model(batch.x, batch.edge_index)
            probs = torch.softmax(out, dim=1)
            preds = out.argmax(dim=1)

            y_true.extend(batch.y.cpu().numpy())
            y_pred.extend(preds.cpu().numpy())

            if params["binary_mode"]:
                y_probs.extend(probs[:, 1].cpu().numpy())

    print("✓ Inference complete")

    return y_true, y_pred, y_probs


@app.cell
def _(
    accuracy_score, balanced_accuracy_score, classification_report,
    f1_score, mlflow, np, params, recall_score, run_id, y_pred, y_true
):
    # Compute metrics
    test_accuracy = accuracy_score(y_true, y_pred)
    test_balanced_acc = balanced_accuracy_score(y_true, y_pred)
    test_recall = recall_score(y_true, y_pred, average='weighted')
    test_f1 = f1_score(y_true, y_pred, average='weighted')

    # Log test metrics to MLflow
    with mlflow.start_run(run_id=run_id):
        mlflow.log_metrics({
            "test_accuracy": test_accuracy,
            "test_balanced_accuracy": test_balanced_acc,
            "test_recall": test_recall,
            "test_f1_score": test_f1
        })

        # Log per-class metrics for multiclass
        if not params["binary_mode"]:
            report_dict = classification_report(y_true, y_pred, output_dict=True)
            for label, metrics in report_dict.items():
                if isinstance(metrics, dict):
                    for metric_name, value in metrics.items():
                        mlflow.log_metric(f"test_{label}_{metric_name}", value)

    print(f"\n{'='*60}")
    print(f"Test Set Evaluation")
    print(f"{'='*60}")
    print(f"Accuracy:          {test_accuracy:.4f}")
    print(f"Balanced Accuracy: {test_balanced_acc:.4f}")
    print(f"Recall (Weighted): {test_recall:.4f}")
    print(f"F1 Score (Weighted): {test_f1:.4f}")
    print(f"{'='*60}\n")

    print(classification_report(y_true, y_pred, digits=4, zero_division=0))

    return test_accuracy, test_balanced_acc, test_recall, test_f1


@app.cell
def _(mo):
    mo.md(r"""
    ## Visualization

    Visualize results with ROC/PR curves or confusion matrix.
    """)
    return


@app.cell
def _(
    artifacts_path, auc, average_precision_score, class_names,
    confusion_matrix, mlflow, np, osp, params, plt,
    precision_recall_curve, roc_curve, run_id, sns,
    y_pred, y_probs, y_true
):
    plt.figure(figsize=(14, 6))

    if params["binary_mode"]:
        # Binary classification: ROC and PR curves
        fpr, tpr, _ = roc_curve(y_true, y_probs)
        roc_auc = auc(fpr, tpr)

        precision, recall, _ = precision_recall_curve(y_true, y_probs)
        pr_auc = average_precision_score(y_true, y_probs)

        _fig, (_ax1, _ax2) = plt.subplots(1, 2, figsize=(14, 6))

        # ROC Curve
        _ax1.plot(fpr, tpr, color='darkorange', lw=2.5,
                label=f'ROC (AUC = {roc_auc:.4f})')
        _ax1.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--',
                label='Random')
        _ax1.set_xlim([0.0, 1.0])
        _ax1.set_ylim([0.0, 1.05])
        _ax1.set_xlabel('False Positive Rate', fontsize=12)
        _ax1.set_ylabel('True Positive Rate', fontsize=12)
        _ax1.set_title('ROC Curve', fontsize=14, fontweight='bold')
        _ax1.legend(loc="lower right", fontsize=11)
        _ax1.grid(True, alpha=0.3)

        # PR Curve
        _ax2.plot(recall, precision, color='darkorange', lw=2.5,
                label=f'PR (AUC = {pr_auc:.4f})')
        _ax2.axhline(y=np.mean(y_true), color='navy', lw=2,
                   linestyle='--', label=f'Baseline ({np.mean(y_true):.3f})')
        _ax2.set_xlim([0.0, 1.0])
        _ax2.set_ylim([0.0, 1.05])
        _ax2.set_xlabel('Recall', fontsize=12)
        _ax2.set_ylabel('Precision', fontsize=12)
        _ax2.set_title('Precision-Recall Curve', fontsize=14, fontweight='bold')
        _ax2.legend(loc="lower left", fontsize=11)
        _ax2.grid(True, alpha=0.3)

        plt.tight_layout()

        # Save and log
        curves_path = osp.join(artifacts_path, "roc_pr_curves_unsw.png")
        plt.savefig(curves_path, dpi=150, bbox_inches='tight')

        with mlflow.start_run(run_id=run_id):
            mlflow.log_artifact(curves_path, artifact_path="plots")
            mlflow.log_metrics({"test_roc_auc": roc_auc, "test_pr_auc": pr_auc})

    else:
        # Multi-class: Confusion Matrix
        cm = confusion_matrix(y_true, y_pred)
        cm_norm = cm.astype('float') / (cm.sum(axis=1)[:, np.newaxis] + 1e-7)

        # Create annotation text
        annot = np.empty_like(cm).astype(object)
        rows, cols = cm.shape
        for i in range(rows):
            for j in range(cols):
                if cm[i, j] > 0:
                    if i == j:
                        annot[i, j] = f"{cm_norm[i, j]*100:.1f}%\n({cm[i, j]})"
                    else:
                        annot[i, j] = f"{cm[i, j]}"
                else:
                    annot[i, j] = ""

        plt.figure(figsize=(12, 10))
        sns.heatmap(
            cm_norm, annot=annot, fmt='', cmap='YlGnBu',
            cbar=True, linewidths=0.5, linecolor='lightgray',
            xticklabels=class_names, yticklabels=class_names,
            cbar_kws={'label': 'Normalized Recall'}
        )
        plt.title('Confusion Matrix (Normalized by Recall)',
                 fontsize=16, fontweight='bold', pad=20)
        plt.xlabel('Predicted Label', fontsize=13)
        plt.ylabel('True Label', fontsize=13)
        plt.xticks(rotation=45, ha='right')
        plt.yticks(rotation=0)
        plt.tight_layout()

        # Save and log
        cm_path = osp.join(artifacts_path, "confusion_matrix_unsw.png")
        plt.savefig(cm_path, dpi=150, bbox_inches='tight')

        with mlflow.start_run(run_id=run_id):
            mlflow.log_artifact(cm_path, artifact_path="plots")

    plt.show()
    print("✓ Visualizations logged to MLflow")

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Summary

    Training and evaluation complete! Check MLflow UI for detailed results and artifacts.
    """)
    return


if __name__ == "__main__":
    app.run()
