import marimo

__generated_with = "0.18.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import pickle
    import sys
    import os

    from pathlib import Path


    import duckdb
    import marimo as mo
    import matplotlib.pyplot as plt
    import mlflow
    import mlflow.pytorch
    import numpy as np
    import seaborn as sns
    import torch
    import torch.nn as nn
    from sklearn.metrics import (
        accuracy_score,
        auc,
        average_precision_score,
        balanced_accuracy_score,
        classification_report,
        confusion_matrix,
        precision_recall_curve,
        roc_curve,
    )
    from sklearn.utils import class_weight
    from torch.optim import Adadelta
    from torch_geometric.loader import RandomNodeLoader
    return (
        Adadelta,
        Path,
        RandomNodeLoader,
        accuracy_score,
        auc,
        average_precision_score,
        balanced_accuracy_score,
        class_weight,
        classification_report,
        confusion_matrix,
        duckdb,
        mlflow,
        mo,
        nn,
        np,
        os,
        pickle,
        plt,
        precision_recall_curve,
        roc_curve,
        sns,
        sys,
        torch,
    )


@app.cell
def _(Path, sys):
    project_root = Path(__file__).resolve().parent.parent.parent

    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    return


@app.cell
def _():
    from graph_building.graph_gcn import GraphGCN
    from graph_building.transform import groupwise_smote, minmax_scale

    from src.gnn.const import (
        BINARY_LABEL,
        EDGE_ATTRIBUTES,
        FEATURE_ATTRIBUTES,
        MULTICLASS_LABEL,
    )
    from src.gnn.helpers import predict, train
    from src.gnn.nb15_gcn import NB15_GCN
    return (
        BINARY_LABEL,
        EDGE_ATTRIBUTES,
        FEATURE_ATTRIBUTES,
        GraphGCN,
        MULTICLASS_LABEL,
        NB15_GCN,
        groupwise_smote,
        minmax_scale,
        predict,
        train,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Setup training
    """)
    return


@app.cell
def _(Path, duckdb, os):
    # Whether to loads saved weights when model file exists
    load_saved = True

    # MLflow envars
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"
    os.environ['AWS_ACCESS_KEY_ID'] = 'admin'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin123'



    conn = duckdb.connect()

    # QUERY TEMPLATE
    # Load parquet files
    LOAD_SQL = """
    SELECT
      ROW_NUMBER() OVER () as index,
      {feature_cols},
      {edge_cols},
      {label},
    FROM (
      SELECT * FROM '{parquet_path}'
      ORDER BY {edge_cols}, stime
    )
    """

    # --- TRAINING - MLFLOW CONFIGURATIONS---
    params = {
        # Data Config
        "binary_mode": False,
        "use_smote": True,
        "smote_min_samples": 100,
        "smote_k_neighbors": 5,
        "graph_n_neighbors": 2,

        # Loader Config
        "num_parts": 256,
        "shuffle": True,

        # Model Config
        "n_features": 14,
        "n_convs": 64,
        "n_hidden": 512,
        "alpha": 0.5,
        "theta": 0.7,
        "dropout": 0.5,

        # Training Config
        "epochs": 1,
        "optimizer": "Adadelta",
    }
    dataset_path = Path("data") / "NB15_preprocessed" 
    artifacts_path = Path("artifacts")

    BINARY = params["binary_mode"]
    USE_SMOTE = params["use_smote"]
    SMOTE_MIN_SAMPLES = params["smote_min_samples"]
    SMOTE_K_NEIGHBORS = params["smote_k_neighbors"]
    GRAPH_N_NEIGHBORS = params["graph_n_neighbors"]

    NUM_PARTS = params["num_parts"]
    SHUFFLE = params["shuffle"]

    N_FEATURES = params["n_features"]
    N_CONVS = params["n_convs"]
    N_HIDDEN = params["n_hidden"]
    ALPHA = params["alpha"]
    THETA = params["theta"]
    DROPOUT = params["dropout"]
    EPOCHS = params["epochs"]

    raw_path = Path(dataset_path) /  "raw"
    graph_path = Path(dataset_path) / "graph"

    trainset_path = Path(raw_path) / "train.parquet"
    testset_path = Path(raw_path) / "test.parquet"
    valset_path = Path(raw_path) / "val.parquet"

    # Named Experiment for MLflow
    EXPERIMENT_NAME = "GNN_NIDS_UNSW_NB15"

    # MLflow run name and model name
    model_name = (
        "gcn_nb15"
        + f"_{'bin' if BINARY else 'multi'}"
        + f"_{'smotek' + str(SMOTE_K_NEIGHBORS) + 'm' + str(SMOTE_MIN_SAMPLES) if (USE_SMOTE and not BINARY) else 'no_smote'}"
        + f"_n{GRAPH_N_NEIGHBORS}"
        + f"_conv{N_CONVS}"
        + f"_hidd{N_HIDDEN}"
    )

    model_save_path = Path(artifacts_path) / f"{model_name}.h5"
    return (
        ALPHA,
        BINARY,
        DROPOUT,
        EXPERIMENT_NAME,
        GRAPH_N_NEIGHBORS,
        LOAD_SQL,
        NUM_PARTS,
        N_CONVS,
        N_FEATURES,
        N_HIDDEN,
        SHUFFLE,
        SMOTE_K_NEIGHBORS,
        SMOTE_MIN_SAMPLES,
        THETA,
        USE_SMOTE,
        artifacts_path,
        conn,
        graph_path,
        load_saved,
        model_name,
        model_save_path,
        params,
        testset_path,
        trainset_path,
        valset_path,
    )


@app.cell
def _(
    BINARY,
    BINARY_LABEL,
    EDGE_ATTRIBUTES,
    FEATURE_ATTRIBUTES,
    LOAD_SQL,
    MULTICLASS_LABEL,
    conn,
    testset_path,
    trainset_path,
    valset_path,
):
    # Load parquet files
    load_train_sql = LOAD_SQL.format(
        feature_cols=", ".join(FEATURE_ATTRIBUTES),
        edge_cols=", ".join(EDGE_ATTRIBUTES),
        label=BINARY_LABEL if BINARY else MULTICLASS_LABEL,
        parquet_path=str(trainset_path),
    )
    train_data = conn.sql(load_train_sql)
    load_val_query = LOAD_SQL.format(
        feature_cols=", ".join(FEATURE_ATTRIBUTES),
        edge_cols=", ".join(EDGE_ATTRIBUTES),
        label=BINARY_LABEL if BINARY else MULTICLASS_LABEL,
        parquet_path=str(valset_path),
    )
    val_data = conn.sql(load_val_query)
    load_test_query = LOAD_SQL.format(
        feature_cols=", ".join(FEATURE_ATTRIBUTES),
        edge_cols=", ".join(EDGE_ATTRIBUTES),
        label=BINARY_LABEL if BINARY else MULTICLASS_LABEL,
        parquet_path=str(testset_path),
    )
    test_data = conn.sql(load_test_query)
    return test_data, train_data, val_data


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Apply MinMax scaling and SMOTE for training set
    """)
    return


@app.cell
def _(
    BINARY,
    EDGE_ATTRIBUTES,
    FEATURE_ATTRIBUTES,
    SMOTE_K_NEIGHBORS,
    SMOTE_MIN_SAMPLES,
    USE_SMOTE,
    artifacts_path,
    groupwise_smote,
    minmax_scale,
    pickle,
    test_data,
    train_data,
    val_data,
):
    # apply transformations
    train_arrow = train_data.arrow().read_all()
    train_arrow, fitted_mm_scaler = minmax_scale(train_arrow, FEATURE_ATTRIBUTES)

    if not BINARY and USE_SMOTE:
        train_arrow = groupwise_smote(
            table=train_arrow,
            feature_attrs=FEATURE_ATTRIBUTES,
            group_attrs=EDGE_ATTRIBUTES,
            min_samples=SMOTE_MIN_SAMPLES,
            k_neighbors=SMOTE_K_NEIGHBORS,
        )

    # using scaler fitted with statistics on train set
    # to avoid data leakage
    scaler_save_path = artifacts_path / "fitted_mm_scaler.pkl"
    with open(scaler_save_path, "wb") as f:
        pickle.dump(fitted_mm_scaler, f)

    val_arrow = val_data.arrow().read_all()
    val_arrow, _ = minmax_scale(val_arrow, FEATURE_ATTRIBUTES, fitted_mm_scaler)

    test_arrow = test_data.arrow().read_all()
    test_arrow, _ = minmax_scale(test_arrow, FEATURE_ATTRIBUTES, fitted_mm_scaler)
    return train_arrow, val_arrow


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Build input graphs
    """)
    return


@app.cell
def _(
    BINARY,
    BINARY_LABEL,
    EDGE_ATTRIBUTES,
    FEATURE_ATTRIBUTES,
    GRAPH_N_NEIGHBORS,
    GraphGCN,
    MULTICLASS_LABEL,
    NUM_PARTS,
    RandomNodeLoader,
    SHUFFLE,
    graph_path,
    torch,
    train_arrow,
    val_arrow,
):
    if False:# any(graph_path.iterdir()):
        print("Pre-built graphs exist, loading...")
        train_graph = torch.load(graph_path / "train.pt", weights_only=False)
        test_graph = torch.load(graph_path / "test.pt", weights_only=False)
        val_graph = torch.load(graph_path / "val.pt", weights_only=False)
    else:
        print("No pre-built graph, building and saving...")


        train_graph = GraphGCN(
            table=train_arrow,
            node_attrs=FEATURE_ATTRIBUTES,
            edge_attrs=EDGE_ATTRIBUTES,
            label=BINARY_LABEL if BINARY else MULTICLASS_LABEL,
            n_neighbors=GRAPH_N_NEIGHBORS,
        ).build(include_labels=True)

        val_graph = GraphGCN(
            table=val_arrow,
            node_attrs=FEATURE_ATTRIBUTES,
            edge_attrs=EDGE_ATTRIBUTES,
            label=BINARY_LABEL if BINARY else MULTICLASS_LABEL,
            n_neighbors=GRAPH_N_NEIGHBORS,
        ).build(include_labels=True)

        test_graph = GraphGCN(
            table=val_arrow,
            node_attrs=FEATURE_ATTRIBUTES,
            edge_attrs=EDGE_ATTRIBUTES,
            label=BINARY_LABEL if BINARY else MULTICLASS_LABEL,
            n_neighbors=GRAPH_N_NEIGHBORS,
        ).build(include_labels=True)

        torch.save(train_graph, graph_path / "train.pt")
        torch.save(test_graph, graph_path / "test.pt")
        torch.save(val_graph, graph_path / "val.pt")

    train_loader = RandomNodeLoader(
        data=train_graph, num_parts=NUM_PARTS, shuffle=SHUFFLE
    )
    val_loader = RandomNodeLoader(data=val_graph, num_parts=NUM_PARTS, shuffle=SHUFFLE)
    test_loader = RandomNodeLoader(
        data=test_graph, num_parts=NUM_PARTS, shuffle=SHUFFLE
    )
    return test_loader, train_graph, train_loader, val_graph, val_loader


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Define GCN model
    """)
    return


@app.cell
def _(ALPHA, BINARY, DROPOUT, NB15_GCN, N_CONVS, N_FEATURES, N_HIDDEN, THETA):
    # Define GCN model
    model = NB15_GCN(
        n_features=N_FEATURES,
        n_classes=2 if BINARY else 10,  # binary
        n_convs=N_CONVS,
        n_hidden=N_HIDDEN,
        alpha=ALPHA,
        theta=THETA,
        dropout=DROPOUT,
    )
    return (model,)


@app.cell
def _(model):
    model
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Configure class weights, optimizer and loss function
    """)
    return


@app.cell
def _(Adadelta, class_weight, model, nn, np, torch, train_graph, val_graph):
    # Configure device, optimizer and loss function
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    y = train_graph.y.numpy()
    class_weights = class_weight.compute_class_weight(
        "balanced", classes=np.unique(y), y=y
    )
    class_weights = torch.tensor(class_weights, dtype=torch.float)
    loss_fn = nn.CrossEntropyLoss(weight=class_weights.to(device))

    y_val = val_graph.y.numpy()
    class_weights_val = class_weight.compute_class_weight(
        "balanced",
        classes=np.unique(y_val),
        y=y_val,
    )
    class_weights_val = torch.tensor(class_weights_val, dtype=torch.float)
    loss_fn_val = nn.CrossEntropyLoss(weight=class_weights.to(device))

    optimizer = Adadelta(model.parameters())
    return device, loss_fn, loss_fn_val, optimizer


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Train models
    """)
    return


@app.cell
def _(
    EXPERIMENT_NAME,
    artifacts_path,
    device,
    load_saved,
    loss_fn,
    loss_fn_val,
    mlflow,
    model,
    model_name,
    model_save_path,
    optimizer,
    params,
    torch,
    train,
    train_loader,
    val_loader,
):
    mlflow.set_tracking_uri("http://localhost:5000")


    # Load existing model
    if model_save_path.exists() and load_saved:
        print(f"Loading model from {model_save_path}...")
        model.load_state_dict(
            torch.load(model_save_path, map_location=device, weights_only=False)
        )

    # Train new model
    else:
        # Setup Experiment
        mlflow.set_experiment(EXPERIMENT_NAME)

        print(f"Experiment: {EXPERIMENT_NAME}")
        print(f"Starting Run: {model_name}")
        print(f"Tracking URI: {mlflow.get_tracking_uri()}")
        print(f"")

        # Start Run
        with mlflow.start_run(run_name=model_name) as run:
            print(f"Run ID: {run.info.run_id}")
            print("")

            mlflow.log_params(params)

            train(
                model=model,
                loss_fn=loss_fn,
                train_dataloader=train_loader,
                optimizer=optimizer,
                device=device,
                model_path=artifacts_path,
                model_name=model_name,
                eval=True,
                loss_fn_val=loss_fn_val,
                val_dataloader=val_loader,
                epochs=params["epochs"],
            )

            mlflow.pytorch.log_model(model, name=model_name)

            scaler_path = artifacts_path / "fitted_mm_scaler.pkl"
            if scaler_path.exists():
                mlflow.log_artifact(scaler_path, artifact_path="preprocessor")
                print("Logged scaler artifact to MLflow.")

        print(f"Run ID: {run.info.run_id} completed.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Models Evaluation
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Run Inference on Test Set
    """)
    return


@app.cell
def _(BINARY):
    class_names = [
        "Normal",  # 0
        "Worms",  # 1
        "Backdoors",  # 2
        "Shellcode",  # 3
        "Analysis",  # 4
        "Reconnaissance",  # 5
        "DoS",  # 6
        "Fuzzers",  # 7
        "Exploits",  # 8
        "Generic",  # 9
    ]
    if BINARY:
        class_names = ["Normal", "Attack"]
    return (class_names,)


@app.cell
def _(device, model, predict, test_loader):
    y_true, y_pred, probs = predict(model, test_loader, device)
    y_probs = probs[:,1]
    y_probs
    return y_pred, y_probs, y_true


@app.cell
def _(BINARY, device, model, test_loader, torch):
    def _():
        # Chuyển model sang chế độ eval
        model.eval()
        y_true = []
        y_pred = []
        y_probs = []
        print("Running inference...")
        with torch.no_grad():
            for batch in test_loader:
                batch = batch.to(device)
                out = model(batch.x, batch.edge_index)
                # ------------------------------------------
                # Tính xác suất
                probs = torch.softmax(out, dim=1)
                # Lấy nhãn dự đoán
                preds = out.argmax(dim=1)
                y_true.extend(batch.y.cpu().numpy())
                y_pred.extend(preds.cpu().numpy())
                # Nếu là Binary, lưu xác suất lớp 1
                if BINARY:
                    y_probs.extend(probs[:, 1].cpu().numpy())
        print("Inference complete.")

    _()
    return


@app.cell
def _(accuracy_score, balanced_accuracy_score, y_pred, y_true):
    print(f"Accuracy on test: {accuracy_score(y_true, y_pred):.4f}")
    print(f"Balanced accuracy: {balanced_accuracy_score(y_true, y_pred):.4f}")
    print("-" * 30)
    return


@app.cell
def _(classification_report, y_pred, y_true):
    print(classification_report(y_true, y_pred, digits=4, zero_division=0))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Visualize Results (ROC-AUC / Confusion Matrix)
    """)
    return


@app.cell
def _(
    BINARY,
    auc,
    average_precision_score,
    class_names,
    confusion_matrix,
    np,
    plt,
    precision_recall_curve,
    roc_curve,
    sns,
    y_pred,
    y_probs,
    y_true,
):
    if BINARY:
        # Existing ROC curve code
        fpr, tpr, _ = roc_curve(y_true, y_probs)
        roc_auc = auc(fpr, tpr)

        # PR curve
        precision, recall, _ = precision_recall_curve(y_true, y_probs)
        pr_auc = average_precision_score(y_true, y_probs)

        # Plot both
        _fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

        # ROC curve
        ax1.plot(fpr, tpr, color="darkorange", lw=2, label=f"ROC (AUC = {roc_auc:.4f})")
        ax1.plot([0, 1], [0, 1], color="navy", lw=2, linestyle="--")
        ax1.set_xlim([0.0, 1.0])
        ax1.set_ylim([0.0, 1.05])
        ax1.set_xlabel("False Positive Rate")
        ax1.set_ylabel("True Positive Rate")
        ax1.set_title("ROC Curve")
        ax1.legend(loc="lower right")
        ax1.grid(True, alpha=0.3)

        # PR curve
        ax2.plot(
            recall,
            precision,
            color="darkorange",
            lw=2,
            label=f"PR (AUC = {pr_auc:.4f})",
        )
        ax2.axhline(
            y=np.mean(y_true),
            color="navy",
            lw=2,
            linestyle="--",
            label=f"Baseline ({np.mean(y_true):.3f})",
        )
        ax2.set_xlim([0.0, 1.0])
        ax2.set_ylim([0.0, 1.05])
        ax2.set_xlabel("Recall")
        ax2.set_ylabel("Precision")
        ax2.set_title("Precision-Recall Curve")
        ax2.legend(loc="lower left")
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()

        # print(f"ROC-AUC: {roc_auc:.4f}")
        # print(f"PR-AUC:  {pr_auc:.4f}")

    else:
        cm = confusion_matrix(y_true, y_pred)
        cm_norm = cm.astype("float") / (cm.sum(axis=1)[:, np.newaxis] + 1e-7)

        # Tạo text hiển thị trong ô (VD: "85%\n(102)")
        annot = np.empty_like(cm).astype(object)
        rows, cols = cm.shape
        for i in range(rows):
            for j in range(cols):
                if cm[i, j] > 0:
                    if i == j:
                        annot[i, j] = f"{cm_norm[i, j] * 100:.1f}%\n({cm[i, j]})"
                    else:
                        annot[i, j] = f"{cm[i, j]}"
                else:
                    annot[i, j] = ""

        sns.heatmap(
            cm_norm,
            annot=annot,
            fmt="",
            cmap="YlGnBu",
            cbar=True,
            linewidths=0.5,
            linecolor="lightgray",
            xticklabels=class_names,
            yticklabels=class_names,
        )
        plt.title("Confusion Matrix (Normalized by Recall)", fontsize=14)
        plt.xlabel("Predicted Label")
        plt.ylabel("True Label")
        plt.xticks(rotation=45, ha="right")
        plt.yticks(rotation=0)

    plt.tight_layout()
    plt.show()
    return


if __name__ == "__main__":
    app.run()
