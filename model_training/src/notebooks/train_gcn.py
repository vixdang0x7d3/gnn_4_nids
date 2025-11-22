import marimo

__generated_with = "0.17.7"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import sys
    import os

    import pickle

    import os.path as osp


    import torch
    import torch.nn as nn
    from torch.optim import Adadelta

    from torch_geometric.loader import RandomNodeLoader

    import duckdb
    import numpy as np

    from sklearn.utils import class_weight

    from sklearn.metrics import accuracy_score
    from sklearn.metrics import recall_score
    from sklearn.metrics import classification_report
    from sklearn.metrics import balanced_accuracy_score
    from sklearn.metrics import confusion_matrix
    return (
        Adadelta,
        RandomNodeLoader,
        accuracy_score,
        balanced_accuracy_score,
        class_weight,
        classification_report,
        duckdb,
        mo,
        nn,
        np,
        osp,
        pickle,
        sys,
        torch,
    )


@app.cell
def _(sys):
    cwd = "/home/v/works/gnn_4_nids"

    if cwd not in sys.path:
        sys.path.append(cwd)

    from src.gnn.nb15_gcn import NB15_GCN
    from src.gnn.helpers import train, predict
    from src.gnn.const import FEATURE_ATTRIBUTES, EDGE_ATTRIBUTES, BINARY_LABEL, MULTICLASS_LABEL

    from src.dataprep.graph_gcn import GraphGCN
    from src.dataprep.transform import groupwise_smote, minmax_scale
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
    ### Define settings
    """)
    return


@app.cell
def _(duckdb, osp):
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
      ORDER BY stime
    )
    """

    # CONFIGURATIONS

    # Paths
    dataset_path = "data/NB15_preprocessed"

    raw_path = osp.join(dataset_path, "raw")
    graph_path = osp.join(dataset_path, "graph")

    trainset_path = osp.join(raw_path, "train.parquet")
    testset_path = osp.join(raw_path, "test.parquet")
    valset_path  = osp.join(raw_path, "val.parquet")

    artifacts_path = "models/"


    # Data configuration
    BINARY=False
    USE_SMOTE=True
    SMOTE_MIN_SAMPLES=100
    SMOTE_K_NEIGHBORS=6
    GRAPH_N_NEIGHBORS=2

    #  Loader configurations
    NUM_PARTS=256
    SHUFFLE=True

    # Model configurations
    N_FEATURES=14
    N_CONVS=64
    N_HIDDEN=512
    ALPHA=0.5
    THETA=0.7
    DROPOUT=0.5
    EPOCHS=12


    # Artifact save paths
    model_name = (
        "gcn_nb15" + 
        f"_{"bin" if BINARY else "multi"}" + 
        f"_{'smotek' + str(SMOTE_K_NEIGHBORS) + 'm' + str(SMOTE_MIN_SAMPLES) if (USE_SMOTE and not BINARY) else 'no_smote'}" + 
        f"_n{GRAPH_N_NEIGHBORS}" + 
        f"_conv{N_CONVS}" +
        f"_hidd{N_HIDDEN}"
    )

    model_save_path = osp.join(artifacts_path, model_name)
    return (
        ALPHA,
        BINARY,
        DROPOUT,
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
        model_name,
        model_save_path,
        testset_path,
        trainset_path,
        valset_path,
    )


@app.cell
def _(mo):
    mo.md(r"""
    ### Load data
    """)
    return


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
        parquet_path=trainset_path
    )

    train_data = conn.sql(load_train_sql)


    load_val_query = LOAD_SQL.format(
        feature_cols=", ".join(FEATURE_ATTRIBUTES),
        edge_cols=", ".join(EDGE_ATTRIBUTES),
        label=BINARY_LABEL if BINARY else MULTICLASS_LABEL,
        parquet_path=valset_path
    )

    val_data = conn.sql(load_val_query)


    load_test_query = LOAD_SQL.format(
        feature_cols=", ".join(FEATURE_ATTRIBUTES),
        edge_cols=", ".join(EDGE_ATTRIBUTES),
        label=BINARY_LABEL if BINARY else MULTICLASS_LABEL,
        parquet_path=testset_path
    )

    test_data = conn.sql(load_test_query)
    return test_data, train_data, val_data


@app.cell
def _(mo):
    mo.md(r"""
    ### Apply MinMax scaling and SMOTE for training set
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
    osp,
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
            k_neighbors=SMOTE_K_NEIGHBORS
        )

    # using scaler fitted with statistics on train set
    # to avoid data leakage
    scaler_save_path = osp.join(artifacts_path, "fitted_mm_scaler.pkl")
    with open(scaler_save_path, "wb") as f:
        pickle.dump(fitted_mm_scaler, f)

    val_arrow = val_data.arrow().read_all()
    val_arrow, _ = minmax_scale(val_arrow, FEATURE_ATTRIBUTES, fitted_mm_scaler)

    test_arrow = test_data.arrow().read_all()
    test_arrow, _ = minmax_scale(test_arrow, FEATURE_ATTRIBUTES, fitted_mm_scaler)
    return train_arrow, val_arrow


@app.cell
def _(mo):
    mo.md(r"""
    ### Build input graphs
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
    train_arrow,
    val_arrow,
):
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

    train_loader = RandomNodeLoader(data=train_graph, num_parts=NUM_PARTS, shuffle=SHUFFLE)
    val_loader = RandomNodeLoader(data=val_graph, num_parts=NUM_PARTS, shuffle=SHUFFLE)
    test_loader = RandomNodeLoader(data=test_graph, num_parts=NUM_PARTS, shuffle=SHUFFLE)
    return test_loader, train_graph, train_loader, val_graph, val_loader


@app.cell
def _(mo):
    mo.md(r"""
    ### Define GCN model
    """)
    return


@app.cell
def _(ALPHA, BINARY, DROPOUT, NB15_GCN, N_CONVS, N_FEATURES, N_HIDDEN, THETA):
    # Define GCN model 
    model = NB15_GCN(
        n_features=N_FEATURES,
        n_classes= 2 if BINARY else 10, # binary
        n_convs=N_CONVS,
        n_hidden=N_HIDDEN,
        alpha=ALPHA,
        theta=THETA,
        dropout=DROPOUT,
    )
    return (model,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Configure class weights, optimizer and loss function
    """)
    return


@app.cell
def _(Adadelta, class_weight, model, nn, np, torch, train_graph, val_graph):
    # Configure device, optimizer and loss function
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model.to(device)

    y = train_graph.y.numpy()
    class_weights = class_weight.compute_class_weight(
        'balanced', 
        classes=np.unique(y), 
        y=y
    )
    class_weights = torch.tensor(class_weights, dtype=torch.float)
    loss_fn = nn.CrossEntropyLoss(weight=class_weights.to(device))

    y_val = val_graph.y.numpy()
    class_weights_val = class_weight.compute_class_weight(
        'balanced',
        classes=np.unique(y_val),
        y=y_val,
    )
    class_weights_val = torch.tensor(
        class_weights_val, 
        dtype=torch.float
    )
    loss_fn_val = nn.CrossEntropyLoss(weight=class_weights.to(device))

    optimizer = Adadelta(model.parameters())
    return device, loss_fn, loss_fn_val, optimizer


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Train model
    """)
    return


@app.cell
def _(
    device,
    loss_fn,
    loss_fn_val,
    model,
    model_name,
    model_save_path,
    optimizer,
    osp,
    torch,
    train,
    train_loader,
    val_loader,
):
    model_file = model_save_path + ".h5"
    print(model_file)

    if osp.exists(model_file):
        model.load_state_dict(torch.load(model_file, weights_only=False))
    else:
        train(
            model=model,
            loss_fn=loss_fn,
            train_dataloader=train_loader,
            optimizer=optimizer,
            device=device,
            model_path="models/",
            model_name=model_name,
            eval=True,
            loss_fn_val=loss_fn_val,
            val_dataloader=val_loader,
            epochs=10,
        )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Basic evaluation
    """)
    return


@app.cell
def _(
    accuracy_score,
    balanced_accuracy_score,
    classification_report,
    device,
    model,
    predict,
    test_loader,
):
    y_true, y_pred = predict(model, test_loader, device)

    print(f'Accuracy on test: {accuracy_score(y_true, y_pred)}')
    print(f'Balanced accuracy on test: {balanced_accuracy_score(y_true, y_pred)}')
    print(f'\n{classification_report(y_true, y_pred, digits=3)}')
    return


if __name__ == "__main__":
    app.run()
