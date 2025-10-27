import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import sys
    import os

    import os.path as osp

    sys.path.append(os.getcwd())

    from src.gnn.dataset import NB15Dataset
    from src.gnn.nb15_gcn import NB15_GCN
    from src.gnn.helpers import train, predict

    import torch
    import torch.nn as nn
    from torch.optim import Adadelta

    from torch_geometric.loader import RandomNodeLoader

    import numpy as np

    from sklearn.utils import class_weight

    from sklearn.metrics import accuracy_score
    from sklearn.metrics import recall_score
    from sklearn.metrics import classification_report
    from sklearn.metrics import balanced_accuracy_score
    from sklearn.metrics import confusion_matrix
    return (
        Adadelta,
        NB15Dataset,
        NB15_GCN,
        RandomNodeLoader,
        accuracy_score,
        balanced_accuracy_score,
        class_weight,
        classification_report,
        nn,
        np,
        os,
        osp,
        predict,
        torch,
        train,
    )


@app.cell
def _(NB15Dataset):
    train_set = NB15Dataset(
        root='dataset/NB15/',
        n_neighbors=1,
        split=0,
    )

    val_set = NB15Dataset(
        root='dataset/NB15/',
        n_neighbors=1,
        split=1,
    )

    test_set = NB15Dataset(
        root='dataset/NB15/',
        n_neighbors=1,
        split=2,
    )
    return test_set, train_set, val_set


@app.cell
def _(train_set):
    train_set[0].num_node_features
    return


@app.cell
def _(RandomNodeLoader, test_set, train_set, val_set):
    # Define train, val and test loader
    train_loader = RandomNodeLoader(train_set[0], num_parts=256, shuffle=True)
    val_loader = RandomNodeLoader(val_set[0], num_parts=256, shuffle=True)
    test_loader = RandomNodeLoader(test_set[0], num_parts=256, shuffle=True)
    return test_loader, train_loader, val_loader


@app.cell
def _(NB15_GCN, torch):
    device= torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    model = NB15_GCN(
        n_features=14,
        n_classes=2, # binary
        n_convs=64,
        n_hidden=512,
        alpha=0.5,
        theta=0.7,
        dropout=0.5,
    )

    model.to(device)
    return device, model


@app.cell
def _(
    Adadelta,
    class_weight,
    device,
    model,
    nn,
    np,
    torch,
    train_set,
    val_set,
):
    y = train_set[0].y.numpy()
    class_weights = class_weight.compute_class_weight(
        'balanced', 
        classes=np.unique(y), 
        y=y
    )
    class_weights = torch.tensor(class_weights, dtype=torch.float)
    loss_fn = nn.CrossEntropyLoss(weight=class_weights.to(device))

    y_val = val_set[0].y.numpy()
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
    return loss_fn, loss_fn_val, optimizer


@app.cell
def _(
    device,
    loss_fn,
    loss_fn_val,
    model,
    optimizer,
    os,
    osp,
    torch,
    train,
    train_loader,
    val_loader,
):
    n_neighbors=1
    n_hidden=512
    n_convs=64

    model_name = f"gcn_nb15_{n_neighbors}_{n_hidden}_{n_convs}"
    model_save_path = os.path.join('models', f'{model_name}.h5')

    if osp.exists(model_save_path):
        model.load_state_dict(
            torch.load(
                model_save_path,
                weights_only=False
            )
        )

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
        )
    return


@app.cell
def _(model):
    model
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
