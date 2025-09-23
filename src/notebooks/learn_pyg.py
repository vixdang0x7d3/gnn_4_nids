import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import torch
    from torch_geometric.data import Data

    return Data, torch, mo


@app.cell
def _(Data, torch):
    _edge_index = torch.tensor([[0, 1, 1, 2], [1, 0, 2, 1]], dtype=torch.long)
    _x = torch.tensor([[-1], [0], [1]], dtype=torch.float)
    _data = Data(x=_x, edge_index=_edge_index)

    _data
    return


@app.cell
def _(Data, torch):
    _edge_index = torch.tensor([[0, 1], [1, 0], [1, 2], [2, 1]], dtype=torch.long)
    _x = torch.tensor([[-1], [0], [1]], dtype=torch.float)
    data0 = Data(
        x=_x, edge_index=_edge_index.t().contiguous()
    )  # use .t().contiguous to convert edge list to PyG acceptable format

    data0
    return data0


@app.cell
def _(data0):
    # checking if edge_index elements are valid
    data0.validate(raise_on_error=True)
    return


@app.cell
def _(data0, torch):
    print(data0.keys())
    print(data0["x"])

    for key, item in data0:
        print(f"{key} found in data0")

    print("edge_attr" in data0)

    print(data0.num_nodes)

    print(data0.num_edges)

    print(data0.num_node_features)

    print(data0.has_isolated_nodes())

    print(data0.has_self_loops())

    print(data0.is_directed())

    device = torch.device("cuda")
    data_cuda = data0.to(device)

    print(data_cuda)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Common Benchmark Datasets
    """)
    return


@app.cell
def _():
    # Idk wtf is this
    from torch_geometric.datasets import TUDataset

    dataset = TUDataset(root="/tmp/ENZYMES", name="ENZYMES")

    print(dataset)
    print(len(dataset))
    print(dataset.num_classes)
    print(dataset.num_node_features)


@app.cell
def _(dataset):
    data = dataset[0]
    print(data)
    print(data.is_undirected())


@app.cell
def _(dataset):
    dataset.shuffle()
    train_dataset = dataset[:540]
    test_dataset = dataset[540:]

    print(train_dataset)
    print(test_dataset)


@app.cell
def _():
    # Cora dataset
    from torch_geometric.datasets import Planetoid

    cora_set = Planetoid(root="/tmp/Cora", name="Cora")
    cora_set

    return cora_set


@app.cell
def _(cora_set):
    print(len(cora_set))
    print(cora_set.num_classes)
    print(cora_set.num_node_features)


@app.cell
def _(cora_set):
    _data = cora_set[0]

    print(_data)
    print(_data.is_undirected())
    print(_data.train_mask.sum().item())
    print(_data.val_mask.sum().item())
    print(_data.test_mask.sum().item())


@app.cell
def _(): ...


if __name__ == "__main__":
    app.run()
