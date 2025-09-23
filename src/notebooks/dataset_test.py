import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    from pathlib import Path
    import sys
    return (sys,)


@app.cell
def _(sys):
    sys.path
    return


@app.cell
def _():
    from gnn.dataset import NB15Dataset
    return (NB15Dataset,)


@app.cell
def _(NB15Dataset):
    nb15 = NB15Dataset(
        root="/tmp/NB15",
        num_neighbors=2,
        binary=True,
        augmentation=True,
        split=0,
    )
    return (nb15,)


@app.cell
def _(nb15):
    nb15.raw_paths[0]
    return


if __name__ == "__main__":
    app.run()
