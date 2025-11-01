import torch

import numpy as np

import duckdb


def load_data(
    conn: duckdb.DuckDBPyConnection,
    data_path: str,
    node_attrs: list[str],
    edge_attrs: list[str],
    label: str,
    index_name: str = "index",
):
    rel = conn.read_parquet(data_path)
    rel = rel.row_number(window_spec=f"over () as {index_name}", projected_columns="*")
    return rel.select(", ".join([index_name, label, *node_attrs, *edge_attrs]))


def extract_nodes(
    rel: duckdb.DuckDBPyRelation,
    node_attrs: list[str],
    label: str,
    index: str = "index",
):
    # Filter relevant columns and convert to arrow table
    arrow_table = rel.order("index").select(", ".join([label, *node_attrs])).arrow()

    # Convert to numpy
    feature_arrays = [arrow_table.column(attr).to_numpy() for attr in node_attrs]

    # Create torch feature matrix and label vector
    x = torch.from_numpy(np.column_stack(feature_arrays)).float()
    y = torch.from_numpy(arrow_table.column(label).to_numpy()).long()

    return x, y


def build_edges(
    rel: duckdb.DuckDBPyRelation,
    edge_attrs: list[str],
    index: str = "index",
    n_neighbors: int = 2,
):
    arrow_table = rel.order("index").select(", ".join([index, *edge_attrs])).arrow()

    if arrow_table.num_rows == 0:
        return torch.empty((2, 0), dtype=torch.long)

    # Convert to numpy for faster access
    node_ids = arrow_table["index"].to_numpy()
    proto = arrow_table["proto"].to_numpy()
    service = arrow_table["service"].to_numpy()
    state = arrow_table["state"].to_numpy()

    # Find group boundaries using vectorized operations
    # A new group starts where any of the grouping columns changes
    group_change = np.concatenate(
        [
            [True],  # First row always starts a group
            (proto[1:] != proto[:-1])
            | (service[1:] != service[:-1])
            | (state[1:] != state[:-1]),
        ]
    )

    # Get indices where groups start
    group_starts = np.where(group_change)[0]
    group_ends = np.concatenate([group_starts[1:], [len(node_ids)]])

    # Pre-allocate lists for edges (more efficient than appending)
    sources = []
    targets = []

    # Process each group
    for start, end in zip(group_starts, group_ends):
        group_size = end - start

        if group_size <= 1:
            continue

        group_node_ids = node_ids[start:end]

        # Create edges within this group
        for i in range(group_size):
            # Connect to next k neighbors
            for j in range(1, min(n_neighbors + 1, group_size - i)):
                sources.append(group_node_ids[i])
                targets.append(group_node_ids[i + j])

    if len(sources) == 0:
        return torch.empty((2, 0), dtype=torch.long)

    # Create edge_index in PyG format [2, num_edges]
    edge_index = torch.tensor([sources, targets], dtype=torch.long)

    return edge_index
