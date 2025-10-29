import torch

import numpy as np

import duckdb
from duckdb import ColumnExpression


def load_data(conn, data_path, node_attrs, edge_attrs, label):
    rel = conn.read_parquet(data_path)
    rel = rel.row_number(window_spec="over () as index", projected_columns="*")

    columns = [
        ColumnExpression("index"),
        ColumnExpression(label),
        *(ColumnExpression(attr) for attr in node_attrs),
        *(ColumnExpression(attr) for attr in edge_attrs),
    ]

    return rel.select(*columns)


def extract_nodes(rel, node_attrs, label):
    columns = [
        ColumnExpression(label),
        *(ColumnExpression(attr) for attr in node_attrs),
    ]

    arrow_table = rel.select(*columns).arrow()

    # Convert to numpy
    feature_arrays = [arrow_table.column(attr).to_numpy() for attr in node_attrs]

    # Create torch feature matrix and label vector
    x = torch.from_numpy(np.column_stack(feature_arrays)).float()
    y = torch.from_numpy(arrow_table.column(label).to_numpy()).long()

    return x, y


def build_edges(rel, edge_attrs, n_neighbors=2):
    columns = [
        ColumnExpression("index"),
        *(ColumnExpression(attr) for attr in edge_attrs),
    ]

    arrow_table = rel.select(*columns).arrow()
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


if __name__ == "__main__":
    data_path = "~/works/gnn_4_nids/data/NB15/raw/nb15_train.parquet"
    conn = duckdb.connect()

    node_attrs = [
        "min_max_sttl",
        "min_max_dur",
        "min_max_dintpkt",
        "min_max_sintpkt",
        "min_max_ct_dst_sport_ltm",
        "min_max_tcprtt",
        "min_max_sbytes",
        "min_max_dbytes",
        "min_max_smeansz",
        "min_max_dmeansz",
        "min_max_sload",
        "min_max_dload",
        "min_max_spkts",
        "min_max_dpkts",
    ]

    edge_attrs = ["proto", "state", "service"]
    raw_rel = load_data(conn, data_path, node_attrs, edge_attrs, label="label")
    x, y = extract_nodes(raw_rel, node_attrs, "label")
    edge_index = build_edges(raw_rel, edge_attrs)
