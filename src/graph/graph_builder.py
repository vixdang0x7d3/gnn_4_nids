import re
import os.path as osp

from tqdm import tqdm

import torch
from torch_geometric.data import Data

import numpy as np

import duckdb
from duckdb import ColumnExpression
from duckdb import ConstantExpression


import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.compute as pc


class InputGraph:
    def __init__(self, dataset_path, node_attrs, edge_attrs):
        self.dataset_path = dataset_path
        self.node_attrs = node_attrs
        self.edge_attrs = edge_attrs

    @property
    def node_attrs_str(self):
        return ", ".join(self.node_attrs)

    @property
    def edge_attrs_str(self):
        return ", ".join(self.edge_attrs)


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


def extract_nodes(rel, node_attrs, label, batch_size=50_000):
    columns = [
        *(ColumnExpression(attr) for attr in node_attrs),
        ColumnExpression(label),
    ]

    arrow_reader = rel.select(*columns).order("index").arrow(rows_per_batch=batch_size)

    x_batches = []
    y_batches = []

    for batch in tqdm(arrow_reader, desc="Loading nodes..."):
        feature_array = [
            batch.column(attr).to_numpy(zero_copy_only=False) for attr in node_attrs
        ]

        x_batch = torch.from_numpy(np.column_stack(feature_array)).float()
        y_batch = torch.from_numpy(batch.column("label").to_numpy()).long()

        x_batches.append(x_batch)
        y_batches.append(y_batch)

    return torch.cat(x_batches, dim=0), torch.cat(y_batches, dim=0)


def build_edges(rel, edge_attrs, n_neighbors=2, batch_size=50_000):
    columns = [
        *(ColumnExpression(attr) for attr in edge_attrs),
    ]

    sources, targets = [], []
    prev_batch_tail = None

    reader = (
        rel.select(
            ColumnExpression("index"),
            *columns,
        ).order(
            *columns,
            ColumnExpression("index"),
        )
    ).arrow(rows_per_batch=batch_size)

    for batch in reader:
        # Combine with tail from previous batch to handle group boundaries
        if prev_batch_tail is not None:
            batch = pa.concat_tables([prev_batch_tail, pa.Table.from_batches([batch])])

        node_ids = batch["index"].to_numpy()
        proto = batch["proto"].to_numpy()
        service = batch["service"].to_numpy()
        state = batch["state"].to_numpy()

        group_change = np.concatenate(
            [
                [True],
                (proto[1:] != proto[:-1])
                | (service[1:] != service[:-1])
                | (state[1:] != state[:-1]),
            ]
        )

        group_starts = np.where(group_change)[0]
        group_ends = np.concatenate([group_starts[1:], [len(node_ids)]])

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

        prev_batch_tail = batch.slice(max(0, len(batch) - n_neighbors), len(batch))

    return (
        torch.tensor([sources, targets], dtype=torch.long)
        if sources
        else torch.empty((2, 0), dtype=torch.long)
    )


if __name__ == "__main__":
    data_path = "~/works/gnn_4_nids/data/NB15_sample/raw/nb15_train.parquet"
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
    count = extract_nodes(raw_rel)

    print(count)
