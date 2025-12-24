import numpy as np
import pyarrow as pa
import torch
from torch_geometric.data import Data

from .graph_base import GraphBase


class GraphGCN(GraphBase):
    def __init__(
        self,
        table: pa.Table,
        node_attrs: list[str],
        edge_attrs: list[str],
        label: str | None = None,
        n_neighbors: int = 2,
    ):
        super().__init__(
            table=table,
            node_attrs=node_attrs,
            edge_attrs=edge_attrs,
            label=label,
            n_neighbors=n_neighbors,
        )

    def build(self, include_labels=True) -> Data:
        x, y = self._extract_nodes()
        edge_index = self._build_edges()

        data = Data(x=x, edge_index=edge_index)

        if include_labels:
            data.y = torch.LongTensor(y)

        return data

    def _extract_nodes(self):
        """
        Extract node features and labels from PyArrow Table.

        Assumes table already has sequential index from load_data,
        therefore no sorting needed.
        """

        # Convert to numpy
        feature_arrays = [
            self.table.column(attr).to_numpy() for attr in self.node_attrs
        ]

        # Create torch feature matrix and label vector
        x = torch.from_numpy(np.column_stack(feature_arrays)).float()

        if self.label is not None:
            y = torch.from_numpy(self.table.column(self.label).to_numpy()).long()
        else:
            y = torch.empty((1, self.table.num_rows), dtype=torch.long)

        return x, y

    def _build_edges(self):
        """
        Build edges within groups defined by edge_attrs

        Assumes table already has sequential index from _load_data,
        Group flows by edge_attrs and connects temporal neighbors within groups
        """

        if self.table.num_rows == 0:
            return torch.empty((2, 0), dtype=torch.long)

        # Convert to numpy for faster access
        node_ids = self.table.column(self.index).to_numpy()

        # Dynamically get edge attribute columns
        edge_columns = []
        for attr in self.edge_attrs:
            if attr in self.table.column_names:
                edge_columns.append(self.table.column(attr).to_numpy())

        if not edge_columns:
            # No edge attributes, treat all nodes as one group
            group_change = np.array([True] + [False] * (len(node_ids) - 1))
        else:
            # Find group boundaries using vectorized operations
            # A new group starts where any of the grouping columns changes
            change_conditions = [(edge_columns[0][1:] != edge_columns[0][:-1])]

            for col_data in edge_columns[1:]:
                change_conditions.append((col_data[1:] != col_data[:-1]))

            # Combine all conditions with OR
            combined_change = change_conditions[0]
            for cond in change_conditions[1:]:
                combined_change = combined_change | cond

            group_change = np.concatenate([[True], combined_change])

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
                for j in range(1, min(self.n_neighbors + 1, group_size - i)):
                    sources.append(group_node_ids[i])
                    targets.append(group_node_ids[i + j])

        if len(sources) == 0:
            return torch.empty((2, 0), dtype=torch.long)

        # Create edge_index in PyG format [2, num_edges]
        edge_index = torch.tensor([sources, targets], dtype=torch.long)

        return edge_index
