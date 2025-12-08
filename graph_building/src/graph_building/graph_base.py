from abc import ABC, abstractmethod

import pyarrow as pa
from torch_geometric.data import Data


class GraphBase(ABC):
    """
    Base builder class responsible for managing graph building
    configuration. Expose a single abstract method build(), which
    returns a torch_geometric.data.Data object.

    Derived classes should assume self.table already has a sequential index.
    """

    def __init__(
        self,
        table: pa.Table,
        node_attrs: list[str],
        edge_attrs: list[str],
        label: str | None,
        n_neighbors: int,
    ):
        self.node_attrs = node_attrs
        self.edge_attrs = edge_attrs
        self.n_neighbors = n_neighbors

        self.label = label or ""
        self.index = "index"

        self._load_table(table)

    @abstractmethod
    def build(self, include_labels: bool = True) -> Data: ...

    def _load_table(self, table: pa.Table):
        """
        Select relevant columns and create a sequential index

        New sequential index starts from 0, which ensures
        consistent node ids for graph construction regardless
        of input data ordering
        """

        # Sort by edge attributes and time
        sort_keys = [
            (col, order)
            for col, order in [
                ("proto", "ascending"),
                ("state", "ascending"),
                ("service", "ascending"),
                ("stime", "ascending"),
            ]
            if col in table.column_names
        ]
        table.sort_by(sort_keys)

        # Create a sequential index on sorted data
        n_rows = table.num_rows
        index_array = pa.array(range(n_rows), type=pa.int64())

        columns = [self.label, *self.node_attrs, *self.edge_attrs]
        if self.label is None:
            columns = columns[:1]

        arrays = [index_array]
        names = [self.index]

        for col in columns:
            if col in table.column_names:
                arrays.append(table.column(col))
                names.append(col)

        self.table = pa.table(dict(zip(names, arrays)))
