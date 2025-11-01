import torch

from typing import Callable
from torch_geometric.data import Data

from .transformer import std_scaler


class InputGraph:
    def __init__(
        self,
        data_path,
        node_attrs,
        edge_attrs,
        loader: Callable,
        edges_builder: Callable,
        nodes_extractor: Callable,
        transforms: list[Callable] | None = None,
        label: str | None = None,
        index: str = "index",
    ):
        self.data_path = data_path
        self.node_attrs = node_attrs
        self.edge_attrs = edge_attrs
        self.load_data = loader
        self.build_edges = edges_builder
        self.extract_nodes = nodes_extractor
        self.transforms = []
        self.fitted = {}
        self.index = index
        self.label = label

    def build(self, include_labels: bool = True) -> Data:
        table = self.load_data(
            self.data_path,
            self.node_attrs,
            self.edge_attrs,
            self.label if include_labels else None,
            self.index,
        )

        # Apply transformation
        for t in self.transforms:
            data = t(table)
            if hasattr(t, "fitted_obj"):
                self.fitted[f"fitted_{t.name}"] = t.fitted_obj

        x, y = self.extract_nodes(table, self.node_attrs)
        edge_index = self.build_edges(table)

        data = Data(x=x, edge_index=edge_index)
        if include_labels:
            data.y = torch.LongTensor(y)

        return data

    def for_inference(self, new_path: str | None = None) -> "InputGraph":
        """Create inference builder with fitted transforms"""
        inference_transforms = []

        for t in self.fitted:
            match t:
                case "fitted_std_scale":
                    inference_transforms.append(std_scaler(self.fitted[t]))
                case _:
                    pass

        return InputGraph(
            data_path=new_path or self.data_path,
            node_attrs=self.node_attrs,
            edge_attrs=self.edge_attrs,
            loader=self.load_data,
            edges_builder=self.build_edges,
            nodes_extractor=self.extract_nodes,
            transforms=inference_transforms,
            index=self.index,
        )
