import re
import os.path as osp

from typing import Callable
from torch_geometric.data import Data


from .transforms import std_scale, groupwise_smote


class InputGraph:
    def __init__(
        self,
        data_path,
        node_attrs,
        edge_attrs,
        edge_builder: Callable,
        transforms: list[Callable] | None = None,
        label: str | None = None,
        index: str = "index",
    ):
        self.data_path = data_path
        self.node_attrs = node_attrs
        self.edge_attrs = edge_attrs
        self.edge_builder = edge_builder
        self.transforms = transforms or []
        self.index = index
        self.label = label
        self.fitted = {}

    def build() -> Data:
        return Data()

    def for_inference(self, new_path: str | None = None) -> "InputGraph":
        """Create inference builder with fitted transforms"""
        inference_transforms = []
        if "scaler" in self.fitted:
            inference_transforms.append(std_scale(self.fitted["scaler"]))

        return InputGraph(
            data_path=new_path or self.data_path,
            node_attrs=self.node_attrs,
            edge_attrs=self.edge_attrs,
            edge_builder=self.edge_builder,  # <-- Reuse same strategy
            transforms=inference_transforms,
            index=self.index,
        )
