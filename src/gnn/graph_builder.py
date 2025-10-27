import re
import os.path as osp

import torch

import duckdb

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.compute as pc

class InputGraph():
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

    def build():

    def _load_dataset(self):
        data = 



def load_data(conn, nodes_attrs, edge_attrs):
    data = 

    conn

    return data
