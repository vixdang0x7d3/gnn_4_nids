import os

from torch_geometric.utils.convert import from_networkx
import networkx as nx

import duckdb

import torch
from torch_geometric.data import Data
from torch_geometric.data import Dataset, extract_zip

from imblearn.over_sampling import SMOTE

import gdown


URL = "https://drive.google.com/file/d/1ey0Q1INfaq7EajVRAqbPUX12AeNukrcY/view?usp=drive_link"
SELECTED_COLS = [
    "int_onehot",
    "min_max_dur",
    "min_max_sbytes",
    "min_max_dbytes",
    "min_max_sttl",
    "min_max_dttl",
    "min_max_sload",
    "min_max_dload",
    "min_max_spkts",
    "min_max_dpkts",
    "min_max_smeansz",
    "min_max_dmeansz",
    "min_max_sintpkt",
    "min_max_dintpkt",
    "min_max_tcprtt",
    "min_max_synack",
    "min_max_ackdat",
    "min_max_ct_state_ttl",
    "min_max_ct_src_dport_ltm",
    "min_max_ct_dst_sport_ltm",
]


class NB15Dataset(Dataset):
    def __init__(
        self,
        root,
        num_neighbors,
        binary,
        augmentation,
        split,
        transform=None,
        pre_transform=None,
        pre_filter=None,
    ):
        self.num_neighbors = num_neighbors
        self.binary = binary
        self.augmentation = augmentation
        self.split = split
        super(NB15Dataset, self).__init__(root, transform, pre_transform, pre_filter)

    @property
    def processed_file_names(self):
        file_path = ""
        match self.split:
            case 0:  # train set
                file_path = (
                    f"nb15_train_{'binary' if self.binary else ''}_{self.num_neighbors}_"
                    f"{'aug' if self.augmentation else ''}.pt"
                )
            case 1:  # val set
                file_path = f"nb15_val_{'binary' if self.binary else ''}_{self.num_neighbors}.pt"
            case 2:  # test set
                file_path = f"nb15_test_{'binary' if self.binary else ''}_{self.num_neighbors}.pt"

        return [file_path]

    @property
    def raw_file_names(self):
        return ["nb15_train.parquet", "nb15_val.parquet", "nb15_test.parquet"]

    def download(self):
        archive_path = os.path.join(self.raw_dir, "features.zip")
        gdown.download(url=URL, output=archive_path, fuzzy=True)
        extract_zip(archive_path, self.raw_dir)
        os.remove(archive_path)

    def process(self):
        select_list = ",".join([f'"{col}"' for col in SELECTED_COLS])
        X = duckdb.sql(f"""SELECT {select_list} FROM '{self.raw_paths[self.split]}'""")

        if self.binary:
            y = duckdb.sql(f"""SELECT label FROM """)
