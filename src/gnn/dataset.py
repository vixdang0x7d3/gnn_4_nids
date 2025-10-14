import os
import os.path as osp

import gdown

import torch
from .graph_builder import GraphBuilder
from torch_geometric.data import Dataset, extract_zip


# remember to change this every time you modify the upstream data pipeline
GDRIVE_URL = "https://drive.google.com/file/d/1dTKTmNuhUm4ys5H9GXo_0i22joS73HWd/view?usp=drive_link"

SELECTED_COLS = [
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


class NB15Dataset(Dataset):
    def __init__(
        self,
        root,
        n_neighbors=2,
        binary=True,
        augmentation=True,
        split=0,
        transform=None,
        pre_transform=None,
        pre_filter=None,
    ):
        self.n_neighbors = n_neighbors
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
                    f"nb15_train{'_binary' if self.binary else ''}"
                    f"_{self.n_neighbors}{'_aug' if self.augmentation else ''}.pt"
                )
            case 1:  # val set
                file_path = (
                    f"nb15_val{'_binary' if self.binary else ''}_{self.n_neighbors}.pt"
                )
            case 2:  # test set
                file_path = (
                    f"nb15_test{'_binary' if self.binary else ''}_{self.n_neighbors}.pt"
                )

        return [file_path]

    @property
    def raw_file_names(self):
        return ["nb15_train.parquet", "nb15_val.parquet", "nb15_test.parquet"]

    def download(self):
        archive_path = osp.join(self.raw_dir, "features.zip")
        gdown.download(url=GDRIVE_URL, output=archive_path, fuzzy=True)
        extract_zip(archive_path, self.raw_dir)
        os.remove(archive_path)

    def process(self):
        gb = GraphBuilder(
            raw_path=self.raw_paths[self.split],
            selected_cols=SELECTED_COLS,
            n_neighbors=self.n_neighbors,
            binary=self.binary,
            augmentation=self.augmentation,
        )

        ptg = gb.build_graph()

        match self.split:
            case 0:  # train
                file_path = (
                    f"nb15_train{'_binary' if self.binary else ''}"
                    f"_{self.n_neighbors}{'_aug' if self.augmentation else ''}.pt"
                )
            case 1:  # val
                file_path = (
                    f"nb15_val{'_binary' if self.binary else ''}_{self.n_neighbors}.pt"
                )
            case 2:
                file_path = (
                    f"nb15_test{'_binary' if self.binary else ''}_{self.n_neighbors}.pt"
                )

        torch.save(ptg, osp.join(self.processed_dir, file_path))

    def len(self):
        return 1

    def get(self, idx: int):
        match self.split:
            case 0:
                file_path = (
                    f"nb15_train{'_binary' if self.binary else ''}"
                    f"_{self.n_neighbors}{'_aug' if self.augmentation else ''}.pt"
                )

            case 1:
                file_path = (
                    f"nb15_val{'_binary' if self.binary else ''}_{self.n_neighbors}.pt"
                )

            case 2:
                file_path = (
                    f"nb15_test{'_binary' if self.binary else ''}_{self.n_neighbors}.pt"
                )

        # weights_only = False required to load PyG's Data class
        data = torch.load(osp.join(self.processed_dir, file_path), weights_only=False)
        return data
