import gc
import re
import os.path as osp

import torch
import duckdb
import numpy as np
import pandas as pd

from tqdm import tqdm

from imblearn.over_sampling import SMOTE
from torch_geometric.data import Data


class GraphBuilder:
    def __init__(
        self,
        raw_path,
        selected_cols,
        n_neighbors,
        binary,
        augmentation,
    ):
        self.raw_path = raw_path
        self.selected_cols = selected_cols
        self.n_neighbors = n_neighbors
        self.binary = binary
        self.augmentation = augmentation

    @property
    def select_cols_str(self):
        return ", ".join(self.selected_cols)

    @property
    def require_smote(self):
        fname = osp.basename(self.raw_path)
        found = re.search(r"nb15_(.*)\.parquet", fname)
        if found:
            is_train = found.group(1) == "train"
            return is_train and self.augmentation and not self.binary

        raise ValueError("Invalid raw path")

    def build_graph(self):
        try:
            self._setup_duckdb()
            self._load_raw_data()

            # Apply SMOTE if needed
            if self.require_smote:
                augmented_table = self._apply_smote()
            else:
                augmented_table = "raw_data"

            # Create node embedding matrix
            # breakpoint()
            x, y = self._extract_nodes(augmented_table)

            # Create COO compatible edge index
            if self.n_neighbors > 0:
                edge_index = self._create_edges(augmented_table)
            else:
                edge_index = torch.empty((2, 0), dtype=torch.long)

            data = Data(x=x, y=y, edge_index=edge_index)
            data.num_nodes = x.size(0)
            data.num_features = x.size(1)
            data.num_node_types = len(torch.unique(y))

            return data
        except Exception as e:
            print(f"Graph building error : {e}")

        finally:
            self._cleanup()

    def _setup_duckdb(self):
        self.conn = duckdb.connect()
        self.conn.execute("SET temp_directory = '/tmp'")
        # self.conn.execute("SET threads=4")

    def _cleanup(self):
        if self.conn:
            self.conn.close()
            self.conn = None
        gc.collect()

    def _load_raw_data(self):
        self.conn.execute(f"""
            CREATE TEMP TABLE raw_data AS
            SELECT ROW_NUMBER() OVER () - 1 as node_id, 
                   proto,
                   state,
                   service,
                   {self.select_cols_str}, 
                   {"label" if self.binary else "multiclass_label"} AS label,
            FROM read_parquet('{self.raw_path}')
        """)

    def _apply_smote(self):
        # breakpoint()
        valid_groups = self.conn.sql("""
            SELECT proto, state, service, COUNT(*) as group_count, label
            FROM raw_data
            GROUP BY proto, state, service, label
            ORDER BY proto, state, service, label
        """).fetchall()

        groups = {}
        for proto, state, service, count, label in valid_groups:
            group_key = (proto, state, service)
            if group_key not in groups:
                groups[group_key] = {}

            groups[group_key][label] = count

        valid_group_targets = []
        for group_key, label_counts in groups.items():
            if len(label_counts) >= 2 and sum(label_counts.values()) >= 2:
                valid_group_targets.append((group_key, label_counts))

        # breakpoint()
        temp_tables = []
        for i, (group_key, label_counts) in enumerate(valid_group_targets):
            proto, state, service = group_key
            table_name = f"smote_group_{i}"

            success = self._apply_smote_to_group(
                proto, state, service, label_counts, table_name
            )

            if success:
                temp_tables.append(table_name)

        # breakpoint()
        if not temp_tables:
            print("No SMOTE groups created, using original data")
            return "raw_data"

        union_query = " UNION ALL ".join([f"SELECT * FROM {t}" for t in temp_tables])
        self.conn.execute(f"""
        CREATE TEMP TABLE smote_processed AS
        SELECT ROW_NUMBER() OVER () - 1 as node_id,
               proto, 
               state, service,
               {self.select_cols_str},
               label
        FROM ({union_query})
        ORDER BY proto, state, service
        """)

        # Clean up individual group tables
        for table in temp_tables:
            self.conn.execute(f"DROP TABLE {table}")

        # Report final distribution
        final_counts = dict(
            self.conn.sql(
                "SELECT label, COUNT(*) FROM smote_processed GROUP BY label ORDER BY label"
            ).fetchall()
        )

        original_counts = dict(
            self.conn.sql(
                "SELECT label, COUNT(*) FROM raw_data GROUP BY label ORDER BY label"
            ).fetchall()
        )

        print(f"Original distribution: {original_counts}")
        print(f"Final distribution: {final_counts}")

        return "smote_processed"

    def _apply_smote_to_group(self, proto, state, service, label_counts, table_name):
        try:
            group_data = self.conn.sql(f"""
            SELECT {self.select_cols_str}, label
            FROM raw_data
            WHERE proto='{proto}' AND
                  state='{state}' AND
                  service='{service}'
            """).fetchnumpy()

            if len(group_data) < 2:
                return False

            target_count = max(label_counts.values())

            group_strategy = {}
            for label, current_count in label_counts.items():
                if current_count < target_count:
                    group_strategy[label] = target_count

            if not group_strategy:
                _df = pd.DataFrame(
                    {
                        **{col: group_data[col] for col in self.selected_cols},
                        "proto": proto,
                        "state": state,
                        "service": service,
                        "label": group_data["label"],
                    }
                )
            else:
                X = np.column_stack([group_data[col] for col in self.selected_cols])
                y = group_data["label"]

                sm = SMOTE(
                    sampling_strategy=group_strategy,
                    random_state=42,
                    k_neighbors=min(5, len(X) - 1),
                )

                X_resampled, y_resampled = sm.fit_resample(X, y)

                _df = pd.DataFrame(
                    {
                        **{
                            col: X_resampled[:, i]
                            for i, col in enumerate(self.selected_cols)
                        },
                        "proto": proto,
                        "state": state,
                        "service": service,
                        "label": y_resampled,
                    }
                )

            # Register and create temp table
            self.conn.register(f"temp_{table_name}", _df)
            self.conn.execute(f"""
            CREATE TEMP TABLE {table_name} AS
            SELECT * FROM temp_{table_name}
            """)
            self.conn.unregister(f"temp_{table_name}")

            return True

        except Exception as e:
            print(f"SMOTE failed for group ({proto}, {state}, {service}): {e}")
            return False

    def _extract_nodes(self, data_table, batch_size=50_000):
        total_rows = self.conn.sql(f"""
        SELECT COUNT(*) FROM {data_table}
        """).fetchone()[0]

        x_batches = []
        y_batches = []

        for offset in tqdm(range(0, total_rows, batch_size), desc="Loading nodes"):
            batch_query = f"""
            SELECT {self.select_cols_str}, label
            FROM {data_table}
            ORDER BY node_id
            LIMIT {batch_size} OFFSET {offset}
            """

            batch_data = self.conn.sql(batch_query).fetchnumpy()

            x_batch = torch.tensor(
                np.column_stack([batch_data[col] for col in self.selected_cols]),
                dtype=torch.float,
            )

            y_batch = torch.tensor(batch_data["label"], dtype=torch.long)

            x_batches.append(x_batch)
            y_batches.append(y_batch)

            del batch_data

            if offset % (batch_size * 3) == 0:
                gc.collect()

        x = torch.concat(x_batches, dim=0)
        y = torch.concat(y_batches, dim=0)

        del x_batches, y_batches
        gc.collect()

        return x, y

    def _create_edges(self, data_table):
        edge_query = f"""
        WITH grouped_data AS (
            SELECT node_id, proto, service, state,
                   ROW_NUMBER() OVER (PARTITION BY proto, state, service ORDER BY node_id) AS row_num,
                   COUNT(*) OVER (PARTITION BY proto, state, service) AS group_size
            FROM {data_table}
        )
        SELECT DISTINCT a.node_id AS source, b.node_id AS target
        FROM grouped_data a
        JOIN grouped_data b ON (
            a.proto = b.proto AND
            a.service = b.service AND
            a.state = b.state AND
            b.row_num > a.row_num AND
            b.row_num <= a.row_num + {self.n_neighbors}
        )
        WHERE a.group_size > 1
        ORDER BY a.node_id, b.node_id
        """

        edges = self.conn.sql(edge_query).fetchall()

        if len(edges) == 0:
            return torch.empty((2, 0), dtype=torch.long)

        edge_index = torch.tensor(edges, dtype=torch.long).t().contiguous()

        return edge_index
