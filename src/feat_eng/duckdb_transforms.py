from typing import Callable

import numpy as np
import pyarrow as pa

import duckdb
from duckdb import (
    ColumnExpression,
    FunctionExpression,
    StarExpression,
)

from imblearn.over_sampling import SMOTE
from sklearn.preprocessing import StandardScaler


def select_features(
    rel: duckdb.DuckDBPyRelation,
    conn: duckdb.DuckDBPyConnection,
    feature_attrs: list[str],
) -> duckdb.DuckDBPyRelation:
    raise NotImplemented


def std_scale(
    rel: duckdb.DuckDBPyRelation,
    conn: duckdb.DuckDBPyConnection,
    feature_attrs: list[str],
    fitted_scaler: StandardScaler | None = None,
) -> duckdb.DuckDBPyRelation:
    raise NotImplemented


def groupwise_smote(
    rel: duckdb.DuckDBPyRelation,
    conn: duckdb.DuckDBPyConnection,
    feature_attrs,
    group_attrs,
    label,
    normal_class,
    strategy,
    k_neighbors,
    random_state,
    min_samples,
):
    # Define column expresions
    group_exprs = [ColumnExpression(col) for col in group_attrs]
    feature_exprs = [ColumnExpression(col) for col in feature_attrs]
    label_expr = ColumnExpression(label)

    # Get valid groups
    groups_rel = (
        rel.filter(label_expr != normal_class)
        .aggregate(
            group_expr=", ".join(group_attrs),
            aggr_expr="COUNT(*)",
        )
        .filter(ColumnExpression("count") >= min_samples)
        .select(*group_exprs)
    )
    groups_arrow = groups_rel.arrow()

    print(f"Processing {len(groups_arrow)} triplet groups...")

    resampled_batches = []
    skipped = 0

    # Process each group
    for i in range(len(groups_arrow)):
        conditions = []
        group_values = {}

        for attr in group_attrs:
            val = groups_arrow[attr][i].as_py()
            group_values[attr] = val
            col_expr = ColumnExpression(attr)
            conditions.append(col_expr == val)

        filter_expr = conditions[0]
        for cond in conditions[1:]:
            filter_expr = filter_expr & cond

        # Get normal samples
        normal_rel = rel.filter(filter_expr & (label_expr == normal_class)).select(
            *feature_exprs, *group_exprs, label_expr
        )

        normal_arrow = normal_rel.arrow()

        attack_rel = rel.filter(filter_expr & (label_expr != normal_class)).select(
            *feature_exprs, *group_exprs, label_expr
        )

        attack_arrow = attack_rel.arrow()

        if len(attack_arrow) < min_samples:
            resampled_batches.append(pa.concat_tables([normal_arrow, attack_arrow]))
            skipped += 1

        # Convert to numpy for SMOTE
        X_attack = np.column_stack(
            [attack_arrow[col].to_numpy() for col in feature_attrs]
        )
        y_attack = attack_arrow[label].to_numpy()

        # Get attack class counts
        attack_unique, attack_counts = np.unique(y_attack, return_counts=True)
        attack_counter = dict(zip(attack_unique, attack_counts))

        if len(attack_counter) < 2:
            # Only one attack class
            resampled_batches.append(pa.concat_tables([normal_arrow, attack_arrow]))
            continue

        # Determine target count
        counts = list(attack_counter.values())
        if strategy == "max":
            target_count = int(np.max(counts))
        elif strategy == "mean":
            target_count = int(np.mean(counts))
        elif strategy == "median":
            target_count = int(np.median(counts))
        elif isinstance(strategy, int):
            target_count = strategy
        else:
            raise ValueError(f"Unknown strategy: {strategy}")

        # Build sampling strategy
        sampling_strategy = {
            cls: max(count, target_count) for cls, count in attack_counter.items()
        }

        # Adjust k_neighbors if needed
        min_class_count = int(np.min(counts))
        effective_k = min(k_neighbors, min_class_count - 1)

        if effective_k < 1:
            resampled_batches.append(pa.concat_tables([normal_arrow, attack_arrow]))
            skipped += 1
            continue

        try:
            # Apply SMOTE
            smote = SMOTE(
                sampling_strategy=sampling_strategy,
                k_neighbors=effective_k,
                random_state=random_state,
            )
            X_resampled, y_resampled = smote.fit_resample(X_attack, y_attack)

            # Build PyArrow table for resampled attacks
            resampled_dict = {}

            # Add features
            for idx, col in enumerate(feature_attrs):
                resampled_dict[col] = pa.array(X_resampled[:, idx])

            # Add group columns (constant values)
            for col in group_attrs:
                resampled_dict[col] = pa.array([group_values[col]] * len(y_resampled))

            # Add labels
            resampled_dict[label] = pa.array(y_resampled)

            resampled_attack_table = pa.table(resampled_dict)

            # Combine normal and resampled attacks
            combined = pa.concat_tables([normal_arrow, resampled_attack_table])
            resampled_batches.append(combined)

        except Exception:
            # Keep original on error
            resampled_batches.append(pa.concat_tables([normal_arrow, attack_arrow]))
            skipped += 1

    print(f"Skipped {skipped} groups (too few samples or errors)")

    # Concatenate all groups
    result_arrow = pa.concat_tables(resampled_batches)

    return duckdb.from_arrow(result_arrow)
