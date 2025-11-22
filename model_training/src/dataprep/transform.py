import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from imblearn.over_sampling import SMOTE
from sklearn.preprocessing import MinMaxScaler


def minmax_scale(
    table: pa.Table,
    feature_attrs: list[str],
    fitted_scaler: MinMaxScaler | None = None,
) -> tuple[pa.Table, MinMaxScaler]:
    """Apply standard scaling to features in PyArrow Table"""

    scaler = fitted_scaler or MinMaxScaler()

    feature_arrays = [table[col].to_numpy() for col in feature_attrs]
    X = np.column_stack(feature_arrays)

    X_scaled = scaler.transform(X) if fitted_scaler else scaler.fit_transform(X)

    schema = pa.schema(
        [
            (field.name, pa.float64())
            if field.name in feature_attrs
            else (field.name, field.type)
            for field in table.schema
        ]
    )

    arrays = []
    for field in schema:
        col_name = field.name
        if col_name in feature_attrs:
            # Scaled feature - preserve original type
            idx = feature_attrs.index(col_name)
            scaled_array = pa.array(X_scaled[:, idx], type=field.type)
            arrays.append(scaled_array)
        else:
            arrays.append(table[col_name])

    result_table = pa.Table.from_arrays(arrays, schema=schema)

    return result_table, scaler


def groupwise_smote(
    table: pa.Table,
    feature_attrs: list[str],
    group_attrs: list[str],
    min_samples: int,
    index="index",
    label: str = "multiclass_label",
    normal_class: int = 0,
    k_neighbors: int = 6,
    random_state: int = 42,
    strategy: str = "max",
) -> pa.Table:
    """
    Apply SMOTE within each group defined by group_attrs.

    Note: This function will create new synthetic samples, which means
    the sequential index will be lost. After SMOTE, a new sequential
    index should be created if needed.
    """

    # print(table)

    valid_groups = (
        table.filter(pc.field(label) != pc.scalar(0))
        .group_by(group_attrs)
        .aggregate([("index", "count")])
        .filter(pc.field("index_count") > pc.scalar(min_samples))
    )

    print(f"Processing {len(valid_groups)} triplet groups...")

    resampled_batches = []
    skipped = 0

    # Process each group
    for i in range(len(valid_groups)):
        conditions = []
        group_values = {}

        for col in group_attrs:
            val = valid_groups[col][i].as_py()
            group_values[col] = val
            conditions.append(pc.field(col) == pc.scalar(val))

        filter_expr = conditions[0]
        for cond in conditions[1:]:
            filter_expr = filter_expr & cond

        # Get normal samples
        normal_arrow = table.filter(
            filter_expr & (pc.field(label) == pc.scalar(normal_class))
        ).select(feature_attrs + group_attrs + [label])

        attack_arrow = table.filter(
            filter_expr & (pc.field(label) != pc.scalar(normal_class))
        ).select(feature_attrs + group_attrs + [label])

        if len(attack_arrow) < min_samples:
            resampled_batches.append(pa.concat_tables([normal_arrow, attack_arrow]))
            skipped += 1
            continue

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

    # Recreate sequential index after SMOTE (since we added synthetic samples)
    n_rows = result_arrow.num_rows
    new_index = pa.array(range(n_rows), type=pa.int64())

    if index in result_arrow.column_names:
        # Replace existing index
        col_names = result_arrow.column_names
        arrays = []
        names = []

        for name in col_names:
            if name == index:
                arrays.append(new_index)
                names.append(name)
            else:
                arrays.append(result_arrow.column(name))
                names.append(name)
        result_arrow = pa.table(dict(zip(names, arrays)))
    else:
        arrays = [new_index] + [
            result_arrow.column(i) for i in range(result_arrow.num_columns)
        ]
        names = [index] + result_arrow.column_names
        result_arrow = pa.table(dict(zip(names, arrays)))

    return result_arrow
