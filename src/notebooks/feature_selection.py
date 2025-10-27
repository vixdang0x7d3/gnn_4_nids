import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import marimo as mo

    import math
    from concurrent.futures import ThreadPoolExecutor, as_completed

    import multiprocessing as mp

    import duckdb
    import pandas as pd
    import numpy as np
    import seaborn as sns

    from sklearn.feature_selection import SelectKBest, chi2, RFE, mutual_info_classif
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.linear_model import LogisticRegression

    import matplotlib.pyplot as plt

    conn = duckdb.connect()


@app.cell
def _():
    conn.sql("drop table if exists data")
    data = conn.sql(r"""
        FROM 'dataset/NB15/raw/nb15_train.parquet'
        UNION ALL
        FROM 'dataset/NB15/raw/nb15_test.parquet'
        UNION ALL
        FROM 'dataset/NB15/raw/nb15_val.parquet'
    """)
    return (data,)


@app.cell
def _():
    conn.sql(r"""
        SUMMARIZE data
    """).to_df()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""### Exploratory Data Analysis""")
    return


@app.cell
def _():
    conn.sql("drop table if exists label_df")
    label_df = conn.sql("select label from data").df()

    plt.figure(figsize=(12, 8))
    ax = sns.countplot(
        x="label",
        hue="label",
        data=label_df,
        palette=["#3498db", "#e74c3c"],
    )

    plt.title("Label distribution (Normal (0) vs. Attack (1))")
    plt.xlabel("Label", fontsize=12)
    plt.ylabel("Samples", fontsize=12)
    plt.xticks(ticks=[0, 1], labels=["Normal", "Attack"])

    for p in ax.patches:
        ax.annotate(
            f"{p.get_height():.2f}",
            (p.get_x() + p.get_width() / 2.0, p.get_height()),
            ha="center",
            va="center",
            xytext=(0, 10),
            textcoords="offset points",
            fontsize=12,
        )

    ax
    return


@app.cell
def _():
    # Your features list (cleaned up duplicates)
    features_to_compare = [
        "min_max_sttl",
        "min_max_ct_state_ttl",
        "min_max_dload",
        "min_max_smeansz",
        "min_max_dur",
        "min_max_sbytes",
        "min_max_dbytes",
        "min_max_dttl",
        "min_max_sloss",
        "min_max_dloss",
        "min_max_sload",
        "min_max_spkts",
        "min_max_dpkts",
        "min_max_swin",
        "min_max_dwin",
        "min_max_stcpb",
        "min_max_dtcpb",
        "min_max_dmeansz",
        "min_max_trans_depth",
        "min_max_res_bdy_len",
        "min_max_sjit",
        "min_max_djit",
        "min_max_sintpkt",
        "min_max_dintpkt",
        "min_max_tcprtt",
        "min_max_synack",
        "min_max_ackdat",
        "min_max_ct_flw_http_mthd",
        "min_max_is_ftp_login",
        "min_max_ct_ftp_cmd",
        "min_max_ct_srv_src",
        "min_max_ct_srv_dst",
        "min_max_ct_dst_ltm",
        "min_max_ct_src_ltm",
        "min_max_ct_src_dport_ltm",
        "min_max_ct_dst_sport_ltm",
        "min_max_ct_dst_src_ltm",
    ]

    # Calculate grid dimensions
    n_features = len(features_to_compare)
    n_cols = 6  # More reasonable number of columns
    n_rows = math.ceil(n_features / n_cols)

    # Set up the plot with better proportions
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(24, 4 * n_rows))
    axes = axes.flatten() if n_features > 1 else [axes]

    # Color palette
    colors = ["#2ecc71", "#e74c3c"]  # Green for normal, red for attack

    for i, feature in enumerate(features_to_compare):
        # Get your data (replace this with your actual data query)
        label_feature_df = conn.sql(f"""
            SELECT label, {feature} 
            FROM data
            WHERE {feature} IS NOT NULL
        """).df()

        # Create boxplot
        sns.boxplot(
            data=label_feature_df,
            x="label",
            hue="label",
            y=feature,
            ax=axes[i],
            palette=colors,
            showfliers=False,  # Remove outliers for cleaner look
        )

        # Clean up the subplot
        axes[i].set_title(
            feature,
            fontsize=10,
            pad=10,
        )
        axes[i].set_xlabel("")
        axes[i].set_ylabel("")
        axes[i].set_xticks([0, 1])
        axes[i].set_xticklabels(["Normal", "Attack"])
        axes[i].grid(True, alpha=0.3)

    # Remove empty subplots
    for i in range(n_features, len(axes)):
        fig.delaxes(axes[i])

    # Overall title and layout
    fig.suptitle("Feature Distributions: Normal vs Attack Traffic", fontsize=16, y=0.98)
    plt.tight_layout()
    plt.subplots_adjust(top=0.94)

    fig
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    ### Correlation analysis
    Check for feature-label correlation and codependent between feature attributes.
    """
    )
    return


@app.cell
def _():
    top_20_corr = conn.sql(r"""
        with label_corr as (
          select corr(
            columns(* exclude(
                    'multiclass_label',
                    'label', 
                    'state',
                    'proto',
                    'service'
                )
            ),
            label
          )
          from data
        ),
        top_20_corr as (
          select col_name, abs(corr_val) as corr
          from label_corr
          unpivot (corr_val for col_name in (*))
          order by corr desc
          limit 20
        )
        from top_20_corr
    """)

    top_20_corr = top_20_corr.df().set_index("col_name")
    return (top_20_corr,)


@app.cell
def _(top_20_corr):
    _ax = plt.figure(figsize=(10, 8))
    sns.barplot(
        x=top_20_corr["corr"].values,
        y=top_20_corr.index,
        hue=top_20_corr.index,
        palette="viridis",
    )

    _ax
    return


@app.cell
def _(top_20_corr):
    top_n_names = top_20_corr.index.tolist()
    corr_matrix = (
        conn.sql(f"""
        select 
            {", ".join(top_n_names)}
        from
            data
    """)
        .df()
        .corr()
    )

    mask = np.triu(np.ones_like(corr_matrix), k=1)
    corr_matrix = corr_matrix.mask(mask.astype(bool))

    plt.figure(figsize=(12, 10))
    sns.heatmap(
        corr_matrix,
        annot=True,
        cmap="coolwarm",
        fmt=".2f",
        linewidth=0.5,
    )
    return


@app.cell
def _(data):
    X = data.select(r"""
        * exclude (
            'label', 
            'multiclass_label',
            'state',
            'proto',
            'service'
        )
    """).fetchdf()

    y = data.select("label").fetchdf()
    X = X.select_dtypes(include=np.number)

    print(X.shape)
    print(y.shape)
    return X, y


@app.cell(hide_code=True)
def _():
    mo.md(r"""### Collinearity filtering""")
    return


@app.function
def process_single_group(group_name, features, X, y, n_per_groups=3):
    result = {
        "group_name": group_name,
        "selected_features": [],
        "dropped_features": [],
        "scores": [],
        "n_selected": 0,
    }

    if len(features) > 1:
        features_data = X[features]
        mi_scores = mutual_info_classif(features_data, y, random_state=42)
        n_select = min(n_per_groups, len(features))
        top_indices = np.argsort(mi_scores)[-n_select:][::-1]
        selected_features = [features[i] for i in top_indices]
        dropped_features = [f for f in features if f not in selected_features]

    result.update(
        {
            "selected_features": selected_features,
            "dropped_features": dropped_features,
            "scores": mi_scores[top_indices],
            "n_selected": n_select,
        }
    )

    return result


@app.function
def select_representatives(X, y, groups, max_workers=None):
    def _process_group(group_info):
        group_name, features, n_per_group = group_info
        return process_single_group(group_name, features, X, y, n_per_group)

    selected_features = []
    dropped_features = []

    if max_workers is None:
        max_workers = min(32, (mp.cpu_count() or 1) + 4)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_group = {
            executor.submit(_process_group, group): group for group in groups
        }

        print(f"Submitted {len(future_to_group)} tasks...")

        results = []
        completed = 0
        for future in as_completed(future_to_group):
            completed += 1
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                group = future_to_group[future]
                print(f"Group {group[0]} generated an exception: {e}")

    # Sort results by original order to maintain consistency
    group_names = [g[0] for g in groups]
    results.sort(key=lambda x: group_names.index(x["group_name"]))

    # Print detailed results and aggregate
    print(f"\n{'=' * 60}")
    print("CORRELATION FILTERING RESULTS")
    print(f"{'=' * 60}")

    for result in results:
        selected_features.extend(result["selected_features"])
        dropped_features.extend(result["dropped_features"])

        print(f"Correlation features: {result['group_name']}")
        print(f"Selected top {result['n_selected']}: {result['selected_features']}")
        if len(result["scores"]) > 0:
            print(f"Scores: {[f'{score:.4f}' for score in result['scores']]}")
        print(f"Dropped: {result['dropped_features']}\n")

    print(f"Selected {len(selected_features)} representative features")
    print(f"Dropped {len(dropped_features)} features")

    return selected_features, dropped_features


@app.cell
def _(X, y):
    others = [
        "min_max_sbytes",
        "min_max_dbytes",
        "min_max_smeansz",
        "min_max_dmeansz",
        "min_max_res_bdy_len",
        "min_max_sload",
        "min_max_dload",
        "min_max_spkts",
        "min_max_dpkts",
        "min_max_sloss",
        "min_max_dloss",
        "min_max_trans_depth",
        "min_max_is_ftp_login",
        "min_max_ct_flw_http_mthd",
        "min_max_ct_ftp_cmd",
    ]

    ### colinear groups

    ttl_features = [
        "min_max_ct_state_ttl",
        "min_max_sttl",
        "min_max_dttl",
    ]

    count_related_features = [
        "min_max_ct_srv_src",
        "min_max_ct_src_ltm",
        "min_max_ct_dst_ltm",
        "min_max_ct_src_dport_ltm",
        "min_max_ct_dst_sport_ltm",
        "min_max_ct_dst_src_ltm",
    ]

    time_related_features = [
        "min_max_dur",
        "min_max_sjit",
        "min_max_djit",
        "min_max_sintpkt",
        "min_max_dintpkt",
    ]

    tcp_window_features = [
        "min_max_swin",
        "min_max_dwin",
    ]

    tcp_features = [
        "min_max_stcpb",
        "min_max_dtcpb",
        "min_max_tcprtt",
        "min_max_synack",
        "min_max_ackdat",
    ]

    with mo.persistent_cache("filter_result"):
        representatives, dropped = select_representatives(
            X,
            y,
            groups=[
                ("TTL features", ttl_features, 1),
                ("Time features", time_related_features, 3),
                ("Count features", count_related_features, 1),
                ("TCP window features", tcp_window_features, 1),
                ("TCP features", tcp_features, 1),
            ],
        )

    print(representatives)
    print(dropped)
    return others, representatives


@app.cell
def _(X, representatives):
    _corr = X[representatives].corr()
    _mask = np.triu(np.ones_like(_corr), k=1)
    _corr = _corr.mask(_mask.astype(bool))

    plt.figure(figsize=(12, 10))
    sns.heatmap(
        _corr,
        annot=True,
        cmap="coolwarm",
        fmt=".2f",
        linewidth=0.5,
    )
    return


@app.cell
def _(X, others, representatives):
    feature_set = representatives + others
    feature_set

    X_features = X[feature_set]
    return (X_features,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""### Feature selection""")
    return


@app.cell
def _(X_features, y):
    # IG method

    sub_sampling = False
    subset_size = 150_000

    if sub_sampling and len(X_features) > subset_size:
        X_subset = X_features.sample(n=subset_size, random_state=42)
        y_subset = y.loc[X_subset.index]
    else:
        X_subset, y_subset = X_features, y

    mi_selector = SelectKBest(mutual_info_classif, k=15)
    mi_selector.fit(X_subset, y_subset)

    mi_features = X_features.columns[mi_selector.get_support()].tolist()

    mi_features
    return (mi_features,)


@app.cell
def _(X_features, y):
    estimator = RandomForestClassifier(
        n_estimators=100,
        random_state=42,
        n_jobs=-1  # Use all cores for faster training
    )
    rfe_selector = RFE(estimator=estimator, n_features_to_select=15, step=0.1)
    rfe_selector.fit(X_features, y)
    rfe_features = X_features.columns[rfe_selector.get_support()].tolist()
    rfe_features
    return (rfe_features,)


@app.cell
def _(mi_features, rfe_features):
    [f for f in mi_features if f in rfe_features]
    return


if __name__ == "__main__":
    app.run()
