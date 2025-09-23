import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import marimo as mo

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
    mo.md(r"""### Feature distribution between classes""")
    return


@app.cell
def _():
    # numeric features
    features_to_compare = [
        "min_max_sttl",
        "min_max_ct_state_ttl",
        "min_max_dload",
        "min_max_smeansz",
    ]

    _ax = plt.figure(figsize=(18, 12))

    for i, feature in enumerate(features_to_compare):
        label_feature_df = conn.sql(f"""
        select label, {feature} 
        from data
        """).df()

        plt.subplot(2, 2, i + 1)
        sns.boxplot(
            x="label",
            hue="label",
            y=feature,
            data=label_feature_df,
            palette=["#3498db", "#e74c3c"],
        )
        plt.title(f"{feature} distribution", fontsize=14)

        plt.xlabel("Label", fontsize=12)
        plt.ylabel("Value", fontsize=12)
        plt.xticks(ticks=[0, 1], labels=["Normal", "Attack"])

    plt.tight_layout()

    _ax
    return


@app.cell
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
            columns(* exclude('label', 'multiclass_label')),
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
    X = data.select("* exclude('label', 'multiclass_label')").fetchdf()
    y = data.select("label").fetchdf()

    X = X.select_dtypes(include=np.number)

    print(X.shape)
    print(y.shape)
    return X, y


@app.cell
def _():
    mo.md(r"""### Feature selection""")
    return


@app.cell
def _(X, y):
    # Chi2 method
    chi_selector = SelectKBest(chi2, k=20)
    chi_selector.fit(X, y)
    chi_features = X.columns[chi_selector.get_support()].tolist()

    chi_features
    return


@app.cell
def _(X, y):
    # IG method
    subset_size = 150_000
    if len(X) > subset_size:
        X_subset = X.sample(n=subset_size, random_state=42)
        y_subset = y.loc[X_subset.index]
    else:
        X_subset, y_subset = X, y

    mi_selector = SelectKBest(mutual_info_classif, k=20)
    mi_selector.fit(X_subset, y_subset)

    mi_features = X.columns[mi_selector.get_support()].tolist()

    mi_features


@app.cell
def _(X, y):
    estimator = LogisticRegression(solver="liblinear")
    rfe_selector = RFE(estimator=estimator, n_features_to_select=20, step=0.1)
    rfe_selector.fit(X, y)

    rfe_features = X.columns[rfe_selector.get_support()].tolist()

    rfe_features


if __name__ == "__main__":
    app.run()
