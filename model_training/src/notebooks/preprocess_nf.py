import marimo

__generated_with = "0.18.0"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import marimo as mo
    import duckdb
    from pathlib import Path


@app.cell
def _():
    data_dir = Path(__file__).parent.parent.parent / "data" / "nf_nb15_v3_csv"
    feature_desc_csv_path = data_dir / "NetFlow_v3_Features.csv"
    data_csv_path =  data_dir / "NF-UNSW-NB15-v3.csv"
    conn = duckdb.connect()

    print(feature_desc_csv_path)
    print(data_csv_path)
    return


@app.cell
def _():
    data = mo.sql(
        f"""
        conn.sql(\"""
        SELECT * FROM read_csv
    
        \""")
        """
    )
    return


if __name__ == "__main__":
    app.run()
