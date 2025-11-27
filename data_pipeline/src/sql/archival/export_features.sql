-- Export old features to Parquet
COPY (SELECT * FROM og_features WHERE stime < :cutoff_ts) TO :output_path (FORMAT PARQUET)
