-- Export old features to Parquet
COPY (SELECT * FROM features WHERE stime < :cutoff_ts) TO :output_path (FORMAT PARQUET)
