COPY (SELECT * FROM ${source} WHERE ts < :cutoff_ts) TO :output_path (FORMAT PARQUET);
