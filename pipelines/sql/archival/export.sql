COPY (SELECT * FROM ${source} WHERE ts < :cutoff_ts ORDER BY ts LIMIT 10000) TO :output_path (FORMAT PARQUET);
