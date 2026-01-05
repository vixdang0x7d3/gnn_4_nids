-- export new features to parquet
-- identifiers: ${source}
-- params: :cutoff_ts, :output_path

COPY (
    SELECT * FROM ${source} WHERE ts > :cutoff_ts ORDER BY ts
) TO :output_path (FORMAT PARQUET);
