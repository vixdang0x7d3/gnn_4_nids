#!/usr/bin/env bash

set -xe

uv run python -m data_pipeline.etl \
    --db-path ./data/output/nids_data.duckdb \
    --archive-path ./data/output/archive \
    --sql-dir ./sql \
    --bootstrap-servers localhost:9092 \
    --topic zeek.logs \
    --group-id zeek-consumer \
    --aggregation-interval 5 \
    --archive-age-hours 0.05 \
    --retention-hours 0.5 \
    --feature-set og \
    "$@"
