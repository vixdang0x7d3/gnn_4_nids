#!/usr/bin/env bash

set -xe

uv run python -m data_pipeline.etl \
    --db-path ./data/output/nids_data.duckdb \
    --archive-path ./data/output/archive \
    --sql-dir ./sql \
    --bootstrap-servers localhost:9092 \
    --topic zeek.dpi \
    --group-id zeek-consumer-local \
    --aggregation-interval 5 \
    --archive-age-sec 5 \
    --retention-sec 30 \
    --feature-set nf \
    --batch-size-threshold 2000 \
    "$@"
