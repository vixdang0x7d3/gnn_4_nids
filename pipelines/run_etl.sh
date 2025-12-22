#!/usr/bin/env bash

set -xe

uv run -m src.pipeline.etl \
    --db-path ./data/output/nids_data.duckdb \
    --archive-path ./data/output/archive \
    --sql-dir ./src/sql \
    --bootstrap-server localhost:9092 \
    --topic zeek.logs \
    --group-id zeek-consumer \
    --aggregation-interval 30 \
    --archive-interval 120 \

