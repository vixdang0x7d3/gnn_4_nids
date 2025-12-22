FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy and install the data_pipeline wheel
COPY packages/pipelines-*.whl /tmp/
COPY packages/graph_building-*.whl /tmp/

# Copy SQL statements
COPY ../../../pipelines/sql  /sql

RUN pip install --no-cache-dir /tmp/graph_building-*.whl && \
    rm /tmp/graph_building-*.whl

RUN pip install --no-cache-dir /tmp/pipelines-*.whl && \
    rm /tmp/pipelines-*.whl

# Create data directory
RUN mkdir -p /data/archive

# Run the streaming ETL pipeline
CMD ["python", "-m", "data_pipeline.etl", \
    "--db-path", "/data/zeek_features.db", \
    "--bootstrap-servers", "broker:29092", \
    "--topic", "zeek.logs"]
