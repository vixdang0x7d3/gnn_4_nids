FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy SQL statements
COPY ./pipelines/sql  /sql

# Copy and install data_pipeline wheel (no graph_building needed!)
COPY ./infrastructure/data_pipeline/packages/data_pipeline-*.whl /tmp/

RUN pip install --no-cache-dir /tmp/data_pipeline-*.whl && \
    rm /tmp/*.whl

# Create data directory
RUN mkdir -p /data/archive

# Run the streaming ETL pipeline
CMD ["python", "-m", "data_pipeline.etl", \
    "--db-path", "/data/zeek_features.db", \
    "--bootstrap-servers", "broker:29092", \
    "--topic", "zeek.logs"]
