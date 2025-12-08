FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy and install the data_pipeline wheel
COPY packages/data_pipeline-*.whl /tmp/
COPY packages/graph_building-*.whl /tmp/


RUN pip install --no-cache-dir /tmp/graph_building-*.whl && \
    rm /tmp/graph_building-*.whl

RUN pip install --no-cache-dir /tmp/data_pipeline-*.whl && \
    rm /tmp/data_pipeline-*.whl

# Create data directory
RUN mkdir -p /data/archive

# Create sql directory 
RUN mkdir -p /sql

# Run the streaming ETL pipeline
CMD ["python", "-m", "pipeline.etl", \
     "--db-path", "/data/zeek_features.db", \
     "--bootstrap-servers", "broker:29092", \
     "--topic", "zeek.logs"]
