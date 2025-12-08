FROM ghcr.io/mlflow/mlflow:v3.6.0

# Install PostgreSQL client library
RUN pip install --no-cache-dir psycopg2-binary boto3

# Use psycopg2-binary for easier installation without needing PostgreSQL dev libraries
# If you need the full psycopg2, you'd need to install build dependencies first:
# RUN apt-get update && apt-get install -y postgresql-client libpq-dev gcc && \
#     pip install --no-cache-dir psycopg2 && \
#     apt-get clean && rm -rf /var/lib/apt/lists/*
