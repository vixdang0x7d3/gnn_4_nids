FROM apache/airflow:3.1.3-python3.13

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install base dependencies first
ENV TMPDIR=/tmp
ENV PIP_BUILD=/tmp/pip-build
ENV PIP_CACHE_DIR=/tmp/pip-cache
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy and install local packages
# These wheels are built by build_packages.sh before docker build
COPY --chown=airflow:root packages/*.whl /tmp/packages/

# Install in dependency order: graph_building -> data_pipeline -> model_training
RUN pip install --no-cache-dir /tmp/packages/graph_building-*.whl && \
    pip install --no-cache-dir /tmp/packages/data_pipeline-*.whl && \
    pip install --no-cache-dir /tmp/packages/model_training-*.whl && \
    rm -rf /tmp/packages

# Verify installations
RUN python -c "import graph_building; print('graph_building installed')" && \
    python -c "import pipeline; import sqlutils; print('data_pipeline installed')" && \
    python -c "import gnn; print('model_training installed')"
