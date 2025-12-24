FROM apache/airflow:3.1.3-python3.13

USER root

# Install system dependencies (minimal, only what's needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install torch FIRST with CPU-only variant (much smaller and faster)
# This is the slowest dependency, so we cache it separately
RUN pip install --no-cache-dir \
    torch==2.8.0+cpu \
    --index-url https://download.pytorch.org/whl/cpu

# Install torch-geometric and its dependencies
# These have pre-built wheels, so they're fast
RUN pip install --no-cache-dir \
    torch-geometric>=2.7.0 \
    imblearn>=0.0

# Install remaining dependencies from requirements.txt
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Copy and install local packages (only what DAGs need)
# These wheels are built by build_packages.sh before docker build
COPY --chown=airflow:root packages/graph_building-*.whl /tmp/packages/
COPY --chown=airflow:root packages/data_pipeline-*.whl /tmp/packages/

# Install in dependency order: graph_building -> data_pipeline
RUN pip install --no-cache-dir \
    /tmp/packages/graph_building-*.whl \
    /tmp/packages/data_pipeline-*.whl && \
    rm -rf /tmp/packages

# Verify installations
RUN python -c "import torch; print(f'torch {torch.__version__} (CPU-only)')" && \
    python -c "import graph_building; print('graph_building installed')" && \
    python -c "import data_pipeline; import sqlutils; print('data_pipeline installed')"
