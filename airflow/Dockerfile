# Airflow 3.4.0 (Python 3.12) with the orderledger CLI in its own virtualenv.
# Airflow only coordinates: the warehouse code and its dependencies (dbt, DuckDB) live in
# /opt/orderledger-venv and are invoked as a subprocess, so they never mix with Airflow's own
# dependency set. Build context is the repository root (see docker-compose.yaml).
FROM apache/airflow:3.4.0-python3.12@sha256:818f94affc5bd7ce0be0cc04f2ace09aa8382e65190a73a08881229ff32700cb

USER root
RUN mkdir -p /opt/orderledger /opt/orderledger-venv /data/ws \
 && chown -R airflow:0 /opt/orderledger /opt/orderledger-venv /data/ws
USER airflow

# Inputs of the lock file first, so the dependency layer is cached across code edits.
COPY --chown=airflow:0 pyproject.toml uv.lock /opt/orderledger/
COPY --chown=airflow:0 src /opt/orderledger/src
COPY --chown=airflow:0 dbt /opt/orderledger/dbt
COPY --chown=airflow:0 scenarios /opt/orderledger/scenarios
COPY --chown=airflow:0 tests/fixtures/synth /opt/orderledger/fixtures/synth

# uv is pinned to the version used to write uv.lock; --frozen installs exactly the locked set.
RUN pip install --no-cache-dir uv==0.13.1 \
 && cd /opt/orderledger \
 && UV_PROJECT_ENVIRONMENT=/opt/orderledger-venv UV_PYTHON=/usr/python/bin/python3.12 \
    uv sync --frozen --no-dev

ENV ORDERLEDGER_CLI=/opt/orderledger-venv/bin/orderledger \
    ORDERLEDGER_WS=/data/ws \
    ORDERLEDGER_CSV_DIR=/opt/orderledger/fixtures/synth \
    ORDERLEDGER_SCENARIO=/opt/orderledger/scenarios/airflow-demo.yml
