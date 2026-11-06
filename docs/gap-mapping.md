# Mapping to the job-market gaps

The job research behind this project was a **directional** sample (n=24 main postings, a single
day of LinkedIn search, about two thirds requiring German above B1). It is not a census. This
page states what the finished work does and does not evidence, per gap.

| Gap named in the research | What this project evidences | What it does **not** |
|---|---|---|
| Analytical data modelling (dimensional models, history) | Documented fact grains and keys, Type 1 dimensions, a lifecycle interval table with a stated coverage period, separate aggregates per time basis ([data-model.md](data-model.md), [metrics.md](metrics.md)) | SCD Type 2 or Data Vault: the data does not justify them (ADR-003; 0.13% of people at more than one location). Not claimed |
| dbt | A dbt project (dbt-core 1.11, dbt-duckdb): staging, incremental models with `delete+insert`, macros, pre-hooks, generic and singular tests, tags | dbt Cloud, other adapters, dbt snapshots, semantic layer, production deploys |
| Advanced SQL | Window functions, filtered aggregates, `arg_max`, set operations (`EXCEPT ALL`), JSON extraction, SQL-based reconciliation | Warehouse-specific tuning (partitioning, clustering) |
| Incremental processing, idempotent reruns, backfills | Watermark, lookback, recompute-by-key, targeted backfill, crash-and-retry, equivalence of incremental and full replay on 99k orders (0 differing rows) | Streaming, true CDC |
| Data quality and monitoring | dbt tests, source-inventory and fact/metric reconciliation gating publication, run log, separate source and execution freshness | A monitoring or alerting system |
| CI/CD | GitHub Actions: lint and the full test suite on every push (passed) | Deployment pipelines |
| Orchestration (Airflow, Dagster) | **Partly closed.** Airflow 3.4.0 (local, Docker): scheduled replay DAG with interval-to-batch mapping, retries that distinguish retryable from deterministic failures, a parameterised backfill DAG, a single-writer pool plus file lock. Verified on the synthetic fixture ([orchestration-evidence.md](orchestration-evidence.md)) | Production or managed orchestration (MWAA, Astronomer, Kubernetes), Celery or Kubernetes executors, sensors and deferrable operators, alerting and SLAs, Dagster. Not run on the full Olist data |
| Cloud warehouse / lakehouse platform (Databricks, Snowflake, BigQuery, Fabric) | **Not closed.** Local DuckDB only; guarantees for other platforms are documented as unverified | Any platform run |
| Spark / PySpark | **Not closed** | |
| Streaming (Kafka, Flink) | **Not closed** | |
| Years of professional experience; German above B1 | Cannot be closed by a project | |

## Entry-level versus stretch, and language

Of the research sample, the roles this project most directly supports are analytics-engineering
and data-engineering roles that name dbt, SQL and data modelling and that state English as the
working language. Roles that require a named cloud platform or Airflow are only partly supported.
Postings that merely omit a language requirement are not treated as confirmed English-working.
