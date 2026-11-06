# Resume bullets (verified work only)

Each bullet states only what the evidence reports ([evidence-report.md](evidence-report.md),
[orchestration-evidence.md](orchestration-evidence.md)) support. This is a **portfolio project**:
describe it as such in an interview ("a personal project on public data"), never as production
experience. Not covered, so not claimable: Airflow in production or on a managed service, Airflow on the
full Olist data, any cloud platform or Databricks run, Spark, streaming, production users,
performance gains, cost savings.

1. **Built an incremental analytics warehouse (Python, DuckDB, dbt) over 99,000 e-commerce
   orders that rebuilds each changed order from its full event history, so incremental runs over
   56 and 28 batches produced the same published tables as a single-pass replay, with no
   differing rows.**

2. **Gated every publication on dbt tests and source-to-fact-to-metric reconciliation, so failed
   runs left published tables and the watermark untouched; late delivery-date corrections outside
   the lookback were flagged by date and repaired with a targeted backfill.**

3. **Defined fact grains, purchase-date and delivery-date metrics and an event contract with
   separate identity, version order and time, and added an independent source inventory that
   detects events the source never sent.**

4. **Orchestrated the pipeline with Airflow in Docker: scheduled runs mapped to replay batches,
   automatic retries only for retryable failures, a parameterised backfill DAG, and a single-writer
   pool plus file lock. Reconciliation failures were not retried and kept the last validated
   publication; the final tables matched a single-pass replay.** *(Verified on the synthetic
   fixture, local Docker only.)*

Optional technologies line: Python, SQL, dbt, DuckDB, Parquet, Airflow, Docker, GitHub Actions, pytest.

Interview-ready facts and honest caveats:
* The source has no change history; arrival times and all corrections, deletions and duplicates
  are synthetic and labelled as such.
* At this data size an incremental run is not faster than a full rebuild (27 s vs a 30 s median
  per batch); the project demonstrates correctness, not speed.
* Airflow was a local Docker demo on one machine (LocalExecutor), driven by a 3-minute schedule on
  the synthetic fixture. It is not production orchestration and not a cloud deployment.
* Airflow retries only failures the CLI marks retryable; reconciliation failures need a person to
  run the backfill DAG and clear the failed task. That is by design, not a gap.
* Two defects were found by running it (a date-arithmetic crash with Airflow's datetime type, and a
  `max_active_runs=1` deadlock) and fixed; the walkthrough in
  [orchestration-evidence.md](orchestration-evidence.md) covers them.
