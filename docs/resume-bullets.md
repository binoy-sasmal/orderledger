# Resume bullets (verified work only)

Each bullet states only what the evidence report supports. This is a **portfolio project**:
describe it as such in an interview ("a personal project on public data"), never as production
experience. Not covered, so not claimable: Airflow, any cloud platform or Databricks run,
Spark, streaming, production users, performance gains, cost savings.

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

Optional technologies line: Python, SQL, dbt, DuckDB, Parquet, GitHub Actions, pytest.

Interview-ready facts and honest caveats:
* The source has no change history; arrival times and all corrections, deletions and duplicates
  are synthetic and labelled as such.
* At this data size an incremental run is not faster than a full rebuild (27 s vs a 30 s median
  per batch); the project demonstrates correctness, not speed.
