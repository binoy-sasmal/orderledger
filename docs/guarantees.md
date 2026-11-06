# Guarantees per implementation

The publish step is implementation specific. Nothing below is assumed to transfer.

## Local: DuckDB (implemented, tested)

* One database file, one writer process at a time (the pipeline runs steps sequentially; dbt runs
  in a subprocess so it releases the file).
* **Publish is atomic across all tables and the watermark.** `published.*` is replaced and
  `ctl.state` advanced in one DuckDB transaction. A crash before commit leaves both unchanged
  (tested with an injected crash before commit: `test_at02_at11_crash_then_retry...[publish]`).
* Readers of `published` see the previous complete state or the new complete state, never a mix,
  because DuckDB gives transactional isolation inside one file.
* Not guaranteed: concurrent writers, and durability beyond a single local file.

## Platform stage (Databricks Free Edition): NOT YET VERIFIED

No claim is made until the publication spike (milestone M7) runs. Open questions to answer from
current Databricks documentation and an actual run, not from DuckDB behaviour:

* Delta tables are atomic **per table**. Whether several tables can be changed in one
  transaction on the target platform is unverified and must not be assumed.
* A portable design that does not need multi-table transactions: build versioned tables in a work
  schema, publish by repointing views to the new version, and write the watermark last. A crash
  between those steps then repeats the same work, because publication is idempotent and the
  watermark has not moved. Readers could briefly see new views with the old watermark; that is a
  documented, weaker guarantee than the local one.
* Free Edition limits that shape the run (per Databricks' documentation, 2026-11-06): serverless
  only, one 2X-Small SQL warehouse, 5 concurrent job tasks, restricted outbound internet,
  fair-use shutdown, non-commercial use. See `docs/platform.md` once the stage runs.

## Portfolio deployment is not production

Nothing here has real users, an SLA, on-call, or production data volumes.
