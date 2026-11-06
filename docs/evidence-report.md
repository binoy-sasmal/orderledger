# Evidence report

Recorded 2026-11-06 from actual runs. Raw numbers: [evidence-data.json](evidence-data.json),
produced by `scripts/full_run.py`. Figures derived from the Olist dataset carry its
CC BY-NC-SA 4.0 attribution (see `NOTICE.md`).

**What this is.** A portfolio project run on a local Windows machine (Windows 10.0.26200,
Python 3.12, DuckDB 2.0.0, dbt-core 1.11.17, dbt-duckdb 1.11.1). One developer, historical data,
simulated clock. It is not production experience, and no platform other than local DuckDB has
been run.

## Dataset (measured)

Olist Brazilian E-Commerce Public Dataset, version 2, downloaded 2026-11-06. Sizes matched the
sizes Kaggle's metadata reported; SHA-256 values below are what this download contained (Kaggle
publishes no checksum, so they are observations, not upstream truth).

| File | SHA-256 |
|---|---|
| olist_orders_dataset.csv | 8df58ef3d2d7e9944010f7beecd9b75367f5588ec6e3c91cec19ae3345ef9ecf |
| olist_order_items_dataset.csv | 0bc4d068c4fe38cbb01bd90e8746e3c613fe7b4baef75fab7b0e329701c3e279 |
| olist_customers_dataset.csv | 983a422239e1712ded753b3bf9ecf47dc73f144d306029dcfa99e70a226883d2 |
| olist_products_dataset.csv | 3e6569628a17fbc75fd206ee357b59e20364b9afa90f5b6cd5b4d624c58aa9cc |
| product_category_name_translation.csv | a81f0d1f27b27e7293f761bc79e3ce8f348ee39c4b3ed3e49bde38f478586278 |

99,441 orders, 112,650 items, 32,951 products, purchases 2016-09-04 to 2018-10-17.
Full profile: [source-profile.md](source-profile.md).

## Event history built from it

| | Count |
|---|---|
| Events in the history | 643,721 (distinct `event_id`: 638,658) |
| Source-origin events (the `clean` run, no synthetic events) | 637,898 |
| Synthetic events added | 5,823 = 5,063 duplicate deliveries + 400 price corrections + 200 delivery-time corrections + 100 deletions + 60 out-of-order price corrections |
| Source events whose *arrival* was delayed (synthetic) | 450 (300 inside the lookback, 150 by 25 days) |
| Batches | 56 (14-day), 28 (28-day) |

Arrival time, batch membership and all of the above perturbations are synthetic
([contracts.md](contracts.md)). The source contains no change history.

## Correctness results (full data)

| Check | Result |
|---|---|
| Incremental 14-day run (56 batches, with backfills) vs single-pass full replay of the same events | **0 differing rows in all 8 published tables** |
| Incremental 14-day run vs incremental 28-day run (different batch boundaries) | **0 differing rows in all 8 published tables** |
| Warehouse headline metrics vs the plain-Python reference (orders, GMV, delivered, on-time by date), with synthetic events | all four match |
| Warehouse headline metrics vs a direct SQL computation over the CSVs, without synthetic events | all four match |
| Same checks on the synthetic CI fixture | 35 tests pass locally; CI passed on GitHub (Linux, 10 min 42 s) |

Not an independent verification: the reference implementation and the warehouse share an author
and the written rules. The CSV-direct comparison is the more independent check.

## Late data and reconciliation (full data)

Reconciliation refused to publish, named the stale dates, and a targeted backfill repaired them:

| Run | Batches | Runs refused by reconciliation (then backfilled) |
|---|---|---|
| 14-day | 56 | 36 |
| 28-day | 28 | 20 |

Every refusal was the case documented in [late-data.md](late-data.md): one of the 200 synthetic
delivery-time corrections, arriving 25 days after the original and moving an order off an
already-published date, outside the 7-day lookback. This high rate is a property of the
**scenario** (200 such corrections over about two years), not of the real data. First arrivals
that were late (the 150 events delayed 25 days) never needed a backfill.

## Data quality observed in the source

* 1,381 non-deleted orders have inconsistent lifecycle timestamps (for example carrier handover
  before approval) and are excluded from delivery metrics and state intervals. They are flagged,
  not repaired.
* 6 orders are `canceled` yet carry a delivery time; they are counted in
  `status_conflict_canceled`. Cancellation has no timestamp in the source.
* 96,001 people in `dim_customer` (deleted orders removed).

## Published sizes

fct_orders 99,441 rows; fct_order_items 112,650; order_state_intervals 386,947; dim_customer
96,001; dim_product 32,951; agg_daily_purchases 634 days; agg_daily_deliveries 646 days. The
warehouse file after the 14-day run was about 172 MB.

## Runtime (measured, local, single run each)

| | Seconds |
|---|---|
| Simulate 643,721 events | about 5 |
| Single-pass full replay (load all, build, test, reconcile, publish) | 27.1 |
| Incremental run, per batch, median | 29.8 (range 13.7 to 38.4) |
| Incremental 14-day total (56 batches plus 36 backfill runs) | 1,471 |
| Incremental 28-day total (28 batches plus 20 backfill runs) | 776 |

**Honest reading.** At this size an incremental run is *not* faster than rebuilding everything:
a single-pass replay took 27 s. The fastest runs, which touched almost nothing, took about
13.7 s, so there is a fixed floor per run; I did not profile it, but it is probably mostly the
three dbt processes starting. Per-run time also rose over the run (about 14 s early, 36 to 38 s
late); a likely cause is that the raw history is scanned again for each run, also not profiled.
These numbers support the correctness of the incremental design, not any performance benefit.
No cost saving or scale claim is made.

## Platform execution

None. The code runs on local DuckDB only. The Databricks Free Edition stage and the Airflow
wrapper were not built, so neither the cloud-platform gap nor the orchestration gap is closed
by this project. See [gap-mapping.md](gap-mapping.md).

## Limitations

* Historical data on a simulated clock; no live users, no scheduler, no production data volumes.
* All change events beyond lifecycle timestamps are synthetic and were designed by the author.
* DuckDB's one-writer, single-transaction publish does not transfer unchanged to other platforms
  ([guarantees.md](guarantees.md)).
* Late arrivals and corrections follow rules chosen by the author (`source_seq` decides
  versions); a real source may need different rules.
* 7-day lookback and scenario counts are configuration choices, not measured real-world rates.
