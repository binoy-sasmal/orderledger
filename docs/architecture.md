# Architecture

```mermaid
flowchart LR
  A[Olist CSVs<br/>pinned, not in git] --> B[simulate<br/>scenario file, seeded]
  B --> C[(events/*.parquet<br/>manifest.json<br/>inventory.json)]
  A -. separate code path .-> I[inventory.json<br/>expected source counts]
  C --> D[load<br/>contract, contradictions,<br/>one transaction per batch]
  D --> E[(raw.events<br/>append-only)]
  E --> F[dbt tag:facts<br/>staging, touched_orders,<br/>fct_orders, fct_order_items, dims]
  F --> G[dbt tag:marts<br/>aggregates, intervals,<br/>recon views]
  G --> H[dbt tests<br/>+ cross-model tests]
  H --> J{reconcile<br/>source - raw - facts - marts}
  I --> J
  J -- all pass --> K[publish: copy work to published<br/>+ advance watermark<br/>in ONE transaction]
  J -- any fail --> L[run marked failed<br/>published and watermark untouched]
```

## Run lifecycle (`orderledger run`)

1. **Load** every pending batch up to the requested one. A file already loaded is skipped.
2. **dbt facts**: `touched_orders` = orders with an event whose `arrival_seq` is above the
   published **watermark** (plus a forced backfill window). Each touched order is rebuilt from its
   *full* event history, so the result never depends on batch boundaries.
3. **dbt marts**: aggregates restate the lookback window around the simulated checkpoint, the new
   dates of touched orders, and any forced window.
4. **Tests and reconciliation**: dbt generic and singular tests, then the reconciliation checks.
5. **Publish** (write-audit-publish): only if everything passed, `work.*` is copied to
   `published.*` and the watermark advances, in a single DuckDB transaction.

Every run, including failed ones, writes a row to `ctl.run_log` (step that failed, error,
counts, watermarks, duration). Reconciliation results are stored in `ctl.reconciliation`.

## Why a retry is safe

The watermark only moves at publish. A run that fails anywhere leaves it where it was, so the
retry recomputes exactly the same touched orders from the same raw history. Recomputation is by
key from full history (no deltas are ever added), so it is idempotent.

## Time

`checkpoint_date` is the simulated "now" of a run: the end of the last loaded batch. The
lookback is anchored to it, never to the wall clock, so results are reproducible. Real time is
used only for `published_at` and execution freshness.
See [late-data.md](late-data.md) for what the lookback does and does not guard against.

## Orchestration (Airflow, local)

Airflow 3.4.0 coordinates the same CLI; it adds scheduling, retries and visibility, not logic.
Details, scope and limits: [orchestration.md](orchestration.md). Recorded runs (synthetic fixture):
[orchestration-evidence.md](orchestration-evidence.md).

```mermaid
flowchart LR
  S[Scheduler<br/>one interval per replay batch] --> E[ensure_events]
  E --> P[plan<br/>interval to batch k+1]
  P --> R[run_batch<br/>orderledger run --through k+1]
  R --> F[check_freshness]
  R -. exit 75 retryable .-> R
  R -. exit 1 deterministic .-> X[failed, NOT retried<br/>published and watermark preserved]
  X --> B[orderledger_backfill<br/>manual, validated parameters]
  B --> R
  POOL[(pool: 1 slot)] -.-> E & R & F & B
  LOCK[(workspace file lock)] -.-> R & B
```
