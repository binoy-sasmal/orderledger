# orderledger

An incremental analytics warehouse that keeps business metrics correct when source records
arrive late, change, repeat or need reprocessing. Built on the public Olist e-commerce dataset,
with Python, DuckDB and dbt.

> **Portfolio project.** One developer, historical data, no users, no production deployment.
> Nothing here is production experience, and no performance or scale claim is made beyond what
> `docs/evidence-report.md` records from actual runs.

## The problem

An analyst needs trustworthy daily order metrics (orders, GMV, on-time delivery) from a source
that keeps changing: delivery times get corrected, prices get corrected, orders get deleted,
events arrive twice or out of order or weeks late. The warehouse has to publish the same numbers
whether it processes the history in one pass, weekly, or fortnightly, survive a crash halfway
through a run, and **refuse to publish** a number it cannot reconcile.

## What is real and what is synthetic

Olist is a set of **static** tables. It has no change feed, no arrival times, no returns, no
deletions and no cancellation timestamp. This project therefore builds a deterministic **replay
layer** that turns the static rows into a batched event history:

* event content and lifecycle timestamps are **real Olist fields**;
* arrival time and batch membership are **always simulated**;
* duplicates, late arrivals, corrections, deletions and out-of-order arrivals are **synthetic**,
  requested by a scenario file and labelled in every event (`content_origin`, `scenario_tag`);
* cancellation is only an observed status snapshot; no cancellation time is invented.

Details: [docs/contracts.md](docs/contracts.md).

## How it works

```
Olist CSVs -> simulate -> events/*.parquet + manifest -> load (validated, idempotent)
  -> raw.events -> dbt facts -> dbt aggregates -> tests + reconciliation
  -> publish (all-or-nothing) -> published.*
```

* **Incremental by key, from full history.** An order with any new event is rebuilt from all its
  events, so the result cannot depend on how events were batched.
* **Watermark and lookback.** A published watermark marks what is processed. Aggregates restate
  a lookback window anchored to the *simulated* checkpoint. A late correction that moves an order
  off an old date is caught by reconciliation, which fails the run; `backfill` repairs exactly
  those dates ([docs/late-data.md](docs/late-data.md)).
* **Write-audit-publish.** Nothing reaches `published` unless dbt tests and source-to-fact-to-
  metric reconciliation pass ([docs/architecture.md](docs/architecture.md)).
* **Operational record.** Every run, including failures, is logged in `ctl.run_log`; freshness is
  reported separately for simulated source time and real execution time.

**Result on the full Olist data** (99k orders, 643k events, 56 and 28 batches): incremental runs
equal a single-pass replay with zero differing rows; reconciliation refused to publish and a
targeted backfill repaired it whenever a late correction fell outside the lookback. See the
[evidence report](docs/evidence-report.md), [gap mapping](docs/gap-mapping.md) and
[demo transcript](docs/demo-transcript.txt). Not built: Airflow and any cloud-platform run.

Documents: [metric definitions](docs/metrics.md), [data model](docs/data-model.md),
[design decisions](docs/decisions/README.md), [guarantees per platform](docs/guarantees.md),
[acceptance tests](docs/acceptance.md).

## Setup

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12.

```
uv sync
uv run pytest                      # synthetic fixture only; takes several minutes
```

### Run on the synthetic fixture

```
uv run python scripts/demo.py      # normal run, crash+retry, late data, backfill, reconciliation
```

### Run on the real Olist data

The dataset is CC BY-NC-SA 4.0 and is not in this repo. Follow
[docs/data-acquisition.md](docs/data-acquisition.md), then:

```
uv run orderledger check-data data/olist
uv run orderledger profile data/olist --out docs/source-profile.md
uv run orderledger simulate data/olist --scenario scenarios/ci.yml --ws warehouse/main
uv run orderledger run --ws warehouse/main --through 1     # then 2, 3, ... or omit --through
uv run orderledger status --ws warehouse/main
uv run orderledger reconcile --ws warehouse/main
uv run orderledger backfill --ws warehouse/main --auto     # after a stale-date failure
uv run orderledger freshness --ws warehouse/main --expected-through 2018-10-01
```

## Layout

| Path | Purpose |
|---|---|
| `src/orderledger/` | simulator, loader, pipeline, reconciliation, CLI, reference implementation |
| `dbt/` | staging, facts, dimensions, aggregates, metric macros, tests |
| `tests/` | acceptance tests on an independently authored synthetic fixture |
| `scenarios/` | scenario files describing the synthetic perturbations |
| `docs/` | design, metrics, decisions, acceptance, evidence |

## Limits

* Historical data replayed on a simulated clock; no scheduler in production, no live users.
* DuckDB allows one writer; the all-or-nothing publish relies on it and does not transfer
  unchanged to other platforms ([docs/guarantees.md](docs/guarantees.md)).
* The Python reference implementation shares an author with the warehouse; it is a cross-check,
  not independent verification.
* Olist volumes are small (under a million events); this demonstrates correctness, not scale.

## Data attribution and licence

Code: MIT. Data: Olist Brazilian E-Commerce Public Dataset, CC BY-NC-SA 4.0 (see `NOTICE.md`).
