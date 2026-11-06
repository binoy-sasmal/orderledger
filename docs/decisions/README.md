# Design decisions

Short records of the choices that shaped the project. Each states the decision, the alternatives
that were rejected, and what would change it.

## ADR-001: A replayable change log instead of a source database

**Decision.** The source of change is an immutable set of Parquet batches plus a manifest, not a
Postgres instance that is mutated.
**Why.** The hard problems (late, repeated, changed and deleted records, reruns, backfills) all
exist in a change log. A database whose only job is to hand over rows we already have adds a
container and a failure mode without adding evidence. Warehouses commonly receive exactly this
kind of change feed from upstream.
**Rejected.** Postgres as a simulated operational source with CDC. More realistic ceremony, no
extra correctness evidence, and it would put time into the simulator rather than the warehouse.
**Would change if.** A role requirement is specifically CDC tooling.

## ADR-002: Identity, version order and time are separate fields

**Decision.** `event_id` (identity), `entity_id` (what changed), `source_seq` (version order),
`effective_ts` (business time) and `arrival_seq` (simulated arrival) are distinct. `source_seq`
alone decides which version is current; `effective_ts` never does.
**Why.** Ordering by business time breaks when a correction carries an earlier timestamp, and
ordering by arrival breaks under out-of-order delivery. Same-effective-time corrections are then
simply ordered by `source_seq`.
**Contradictions.** The same `event_id` with different content, or two ids claiming one entity
version, quarantine the batch and fail the run. Silent deduplication would hide a source defect.

## ADR-003: Customer dimension is Type 1; no SCD Type 2 unless the data earns it

**Decision (confirmed by the measured profile, `docs/source-profile.md`).**
`dim_customer` is Type 1 on `customer_unique_id`. No SCD2 is built on addresses.
**Measured:** 96,096 people; 2,997 (3.1%) have more than one order; 122 (0.13%) were observed
at more than one city/state and 39 in more than one state. Too few, and the address is order-level.
**Why.** In Olist, `customer_id` is created per order, so an address is *the address observed on
that order*, not verified residence history. Whether any person was observed at more than one
location is a measured fact, not an assumption; it is recorded in the profile. A history table
over a handful of people would demonstrate the technique rather than answer a business question.
**Where interval modelling lives instead.** `order_state_intervals`: order lifecycle states as
half-open intervals from recorded timestamps, with coverage that starts at purchase, no history
invented before the first observation, and contradicting sequences excluded rather than repaired.
This answers a real question (how long orders sit in each stage).
**Would change if.** The profile shows enough repeat customers with changed addresses *and* a
metric that depends on the address at purchase time.

## ADR-004: Two aggregates, one per time basis

**Decision.** `agg_daily_purchases` (by purchase date) and `agg_daily_deliveries` (by delivery
date). See [metrics.md](../metrics.md).
**Why.** A delivery-date metric inside a purchase-date row would mix populations and denominators
and would change meaning when a delivery arrives late. Separate aggregates keep each definition
and each late-update behaviour explicit.

## ADR-005: Status is an observation, cancellation has no time

**Decision.** `order_status_observed` has no `effective_ts` and arrives in the final batch.
Lifecycle timestamps are kept as separate facts. No cancellation timestamp is created.
**Consequence.** `cancellation_share` is defined only once every order of a day has a status
snapshot, so it is NULL at earlier checkpoints. Delivery metrics ignore status and report
`status_conflict_canceled` instead. Equivalence tests that cover cancellation apply only at the
final checkpoint.

## ADR-006: Write-audit-publish, implemented per platform

**Decision.** Build in `work`, audit (tests and reconciliation), then publish. On DuckDB the
publish is one transaction over all tables and the watermark. On other platforms the guarantees
differ and are documented separately ([guarantees.md](../guarantees.md)); nothing is assumed to
transfer.

## ADR-007: Incremental by key from full history; aggregates restated by window

**Decision.** Facts are rebuilt per touched key from the order's full event history (never by
adding deltas). Aggregates restate a lookback window anchored to the simulated checkpoint plus the
new dates of touched orders. The one case this misses (an order moving off an old date) is
detected by a full-recompute comparison and repaired by backfill.
**Why.** Recompute-by-key makes reruns idempotent and independent of batch boundaries, which is
what the equivalence tests rely on. The lookback is a real, bounded-cost design choice with a
real failure mode that is made visible instead of hidden.

## ADR-008: dbt runs in a subprocess; no mypy

dbt-duckdb keeps the database file open for the life of its process, which blocks the pipeline's
own connections, so each dbt phase runs in a child process. `mypy` is not used because current
releases conflict with dbt-core's `pathspec` pin; `ruff` and the tests are the checks.

## ADR-009: Local DuckDB proves modelling, not the platform gap

Local DuckDB work shows modelling, SQL, dbt, incremental logic and correctness. It does not close
the cloud warehouse gap. A bounded platform stage (M7) is planned and is claimed only if it runs.
