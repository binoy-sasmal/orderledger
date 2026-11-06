# Event contract (schema version 1)

The warehouse ingests a **change history**: batched Parquet files plus a manifest. Olist itself is
a set of static snapshot tables, so this history is **constructed** by
[`simulate.py`](../src/orderledger/simulate.py). This page states exactly what is real.

## What is real and what is synthetic

| Element | Origin |
|---|---|
| Event content (customer, timestamps, prices, items, status) | Olist fields (`content_origin = source-field`) |
| `effective_ts` of lifecycle events | The Olist timestamp (`timing_origin = source-timestamp`) |
| `arrival_ts`, `arrival_seq`, batch membership | **Always simulated.** Olist has no arrival data. Arrival is the effective time plus a deterministic 0 to 36 hour delay |
| Status, product attributes | Snapshots with no time of their own (`timing_origin = not-applicable`, `effective_ts` NULL) |
| Duplicates, late arrivals, price corrections, delivery-time corrections, deletions, out-of-order arrivals | **Synthetic.** Olist contains none of these. Each is requested in a scenario file and labelled `content_origin = synthetic-perturbation` with a `scenario_tag` |
| No cancellation time | None exists in the source and none is invented. Cancellation is an observed status only |

Same CSVs plus the same scenario produce the same logical events (seeded hashes, no randomness).

## Identity, version order and time are separate

| Field | Meaning |
|---|---|
| `event_id` | Identity of one change record. A re-delivery of the same change reuses it |
| `entity_type`, `entity_id` | The business object: `order`, `order_item` (`order_id#item_no`), `product` |
| `event_type` | e.g. `order_placed`, `order_approved`, `order_handed_to_carrier`, `order_delivered`, `order_status_observed`, `order_deleted`, `order_item_added`, `order_item_price_corrected`, `product_observed` |
| `source_seq` | Version order of changes for one entity. **The higher number supersedes.** It is the only thing that decides which version of a fact is current |
| `effective_ts` | Business time of the change. Not used to order versions |
| `arrival_ts`, `arrival_seq` | Simulated arrival; `arrival_seq` is the global arrival order and drives the incremental watermark |
| `payload` | JSON body. Its `md5` is stored as `payload_hash` at load |

## Resolution rules

| Situation | Treatment | Tested by |
|---|---|---|
| Same `event_id`, same payload, delivered again | Exact duplicate. Kept in `raw.events` (raw is what arrived), collapsed to one in staging | AT-01, AT-08 |
| Same bytes presented again as another batch | Recognised by file hash, recorded, skipped | AT-01 |
| Same `event_id`, **different** payload | **Contradiction.** The batch is quarantined, the load stops, the run fails. Never silently deduplicated | `test_load` |
| Two different `event_id`s claim the same `(entity, source_seq)` | Contradiction, same handling | `test_load` |
| Higher `source_seq` arrives first, lower later | The higher version stays current; the stale one is ignored | AT-08 (out of order) |
| Two changes with the same `effective_ts` | Ordered by `source_seq`, higher wins | AT-08 |
| Correction (higher `source_seq`) | Replaces the earlier value of that fact | AT-08 |
| Deletion (`order_deleted`) | Soft delete: flagged, excluded from metrics, raw history kept | AT-08 |
| Event for an order whose `order_placed` has not arrived | Waits in raw; reported as an orphan (warning); appears in facts once the order arrives | reconciliation |

## Batch manifest and inventory

* `manifest.json` lists each batch file with SHA-256, row count, simulated period and
  `max_arrival_seq`. The loader verifies hash, row count, column names and types, value rules,
  and contradictions before a batch is applied, in one transaction.
* `inventory.json` is the **expected source inventory**, computed straight from the CSVs by
  [`inventory.py`](../src/orderledger/inventory.py), a separate code path from event
  construction. It is used to detect source events that never arrived (AT-09), which the
  manifest alone cannot do, because the manifest describes what was sent.
* A **breaking schema change** (renamed, missing, extra or retyped column) quarantines the batch
  and fails the run (AT-10).
