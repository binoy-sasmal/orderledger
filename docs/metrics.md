# Metric definitions

Every metric states its **time basis**, **eligible population**, **denominator**, **cancellation
treatment** and **late-update behaviour**. The SQL lives once, in
[`dbt/macros/metrics.sql`](../dbt/macros/metrics.sql); the full-recompute reconciliation views
reuse the same macros, so the incremental tables and the checks cannot drift apart.

Both aggregates hold **one row per date with activity**. A date with no qualifying order has no
row, in the incremental tables and in a full replay alike.

Shared rules:

* **Deleted orders** (a synthetic `order_deleted` event) are excluded from every metric. Their
  raw history is kept.
* **Order status is a snapshot without a time.** The source has no cancellation timestamp, so
  none exists here. `observed_status` arrives in the final batch and is used only where noted.
* **Sequence validity.** An order is *sequence-valid* when every recorded lifecycle timestamp is
  not earlier than any earlier stage's recorded timestamp (purchase ≤ approval ≤ carrier
  handover ≤ delivery, where present). Invalid orders are kept in `fct_orders` and flagged, but
  excluded from delivery metrics and from `order_state_intervals`; nothing is repaired.

## `agg_daily_purchases` (grain: one row per purchase date)

| Column | Definition |
|---|---|
| time basis | Calendar date of `purchase_ts` (the order's recorded purchase time) |
| population | Every non-deleted order placed that day, whatever its status |
| `orders_placed` | Count of those orders |
| `gmv` | Sum of current item prices of those orders. **Gross**: includes orders later canceled; excludes freight |
| `freight_value` | Sum of item freight of those orders |
| `status_known_orders` | Orders whose status snapshot has arrived |
| `canceled_orders` | Orders whose snapshot status is `canceled` |
| `cancellation_share` | `canceled_orders / orders_placed`, **defined only when every order of the day has a status** (`status_known_orders = orders_placed`); otherwise NULL |
| late updates | Restated when a touched order falls in the run's date set (lookback window, new dates of touched orders, forced backfill window). A late change that moves an order *off* an older date is detected by reconciliation, then repaired by backfill |

Consequence: the snapshot arrives in the final batch, so `cancellation_share` is NULL at every
earlier checkpoint. It is not an intermediate-checkpoint metric.

## `agg_daily_deliveries` (grain: one row per delivery date)

| Column | Definition |
|---|---|
| time basis | Calendar date of the recorded customer delivery timestamp |
| population | Non-deleted orders with a recorded delivery time (a different population from purchases) |
| `delivered_orders` | Of those, sequence-valid orders |
| `eligible_orders` | Delivered orders that also have an estimated delivery date |
| `on_time_orders` | Eligible orders delivered on or before the estimated delivery **date** (date comparison, not timestamp) |
| `on_time_rate` | `on_time_orders / eligible_orders`; NULL when `eligible_orders = 0` |
| `delivery_seconds_total`, `avg_delivery_days` | Purchase-to-delivery time over the sequence-valid delivered orders; the average is derived from the exact integer sum |
| `excluded_invalid_sequence` | Orders with a delivery time but an inconsistent timestamp order (not counted above) |
| `status_conflict_canceled` | Orders that carry a delivery time **and** whose snapshot status is `canceled` |
| cancellation treatment | Status is not used. A canceled order with a recorded delivery time is counted as delivered and reported in `status_conflict_canceled`. This is a limit of the source, not a decision that the order was delivered |
| late updates | As for purchases, keyed on the delivery date |

Why two aggregates: purchase-date cohorts and delivery-date throughput answer different
questions and have different populations and denominators, so they are never mixed in one row.

## Other tables

* `fct_orders`, grain one row per placed order: lifecycle timestamps, `observed_status` and
  `is_deleted` as separate columns.
* `fct_order_items`, grain one row per `(order_id, item_no)`: current price after corrections.
* `order_state_intervals`: order lifecycle states as half-open `[valid_from, valid_to)`
  intervals from recorded timestamps only. Coverage starts at purchase; there is no history before
  the first observation. `valid_to` is NULL for the last known state.
* `dim_customer` (Type 1, key `customer_unique_id`), `dim_product` (Type 1), `dim_date`.
