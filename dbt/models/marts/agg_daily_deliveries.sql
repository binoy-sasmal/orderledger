{{ config(materialized='incremental', unique_key='delivered_date', incremental_strategy='delete+insert', pre_hook="{{ purge_window('delivered_date', 'delivered_date') }}", tags=['marts']) }}
-- depends_on: {{ ref('touched_orders') }}
-- Time basis: delivery date (recorded customer delivery timestamp). Population: non-deleted
-- orders with a recorded delivery time and a consistent timestamp sequence. On-time rate:
-- delivered on or before the estimated delivery date, over orders that have an estimate.
-- Order status is NOT used: it is a snapshot with no time, and canceled orders that carry a
-- delivery time are counted in status_conflict_canceled. One row per date with activity.
with dates as (
    {% if is_incremental() %}
    {{ window_dates('delivered_date', 'delivered_date') }}
    {% else %}
    select date_day as delivered_date from {{ ref('dim_date') }}
    where date_day between (select min(delivered_date) from {{ ref('fct_orders') }})
                       and (select max(delivered_date) from {{ ref('fct_orders') }})
    {% endif %}
)
{{ delivery_metrics('dates') }}
