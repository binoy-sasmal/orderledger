{{ config(materialized='incremental', unique_key='purchase_date', incremental_strategy='delete+insert', pre_hook="{{ purge_window('purchase_date', 'purchase_date') }}", tags=['marts']) }}
-- depends_on: {{ ref('touched_orders') }}
-- Time basis: purchase date of the order. Population: every non-deleted order placed that day.
-- One row per date with activity. Restated on each run for: dates within the lookback of the
-- checkpoint, dates of orders touched in this run (their NEW dates), and any forced backfill
-- window. A late change that moves an order OFF an older date is not seen here; reconciliation
-- detects it and a backfill repairs it.
with dates as (
    {% if is_incremental() %}
    {{ window_dates('purchase_date', 'purchase_date') }}
    {% else %}
    select date_day as purchase_date from {{ ref('dim_date') }}
    where date_day between (select min(purchase_date) from {{ ref('fct_orders') }})
                       and (select max(purchase_date) from {{ ref('fct_orders') }})
    {% endif %}
)
{{ purchase_metrics('dates') }}
