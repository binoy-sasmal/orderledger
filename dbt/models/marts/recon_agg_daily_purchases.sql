{{ config(materialized='view', tags=['marts']) }}
-- Full recomputation over every date; reconciliation compares it with the incremental table.
with dates as (
    select date_day as purchase_date from {{ ref('dim_date') }}
    where date_day between (select min(purchase_date) from {{ ref('fct_orders') }})
                       and (select max(purchase_date) from {{ ref('fct_orders') }})
)
{{ purchase_metrics('dates') }}
