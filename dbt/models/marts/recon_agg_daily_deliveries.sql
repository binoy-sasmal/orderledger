{{ config(materialized='view', tags=['marts']) }}
with dates as (
    select date_day as delivered_date from {{ ref('dim_date') }}
    where date_day between (select min(delivered_date) from {{ ref('fct_orders') }})
                       and (select max(delivered_date) from {{ ref('fct_orders') }})
)
{{ delivery_metrics('dates') }}
