{{ config(materialized='table', tags=['facts']) }}
-- Type 1 on customer_unique_id (the person). The state and city on an order are the address
-- OBSERVED ON THAT ORDER, not verified residence history, so no SCD2 is built here (see
-- docs/decisions/ADR-003). The latest observed address is shown for convenience.
select
    customer_unique_id,
    arg_max(customer_state, struct_pack(t := purchase_ts, o := order_id)) as latest_state,
    arg_max(customer_city, struct_pack(t := purchase_ts, o := order_id)) as latest_city,
    count(distinct customer_state) as distinct_states_observed,
    min(purchase_ts) as first_purchase_ts,
    max(purchase_ts) as last_purchase_ts,
    count(*) as order_count
from {{ ref('fct_orders') }}
where not is_deleted and customer_unique_id is not null
group by customer_unique_id
