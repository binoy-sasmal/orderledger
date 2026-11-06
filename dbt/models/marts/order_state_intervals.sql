{{ config(materialized='table', tags=['marts']) }}
-- Order lifecycle states as half-open intervals [valid_from, valid_to), built only from
-- recorded timestamps. Coverage starts at purchase: nothing exists before the first
-- observation. valid_to is NULL for the last known state (open until the latest observation).
-- Orders whose recorded timestamps contradict each other are excluded (is_sequence_valid).
with f as (
    select * from {{ ref('fct_orders') }} where is_sequence_valid and not is_deleted
),
s as (
    select order_id, 'placed' as state, 1 as stage, purchase_ts as ts from f
    union all
    select order_id, 'approved', 2, approved_ts from f where approved_ts is not null
    union all
    select order_id, 'in_transit', 3, carrier_ts from f where carrier_ts is not null
    union all
    select order_id, 'delivered', 4, delivered_ts from f where delivered_ts is not null
)
select
    order_id, state, stage,
    ts as valid_from,
    lead(ts) over (partition by order_id order by stage) as valid_to
from s
