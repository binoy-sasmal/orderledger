{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='delete+insert', tags=['facts']) }}
-- depends_on: {{ ref('touched_orders') }}
-- Grain: one row per order that has an order_placed event. Lifecycle timestamps and status
-- are kept as separate columns: observed_status is a snapshot with no time of its own, and
-- no cancellation time exists or is invented.
with scope as (
    select entity_id as order_id, event_type, payload
    from {{ ref('stg_current_events') }}
    where entity_type = 'order'
    {% if is_incremental() %}
      and entity_id in (select order_id from {{ ref('touched_orders') }})
    {% endif %}
),
o as (
    select
        order_id,
        max(case when event_type = 'order_placed' then json_extract_string(payload, '$.customer_unique_id') end) as customer_unique_id,
        max(case when event_type = 'order_placed' then json_extract_string(payload, '$.customer_state') end) as customer_state,
        max(case when event_type = 'order_placed' then json_extract_string(payload, '$.customer_city') end) as customer_city,
        max(case when event_type = 'order_placed' then cast(json_extract_string(payload, '$.purchase_ts') as timestamp) end) as purchase_ts,
        max(case when event_type = 'order_placed' then cast(json_extract_string(payload, '$.estimated_delivery_ts') as timestamp) end) as estimated_delivery_ts,
        max(case when event_type = 'order_approved' then cast(json_extract_string(payload, '$.ts') as timestamp) end) as approved_ts,
        max(case when event_type = 'order_handed_to_carrier' then cast(json_extract_string(payload, '$.ts') as timestamp) end) as carrier_ts,
        max(case when event_type = 'order_delivered' then cast(json_extract_string(payload, '$.ts') as timestamp) end) as delivered_ts,
        max(case when event_type = 'order_status_observed' then json_extract_string(payload, '$.status') end) as observed_status,
        coalesce(bool_or(event_type = 'order_deleted'), false) as is_deleted
    from scope
    group by order_id
),
i as (
    select order_id, count(*) as item_count, sum(price) as items_value, sum(freight) as freight_value
    from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
    where order_id in (select order_id from {{ ref('touched_orders') }})
    {% endif %}
    group by order_id
),
j as (
    select
        o.*,
        coalesce(i.item_count, 0) as item_count,
        cast(coalesce(i.items_value, 0) as decimal(14, 2)) as items_value,
        cast(coalesce(i.freight_value, 0) as decimal(14, 2)) as freight_value
    from o
    left join i using (order_id)
    where o.purchase_ts is not null
)
select
    order_id, customer_unique_id, customer_state, customer_city,
    purchase_ts, approved_ts, carrier_ts, delivered_ts, estimated_delivery_ts,
    observed_status, is_deleted, item_count, items_value, freight_value,
    cast(purchase_ts as date) as purchase_date,
    cast(delivered_ts as date) as delivered_date,
    -- Every recorded timestamp must not precede an earlier stage's recorded timestamp.
    (   (approved_ts is null or approved_ts >= purchase_ts)
    and (carrier_ts is null or (carrier_ts >= purchase_ts and (approved_ts is null or carrier_ts >= approved_ts)))
    and (delivered_ts is null or (delivered_ts >= purchase_ts
            and (approved_ts is null or delivered_ts >= approved_ts)
            and (carrier_ts is null or delivered_ts >= carrier_ts)))
    ) as is_sequence_valid,
    case when delivered_ts is not null and estimated_delivery_ts is not null
         then cast(delivered_ts as date) <= cast(estimated_delivery_ts as date) end as delivered_on_time,
    case when delivered_ts is not null
         then date_diff('second', purchase_ts, delivered_ts) end as delivery_seconds
from j
