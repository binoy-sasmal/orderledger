{{ config(materialized='view', tags=['facts']) }}
-- One row per item that has an order_item_added event. The price is that of the highest
-- source_seq among the added and price-corrected events; everything else comes from the
-- added event. A correction that arrived before its item waits here until the item arrives.
with added as (
    select
        entity_id as item_key,
        json_extract_string(payload, '$.order_id') as order_id,
        cast(json_extract_string(payload, '$.item_no') as integer) as item_no,
        json_extract_string(payload, '$.product_id') as product_id,
        json_extract_string(payload, '$.seller_id') as seller_id,
        cast(json_extract_string(payload, '$.freight') as decimal(12, 2)) as freight
    from {{ ref('stg_events') }}
    where event_type = 'order_item_added'
),
priced as (
    select item_key, price
    from (
        select
            entity_id as item_key,
            cast(json_extract_string(payload, '$.price') as decimal(12, 2)) as price,
            row_number() over (partition by entity_id order by source_seq desc) as rn
        from {{ ref('stg_events') }}
        where event_type in ('order_item_added', 'order_item_price_corrected')
    )
    where rn = 1
)
select a.*, p.price
from added a
join priced p using (item_key)
