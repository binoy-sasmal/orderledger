{{ config(materialized='incremental', unique_key='item_key', incremental_strategy='delete+insert', tags=['facts']) }}
-- depends_on: {{ ref('touched_orders') }}
-- Grain: one row per (order_id, item_no) whose order has been placed. price is the current
-- price after corrections. Items follow their order's deleted flag.
select
    i.item_key, i.order_id, i.item_no, i.product_id, i.seller_id, i.price, i.freight,
    o.purchase_date, o.customer_unique_id, o.is_deleted
from {{ ref('stg_order_items') }} i
join {{ ref('fct_orders') }} o on o.order_id = i.order_id
{% if is_incremental() %}
where i.order_id in (select order_id from {{ ref('touched_orders') }})
{% endif %}
