{{ config(tags=['facts']) }}
-- Order totals must equal the sum of their item rows (non-deleted orders only).
select o.order_id, o.items_value, coalesce(i.price_sum, 0) as item_rows_sum
from {{ ref('fct_orders') }} o
left join (
    select order_id, sum(price) as price_sum from {{ ref('fct_order_items') }} group by order_id
) i using (order_id)
where o.items_value <> coalesce(i.price_sum, 0)
