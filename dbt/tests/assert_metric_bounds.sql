{{ config(tags=['marts']) }}
-- Metrics must stay inside their definitions: rates in [0, 1] and counts nested correctly.
select 'purchases' as aggregate, cast(purchase_date as varchar) as day
from {{ ref('agg_daily_purchases') }}
where gmv < 0 or freight_value < 0 or orders_placed < 0
   or canceled_orders > status_known_orders or status_known_orders > orders_placed
   or cancellation_share < 0 or cancellation_share > 1
union all
select 'deliveries', cast(delivered_date as varchar)
from {{ ref('agg_daily_deliveries') }}
where on_time_orders > eligible_orders or eligible_orders > delivered_orders
   or on_time_rate < 0 or on_time_rate > 1 or avg_delivery_days < 0
