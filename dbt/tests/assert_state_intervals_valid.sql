{{ config(tags=['cross']) }}
-- Interval rules for order_state_intervals, inside its coverage period:
--   1. an interval never ends before it starts;
--   2. intervals of one order do not overlap;
--   3. the first interval starts exactly at purchase: no history before the first observation.
select order_id, state, 'ends before it starts' as problem
from {{ ref('order_state_intervals') }}
where valid_to is not null and valid_to < valid_from
union all
select a.order_id, a.state, 'overlaps ' || b.state
from {{ ref('order_state_intervals') }} a
join {{ ref('order_state_intervals') }} b
  on a.order_id = b.order_id and a.stage < b.stage
where a.valid_to is null or a.valid_to > b.valid_from
union all
select s.order_id, s.state, 'first interval is not the purchase time'
from {{ ref('order_state_intervals') }} s
join {{ ref('fct_orders') }} o using (order_id)
where s.stage = 1 and s.valid_from <> o.purchase_ts
union all
select o.order_id, 'placed', 'valid placed order without a placed interval'
from {{ ref('fct_orders') }} o
where o.is_sequence_valid and not o.is_deleted
  and not exists (select 1 from {{ ref('order_state_intervals') }} s
                  where s.order_id = o.order_id and s.stage = 1)
