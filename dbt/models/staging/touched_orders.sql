{{ config(materialized='table', tags=['facts']) }}
-- Orders to recompute in this run:
--   * every order with an event that arrived after the published watermark (a new item or a
--     price correction counts for its order), and
--   * on a backfill, every order whose purchase or delivery date falls in the forced window.
-- Recomputation always rebuilds the order from its full event history, so the result does not
-- depend on how events were split into batches.
with arrived as (
    select distinct
        case when entity_type = 'order' then entity_id
             else json_extract_string(payload, '$.order_id') end as order_id
    from {{ source('raw', 'events') }}
    where arrival_seq > {{ var('watermark') }}
      and entity_type in ('order', 'order_item')
),
forced as (
    {% if var('force_from') is not none %}
    select distinct entity_id as order_id
    from {{ ref('stg_current_events') }}
    where entity_type = 'order'
      and event_type in ('order_placed', 'order_delivered')
      and cast(effective_ts as date)
          between cast('{{ var("force_from") }}' as date) and cast('{{ var("force_to") }}' as date)
    {% else %}
    select cast(null as varchar) as order_id where false
    {% endif %}
)
select order_id from arrived where order_id is not null
union
select order_id from forced
