{{ config(materialized='table', tags=['facts']) }}
-- Type 1: the latest observed attributes. Olist has no product attribute history, so no
-- history table is built.
select
    entity_id as product_id,
    json_extract_string(payload, '$.category') as category,
    json_extract_string(payload, '$.category_en') as category_en
from {{ ref('stg_current_events') }}
where event_type = 'product_observed'
