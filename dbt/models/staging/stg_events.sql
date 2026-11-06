{{ config(materialized='view', tags=['facts']) }}
-- Exact re-deliveries share an event_id and payload (the loader rejects the same id with
-- different content), so keeping the first arrival loses nothing.
select * exclude (rn)
from (
    select *, row_number() over (partition by event_id order by arrival_seq) as rn
    from {{ source('raw', 'events') }}
)
where rn = 1
