{{ config(materialized='view', tags=['facts']) }}
-- Current version of each fact: the highest source_seq wins, whatever order events arrived in.
-- effective_ts never decides which version is current. Ties cannot occur: the loader rejects
-- two event_ids that claim the same entity version.
select * exclude (rn)
from (
    select *, row_number() over (
        partition by entity_type, entity_id, event_type order by source_seq desc
    ) as rn
    from {{ ref('stg_events') }}
)
where rn = 1
