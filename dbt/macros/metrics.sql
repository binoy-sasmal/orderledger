{# Metric definitions live here once, so the incremental tables and the full-recompute
   reconciliation views cannot drift apart. See docs/metrics.md.
   Aggregates hold one row per date WITH activity; a date with no qualifying orders has no
   row, in both the incremental tables and a full replay. #}

{% macro window_dates(col, fct_col) %}
{# Dates restated by an incremental run: the lookback window ending at the simulated
   checkpoint, the (new) dates of orders touched in this run, and any forced backfill window. #}
select date_day as {{ col }} from {{ ref('dim_date') }}
where date_day between cast('{{ var("checkpoint_date") }}' as date) - interval {{ var("lookback_days") }} day
                   and cast('{{ var("checkpoint_date") }}' as date)
   or date_day in (select {{ fct_col }} from {{ ref('fct_orders') }}
                   where order_id in (select order_id from {{ ref('touched_orders') }}))
   {% if var('force_from') is not none %}
   or date_day between cast('{{ var("force_from") }}' as date) and cast('{{ var("force_to") }}' as date)
   {% endif %}
{% endmacro %}

{% macro purge_window(col, fct_col) %}
{# Pre-hook: remove the rows about to be restated, so a date that no longer has activity
   disappears instead of lingering. Skipped on the first build (no table yet). #}
{% if load_relation(this) is not none %}
delete from {{ this }} where {{ col }} in ({{ window_dates(col, fct_col) }})
{% endif %}
{% endmacro %}

{% macro purchase_metrics(dates) %}
select
    d.purchase_date,
    count(o.order_id) as orders_placed,
    cast(coalesce(sum(o.items_value), 0) as decimal(18, 2)) as gmv,
    cast(coalesce(sum(o.freight_value), 0) as decimal(18, 2)) as freight_value,
    count(o.observed_status) as status_known_orders,
    count(o.order_id) filter (where o.observed_status = 'canceled') as canceled_orders,
    case when count(o.observed_status) = count(o.order_id)
         then round(count(o.order_id) filter (where o.observed_status = 'canceled')::double / count(o.order_id), 6)
    end as cancellation_share
from {{ dates }} d
join {{ ref('fct_orders') }} o
  on o.purchase_date = d.purchase_date and not o.is_deleted
group by d.purchase_date
{% endmacro %}

{% macro delivery_metrics(dates) %}
select
    d.delivered_date,
    count(o.order_id) filter (where o.is_sequence_valid) as delivered_orders,
    count(o.order_id) filter (where o.is_sequence_valid and o.estimated_delivery_ts is not null) as eligible_orders,
    count(o.order_id) filter (where o.is_sequence_valid and o.delivered_on_time) as on_time_orders,
    case when count(o.order_id) filter (where o.is_sequence_valid and o.estimated_delivery_ts is not null) > 0
         then round(count(o.order_id) filter (where o.is_sequence_valid and o.delivered_on_time)::double
                    / count(o.order_id) filter (where o.is_sequence_valid and o.estimated_delivery_ts is not null), 6)
    end as on_time_rate,
    coalesce(sum(o.delivery_seconds) filter (where o.is_sequence_valid), 0)::bigint as delivery_seconds_total,
    case when count(o.order_id) filter (where o.is_sequence_valid) > 0
         then round(sum(o.delivery_seconds) filter (where o.is_sequence_valid)::double
                    / count(o.order_id) filter (where o.is_sequence_valid) / 86400.0, 4)
    end as avg_delivery_days,
    count(o.order_id) filter (where not o.is_sequence_valid) as excluded_invalid_sequence,
    count(o.order_id) filter (where o.observed_status = 'canceled') as status_conflict_canceled
from {{ dates }} d
join {{ ref('fct_orders') }} o
  on o.delivered_date = d.delivered_date and not o.is_deleted
group by d.delivered_date
{% endmacro %}
