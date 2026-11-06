{{ config(materialized='table', tags=['facts']) }}
select
    cast(d as date) as date_day,
    year(d) as year,
    month(d) as month,
    strftime(d, '%Y-%m') as year_month,
    isodow(d) as iso_weekday
from range(timestamp '2016-01-01', timestamp '2020-01-01', interval 1 day) as t(d)
