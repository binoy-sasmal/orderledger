"""AT-12: headline metrics agree with two independent computations.

1. Without synthetic events, the warehouse must equal a direct SQL computation over the
   source CSVs (a path that never touches events).
2. With synthetic events, it must equal the narrow plain-Python reference (reference.py).
The reference and the warehouse share an author and the written rules; see docs/metrics.md.
"""

from __future__ import annotations

from decimal import Decimal

import duckdb

from helpers import FIXTURE, rows
from orderledger.reference import reference_metrics


def _csv_metrics():
    con = duckdb.connect()
    o = f"read_csv('{(FIXTURE / 'olist_orders_dataset.csv').as_posix()}', header=true)"
    i = f"read_csv('{(FIXTURE / 'olist_order_items_dataset.csv').as_posix()}', header=true)"
    orders = dict(con.execute(
        f"select cast(order_purchase_timestamp as date)::varchar, count(*) from {o} group by 1").fetchall())
    gmv = dict(con.execute(
        f"select cast(o.order_purchase_timestamp as date)::varchar, sum(i.price) from {o} o join {i} i using (order_id) group by 1").fetchall())
    d = con.execute(f"""
        select cast(order_delivered_customer_date as date)::varchar,
               count(*),
               count(*) filter (where order_estimated_delivery_date is not null
                                and cast(order_delivered_customer_date as date) <= cast(order_estimated_delivery_date as date))
        from {o}
        where order_delivered_customer_date is not null
          and (order_approved_at is null or order_approved_at >= order_purchase_timestamp)
          and (order_delivered_carrier_date is null or (order_delivered_carrier_date >= order_purchase_timestamp
               and (order_approved_at is null or order_delivered_carrier_date >= order_approved_at)))
          and order_delivered_customer_date >= order_purchase_timestamp
          and (order_approved_at is null or order_delivered_customer_date >= order_approved_at)
          and (order_delivered_carrier_date is null or order_delivered_customer_date >= order_delivered_carrier_date)
        group by 1""").fetchall()
    return orders, gmv, {k: n for k, n, _ in d}, {k: m for k, _, m in d if m}


def _published(ws):
    p = rows(ws, "select purchase_date::varchar, orders_placed, gmv from published.agg_daily_purchases")
    d = rows(ws, "select delivered_date::varchar, delivered_orders, on_time_orders from published.agg_daily_deliveries")
    return ({k: n for k, n, _ in p}, {k: Decimal(g) for k, _, g in p if g},
            {k: n for k, n, _ in d if n}, {k: m for k, _, m in d if m})


def test_warehouse_matches_a_direct_csv_computation_without_synthetic_events(clean_full):
    exp_orders, exp_gmv, exp_deliv, exp_ontime = _csv_metrics()
    got_orders, got_gmv, got_deliv, got_ontime = _published(clean_full)
    assert got_orders == exp_orders
    assert got_gmv == {k: Decimal(str(v)).quantize(Decimal("0.01")) for k, v in exp_gmv.items()}
    assert got_deliv == exp_deliv
    assert got_ontime == exp_ontime


def test_warehouse_matches_the_python_reference_with_synthetic_events(full7):
    ref = reference_metrics(full7 / "events")
    got_orders, got_gmv, got_deliv, got_ontime = _published(full7)
    assert got_orders == ref["orders"]
    assert got_gmv == {k: v for k, v in ref["gmv"].items() if v}
    assert got_deliv == ref["delivered"]
    assert got_ontime == ref["on_time"]
    # the scenario really changed the numbers relative to the unperturbed source
    assert got_orders != _csv_metrics()[0] or got_gmv != _csv_metrics()[1]
