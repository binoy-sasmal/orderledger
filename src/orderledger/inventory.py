"""Expected source inventory, computed straight from the source CSVs.

Deliberately a separate code path from simulate.py (which builds events): it never looks
at events, so a source event the simulator or transport lost shows up as a mismatch.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

FILES = {
    "orders": "olist_orders_dataset.csv",
    "items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
}


def source_inventory(csv_dir: Path) -> dict:
    con = duckdb.connect()
    p = {k: (csv_dir / v).as_posix() for k, v in FILES.items()}
    one = lambda sql: con.execute(sql).fetchone()[0]  # noqa: E731
    o = f"read_csv('{p['orders']}', header=true)"
    counts = {
        "order_placed": one(f"select count(*) from {o} where order_purchase_timestamp is not null"),
        "order_approved": one(f"select count(*) from {o} where order_approved_at is not null"),
        "order_handed_to_carrier": one(
            f"select count(*) from {o} where order_delivered_carrier_date is not null"
        ),
        "order_delivered": one(
            f"select count(*) from {o} where order_delivered_customer_date is not null"
        ),
        "order_status_observed": one(f"select count(*) from {o}"),
        "order_item_added": one(f"select count(*) from read_csv('{p['items']}', header=true)"),
        "product_observed": one(f"select count(*) from read_csv('{p['products']}', header=true)"),
    }
    by_date = con.execute(
        f"select cast(order_purchase_timestamp as date)::varchar d, count(*) from {o} "
        "where order_purchase_timestamp is not null group by 1 order by 1"
    ).fetchall()
    return {"event_counts": counts, "placed_by_date": {d: n for d, n in by_date}}
