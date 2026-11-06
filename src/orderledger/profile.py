"""Source profiling: sizes, null rates, contradictions and customer/address facts (M0).

Output is a Markdown report. It reads only the CSVs and states what was measured; the
design decisions that depend on it (e.g. whether SCD2 is justified) are recorded in
docs/decisions/ and cite this report.
"""

from __future__ import annotations

from pathlib import Path

import duckdb


def _t(headers: list[str], rows: list[tuple]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def profile_markdown(csv_dir: Path) -> str:
    con = duckdb.connect()
    for name, f in {
        "orders": "olist_orders_dataset.csv",
        "items": "olist_order_items_dataset.csv",
        "customers": "olist_customers_dataset.csv",
        "products": "olist_products_dataset.csv",
    }.items():
        con.execute(f"create view {name} as select * from read_csv('{(csv_dir / f).as_posix()}', header=true)")
    q = lambda s: con.execute(s).fetchall()  # noqa: E731
    one = lambda s: con.execute(s).fetchone()[0]  # noqa: E731
    md = ["# Source profile", "", "Measured directly from the source CSVs by `orderledger profile`.", ""]

    md += ["## Row counts", "", _t(["table", "rows"], [(t, one(f"select count(*) from {t}")) for t in ("orders", "items", "customers", "products")]), ""]
    md += ["## Orders: null counts per timestamp", "",
           _t(["column", "nulls", "of"], [
               (c, one(f"select count(*) from orders where {c} is null"), one("select count(*) from orders"))
               for c in ("order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
                         "order_delivered_customer_date", "order_estimated_delivery_date")]), ""]
    md += ["## Order status", "", _t(["status", "orders"], q("select order_status, count(*) from orders group by 1 order by 2 desc")), ""]
    rng = q("select min(order_purchase_timestamp), max(order_purchase_timestamp) from orders")[0]
    md += [f"Purchase timestamps range from {rng[0]} to {rng[1]}.", ""]

    md += ["## Contradictions in the source", "", _t(["observation", "orders"], [
        ("status canceled but a customer delivery time exists", one("select count(*) from orders where order_status='canceled' and order_delivered_customer_date is not null")),
        ("status delivered but no customer delivery time", one("select count(*) from orders where order_status='delivered' and order_delivered_customer_date is null")),
        ("delivery time exists but status is neither delivered nor canceled", one("select count(*) from orders where order_delivered_customer_date is not null and order_status not in ('delivered','canceled')")),
        ("approved before purchase", one("select count(*) from orders where order_approved_at < order_purchase_timestamp")),
        ("carrier handover before approval", one("select count(*) from orders where order_delivered_carrier_date < order_approved_at")),
        ("carrier handover before purchase", one("select count(*) from orders where order_delivered_carrier_date < order_purchase_timestamp")),
        ("delivery before carrier handover", one("select count(*) from orders where order_delivered_customer_date < order_delivered_carrier_date")),
        ("delivery before purchase", one("select count(*) from orders where order_delivered_customer_date < order_purchase_timestamp")),
        ("order with no item rows", one("select count(*) from orders where order_id not in (select order_id from items)")),
        ("item whose order is missing", one("select count(*) from items where order_id not in (select order_id from orders)")),
    ]), ""]
    md += ["Cancellation has no timestamp in the source, so none is derived.", ""]

    md += ["## Customers and addresses (decides whether SCD2 is justified)", ""]
    ids = one("select count(*) from customers")
    uniq = one("select count(distinct customer_unique_id) from customers")
    rep = one("select count(*) from (select customer_unique_id from customers group by 1 having count(*) > 1)")
    moved = one("select count(*) from (select customer_unique_id from customers group by 1 "
                "having count(distinct customer_state || '|' || customer_city) > 1)")
    moved_state = one("select count(*) from (select customer_unique_id from customers group by 1 "
                      "having count(distinct customer_state) > 1)")
    md += [_t(["measure", "value"], [
        ("customer_id rows (one per order)", ids),
        ("distinct customer_unique_id (people)", uniq),
        ("people with more than one order", rep),
        ("people observed with more than one city/state", moved),
        ("people observed in more than one state", moved_state),
    ]), "",
        f"{rep} of {uniq} people ({100 * rep / uniq:.1f}%) have more than one order; "
        f"{moved} ({100 * moved / uniq:.2f}%) were observed at more than one city/state. "
        "An address here is the one observed on a particular order, not verified residence history.", ""]

    md += ["## Items and products", "", _t(["measure", "value"], [
        ("orders with at least one item", one("select count(distinct order_id) from items")),
        ("items per order, max", one("select max(c) from (select count(*) c from items group by order_id)")),
        ("items with null price", one("select count(*) from items where price is null")),
        ("products with no category", one("select count(*) from products where product_category_name is null")),
        ("items whose product is missing", one("select count(*) from items where product_id not in (select product_id from products)")),
    ]), ""]
    return "\n".join(md)
