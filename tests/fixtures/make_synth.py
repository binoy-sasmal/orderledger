"""Generate the tiny synthetic CI fixture in Olist's CSV layout.

Authored for this project: invented IDs, invented values, no Olist rows. Includes the
quirks the pipeline must handle: cancelled orders with a delivery time, missing approval
times, out-of-order timestamps, undelivered orders, orders without items, products without
a category, and a customer who appears on two orders with different addresses.

    python tests/fixtures/make_synth.py
"""

from __future__ import annotations

import csv
import datetime as dt
import random
from pathlib import Path

OUT = Path(__file__).parent / "synth"
rng = random.Random(20261106)
START = dt.datetime(2018, 1, 1, 9, 0, 0)
STATES = ["SP", "RJ", "MG", "RS", "BA"]
CITIES = {"SP": "sao paulo", "RJ": "rio", "MG": "belo", "RS": "porto", "BA": "salvador"}
N_ORDERS = 70


def fmt(t: dt.datetime | None) -> str:
    return "" if t is None else t.strftime("%Y-%m-%d %H:%M:%S")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    cats = [("toys", "toys"), ("books", "books"), ("garden", "garden_tools"), (None, None)]
    products = []
    for i in range(12):
        c = cats[i % len(cats)]
        products.append((f"p{i:03d}", c[0] or ""))
    with open(OUT / "product_category_name_translation.csv", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["product_category_name", "product_category_name_english"])
        for c in cats[:3]:
            w.writerow(c)
    with open(OUT / "olist_products_dataset.csv", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["product_id", "product_category_name"])
        w.writerows(products)

    customers, orders, items = [], [], []
    persons = [f"u{i:03d}" for i in range(55)]
    for n in range(N_ORDERS):
        oid = f"o{n:04d}"
        cid = f"c{n:04d}"
        uid = persons[n] if n < 55 else persons[n - 55]  # 15 repeat customers
        state = rng.choice(STATES)
        customers.append((cid, uid, "10000", CITIES[state], state))
        purchase = START + dt.timedelta(hours=rng.randint(0, 24 * 60))
        approved = purchase + dt.timedelta(hours=rng.randint(1, 20))
        carrier = approved + dt.timedelta(days=rng.randint(1, 3))
        delivered = carrier + dt.timedelta(days=rng.randint(2, 12))
        estimated = purchase + dt.timedelta(days=rng.randint(8, 16))
        status = "delivered"
        if n % 17 == 3:  # shipped, not delivered
            delivered, status = None, "shipped"
        if n % 19 == 5:  # never shipped
            carrier = delivered = None
            status = "processing"
        if n % 23 == 7:  # cancelled, yet carries a delivery time (contradiction)
            status = "canceled"
        if n % 13 == 4:  # approval time missing
            approved = None
        if n % 29 == 6 and delivered:  # carrier time after delivery time
            carrier = delivered + dt.timedelta(days=1)
        orders.append((oid, cid, status, fmt(purchase), fmt(approved), fmt(carrier), fmt(delivered), fmt(estimated)))
        if n % 31 != 9:  # a few orders without items
            for k in range(1, rng.choice([1, 1, 2, 3]) + 1):
                p = rng.choice(products)[0]
                items.append((oid, k, p, f"s{rng.randint(1, 5)}", f"{rng.randint(5, 300)}.{rng.choice(['00', '50', '99'])}", f"{rng.randint(5, 30)}.{rng.choice(['10', '90'])}"))
    # a repeat customer with a changed address: same person, second order in another state
    with open(OUT / "olist_customers_dataset.csv", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"])
        w.writerows(customers)
    with open(OUT / "olist_orders_dataset.csv", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
                    "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"])
        w.writerows(orders)
    with open(OUT / "olist_order_items_dataset.csv", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"])
        w.writerows(items)
    print(f"{len(orders)} orders, {len(items)} items, {len(products)} products")


if __name__ == "__main__":
    main()
