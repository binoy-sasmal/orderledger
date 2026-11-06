"""Narrow reference implementation of the headline metrics, in plain Python.

It reads the event Parquet files and applies the documented rules with dicts and loops, no
SQL and no dbt. It covers only: orders placed and GMV by purchase date, delivered orders
and on-time counts by delivery date. It is a cross-check, not independent verification:
the same author wrote it and the warehouse, from the same written rules (docs/metrics.md).
"""

from __future__ import annotations

import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import duckdb

CENT = Decimal("0.01")


def reference_metrics(events_dir: Path, through_batch: int | None = None) -> dict:
    con = duckdb.connect()
    files = sorted(events_dir.glob("batch_*.parquet"))
    if through_batch is not None:
        files = files[:through_batch]
    rows = []
    for f in files:
        rows += con.execute(
            f"select event_id, entity_type, entity_id, event_type, source_seq, payload "
            f"from read_parquet('{f.as_posix()}')"
        ).fetchall()

    seen: set[str] = set()
    current: dict[tuple[str, str], tuple[int, dict]] = {}  # (entity_id, event_type) -> latest
    item_price: dict[str, tuple[int, Decimal]] = {}
    item_order: dict[str, str] = {}
    for event_id, entity_type, entity_id, event_type, seq, payload in rows:
        if event_id in seen:
            continue
        seen.add(event_id)
        body = json.loads(payload)
        if entity_type == "order":
            key = (entity_id, event_type)
            if key not in current or seq > current[key][0]:
                current[key] = (seq, body)
        elif entity_type == "order_item" and event_type in ("order_item_added", "order_item_price_corrected"):
            price = Decimal(str(body["price"])).quantize(CENT)
            if entity_id not in item_price or seq > item_price[entity_id][0]:
                item_price[entity_id] = (seq, price)
            if event_type == "order_item_added":
                item_order[entity_id] = body["order_id"]

    def ts(order: str, etype: str, field: str):
        hit = current.get((order, etype))
        return None if hit is None or hit[1].get(field) is None else hit[1][field]

    placed_orders = {o for (o, t) in current if t == "order_placed"}
    items_by_order: dict[str, Decimal] = defaultdict(Decimal)
    for item, order in item_order.items():
        if order in placed_orders:
            items_by_order[order] += item_price[item][1]

    orders: dict[str, int] = defaultdict(int)
    gmv: dict[str, Decimal] = defaultdict(Decimal)
    delivered: dict[str, int] = defaultdict(int)
    on_time: dict[str, int] = defaultdict(int)
    for o in placed_orders:
        if (o, "order_deleted") in current:
            continue
        purchase = ts(o, "order_placed", "purchase_ts")
        orders[purchase[:10]] += 1
        gmv[purchase[:10]] += items_by_order.get(o, Decimal(0))
        stages = [purchase, ts(o, "order_approved", "ts"), ts(o, "order_handed_to_carrier", "ts")]
        deliv = ts(o, "order_delivered", "ts")
        if deliv is None:
            continue
        earlier = [s for s in stages if s is not None]
        if any(deliv < s for s in earlier) or any(b < a for a, b in zip(earlier, earlier[1:], strict=False)):
            continue  # inconsistent timestamp order: excluded from delivery metrics
        delivered[deliv[:10]] += 1
        est = ts(o, "order_placed", "estimated_delivery_ts")
        if est is not None and deliv[:10] <= est[:10]:
            on_time[deliv[:10]] += 1
    return {"orders": dict(orders), "gmv": dict(gmv), "delivered": dict(delivered), "on_time": dict(on_time)}
