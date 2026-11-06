"""Deterministic replay simulator: turns static Olist CSVs into a batched change history.

What is real and what is not (see docs/contracts.md):
  * Content and effective timestamps of source-origin events are Olist fields.
  * Arrival time and order are ALWAYS simulated: Olist has no arrival data.
  * Olist has no change history. Duplicates, late arrivals, corrections, deletions and
    out-of-order arrivals exist only because a scenario file asks for them; every such
    event carries content_origin='synthetic-perturbation' and a scenario_tag.
  * order_status is a snapshot with no timestamp. It is emitted once, in the final batch,
    with effective_ts NULL. No cancellation time is invented.

Same CSVs + same scenario => the same logical events (seeded hashes, no randomness).
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import duckdb
import yaml

from .contracts import SCHEMA_VERSION
from .inventory import source_inventory

CSV = {
    "orders": "olist_orders_dataset.csv",
    "items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "translation": "product_category_name_translation.csv",
}

DEFAULT_SCENARIO: dict = {
    "seed": 7,
    "batch_days": 7,
    "duplicates": {"rate": 0.0, "delay_days": 2},
    "late_inside": {"count": 0, "delay_days": 4},
    "late_outside": {"count": 0, "delay_days": 25},
    "price_corrections": {"count": 0, "delay_days": 3},
    "delivery_corrections": {"count": 0, "shift_days": 2, "delay_days": 25},
    "deletions": {"count": 0, "delay_days": 5},
    "out_of_order": {"count": 0, "original_delay_days": 10},
    "drop_source_events": {"count": 0},  # test-only fault: events the source never sent
}

MILESTONES = [
    ("order_approved", "approved", "order_approved_at", 2),
    ("order_handed_to_carrier", "carrier", "order_delivered_carrier_date", 3),
    ("order_delivered", "delivered", "order_delivered_customer_date", 4),
]

EV_DDL = """
create or replace temp table ev (
  event_id varchar, entity_type varchar, entity_id varchar, event_type varchar,
  source_seq bigint, effective_ts timestamp, arrival_ts timestamp, payload varchar,
  content_origin varchar, timing_origin varchar, scenario_tag varchar, is_dup boolean)
"""


def load_scenario(path: Path | None) -> dict:
    sc = json.loads(json.dumps(DEFAULT_SCENARIO))
    if path is not None:
        user = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for k, v in user.items():
            if isinstance(v, dict) and isinstance(sc.get(k), dict):
                sc[k].update(v)
            else:
                sc[k] = v
    return sc


def _delay(expr: str) -> str:
    return f"to_hours(cast(hash({expr}) % 36 as integer))"


def _pick(con, seed: int, tag: str, n: int, source_sql: str, key: str) -> list[str]:
    """Deterministically choose n keys not chosen by an earlier perturbation."""
    if n <= 0:
        return []
    rows = con.execute(
        f"select {key} from ({source_sql}) s where {key} not in (select k from chosen) "
        f"order by hash({key} || '|{seed}|{tag}') limit {int(n)}"
    ).fetchall()
    keys = [r[0] for r in rows]
    con.executemany("insert into chosen values (?, ?)", [(k, tag) for k in keys])
    return keys


def _build_events(con, sc: dict) -> None:
    seed = int(sc["seed"])
    con.execute(EV_DDL)
    con.execute("create or replace temp table chosen (k varchar, tag varchar)")

    con.execute(f"""
      insert into ev
      select 'ord:'||o.order_id||':placed', 'order', o.order_id, 'order_placed', 1,
        o.order_purchase_timestamp,
        o.order_purchase_timestamp + {_delay("'ord:'||o.order_id||':placed'")},
        json_object('customer_id', o.customer_id, 'customer_unique_id', c.customer_unique_id,
                    'customer_state', c.customer_state, 'customer_city', c.customer_city,
                    'purchase_ts', o.order_purchase_timestamp,
                    'estimated_delivery_ts', o.order_estimated_delivery_date)::varchar,
        'source-field', 'source-timestamp', null, false
      from orders o left join customers c using (customer_id)
      where o.order_purchase_timestamp is not null""")

    for etype, suffix, col, seq in MILESTONES:
        con.execute(f"""
          insert into ev
          select 'ord:'||order_id||':{suffix}', 'order', order_id, '{etype}', {seq}, {col},
            {col} + {_delay(f"'ord:'||order_id||':{suffix}'")},
            json_object('ts', {col})::varchar, 'source-field', 'source-timestamp', null, false
          from orders where {col} is not null""")

    con.execute(f"""
      insert into ev
      select 'item:'||i.order_id||'#'||i.order_item_id||':added', 'order_item',
        i.order_id||'#'||i.order_item_id, 'order_item_added', 1,
        o.order_purchase_timestamp,
        o.order_purchase_timestamp + {_delay("'item:'||i.order_id||'#'||i.order_item_id||':added'")},
        json_object('order_id', i.order_id, 'item_no', i.order_item_id, 'product_id', i.product_id,
                    'seller_id', i.seller_id, 'price', i.price, 'freight', i.freight_value)::varchar,
        'source-field', 'source-timestamp', null, false
      from items i join orders o using (order_id)
      where o.order_purchase_timestamp is not null""")

    # ---- synthetic perturbations (each labelled) -------------------------------------------
    delivered = "select entity_id as oid from ev where event_type = 'order_delivered'"
    items = "select entity_id as iid from ev where event_type = 'order_item_added'"
    placed = "select entity_id as oid from ev where event_type = 'order_placed'"

    def shift_delivered(tag: str, spec: dict) -> None:
        keys = _pick(con, seed, tag, spec["count"], delivered, "oid")
        if keys:
            con.execute(
                f"update ev set arrival_ts = arrival_ts + to_days({int(spec['delay_days'])}), "
                f"scenario_tag = '{tag}' where event_type = 'order_delivered' and entity_id in "
                f"(select k from chosen where tag = '{tag}')"
            )

    shift_delivered("late_inside_lookback", sc["late_inside"])
    shift_delivered("late_outside_lookback", sc["late_outside"])

    spec = sc["price_corrections"]
    keys = _pick(con, seed, "price_correction", spec["count"], items, "iid")
    if keys:
        con.execute(f"""
          insert into ev
          select 'item:'||e.entity_id||':price:c1', 'order_item', e.entity_id,
            'order_item_price_corrected', 2, e.effective_ts,
            e.arrival_ts + to_days({int(spec["delay_days"])}),
            json_object('order_id', json_extract_string(e.payload, '$.order_id'),
                        'item_no', json_extract(e.payload, '$.item_no'),
                        'price', round(cast(json_extract_string(e.payload, '$.price') as double) * 1.1, 2)
                       )::varchar,
            'synthetic-perturbation', 'synthetic', 'price_correction', false
          from ev e where e.event_type = 'order_item_added'
            and e.entity_id in (select k from chosen where tag = 'price_correction')""")

    spec = sc["delivery_corrections"]
    keys = _pick(con, seed, "delivery_correction", spec["count"], delivered, "oid")
    if keys:
        con.execute(f"""
          insert into ev
          select 'ord:'||e.entity_id||':delivered:c1', 'order', e.entity_id, 'order_delivered', 5,
            cast(json_extract_string(e.payload, '$.ts') as timestamp) + to_days({int(spec["shift_days"])}),
            e.arrival_ts + to_days({int(spec["delay_days"])}),
            json_object('ts', cast(json_extract_string(e.payload, '$.ts') as timestamp)
                              + to_days({int(spec["shift_days"])}))::varchar,
            'synthetic-perturbation', 'synthetic', 'delivery_correction', false
          from ev e where e.event_type = 'order_delivered' and e.scenario_tag is not distinct from null
            and e.entity_id in (select k from chosen where tag = 'delivery_correction')""")

    spec = sc["deletions"]
    keys = _pick(con, seed, "deletion", spec["count"], placed, "oid")
    if keys:
        con.execute(f"""
          insert into ev
          select 'ord:'||e.entity_id||':deleted', 'order', e.entity_id, 'order_deleted', 20, null,
            e.arrival_ts + to_days({int(spec["delay_days"])}),
            json_object('reason', 'synthetic deletion')::varchar,
            'synthetic-perturbation', 'not-applicable', 'deletion', false
          from ev e where e.event_type = 'order_placed'
            and e.entity_id in (select k from chosen where tag = 'deletion')""")

    spec = sc["out_of_order"]
    keys = _pick(con, seed, "out_of_order", spec["count"], items, "iid")
    if keys:
        # The correction (source_seq 2) arrives BEFORE the item it corrects (source_seq 1).
        con.execute("""
          insert into ev
          select 'item:'||e.entity_id||':price:c1', 'order_item', e.entity_id,
            'order_item_price_corrected', 2, e.effective_ts, e.effective_ts,
            json_object('order_id', json_extract_string(e.payload, '$.order_id'),
                        'item_no', json_extract(e.payload, '$.item_no'),
                        'price', round(cast(json_extract_string(e.payload, '$.price') as double) * 1.1, 2)
                       )::varchar,
            'synthetic-perturbation', 'synthetic', 'out_of_order', false
          from ev e where e.event_type = 'order_item_added'
            and e.entity_id in (select k from chosen where tag = 'out_of_order')""")
        con.execute(
            f"update ev set arrival_ts = arrival_ts + to_days({int(spec['original_delay_days'])}), "
            "scenario_tag = 'out_of_order' where event_type = 'order_item_added' and entity_id in "
            "(select k from chosen where tag = 'out_of_order')"
        )

    spec = sc["drop_source_events"]
    keys = _pick(con, seed, "drop", spec["count"], delivered, "oid")
    if keys:
        con.execute(
            "delete from ev where event_type = 'order_delivered' and event_id like 'ord:%:delivered' "
            "and entity_id in (select k from chosen where tag = 'drop')"
        )

    rate = float(sc["duplicates"]["rate"])
    if rate > 0:
        con.execute(f"""
          insert into ev
          select event_id, entity_type, entity_id, event_type, source_seq, effective_ts,
            arrival_ts + to_days({int(sc["duplicates"]["delay_days"])}), payload,
            content_origin, timing_origin, 'duplicate_delivery', true
          from ev where not is_dup and hash(event_id || '|{seed}|dup') % 10000 < {int(rate * 10000)}""")

    # ---- snapshots without a time of their own ---------------------------------------------
    con.execute("""
      insert into ev
      select 'ord:'||order_id||':status', 'order', order_id, 'order_status_observed', 10, null,
        (select date_trunc('day', max(arrival_ts)) + interval 1 day + interval 12 hour from ev),
        json_object('status', order_status)::varchar,
        'source-field', 'not-applicable', null, false
      from orders where order_purchase_timestamp is not null""")
    con.execute("""
      insert into ev
      select 'prod:'||p.product_id, 'product', p.product_id, 'product_observed', 1, null,
        (select date_trunc('day', min(arrival_ts)) - interval 1 day + interval 12 hour from ev),
        json_object('category', p.product_category_name,
                    'category_en', t.product_category_name_english)::varchar,
        'source-field', 'not-applicable', null, false
      from products p left join translation t using (product_category_name)""")


def simulate(csv_dir: Path, out_dir: Path, scenario: dict) -> dict:
    """Write batched Parquet event files, manifest.json and inventory.json into out_dir."""
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)
    con = duckdb.connect()
    for name, fname in CSV.items():
        con.execute(
            f"create view {name} as select * from read_csv('{(csv_dir / fname).as_posix()}', header=true)"
        )
    _build_events(con, scenario)

    con.execute("""
      create temp table evn as
      select *, row_number() over (order by arrival_ts, event_id, is_dup) as arrival_seq
      from ev""")
    start = con.execute("select min(arrival_ts)::date from evn").fetchone()[0]
    bd = int(scenario["batch_days"])
    con.execute(
        f"create temp table evb as select *, "
        f"(datediff('day', date '{start}', arrival_ts::date) // {bd}) + 1 as batch_no from evn"
    )
    n_batches = con.execute("select max(batch_no) from evb").fetchone()[0]

    cols = (
        "event_id, entity_type, entity_id, event_type, source_seq, effective_ts, arrival_ts, "
        "arrival_seq, payload, content_origin, timing_origin, scenario_tag"
    )
    manifest_batches = []
    for n in range(1, n_batches + 1):
        name = f"batch_{n:05d}.parquet"
        path = out_dir / name
        con.execute(
            f"copy (select {cols} from evb where batch_no = {n} order by arrival_seq) "
            f"to '{path.as_posix()}' (format parquet)"
        )
        rows, max_seq = con.execute(
            f"select count(*), coalesce(max(arrival_seq), 0) from evb where batch_no = {n}"
        ).fetchone()
        manifest_batches.append(
            {
                "batch_no": n,
                "file": name,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "row_count": rows,
                "period_from": str(start + _days(bd * (n - 1))),
                "period_to": str(start + _days(bd * n - 1)),
                "max_arrival_seq": max_seq,
            }
        )
    manifest = {"schema_version": SCHEMA_VERSION, "scenario": scenario, "batches": manifest_batches}
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    # Independent expectation: computed from the CSVs, never from events.
    (out_dir / "inventory.json").write_text(
        json.dumps(source_inventory(csv_dir), indent=1), encoding="utf-8"
    )
    con.close()
    return manifest


def _days(n: int):
    import datetime

    return datetime.timedelta(days=n)
