"""Reconciliation between source expectation, raw events, facts and aggregates.

Runs against the `work` schema before anything is published. Any 'fail' blocks publication.
The source expectation (inventory.json) is computed from the source CSVs by a separate code
path from the one that builds events, so events lost before they were ever sent are visible.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path

import duckdb

from .contracts import SOURCE_EVENT_TYPES


@dataclass
class Check:
    name: str
    status: str  # ok | warn | fail | skipped
    detail: str = ""


def _ranges(days: list[dt.date]) -> list[tuple[dt.date, dt.date]]:
    out: list[tuple[dt.date, dt.date]] = []
    for d in sorted(set(days)):
        if out and (d - out[-1][1]).days == 1:
            out[-1] = (out[-1][0], d)
        else:
            out.append((d, d))
    return out


def backfill_hint(days: list[dt.date]) -> str:
    return "; ".join(
        f"orderledger backfill --from {a} --to {b}" for a, b in _ranges(days)
    )


PURCHASE_DIFF = """
select distinct purchase_date from (
  (select * from work.recon_agg_daily_purchases except select * from work.agg_daily_purchases)
  union all
  (select * from work.agg_daily_purchases except select * from work.recon_agg_daily_purchases))"""

DELIVERY_DIFF = """
select distinct delivered_date from (
  (select * from work.recon_agg_daily_deliveries except select * from work.agg_daily_deliveries)
  union all
  (select * from work.agg_daily_deliveries except select * from work.recon_agg_daily_deliveries))"""


def stale_dates(con: duckdb.DuckDBPyConnection) -> tuple[list[dt.date], list[dt.date]]:
    p = [r[0] for r in con.execute(PURCHASE_DIFF).fetchall()]
    d = [r[0] for r in con.execute(DELIVERY_DIFF).fetchall()]
    return sorted(p), sorted(d)


def run_checks(con: duckdb.DuckDBPyConnection, events_dir: Path, complete: bool) -> list[Check]:
    checks: list[Check] = []
    one = lambda sql: con.execute(sql).fetchone()[0]  # noqa: E731

    # 1. Source inventory (independent of event construction). Only decidable at full history.
    inv_path = events_dir / "inventory.json"
    if inv_path.exists():
        inv = json.loads(inv_path.read_text(encoding="utf-8"))
        got = dict(
            con.execute(
                "select event_type, count(distinct event_id) from raw.events "
                "where content_origin = 'source-field' group by 1"
            ).fetchall()
        )
        diffs = {
            t: (inv["event_counts"].get(t, 0), got.get(t, 0))
            for t in SOURCE_EVENT_TYPES
            if inv["event_counts"].get(t, 0) != got.get(t, 0)
        }
        if not complete:
            over = {t: v for t, v in diffs.items() if v[1] > v[0]}
            checks.append(
                Check("source_inventory", "fail" if over else "skipped",
                      f"more events than the source holds: {over}" if over
                      else "partial history: expected counts only decidable once all batches loaded")
            )
        elif diffs:
            checks.append(
                Check("source_inventory", "fail",
                      "missing/extra source events (expected, loaded): "
                      + ", ".join(f"{t}={v}" for t, v in sorted(diffs.items())))
            )
        else:
            checks.append(Check("source_inventory", "ok", "every source event type matches the source CSVs"))

        if complete:
            exp = inv["placed_by_date"]
            got_d = {
                str(d): n
                for d, n in con.execute(
                    "select purchase_date, count(*) from work.fct_orders group by 1"
                ).fetchall()
            }
            bad = sorted(d for d in set(exp) | set(got_d) if exp.get(d, 0) != got_d.get(d, 0))
            checks.append(
                Check("placed_orders_by_date", "fail" if bad else "ok",
                      f"{len(bad)} dates differ from the source, e.g. {bad[:5]}" if bad
                      else f"orders per purchase date match the source on {len(exp)} dates")
            )
    else:
        checks.append(Check("source_inventory", "skipped", "no inventory.json"))

    # 2. Raw -> facts
    placed = one("select count(distinct entity_id) from work.stg_events where event_type = 'order_placed'")
    facts = one("select count(*) from work.fct_orders")
    checks.append(Check("raw_to_fct_orders", "ok" if placed == facts else "fail",
                        f"placed orders in events={placed}, fct_orders={facts}"))
    exp_items, exp_sum = con.execute(
        "select count(*), coalesce(sum(price), 0) from work.stg_order_items "
        "where order_id in (select entity_id from work.stg_events where event_type = 'order_placed')"
    ).fetchone()
    got_items, got_sum = con.execute(
        "select count(*), coalesce(sum(price), 0) from work.fct_order_items"
    ).fetchone()
    checks.append(Check("raw_to_fct_order_items",
                        "ok" if (exp_items, exp_sum) == (got_items, got_sum) else "fail",
                        f"items/price in events=({exp_items}, {exp_sum}), fct_order_items=({got_items}, {got_sum})"))
    orphan_orders = one(
        "select count(distinct entity_id) from work.stg_events where entity_type = 'order' "
        "and entity_id not in (select entity_id from work.stg_events where event_type = 'order_placed')"
    )
    orphan_items = one(
        "select count(*) from work.stg_order_items where order_id not in "
        "(select entity_id from work.stg_events where event_type = 'order_placed')"
    )
    checks.append(Check("orphans", "warn" if (orphan_orders or orphan_items) else "ok",
                        f"orders without order_placed={orphan_orders}, items without a placed order={orphan_items} "
                        "(waiting for late data; not published until the order arrives)"))

    # 3. Facts -> marts (a late change outside the lookback leaves a stale date)
    p_stale, d_stale = stale_dates(con)
    if p_stale or d_stale:
        hint = backfill_hint(p_stale + d_stale)
        checks.append(Check("facts_to_marts", "fail",
                            f"stale aggregate dates: purchases={[str(x) for x in p_stale]} "
                            f"deliveries={[str(x) for x in d_stale]}. Fix with: {hint}"))
    else:
        checks.append(Check("facts_to_marts", "ok", "incremental aggregates equal a full recomputation"))

    # 4. Informational data-quality counts
    conflicts = one("select count(*) from work.fct_orders where observed_status = 'canceled' "
                    "and delivered_ts is not null and not is_deleted")
    invalid = one("select count(*) from work.fct_orders where not is_sequence_valid and not is_deleted")
    checks.append(Check("data_quality_notes", "ok",
                        f"canceled orders carrying a delivery time={conflicts}; "
                        f"orders with inconsistent timestamp order (excluded from delivery metrics)={invalid}"))
    return checks
