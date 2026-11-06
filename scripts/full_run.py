"""Full-data evidence run on the real Olist CSVs. Writes warehouse/evidence.json.

    uv run python scripts/full_run.py data/olist

Builds (1) an incremental 14-day run with backfills where reconciliation requires them,
(2) a single-pass full replay of the same events, (3) an incremental 28-day run (different
batch boundaries), (4) a no-synthetic-events replay checked against a direct CSV computation.
Everything is recorded as measured; nothing is estimated.
"""

from __future__ import annotations

import json
import shutil
import statistics
import sys
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))

from helpers import NO_PERTURBATIONS, diff_published  # noqa: E402
from orderledger.db import db_path  # noqa: E402
from orderledger.pipeline import PipelineError, auto_backfill_window, run_pipeline  # noqa: E402
from orderledger.reference import csv_metrics, published_metrics, reference_metrics  # noqa: E402
from orderledger.simulate import load_scenario, simulate  # noqa: E402

csv_dir = Path(sys.argv[1] if len(sys.argv) > 1 else ROOT / "data/olist")
OUT = ROOT / "warehouse"
ev: dict = {"csv_dir": str(csv_dir)}


def build(name: str, overrides: dict, incremental: bool) -> tuple[Path, dict]:
    ws = OUT / name
    if ws.exists():
        shutil.rmtree(ws)
    sc = load_scenario(ROOT / "scenarios/full.yml")
    for k, v in overrides.items():
        sc[k] = {**sc[k], **v} if isinstance(v, dict) and isinstance(sc.get(k), dict) else v
    t0 = time.monotonic()
    manifest = simulate(csv_dir, ws / "events", sc)
    sim_s = time.monotonic() - t0
    n_batches = len(manifest["batches"])
    info = {
        "batches": n_batches,
        "events": sum(b["row_count"] for b in manifest["batches"]),
        "simulate_seconds": round(sim_s, 1),
        "runs": [],
        "recon_failures": [],
    }
    t0 = time.monotonic()
    if incremental:
        for n in range(1, n_batches + 1):
            r0 = time.monotonic()
            try:
                run_pipeline(ws, through=n)
            except PipelineError as e:
                assert e.step == "recon" and "facts_to_marts" in e.message, e
                w = auto_backfill_window(ws)
                info["recon_failures"].append({"batch": n, "window": [str(w[0]), str(w[1])]})
                run_pipeline(ws, kind="backfill", force=w)
            info["runs"].append(round(time.monotonic() - r0, 1))
            print(name, n, "/", n_batches, info["runs"][-1], "s", flush=True)
    else:
        run_pipeline(ws)
        info["runs"].append(round(time.monotonic() - t0, 1))
    info["total_seconds"] = round(time.monotonic() - t0, 1)
    info["median_run_seconds"] = round(statistics.median(info["runs"]), 1)
    return ws, info


inc14, ev["incremental_14d"] = build("full14", {"batch_days": 14}, True)
replay, ev["full_replay_14d"] = build("fullreplay", {"batch_days": 14}, False)
inc28, ev["incremental_28d"] = build("full28", {"batch_days": 28}, True)

ev["diff_incremental_vs_replay"] = diff_published(db_path(inc14), db_path(replay))
ev["diff_14d_vs_28d"] = diff_published(db_path(inc14), db_path(inc28))

con = duckdb.connect(str(db_path(inc14)), read_only=True)
one = lambda s: con.execute(s).fetchone()[0]  # noqa: E731
ev["raw_events_rows"] = one("select count(*) from raw.events")
ev["raw_events_distinct"] = one("select count(distinct event_id) from raw.events")
ev["events_by_type"] = dict(con.execute("select event_type, count(*) from raw.events group by 1 order by 1").fetchall())
ev["synthetic_events_by_tag"] = dict(
    con.execute(
        "select coalesce(scenario_tag,'-'), count(*) from raw.events "
        "where scenario_tag is not null group by 1 order by 1"
    ).fetchall()
)
ev["published_rows"] = {
    t: one(f"select count(*) from published.{t}")
    for t in ["fct_orders", "fct_order_items", "order_state_intervals", "dim_customer",
              "dim_product", "agg_daily_purchases", "agg_daily_deliveries"]
}
ev["quality_notes"] = {
    "orders_excluded_invalid_sequence": one(
        "select count(*) from published.fct_orders where not is_sequence_valid and not is_deleted"),
    "canceled_with_delivery_time": one(
        "select count(*) from published.fct_orders where observed_status='canceled' "
        "and delivered_ts is not null and not is_deleted"),
}
con.close()

ref = reference_metrics(inc14 / "events")
got = published_metrics(db_path(inc14))
ref["gmv"] = {k: v for k, v in ref["gmv"].items() if v}
ev["python_reference_matches"] = {k: ref[k] == got[k] for k in ref}

clean, ev["clean_replay"] = build("fullclean", NO_PERTURBATIONS, False)
exp = csv_metrics(csv_dir)
gotc = published_metrics(db_path(clean))
ev["direct_csv_matches"] = {k: exp[k] == gotc[k] for k in exp}

(OUT / "evidence.json").write_text(json.dumps(ev, indent=1, default=str), encoding="utf-8")
print(json.dumps(ev, indent=1, default=str))
