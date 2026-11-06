"""Shared helpers: build workspaces from the synthetic fixture, replay, snapshot, diff."""

from __future__ import annotations

import copy
import shutil
from pathlib import Path

import duckdb

from orderledger.db import PUBLISHED_TABLES, connect, db_path
from orderledger.load import read_manifest
from orderledger.pipeline import PipelineError, auto_backfill_window, run_pipeline
from orderledger.simulate import load_scenario, simulate

FIXTURE = Path(__file__).parent / "fixtures" / "synth"
SCENARIOS = Path(__file__).resolve().parents[1] / "scenarios"

NO_PERTURBATIONS = {
    "duplicates": {"rate": 0.0},
    "late_inside": {"count": 0},
    "late_outside": {"count": 0},
    "price_corrections": {"count": 0},
    "delivery_corrections": {"count": 0},
    "deletions": {"count": 0},
    "out_of_order": {"count": 0},
    "drop_source_events": {"count": 0},
}


def make_ws(base: Path, name: str, overrides: dict | None = None) -> Path:
    ws = base / name
    sc = load_scenario(SCENARIOS / "ci.yml")
    for k, v in (overrides or {}).items():
        if isinstance(v, dict) and isinstance(sc.get(k), dict):
            sc[k].update(copy.deepcopy(v))
        else:
            sc[k] = v
    simulate(FIXTURE, ws / "events", sc)
    return ws


def snapshot(ws: Path, label: object) -> Path:
    con = connect(ws)
    con.execute("checkpoint")
    con.close()
    dest = ws / f"snap_{label}.duckdb"
    shutil.copy(db_path(ws), dest)
    return dest


def run_batch(ws: Path, n: int, lookback: int = 7) -> str | None:
    """Process through batch n. The only failure tolerated is stale aggregates (the documented
    late-correction case), which is repaired by backfill. Returns the failure text, if any."""
    try:
        run_pipeline(ws, through=n, lookback_days=lookback)
        return None
    except PipelineError as e:
        assert e.step == "recon" and "facts_to_marts" in e.message, f"unexpected failure: {e}"
        window = auto_backfill_window(ws)
        assert window is not None
        run_pipeline(ws, kind="backfill", lookback_days=lookback, force=window)
        return e.message


def build_incremental(ws: Path, start: int = 1, snapshots=(), lookback: int = 7) -> list[tuple[int, str]]:
    """Run batch by batch, repairing stale aggregates by backfill; return the failures seen."""
    failures: list[tuple[int, str]] = []
    n_batches = len(read_manifest(ws / "events")["batches"])
    for n in range(start, n_batches + 1):
        failed = run_batch(ws, n, lookback)
        if failed:
            failures.append((n, failed))
        if n in snapshots:
            snapshot(ws, n)
    return failures


def diff_published(a: Path, b: Path) -> dict[str, int]:
    """Rows that differ (multiset) per published table between two warehouse files."""
    con = duckdb.connect()
    con.execute(f"attach '{a.as_posix()}' as a (read_only)")
    con.execute(f"attach '{b.as_posix()}' as b (read_only)")
    out = {}
    for t in PUBLISHED_TABLES:
        out[t] = con.execute(
            f"select count(*) from ((select * from a.published.{t} except all select * from b.published.{t}) "
            f"union all (select * from b.published.{t} except all select * from a.published.{t}))"
        ).fetchone()[0]
    con.close()
    return out


def rows(ws_or_file: Path, sql: str):
    f = ws_or_file if ws_or_file.suffix == ".duckdb" else db_path(ws_or_file)
    con = duckdb.connect(str(f), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()
