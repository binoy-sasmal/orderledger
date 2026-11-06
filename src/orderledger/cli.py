"""Command line: simulate, run, backfill, reconcile, status, freshness, profile."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import typer

from .db import connect
from .pipeline import STEPS, PipelineError, auto_backfill_window, run_pipeline
from .reconcile import run_checks

app = typer.Typer(no_args_is_help=True, add_completion=False, help=__doc__)
WS = typer.Option(Path("warehouse/main"), "--ws", help="Workspace folder (events/ and wh.duckdb).")


def _date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


@app.command()
def simulate(
    csv_dir: Path = typer.Argument(..., help="Folder with the Olist CSV files (or the CI fixture)."),
    scenario: Path = typer.Option(None, help="Scenario YAML; defaults to no perturbations."),
    ws: Path = WS,
) -> None:
    """Turn source CSVs into batched, replayable change events (see docs/contracts.md)."""
    from .simulate import load_scenario
    from .simulate import simulate as sim

    manifest = sim(csv_dir, ws / "events", load_scenario(scenario))
    n = sum(b["row_count"] for b in manifest["batches"])
    typer.echo(f"wrote {len(manifest['batches'])} batches, {n} events to {ws / 'events'}")


def _print_run(out) -> None:
    typer.echo(
        f"run {out.run_id} {out.status}: checkpoint={out.checkpoint_date} "
        f"batches_loaded={out.batches_loaded} rows_loaded={out.rows_loaded} "
        f"watermark {out.watermark_before}->{out.watermark_after} in {out.duration_s}s"
    )
    for c in out.checks:
        typer.echo(f"  [{c.status:7}] {c.name}: {c.detail}")


@app.command()
def run(
    ws: Path = WS,
    through: int = typer.Option(None, help="Load batches up to this number (default: all)."),
    lookback_days: int = typer.Option(7, help="Restate aggregate dates this close to the checkpoint."),
    fail_after: str = typer.Option(None, help=f"Test hook: crash after a step ({', '.join(STEPS)})."),
) -> None:
    """Load pending batches, build, test, reconcile and publish. Exits 1 if anything fails."""
    try:
        _print_run(run_pipeline(ws, through, lookback_days, fail_after=fail_after))
    except PipelineError as e:
        typer.echo(f"FAILED {e}", err=True)
        raise typer.Exit(1) from e


@app.command()
def backfill(
    ws: Path = WS,
    date_from: str = typer.Option(None, "--from", help="First date to recompute (inclusive)."),
    date_to: str = typer.Option(None, "--to", help="Last date to recompute (inclusive)."),
    auto: bool = typer.Option(False, help="Recompute the dates reconciliation flags as stale."),
    lookback_days: int = typer.Option(7),
) -> None:
    """Recompute orders and aggregates whose purchase or delivery date is in a window."""
    if auto:
        window = auto_backfill_window(ws)
        if window is None:
            typer.echo("nothing stale")
            return
    elif date_from and date_to:
        window = (_date(date_from), _date(date_to))
    else:
        typer.echo("give --from and --to, or --auto", err=True)
        raise typer.Exit(2)
    typer.echo(f"backfilling {window[0]} .. {window[1]}")
    try:
        _print_run(run_pipeline(ws, None, lookback_days, kind="backfill", force=window))
    except PipelineError as e:
        typer.echo(f"FAILED {e}", err=True)
        raise typer.Exit(1) from e


@app.command()
def reconcile(ws: Path = WS) -> None:
    """Re-run the reconciliation checks against the current build area. Exits 1 on a failure."""
    from .load import read_manifest

    con = connect(ws, read_only=True)
    done = con.execute("select count(*) from raw.load_log").fetchone()[0]
    complete = done == len(read_manifest(ws / "events")["batches"])
    checks = run_checks(con, ws / "events", complete)
    bad = False
    for c in checks:
        typer.echo(f"[{c.status:7}] {c.name}: {c.detail}")
        bad = bad or c.status == "fail"
    raise typer.Exit(1 if bad else 0)


@app.command()
def status(ws: Path = WS) -> None:
    """Published state, recent runs and any quarantined batches."""
    con = connect(ws, read_only=True)
    wm, cp, rid, pub = con.execute("select watermark, checkpoint_date, last_run_id, published_at from ctl.state").fetchone()
    typer.echo(f"published: watermark={wm} checkpoint={cp} run={rid} at={pub}")
    for r in con.execute(
        "select run_id, kind, status, failed_step, checkpoint_date, round(duration_s, 1), left(coalesce(error, ''), 100) "
        "from ctl.run_log order by started_at desc limit 8"
    ).fetchall():
        typer.echo("  run " + " | ".join(str(x) for x in r))
    for r in con.execute("select batch_no, reason from raw.quarantine order by quarantined_at desc limit 5").fetchall():
        typer.echo(f"  QUARANTINED batch {r[0]}: {r[1]}")


@app.command()
def freshness(
    ws: Path = WS,
    expected_through: str = typer.Option(None, help="Simulated source date the published data must reach."),
    max_age_hours: float = typer.Option(None, help="Real-time age limit of the last successful publish."),
) -> None:
    """Two separate checks. Source freshness is simulated time (checkpoint vs the expected date).
    Execution freshness is real time (age of the last successful publish). Exits 2 on a breach."""
    con = connect(ws, read_only=True)
    cp, pub = con.execute("select checkpoint_date, published_at from ctl.state").fetchone()
    bad = False
    if cp is None:
        typer.echo("nothing published yet")
        raise typer.Exit(2)
    typer.echo(f"source (simulated): published through {cp}")
    if expected_through and cp < _date(expected_through):
        typer.echo(f"  STALE: expected through {expected_through}", err=True)
        bad = True
    age = (dt.datetime.now() - pub).total_seconds() / 3600
    typer.echo(f"execution (real): last publish {age:.2f} h ago")
    if max_age_hours is not None and age > max_age_hours:
        typer.echo(f"  STALE: older than {max_age_hours} h", err=True)
        bad = True
    raise typer.Exit(2 if bad else 0)


@app.command()
def profile(csv_dir: Path = typer.Argument(...), out: Path = typer.Option(None)) -> None:
    """Profile the source CSVs (row counts, null rates, contradictions, repeat customers)."""
    from .profile import profile_markdown

    md = profile_markdown(csv_dir)
    if out:
        out.write_text(md, encoding="utf-8")
        typer.echo(f"wrote {out}")
    else:
        typer.echo(md)


EXPECTED_BYTES = {  # sizes reported by Kaggle's dataset metadata API on 2026-11-06 (version 2)
    "olist_orders_dataset.csv": 17654914,
    "olist_order_items_dataset.csv": 15438671,
    "olist_customers_dataset.csv": 9033957,
    "olist_products_dataset.csv": 2379446,
    "product_category_name_translation.csv": 2613,
}


@app.command("check-data")
def check_data(csv_dir: Path = typer.Argument(Path("data/olist"))) -> None:
    """Verify the downloaded Olist files (presence, size) and print SHA-256 to record."""
    import hashlib

    bad = False
    for name, size in EXPECTED_BYTES.items():
        f = csv_dir / name
        if not f.exists():
            typer.echo(f"MISSING  {name}")
            bad = True
            continue
        got = f.stat().st_size
        sha = hashlib.sha256(f.read_bytes()).hexdigest()
        flag = "ok" if got == size else f"SIZE {got} != expected {size}"
        bad = bad or got != size
        typer.echo(f"{flag:8} {name}  sha256={sha}")
    raise typer.Exit(1 if bad else 0)
