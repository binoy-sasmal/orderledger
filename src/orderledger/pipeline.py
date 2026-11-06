"""Run orchestration: load -> dbt facts -> dbt marts -> dbt cross tests -> reconcile -> publish.

Write-audit-publish on DuckDB: dbt builds into schema `work` (its incremental state lives
there). Only a run whose tests AND reconciliation pass copies `work` into `published` and
advances the watermark, together, in ONE transaction. A failed run leaves `published` and
the watermark untouched, so a retry recomputes exactly the same touched orders.

This single-transaction publish relies on DuckDB. It is not portable: see docs/guarantees.md.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from .db import PUBLISHED_TABLES, connect, db_path
from .load import LoadError, load_batches
from .reconcile import Check, run_checks

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"
STEPS = ("load", "facts", "marts", "tests", "recon", "publish")


class PipelineError(Exception):
    def __init__(self, step: str, message: str):
        super().__init__(f"[{step}] {message}")
        self.step = step
        self.message = message


class InjectedFailure(PipelineError):
    """Raised on purpose by tests and the demo to simulate a crash after a step."""


@dataclass
class RunOutcome:
    run_id: str
    status: str
    checkpoint_date: dt.date | None = None
    batches_loaded: int = 0
    rows_loaded: int = 0
    watermark_before: int = 0
    watermark_after: int = 0
    duration_s: float = 0.0
    checks: list[Check] = field(default_factory=list)


def run_dbt(ws: Path, command: str, select: str, variables: dict) -> None:
    """Run dbt in a subprocess. dbt-duckdb keeps the database file open for the life of its
    process, which would block this process's own connections, so each phase gets its own."""
    env = {**os.environ, "ORDERLEDGER_DB": str(db_path(ws)), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    args = [
        sys.executable, "-c", "import sys; from dbt.cli.main import cli; cli(sys.argv[1:])",
        "--quiet", "--no-use-colors",
        command,
        "--project-dir", str(DBT_DIR),
        "--profiles-dir", str(DBT_DIR),
        "--target-path", str(ws / "dbt_target"),
        "--log-path", str(ws / "dbt_logs"),
        "--select", select,
        "--vars", json.dumps(variables),
    ]
    if command == "build":
        args += ["--indirect-selection", "cautious"]
    res = subprocess.run(args, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if res.returncode == 0:
        return
    text = (res.stdout or "") + (res.stderr or "")
    keep = [ln.strip() for ln in text.splitlines()
            if re.search(r"Failure in|Error|FAIL|ERROR|Got \d+ result", ln)]
    raise PipelineError("dbt", " | ".join(keep[:12]) or text[-800:])


def auto_backfill_window(ws: Path) -> tuple[dt.date, dt.date] | None:
    """The span of dates reconciliation currently flags as stale in the build area, if any."""
    from .reconcile import stale_dates

    con = connect(ws, read_only=True)
    try:
        purchases, deliveries = stale_dates(con)
    finally:
        con.close()
    days = purchases + deliveries
    return (min(days), max(days)) if days else None


def _inject(fail_after: str | None, step: str) -> None:
    if fail_after == step:
        raise InjectedFailure(step, "injected failure (simulated crash)")


def run_pipeline(
    ws: Path,
    through: int | None = None,
    lookback_days: int = 7,
    kind: str = "run",
    force: tuple[dt.date, dt.date] | None = None,
    fail_after: str | None = None,
) -> RunOutcome:
    started = time.monotonic()
    run_id = uuid.uuid4().hex[:12]
    con = connect(ws)
    wm_before, _cp, _r, _p = con.execute("select watermark, checkpoint_date, last_run_id, published_at "
                                         "from ctl.state").fetchone()
    con.execute(
        "insert into ctl.run_log (run_id, kind, started_at, status, through_batch, watermark_before) "
        "values (?, ?, now()::timestamp, 'running', ?, ?)",
        [run_id, kind, through, wm_before],
    )
    out = RunOutcome(run_id=run_id, status="running", watermark_before=wm_before)
    step = "load"
    try:
        if kind == "backfill":
            # A backfill reprocesses history that is already loaded; it never ingests new batches.
            through = con.execute("select coalesce(max(batch_no), 0) from raw.load_log").fetchone()[0]
        try:
            lr = load_batches(con, ws / "events", through)
        except LoadError as e:
            raise PipelineError("load", str(e)) from e
        if lr.checkpoint_date is None:
            raise PipelineError("load", "no batches loaded")
        out.checkpoint_date, out.batches_loaded, out.rows_loaded = lr.checkpoint_date, lr.batches_loaded, lr.rows_loaded
        con.close()
        _inject(fail_after, "load")

        variables = {
            "watermark": wm_before,
            "checkpoint_date": str(lr.checkpoint_date),
            "lookback_days": lookback_days,
            "force_from": str(force[0]) if force else None,
            "force_to": str(force[1]) if force else None,
        }
        step = "facts"
        run_dbt(ws, "build", "tag:facts", variables)
        _inject(fail_after, "facts")
        step = "marts"
        run_dbt(ws, "build", "tag:marts", variables)
        _inject(fail_after, "marts")
        step = "tests"
        run_dbt(ws, "test", "tag:cross", variables)
        _inject(fail_after, "tests")

        step = "recon"
        con = connect(ws)
        out.checks = run_checks(con, ws / "events", lr.complete)
        con.execute("delete from ctl.reconciliation where run_id = ?", [run_id])
        for c in out.checks:
            con.execute("insert into ctl.reconciliation values (?, ?, ?, ?, now()::timestamp)",
                        [run_id, c.name, c.status, c.detail])
        failed = [c for c in out.checks if c.status == "fail"]
        if failed:
            raise PipelineError("recon", "; ".join(f"{c.name}: {c.detail}" for c in failed))
        _inject(fail_after, "recon")

        step = "publish"
        wm_after = con.execute("select coalesce(max(arrival_seq), 0) from raw.events").fetchone()[0]
        con.execute("begin")
        try:
            for t in PUBLISHED_TABLES:
                con.execute(f"create or replace table published.{t} as select * from work.{t}")
            _inject(fail_after, "publish")  # crash before commit: nothing becomes visible
            con.execute(
                "update ctl.state set watermark = ?, checkpoint_date = ?, last_run_id = ?, "
                "published_at = now()::timestamp where id = 1",
                [wm_after, lr.checkpoint_date, run_id],
            )
            con.execute("commit")
        except Exception:
            con.execute("rollback")
            raise
        out.watermark_after = wm_after
        out.status = "success"
    except PipelineError as e:
        out.status = "failed"
        _close(con)
        _finish(ws, run_id, out, started, e)
        raise
    except Exception as e:  # unexpected: still leave an auditable record
        out.status = "failed"
        _close(con)
        _finish(ws, run_id, out, started, PipelineError(step, repr(e)))
        raise
    _close(con)
    _finish(ws, run_id, out, started, None)
    return out


def _close(con) -> None:
    try:
        con.close()
    except Exception:  # already closed
        pass


def _finish(ws: Path, run_id: str, out: RunOutcome, started: float, err: PipelineError | None) -> None:
    out.duration_s = round(time.monotonic() - started, 3)
    try:
        con = connect(ws)
    except Exception:  # pragma: no cover - a held lock; the 'running' row stays as evidence
        return
    con.execute(
        "update ctl.run_log set finished_at = now()::timestamp, status = ?, failed_step = ?, error = ?, "
        "checkpoint_date = ?, batches_loaded = ?, rows_loaded = ?, watermark_after = ?, duration_s = ? "
        "where run_id = ?",
        [out.status, err.step if err else None, err.message[:2000] if err else None,
         out.checkpoint_date, out.batches_loaded, out.rows_loaded,
         out.watermark_after or None, out.duration_s, run_id],
    )
    con.close()
