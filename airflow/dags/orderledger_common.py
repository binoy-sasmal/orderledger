"""Shared helpers for the OrderLedger DAGs.

Airflow coordinates; it does not contain business logic. Every task shells out to the existing
`orderledger` CLI, which owns loading, dbt, reconciliation, publishing and locking. The CLI's
exit code tells Airflow whether a retry can help:

    0   success
    75  retryable (workspace lock busy, crash, infrastructure error): Airflow retries
    1   deterministic failure (contract, test or reconciliation): NOT retried, stays visible
    2   freshness breach: NOT retried, stays visible
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, "/opt/orderledger/src")  # stdlib-only helpers shared with the repository tests

from airflow.sdk.exceptions import AirflowFailException  # noqa: E402

from orderledger.orchestration import EXIT_OK, EXIT_RETRYABLE  # noqa: E402

POOL = "orderledger_warehouse"  # one slot: a single writer for the DuckDB workspace
WS = Path(os.environ.get("ORDERLEDGER_WS", "/data/ws"))
CLI = os.environ.get("ORDERLEDGER_CLI", "/opt/orderledger-venv/bin/orderledger")
CSV_DIR = os.environ.get("ORDERLEDGER_CSV_DIR", "/opt/orderledger/fixtures/synth")
SCENARIO = os.environ.get("ORDERLEDGER_SCENARIO", "/opt/orderledger/scenarios/airflow-demo.yml")


def replay_anchor() -> datetime:
    """Start of the first scheduled interval. Interval k replays batch k+1; the wall-clock date
    never selects a business date."""
    raw = os.environ.get("ORDERLEDGER_REPLAY_START")
    if not raw:
        raise RuntimeError("ORDERLEDGER_REPLAY_START is not set; run `python airflow/setup_env.py`")
    anchor = datetime.fromisoformat(raw)
    return anchor if anchor.tzinfo else anchor.replace(tzinfo=UTC)


def replay_period() -> timedelta:
    return timedelta(seconds=int(os.environ.get("ORDERLEDGER_REPLAY_PERIOD_SECONDS", "180")))


def run_cli(*args: str) -> int:
    """Run the CLI, copy its output into the task log, return its exit code."""
    print("$ orderledger " + " ".join(args), flush=True)
    res = subprocess.run([CLI, *args], capture_output=True, text=True, encoding="utf-8", errors="replace")
    if res.stdout:
        print(res.stdout, flush=True)
    if res.stderr:
        print(res.stderr, file=sys.stderr, flush=True)
    print(f"(exit code {res.returncode})", flush=True)
    return res.returncode


def raise_for_exit(code: int, what: str) -> None:
    if code == EXIT_OK:
        return
    if code == EXIT_RETRYABLE:
        # An ordinary exception: Airflow applies the task's retry policy.
        raise RuntimeError(f"{what}: retryable failure (exit {code}); Airflow will retry if attempts remain")
    # Deterministic: failing without retries keeps the data problem visible instead of masking it.
    raise AirflowFailException(f"{what}: deterministic failure (exit {code}); not retried, see the log above")
