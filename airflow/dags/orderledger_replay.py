"""Scheduled replay of the OrderLedger event batches.

Each scheduled interval replays ONE batch of the historical event feed. Interval k (counted from
the DAG start_date, the replay anchor) processes batch k+1. The wall-clock date is only the time
the scheduler fires; it never selects the dataset's business date.

Airflow catchup here means "create the runs for scheduled intervals that were missed". It is
not the warehouse backfill, which repairs stale aggregate dates (see the orderledger_backfill DAG).
"""

from __future__ import annotations

from datetime import timedelta

from orderledger_common import (
    CSV_DIR,
    POOL,
    SCENARIO,
    WS,
    raise_for_exit,
    replay_anchor,
    replay_period,
    run_cli,
)

from airflow.sdk import Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from orderledger.orchestration import (
    batch_for_interval,
    manifest_batches,
    parse_inject_setting,
    period_to,
)

DOC = __doc__

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "execution_timeout": timedelta(minutes=20),
}


@dag(
    dag_id="orderledger_replay",
    description="Replay one OrderLedger event batch per scheduled interval",
    start_date=replay_anchor(),
    schedule=replay_period(),
    catchup=True,
    # Several runs may be active at once, but batches are still processed strictly in order:
    # `depends_on_past` on run_batch holds a later batch until the earlier one succeeded, and the
    # one-slot pool serializes every warehouse write. max_active_runs=1 would DEADLOCK here: a
    # failed run that is cleared for a rerun is queued behind the later run that is waiting for it.
    max_active_runs=4,
    default_args=DEFAULT_ARGS,
    tags=["orderledger"],
    doc_md=DOC,
)
def orderledger_replay():
    @task(pool=POOL)
    def ensure_events() -> None:
        """Build the replayable event history once (idempotent). Uses the synthetic fixture unless
        ORDERLEDGER_CSV_DIR points at the real Olist files, so no Kaggle login is needed."""
        if (WS / "events" / "manifest.json").exists():
            print("event history already present; nothing to do")
            return
        raise_for_exit(run_cli("simulate", CSV_DIR, "--scenario", SCENARIO, "--ws", str(WS)), "simulate")

    @task
    def plan() -> int:
        """Map this scheduled interval to its replay batch, or skip when there is none."""
        ctx = get_current_context()
        batch_no = batch_for_interval(ctx["data_interval_start"], replay_anchor(), replay_period())
        n_batches = len(manifest_batches(WS))
        print(f"interval starting {ctx['data_interval_start']} -> replay batch {batch_no} of {n_batches}")
        if batch_no < 1 or batch_no > n_batches:
            raise AirflowSkipException(f"no replay batch for this interval (batch {batch_no} of {n_batches})")
        return batch_no

    # depends_on_past: a batch is not processed while the previous batch's run is unresolved, so
    # a data problem stops progress at the batch where it appeared instead of failing every run.
    @task(pool=POOL, depends_on_past=True)
    def run_batch(batch_no: int) -> None:
        """Load, build, test, reconcile and publish through this batch (the existing CLI)."""
        args = ["run", "--ws", str(WS), "--through", str(batch_no)]
        inject = parse_inject_setting(Variable.get("orderledger_inject_transient", default=""))
        if inject and inject[0] == batch_no:
            # Demo hook: crash once after the named step. The marker lives in the workspace, so
            # the retry of this very task finds the failure already spent and succeeds.
            args += ["--inject-failure-once", inject[1], "--inject-token", f"airflow-batch-{batch_no}"]
        raise_for_exit(run_cli(*args), f"run batch {batch_no}")

    @task(pool=POOL)
    def check_freshness(batch_no: int) -> None:
        """Published data must have reached the simulated end date of this batch."""
        expected = period_to(WS, batch_no)
        raise_for_exit(
            run_cli("freshness", "--ws", str(WS), "--expected-through", str(expected)), "freshness"
        )

    built = ensure_events()
    batch = plan()
    built >> batch
    ran = run_batch(batch)
    ran >> check_freshness(batch)


orderledger_replay()
