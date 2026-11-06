"""Targeted warehouse backfill, triggered manually with parameters.

This repairs STALE AGGREGATE DATES that reconciliation named (a late correction that moved an
order off an already-published date). It reprocesses history that is already loaded and never
ingests new batches. It is not Airflow's catchup/backfill of scheduled intervals.

It is never triggered automatically. A reconciliation failure stays failed until a person (or a
deliberately designed automation) decides to run this DAG: silently repairing a failed data
quality check would hide the problem it reports.

Parameters (exactly one mode):
  auto=true                       recompute the dates reconciliation currently flags as stale
  date_from + date_to (YYYY-MM-DD)  recompute that inclusive window
"""

from __future__ import annotations

from datetime import timedelta

from orderledger_common import POOL, WS, raise_for_exit, run_cli

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException
from orderledger.orchestration import ParamError, validate_backfill_params

DOC = __doc__


@dag(
    dag_id="orderledger_backfill",
    description="Repair stale aggregate dates in the OrderLedger warehouse",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "auto": Param(False, type="boolean", description="Recompute the dates reconciliation flags as stale"),
        "date_from": Param("", type="string", description="First date to recompute, YYYY-MM-DD"),
        "date_to": Param("", type="string", description="Last date to recompute, YYYY-MM-DD"),
    },
    tags=["orderledger"],
    doc_md=DOC,
)
def orderledger_backfill():
    @task
    def validate() -> dict:
        """Reject bad parameters before anything touches the warehouse. Deterministic: no retries."""
        p = get_current_context()["params"]
        try:
            return validate_backfill_params(p.get("date_from", ""), p.get("date_to", ""), bool(p.get("auto")))
        except ParamError as e:
            raise AirflowFailException(f"invalid backfill parameters: {e}") from e

    @task(pool=POOL, retries=2, retry_delay=timedelta(seconds=15), execution_timeout=timedelta(minutes=20))
    def repair(window: dict) -> None:
        args = ["backfill", "--ws", str(WS)]
        args += ["--auto"] if window["auto"] else ["--from", window["date_from"], "--to", window["date_to"]]
        raise_for_exit(run_cli(*args), "backfill")

    repair(validate())


orderledger_backfill()
