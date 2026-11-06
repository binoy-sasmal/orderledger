"""DAG loading and structure checks. Run inside the Airflow image (see docs/orchestration.md):

    docker compose -f airflow/docker-compose.yaml run --rm --no-deps airflow-scheduler \
        python /opt/airflow/check_dags.py

Fails (non-zero exit) if a DAG does not import, or if the properties the orchestration relies on
(single-writer pool, retry policy, ordering, parameters) are missing.
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

os.environ.setdefault("ORDERLEDGER_REPLAY_START", "2026-01-01T00:00:00+00:00")
sys.path.insert(0, "/opt/airflow/dags")  # the DAG processor puts the DAG folder on the path too

from airflow.models.dagbag import DagBag  # noqa: E402

bag = DagBag(dag_folder="/opt/airflow/dags", bundle_path=Path("/opt/airflow/dags"), bundle_name="dags-folder")
problems: list[str] = []


def check(cond: bool, msg: str) -> None:
    if not cond:
        problems.append(msg)


check(not bag.import_errors, f"import errors: {bag.import_errors}")
replay, backfill = bag.dags.get("orderledger_replay"), bag.dags.get("orderledger_backfill")
check(replay is not None and backfill is not None, f"missing DAGs, found {sorted(bag.dags)}")

if replay is not None:
    check(set(replay.task_dict) == {"ensure_events", "plan", "run_batch", "check_freshness"},
          f"replay tasks: {sorted(replay.task_dict)}")
    check(replay.max_active_runs == 4, "max_active_runs=1 deadlocks with depends_on_past when a failed run is cleared")
    check(replay.catchup is True, "replay uses catchup to create runs for missed scheduled intervals")
    rb = replay.task_dict.get("run_batch")
    if rb is not None:
        check(rb.pool == "orderledger_warehouse", "run_batch must use the single-writer pool")
        check(rb.retries == 2 and rb.retry_delay == timedelta(seconds=15), "run_batch retry policy")
        check(rb.depends_on_past is True, "run_batch must not skip past an unresolved batch")
    for name in ("ensure_events", "run_batch", "check_freshness"):
        t = replay.task_dict.get(name)
        check(t is not None and t.pool == "orderledger_warehouse", f"{name} must use the writer pool")
    check("run_batch" in replay.task_dict["plan"].downstream_task_ids, "plan must feed run_batch")
    check("check_freshness" in replay.task_dict["run_batch"].downstream_task_ids, "freshness after run")

if backfill is not None:
    check(set(backfill.task_dict) == {"validate", "repair"}, f"backfill tasks: {sorted(backfill.task_dict)}")
    check(set(backfill.params) == {"auto", "date_from", "date_to"}, f"backfill params: {list(backfill.params)}")
    rep = backfill.task_dict.get("repair")
    check(rep is not None and rep.pool == "orderledger_warehouse", "repair must use the writer pool")
    check(backfill.task_dict["validate"].retries == 0, "parameter validation must not be retried")
    check(backfill.max_active_runs == 1, "one backfill at a time")

if problems:
    print("DAG CHECKS FAILED:\n- " + "\n- ".join(problems))
    sys.exit(1)
print(f"DAG CHECKS OK: {sorted(bag.dags)}")
