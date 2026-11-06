# Orchestration evidence (Airflow, local, synthetic fixture)

Recorded 2026-11-06 by `airflow/demo.py`, which rebuilds the stack from scratch and asserts every
outcome below (23 checks, all passed, exit code 0). The raw run records exported through
Airflow's REST API are in [orchestration-run-records.json](orchestration-run-records.json)
(no credentials; scanned against the generated secrets before saving).

**Scope of this evidence.** It uses the **synthetic CI fixture** (70 orders, 508 events, 7 replay
batches of 14 days) so no Kaggle login is needed. It demonstrates scheduling, retries, backfill and
serialization. The full-data results in [evidence-report.md](evidence-report.md) were produced
**without** Airflow and are not repeated here. It is a local demo on one laptop, not production
orchestration and not a cloud deployment. Screenshots were not captured; the exported records and
task logs are the record.

## Environment (as run)

| | |
|---|---|
| Airflow | 3.4.0 (`apache/airflow:3.4.0-python3.12@sha256:818f94af...`, Python 3.12.16), LocalExecutor, Postgres 16 |
| Orchestrator code | `airflow/dags/orderledger_replay.py`, `orderledger_backfill.py`, `orderledger_common.py` |
| Warehouse code | the unchanged `orderledger` CLI in its own virtualenv (`uv sync --frozen`, uv 0.13.1) |
| Docker | Docker Desktop, engine 29.6.1, Compose v5.3.0, Windows host with WSL2 |
| Schedule | one interval every 180 s; anchor `2026-11-06T22:56:00+00:00`; 2 intervals due at startup (catchup), the rest created by the scheduler as time passed |

## What ran

Replay DAG `orderledger_replay`, scheduler-created runs (`run_type = scheduled`). Interval *k* from
the anchor replays batch *k+1*:

| Batch | Run ID | Outcome |
|---|---|---|
| 1 | `scheduled__2026-11-06T22:56:00+00:00` | success, no retry |
| 2 | `scheduled__2026-11-06T22:59:00+00:00` | `run_batch` **attempt 1 failed (exit 75, injected once), attempt 2 succeeded** |
| 3 | `scheduled__2026-11-06T23:02:00+00:00` | success |
| 4 | `scheduled__2026-11-06T23:05:00+00:00` | attempt 1 **failed deterministically (exit 1), not retried**; repaired by backfill; cleared; rerun succeeded |
| 5, 6 | `...23:08:00`, `...23:11:00` | success (held back by `depends_on_past` while batch 4 was unresolved) |
| 7 | `scheduled__2026-11-06T23:14:00+00:00` | attempt 1 **failed deterministically**, not retried; repaired by explicit-window backfill; cleared; rerun succeeded |
| (8) | `scheduled__2026-11-06T23:17:00+00:00` | interval beyond the last batch: `plan`, `run_batch`, `check_freshness` **skipped**, run success |

Backfill DAG `orderledger_backfill` (manual runs):

| Run ID | Parameters | Outcome |
|---|---|---|
| `bf-invalid-params` | `date_from` only | `validate` failed (one attempt), `repair` never ran, warehouse untouched |
| `bf-auto-repair` | `auto=true`, outside writer holding the lock for 25 s | `repair` attempts: **failed, failed, succeeded** (two rejected with exit 75 and "another writer holds the lock", then success); watermark 299 → 385 |
| `bf-explicit-repair` | `date_from`/`date_to` = the dates batch 7 named | success |

## Acceptance criteria

| Criterion | Evidence |
|---|---|
| A scheduler-triggered run succeeds | Batches 1 to 7 were created and run by the scheduler (`scheduled__` run IDs) |
| A failed task retries and succeeds | Batch 2: attempt 1 exit 75 (injected once), attempt 2 exit 0. The injector writes a marker in the workspace, so the failure cannot recur on the retry |
| Retries do not duplicate results or advance the watermark early | The failed attempt published nothing (tested directly in `test_injected_failure_happens_once...`). The workspace's own run log shows the crashed run followed by a clean one. The final tables equal a single-pass replay (below) |
| A deterministic reconciliation failure stays visible and keeps the last publication | Batch 4: one attempt only, exit 1, stale delivery date named in the log. The published checkpoint stayed at the end of batch 3 (`2018-02-11`, watermark 299). `check_freshness` did not run (`upstream_failed`) |
| Retryable and deterministic failures are distinguished | Exit 75 → retried (batch 2, backfill lock rejections). Exit 1 and bad parameters → `AirflowFailException`, never retried (batches 4 and 7, `bf-invalid-params`) |
| A targeted backfill repairs the dates and passes reconciliation | `bf-auto-repair` and `bf-explicit-repair`; after each, the cleared batch reran and passed reconciliation |
| Overlapping writes are serialized or safely rejected | An outside writer held the workspace lock; the backfill's first two attempts were rejected with exit 75 and retried; the third ran after release. Two real CLI runs started together were serialized, with the same result as a sequential run (`test_overlapping_runs_are_serialized...`) |
| Existing regression tests pass | Full suite: 56 passed (35 original plus 21 orchestration tests) |
| DAG loading and orchestration behaviour checked | `airflow/check_dags.py` in the image (imports, task graph, pool, retry policy, parameters); `test_orchestration.py` for mapping, validation, exit codes, once-only injection and locking |
| Reproducible from documented commands | `uv run python airflow/demo.py` rebuilt the stack from scratch (`down -v`, new secrets, image build, DAG checks) and passed |

**Final state equals a replay.** The workspace was copied out of the Docker volume and compared with
a single-pass replay of the same events: **0 differing rows in all 8 published tables**
(`scripts/compare_workspaces.py`). Its run log: 7 successful runs, 3 failed runs (1 injected
transient, 2 deterministic reconciliation failures), 2 successful backfills; watermark 508,
checkpoint 2018-04-08.

## What the first manual pass found (kept for honesty)

Before the scripted run, a manual pass found two defects, both fixed and now covered:

1. **`batch_for_interval` crashed under Airflow**: it subtracted datetimes, and Airflow passes
   pendulum datetimes. Now integer-timestamp based; regression test with a foreign datetime type.
2. **Deadlock with `max_active_runs=1` plus `depends_on_past`**: clearing a failed earlier run
   queued it behind the later run that was waiting for it. Fixed with `max_active_runs=4`, keeping
   order through `depends_on_past` and the one-slot pool (documented in the DAG and in
   [orchestration.md](orchestration.md)).

## Limitations

* One machine, LocalExecutor, a 3-minute demo period standing in for a real cadence.
* Fixture data only. The 643k-event full-data run was not re-run under Airflow.
* A person (or a deliberately designed automation) must run the backfill and then clear the failed
  task. Nothing repairs a failed data-quality check automatically, on purpose.
* The failure injector and the lock-holding helper are test aids, not product features.
* The file lock and the pool protect writers that go through `run_pipeline` / Airflow on the shared
  volume. They do not protect against another tool opening `wh.duckdb` directly.
* No alerting, SLAs, secrets manager or access control beyond local Airflow login; the stack is
  bound to `127.0.0.1` and is not hardened for production.

## Interview walkthrough

1. **What Airflow does here.** It decides *when* and *whether to retry*. The warehouse logic is the
   same tested CLI; Airflow runs it as one task per replay batch.
2. **Scheduling.** Each scheduled interval is one replay batch, mapped by position from an anchor, so
   the wall-clock date never picks a business date. Catchup creates missed intervals; it is a
   different thing from the warehouse backfill.
3. **Retries.** The CLI returns exit 75 for things a retry can fix (lock busy, crash) and exit 1 for
   data problems. Airflow retries the first and refuses to retry the second, so a reconciliation
   failure stays visible. I injected a crash once into batch 2 to prove the retry works and does not
   duplicate or publish early.
4. **Backfill.** A reconciliation failure leaves the last good publication untouched. A separate,
   manually triggered DAG with validated parameters repairs exactly the named dates; then I clear
   the failed task and the replay continues. It never auto-repairs a failed check.
5. **Concurrency.** DuckDB has one writer. A one-slot Airflow pool serializes tasks, and a file lock
   in the CLI rejects any other writer with a retryable code. The lock's reach is the shared volume,
   not every possible tool.
6. **What I got wrong first.** A date-arithmetic crash and a scheduling deadlock, both found by
   running it, fixed and tested.
