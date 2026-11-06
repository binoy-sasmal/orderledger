# Orchestration with Airflow

Airflow 3.4.0 runs locally in Docker and **coordinates** the existing pipeline. It owns *when*
things run, retries and visibility. It owns no business logic: every task shells out to the
`orderledger` CLI, which still owns loading, dbt, reconciliation, publishing and locking.

> Local demo on the synthetic fixture. Not production orchestration, not a cloud deployment, no
> SLA. Earlier full-data results in [evidence-report.md](evidence-report.md) were produced
> **without** Airflow; the orchestration evidence in [orchestration-evidence.md](orchestration-evidence.md)
> uses the synthetic CI fixture, so no Kaggle credentials are needed.

## Components

| Piece | What it is |
|---|---|
| `airflow/Dockerfile` | `apache/airflow:3.4.0-python3.12` pinned by digest, plus the orderledger CLI in its **own** virtualenv (`uv sync --frozen`, uv 0.13.1, from `uv.lock`) so dbt/DuckDB never mix with Airflow's dependencies |
| `airflow/docker-compose.yaml` | Postgres, `airflow-init`, API server, scheduler, DAG processor. **LocalExecutor**: no Celery, Redis or workers (the official file is `airflow/official-docker-compose-3.4.0.yaml`, kept for comparison). The API server binds to `127.0.0.1` only |
| `airflow/setup_env.py` | Writes `airflow/.env` with generated secrets (git-ignored, never printed) and the replay anchor |
| `airflow/dags/` | `orderledger_replay` (scheduled), `orderledger_backfill` (manual, parameterised), shared helpers |
| `airflow/check_dags.py` | DAG import and structure checks, run inside the image |
| `src/orderledger/orchestration.py` | Stdlib-only helpers the DAGs import: interval-to-batch mapping, parameter validation, exit-code constants. Unit-tested in the repository |
| `src/orderledger/locking.py` | The workspace write lock |

The warehouse workspace lives in the Docker **named volume** `orderledger-ws` (mounted at
`/data/ws`), not a Windows bind mount, to avoid file-locking problems across the Docker Desktop
boundary.

## Scheduling and how intervals map to batches

`orderledger_replay` has `schedule = N seconds`, `catchup = True`, `max_active_runs = 1`.
Its `start_date` is the **replay anchor** (`ORDERLEDGER_REPLAY_START`). Interval *k* (counted from
the anchor) replays **batch k+1** of the historical event feed:

```
batch_no = (data_interval_start - anchor) / period + 1        # orchestration.batch_for_interval
```

* The wall-clock date only decides when the scheduler fires. It never selects a business date:
  the same positions give the same batches on any day (tested).
* A misaligned interval is an error. An interval beyond the last batch is **skipped**, not failed.
* Tasks: `ensure_events` (builds the event history once, from the fixture or, if
  `ORDERLEDGER_CSV_DIR` is changed, from real Olist files) → `plan` (interval → batch) →
  `run_batch` (`orderledger run --through <batch>`) → `check_freshness`
  (`orderledger freshness --expected-through <batch end>`).
* `run_batch` has `depends_on_past=True`: while the previous batch is unresolved, later batches
  wait instead of piling up failures.

**Airflow catchup is not the warehouse backfill.** Catchup creates runs for *scheduled
intervals* that were missed. The warehouse backfill repairs *stale aggregate dates*
([late-data.md](late-data.md)). They are different mechanisms and are not conflated.

## Failure handling: retry or not

The CLI exit code tells Airflow whether a retry can help:

| Exit | Meaning | Airflow |
|---|---|---|
| 0 | success | success |
| 75 | **retryable**: workspace lock busy, crash, infrastructure error | ordinary exception → retried (2 retries, 15 s apart) |
| 1 | **deterministic**: contract violation, failed dbt test, reconciliation failure, bad parameters | `AirflowFailException` → **no retry**, stays failed and visible |
| 2 | freshness breach | `AirflowFailException` → no retry |

Why: retrying a data-quality failure cannot succeed and only hides it. A reconciliation failure
leaves `published` and the watermark exactly as the last validated run left them.

**Transient failure injection (demo only).** The Airflow Variable `orderledger_inject_transient`
= `<batch>:<step>` (for example `2:marts`) makes the CLI crash **once** after that step while
processing that batch. A marker file in the workspace records that the failure was spent, so the
retry of the same task succeeds. The first attempt publishes nothing and does not move the
watermark; the retry produces the same result as a clean run (tested: `test_injected_failure_...`).

## Backfill through Airflow

`orderledger_backfill` (`schedule=None`, parameters `auto`, `date_from`, `date_to`) runs the
existing `orderledger backfill`. Parameters are validated first (`validate` task, deterministic,
never retried): exactly one mode; ISO dates; `from <= to`; at most 400 days.

It is **never triggered automatically**. A failed reconciliation stays failed until someone (or a
deliberately designed automation) runs it. Repairing a failed check automatically would hide the
problem the check reports. After a successful backfill, clear the failed `run_batch` task: the
rerun finds nothing stale and publishes, and later batches continue.

## Concurrency

Two layers prevent overlapping writers to the one DuckDB file:

1. **Airflow pool `orderledger_warehouse` with one slot.** Every task that touches the warehouse
   (`ensure_events`, `run_batch`, `check_freshness`, the backfill `repair`) takes it, so scheduled
   runs, retries and the backfill DAG are serialized by the scheduler.
2. **A file lock inside the CLI** (`<workspace>/.write.lock`, held by `run_pipeline` for the whole
   run). A second writer is rejected immediately with exit 75 (retryable), or waits up to
   `--lock-wait` seconds.

Scope and limits, stated plainly:

* The file lock covers every process that goes through `run_pipeline` on the same workspace:
  Airflow tasks, `docker compose exec ... orderledger run`, any CLI invocation inside the
  container. It does **not** stop someone opening `wh.duckdb` directly with another tool, and a
  copy of the workspace on another filesystem has its own lock.
* A CLI run on the Windows host uses a different workspace (a host path), not the named volume,
  so it cannot collide with Airflow and is not protected against it either; they do not share
  state.
* Read-only commands (`status`, `reconcile`, `freshness`) do not take the lock; DuckDB itself
  refuses a read-only open while a writer is active, which surfaces as a failed command, not as
  corruption.
* Single machine only. This is deliberately not a distributed lock.

## Reproduce

One command rebuilds everything and checks the outcomes (about 20 minutes; Docker must be running;
uses the synthetic fixture, no Kaggle login):

```
uv sync --frozen
uv run python airflow/demo.py
```

Step by step:

```
python airflow/setup_env.py
docker compose -f airflow/docker-compose.yaml up -d --build
MSYS_NO_PATHCONV=1 docker compose -f airflow/docker-compose.yaml run --rm --no-deps \
    --entrypoint python airflow-scheduler /opt/airflow/check_dags.py
docker compose -f airflow/docker-compose.yaml exec airflow-scheduler airflow dags unpause orderledger_replay
```

The UI is at http://127.0.0.1:8080 (user `airflow`, password in `airflow/.env`). The full scripted
demonstration and its recorded outcome: [orchestration-evidence.md](orchestration-evidence.md).
