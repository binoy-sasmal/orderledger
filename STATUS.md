# Project status

Updated: 2026-11-07 (session 2, Airflow extension). Owner deadline for the extension: one extra day.
**Uncommitted work is waiting for the owner's approval; nothing from session 2 is committed or pushed.**

**Goal.** Incremental e-commerce analytics warehouse (Olist data, dbt, DuckDB) evidencing data and
analytics engineering skills, now with local Airflow orchestration. Primary target: data
engineering and analytics engineering roles in Germany.

## Owner decisions

* Folder `D:\Projects\orderledger`; public repo `github.com/binoy-sasmal/orderledger` (created by
  the owner; the PAT cannot create repositories, push works).
* Cloud budget **EUR 0**, strictly. No paid service, billing-enabled account, cloud resource or trial.
* Olist data downloaded by the owner into `data/olist/` (git-ignored, CC BY-NC-SA 4.0).
* The resume repository and resume variants are **not** modified by this project.
* Session 2: do not commit or push until the owner approves the final changes.

## Milestones

| # | Milestone | State |
|---|---|---|
| M0 to M5 | Repo, profile, simulator, loader, dbt models, reconciliation, publish, CI, docs | Done (committed and pushed, session 1) |
| M6 | Airflow orchestration (local Docker) | **Done, verified on the synthetic fixture; uncommitted** |
| M7 | Databricks Free Edition run | Not started, out of scope for the extension. Nothing claimed about any platform |
| M8 | Evidence report, gap mapping, resume bullets | Done; updated for Airflow (uncommitted) |

## Verified results

* Session 1 (committed): 35 tests, CI green; full Olist data: incremental runs over 56 and 28
  batches equal a single-pass replay (0 differing rows, 8 tables); metrics match a direct CSV
  computation and a Python reference. Details: `docs/evidence-report.md`.
* Session 2 (uncommitted): full suite 56 passed locally (35 + 21 orchestration tests); lint clean.
  `airflow/demo.py` rebuilt the stack from scratch and passed 23/23 checks: scheduler-created runs,
  an injected transient failure retried (exit 75), deterministic reconciliation failures not retried
  with the last publication preserved, invalid backfill parameters rejected, a backfill repaired
  while an outside writer held the lock (rejected twice, then succeeded), and the final tables
  equal a single-pass replay. Details: `docs/orchestration-evidence.md`.

## Changed in session 2

New: `airflow/` (Dockerfile, compose, DAGs, `check_dags.py`, `setup_env.py`, `demo.py`,
`export_evidence.py`), `src/orderledger/locking.py`, `src/orderledger/orchestration.py`,
`scripts/compare_workspaces.py`, `scenarios/airflow-demo.yml`, `.dockerignore`,
`tests/test_orchestration.py`, `docs/orchestration*.md`, `docs/orchestration-run-records.json`.
Modified: `src/orderledger/pipeline.py` (write lock, retryable errors, once-only injection),
`src/orderledger/cli.py` (exit codes 1/75, `--lock-wait`, `--inject-*`, `hold-lock`, `backfill
--auto` under the lock), README, architecture, evidence report, gap mapping, acceptance, decisions,
resume bullets, `.gitignore`. Warehouse behaviour and the full-data results are unchanged.

## Open, if continuing

1. Owner reviews and approves, then commit and push (CI will run the 56-test suite, longer than before).
2. Optional: a CI job that builds the Airflow image and runs `check_dags.py` (not added: it could
   not be verified on GitHub without pushing).
3. Optional: UI screenshots (none captured; JSON run records are the record).
4. Databricks Free Edition publication spike: verify multi-table transaction support first.
5. Profile the per-run fixed cost (three dbt processes) and the growth in per-run time.

## Facts to carry across sessions

* dbt-core pinned to 1.11.x (1.12 pulls a parser wheel from GitHub that fails TLS here); `mypy`
  excluded (conflicts with dbt's pathspec pin). dbt runs in a subprocess.
* Windows: `UV_LINK_MODE=copy`; the uv `VIRTUAL_ENV` warning is harmless. In Git Bash use
  `MSYS_NO_PATHCONV=1` for docker commands that take container paths. `rm` is denied by the
  project settings.
* Airflow: image `apache/airflow:3.4.0-python3.12` pinned by digest; secrets are generated into
  `airflow/.env` (git-ignored); the UI is `http://127.0.0.1:8080`. Stop with
  `docker compose -f airflow/docker-compose.yaml stop` (volumes kept) or `down -v` (wipes them).
* GitHub token (fine-grained PAT) expires **2026-12-09**.
* Never claim: production use or orchestration, real users, cost savings, speed benefit (an
  incremental run was not faster than a rebuild at this size), CDC in the source, Airflow on the
  full Olist data, Airflow beyond a local Docker demo, or any cloud platform run.
