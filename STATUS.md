# Project status

Updated: 2026-11-06 (end of session 1). Owner deadline: Monday 2026-11-09.

**Goal.** Incremental e-commerce analytics warehouse (Olist data, dbt, DuckDB) evidencing
data and analytics engineering skills. Primary target: data engineering and analytics
engineering roles in Germany.

## Owner decisions

* Folder `D:\Projects\orderledger`; public repo `github.com/binoy-sasmal/orderledger`
  (created by the owner; the PAT cannot create repositories, push works).
* Cloud budget **EUR 0**. No paid resource may be created.
* Olist data downloaded by the owner into `data/olist/` (git-ignored, CC BY-NC-SA 4.0).
* The resume repository and resume variants are **not** modified by this project.

## Milestones

| # | Milestone | State |
|---|---|---|
| M0 | Repo, status doc, source profile | Done (`docs/source-profile.md`) |
| M1 | Replay simulator and event contract | Done |
| M2 | Loader, quarantine, run metadata | Done |
| M3 | dbt models, metric definitions, tests | Done |
| M4 | Reconciliation, write-audit-publish, backfill, freshness | Done |
| M5 | CI, README, demo, diagrams | Done (CI green on GitHub; demo transcript in `docs/demo-transcript.txt`) |
| M6 | Airflow DAG | **Not started.** Needs Docker Desktop running. Not claimed |
| M7 | Databricks Free Edition run | **Not started.** Nothing claimed about any platform |
| M8 | Evidence report, gap mapping, resume bullets | Done for the local DuckDB work |

## Verified results

* 35 tests pass locally on the synthetic fixture; CI (Linux) passed.
* Full Olist data: incremental runs over 56 and 28 batches equal a single-pass replay (0
  differing rows in 8 tables); metrics match a direct CSV computation and a Python reference.
  Details: `docs/evidence-report.md`.

## Open, if continuing

1. Airflow wrapper (DAG calling the CLI; retries, backfill run). Start Docker Desktop first.
2. Databricks Free Edition publication spike: verify current multi-table transaction support
   before any guarantee is claimed; run the dbt project there; record limits and runtime.
3. Profile the per-run fixed cost (three dbt processes) and the growth in per-run time.
4. Optionally a README diagram image and a short walkthrough.

## Facts to carry across sessions

* dbt-core pinned to 1.11.x (1.12 pulls a parser wheel from GitHub that fails TLS here);
  `mypy` excluded (conflicts with dbt's pathspec pin).
* dbt runs in a subprocess (dbt-duckdb keeps the DB file open in-process).
* Windows: `UV_LINK_MODE=copy`; the uv `VIRTUAL_ENV` warning is harmless.
* GitHub token (fine-grained PAT) expires **2026-12-09**.
* Never claim: production use, real users, cost savings, speed benefit (an incremental run was
  not faster than a rebuild at this size), CDC in the source (Olist has none), Airflow, or any
  cloud platform run.
