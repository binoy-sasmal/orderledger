# Project status

Updated: 2026-11-06 (session 1). Deadline set by owner: Monday 2026-11-09.

**Goal.** Incremental e-commerce analytics warehouse (Olist data, dbt, DuckDB, optional
Airflow and a Databricks Free Edition run) to evidence data/analytics engineering skills.
Primary target: data engineering and analytics engineering roles in Germany.

## Decisions made by the owner

* Folder `D:\Projects\orderledger`; public GitHub repo `binoy-sasmal/orderledger` (creation and
  push authorised).
* Cloud budget: **EUR 0**. No paid resource may be created.
* Olist data is downloaded by the owner into `data/olist/` (see `docs/data-acquisition.md`).
* Resume repository and resume variants are **not** modified by this project.

## Milestones

| # | Milestone | State |
|---|---|---|
| M0 | Repo, status doc, source profile | Repo and tooling done. **Source profile waits for the Olist download** |
| M1 | Replay simulator and event contract | Done, tested on synthetic fixture |
| M2 | Loader, quarantine, run metadata | Done, tested |
| M3 | dbt models, metric definitions, tests | Done, tested on synthetic fixture |
| M4 | Reconciliation, write-audit-publish, backfill, freshness | Done, acceptance suite running |
| M5 | CI, README, demo, diagrams | CI and docs written; demo script pending |
| M6 | Airflow DAG | Stretch. Not started. Requires Docker Desktop running |
| M7 | Databricks Free Edition run | Stretch. Not started. Nothing is claimed until it runs |
| M8 | Evidence report, gap mapping, resume bullets | Pending real-data run |

## Next steps

1. Owner downloads the five Olist CSVs (instructions: `docs/data-acquisition.md`), then run
   `uv run orderledger check-data data/olist`.
2. `uv run orderledger profile data/olist --out docs/source-profile.md`; finalise ADR-003 from it.
3. Full-data run: `simulate`, then incremental `run`s; record sizes, runtimes, results.
4. Demo script, evidence report, gap mapping.
5. Stretch: Airflow wrapper; Databricks publication spike.

## Facts to carry across sessions

* dbt-core is pinned to 1.11.x (1.12 pulls a parser wheel from GitHub that fails TLS on the
  owner's machine); `mypy` is excluded (conflicts with dbt's pathspec pin).
* dbt must run in a subprocess (dbt-duckdb keeps the DB file open in-process).
* Windows: use `UV_LINK_MODE=copy`; the `VIRTUAL_ENV` warning from uv is harmless.
* GitHub token (fine-grained PAT) expires **2026-12-09**.
* Never claim: production use, real users, cost savings, benchmarks not measured here, CDC in
  the source data (Olist has none; all change events beyond lifecycle timestamps are synthetic).
