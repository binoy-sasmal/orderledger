# Acceptance tests

CI runs everything on the synthetic fixture (`tests/fixtures/synth`, 70 orders). The same tests
are designed to hold on the real data; full-data results are recorded in
[evidence-report.md](evidence-report.md) once that run exists.

Equivalence compares the **business columns of every published table** as multisets (`EXCEPT
ALL` both ways). Run IDs, load timestamps and other operational values live only in `ctl.*`
and are never compared. Byte-identical Parquet is not required; determinism is checked on
logical event content.

| ID | Requirement | Test |
|---|---|---|
| AT-01 | Replaying the same file, or re-delivering events, does not double-count | `test_acceptance::test_at01_...`, `test_load::test_loading_twice_changes_nothing` |
| AT-02 | Retrying after a partial failure gives the same correct outcome | `test_at02_at11_crash_then_retry...` (crash after marts; crash before publish commit) |
| AT-03 | Incremental processing equals a full replay of the same history | `test_at03_incremental_equals_full_replay` |
| AT-04 | Different batch boundaries do not change results, compared at equivalent checkpoints after required backfills | `test_at04_batch_boundaries_...` (weekly batch 8 vs 14-day batch 4, then final) |
| AT-05 | Late change inside the lookback is restated automatically | `test_at05_correction_inside_the_lookback_needs_no_backfill` |
| AT-06 | Late change outside the lookback fails reconciliation, naming dates; backfill repairs it | `test_at06_...` (two tests) |
| AT-07 | Intervals do not overlap and start at purchase; the check really fails when an overlap is planted | `test_at07_...` (two tests) |
| AT-08 | Updates, duplicates, deletes, out-of-order arrivals follow the documented semantics | `test_at08_...` (four tests), `test_load` contradictions |
| AT-09 | Deliberate discrepancies are detected (tampered metric, missing fact, duplicated fact, source events never sent) | `test_reconcile::test_at09_...` |
| AT-10 | Breaking schema changes and freshness failures surface clearly | `test_load::test_breaking_schema_change_is_quarantined`, `test_reconcile::test_at10_...` |
| AT-11 | A failed run never reaches `published` and never advances the watermark | `test_at02_at11...`, `test_at11_a_reconciliation_failure_publishes_nothing` |
| AT-12 | Headline metrics equal two independent computations | `test_reference.py` (direct SQL over the CSVs without synthetic events; plain-Python reference with them) |
| AT-13 | Same input and scenario give the same logical events; every synthetic event is labelled | `test_events.py` |

## Orchestration checks (added with the Airflow stage)

| Check | Where |
|---|---|
| Interval to batch mapping is positional, wall-clock independent, and robust to Airflow's datetime type | `test_orchestration.py` |
| Backfill parameters validated (both modes, bad dates, reversed, too wide) | `test_orchestration.py` |
| Exit codes: 75 retryable, 1 deterministic; once-only injected failure then success with no duplicated results | `test_orchestration.py` |
| Second writer rejected / waits; overlapping real runs serialized to the same result | `test_orchestration.py` |
| DAGs import and keep their pool, retry policy, ordering and parameters | `airflow/check_dags.py` (in the Airflow image) |
| End-to-end scheduling, retry, deterministic failure, backfill, lock collision, equality with a replay | `airflow/demo.py` (23 checks; needs Docker) |

CI runs the pytest suite only. `check_dags.py` and `demo.py` need Docker and the 3 GB Airflow image
and are run locally; they are not part of the GitHub Actions workflow.

## Known limits of these tests

* The Python reference and the warehouse share an author and the written rules, so the reference
  is a cross-check, not independent verification. The CSV-direct check (no events involved) is
  the more independent one, and only covers runs without synthetic events.
* Freshness is covered by CLI exit codes. The Airflow demo runs the freshness check after each
  batch and shows it skipped when the batch failed; it does not alert anyone.
* Fixture-scale tests do not exercise performance; runtime is measured separately.
