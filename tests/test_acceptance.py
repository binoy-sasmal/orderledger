"""Acceptance tests AT-01 .. AT-11 (see docs/acceptance.md)."""

from __future__ import annotations

import hashlib
import json
import shutil

import duckdb
import pytest

from helpers import (
    NO_PERTURBATIONS,
    build_incremental,
    diff_published,
    make_ws,
    rows,
    run_batch,
    snapshot,
)
from orderledger.db import db_path
from orderledger.load import read_manifest
from orderledger.pipeline import InjectedFailure, PipelineError, run_dbt, run_pipeline

ZERO = lambda d: all(v == 0 for v in d.values())  # noqa: E731


def _db(ws):
    return db_path(ws)


# ---- AT-03 / AT-04 --------------------------------------------------------------------------
def test_at03_incremental_equals_full_replay(inc7, full7):
    assert ZERO(diff_published(_db(inc7[0]), _db(full7)))


def test_at04_batch_boundaries_do_not_change_results(inc7, inc14):
    ws7, ws14 = inc7[0], inc14[0]
    # weekly batch 8 and 14-day batch 4 end on the same simulated date, after the same events
    m7 = read_manifest(ws7 / "events")["batches"][7]["period_to"]
    m14 = read_manifest(ws14 / "events")["batches"][3]["period_to"]
    assert m7 == m14
    assert ZERO(diff_published(ws7 / "snap_8.duckdb", ws14 / "snap_4.duckdb"))
    assert ZERO(diff_published(_db(ws7), _db(ws14)))


# ---- AT-06: late correction outside the lookback --------------------------------------------
def test_at06_late_correction_outside_lookback_is_detected_and_backfilled(inc7, full7):
    failures = inc7[1]
    assert failures, "the CI scenario contains delivery corrections that arrive 25 days late"
    n, message = failures[0]
    assert "stale aggregate dates" in message and "orderledger backfill --from" in message
    # after the backfill the published result is the true one
    assert ZERO(diff_published(_db(inc7[0]), _db(full7)))


def test_at06_isolated_correction_arriving_after_the_lookback(base, full7):
    ws = make_ws(base, "outside", {**NO_PERTURBATIONS, "batch_days": 14,
                                    "delivery_corrections": {"count": 2, "shift_days": 2, "delay_days": 25}})
    failures = build_incremental(ws)
    assert failures, "an order moved off an old date must be flagged"
    ref = make_ws(base, "outside_ref", {**NO_PERTURBATIONS, "batch_days": 14,
                                         "delivery_corrections": {"count": 2, "shift_days": 2, "delay_days": 25}})
    run_pipeline(ref)
    assert ZERO(diff_published(_db(ws), _db(ref)))


# ---- AT-05: late arrival inside the lookback is restated without manual action --------------
def test_at05_correction_inside_the_lookback_needs_no_backfill(base):
    cfg = {**NO_PERTURBATIONS, "batch_days": 14,
           "delivery_corrections": {"count": 2, "shift_days": 2, "delay_days": 3}}
    ws = make_ws(base, "inside", cfg)
    failures = build_incremental(ws, lookback=28)
    assert failures == []
    ref = make_ws(base, "inside_ref", cfg)
    run_pipeline(ref, lookback_days=28)
    assert ZERO(diff_published(_db(ws), _db(ref)))


def test_first_arrival_is_applied_however_late_it_is(base):
    """A late FIRST arrival (not a correction) needs no lookback: its order is touched."""
    cfg = {**NO_PERTURBATIONS, "batch_days": 14, "late_outside": {"count": 3, "delay_days": 25}}
    ws = make_ws(base, "late_first", cfg)
    assert build_incremental(ws) == []
    ref = make_ws(base, "late_first_ref", cfg)
    run_pipeline(ref)
    assert ZERO(diff_published(_db(ws), _db(ref)))


# ---- AT-01: replays and duplicates -----------------------------------------------------------
def test_at01_replaying_files_and_redelivering_events_changes_nothing(base, full7):
    ws = base / "replay"
    shutil.copytree(full7, ws)
    ev = ws / "events"
    m = read_manifest(ev)
    last = m["batches"][-1]
    n = last["batch_no"]
    # (a) identical bytes presented again under a new batch number
    shutil.copy(ev / "batch_00005.parquet", ev / f"batch_{n + 1:05d}.parquet")
    src5 = m["batches"][4]
    # (b) a new file re-delivering existing events with fresh arrival numbers
    p = ev / f"batch_{n + 2:05d}.parquet"
    con = duckdb.connect()
    con.execute(
        f"copy (select * exclude (arrival_seq), arrival_seq + 100000 as arrival_seq "
        f"from read_parquet('{(ev / 'batch_00003.parquet').as_posix()}') limit 25) "
        f"to '{p.as_posix()}' (format parquet)"
    )
    cnt = con.execute(f"select count(*) from read_parquet('{p.as_posix()}')").fetchone()[0]
    import datetime as dt

    for k, (name, sha, rc) in enumerate(
        [(f"batch_{n + 1:05d}.parquet", src5["sha256"], src5["row_count"]),
         (f"batch_{n + 2:05d}.parquet", hashlib.sha256(p.read_bytes()).hexdigest(), cnt)], start=1
    ):
        start = dt.date.fromisoformat(last["period_to"]) + dt.timedelta(days=1 + 7 * (k - 1))
        m["batches"].append({"batch_no": n + k, "file": name, "sha256": sha, "row_count": rc,
                             "period_from": str(start), "period_to": str(start + dt.timedelta(days=6)),
                             "max_arrival_seq": 0})
    (ev / "manifest.json").write_text(json.dumps(m))
    out = run_pipeline(ws)
    # (b) is ingested; (a) is recognised as identical bytes already loaded and skipped
    assert out.status == "success" and out.batches_loaded == 1
    assert rows(ws, "select count(*) from raw.load_log")[0][0] == n + 2
    assert rows(ws, f"select row_count from raw.load_log where batch_no = {n + 1}")[0][0] == 0
    assert ZERO(diff_published(_db(ws), _db(full7)))


# ---- AT-02 / AT-11: crash, retry, nothing half-published -------------------------------------
@pytest.mark.parametrize("crash_step", ["marts", "publish"])
def test_at02_at11_crash_then_retry_gives_the_correct_result(base, full7, crash_step):
    ws = make_ws(base, f"retry_{crash_step}", {"batch_days": 14})
    run_batch(ws, 1)
    run_batch(ws, 2)
    pre = snapshot(ws, "pre")
    wm = rows(ws, "select watermark from ctl.state")[0][0]
    with pytest.raises(InjectedFailure):
        run_pipeline(ws, through=3, fail_after=crash_step)
    # AT-11: the failed run published nothing and did not advance the watermark
    assert ZERO(diff_published(_db(ws), pre))
    assert rows(ws, "select watermark from ctl.state")[0][0] == wm
    failed = rows(ws, "select failed_step, status from ctl.run_log where status = 'failed'")
    assert failed == [(crash_step, "failed")]
    run_batch(ws, 3)  # retry
    build_incremental(ws, start=4)
    assert ZERO(diff_published(_db(ws), _db(full7)))


def test_at11_a_reconciliation_failure_publishes_nothing(base):
    ws = make_ws(base, "no_publish", {**NO_PERTURBATIONS, "batch_days": 14,
                                       "delivery_corrections": {"count": 2, "shift_days": 2, "delay_days": 25}})
    n = 1
    while True:
        pre = snapshot(ws, "pre")
        wm = rows(ws, "select watermark from ctl.state")[0][0]
        try:
            run_pipeline(ws, through=n)
        except PipelineError as e:
            assert e.step == "recon"
            assert ZERO(diff_published(_db(ws), pre))
            assert rows(ws, "select watermark from ctl.state")[0][0] == wm
            return
        n += 1
        assert n <= len(read_manifest(ws / "events")["batches"]), "expected a reconciliation failure"


# ---- AT-07: interval rules -------------------------------------------------------------------
def test_at07_intervals_do_not_overlap_and_start_at_purchase(full7):
    overlaps = rows(full7, """
        select count(*) from published.order_state_intervals a join published.order_state_intervals b
          on a.order_id = b.order_id and a.stage < b.stage
        where a.valid_to is null or a.valid_to > b.valid_from""")[0][0]
    assert overlaps == 0
    first_ok = rows(full7, """
        select count(*) from published.order_state_intervals s join published.fct_orders o using (order_id)
        where s.stage = 1 and s.valid_from <> o.purchase_ts""")[0][0]
    assert first_ok == 0
    # nothing exists before the first observation, and invalid sequences are excluded, not repaired
    assert rows(full7, """select count(*) from published.order_state_intervals s join published.fct_orders o using (order_id)
        where s.valid_from < o.purchase_ts or not o.is_sequence_valid""")[0][0] == 0


def test_at07_the_interval_test_really_fails_when_an_overlap_is_planted(base, full7):
    ws = base / "planted_overlap"
    shutil.copytree(full7, ws)
    con = duckdb.connect(str(_db(ws)))
    con.execute("""insert into work.order_state_intervals
        select order_id, 'approved', 2, valid_from, valid_to + interval 3 day
        from work.order_state_intervals where stage = 1 and valid_to is not null limit 1""")
    con.close()
    with pytest.raises(PipelineError, match="assert_state_intervals_valid"):
        run_dbt(ws, "test", "tag:cross", {})


# ---- AT-08: documented semantics -------------------------------------------------------------
def _events(ws, where):
    g = (ws / "events" / "batch_*.parquet").as_posix()
    return duckdb.connect().execute(f"select * from read_parquet('{g}') where {where}").fetchall()


def test_at08_duplicates_do_not_change_results(base, full7):
    cfg = {"duplicates": {"rate": 0.0}}
    ref = make_ws(base, "nodup", cfg)
    run_pipeline(ref)
    # the duplicate-free history has a different batch file set but the same logical content
    assert ZERO(diff_published(_db(full7), _db(ref)))


def test_at08_price_corrections_and_out_of_order_arrivals_resolve_to_the_latest_version(full7):
    g = (full7 / "events" / "batch_*.parquet").as_posix()
    corrected = duckdb.connect().execute(
        f"select distinct entity_id, cast(json_extract_string(payload,'$.price') as decimal(12,2)) "
        f"from read_parquet('{g}') where event_type = 'order_item_price_corrected'").fetchall()
    assert len(corrected) >= 4
    got = dict(rows(full7, "select item_key, price from published.fct_order_items"))
    for item, price in corrected:
        assert got[item] == price, item
    arrived_first = duckdb.connect().execute(
        f"select count(*) from read_parquet('{g}') c join read_parquet('{g}') a "
        f"on a.entity_id = c.entity_id and a.event_type = 'order_item_added' "
        f"where c.event_type = 'order_item_price_corrected' and c.scenario_tag = 'out_of_order' "
        f"and c.arrival_seq < a.arrival_seq").fetchone()[0]
    assert arrived_first >= 1, "the scenario must contain a correction that precedes its item"


def test_at08_corrected_delivery_time_wins_and_deleted_orders_leave_the_metrics(full7):
    g = (full7 / "events" / "batch_*.parquet").as_posix()
    con = duckdb.connect()
    fixes = con.execute(
        f"select entity_id, cast(json_extract_string(payload,'$.ts') as timestamp) "
        f"from read_parquet('{g}') where event_id like '%:delivered:c1'").fetchall()
    assert fixes
    got = dict(rows(full7, "select order_id, delivered_ts from published.fct_orders"))
    for oid, ts in fixes:
        assert got[oid] == ts
    deleted = [r[0] for r in con.execute(
        f"select entity_id from read_parquet('{g}') where event_type = 'order_deleted'").fetchall()]
    assert deleted
    flagged = rows(full7, "select count(*) from published.fct_orders where is_deleted")[0][0]
    assert flagged == len(set(deleted))
    live = rows(full7, "select count(*) from published.fct_orders where not is_deleted")[0][0]
    assert rows(full7, "select sum(orders_placed) from published.agg_daily_purchases")[0][0] == live
    # raw history is kept: the deleted orders still have their events
    assert rows(full7, f"select count(*) from raw.events where entity_id in ({','.join(repr(d) for d in deleted)})")[0][0] > 0


def test_at08_status_is_a_snapshot_not_a_timeline(full7):
    # no cancellation time: canceled orders keep the lifecycle timestamps the source recorded
    conflicts = rows(full7, "select count(*) from published.fct_orders where observed_status = 'canceled' and delivered_ts is not null")[0][0]
    assert conflicts >= 1
    assert rows(full7, "select sum(status_conflict_canceled) from published.agg_daily_deliveries")[0][0] >= 1
    cols = [r[0] for r in rows(full7, "select column_name from information_schema.columns where table_name = 'fct_orders' and table_schema = 'published'")]
    assert not any("cancel" in c for c in cols)


# ---- AT-12 is in test_reference.py; AT-09 / AT-10 are in test_reconcile.py ------------------
