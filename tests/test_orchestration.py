"""Orchestration-facing behaviour: interval-to-batch mapping, parameter validation, exit codes that
tell a scheduler whether to retry, once-only failure injection and the workspace write lock.
These run without Airflow; the DAG files themselves are checked in the Airflow image
(`airflow/check_dags.py`, see docs/orchestration.md)."""

from __future__ import annotations

import datetime as dt
import subprocess
import sys
import time

import pytest
from typer.testing import CliRunner

from helpers import NO_PERTURBATIONS, diff_published, make_ws, rows
from orderledger.cli import app
from orderledger.db import db_path
from orderledger.locking import WorkspaceBusy, workspace_lock
from orderledger.orchestration import (
    EXIT_DETERMINISTIC,
    EXIT_RETRYABLE,
    ParamError,
    batch_for_interval,
    parse_inject_setting,
    period_to,
    validate_backfill_params,
)
from orderledger.pipeline import PipelineError, run_pipeline

UTC = dt.UTC
ZERO = lambda d: all(v == 0 for v in d.values())  # noqa: E731
CLI = [sys.executable, "-c", "from orderledger.cli import app; app()"]


# ---- interval -> replay batch ----------------------------------------------------------------
def test_intervals_map_to_batches_by_position_not_by_the_clock():
    anchor = dt.datetime(2026, 11, 7, 9, 0, tzinfo=UTC)
    step = dt.timedelta(minutes=5)
    assert [batch_for_interval(anchor + i * step, anchor, step) for i in range(4)] == [1, 2, 3, 4]
    # the same positions on a different day give the same batches: wall-clock date is irrelevant
    other = dt.datetime(2031, 1, 1, 0, 0, tzinfo=UTC)
    assert [batch_for_interval(other + i * step, other, step) for i in range(4)] == [1, 2, 3, 4]
    assert batch_for_interval(anchor - step, anchor, step) == 0  # before the anchor: nothing to do


def test_datetime_subclasses_with_foreign_subtraction_work():
    """Airflow passes pendulum datetimes; arithmetic between them must not matter."""

    class Foreign(dt.datetime):
        def __sub__(self, other):  # noqa: ANN001 - mimics a library type whose difference is not a timedelta
            raise AssertionError("datetime subtraction must not be used")

    anchor = dt.datetime(2026, 11, 7, 9, 0, tzinfo=UTC)
    step = dt.timedelta(minutes=3)
    start = Foreign(2026, 11, 7, 9, 6, tzinfo=UTC)
    assert batch_for_interval(start, anchor, step) == 3
    with pytest.raises(ValueError, match="timezone-aware"):
        batch_for_interval(dt.datetime(2026, 11, 7, 9, 6), anchor, step)


def test_misaligned_or_invalid_intervals_are_rejected():
    anchor = dt.datetime(2026, 11, 7, 9, 0, tzinfo=UTC)
    with pytest.raises(ValueError, match="not aligned"):
        batch_for_interval(anchor + dt.timedelta(minutes=2), anchor, dt.timedelta(minutes=5))
    with pytest.raises(ValueError, match="positive"):
        batch_for_interval(anchor, anchor, dt.timedelta(0))


def test_period_to_reads_the_simulated_batch_end(base):
    ws = make_ws(base, "orch_manifest", {"batch_days": 14})
    assert period_to(ws, 1) == dt.date(2018, 1, 14)
    with pytest.raises(KeyError):
        period_to(ws, 999)


# ---- backfill parameters ---------------------------------------------------------------------
@pytest.mark.parametrize(
    "args",
    [
        ("", "", False),  # nothing given
        ("2018-01-30", "", False),  # half a window
        ("2018-01-30", "2018-01-01", False),  # reversed
        ("2018-1-30", "2018-02-01", False),  # malformed
        ("2018-02-30", "2018-03-01", False),  # impossible date
        ("2018-01-01", "2019-12-31", False),  # too wide
        ("2018-01-30", "2018-01-30", True),  # both modes
    ],
)
def test_invalid_backfill_parameters_are_rejected(args):
    with pytest.raises(ParamError):
        validate_backfill_params(*args)


def test_valid_backfill_parameters():
    assert validate_backfill_params("", "", True) == {"auto": True}
    assert validate_backfill_params(" 2018-01-30 ", "2018-02-01", False) == {
        "auto": False, "date_from": "2018-01-30", "date_to": "2018-02-01"}


def test_inject_setting_parsing():
    assert parse_inject_setting("") is None
    assert parse_inject_setting("3:marts") == (3, "marts")
    with pytest.raises(ParamError):
        parse_inject_setting("marts")
    with pytest.raises(ParamError):
        parse_inject_setting("3:nonsense")


# ---- the write lock --------------------------------------------------------------------------
def test_a_second_writer_is_rejected_and_the_lock_is_released(tmp_path):
    with workspace_lock(tmp_path, 0):
        with pytest.raises(WorkspaceBusy, match="another writer"):
            with workspace_lock(tmp_path, 0):
                pass
    with workspace_lock(tmp_path, 0):  # free again after release
        pass


def test_a_waiting_writer_gets_the_lock_when_the_holder_finishes(tmp_path):
    holder = subprocess.Popen([*CLI, "hold-lock", "--ws", str(tmp_path), "--seconds", "2"],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    deadline = time.monotonic() + 30
    while not (tmp_path / ".write.lock").exists() and time.monotonic() < deadline:
        time.sleep(0.1)
    time.sleep(0.5)  # holder has started
    t0 = time.monotonic()
    with workspace_lock(tmp_path, wait_seconds=20):
        waited = time.monotonic() - t0
    holder.wait(timeout=30)
    assert 0.3 < waited < 15


def test_cli_run_is_rejected_with_a_retryable_exit_code_while_another_writer_holds_the_lock(base):
    ws = make_ws(base, "locked_run", {**NO_PERTURBATIONS, "batch_days": 14})
    holder = subprocess.Popen([*CLI, "hold-lock", "--ws", str(ws), "--seconds", "20"],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        time.sleep(4)
        res = CliRunner().invoke(app, ["run", "--ws", str(ws), "--through", "1"])
        assert res.exit_code == EXIT_RETRYABLE
        assert not db_path(ws).exists() or rows(ws, "select count(*) from raw.events")[0][0] == 0
    finally:
        holder.kill()
        holder.wait(timeout=30)


def test_overlapping_runs_are_serialized_and_give_the_same_result(base):
    cfg = {**NO_PERTURBATIONS, "batch_days": 14}
    ws = make_ws(base, "overlap", cfg)
    ref = make_ws(base, "overlap_ref", cfg)
    run_pipeline(ref, through=2)
    # two writers started together; each waits for the lock instead of colliding
    a = subprocess.Popen([*CLI, "run", "--ws", str(ws), "--through", "1", "--lock-wait", "300"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    b = subprocess.Popen([*CLI, "run", "--ws", str(ws), "--through", "2", "--lock-wait", "300"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert a.wait(timeout=600) == 0 and b.wait(timeout=600) == 0
    assert ZERO(diff_published(db_path(ws), db_path(ref)))
    assert rows(ws, "select count(*) from ctl.run_log where status = 'success'")[0][0] == 2


# ---- retryable versus deterministic failures -------------------------------------------------
def test_deterministic_reconciliation_failure_exits_1_and_publishes_nothing(base):
    ws = make_ws(base, "det_fail", {**NO_PERTURBATIONS, "drop_source_events": {"count": 2}})
    res = CliRunner().invoke(app, ["run", "--ws", str(ws)])
    assert res.exit_code == EXIT_DETERMINISTIC
    assert "deterministic" in res.output and "source_inventory" in res.output
    assert rows(ws, "select watermark from ctl.state")[0][0] == 0
    assert rows(ws, "select count(*) from ctl.run_log where status = 'failed' and failed_step = 'recon'")[0][0] == 1


def test_injected_failure_happens_once_then_the_retry_succeeds_without_duplicating_results(base):
    cfg = {**NO_PERTURBATIONS, "batch_days": 14}
    ws = make_ws(base, "inject_once", cfg)
    ref = make_ws(base, "inject_once_ref", cfg)
    run_pipeline(ref, through=1)
    args = ["run", "--ws", str(ws), "--through", "1", "--inject-failure-once", "marts", "--inject-token", "t1"]
    first = CliRunner().invoke(app, args)
    assert first.exit_code == EXIT_RETRYABLE and "retryable" in first.output
    # the failed attempt published nothing and did not move the watermark
    assert rows(ws, "select watermark from ctl.state")[0][0] == 0
    assert rows(ws, "select count(*) from information_schema.tables where table_schema = 'published'")[0][0] == 0
    second = CliRunner().invoke(app, args)  # the retry: same command, the failure is spent
    assert second.exit_code == 0
    assert rows(ws, "select watermark from ctl.state")[0][0] > 0
    assert ZERO(diff_published(db_path(ws), db_path(ref)))
    # a different token is a different 'once'
    with pytest.raises(PipelineError) as e:
        run_pipeline(ws, through=1, inject_once=("facts", "t2"))
    assert e.value.retryable


def test_backfill_auto_with_nothing_stale_is_a_noop(base, full7):
    import shutil

    ws = base / "noop_backfill"
    shutil.copytree(full7, ws)
    res = CliRunner().invoke(app, ["backfill", "--ws", str(ws), "--auto"])
    assert res.exit_code == 0 and "nothing stale" in res.output


def test_backfill_requires_a_window_or_auto(base):
    ws = make_ws(base, "bf_args", {"batch_days": 14})
    res = CliRunner().invoke(app, ["backfill", "--ws", str(ws)])
    assert res.exit_code == 2
