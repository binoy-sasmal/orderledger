"""AT-09 (planted discrepancies are detected) and AT-10 (schema change, freshness)."""

from __future__ import annotations

import shutil

import duckdb
import pytest
from typer.testing import CliRunner

from helpers import NO_PERTURBATIONS, make_ws, rows
from orderledger.cli import app
from orderledger.db import db_path
from orderledger.pipeline import PipelineError, run_pipeline
from orderledger.reconcile import run_checks


def _tampered(base, full7, name, sql):
    ws = base / name
    shutil.copytree(full7, ws)
    con = duckdb.connect(str(db_path(ws)))
    con.execute(sql)
    checks = {c.name: c for c in run_checks(con, ws / "events", complete=True)}
    con.close()
    return checks


def test_untouched_warehouse_reconciles_cleanly(base, full7):
    ws = base / "untouched"
    shutil.copytree(full7, ws)
    con = duckdb.connect(str(db_path(ws)), read_only=True)
    checks = run_checks(con, ws / "events", complete=True)
    assert [c for c in checks if c.status == "fail"] == []
    assert {c.name: c.status for c in checks}["source_inventory"] == "ok"


def test_at09_a_tampered_metric_is_detected_with_the_date(base, full7):
    day = rows(full7, "select min(purchase_date) from published.agg_daily_purchases")[0][0]
    c = _tampered(base, full7, "t_metric",
                  f"update work.agg_daily_purchases set gmv = gmv + 1 where purchase_date = '{day}'")
    assert c["facts_to_marts"].status == "fail" and str(day) in c["facts_to_marts"].detail
    assert f"backfill --from {day} --to {day}" in c["facts_to_marts"].detail


def test_at09_a_missing_fact_row_is_detected(base, full7):
    c = _tampered(base, full7, "t_missing_fact",
                  "delete from work.fct_orders where order_id = (select min(order_id) from work.fct_orders)")
    assert c["raw_to_fct_orders"].status == "fail"
    assert c["placed_orders_by_date"].status == "fail"


def test_at09_a_duplicated_fact_row_is_detected(base, full7):
    c = _tampered(base, full7, "t_dup_fact",
                  "insert into work.fct_order_items select * from work.fct_order_items limit 1")
    assert c["raw_to_fct_order_items"].status == "fail"


def test_at09_events_the_source_never_sent_are_caught_by_the_independent_inventory(base):
    # dropped BEFORE the manifest is written, so file hashes and row counts all agree;
    # only the inventory computed from the source CSVs can see the gap
    ws = make_ws(base, "dropped", {**NO_PERTURBATIONS, "drop_source_events": {"count": 2}})
    with pytest.raises(PipelineError) as e:
        run_pipeline(ws)
    assert e.value.step == "recon" and "source_inventory" in e.value.message
    assert "order_delivered=" in e.value.message
    assert rows(ws, "select watermark from ctl.state")[0][0] == 0  # nothing published


def test_at10_freshness_reports_source_and_execution_age_separately(full7):
    r = CliRunner()
    ok = r.invoke(app, ["freshness", "--ws", str(full7)])
    assert ok.exit_code == 0 and "source (simulated)" in ok.output and "execution (real)" in ok.output
    stale_source = r.invoke(app, ["freshness", "--ws", str(full7), "--expected-through", "2099-01-01"])
    assert stale_source.exit_code == 2
    stale_run = r.invoke(app, ["freshness", "--ws", str(full7), "--max-age-hours", "0"])
    assert stale_run.exit_code == 2


def test_at10_cli_exits_nonzero_when_a_run_fails(base):
    ws = make_ws(base, "cli_fail", {**NO_PERTURBATIONS, "drop_source_events": {"count": 1}})
    res = CliRunner().invoke(app, ["run", "--ws", str(ws)])
    assert res.exit_code == 1
    status = CliRunner().invoke(app, ["status", "--ws", str(ws)])
    assert "failed" in status.output and "recon" in status.output
