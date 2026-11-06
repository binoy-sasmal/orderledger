"""AT-13: the simulator is deterministic, labels what is synthetic and traces to source rows."""

from __future__ import annotations

import json

import duckdb

from helpers import FIXTURE, SCENARIOS, make_ws
from orderledger.simulate import load_scenario, simulate


def _glob(d):
    return (d / "batch_*.parquet").as_posix()


def test_same_input_gives_the_same_logical_events(tmp_path):
    sc = load_scenario(SCENARIOS / "ci.yml")
    simulate(FIXTURE, tmp_path / "a", sc)
    simulate(FIXTURE, tmp_path / "b", sc)
    con = duckdb.connect()
    a, b = _glob(tmp_path / "a"), _glob(tmp_path / "b")
    diff = con.execute(
        f"select count(*) from ((select * from read_parquet('{a}') except all select * from read_parquet('{b}')) "
        f"union all (select * from read_parquet('{b}') except all select * from read_parquet('{a}')))"
    ).fetchone()[0]
    assert diff == 0


def test_synthetic_events_are_always_labelled(base):
    ws = make_ws(base, "labels")
    con = duckdb.connect()
    g = _glob(ws / "events")
    q = lambda s: con.execute(s).fetchall()  # noqa: E731
    # every synthetic-content event names its scenario; source-content events never claim synthetic time
    assert q(f"select count(*) from read_parquet('{g}') where content_origin='synthetic-perturbation' and scenario_tag is null")[0][0] == 0
    assert q(f"select count(*) from read_parquet('{g}') where content_origin='source-field' and timing_origin='synthetic'")[0][0] == 0
    # snapshots without a time of their own carry no effective time
    assert q(f"select count(*) from read_parquet('{g}') where event_type in ('order_status_observed','product_observed') and effective_ts is not null")[0][0] == 0
    # cancellation is an observed status only: no event type or payload invents a cancellation time
    assert q(f"select count(*) from read_parquet('{g}') where event_type ilike '%cancel%'")[0][0] == 0
    # the snapshot arrives after every lifecycle event
    last_life, first_status = q(
        f"select (select max(arrival_ts) from read_parquet('{g}') where event_type <> 'order_status_observed'), "
        f"(select min(arrival_ts) from read_parquet('{g}') where event_type = 'order_status_observed')"
    )[0]
    assert first_status > last_life


def test_event_counts_match_the_source_inventory(base):
    ws = make_ws(base, "inventory")
    inv = json.loads((ws / "events" / "inventory.json").read_text())
    con = duckdb.connect()
    got = dict(con.execute(
        f"select event_type, count(distinct event_id) from read_parquet('{_glob(ws / 'events')}') "
        "where content_origin='source-field' group by 1").fetchall())
    for t, n in inv["event_counts"].items():
        assert got[t] == n, t


def test_arrival_order_does_not_depend_on_batch_size(base):
    a = make_ws(base, "bs7")
    b = make_ws(base, "bs14", {"batch_days": 14})
    con = duckdb.connect()
    q = (
        "select event_id, arrival_seq from read_parquet('{}') order by 2"
    )
    assert con.execute(q.format(_glob(a / "events"))).fetchall() == con.execute(q.format(_glob(b / "events"))).fetchall()
