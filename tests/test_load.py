"""Loader: replay safety, contract enforcement, contradictions, tampering (AT-01, AT-10)."""

from __future__ import annotations

import hashlib
import json
import shutil

import duckdb
import pytest

from helpers import make_ws
from orderledger.db import connect
from orderledger.load import LoadError, load_batches


def _loaded(con):
    return con.execute("select count(*) from raw.events").fetchone()[0]


def _rewrite_batch(ws, n, select_sql):
    """Replace batch n's file and make the manifest agree, so only the content is wrong."""
    path = ws / "events" / f"batch_{n:05d}.parquet"
    tmp = ws / "events" / "tmp.parquet"
    c = duckdb.connect()
    c.execute(f"copy ({select_sql.format(src=path.as_posix())}) to '{tmp.as_posix()}' (format parquet)")
    rows = c.execute(f"select count(*) from read_parquet('{tmp.as_posix()}')").fetchone()[0]
    shutil.move(tmp, path)
    m = json.loads((ws / "events" / "manifest.json").read_text())
    entry = m["batches"][n - 1]
    entry["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    entry["row_count"] = rows
    (ws / "events" / "manifest.json").write_text(json.dumps(m))


def test_loading_twice_changes_nothing(base):
    ws = make_ws(base, "load_twice")
    con = connect(ws)
    first = load_batches(con, ws / "events", None)
    n = _loaded(con)
    second = load_batches(con, ws / "events", None)
    assert first.rows_loaded == n and second.batches_loaded == 0 and _loaded(con) == n


def test_breaking_schema_change_is_quarantined(base):
    ws = make_ws(base, "schema_change")
    _rewrite_batch(ws, 2, "select * exclude (payload), payload as body from read_parquet('{src}')")
    con = connect(ws)
    with pytest.raises(LoadError, match="breaking schema change"):
        load_batches(con, ws / "events", None)
    assert con.execute("select max(batch_no) from raw.load_log").fetchone()[0] == 1  # nothing after the gap
    assert "missing=['payload']" in con.execute("select reason from raw.quarantine").fetchone()[0]


def test_contradictory_event_is_quarantined_not_deduplicated(base):
    ws = make_ws(base, "contradiction")
    first = (ws / "events" / "batch_00001.parquet").as_posix()
    # batch 2 now re-sends batch 1's event_ids with different content
    _rewrite_batch(ws, 2, f"select * exclude (payload), payload || ' ' as payload from read_parquet('{first}')")
    con = connect(ws)
    with pytest.raises(LoadError, match="contradictory events"):
        load_batches(con, ws / "events", None)
    assert con.execute("select count(*) from raw.events where batch_no = 2").fetchone()[0] == 0


def test_two_event_ids_claiming_one_entity_version_are_a_contradiction(base):
    ws = make_ws(base, "contradiction2")
    first = (ws / "events" / "batch_00001.parquet").as_posix()
    _rewrite_batch(ws, 2, f"select * exclude (event_id), event_id || '-x' as event_id from read_parquet('{first}')")
    con = connect(ws)
    with pytest.raises(LoadError, match="claimed by different event_ids"):
        load_batches(con, ws / "events", None)


def test_tampered_file_is_rejected_by_hash(base):
    ws = make_ws(base, "tamper")
    with open(ws / "events" / "batch_00002.parquet", "ab") as f:
        f.write(b"\0")
    con = connect(ws)
    with pytest.raises(LoadError, match="file hash"):
        load_batches(con, ws / "events", None)
    assert con.execute("select count(*) from raw.quarantine").fetchone()[0] == 1


def test_missing_batch_file_stops_the_load(base):
    ws = make_ws(base, "missing_file")
    (ws / "events" / "batch_00003.parquet").unlink()
    con = connect(ws)
    with pytest.raises(LoadError, match="missing"):
        load_batches(con, ws / "events", None)
    assert con.execute("select max(batch_no) from raw.load_log").fetchone()[0] == 2
