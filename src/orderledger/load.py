"""Idempotent, validated ingestion of event batch files into raw.events.

A batch is loaded in one transaction or not at all. A file is skipped when its batch number
is already in raw.load_log with the same content hash (replaying a file never doubles
anything). A batch that fails a check is quarantined and the load stops, so no later batch
is applied over a gap.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import duckdb

from .contracts import (
    CONTENT_ORIGINS,
    EVENT_COLUMNS,
    EVENT_TYPE_ENTITY,
    SCHEMA_VERSION,
    TIMING_ORIGINS,
)


class LoadError(Exception):
    """A batch failed validation; it was quarantined and nothing from it was loaded."""


@dataclass
class LoadResult:
    batches_loaded: int
    rows_loaded: int
    last_batch: int
    checkpoint_date: dt.date | None
    complete: bool


def read_manifest(events_dir: Path) -> dict:
    return json.loads((events_dir / "manifest.json").read_text(encoding="utf-8"))


def _quote(values) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def _validate(con: duckdb.DuckDBPyConnection, path: Path, entry: dict, sha: str) -> str | None:
    """Return a reason string when the file must be quarantined, else None."""
    if sha != entry["sha256"]:
        return f"file hash {sha[:12]} differs from manifest {entry['sha256'][:12]}"
    p = path.as_posix()
    got = {r[0]: r[1] for r in con.execute(f"describe select * from read_parquet('{p}')").fetchall()}
    missing = [c for c in EVENT_COLUMNS if c not in got]
    extra = [c for c in got if c not in EVENT_COLUMNS]
    wrong = [f"{c}:{got[c]}!={t}" for c, t in EVENT_COLUMNS.items() if c in got and got[c] != t]
    if missing or extra or wrong:
        return f"breaking schema change: missing={missing} unexpected={extra} type_mismatch={wrong}"
    n = con.execute(f"select count(*) from read_parquet('{p}')").fetchone()[0]
    if n != entry["row_count"]:
        return f"row count {n} differs from manifest {entry['row_count']}"
    bad = con.execute(
        f"""select count(*) from read_parquet('{p}') where event_id is null or entity_id is null
              or payload is null or arrival_ts is null or source_seq is null or source_seq < 1
              or event_type not in ({_quote(EVENT_TYPE_ENTITY)})
              or content_origin not in ({_quote(CONTENT_ORIGINS)})
              or timing_origin not in ({_quote(TIMING_ORIGINS)})"""
    ).fetchone()[0]
    if bad:
        return f"{bad} rows violate the value contract"
    mismatch = " or ".join(
        f"(event_type = '{et}' and entity_type <> '{ent}')" for et, ent in EVENT_TYPE_ENTITY.items()
    )
    if con.execute(f"select count(*) from read_parquet('{p}') where {mismatch}").fetchone()[0]:
        return "entity_type does not match event_type"
    return None


def _contradictions(con: duckdb.DuckDBPyConnection, path: Path) -> str | None:
    """Same event_id with different content, or two event_ids claiming one entity version."""
    p = path.as_posix()
    base = f"""
      with incoming as (select event_id, entity_type, entity_id, source_seq, md5(payload) ph
                        from read_parquet('{p}')),
      allrows as (
        select event_id, entity_type, entity_id, source_seq, ph from incoming
        union all
        select event_id, entity_type, entity_id, source_seq, payload_hash from raw.events
         where entity_id in (select entity_id from incoming))"""
    a = con.execute(
        base + " select count(*) from (select event_id from allrows group by 1 "
        "having count(distinct ph) > 1)"
    ).fetchone()[0]
    b = con.execute(
        base + " select count(*) from (select entity_type, entity_id, source_seq from allrows "
        "group by 1, 2, 3 having count(distinct event_id) > 1)"
    ).fetchone()[0]
    if a or b:
        return (
            f"contradictory events: {a} event_id(s) with different content, "
            f"{b} entity version(s) claimed by different event_ids"
        )
    return None


def _quarantine(con: duckdb.DuckDBPyConnection, entry: dict, reason: str) -> None:
    con.execute(
        "insert into raw.quarantine values (?, ?, ?, now()::timestamp)",
        [entry["batch_no"], entry["file"], reason],
    )


def load_batches(con: duckdb.DuckDBPyConnection, events_dir: Path, through: int | None) -> LoadResult:
    manifest = read_manifest(events_dir)
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise LoadError(f"manifest schema_version {manifest.get('schema_version')} != {SCHEMA_VERSION}")
    batches = sorted(manifest["batches"], key=lambda b: b["batch_no"])
    limit = through if through is not None else batches[-1]["batch_no"]
    loaded = rows = 0
    for entry in batches:
        n = entry["batch_no"]
        if n > limit:
            break
        path = events_dir / entry["file"]
        done = con.execute("select file_sha256 from raw.load_log where batch_no = ?", [n]).fetchone()
        if done:
            if done[0] != entry["sha256"]:
                _quarantine(con, entry, "batch already loaded with different content")
                raise LoadError(f"batch {n}: already loaded with different content")
            continue
        if not path.exists():
            _quarantine(con, entry, "batch file missing")
            raise LoadError(f"batch {n}: file {entry['file']} is missing")
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        replay = con.execute(
            "select batch_no from raw.load_log where file_sha256 = ? and batch_no <> ?", [sha, n]
        ).fetchone()
        if replay and sha == entry["sha256"]:
            # The same bytes were already loaded under another batch number: record and skip.
            con.execute(
                "insert into raw.load_log values (?, ?, ?, 0, now()::timestamp)",
                [n, f"{entry['file']} (replay of batch {replay[0]})", sha],
            )
            continue
        reason = _validate(con, path, entry, sha) or _contradictions(con, path)
        if reason:
            _quarantine(con, entry, reason)
            raise LoadError(f"batch {n} quarantined: {reason}")
        con.execute("begin")
        try:
            con.execute(
                f"""insert into raw.events
                    select event_id, entity_type, entity_id, event_type, source_seq, effective_ts,
                           arrival_ts, arrival_seq, payload, md5(payload), content_origin,
                           timing_origin, scenario_tag, {n}, '{sha}'
                    from read_parquet('{path.as_posix()}')"""
            )
            con.execute(
                "insert into raw.load_log values (?, ?, ?, ?, now()::timestamp)",
                [n, entry["file"], sha, entry["row_count"]],
            )
            con.execute("commit")
        except Exception:
            con.execute("rollback")
            raise
        loaded += 1
        rows += entry["row_count"]
    # The checkpoint is the end of the last batch actually present in raw, whatever `through` was.
    last = con.execute("select coalesce(max(batch_no), 0) from raw.load_log").fetchone()[0]
    by_no = {b["batch_no"]: b for b in batches}
    checkpoint = dt.date.fromisoformat(by_no[last]["period_to"]) if last else None
    return LoadResult(loaded, rows, last, checkpoint, last == batches[-1]["batch_no"])
