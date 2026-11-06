"""Warehouse file, schemas and control tables.

Schemas: raw (append-only history), ctl (state, run log, reconciliation results),
work (dbt build area, state carried between runs), published (what consumers read).
"""

from __future__ import annotations

from pathlib import Path

import duckdb

DDL = """
create schema if not exists raw;
create schema if not exists ctl;
create schema if not exists work;
create schema if not exists published;

create table if not exists raw.events (
  event_id varchar not null, entity_type varchar not null, entity_id varchar not null,
  event_type varchar not null, source_seq bigint not null, effective_ts timestamp,
  arrival_ts timestamp not null, arrival_seq bigint not null, payload varchar not null,
  payload_hash varchar not null, content_origin varchar not null, timing_origin varchar not null,
  scenario_tag varchar, batch_no integer not null, file_sha256 varchar not null);
create table if not exists raw.load_log (
  batch_no integer primary key, file_name varchar, file_sha256 varchar,
  row_count bigint, loaded_at timestamp);
create table if not exists raw.quarantine (
  batch_no integer, file_name varchar, reason varchar, quarantined_at timestamp);

create table if not exists ctl.state (
  id integer primary key, watermark bigint, checkpoint_date date,
  last_run_id varchar, published_at timestamp);
create table if not exists ctl.run_log (
  run_id varchar primary key, kind varchar, started_at timestamp, finished_at timestamp,
  status varchar, failed_step varchar, error varchar, through_batch integer,
  checkpoint_date date, batches_loaded integer, rows_loaded bigint,
  watermark_before bigint, watermark_after bigint, duration_s double);
create table if not exists ctl.reconciliation (
  run_id varchar, check_name varchar, status varchar, detail varchar, checked_at timestamp);
"""

PUBLISHED_TABLES = (
    "dim_date",
    "dim_customer",
    "dim_product",
    "fct_orders",
    "fct_order_items",
    "order_state_intervals",
    "agg_daily_purchases",
    "agg_daily_deliveries",
)


def db_path(ws: Path) -> Path:
    return ws / "wh.duckdb"


def connect(ws: Path, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    ws.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path(ws)), read_only=read_only)
    if not read_only:
        con.execute(DDL)
        con.execute(
            "insert into ctl.state select 1, 0, null, null, null "
            "where not exists (select 1 from ctl.state)"
        )
    return con
