"""Pure helpers the Airflow DAGs use. Standard library only, so the DAG environment (which does
not have DuckDB or dbt) can import it and the repository tests can check it without Airflow.

Airflow decides WHEN something runs. It never decides WHICH business date is processed:
a scheduled interval is mapped to a replay batch by ordinal position, not by the wall clock.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

# Exit codes of the orderledger CLI that Airflow acts on.
EXIT_OK = 0
EXIT_DETERMINISTIC = 1  # validation, contract, test or reconciliation failure: do NOT retry
EXIT_STALE_SOURCE = 2  # freshness breach: report, do NOT retry
EXIT_RETRYABLE = 75  # EX_TEMPFAIL: lock busy, crash, infrastructure error: retry is safe


def batch_for_interval(
    data_interval_start: dt.datetime, anchor: dt.datetime, period: dt.timedelta
) -> int:
    """Replay batch number for a scheduled interval: 1 for the interval that starts at `anchor`,
    2 for the next, and so on. Returns 0 or a negative number for intervals before the anchor.

    The mapping is positional. Only the position of the interval relative to the anchor matters,
    so processing the same history on a different day gives the same batches.
    """
    if period <= dt.timedelta(0):
        raise ValueError("period must be positive")
    # Integer microseconds: Airflow passes pendulum datetimes, whose differences are not plain
    # timedeltas, so avoid datetime arithmetic and compare timestamps. Both must be tz-aware.
    if data_interval_start.tzinfo is None or anchor.tzinfo is None:
        raise ValueError("interval start and anchor must be timezone-aware")
    delta = round((data_interval_start.timestamp() - anchor.timestamp()) * 1_000_000)
    steps, remainder = divmod(delta, period // dt.timedelta(microseconds=1))
    if remainder:
        raise ValueError(
            f"interval start {data_interval_start} is not aligned to anchor {anchor} + k*{period}"
        )
    return steps + 1


def manifest_batches(ws: Path) -> list[dict]:
    path = ws / "events" / "manifest.json"
    return json.loads(path.read_text(encoding="utf-8"))["batches"] if path.exists() else []


def period_to(ws: Path, batch_no: int) -> dt.date:
    """Simulated end date of a replay batch (what the published data must reach)."""
    for b in manifest_batches(ws):
        if b["batch_no"] == batch_no:
            return dt.date.fromisoformat(b["period_to"])
    raise KeyError(f"batch {batch_no} is not in the manifest of {ws}")


_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ParamError(ValueError):
    """Backfill parameters are invalid. Deterministic: never retried."""


def validate_backfill_params(date_from: str, date_to: str, auto: bool) -> dict:
    """Validate the backfill DAG parameters. Exactly one mode: auto, or an explicit date window."""
    date_from, date_to = (date_from or "").strip(), (date_to or "").strip()
    if auto:
        if date_from or date_to:
            raise ParamError("use either auto=true or an explicit date window, not both")
        return {"auto": True}
    if not (date_from and date_to):
        raise ParamError("give both date_from and date_to (YYYY-MM-DD), or set auto=true")
    for name, value in (("date_from", date_from), ("date_to", date_to)):
        if not _DATE.match(value):
            raise ParamError(f"{name} must look like YYYY-MM-DD, got {value!r}")
    try:
        a, b = dt.date.fromisoformat(date_from), dt.date.fromisoformat(date_to)
    except ValueError as e:
        raise ParamError(str(e)) from e
    if a > b:
        raise ParamError(f"date_from {a} is after date_to {b}")
    if (b - a).days > 400:
        raise ParamError("window longer than 400 days; split it")
    return {"auto": False, "date_from": str(a), "date_to": str(b)}


def parse_inject_setting(value: str) -> tuple[int, str] | None:
    """The Airflow Variable `orderledger_inject_transient` has the form `<batch>:<step>`
    (for example `3:marts`): crash once, after that step, while processing that batch."""
    value = (value or "").strip()
    if not value:
        return None
    m = re.fullmatch(r"(\d+):(facts|marts|tests|recon|publish)", value)
    if not m:
        raise ParamError(f"orderledger_inject_transient must look like '3:marts', got {value!r}")
    return int(m.group(1)), m.group(2)
