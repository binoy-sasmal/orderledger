from __future__ import annotations

import pytest

from helpers import (
    NO_PERTURBATIONS,
    build_incremental,
    make_ws,
    snapshot,
)
from orderledger.pipeline import run_pipeline


@pytest.fixture(scope="session")
def base(tmp_path_factory):
    return tmp_path_factory.mktemp("orderledger")


@pytest.fixture(scope="session")
def full7(base):
    """Weekly batches, every batch loaded and processed in ONE run (the full replay)."""
    ws = make_ws(base, "full7")
    run_pipeline(ws)
    return ws


@pytest.fixture(scope="session")
def inc7(base):
    """The same events, processed incrementally one weekly batch at a time."""
    ws = make_ws(base, "inc7")
    failures = build_incremental(ws, snapshots=(8,))
    return ws, failures


@pytest.fixture(scope="session")
def inc14(base):
    """The same events with 14-day batches: different boundaries, equivalent checkpoints."""
    ws = make_ws(base, "inc14", {"batch_days": 14})
    failures = build_incremental(ws, snapshots=(4,))
    return ws, failures


@pytest.fixture(scope="session")
def clean_full(base):
    """No synthetic events at all: source content only."""
    ws = make_ws(base, "clean", NO_PERTURBATIONS)
    run_pipeline(ws)
    return ws


__all__ = ["snapshot"]
