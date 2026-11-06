"""Scripted demo: normal run, failure and retry, late data, backfill, reconciliation.

    uv run python scripts/demo.py [csv_dir] [--ws warehouse/demo] [--batch-days 14]

Runs the real CLI end to end and prints a transcript (also written to <ws>/demo-transcript.txt).
Defaults to the synthetic CI fixture; point csv_dir at data/olist for the real dataset.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLI = [sys.executable, "-c", "from orderledger.cli import app; app()"]
LOG: list[str] = []


def say(text: str = "") -> None:
    print(text)
    LOG.append(text)


def cli(*args: str, expect_fail: bool = False) -> subprocess.CompletedProcess:
    say(f"\n$ orderledger {' '.join(args)}")
    res = subprocess.run([*CLI, *args], capture_output=True, text=True, cwd=ROOT, encoding="utf-8", errors="replace")
    out = "\n".join(ln for ln in (res.stdout + res.stderr).splitlines() if "VIRTUAL_ENV" not in ln)
    say(out.rstrip())
    say(f"(exit code {res.returncode})")
    if (res.returncode != 0) != expect_fail:
        raise SystemExit(f"unexpected exit code {res.returncode} for: {' '.join(args)}")
    return res


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_dir", nargs="?", default=str(ROOT / "tests/fixtures/synth"))
    ap.add_argument("--ws", default=str(ROOT / "warehouse/demo"))
    ap.add_argument("--batch-days", type=int, default=14)
    a = ap.parse_args()
    ws = Path(a.ws)
    if ws.exists():
        shutil.rmtree(ws)
    scenario = ws.parent / "demo-scenario.yml"
    scenario.parent.mkdir(parents=True, exist_ok=True)
    base = (ROOT / "scenarios/ci.yml").read_text(encoding="utf-8")
    scenario.write_text(base + f"batch_days: {a.batch_days}\n", encoding="utf-8")

    say("== 1. Build the replayable change history (synthetic events are labelled as such)")
    cli("simulate", a.csv_dir, "--scenario", str(scenario), "--ws", str(ws))

    say("\n== 2. Normal incremental runs")
    cli("run", "--ws", str(ws), "--through", "1")
    cli("run", "--ws", str(ws), "--through", "2")

    say("\n== 3. A crash after the aggregates are built: nothing is published, the retry is correct")
    cli("run", "--ws", str(ws), "--through", "3", "--fail-after", "marts", expect_fail=True)
    cli("status", "--ws", str(ws))
    cli("run", "--ws", str(ws), "--through", "3")

    say("\n== 4. Keep going until a late correction lands outside the lookback")
    n = 4
    while n <= 99:
        res = subprocess.run([*CLI, "run", "--ws", str(ws), "--through", str(n)], capture_output=True,
                             text=True, cwd=ROOT, encoding="utf-8", errors="replace")
        if res.returncode != 0:
            say(f"\n$ orderledger run --ws {ws} --through {n}")
            say("\n".join(ln for ln in (res.stdout + res.stderr).splitlines() if "VIRTUAL_ENV" not in ln))
            say(f"(exit code {res.returncode})  <- reconciliation refused to publish")
            break
        say(f"batch {n}: ok")
        n += 1
    else:
        raise SystemExit("no stale date appeared in this scenario")

    say("\n== 5. Targeted backfill of exactly the dates reconciliation named, then reconcile")
    cli("backfill", "--ws", str(ws), "--auto")
    say("\n== 6. Finish the remaining batches and check the final state")
    while True:
        res = subprocess.run([*CLI, "run", "--ws", str(ws), "--through", str(n + 1)], capture_output=True,
                             text=True, cwd=ROOT, encoding="utf-8", errors="replace")
        n += 1
        say(f"batch {n}: " + ("ok" if res.returncode == 0 else "failed"))
        if res.returncode != 0:
            cli("backfill", "--ws", str(ws), "--auto")
        if "batches_loaded=0" in res.stdout and res.returncode == 0:
            break
        if n > 99:
            break
    cli("reconcile", "--ws", str(ws))
    cli("freshness", "--ws", str(ws))
    cli("status", "--ws", str(ws))
    (ws / "demo-transcript.txt").write_text("\n".join(LOG), encoding="utf-8")


if __name__ == "__main__":
    main()
