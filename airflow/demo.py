"""Scripted, self-checking Airflow demonstration (about 20 minutes; Docker must be running).

    uv run python airflow/demo.py

Rebuilds the local Airflow stack from scratch and drives the scenarios below, asserting the
outcome of each. Exit code 0 only if every assertion holds. It uses the synthetic CI fixture, so
no Kaggle credentials are needed, and it prints no secrets.

  1. scheduler-triggered replay runs, one scheduled interval per replay batch
  2. a transient failure injected once into batch 2 -> automatic retry succeeds
  3. batch 4: deterministic reconciliation failure -> NOT retried; last publication preserved
  4. backfill DAG: invalid parameters rejected without touching the warehouse
  5. backfill DAG (auto) while an outside writer holds the lock -> rejected, retried, repaired
  6. the failed batch is cleared -> succeeds; the replay continues
  7. batch 7: a second stale date repaired with an explicit date window
  8. an interval beyond the last batch is skipped, not failed
  9. the final published tables equal a single-pass replay of the same events
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-f", str(ROOT / "airflow" / "docker-compose.yaml")]
REPLAY, BACKFILL = "orderledger_replay", "orderledger_backfill"
CLI = "/opt/orderledger-venv/bin/orderledger"
LOGS = "/opt/airflow/logs"
results: list[tuple[str, bool, str]] = []


def sh(args: list[str], check: bool = True, stdin: str | None = None) -> subprocess.CompletedProcess:
    env = {**os.environ, "MSYS_NO_PATHCONV": "1", "UV_LINK_MODE": "copy"}
    res = subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="replace",
                         env=env, input=stdin)
    if check and res.returncode != 0:
        raise SystemExit(f"command failed ({res.returncode}): {' '.join(args[:8])}\n"
                         f"{res.stdout[-800:]}\n{res.stderr[-800:]}")
    return res


def ex(*args: str, check: bool = True) -> str:
    """Run a command inside the scheduler container."""
    return sh([*COMPOSE, "exec", "-T", "airflow-scheduler", *args], check=check).stdout


def af(*args: str, check: bool = True) -> str:
    return ex("airflow", *args, check=check)


def json_out(text: str):
    """The Airflow CLI prints log noise before a JSON payload."""
    starts = [i for i in (text.find("["), text.find("{")) if i >= 0]
    return json.loads(text[min(starts):]) if starts else []


def run_state(dag: str, run_id: str) -> str | None:
    for r in json_out(af("dags", "list-runs", dag, "-o", "json")):
        if r.get("run_id") == run_id:
            return r.get("state")
    return None


def task_states(dag: str, run_id: str) -> dict[str, str]:
    return {r["task_id"]: r["state"] for r in json_out(af("tasks", "states-for-dag-run", dag, run_id, "-o", "json"))}


def wait(cond, what: str, timeout: int = 900, every: int = 8):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = cond()
        if value:
            return value
        time.sleep(every)
    raise SystemExit(f"timed out waiting for: {what}")


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f": {detail}" if detail else ""), flush=True)


def attempts(dag: str, run_id: str, task: str) -> list[int]:
    out = ex("ls", f"{LOGS}/dag_id={dag}/run_id={run_id}/task_id={task}/", check=False)
    return sorted(int(m) for m in re.findall(r"attempt=(\d+)\.log", out))


def log_text(dag: str, run_id: str, task: str, attempt: int) -> str:
    raw = ex("cat", f"{LOGS}/dag_id={dag}/run_id={run_id}/task_id={task}/attempt={attempt}.log", check=False)
    events = []
    for line in raw.splitlines():
        try:
            events.append(str(json.loads(line).get("event", "")))
        except ValueError:
            continue
    return "\n".join(events)


def status() -> str:
    return ex(CLI, "status", "--ws", "/data/ws")


def published() -> tuple[int, str]:
    m = re.search(r"watermark=(\d+) checkpoint=(\S+)", status())
    return int(m.group(1)), m.group(2)


def main() -> None:
    py = sys.executable
    print("== setup: fresh stack, image pinned by digest, generated secrets (never printed)")
    sh([*COMPOSE, "down", "-v"], check=False)
    sh([py, str(ROOT / "airflow" / "setup_env.py"), "--force", "--catchup", "2"])
    sh([*COMPOSE, "build"])
    chk = sh([*COMPOSE, "run", "--rm", "--no-deps", "--entrypoint", "python", "airflow-scheduler",
              "/opt/airflow/check_dags.py"])
    check("DAG import and structure checks pass in the Airflow image", "DAG CHECKS OK" in chk.stdout)
    sh([*COMPOSE, "up", "-d"])
    wait(lambda: sh([*COMPOSE, "ps", "--format", "{{.Status}}"]).stdout.count("healthy") >= 4,
         "stack healthy", 300, 5)

    env = dict(ln.split("=", 1) for ln in (ROOT / "airflow" / ".env").read_text().splitlines() if "=" in ln)
    anchor = dt.datetime.fromisoformat(env["ORDERLEDGER_REPLAY_START"])
    period = dt.timedelta(seconds=int(env["ORDERLEDGER_REPLAY_PERIOD_SECONDS"]))

    def logical(n: int) -> str:  # logical date (interval start) of replay batch n
        return (anchor + (n - 1) * period).isoformat()

    def rid(n: int) -> str:
        return f"scheduled__{logical(n)}"

    version = [ln for ln in af("version").splitlines() if ln.strip()][-1].strip()
    check("Airflow version is 3.4.0", version == "3.4.0", version)
    check("single-slot writer pool exists", re.search(r"orderledger_warehouse\s*\|\s*1\b", af("pools", "list")) is not None)

    af("variables", "set", "orderledger_inject_transient", "2:marts")
    af("dags", "unpause", REPLAY)
    af("dags", "unpause", BACKFILL)

    print("\n== 1+2: scheduler-triggered runs; one injected transient failure, retried")
    wait(lambda: run_state(REPLAY, rid(1)) == "success", "batch 1 run succeeds")
    check("a scheduler-created run succeeded (batch 1)", rid(1).startswith("scheduled__"), rid(1))
    wait(lambda: run_state(REPLAY, rid(2)) == "success", "batch 2 run succeeds after its retry")
    n2 = attempts(REPLAY, rid(2), "run_batch")
    a1, a2 = log_text(REPLAY, rid(2), "run_batch", 1), log_text(REPLAY, rid(2), "run_batch", 2)
    check("batch 2: attempt 1 failed once with a retryable exit code (75)",
          n2[:2] == [1, 2] and "(exit code 75)" in a1 and "injected transient failure" in a1)
    check("batch 2: the retry succeeded without a second failure", "(exit code 0)" in a2 and len(n2) == 2)
    check("batch 2: the crashed attempt is on record in the workspace run log",
          re.search(r"failed \| marts", status()) is not None)

    print("\n== 3: deterministic reconciliation failure is not retried")
    wait(lambda: run_state(REPLAY, rid(3)) == "success", "batch 3 run succeeds")
    batches = json.loads(ex("cat", "/data/ws/events/manifest.json"))["batches"]
    wait(lambda: run_state(REPLAY, rid(4)) == "failed", "batch 4 run fails")
    wm_before, cp_before = published()
    b4 = attempts(REPLAY, rid(4), "run_batch")
    l4 = log_text(REPLAY, rid(4), "run_batch", 1)
    check("batch 4 failed once and was NOT retried", b4 == [1], f"attempts={b4}")
    check("batch 4 failure is classified deterministic (exit 1, stale dates named)",
          "(deterministic)" in l4 and "(exit code 1)" in l4 and "stale aggregate dates" in l4)
    check("last validated publication preserved (checkpoint is still the end of batch 3)",
          cp_before == batches[2]["period_to"], f"checkpoint={cp_before}, watermark={wm_before}")
    check("the freshness task did not run on unvalidated data",
          task_states(REPLAY, rid(4)).get("check_freshness") == "upstream_failed")

    print("\n== 4: backfill parameters are validated")
    af("dags", "trigger", BACKFILL, "--run-id", "bf-invalid-params", "--conf", '{"date_from": "2018-01-30"}')
    wait(lambda: run_state(BACKFILL, "bf-invalid-params") == "failed", "invalid backfill rejected", 180)
    ts = task_states(BACKFILL, "bf-invalid-params")
    check("invalid parameters rejected by validate, repair never ran",
          ts.get("validate") == "failed" and ts.get("repair") == "upstream_failed", str(ts))
    check("validation failure was not retried", attempts(BACKFILL, "bf-invalid-params", "validate") == [1])
    check("warehouse untouched by the rejected backfill", published() == (wm_before, cp_before))

    print("\n== 5: repair through Airflow while an outside writer holds the lock")
    holder = subprocess.Popen([*COMPOSE, "exec", "-T", "airflow-scheduler", CLI, "hold-lock", "--ws", "/data/ws",
                               "--seconds", "25"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(3)
    af("dags", "trigger", BACKFILL, "--run-id", "bf-auto-repair", "--conf", '{"auto": true}')
    wait(lambda: run_state(BACKFILL, "bf-auto-repair") in ("success", "failed"), "auto backfill finishes", 400)
    holder.wait(timeout=120)
    ra = attempts(BACKFILL, "bf-auto-repair", "repair")
    first = log_text(BACKFILL, "bf-auto-repair", "repair", ra[0])
    last = log_text(BACKFILL, "bf-auto-repair", "repair", ra[-1])
    check("the overlapping writer was rejected (exit 75) and Airflow retried",
          len(ra) >= 2 and "another writer holds the lock" in first and "(exit code 75)" in first, f"attempts={ra}")
    check("the auto backfill succeeded and passed reconciliation",
          run_state(BACKFILL, "bf-auto-repair") == "success" and "(exit code 0)" in last
          and "facts_to_marts: incremental aggregates equal a full recomputation" in last)
    wm_after, _ = published()
    check("the repair advanced the watermark", wm_after > wm_before, f"{wm_before} -> {wm_after}")

    print("\n== 6: clear the failed batch; the replay continues")
    af("tasks", "clear", REPLAY, "--task-regex", "run_batch|check_freshness", "--start-date", logical(4),
       "--end-date", logical(4), "--yes")
    wait(lambda: run_state(REPLAY, rid(4)) == "success", "batch 4 rerun succeeds")
    check("batch 4 succeeded on a deliberate rerun after the repair",
          "(exit code 0)" in log_text(REPLAY, rid(4), "run_batch", 2))

    print("\n== 7: a second stale date, repaired with an explicit window")
    wait(lambda: run_state(REPLAY, rid(7)) == "failed", "batch 7 run fails", 900)
    l7 = log_text(REPLAY, rid(7), "run_batch", 1)
    m = re.search(r"backfill --from (\d{4}-\d\d-\d\d) --to (\d{4}-\d\d-\d\d)", l7)
    check("batch 7: deterministic failure naming the stale dates, not retried",
          m is not None and "(exit code 1)" in l7 and attempts(REPLAY, rid(7), "run_batch") == [1])
    af("dags", "trigger", BACKFILL, "--run-id", "bf-explicit-repair", "--conf",
       json.dumps({"date_from": m.group(1), "date_to": m.group(2)}))
    wait(lambda: run_state(BACKFILL, "bf-explicit-repair") in ("success", "failed"), "explicit backfill", 400)
    check("explicit-window backfill succeeded", run_state(BACKFILL, "bf-explicit-repair") == "success")
    af("tasks", "clear", REPLAY, "--task-regex", "run_batch|check_freshness", "--start-date", logical(7),
       "--end-date", logical(7), "--yes")
    wait(lambda: run_state(REPLAY, rid(7)) == "success", "batch 7 rerun succeeds")

    print("\n== 8: an interval beyond the last batch is skipped")
    last_batch = len(batches)
    wait(lambda: run_state(REPLAY, rid(last_batch + 1)) == "success", "beyond-last interval completes", 600)
    ts = task_states(REPLAY, rid(last_batch + 1))
    check("interval beyond the last batch skipped, not failed",
          ts.get("plan") == "skipped" and ts.get("run_batch") == "skipped", str(ts))

    print("\n== 9: Airflow-built tables equal a single-pass replay")
    af("dags", "pause", REPLAY)
    stamp = dt.datetime.now().strftime("%H%M%S")
    ref = ROOT / "warehouse" / f"af_ref_{stamp}"
    cli = [py, "-c", "from orderledger.cli import app; app()"]
    sh([*cli, "simulate", str(ROOT / "tests" / "fixtures" / "synth"), "--scenario",
        str(ROOT / "scenarios" / "airflow-demo.yml"), "--ws", str(ref)])
    sh([*cli, "run", "--ws", str(ref)])
    copied = ROOT / "warehouse" / f"af_airflow_{stamp}.duckdb"
    sh([*COMPOSE, "cp", "airflow-scheduler:/data/ws/wh.duckdb", str(copied)])
    cmp_ = sh([py, str(ROOT / "scripts" / "compare_workspaces.py"), str(copied), str(ref / "wh.duckdb")], check=False)
    print(cmp_.stdout)
    check("final published tables identical to a single-pass replay (all 8 tables, 0 differing rows)",
          cmp_.returncode == 0 and "IDENTICAL" in cmp_.stdout)
    records = ROOT / "docs" / "orchestration-run-records.json"
    exported = sh([*COMPOSE, "exec", "-T", "airflow-apiserver", "python", "-"],
                  stdin=(ROOT / "airflow" / "export_evidence.py").read_text(encoding="utf-8"))
    records.write_text(exported.stdout, encoding="utf-8")
    check("run records exported without secrets",
          not any(v and v in exported.stdout for k, v in env.items()
                  if k in ("FERNET_KEY", "AIRFLOW__API_AUTH__JWT_SECRET", "_AIRFLOW_WWW_USER_PASSWORD")))

    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
