"""Export Airflow run records as JSON (no secrets). Run inside the API-server container:

    docker compose -f airflow/docker-compose.yaml exec -T airflow-apiserver python - \
        < airflow/export_evidence.py > docs/orchestration-run-records.json

Uses the REST API with a token obtained from the credentials already present in the container
environment. Credentials and tokens are never printed or written.
"""

from __future__ import annotations

import json
import os
import urllib.request

BASE = "http://localhost:8080"


def call(path: str, token: str | None = None, body: dict | None = None):
    req = urllib.request.Request(BASE + path, method="POST" if body else "GET")
    if body is not None:
        req.add_header("Content-Type", "application/json")
        req.data = json.dumps(body).encode()
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


token = call(
    "/auth/token",
    body={"username": os.environ["_AIRFLOW_WWW_USER_USERNAME"], "password": os.environ["_AIRFLOW_WWW_USER_PASSWORD"]},
)["access_token"]

KEEP_RUN = ("dag_run_id", "state", "run_type", "logical_date", "data_interval_start", "data_interval_end",
            "start_date", "end_date", "triggered_by", "conf")
KEEP_TI = ("task_id", "state", "try_number", "max_tries", "start_date", "end_date", "duration", "pool",
           "operator")
out: dict = {"airflow_version": call("/api/v2/version", token).get("version"), "dags": {}}
for dag_id in ("orderledger_replay", "orderledger_backfill"):
    runs = call(f"/api/v2/dags/{dag_id}/dagRuns?limit=100&order_by=logical_date", token)["dag_runs"]
    rec = []
    for run in runs:
        rid = run["dag_run_id"]
        tis = call(f"/api/v2/dags/{dag_id}/dagRuns/{rid}/taskInstances?limit=100", token)["task_instances"]
        tasks = []
        for ti in tis:
            item = {k: ti.get(k) for k in KEEP_TI}
            tries = call(f"/api/v2/dags/{dag_id}/dagRuns/{rid}/taskInstances/{ti['task_id']}/tries", token)
            item["attempts"] = [
                {"try_number": t.get("try_number"), "state": t.get("state"),
                 "start_date": t.get("start_date"), "end_date": t.get("end_date")}
                for t in tries.get("task_instances", [])
            ]
            tasks.append(item)
        rec.append({**{k: run.get(k) for k in KEEP_RUN}, "tasks": sorted(tasks, key=lambda t: t["task_id"])})
    out["dags"][dag_id] = rec
print(json.dumps(out, indent=1, default=str))
