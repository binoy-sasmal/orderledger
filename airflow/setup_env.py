"""Write airflow/.env with generated secrets and the replay anchor (git-ignored).

    python airflow/setup_env.py [--period-seconds 180] [--catchup 3] [--force]

* Secrets (Fernet key, JWT secret, UI password) are generated locally and never printed.
* ORDERLEDGER_REPLAY_START is the start of the first scheduled interval. It is placed
  `--catchup` periods in the past so the scheduler creates that many runs at once (Airflow
  catchup) and then one more every period. It chooses WHEN batches are replayed, not WHICH
  business dates exist: interval k always replays batch k+1.
"""

from __future__ import annotations

import argparse
import base64
import os
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path

ENV = Path(__file__).with_name(".env")

ap = argparse.ArgumentParser()
ap.add_argument("--period-seconds", type=int, default=180)
ap.add_argument("--catchup", type=int, default=3, help="scheduled intervals created immediately")
ap.add_argument("--force", action="store_true", help="overwrite an existing .env (new secrets)")
a = ap.parse_args()

if ENV.exists() and not a.force:
    raise SystemExit(f"{ENV} exists; keep it, or pass --force to regenerate (this rotates secrets)")

now = datetime.now(UTC).replace(second=0, microsecond=0)
start = now - timedelta(seconds=a.period_seconds * a.catchup)
lines = [
    "AIRFLOW_UID=50000",
    f"FERNET_KEY={base64.urlsafe_b64encode(os.urandom(32)).decode()}",
    f"AIRFLOW__API_AUTH__JWT_SECRET={secrets.token_urlsafe(48)}",
    "_AIRFLOW_WWW_USER_USERNAME=airflow",
    f"_AIRFLOW_WWW_USER_PASSWORD={secrets.token_urlsafe(16)}",
    f"ORDERLEDGER_REPLAY_START={start.isoformat()}",
    f"ORDERLEDGER_REPLAY_PERIOD_SECONDS={a.period_seconds}",
]
ENV.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"wrote {ENV} (secrets not shown; the UI login is in that file)")
print(f"replay anchor {start.isoformat()}, period {a.period_seconds}s, {a.catchup} intervals due immediately")
