"""Compare the published tables of two warehouse files (multiset, every business column).

    uv run python scripts/compare_workspaces.py A.duckdb B.duckdb

Exit code 0 when every published table is identical, 1 otherwise. Used to check that a workspace
built under Airflow (copied out of its Docker volume) equals a single-pass replay of the same
events. Operational values live only in ctl.* and are not compared.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))

from helpers import diff_published  # noqa: E402

if len(sys.argv) != 3:
    raise SystemExit(__doc__)
a, b = Path(sys.argv[1]), Path(sys.argv[2])
diff = diff_published(a, b)
print(diff)
con = duckdb.connect(str(a), read_only=True)
print("watermark/checkpoint of A:", con.execute("select watermark, checkpoint_date from ctl.state").fetchone())
print("run log of A:", con.execute("select kind, status, count(*) from ctl.run_log group by 1, 2 order by 1, 2").fetchall())
con.close()
ok = all(v == 0 for v in diff.values())
print("IDENTICAL" if ok else "DIFFERENCES")
raise SystemExit(0 if ok else 1)
