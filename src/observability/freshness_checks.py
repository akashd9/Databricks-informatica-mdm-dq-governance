"""Freshness gate: runs before the medallion pipeline starts. If a source
hasn't landed new files within its configured SLA window, the job halts
here rather than building a golden record on stale inputs.
"""
from datetime import datetime, timezone
from src.config_loader import load

_sources = load("sources.yml")["sources"]

_stale = []
for source in _sources:
    try:
        files = dbutils.fs.ls(source["landing_path"])
    except Exception:
        _stale.append((source["name"], "landing path not found"))
        continue
    if not files:
        _stale.append((source["name"], "no files found"))
        continue

    latest_mtime = max(f.modificationTime for f in files) / 1000
    age_hours = (datetime.now(timezone.utc).timestamp() - latest_mtime) / 3600
    sla = source["freshness_sla_hours"]
    if age_hours > sla:
        _stale.append((source["name"], f"latest file is {age_hours:.1f}h old, SLA is {sla}h"))

if _stale:
    raise RuntimeError(f"Freshness gate failed for sources: {_stale}")

print(f"Freshness gate passed for {len(_sources)} sources.")
