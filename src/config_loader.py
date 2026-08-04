"""Central config loader. Resolves paths relative to the repo root so the
same code works whether it's synced via Databricks Git folders, deployed by
a Databricks Asset Bundle, or imported locally for unit tests.
"""
from pathlib import Path
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CONFIG_DIR = _REPO_ROOT / "config"


def load(name: str) -> dict:
    with open(_CONFIG_DIR / name) as f:
        return yaml.safe_load(f)
