# Databricks notebook source
"""Job-task entry point: refreshes the steward review queue after the
pipeline runs. Kept separate from steward_review.py (a plain importable
library — dq_gate.py imports get_approved_override_ids from it) so
importing the library never has the side effect of running a queue refresh.
"""

import os
import sys

# Ensures `from src.xxx import ...` resolves regardless of execution context
# (job notebook_task run vs module imported by another file) — job/DLT
# execution doesn't always add the bundle root to sys.path automatically.
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from src.governance.steward_review import setup_tables, refresh_queue

setup_tables()
refresh_queue()
