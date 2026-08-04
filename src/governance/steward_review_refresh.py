# Databricks notebook source
"""Job-task entry point: refreshes the steward review queue after the
pipeline runs. Kept separate from steward_review.py (a plain importable
library — dq_gate.py imports get_approved_override_ids from it) so
importing the library never has the side effect of running a queue refresh.
"""
from src.governance.steward_review import setup_tables, refresh_queue

setup_tables()
refresh_queue()
