# Databricks notebook source
"""Job-task entry point: applies approved glossary submissions before the
governance gate checks coverage, so an approval taking effect on the very
next pipeline run (not just "eventually, next time someone remembers to run
this") is the normal case, not a manual step.
"""

import os
import sys

# Ensures `from src.xxx import ...` resolves regardless of execution context
# (job notebook_task run vs module imported by another file) — job/DLT
# execution doesn't always add the bundle root to sys.path automatically.
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from src.governance.glossary_submissions import setup_tables, apply_approved_submissions

setup_tables()
apply_approved_submissions()
