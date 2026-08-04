# Databricks notebook source
"""Job-task entry point: applies approved glossary submissions before the
governance gate checks coverage, so an approval taking effect on the very
next pipeline run (not just "eventually, next time someone remembers to run
this") is the normal case, not a manual step.
"""
from src.governance.glossary_submissions import setup_tables, apply_approved_submissions

setup_tables()
apply_approved_submissions()
