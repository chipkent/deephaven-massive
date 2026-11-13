# 47. The column reference cannot hold an orphaned page

Date: 2026-10-26

Status: accepted

`scripts/render_reference.py` owns `docs/datasets/`: it writes every page and deletes any file it
did not render, and a parity test fails on any difference.
