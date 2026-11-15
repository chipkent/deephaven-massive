# 61. A debug log good enough to fix a bug from

Date: 2026-10-28

Status: accepted

Standard `logging`, off by default, turned on by `DEEPHAVEN_MASSIVE_LOG`,
`DEEPHAVEN_MASSIVE_LOG_FILE`, or `dm.enable_debug_logging()`. Log offending values and whole rows,
never secrets.
