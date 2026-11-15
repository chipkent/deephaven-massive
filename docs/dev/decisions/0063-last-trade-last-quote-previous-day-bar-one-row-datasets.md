# 63. One-row endpoints are datasets of their own

Date: 2026-10-29

Status: accepted

One-row endpoints are datasets of their own: `last_trade`, `last_quote`, and `previous_day_bar`,
each with `fetch()` and `poll()`. Datasets are named by what they are and verbs name table shapes,
so a `latest()` verb would break the second rule.
