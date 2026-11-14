# 93. `dm.canonicalize_symbols` skips and logs a bad row by default

Date: 2026-11-03

Status: partly superseded: its reason, that one bad row never fails a table, by [107](0107-futures-history-venues-skip-exchanges-plan-refuses-refused-venues.md)

Its `on_error` default is `LOG`, following the rule that one bad row never fails a table.
