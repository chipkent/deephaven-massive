# 93. `dm.canonicalize_symbols` skips and logs a bad row by default

Date: 2026-11-03

Status: accepted

Its `on_error` default is `LOG`, following the rule that one bad row never fails a table.
