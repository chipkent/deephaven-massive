# 66. One `dm.OnError {RAISE, LOG, IGNORE}` for symbol problems, `LOG` by default

Date: 2026-10-29

Status: partly superseded: a crypto pair in code that Massive has no data for, by [104](0104-symbol-canonical-ticker-or-refused-no-data-gives-no-rows.md); a bad row kept with a null `Symbol` and its `RawSymbol`, by [112](0112-data-table-holds-only-rows-read-whole-unreadable-rows-withheld.md); where a bad row is listed (`m.problems()`, not `m.streams()` `Error`), by [124](0124-problems-table-lists-every-feed-problem-log-only-alarms.md)

`MassiveConfig.noncanonical_symbols`, `unreadable_symbols`, and `unavailable_symbols` use it. A bad
row is skipped (or kept with a null `Symbol` and its `RawSymbol`) and listed in `m.streams()`
`Error`; a symbol table whose rows are all bad raises. A `str` or list in code always raises.
