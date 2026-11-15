# 66. One `dm.OnError {RAISE, LOG, IGNORE}` for symbol problems, `LOG` by default

Date: 2026-10-29

Status: partly superseded: a crypto pair passed in code that Massive's crypto ticker list does not hold, and a table whose pairs are all such, by [104](0104-canonical-ticker-refused-with-fix-unlisted-pair-no-rows.md)

`MassiveConfig.noncanonical_symbols`, `unreadable_symbols`, and `unavailable_symbols` use it. A bad
row is skipped (or kept with a null `Symbol` and its `RawSymbol`) and listed in `m.streams()`
`Error`; a symbol table whose rows are all bad raises. A `str` or list in code always raises.
