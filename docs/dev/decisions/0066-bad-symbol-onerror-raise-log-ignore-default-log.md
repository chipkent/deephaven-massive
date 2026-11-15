# 66. One `dm.OnError {RAISE, LOG, IGNORE}` for symbol problems, `LOG` by default

Date: 2026-10-29

Status: partly superseded: `noncanonical_symbols` by [104](0104-ticker-sent-as-written-prefix-pair-separator-padding-refused.md)

`MassiveConfig.noncanonical_symbols`, `unreadable_symbols`, and `unavailable_symbols` use it. A bad
row is skipped (or kept with a null `Symbol` and its `RawSymbol`) and listed in `m.streams()`
`Error`; a symbol table whose rows are all bad raises. A `str` or list in code always raises.
