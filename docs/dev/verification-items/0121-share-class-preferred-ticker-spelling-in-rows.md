# 121. Which spellings Massive writes in its rows for share-class and preferred stock tickers

Date: 2026-11-15

Status: open

Unknown: whether every dataset spells a share class or a preferred as Massive's ticker list
does, `BRK.B` and `KIMpL`.

Meanwhile the library reads each row's ticker back into that form and publishes it as
`Symbol`. A spelling it can't read is a library bug, handled under `unreadable_symbols`: by
default the row keeps a null `Symbol` and the text in `RawSymbol`, and the log names the text
once at ERROR.

Verified by `tests/datasets/test_wire_symbols_integration.py`. It records how `BRK.B`, `BF.B`,
`KIMpL`, and `BACpL` are spelled in the ticker list, the REST previous-day and grouped daily
bars, the day-aggregates flat file, and Benzinga's consensus ratings, and asserts that every
published `Symbol` is canonical. A spelling that differs from the ticker list goes into a report
to Massive, and the read-back learns it.
