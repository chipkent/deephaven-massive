# 121. Which spellings Massive writes in its rows for share-class and preferred stock tickers

Date: 2026-11-15

Status: open

Unknown: whether Massive's rows spell a share class or a preferred as its ticker list does,
such as `BRK.B` and `KIMpL`.

Meanwhile the library publishes only the ticker list's spelling. By default a row in any other spelling
gets a null `Symbol` and its text in `RawSymbol`. A flat-file read of named symbols never sees
such a row.

`tests/datasets/test_wire_symbols_integration.py` verifies it for REST bars, the day-aggregates
flat file, and Benzinga's consensus ratings.
