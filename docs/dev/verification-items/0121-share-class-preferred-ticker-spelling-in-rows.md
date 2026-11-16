# 121. Which spellings Massive writes in its rows for share-class and preferred stock tickers

Date: 2026-11-15

Status: open

Unknown: whether Massive's rows spell a share class or a preferred as its ticker list does,
such as `BRK.B` and `KIMpL`.

Meanwhile the library reads each spelling it knows into that form, and by default keeps any
other in `RawSymbol`.

`tests/datasets/test_wire_symbols_integration.py` verifies it for REST bars, the day-aggregates
flat file, and Benzinga's consensus ratings.
