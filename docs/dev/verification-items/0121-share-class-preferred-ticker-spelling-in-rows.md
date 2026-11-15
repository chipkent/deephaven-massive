# 121. Which spellings Massive writes in its rows for share-class and preferred stock tickers

Date: 2026-11-15

Status: open

The library publishes every `Symbol` in Massive's canonical form (`BRK.B`, `KIMpL`) and reads a
row's ticker back into it; a spelling it cannot read keeps a null `Symbol`, the text in
`RawSymbol`, and one WARNING naming the text, under `unreadable_symbols`, until read. Verified by
`tests/datasets/test_wire_symbols_integration.py`, which records the raw spellings of `BRK.B`,
`BF.B`, `KIMpL`, and `BACpL` in Massive's ticker list, REST previous-day bars and grouped daily
bars, the day-aggregates flat file, and Benzinga's consensus ratings, and asserts that every
published `Symbol` is canonical. A spelling Massive writes otherwise than its ticker list is
evidence for a report to Massive, and for extending the read-back.
