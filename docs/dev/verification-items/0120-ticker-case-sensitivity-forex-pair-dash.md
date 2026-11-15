# 120. Whether Massive reads tickers case-sensitively, and what it answers to a forex pair written with a dash

Date: 2026-11-14

Status: open

`aapl` is refused, naming `AAPL`, and `C:EUR-USD` is refused, naming `C:EURUSD`, as forms that
are not canonical; the answer is recorded and reported, and both refusals stand whatever it is.
Settled by
`tests/datasets/test_symbols_integration.py` (three requests), which records the unified
snapshot of `AAPL`, `aapl`, `KIMpL`, `kimpl`, and `BRK.B`, the previous day bar of `aapl`, and
the quotes of `C:EUR-USD`.
