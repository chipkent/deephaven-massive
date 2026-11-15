# 120. Whether Massive reads tickers case-sensitively, and what it answers to a forex pair written with a dash

Date: 2026-11-14

Status: open

Any case is sent as written, and `C:EUR-USD` is refused, naming `C:EURUSD`, until read.
Settled by `tests/datasets/test_symbols_integration.py` (Stocks Starter, and Currencies Starter
for the pair; three requests), which records the unified snapshot of `AAPL`, `aapl`, `KIMpL`,
`kimpl`, and `BRK.B`, the previous day bar of `aapl`, and the quotes of `C:EUR-USD`.
