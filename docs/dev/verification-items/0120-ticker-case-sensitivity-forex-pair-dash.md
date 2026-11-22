# 120. Whether Massive reads tickers case-sensitively, and what it answers to a forex pair written with a dash

Date: 2026-11-14

Status: retired

Retired 2026-11-15, because no answer changes the library. It refuses `aapl` and `C:EUR-USD`
because neither is Massive's canonical ticker, and it names the form to write, `AAPL` and
`C:EURUSD`.

`tests/datasets/test_symbols_integration.py` still records Massive's answer, in three requests,
for the live-run report.
