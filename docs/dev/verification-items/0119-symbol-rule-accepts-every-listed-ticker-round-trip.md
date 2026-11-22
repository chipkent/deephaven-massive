# 119. Whether the library's symbol grammar accepts every ticker Massive itself lists, and converts it to and from each transport unchanged

Date: 2026-11-14

Status: open

The grammar of each class (decision 58) refuses any other form, naming the form to write, so a
listed form it refuses is a library defect to fix. Verified by
`tests/datasets/test_symbols_integration.py`, which reads every page of the stocks, indices,
forex, and crypto ticker lists and of today's futures contracts, single and combo, and every
current options contract of 14 sampled underlyings, since the full options list holds millions.
Estimated at about 140 requests and 5 minutes unpaced; each case records its count and the
characters its tickers use. Known forms to watch: Massive's option-chain snapshot sample writes
a nine-digit strike (`O:AAPL211022C000150000`), which the grammar refuses; whether Massive lists
such a contract is unproven.
