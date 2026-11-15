# 119. Whether the library's symbol rule accepts every ticker Massive itself lists, and converts it to and from each transport unchanged

Date: 2026-11-14

Status: open

The rule refuses only text with whitespace at either end, a control character, `,`, `*`, a
missing class prefix, or a forex or crypto pair written with `-` or `/`; any case is sent.
Verified by `tests/datasets/test_symbols_integration.py` (Stocks, Indices, Currencies, Options,
and Futures Starter), which reads every page of the stocks, indices, forex, and crypto ticker
lists and of today's futures contracts, single and combo, and every current options contract
of 14 sampled underlyings, since the full options list holds millions. Estimated at about 140
requests and 5 minutes, unpaced; each case records its count and the characters its tickers
use.
