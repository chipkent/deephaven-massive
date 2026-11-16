# 86. Whether the forex conversion's `last` object is the last quote of its `from`/`to` pair, as `/v1/last_quote/currencies/{from}/{to}` and the v2 forex snapshot's `lastQuote` give it, or the quote of another pair: Massive's conversion sample for AUD to USD shows a bid of 1.3672596, its last-quote sample for AUD/USD 0.73122

Date: 2026-11-11

Status: settled 2026-11-12: the forex conversion's `last` is the last quote of its `from`/`to` pair

Kept as `Last*` until read, not `LastQuote*`. Settled by
`tests/massive/refresh/test_overrides_integration.py` (Currencies Starter), which compares the three
in both directions of AUD/USD when they carry one quote and flags a conversion quote that matches
the inverse pair's.

Settled 2026-11-12: in both directions of AUD/USD, 7 of 8 tries read one quote on the conversion,
`/v1/last_quote`, and the snapshot's `lastQuote`; the conversion's columns now take the snapshot's
`LastQuote*` names.
