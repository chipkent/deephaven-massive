# 129. Whether the futures contracts list holds a combo when asked for that combo alone

Date: 2026-11-16

Status: open

Unknown: whether `/futures/v1/contracts` lists every combo. An earlier run asked for three
combos at once and got one full page of 1,000 rows, sorted by product code, with none of
`GCZ6-GCG7`'s, so that combo may have been cut off by the page rather than missing.

Meanwhile the futures calendar looks up only single contracts in the contracts list, with no
`date`.

Settled by `tests/ingest/test_futures_calendar_integration.py::test_combo_tickers_in_contracts_list_one_at_a_time`, which asks for each of up
to nine combos alone (every date; the latest listed day; that day and `type=combo`) and for
them together with that day, and records the rows each read returned.
