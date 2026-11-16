# 129. Does the futures contracts list hold a combo when asked for that combo alone?

Date: 2026-11-16

Status: open

An earlier run asked `/futures/v1/contracts` for three combos at once and got one full page of
1,000 rows, sorted by product code, with none of `GCZ6-GCG7`'s. That combo may have been cut
off by the page rather than missing. If the list holds every combo, a calendar can find a combo's
product there, a page per batch with `date`. If it lacks some, the snapshot must be the source
for them. Until it is read, the calendar looks up only single contracts, with no `date`.

Read by `tests/ingest/test_futures_calendar_integration.py::test_combo_tickers_in_contracts_list_one_at_a_time`, on a key with futures data.
It asks for each of up to nine combos alone three ways (every date; the latest listed day; that
day with `type=combo`), then for all of them together on that day.
