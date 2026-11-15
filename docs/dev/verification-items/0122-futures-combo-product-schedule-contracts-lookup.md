# 122. Whether a futures combo's product and schedule could date its streamed rows

Date: 2026-11-15

Status: open

Streamed futures rows carry no session date, so the futures calendar dates them from Massive's
schedules: it looks up a single contract's product in `/futures/v1/contracts` and loads that
product's sessions from `/futures/v1/schedules`. It never looks up a combo (a spread traded as
one instrument, such as `GCQ6-GCJ7` or `CL:BF Q6-U6-V6`), so every streamed combo row gets a
null `Date`. Unverified: whether a combo's product code is one `/futures/v1/products` lists,
whether `/futures/v1/schedules` serves sessions for it that hold the combo's trades, and whether
`/futures/v1/contracts` answers the calendar's lookup (`ticker.any_of`) for a combo at all.
Until settled, combo rows keep a null `Date`. Settled by
`tests/ingest/test_futures_calendar_integration.py::test_products_schedules_and_contracts_of_futures_combos`,
which records Massive's answers for a few CL and GC combos traded in the last 7 days.
