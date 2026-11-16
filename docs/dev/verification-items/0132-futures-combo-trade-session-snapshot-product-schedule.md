# 132. Whether a futures combo's snapshot product schedule puts its trades in Massive's session

Date: 2026-11-16

Status: open

Unknown: whether the schedule of the product a combo's snapshot row names
(`details.product_code`) puts each of its trades in the session Massive's own
`session_end_date` gives, on a weekday and across many combos, including inter-commodity
spreads (`CL:BZ N7-Q7`) and combos whose ticker does not start with that product code. An
earlier run checked three combos on a Sunday evening, which could not tell one product from
another.

Meanwhile a combo's streamed rows get a null `Date`.

Settled by `tests/ingest/test_futures_calendar_integration.py::test_combo_trade_sessions_by_snapshot_product`, which places the latest
trades of up to 30 combos in each candidate product's sessions and records agreements,
disagreements, and how many combos had candidates whose sessions differ (only those
discriminate).
