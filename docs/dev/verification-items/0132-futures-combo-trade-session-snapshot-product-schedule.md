# 132. Does a futures combo's snapshot product schedule put its trades in Massive's session?

Date: 2026-11-16

Status: open

A combo's snapshot row names a product (`details.product_code`). Whether that product's schedule
puts each of the combo's trades in the session Massive's own `session_end_date` gives is
unknown on a weekday, across many combos, including inter-commodity spreads (`CL:BZ N7-Q7`)
and combos whose ticker does not start with the product code. An earlier run checked three
combos on a Sunday evening, which could not tell one product from another. If it does, combos
are dated like single contracts. If it does not, combo rows keep a null `Date`. Until it is
read, combo rows get a null `Date`.

Read by `tests/ingest/test_futures_calendar_integration.py::test_combo_trade_sessions_by_snapshot_product`, on a key with futures data, run
on a weekday. It places the latest trades of up to 30 combos in each candidate product's
sessions and records agreements and disagreements. It also records how many combos have a trade
that two candidates place in different sessions. Only those trades discriminate: candidates
whose schedules differ can still place every trade read alike, and with no such trade the
question stays open.
