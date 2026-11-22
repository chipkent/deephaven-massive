# 135. How many days ahead does each traded futures product's schedule reach?

Date: 2026-11-16

Status: open

How far ahead Massive publishes each product's sessions is unknown. An earlier run found one
product's latest session in 2028 and read a window cut short by its page cap. If every traded
product reaches past the 10 days ahead the calendar loads, the window holds. If some fall short,
rows dated past their last session cannot be placed, and the calendar must reload them sooner or
say so. Until it is read, the calendar loads each product's sessions from 7 days back to 10
ahead.

Read by `tests/ingest/test_futures_calendar_integration.py::test_schedule_horizon_per_traded_product`, on a key with futures data. It asks
the latest session of each product traded within 7 days, up to 250 spread evenly (else a lower
bound), and records the days ahead of each.
