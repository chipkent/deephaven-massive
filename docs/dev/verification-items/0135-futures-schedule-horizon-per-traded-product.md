# 135. How many days ahead each traded futures product's schedule reaches

Date: 2026-11-16

Status: open

Unknown: how far ahead Massive publishes each product's sessions. An earlier run found one
product's latest session in 2028 and read a window cut short by its page cap, so whether every
traded product reaches the 10 days ahead the calendar loads is unmeasured.

Meanwhile the calendar loads each product's sessions from 7 days back to 10 ahead.

Settled by `tests/ingest/test_futures_calendar_integration.py::test_schedule_horizon_per_traded_product`, which asks the latest session of
each product traded within 7 days (up to 250, spread evenly, else a lower bound) and records
the days ahead of each.
