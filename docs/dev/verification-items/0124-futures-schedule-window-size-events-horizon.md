# 124. How large every futures product's schedule over the calendar's window is, and how far ahead schedules go

Date: 2026-11-15

Status: open

Loading every product's schedule up front costs as many requests as the window's events fill
pages of 1,000, and dates rows only as far ahead as Massive publishes sessions; Massive
documents neither. Until settled, the calendar loads schedules per product as contracts appear.
Settled by `tests/ingest/test_futures_calendar_integration.py::test_schedule_window_size_and_horizon`, which reads every product's events
from 7 days back to 10 ahead, up to 30 pages (beyond them its counts are lower bounds, and it
says so), and the latest session Massive holds. That last request sorts by a column Massive
does not document; if Massive refuses it (HTTP 400), the test records the refusal and passes
without the horizon, so read its evidence line before taking the horizon as settled.
