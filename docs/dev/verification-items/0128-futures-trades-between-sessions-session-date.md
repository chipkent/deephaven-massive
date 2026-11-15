# 128. Whether Massive sends futures trades between scheduled sessions, and which session it gives them

Date: 2026-11-15

Status: open

The calendar dates a streamed trade by the session whose span, first schedule event to last,
holds it, and a trade between one session's last event and the next's first by the next
session. How often Massive sends such a trade, and whether its own `session_end_date` for it is
the next session's, is unknown. Settled by
`tests/ingest/test_futures_calendar_integration.py::test_trades_outside_every_futures_session`,
which reads the trades of 20 products' front contracts in the latest gap between two sessions.
