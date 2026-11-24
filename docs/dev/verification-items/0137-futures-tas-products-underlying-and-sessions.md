# 137. Does Massive serve sessions for a futures Trade at Settlement product, or name its underlying?

Date: 2026-11-16

Status: open

Two runs found 19 futures products traded within 7 days for which the schedules answered no
session (`CLT`, `HG0`, `ZCT`). CME's contract specifications name `CLT`, `HGT`, and `HG0` as
Trade at Settlement (TAS) products, which trade at a differential to the settlement price of an
underlying contract (`CL` for `CLT`); the others are taken to be TAS products from CME notices.
Unknown: whether Massive's schedules hold their sessions under any query, and whether a products
or contracts record names the underlying product. If the schedules serve them, the calendar
reads them as it reads any product. If a record names the underlying, the calendar can date
their rows by its sessions. If neither, the evidence goes to Massive in a report. Meanwhile the
calendar dates no streamed row of such a product: its rows are withheld under
`unscheduled_products`.

Read by `tests/ingest/test_futures_calendar_integration.py::test_response_bodies_of_futures_gaps`,
on a key with futures data. It records the whole schedules answer for `CLT` and `CL`, and the
products and contracts records of the 19 TAS products and their underlyings.

Massive's REST trades document `session_end_date`, the trading day. Unknown: whether Massive
fills it for TAS contracts, and whether the day it gives is the session of the product the TAS
product settles to. `tests/namespaces/test_futures_integration.py::test_trade_at_settlement_trades_and_their_sessions`
records it for six TAS products (`CLT`, `SBT`, `HG0`, `HGT`, `GCT`, `ZWT`): each front
contract's latest trades and their `session_end_date`, whether each trade falls inside the
underlying's session of that date, and the contracts' snapshot rows.
