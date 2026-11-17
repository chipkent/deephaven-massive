# 144. Which trading day counts a stock trade made between 21:00 and midnight?

Date: 2026-11-17

Status: open

Unknown: whether Massive counts a stock trade made between 21:00 and midnight New York time in
its calendar date or in the next trading day. The overnight session that makes such trades
starts on Sunday 2026-12-06. Massive's blog post of 2026-08-26 says trades "reported between 9pm
ET and midnight will count under the next calendar date". No page of Massive's API documentation
says which day its day bars and daily flat files count them in.

Meanwhile the library keeps the calendar date. Each row below changes if Massive counts those
trades in the next trading day. Changing the first two is a public change:

| The library today | If Massive counts 21:00 to midnight in the next day |
|---|---|
| A stock tick's or intraday bar's `Date` is the New York date of its time | A 21:00 trade's `Date` is not its trading day |
| A stock 1-day bar spans 00:00 to 00:00 of its date | The bar's `TimestampStart` is three hours after its first trade |
| A range reads the stock flat files of the New York dates it covers | A range ending between 21:00 and midnight misses rows in the next day's file |

Settled by `test_which_day_massive_counts_overnight_stock_trades_in` in
`tests/namespaces/test_stocks_integration.py`, run after 20:00 New York time on the trading day
that follows an overnight session. Massive's day begins at the first minute whose open equals
SPY's day-bar open: 21:00, midnight, or 04:00. `test_overnight_stock_trades_stream` in the same
file records the `Date` the library gives the trades it streams in the session.
