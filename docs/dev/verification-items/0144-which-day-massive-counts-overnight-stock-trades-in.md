# 144. Which trading day does Massive count a stock trade made between 21:00 and midnight in?

Date: 2026-11-17

Status: open

Unknown: whether Massive files a stock trade made between 21:00 and midnight New York time under
its calendar date or under the next trading day. The overnight session that makes such trades
starts on Sunday 2026-12-06. Massive's blog post of 2026-08-26 says trades "reported
between 9pm ET and midnight will count under the next calendar date", but no page of Massive's
API documentation says which day its day bars and daily flat files put them in.

Meanwhile the library keeps the calendar date. Each row below changes if Massive counts those
trades in the next trading day:

| The library today | If Massive counts 21:00 to midnight in the next day |
|---|---|
| A stock tick's or intraday bar's `Date` is the New York date of its time | A 21:00 trade's `Date` is not its trading day |
| A stock 1-day bar spans 00:00 to 00:00 of its date | The bar's `TimestampStart` is three hours after its first trade |
| A range reads the stock flat files of the New York dates it covers | A range ending between 21:00 and midnight misses rows in the next day's file |

Changing the first two is a public change for the owner. All three wait on this answer.

Settled by `test_which_day_massive_counts_overnight_stock_trades_in` in
`tests/namespaces/test_stocks_integration.py`, on any Stocks plan, at any hour after an
overnight session has run. It records which minute's open SPY's day bar opens at: the first
from 21:00 the evening before, from midnight, or from 04:00. It skips when Massive has no minute
of the overnight session. `test_overnight_stock_trades_stream` in the same file records the
`Date` the library gives the trades it streams in the session.
