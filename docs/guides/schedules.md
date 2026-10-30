# Follow a schedule with a live symbol table

A live table follows a *symbol table*: a Deephaven table with a `Symbol` column. When the table
refreshes, the stream follows its rows. It subscribes to a symbol when its row appears and
unsubscribes when the row goes away. A `poll()` table works the same way: each cycle requests
only the symbols the table holds at that moment.

This makes a schedule easy to build. Give the stream a symbol table that is empty outside the
hours you care about, and the stream receives nothing outside those hours. A `poll()` makes no
requests while the table is empty, which matters on the free Basic tier, where every request
counts against five a minute.

## Regular trading hours only

The recipe below keeps a watchlist live only while New York's regular session is open, 09:30 to
16:00 on weekdays. A one-row clock table ticks every minute. A filter keeps that row only during
the session, and a join with no key columns pairs the watchlist with the clock: when the clock
has no row, the joined table has no rows either.

```python
from deephaven import new_table, time_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession()

watchlist = new_table([string_col("Symbol", ["AAPL", "MSFT", "NVDA"])])

# One row while the regular session is open, none otherwise. 570 minutes is 09:30 and
# 960 is 16:00; days 1 to 5 are Monday to Friday.
session_open = (
    time_table("PT1m")
    .tail(1)
    .update_view(
        [
            "Minute = minuteOfDay(Timestamp, timeZone(`America/New_York`), false)",
            "Day = dayOfWeekValue(Timestamp, timeZone(`America/New_York`))",
        ]
    )
    .where(["Day <= 5", "Minute >= 570", "Minute < 960"])
)

# The watchlist while the session is open, and an empty table the rest of the time.
scheduled = watchlist.join(session_open, on=[], joins=[]).view(["Symbol"])

trades = m.stocks.trades(scheduled).last_by()
```

The clock ticks once a minute, so the stream starts and stops within a minute of the bell. Use
`time_table("PT1s")` for a tighter edge.

## Things to know

- **Holidays.** The recipe knows weekdays, not exchange holidays. On a holiday the stream is
  subscribed but quiet. Add a `where` on a holiday list of your own if that matters.
- **Pre-market and after-hours.** Change the two minute bounds. US stocks also trade before
  and after the regular session, from 04:00 (minute 240) to 20:00 (minute 1200) New York time.
- **Other markets.** Forex and crypto trade around the clock, forex with a weekend break. Futures
  trade in sessions that start the evening before, Chicago time. Use the time zone and hours of
  the market you watch.
- **What stays in the table.** `last_by()` keeps each symbol's last row overnight and filters
  out symbols that leave the symbol table, so after the close it shows nothing until the next
  open. `append_only()` and `ring()` keep every row they received.
- **Symbols must be Massive's tickers.** Every row of a symbol table must hold Massive's exact
  ticker (`AAPL`, `C:EURUSD`, `X:BTCUSD`). Convert other spellings first with
  `dm.canonicalize_symbols`.
- **Change the table, not the stream.** Don't call the library from a table listener. A
  listener runs on Deephaven's update thread, and the library refuses calls from there. A symbol
  table does the same job without one.
