# Read history: ranges, bar windows, and sources

`history(start, end)` reads a dataset's past rows into a static table. This page explains how
the two bounds work, which bars a range returns, how each row is stamped, and where the rows
come from. The `history()` docstring has the short version.

## Two kinds of bounds

The two bounds are of one kind, never mixed.

- **Dates**, such as `"2025-03-03"` or a `date`, are whole days, and both ends are included:
  `history("2025-03-03", "2025-03-03")` is all of March 3. A day is a calendar day in the asset
  class's own time zone: New York for stocks, options, and indices, and UTC for forex and
  crypto. For futures, a date names a trading session, which opens at 17:00 Chicago time the
  evening before (earlier after a holiday).
- **Times**, as timezone-aware `datetime`s, are exact instants, and the end is excluded: rows
  with `start <= Timestamp < end`. For bars, the instant compared is `TimestampStart`. Two equal
  instants give an empty table.

Leave out `end` to read through now: the current instant for a time, today (for futures, the
current session) for a date. A naive `datetime`, with no time zone, is refused.

```python
from datetime import UTC, datetime

import deephaven_massive as dm

m = dm.MassiveSession()
q = m.crypto.trades("X:BTCUSD")

day = q.history("2026-01-02", "2026-01-02")  # all of that UTC day
hour = q.history(datetime(2026, 1, 2, 14, tzinfo=UTC), datetime(2026, 1, 2, 15, tzinfo=UTC))
since = q.history("2026-01-02")  # through now
```

## Which bars a range returns

A bar is returned only when its whole window lies inside the range and has already ended. So
the current day's, week's, or month's bar is never returned, and `history("2025-03-05",
"2025-03-20")` of monthly bars is empty: no whole month fits.

A bar's window depends on its size and class:

- **Intraday and daily bars** cover their own interval.
- **Weeks, months, quarters, and years of stocks, options, and futures** cover their calendar
  period without the weekend days at either end: a week runs Monday to Friday.
- **The same periods of indices**, which can update around the clock, are whole calendar
  periods, weekends included: a week runs Sunday to Saturday.
- **The same periods of forex and crypto**, which trade at weekends, are whole calendar weeks
  (Sunday to Saturday), months, quarters, and years.
- **A bar of several units**, such as 3-day bars, covers all of them, with room for holidays
  to push the end later.
- **A futures bar named by session dates** covers its sessions. Massive does not document
  whether a bar of several sessions, a week, or longer is labelled with its first or its last
  session, so such a bar is kept only when it fits the range either way.

Massive starts every week on Sunday at 00:00 in the class's time zone and stamps the week's bar
there; a week bar stamped on any other day stops the read with an error. There is no holiday
calendar: a stock week whose Friday is a holiday still runs Monday to Friday.
=====

Time bounds keep only whole calendar days of daily bars. Monday 09:30 to Friday 16:00 New York
time returns Tuesday to Thursday, so pass dates when you want daily bars.

## How rows are stamped

Every row says when it happened, its trading day, and when it arrived:

- A tick (a trade or a quote) has `Timestamp`, the moment it happened.
- A bar has `TimestampStart` and `TimestampEnd`, the first instant after the bar. Join other
  data on the end, so that nothing is attached to a bar before the bar closed:
  `trades.aj(bars, on=["Symbol", "Timestamp>=TimestampEnd"])`.
- `Date` is the trading day as `yyyy-MM-dd` text. It is null for a bar that spans more than
  one date, such as a weekly bar.
- `TimestampInsert` is when the row's page or file block arrived: the download time.

A bar of a day or longer starts at 00:00 of its first day in the class's time zone (New York
for stocks, options, and indices; UTC for forex and crypto) and ends at 00:00 after its last
day. Index bars end an hour later, at 01:00 New York time; that has not yet been checked
against Massive. A futures bar keeps Massive's start and ends at 17:00 Chicago time on its
session date. Each source stamps these bars the same way, so the source never changes which day a
bar is labelled with.

Every bar Massive returns is in the table. Once in a while Massive may stamp a bar at a time the
library does not expect, such as a daily bar at 14:30, or a weekly bar on a Wednesday. That bar
is still returned, and its `Date`, `TimestampStart`, and `TimestampEnd` come from the day its
stamp falls on. Those columns may then cover other days than the bar really does, so the
library logs one WARNING per dataset and timespan in a session. The WARNING names the stamp and
the stamps it expected. Please report it on the
[issues page](https://github.com/chipkent/deephaven-massive/issues), so the rule can be fixed.

## Where the rows come from

`history()` reads from one of two sources, and one call never mixes them.

- **REST** serves a dataset that has a REST history endpoint, for explicit symbols. For
  `dm.ALL` it serves only daily bars, through Massive's grouped daily endpoint.
- **Flat files** are Massive's daily files on S3. They serve a request when you have set S3
  credentials, the dataset has files for the request, and every day's file has been published.
  Bars come in files only as 1-minute and 1-day bars (and 1-session bars for futures), and only
  unadjusted. A day's file appears the next morning, from about 11:00 New York time.

The default, `source=dm.HistorySource.AUTO`, picks flat files when only they can serve the
request, or when both can and the request has many symbols (`flat_file_symbol_threshold` or
more, or `dm.ALL`). Otherwise it uses REST. Pass `source=dm.HistorySource.REST` or
`source=dm.HistorySource.FLAT_FILES` to choose yourself.

The two sources do not always fill the same columns: REST trades have `DecimalSize`, and
flat-file trades do not. When `AUTO`'s choice leaves columns empty that the other source fills,
the library logs one WARNING that names them and the `source=` that fills them. Every result's
description, its `TableDescription` attribute, names the source it came from and why.

If your plan does not include the flat files a read needs, the read fails with
`EntitlementError`. When REST can serve the same read, the message says to pass
`source=dm.HistorySource.REST`. `AUTO` reports the refusal too, rather than quietly switch to
REST and change which columns are filled.

Flat-file reads skip a missing weekend file silently and a missing weekday file (a holiday)
with a WARNING. A missing file for the last day within a day of its publication time is an
error instead, since the file may simply not be published yet.

## Split adjustment

Stock and option bars take `adjusted=True` by default: prices from before a split are restated
in the split's terms, as Massive's REST endpoints compute them. Only REST serves adjusted bars,
and the library checks that every page is on the basis the call asked for. Flat files are as
traded, so they serve only `adjusted=False`. Massive adjusts for splits only, never for cash
dividends. The README's [Split adjustment](../../README.md#split-adjustment) section has the
details and a recipe for adjusting as-traded bars yourself.

## How long it takes

`history()` blocks until the whole range is read. Requests run on the calling thread, and
flat files are read by up to `max_concurrency` threads that finish before the call returns.
A long read in the Deephaven console holds up every live table until it ends, so pass
`wait=False` for one: the call returns at once with an empty table, and the whole result
appears in it in one update when the download has finished. Its row in `m.streams()` shows
`LOADING`, then `DONE`, or `FAILED` with the reason.
There is no `limit`: the library pages through everything in the range. On the free Basic tier,
requests are paced at five a minute, and a read that needs a minute or more of them logs its
request count first, at INFO.
