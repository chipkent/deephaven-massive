# History

Examples on this page assume `import deephaven_massive as dm` and `m = dm.MassiveSession()`.

This page covers reading the past with `history()`: how bars are stamped, where the rows come
from, and how prices are adjusted for stock splits.

<a id="bars"></a>

## Bars

A bar summarizes the trades in a window of time: open, high, low, close, volume, the
volume-weighted average price (VWAP), and the number of trades. These rules hold for every
`bars()` dataset.

### The window

`TimestampStart` is when the window opens. `TimestampEnd` is the first instant after it.

| Bar | Window |
|---|---|
| Second, minute, hour | Its own length |
| Day or longer | 00:00 of its first day to 00:00 after its last day, in the [market's time zone](concepts.md#time) |
| Day or longer, of an index | The same, but ending an hour later, at 01:00 New York time |

Weeks, months, quarters, and years follow the market's trading days:

| Asset class | A week covers | `TimestampStart` of a week | A month, quarter, or year |
|---|---|---|---|
| Stocks, options | Monday to Friday | Monday | Leaves out the weekend days at either end |
| Indices, forex, crypto | Sunday to Saturday | Sunday | Covers whole calendar months |

Massive names a week by its Sunday. A bar of several days spans that many calendar days,
weekends included: a 2-day stock bar stamped on a Friday holds Friday's trading alone.

`Date` is the bar's trading day. It is null for a bar that spans more than one date: a bar of
several days, a week or longer, or an intraday bar across midnight.

### Which bars `history()` returns

`history()` returns a bar only when its whole window lies inside the range and has already
ended:

- Today's daily bar arrives tomorrow.
- The current week's or month's bar never appears.
- Monthly bars from March 5 to March 20 return nothing, because no whole month fits.

Pass dates, not instants, for daily bars: a range of instants keeps only the whole calendar days
inside it.

### Join other data on `TimestampEnd`

A bar's values are final only once its window has ended, so join other data on `TimestampEnd`:

```python
trades = m.stocks.trades("AAPL").history("2025-06-02T10:00 ET", "2025-06-02T10:05 ET")
bars = m.stocks.bars("AAPL", dm.Timespan.MINUTE).history("2025-06-02", "2025-06-02")
priced = trades.aj(bars, on=["Symbol", "Timestamp>=TimestampEnd"], joins="LastClose = Close")
```

`priced` is the trades with `LastClose`: the close of the latest minute bar that had ended when
each trade happened. Minute bars of every asset class end on whole minutes, so a stock's and an
index's 10:31 bars join exactly on `TimestampEnd`.

A row read before its window ended can still change, such as an indicator's value for today.
Its `TimestampInsert` is before its `TimestampEnd`. To keep only finished rows:

```python
sma = m.indicators.sma("AAPL", 20, start="2025-06-01").fetch()
finished = sma.where("TimestampEnd <= TimestampInsert")
```

`finished` holds the 20-day moving averages of the days that had ended when you read them.

Two cases need care:

- **A futures bar of several sessions or longer**, read before its last session, passes the
  filter once its latest session so far has ended, though later sessions can still join it.
- **An indicator `poll()` in `dm.PollMode.APPEND` mode** keeps the first unfinished value it
  sees and never receives the finished one, so the filter drops that period for good. A
  `poll()` in its default mode holds the updated value.

<a id="futures-bars"></a>

### Futures bars of a session or longer

A futures trading session belongs to the day it opens, the evening before its date (see
[the market day](concepts.md#time)). Massive groups whole sessions into blocks of calendar days
in UTC:

- `TimestampStart` is 00:00 UTC on the block's first day.
- `TimestampEnd` is 17:00 Chicago time at the close of the bar's last session, Massive's
  `session_end_date`.
- A month bar holds the sessions that open in that month: September 2026's runs from the session
  of September 2 to that of October 1.
- An n-session bar covers n calendar days, so over a weekend it holds fewer than n sessions.

Which bars `history()` returns depends on how you pass the range:

| Range | `history()` returns a bar when |
|---|---|
| Dates | All its sessions lie in the range. A month bar needs the range to reach the session that opens on the month's last day. |
| Instants | Its `TimestampStart` is at or after `start` |

A range of instants that starts at a session's 17:00 Chicago open therefore leaves out that
session's 1-session bar, which starts at 00:00 UTC on the day the session opens. Start the range
at that midnight instead, such as `datetime(2026, 9, 13, tzinfo=UTC)` for the session that opens
on Sunday, September 13, or pass dates.

`ESZ6` is the E-mini S&P 500 future (product `ES`) for December (month code `Z`) 2026 (year
`6`):

```python
september = m.futures.bars("ESZ6", dm.Timespan.MONTH).history("2026-09-01", "2026-10-01")
```

`september` has one bar: `TimestampStart` 2026-09-01 00:00 UTC, `TimestampEnd` 2026-10-01 17:00
Chicago time. The same call ending `"2026-09-30"` returns no bar.

<a id="odd-stamps"></a>

### Bars with unexpected stamps

Massive stamps the same bar differently on different sources, such as a daily stock bar at
00:00 on one and at the 16:00 close on another. The library moves each known stamp to the start
of its day, so every source gives the same rows.

A bar stamped at a time the library does not expect, such as a daily bar at 14:30, could cover
that day or the next, so you get it as Massive stamped it:

- `TimestampStart` is the stamp.
- `TimestampEnd` is one period later, and no earlier than the end of any such period that
  holds the stamp.
- `Date` is empty.
- The log has one WARNING for each time of day such stamps take. Please report it on the
  [issues page](https://github.com/chipkent/deephaven-massive/issues).

`history()` returns such a bar only when the range holds its whole window, so you may need a
range one period longer.

A crypto `daily_open_close` row that Massive marks as not UTC keeps its `Date`, but its window
is the widest that date has in any time zone: from 14 hours before midnight UTC to 12 hours
after the next.

<a id="history-sources"></a>

## Where history comes from

`history()` reads from one of two sources. By default it picks one, and uses the other for what
the first cannot serve, so you get your data whenever either source has it.

| Source | Serves |
|---|---|
| REST, Massive's web requests | Named symbols, on every dataset with a REST history endpoint. For `dm.ALL`, only the daily bars of stocks, forex, and crypto, one request per day. |
| Flat files, Massive's daily bulk files | Any read the files cover, once you have set your [S3 keys](install.md#s3-keys). Bars come in files only as 1-minute and 1-day bars (and 1-session bars for futures), and only as traded (`adjusted=False`). |

Massive usually publishes a day's flat files by about 11:00 New York time the next day, but the
time varies: Friday's files can arrive late on Saturday morning.

### How the default chooses

The default, `source=dm.HistorySource.AUTO`, reads flat files when:

- only the files can serve the read; or
- both can, and the read has 50 or more symbols (the `flat_file_symbol_threshold` setting) or
  `dm.ALL`.

Otherwise it reads REST. A `history()` that follows a [watchlist](watchlists.md) ignores the
symbol count: it reads REST whenever REST can serve the read, so adding one symbol never
downloads a year of flat files.

To choose yourself, pass `source=dm.HistorySource.REST` or `source=dm.HistorySource.FLAT_FILES`.

The two sources sometimes fill different columns: REST trades have `DecimalSize`, the files do
not. When `AUTO`'s choice leaves columns empty that the other source fills, the log has a
WARNING naming them and the `source=` that fills them.

Every `history()` table records its source, and why it was chosen, in its description:
`table.attributes()["TableDescription"]`.

### When a day's flat file is missing

A weekday's file may not be out yet, or Massive may never publish it.

| Read | What happens |
|---|---|
| `AUTO`, and REST can serve the read | That day comes from REST, so you get the whole range. A WARNING and the table's description name the days. |
| `AUTO`, and your plan lacks the files or S3 rejects your S3 keys | The whole read comes from REST, with a WARNING |
| REST cannot serve the read (such as trades of `dm.ALL`), or `source=FLAT_FILES` | A missing day that has a later day's file is taken for a holiday, and a WARNING and the table's description name it. Otherwise the call fails. |
| `source=FLAT_FILES`, and your plan lacks the files | The call fails |

### Split-adjusted bars of every symbol

Split-adjusted bars come only from REST, one symbol at a time (every symbol at once only for
1-day bars). Split-adjusted minute bars of `dm.ALL` therefore have no source: pass
`adjusted=False` for the as-traded files, or name the symbols.

<a id="split-adjustment"></a>

## Split adjustment

When a company splits its stock 10 for 1, each share becomes ten shares worth a tenth of the
price, and a chart of raw prices drops 90% overnight. Massive can restate older prices in the
split's terms, so the series stays continuous: a *split-adjusted* price.

- Massive adjusts stock and option bars for splits, forward and reverse, and never for cash
  dividends. Index, forex, crypto, and futures bars have nothing to adjust.
- Flat files and live WebSocket bars are as traded, with no adjustment.
- `bars()` and `daily_open_close()` of stocks and options take `adjusted=True` by default. Only
  REST serves adjusted bars, and the library checks Massive's answer on every page, so one
  `history()` never mixes adjusted and as-traded prices.
- `adjusted=False` reads REST or flat files.

These examples reach back to June 2024, further than Stocks Basic's two years of history; on
Basic, try them with a split inside your window.

```python
# Split-adjusted daily bars, the default. NVDA split 10 for 1 on 2024-06-10,
# so its 2024-06-07 close reads about 120.9.
adjusted = m.stocks.bars("NVDA", dm.Timespan.DAY).history("2024-06-03", "2024-06-14")

# As traded: the 2024-06-07 close reads about 1208.
traded = m.stocks.bars("NVDA", dm.Timespan.DAY, adjusted=False).history(
    "2024-06-03", "2024-06-14"
)
```

`adjusted` and `traded` each have one row per trading day. Their closes differ tenfold before
June 10 and match from June 10 on.

### Adjust as-traded bars yourself

The splits dataset gives each split's cumulative `HistoricalAdjustmentFactor`. Massive's rule
for a price on date D:

1. Find the first split whose `ExecutionDate` is after D.
2. Multiply the price by that split's factor, and divide the volume by it.
3. With no later split, leave the price as it is.

A reverse as-of join (`raj`) finds that split:

```python
splits = m.stocks.splits("NVDA").fetch()
mine = traded.raj(
    splits, on=["Symbol", "Date<ExecutionDate"], joins=["Factor=HistoricalAdjustmentFactor"]
).update(
    [
        "Factor = isNull(Factor) ? 1.0 : Factor",
        "AdjustedClose = Close * Factor",
        "AdjustedVolume = Volume / Factor",
    ]
)
```

`mine` is `traded` with three new columns: `Factor`, `AdjustedClose`, and `AdjustedVolume`.
Its `AdjustedClose` matches the `Close` of `adjusted`.

- Adjust daily bars, then aggregate them: a longer bar's `Date` is null, so the join cannot
  place it.
- This recipe also applies stock dividends (`AdjustmentType` `stock_dividend`). Massive does not
  document whether its own adjustment does, so the two can differ around one.

### History and live bars together

Live bars are as traded, which is also the adjusted basis until the next split. Adjusted history
and live bars therefore join without a seam: see
[backfill, then live](../../README.md#backfill-then-live).

On a split's execution date every price moves to the new basis:

- A table that runs across a split, or joins adjusted history to live bars, shows a step there.
  Build it again after a split.
- A long download that runs while Massive applies a split overnight can return pages on both
  bases, which nothing can detect. Download again after the split.

The indicators (`m.indicators`) use Massive's own adjusted prices.
