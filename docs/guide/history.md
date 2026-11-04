# History

`history(start, end)` reads the past rows of a dataset over a range of dates or times. It works
on bars, trades, quotes, and every other dataset with a time axis. Given a list of symbols, it
returns a static table. Given a [watchlist](watchlists.md), it returns a live table that adds
and drops each symbol's rows as the watchlist changes. The rules that most often surprise people
are which bars a range returns, which source served the rows, and why prices jump at a split.
Paste each example into the console of the Deephaven IDE.

<a id="bars"></a>

## How bar times work

A bar summarizes the trades in a window of time. It holds the window's open, high, low, and
close prices, its volume, its volume-weighted average price (VWAP), and its number of trades.
Massive puts one time on each bar, called its stamp. Every `bars()` dataset turns that stamp into
the same two columns, `TimestampStart` and `TimestampEnd`, so bars from any source line up.

### When a bar's window starts and ends

`TimestampStart` is when a bar's window opens, and `TimestampEnd` is the first instant after it
closes. The window's length, its period, depends on the bar:

| Bar | Window |
|---|---|
| Second, minute, hour | Its own length |
| Day or longer | 00:00 of its first day to 00:00 after its last day, in the [market's time zone](arguments.md#time) |
| Day or longer, of an index | The same, but ending an hour later, at 01:00 New York time |

Weeks, months, quarters, and years follow each market's trading days:

| Asset class | A week covers | `TimestampStart` of a week | A month, quarter, or year |
|---|---|---|---|
| Stocks, options | Monday to Friday | Monday | Leaves out the weekend days at either end |
| Indices, forex, crypto | Sunday to Saturday | Sunday | Covers whole calendar months |

Massive stamps every week on its Sunday, yet a stock week's window starts on Monday. A bar of
several days spans that many calendar days, weekends included, so a 2-day stock bar stamped on a
Friday holds Friday's trading alone. `Date` is the bar's trading day. It's null for a bar that
spans more than one date: a bar of several days, a week or longer, or an intraday bar that
crosses midnight.

### Which bars a range returns

`history()` returns a bar only when its whole window lies inside the range and has already
ended. Today's daily bar comes from REST once the day ends, or from the flat files the next
morning. The current week's or month's bar appears only once that week or month is over.
Monthly bars from March 5 to March 20 return nothing, because no whole month fits. Pass dates,
not instants, for daily bars. A range of instants keeps only the whole calendar days inside it,
while a range of dates holds every day you name.

<a id="odd-stamps"></a>

### Which bars carry unusual times

Massive stamps the same daily bar at different times on different sources. A REST range read
stamps a stock's daily bar at 00:00, while the stock flat files and `previous_day_bar()` stamp it
at the 16:00 New York close. Whatever the source, `TimestampStart` is 00:00 of the bar's day, and
REST and the flat files give you the same rows.

Massive occasionally stamps a bar at an unusual time, such as a daily bar at 14:30. That bar
could cover its own day or the next, so you get it as Massive stamped it:

- `TimestampStart` is the stamp.
- `TimestampEnd` is one period after the stamp, such as 14:30 the next day for a daily bar. If
  the period the stamp falls in ends later, `TimestampEnd` is that period's end instead.
- `Date` is empty.
- The log has one WARNING for each time of day such stamps take.

`history()` returns such a bar only when the range holds its whole window, so read a range one
period longer to be sure you get it. Report such a WARNING on the
[issues page](https://github.com/chipkent/deephaven-massive/issues), with the dataset and the
stamp.

### Which window a crypto open and close covers

Massive marks some crypto `daily_open_close` rows as not in UTC, the time zone of a crypto day.
Such a row keeps its `Date`, and its window is the widest that date covers in any time zone. The
window runs from 14 hours before midnight UTC to 12 hours after the next midnight.

### How to join other data on a bar's end

A bar's values are final only once its window has ended, and `TimestampEnd` marks that moment.
Join other data on `TimestampEnd` to match each row with the latest finished bar. This prices
each of Apple's trades from 10:00 to 10:05 on June 2, 2025 against the minute bar before it.
You need a Stocks Developer plan or higher, the plans that include trades:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
trades = m.stocks.trades("AAPL").history("2025-06-02T10:00 ET", "2025-06-02T10:05 ET")
bars = m.stocks.bars("AAPL", dm.Timespan.MINUTE).history("2025-06-02", "2025-06-02")
priced = trades.aj(bars, on=["Symbol", "Timestamp>=TimestampEnd"], joins="LastClose = Close")
```

`priced` is the trades with a new `LastClose` column, the close of the latest minute bar that had
ended when each trade happened. Minute bars of every asset class end on whole minutes, so a
stock's 10:31 bar and an index's 10:31 bar join exactly on `TimestampEnd`.

### How to keep only finished rows

A row you read before its window ended can still change, such as an indicator's value for today.
Its `TimestampInsert`, the time you received it, is earlier than its `TimestampEnd`, and
comparing the two keeps only the finished rows. An indicator read with no `end` asks for the
latest value, which Massive serves delayed on Stocks Starter and Developer and end-of-day on
Stocks Basic. The session in this example accepts delayed stock data, so it runs on any Stocks
plan, the free one included:

```python
import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS}))
sma = m.indicators.sma("AAPL", 20, start="2025-06-01").fetch()
finished = sma.where("TimestampEnd <= TimestampInsert")
```

On Stocks Starter or higher, `sma` includes today's row while the market is open, and `finished`
leaves it out until the day ends. On Stocks Basic, today's value arrives only after the close.
From then until midnight New York time, `sma` includes it and `finished` leaves it out.

<a id="busiest-minute"></a>

### How to read every trade of a bar

A bar's `TimestampStart` and `TimestampEnd` make an exact range for another read. To see every
trade inside AAPL's busiest minute on June 2, 2025, find that minute, then read the trades
between its two times. You need Stocks Developer or higher:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
minutes = m.stocks.bars("AAPL", dm.Timespan.MINUTE).history("2025-06-02", "2025-06-02")
top = minutes.sort_descending("Volume").head(1)
if top.size > 0:  # the day had bars
    busiest = list(top.iter_dict())[0]
    inside = m.stocks.trades("AAPL").history(busiest["TimestampStart"], busiest["TimestampEnd"])
```

`busiest` is the minute bar with the most volume, as a dictionary of its columns. `inside` holds
the trades from the start of that minute up to, but not including, its end. Both columns are
instants, and a range of instants includes its start and excludes its end (see
[start and end](arguments.md#start-and-end)).

<a id="futures-bars"></a>

### How futures bars of a session or longer work

A futures trading session starts at 17:00 Chicago time and ends at 17:00 the next day, with a
trading pause from 16:00 to 17:00 before it ends. It takes the date of the day it ends (see [the
market day](arguments.md#time)). Massive groups whole sessions into blocks of calendar days in UTC,
and puts each session in the block that holds the day the session starts:

- `TimestampStart` is 00:00 UTC on the block's first day.
- `TimestampEnd` is 17:00 Chicago time on the date of the bar's last session, Massive's
  `session_end_date`.
- A month bar holds the sessions that start in that month. September 2026's bar holds the sessions
  that start from September 1 to September 30, which are dated September 2 to October 1.
- An n-session bar covers n calendar days. Over a weekend, it holds fewer than n sessions.

Which bars `history()` returns depends on how you name the range:

| Range | `history()` returns a bar when |
|---|---|
| Dates | All its sessions lie in the range. A month bar needs the range to reach the date of its last session. |
| Instants | Its `TimestampStart` is at or after `start`, and its `TimestampEnd` is at or before both `end` and now |

A range of instants that starts at a session's 17:00 Chicago start leaves out that session's
1-session bar, which starts at 00:00 UTC on the day the session starts. To include it, start the
range at that midnight, such as 2026-09-13 00:00 UTC for the session that starts on Sunday,
September 13, or pass dates. The finished-rows comparison also misjudges a futures bar of
several sessions or longer that you read before its last session. That bar passes once its
latest session so far has ended, though later sessions can still join it.

`ESZ6` is the December 2026 E-mini S&P 500 future. `ES` is the product, `Z` is the code for
December, and `6` is the year. Its September 2026 month bar reads on any Futures plan:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
september = m.futures.bars("ESZ6", dm.Timespan.MONTH).history("2026-09-01", "2026-10-01")
```

`september` has one bar. Its `TimestampStart` is 2026-09-01 00:00 UTC, and its `TimestampEnd` is
2026-10-01 17:00 Chicago time. The same call ending `"2026-09-30"` returns no bar, because the
month's last session is dated October 1.

<a id="history-sources"></a>

## Where history comes from

`history()` reads from one of two sources, and by default it picks one for you. When the source
it picked can't serve part of the read, `history()` reads that part from the other. `dm.ALL` in
place of symbols asks for every symbol in the market:

| Source | What it serves |
|---|---|
| REST, Massive's request API | Any dataset you read by naming symbols. For `dm.ALL`, it serves only the daily bars of stocks, forex, and crypto, with one request per day. |
| Flat files, Massive's daily files of a whole market | Any read the files cover, once you've set your [S3 keys](install.md#s3-keys). Bars come in files only as 1-minute and 1-day bars, plus 1-session bars for futures. |

Massive usually publishes a day's flat files by about 11:00 New York time the next day, but the
time varies, and Friday's files can arrive late on Saturday morning. Every `history()` table
records its source, and why it was chosen, in its description. Read it with
`table.attributes()["TableDescription"]`.

### How the default chooses

The default, `source=dm.HistorySource.AUTO`, reads flat files when only the files can serve the
read. When both can, it reads flat files for 50 or more symbols, or for `dm.ALL`, and REST for
fewer. The `flat_file_symbol_threshold` setting holds that 50. A `history()` that follows a
watchlist reads REST whenever REST can serve the read, whatever the number of symbols.

To choose a source yourself, pass `source=dm.HistorySource.REST` or
`source=dm.HistorySource.FLAT_FILES`. The two sources sometimes fill different columns: REST
trades have `DecimalSize`, for example, and the files don't. When `AUTO`'s choice leaves columns
empty that the other source fills, a WARNING in the log names them and the `source=` that fills
them.

### What happens when a day's flat file is missing

A weekday's file can be missing because it isn't out yet, or because Massive never publishes it.
What happens depends on whether REST can stand in, and on whether a later day's file exists:

| Who can read the day | Later day's file | What happens |
|---|---|---|
| REST, under `AUTO` | Either way | That day comes from REST, and you get the whole range. A WARNING and the table's description name the days. |
| REST, under `AUTO`, when your plan lacks the files or S3 rejects your S3 keys | Either way | The whole read comes from REST, with a WARNING |
| Flat files only: REST can't serve the read, such as trades of `dm.ALL`, or you passed `source=FLAT_FILES` | Exists | The missing day counts as a holiday. A WARNING and the table's description name it. |
| Flat files only | Doesn't exist | The call fails, and the error says which day is missing |
| Flat files only | No day of the range has a file | You get an empty table, with a WARNING for each missing weekday |
| `source=FLAT_FILES`, when your plan lacks the files | Either way | The call fails |

<a id="split-adjustment"></a>

## Split adjustment

When a company splits its stock 10 for 1, each share becomes ten shares worth a tenth of the
price. A chart of the prices as traded, the prices that were actually paid, drops 90% overnight.
Massive can restate the older prices in the split's terms, and the series stays continuous. That
restated price is the split-adjusted price, and the split terms a price is stated in are its
basis.

Massive adjusts stock and option bars for splits, forward and reverse, and never for cash
dividends. Index, forex, crypto, and futures bars have nothing to adjust. These calls return
split-adjusted prices by default, through their `adjusted=True` argument: `bars()`,
`daily_open_close()`, and `previous_day_bar()` of stocks and options, and the indicators. The
indicators compute from Massive's own split-adjusted bars.

### Where adjusted bars come from

Only REST serves split-adjusted bars. Flat files and live bars are always as traded, and
`adjusted=False` reads REST or flat files. REST serves adjusted bars one symbol at a time, except
the 1-day bars of stocks, which it serves for every symbol at once. Adjusted minute bars of `dm.ALL`
have no source, so pass `adjusted=False` for the as-traded files, or name the symbols.

Each page Massive sends for a `bars()` read says whether its prices are adjusted. If a page says
the opposite of what you asked for, the call fails with an error that asks you to retry it. A
page that doesn't say can't be checked, and the log has a WARNING that names the dataset.

### How to read adjusted and as-traded bars

NVDA split 10 for 1 on June 10, 2024. The daily bars around the split, read both ways, reach back
further than Stocks Basic's two years of history, so they need Stocks Starter or higher:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
# Split-adjusted, the default: the 2024-06-07 close reads about 120.9.
adjusted = m.stocks.bars("NVDA", dm.Timespan.DAY).history("2024-06-03", "2024-06-14")
# As traded: the 2024-06-07 close reads about 1208.
traded = m.stocks.bars("NVDA", dm.Timespan.DAY, adjusted=False).history(
    "2024-06-03", "2024-06-14"
)
```

`adjusted` and `traded` each have one row per trading day. Their closes differ tenfold before
June 10 and match from June 10 on.

### How to adjust as-traded bars yourself

The splits dataset gives each split a cumulative `HistoricalAdjustmentFactor`. Massive's rule
for a price on date D takes three steps:

1. Find the first split whose `ExecutionDate` is after D.
2. Multiply the price by that split's factor, and divide the volume by it.
3. If no split comes later, leave the price as it is.

A reverse as-of join (`raj`) finds that split for each bar. Run on Stocks Starter or higher, this
block reads NVDA's as-traded bars and adjusts them:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
traded = m.stocks.bars("NVDA", dm.Timespan.DAY, adjusted=False).history(
    "2024-06-03", "2024-06-14"
)
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

`mine` is `traded` with three new columns, `Factor`, `AdjustedClose`, and `AdjustedVolume`. Its
`AdjustedClose` matches the `Close` of the split-adjusted bars. Adjust daily bars first, then
aggregate them into longer bars, because a longer bar's `Date` is null and the join can't place
it.

The recipe also applies stock dividends, whose `AdjustmentType` is `stock_dividend`. Massive
doesn't document whether its own adjustment applies them, and the two results can differ around
a stock dividend.

### How history and live bars combine

Adjusted history joins live bars without a seam until the next split. Live bars are as traded,
and until a split, as-traded and adjusted prices are the same, as
[backfill, then live](../../README.md#backfill-then-live) shows. On a split's execution date,
every price moves to the new basis. A table that runs across a split, or joins adjusted history
to live bars, shows a step on that date, so build it again after the split.

Massive applies a split overnight. If your download runs while Massive does so, some pages can
arrive on the old basis and some on the new, and both say they're adjusted. Download the range
again after the split.
