## 12. Timestamp units and day boundaries

The unit of every timestamp-like wire field is a fact about Massive's wire format. It is the
hand-verified `UNITS` table in `_massive/refresh/overrides.py`, and the generator writes it onto
each generated field, so callers read it as `source(...).field(column).unit`. A timestamp-like
field without a `UNITS` entry fails the refresh (the `timestamp-unit` gate,
[section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)). The table is complete for every generated source, so no
dataset planned PR edits it.

The OpenAPI spec tags some integer fields with a unit type in `x-polygon-go-type`
(`INanoseconds`, `IMicroseconds`, `IMilliseconds`, `IDaysPolygonDateString`), but the tags are
not reliable: all 20 `IMicroseconds` tags sit on indicator `timestamp` fields whose descriptions
say "Unix Msec", and the indicator responses carry milliseconds. The tags are a cross-check: a
`UNITS` entry that differs from its tag is listed, with its reason, in the tag-conflict list
beside the table, and the indicator `timestamp` fields are its first entries.

| Transport and fields | Unit |
|---|---|
| WebSocket stocks `T`/`Q` (`t`, `pt`, `trft`); `A`/`AM` (`s`, `e`); options `T`/`Q`; futures `T`/`Q` (`t`); indices `V` (`t`) and `A`/`AM`; forex `C`/`CA`/`CAS`; crypto `XT`/`XQ`/`XA`/`XAS` (`t`) | ms (documented as Unix ms on each documentation page; 13-digit samples) |
| WebSocket futures `Q` `bt`, `at`; crypto `XT`/`XQ` received time `r` | ms (the descriptions name no unit; the samples have 13 digits, as do the other times of the same message) |
| WebSocket `NOI` `t` | ns (documented as Unix ns; the sample has 19 digits) |
| WebSocket `LULD` `t` | **UNVERIFIED**: documented as Unix ms, but the sample (`1764086430905642800`) has 19 digits, which as ms would overflow; registered as ns. On a mismatch the overflow check or the bound drops every row, and the stream fails after `DROPPED_RUN_LIMIT` of them ([section 19.5](19-wire-clients-and-the-spec-snapshot.md#195-runtime-drift)). |
| WebSocket `FMV` `t` | Documented as ns; the documentation sample is `1678220098130` (13 digits, ms). Rule, FMV only: `t >= 10**17` means ns, otherwise ms. Every ns value after 1973-03-03 is at least 10^17, and an ms value reaches 10^17 only in the year 3,170,000, so the rule cannot misclassify. Logged once; tested. |
| REST `/v3/trades`, `/v3/quotes`, `/v2/last/trade`, `/v2/last/nbbo`; snapshot `last_updated`, `fmv_last_updated`; futures `/trades` and `/quotes` (`timestamp`, `bid_timestamp`, `ask_timestamp`); futures aggregates `window_start` (documented as "nanosecond Unix timestamp"); futures snapshot `last_quote` and `last_trade` times; stocks v2 snapshot `updated`, `lastTrade.t`, `lastQuote.t` | ns |
| REST aggregates `t`; `/prev` `t`; indicators `timestamp`; every v2 snapshot `min.t`; forex `lastQuote.t`, crypto `lastTrade.t` (13-digit documentation samples) | ms |
| REST forex and crypto v2 snapshot and movers `updated`; futures snapshot `last_minute.last_updated` (13-digit documentation samples; the live run of 2026-11-02 received 19 digits, [section 23](23-open-verification-items.md#23-open-verification-items) items 75 and 79) | ms or ns, each value read by its magnitude (`ms\|ns`) |
| REST forex `/v1/last_quote` `timestamp`; crypto `/v1/last` `timestamp`; forex `/v1/conversion` `last.timestamp` (the same `last` object as `/v1/last_quote`) | **UNVERIFIED** (`unverified-ms`). The spec tags the `last.timestamp` fields `IMilliseconds`, which agrees; registered as ms with a fixture test. |
| Flat files: all tick files; `*_aggs_v1.window_start`; `us_indices/values_v1.timestamp` (the documentation says ms, the sample is 19 digits) | ns |

**The timestamp routine.** One routine in `_datasets/timestamps.py` converts every epoch-integer
`Instant` on every transport, in this order:

1. **Epoch 0 means absent.** In every epoch `Instant` column except the dataset's declared time
   column, a `0` becomes null. Massive's flat files write `0` where a timestamp has no value
   (the documented stocks-trades sample has `trf_timestamp` `0` on every row without a TRF);
   WebSocket and REST are expected to omit the field, and the rule applies on every transport
   regardless. The time column keeps its value, so a `0` there fails
   step 3. Prices, sizes, and every other numeric column keep `0` (a one-sided quote has
   `ask_price` `0`).
2. **Checked multiply.** The integer is multiplied to nanoseconds from its unit (`s`, `ms`,
   `us`, or `ns`) with an overflow check.
3. **Bound.** The result must lie in [1990, 2100].

A value that fails step 2 or 3 is null and marked in the routine's mask (`masked_instants`),
and the text `"timestamp unit mismatch for <dataset>.<column>: <value> <unit> ..."`
(`mismatch_message`) names the dataset, the column, the unit, and the value. In the time
column it is a key that cannot be read, so the row is dropped and counted; in any other column
it is an unconvertible value, null with a WARNING ([section 19.5](19-wire-clients-and-the-spec-snapshot.md#195-runtime-drift)).

The bound applies to epoch-integer `Instant`s only, never to ISO-8601 text, so a pre-1990 ISO
date stays valid. The overflow check and the bound together catch every unit confusion. For
a field whose generated unit is `MS_OR_NS` (FMV `t`, LULD `t`, and the index values file's
`timestamp`, whose documentation and samples disagree), the magnitude rule above picks the unit
per value first, then the routine runs.

**Examples of generated time column names:** `Timestamp`, `ParticipantTimestamp`,
`TrfTimestamp`, `BidTimestamp`, `AskTimestamp` (futures quotes), `ReceivedTimestamp` (crypto
trades and quotes), `LastUpdated` (snapshots). A dataset's declared keys rename the vendor's
primary times into the library's own columns ("Time columns" below), so the bars' `s`, `t`,
and `window_start` become `TimestampStart` and the WebSocket bars' `e` becomes `TimestampEnd`.
Every calendar date, the vendor's (`ExDividendDate`, the economy `Date`) and the library's
(`Date`), is ISO `yyyy-MM-dd` text, a `String`, so a date compares and joins as text. The
library stores one shared `String` per distinct date. The Arrow conversion makes one Java
`String` per row, so every dataset table is built through `_tables/columns.py` `dataset_table`:
the other columns convert from Arrow, and each date column is built from a process-wide pool of
one `String` per date, run by run (a copy and doublings per run of rows of one date), and joins
them as an in-memory column source, with no Deephaven `update()`, whose fixed cost of a few
milliseconds would fall on every stream flush. A column of interleaved dates (more than 4,096
runs) is interned by a formula instead. A million rows of five dates hold five `String`s, a
reference per row; the fixed cost per table is under a millisecond, and large batches build
faster than a plain Arrow conversion; nothing else may build a dataset table, and only date
columns, a small set, go into the pool.

**`history(start, end=None)` bounds.** A range has two kinds of bounds, and one call never mixes
them:

- **Dates** (`"YYYY-MM-DD"` or a `date`) are whole days, inclusive at both ends:
  `history("2025-03-03", "2025-03-03")` is all of March 3. A day is a calendar day in the class
  day zone below; for futures a date names the trading session whose `session_end_date` it is.
- **Aware datetimes** are exact instants, a half-open range `[start, end)`: a row is kept when
  `start <= Timestamp < end`.

A date mixed with a datetime raises `ValueError` ("start and end must both be dates or both be
datetimes: ..."): a date means a whole day or session and a datetime one instant, and a futures
session's first instant moves with the holiday calendar, which the library does not have. There
is no inclusive or exclusive option. `end=None`, the default, reads through now: the current
instant after a datetime `start`, and after a date `start` the current day in the day zone (for
futures, the session the current instant belongs to). A reversed range (`start` after `end`, or
after now when `end` is None) raises `ValueError`. Two equal datetimes are an empty range: the
call returns an empty table with every column and makes no request. Two equal dates are that
one day. Datetimes are compared in UTC, so the repeated hour of a daylight-saving fall-back is
ordered correctly. `dates.bounds` normalizes the bounds to a `DaySpan` (dates) or a `TimeRange`
(datetimes), and `history_source.history_window` turns them into the rows to keep, the
partition dates, and the instants a REST request covers (`HistoryWindow`).

**Bars: whole windows.** A bar, of any timespan and multiplier, is returned only when its whole
window lies inside the range and has ended. One rule decides both, the bar's own `TimestampEnd`
("Time columns" below): a bar is kept only when its `TimestampStart` is no earlier than the
range's start and its `TimestampEnd` is no later than the earlier of now and the range's end.
The window carries these as `HistoryWindow.bars`, a `row_times.BarBounds` whose `start_rule` is
explicit: `StartRule.RANGE` checks the start, and `StartRule.LABEL`, for futures intraday and
1-session bars named by dates, keeps bars by their session label instead, since a folded session
opens before its date. For futures bars of a session or longer, `BarBounds.futures_completion`
adds the judgment that the bar has certainly ended (below). Ticks have no bar window (`bars` is
None). A range of dates ends where its last day's bar would
(`dates.day_end`: the next midnight in the class day zone; 01:00 New York time for indices;
17:00 Chicago on the last session's date for futures); a range of datetimes at its end. The
transports drop a later bar once its time columns are filled (`row_times.within`), so selection
and the join key can never disagree: wherever the end is conservative, so is the selection. The
windows:

- An intraday bar (second, minute, hour) spans `multiplier` units from its `Timestamp`, on whole
  units of UTC time (whole hours in every zone the library uses).
- A 1-day bar is its calendar day in the class day zone (to 01:00 New York time after it for
  indices). A bar of `multiplier` days spans that many calendar days, weekends included, for
  every class but futures: the live check of [section 23](23-open-verification-items.md#23-open-verification-items) item 63
  found Massive's 2-day AAPL bars stamped on Friday, Sunday, and Tuesday, the Friday bar
  holding Friday's volume alone and the Sunday bar Monday's. Only stocks were observed;
  options and indices read the same aggregates endpoint and count the same way. Futures bars
  of a session or longer are buckets of UTC days (below). Datetime bounds therefore
  keep only whole calendar days of 1-day bars: Monday 09:30 to Friday 16:00 New York time
  returns Tuesday to Thursday, so daily bars are asked for with dates.
- Massive starts a week on Sunday, at 00:00 in the class day zone, and stamps the week's bar
  there: the live check of [section 23](23-open-verification-items.md#23-open-verification-items) item 33 found every crypto
  and forex week bar at Sunday 00:00 UTC (2026-10-11T00:00Z) and every stock week bar at Sunday
  00:00 New York time (04:00Z), each holding exactly the volume from that Sunday on. So every
  class's week is named by its Sunday. For stocks and options, which do not trade at weekends,
  a week's window is its Monday to Friday, and months, quarters, and years are calendar periods
  less their leading and trailing weekend days, so March 2024 runs from Friday the 1st to
  Friday the 29th. Since an index can update around the clock (`I:AAVE100`), its window is the
  whole calendar period, at its start and its end: an index week runs Sunday to Saturday and
  ends an hour after the next Sunday begins, so a range must reach that Saturday to keep it,
  and an index month of June 2024 starts on Saturday the 1st, so a range from Monday the 3rd
  leaves it out. Forex (which trades from Sunday evening UTC) and crypto (every day) keep their
  weekends too: Sunday-to-Saturday weeks and whole calendar months, quarters, and years. A bar
  of `multiplier` periods spans that many.
- A futures bar of a session or longer is a bucket of whole UTC days, as the live check of
  2026-11-02 found ([section 23](23-open-verification-items.md#23-open-verification-items) item 40). Massive puts each
  session in the bucket holding the day of its first open (or first trade), sets
  `window_start` to 00:00 UTC on the bucket's first day, and labels the bar with the bucket's
  last session (`session_end_date`). An `n`-session bucket is `n` calendar days, so it can hold
  fewer than `n` sessions (the ESZ6 2-session bar from Thursday 2026-08-13 holds only Friday's
  session); a week is 7 days, and months, quarters, and years sit on the calendar grid (the
  September 2026 bar starts 2026-09-01 00:00 UTC and is labelled 2026-10-01, the session that
  opens on September 30). The bucket's end day is therefore `D + n` days, `D + 7n` days, or
  `n`, `3n`, or `12n` calendar months from its first day `D` (`bar_periods.futures_bucket_end`;
  a futures week is taken to start on Sunday, as every other class's does, and `{from}`
  anchors it there). A bar has *certainly ended* (`bar_periods.futures_bar_ended`) once its
  `TimestampEnd` has passed and, when its label is earlier than the bucket's latest possible
  session (the day after its last Sunday to Thursday, or its last Friday, for a product that
  trades by day, when later), once 17:00 Chicago on the weekday after that latest session has
  passed: that weekday covers a fold or a last session that has not traded yet. The judgment
  reads only the visible columns: the bucket's first day is the UTC date of `TimestampStart`,
  and the label is the Chicago date of `TimestampEnd`.
- Futures bars named by datetimes: a bar is kept when its `TimestampStart` is at or after
  `start`, its `TimestampEnd` at or before `end`, and it has certainly ended; intraday bars
  follow the intraday rule.
- Futures bars named by session dates (dates name sessions, and the whole window must lie in
  the range): an intraday bar when its session lies in the range; a 1-session bar when its
  label lies in the range; every longer bar (several sessions, a week, a month, a quarter, a
  year) when the first weekday on or after its bucket's first day is on or after the first
  date, its label is on or before the last date, its `TimestampStart` is no earlier than the
  `{from}` sent (so its whole window lies in the request), and it has certainly ended. One
  rule covers every longer unit and is predictable from the visible columns. It selects whole
  windows by their stamps: a bucket Massive cut short and stamped at `{from}` would look whole
  and be kept; whether Massive cuts one short is open ([section 23](23-open-verification-items.md#23-open-verification-items)
  item 40). So `history("2026-09-01", "2026-09-30")` of month bars returns no bar, and
  `history("2026-09-01", "2026-10-01")` returns September's.

A bar whose window has not ended yet is never returned, so the current day, week, month,
quarter, or year is left out whatever `end` says (a forex month that ends on a Sunday is
unfinished until that Sunday ends, and an index day bar until 01:00 New York time the next day);
for futures named by date, only finished sessions count (the sessions before the one the current
instant belongs to). There is no holiday calendar: a holiday is a weekday like any other, so a
week whose Friday is a holiday still runs Monday to Friday, and monthly stock bars for March
2024 need a range through Good Friday, 03-29. The REST aggregates' `{from}` is the first instant
that belongs to the first bar that fits (a week's Sunday, the 1st of a month even when that is
a weekend), the start of the kept range: whether Massive selects bars
by their stamp or builds them from `{from}`, the first whole period's bar is returned, and a
partial bucket begun before it is dropped. `{to}` is the last whole millisecond before the
range's end. Rows outside the window are then dropped.

**Day labels and stamps.** A futures bar's `TimestampStart` is Massive's `window_start`,
unchanged, and its session is its `session_end_date`. Every other bar of a day or longer, of
any multiplier, from every source (the aggregates range, grouped daily, `/prev`, and the
`day_aggs_v1` flat files), has its stamp moved to 00:00, in the class day zone, of
the day it falls on, so New York midnight for stocks, options, and indices and UTC midnight for
forex and crypto (`row_times.stamp_rule`, `row_times.day_starts`); its time columns are
then computed from that day ("Time columns" below). The classes stamped are exactly
those of `dates.DAY_ZONES`. One call then returns the same rows whichever source answers it.
Massive stamps the same bar differently by source, so each known stamp is moved back to 00:00
of the day it falls on. Bars are always served; an unexpected stamp warns, never refuses
(decision 70). A stamp Massive is not known to use is not moved (decision 85): which day it
covers would be a guess, and a wrong guess could put `TimestampEnd` before the data closed, a
look-ahead. The row keeps Massive's stamp as `TimestampStart`, has a null `Date`, and ends at
the stamp plus its period on the zone's wall clock (`row_times._plus_period`; a month's 31st
with no twin moves to the next month's 1st), which is no earlier than the end of any such
period that starts at or before the stamp and holds it. It logs one WARNING per form of stamp
per session (the session's once-per-session warnings; a form is the dataset, the zone, the bar
length, and the stamp's time of day, with its weekday for week bars and its day of the month
for longer ones, so each distinct odd stamp warns and a daily repeat does not) naming the dataset, the source, the symbol,
the stamp, and the stamps expected, saying how the row is served and that `Date` is left empty,
and pointing to the project's issues page. `history()` keeps such a row by the whole-window
rule like any other, so a range must hold its whole conservative window: a 1-day bar stamped
at 14:30 needs a range that reaches the next day. `TimeRule` reads the stamp rule again
(`StampRule.expects`) to tell the rows kept as sent, since a moved stamp is always expected.
The known times are
wall-clock times of day in the day zone, compared to the millisecond, so a New York session
close is 16:00 in winter and in summer; the zone's offsets come from `zoneinfo`, one
conversion per distinct stamp, not from Arrow's time-zone database. A week, month, quarter, or
year bar is expected on the day its period starts, since the whole-window rule above places a
bar by its stamp's day; one stamped on another day is placed in the period that day falls in,
and warns:

| Class | 1-day bars (`multiplier` 1) | Longer bars: time (REST range only) | Week bars: day | Month, quarter, year bars: day |
|---|---|---|---|---|
| Stocks | 00:00 (range); 16:00, the session close (grouped daily, `/prev`, `day_aggs_v1`); 13:00, a short day's close | 00:00 | Sunday | the 1st |
| Options | 00:00 (range, `day_aggs_v1`); 16:00 (`/prev`); 13:00; 16:15 and 13:15, the closes of index options such as SPXW | 00:00 | Sunday | the 1st |
| Indices | 00:00; 01:00, which is midnight in Chicago (range, `day_aggs_v1`); 16:00 (`/prev`); 13:00 | 00:00; 01:00 | Sunday | the 1st |
| Forex | 00:00 (range); 23:59:59.999, the day's last millisecond (grouped daily, `/prev`, `day_aggs_v1`) | 00:00 | Sunday | the 1st |
| Crypto | 00:00; 23:59:59.999 (`day_aggs_v1`) | 00:00 | Sunday | the 1st |

The 1st is of a month for month bars, of January, April, July, or October for quarter bars,
and of January for year bars. A bar of several days may start on any day. A time known for one
class is not accepted for another, and every class's times apply to every source, since each
class accepts 00:00 anyway. Some times are presumed rather than sampled, and the live checks
of [section 23](23-open-verification-items.md#23-open-verification-items) items 50 to 52 confirm them:

- The indices longer bars' 01:00 follows the range's 1-day bars; no sample shows one.
- 13:00 for stocks, options, and indices is the close of a short trading day (the day after
  Thanksgiving, Christmas Eve, July 3). A source that stamps a day at its close may stamp such
  a day there, so the check accepts it rather than fail real data on every short day.
- 16:15 and 13:15 for options are the closes of index options on full and short days, where
  `/prev` may stamp such a contract.
- The week day is the Sunday Massive starts every week on, as the live check of item 33
  found for stocks, forex, and crypto; options and indices are presumed to follow the same
  aggregates. A week bar stamped on any other day is kept at its stamp and ends a week later,
  and warns, since it may cover another week.

Every accepted time lies inside its own day in the zone, so none of them, on any date, changes
which day a bar is labelled with, and the whole-window rule keeps or drops a bar stamped at its
day's end exactly as it does the same bar stamped at the start. Crypto's day cut is not
verified: the documented range and `/prev` samples sit at midnight New York time and the
grouped-daily samples at times such as 20:48 UTC, no day boundary at all, so crypto daily REST
history, `dm.ALL` grouped daily, and `/prev` all warn, and serve their bars at those stamps
with no `Date`, if live REST data follows those samples,
until the owner decides after the live check ([section 23](23-open-verification-items.md#23-open-verification-items) item
1). `AUTO` picks the source as for every other dataset ([section 16](16-historysourceauto.md#16-historysourceauto)):
the stamp rule, not a fixed source, keeps both sources on the same days.

The WARNING names no other source or verb to use instead: every verb serves the bar, and the
WARNING asks for the stamp to be reported at the project's issue tracker so the rule can be
settled.

**Time columns.** Every market-data row says when it happened, which trading day it belongs to,
and when the library received it (`_datasets/row_times.py`). `DatasetSpec.row_times`
(`RowTimes`) says which columns a dataset has: `TICK` for every non-bar dataset in a kind
(trades, quotes, index values, FMV, LULD, imbalances), `BAR` for the bars and the indicators,
`DAY` for the daily open-close summaries, and `NONE` for the rest (snapshots, movers, reference,
partner, and economy data). A declaration's default is the explicit member `BY_KIND`, which
`DatasetSpec.times` resolves from the dataset's kind to one of the others. The columns come
first, in the order `Symbol`, `Date`, `Timestamp` or `TimestampStart`, `TimestampEnd`, except
`TimestampInsert`, which is last.

- **`Timestamp`** is a tick's event time, Massive's own. A `NONE` dataset keeps a `Timestamp`
  only where it declares a time key (`futures.schedules` and `futures.market_status` declare
  their vendor `timestamp` as one, since the name is the library's).
- **`TimestampStart`** is where a bar's window begins and **`TimestampEnd`** its exclusive end:
  the first instant at which the window is closed. Joins key on the end
  (`aj(t, bars, on=["Symbol", "Timestamp>=TimestampEnd"])`), so an as-of join never attaches a
  bar whose window is still open. The end is when the window closes, not when Massive published
  the bar, and Massive may correct a bar afterwards. Bars have no plain `Timestamp`. A row
  read before its window ended has `TimestampInsert` before `TimestampEnd` and can still
  change, so `t.where("TimestampEnd <= TimestampInsert")` keeps only rows read after their
  window ended; every `TimestampEnd` description says so (decision 86). A
  `poll(mode=APPEND)` table of indicators keeps the first unfinished value it sees and never
  receives the finished one, so the filter drops that period for good. The test is exact by
  the library's windows except where it reads the local clock (which can be off), for a row
  read within moments of its end or from a delayed feed (Massive's value may still lack the
  window's last trades), and for a futures bar of several sessions or longer read before its
  bucket closed (its end moves as sessions join it); an end later than the window's (an index
  day's extra hour, a kept stamp, a crypto summary not in UTC) only keeps a finished row out
  longer.
- **`Date`** is the trading day, ISO `yyyy-MM-dd` text, stored as a column (not computed on
  view): on every tick, bar, and daily-summary table, null on any bar that spans more than one
  date. A `NONE` dataset has no library `Date` (snapshots included); a vendor field named
  `date` there keeps its own column.
- **`TimestampInsert`** is the local clock (`time.time_ns()`) when the REST page, flat-file
  block, or WebSocket frame that carried the row arrived, the same for every row of one, on
  every dataset and every transport. It is not monotonic and is not Massive's `ReceivedTimestamp`;
  on `history()` rows it is the download time, and in a keyed `poll()` table the latest poll's.
  It is never part of a row's identity.

| Rows | `TimestampStart` | `TimestampEnd` | `Date` |
|---|---|---|---|
| Tick | (none: `Timestamp`) | (none) | the day zone's date of `Timestamp`; futures: `session_end_date` from REST and flat files, the WebSocket rule below on streams |
| Intraday bar, n seconds, minutes, or hours | Massive's start | start + n units; a futures one-second WebSocket bar: Massive's `e`, or start + 1 s when Massive sends none (a WARNING when `e` is not start + 1 s) | the start's date, null when the bar ends on a later date (it crosses midnight); futures: as ticks, null when the bar crosses 17:00 Chicago time |
| 1-day bar | 00:00 of the day | 00:00 of the next day; indices 01:00 New York time (`dates.day_end`) | the day |
| n-day, week, month, quarter, year bar | 00:00 of the window's first day: for stocks and options a week's Monday and a month's, quarter's, or year's first weekday; for indices, forex, and crypto its first calendar day (a week's Sunday) | the end of the window's last day: n days count calendar days, weekends included, as Massive counts them; a stock or option period's last weekday; an index period's last calendar day, as an index can update around the clock; indices one hour later | null |
| Futures bar of a session or longer | Massive's `window_start`, unchanged: 00:00 UTC on the first day of its bucket | 17:00 Chicago time on its label, `session_end_date`, the bucket's last session | the label for a 1-session bar; null otherwise |
| Bar or indicator value of a day or longer at a stamp Massive is not known to use | Massive's stamp, unchanged | the stamp plus its period, on the zone's wall clock | null |
| Daily open-close | 00:00 of its date | as a 1-day bar | its date (`from`, crypto `day`) |
| Crypto open-close with `isUTC` false | 14 hours before 00:00 UTC of its date (00:00 at UTC+14) | 12 hours after 00:00 UTC of the next day (24:00 at UTC-12) | its date |
| Indicator value | the bar rule of the query's timespan, in the day zone of the symbol's class; a value of a day or longer is stamp-checked as that class's range bars | the same | the same |

The ends of the index bars are conservative: an index day bar is stamped at midnight in Chicago,
and whether it covers the New York or the Chicago day is not verified
([section 23](23-open-verification-items.md#23-open-verification-items) item 52). A futures bar's `TimestampStart` is a true
lower bound for every product: 17:00 Chicago was rejected because it lands after a livestock
session's 08:30 trades. Consecutive futures bars therefore overlap by about 22 hours, as
1-session bars always have. A futures bar longer than one session carries no estimate in any
column: its `TimestampEnd` is its label's close, and `history()` returns it only once it has
certainly ended (above). A 1-day index bar keeps its `Date` although its end is an hour into the
next day. A stock, option, or index bar of several days counts calendar days, as Massive does
([section 23](23-open-verification-items.md#23-open-verification-items) item 63), so a 2-day bar stamped on a Friday ends at
00:00 on Sunday, and the next one, stamped on that Sunday, at 00:00 on Tuesday. An index week, month, quarter, or year
ends after the calendar period's last day, since an index such as `I:AAVE100` updates at
weekends too. The next midnight is always the calendar's next midnight in the zone, never 24
hours later, so a day across a daylight-saving change keeps its true length. The library
computes day boundaries with `zoneinfo`, once per distinct UTC day of a batch, and applies them
with Arrow; Arrow's own time-zone database is not used.

The vendor's own time fields feed these columns and are not kept beside them: the bar start
(`t`, `s`, `window_start`) becomes `TimestampStart` (the `time` key), the WebSocket bars' `e`
becomes `TimestampEnd` (the `end` key, recomputed), and the futures `session_end_date` and the
open-close `from` and `day` become `Date` (the `date` key). Grouped daily's `{date}` needs no
column: a 1-day bar's `Date` is the day its stamp falls on, which is that date.

Joins on `Date`: a daily bar joined to another daily bar on `Date`, across symbols
(`aapl.natural_join(spy, on=["Date"], joins=["SpyClose=Close"])`), is sound. Joining an
intraday row to the same day's daily bar attaches values not known until that day ended
(`minute.natural_join(daily, on=["Symbol", "Date"], joins=["DayClose=Close"])` is lookahead).
A null `Date` matches another null in a join, so longer bars join on `TimestampEnd`. The
descriptions of `Date` and `TimestampEnd` say so.

**Futures dates on the WebSocket.** Massive's REST and flat-file futures rows carry their
session; WebSocket futures messages carry none. The flush dates each streamed futures row by the
rule the library uses everywhere, the session that ends at the next 17:00 Chicago time
(`futures_sessions.futures_session` and `futures_sessions.weekday_on_or_after`, one constant,
`FUTURES_SESSION_ROLL`, whose column forms `session_days` and `weekday_sessions` sit beside
them, and a test checks the two forms agree; a Saturday's or Sunday's moves to Monday), and corrects it with the futures schedule calendar
(`_ingest/futures_calendar.py`, `FuturesCalendar`, which reads the paths of the
`futures.contracts` and `futures.schedules` datasets), since the rule is wrong after a holiday,
which folds its trading into the next session and opens it days early, and for a product whose
pre-open starts before 17:00 Chicago time (16:45 on CBOT). The calendar looks up each contract's
product (`/futures/v1/contracts`, `ticker.any_of`) and each product's sessions from a week back
to ten days ahead (`/futures/v1/schedules`, on every futures plan, records from 2025-03-17): one
session per `session_end_date`, open from its first event (a pre-open or the open) to its last
(the close). A row inside a loaded session gets its date; a row between a close and the next
loaded session's first event gets that next session's date. The calendar belongs to the connection group, since its connections serve every
session of the group: it is created with the group's first futures connection and lives until
the group's last session leaves. One worker thread makes every request, through the REST client
of any session of the group still open (the contracts and schedules are the same for every
session of the key), when a futures stream opens with explicit symbols, when a reactive table adds some, and
when an unknown contract first streams (a `dm.ALL` stream's contracts are looked up as they
arrive); the flush never waits on it. Only a single contract ticker is looked up, by the futures
grammar of the symbols module (`symbols.is_futures_contract`); a spread or combination
(`GCQ6-GCJ7`, `BOB 02-01 U6`, `BZ:BF F7-G7-H7`, `6A:XF:AUDUSD:Q6` in the vendor's files) is
never looked up: Massive documents no grammar that names a combo's legs, so its schedule
cannot be told from its ticker. A row the schedules cannot place is a **miss**: a spread's, one
whose contract or product is still loading (a stream's first rows, or a stalled or
rate-limited lookup), one past the loaded sessions, one of a contract Massive's contracts do
not list, or one whose lookup failed. A miss gets a null `Date`: without the schedule the
rule's date can be wrong on any weekday, since a holiday folds its trading into the next
session and the library has no holiday calendar to rule that out, and a wrong date is worse
than none. Its `Timestamp` and every other column are kept, and the streams' `Error` is not
touched: the stream is healthy. A miss whose lookup is still running logs at DEBUG; any other
logs at WARNING once per product (or contract) and rule date. So that a stream's first rows
are dated, a futures stream verb with explicit contracts asks for them and waits, before it
subscribes, until their products and schedules are loaded or failed, at most 15 s
(`SCHEDULE_WAIT`): it waits only on the calendar's worker, which makes no Deephaven call, so a
console command holding the update graph's lock may run it, and such a caller waits at most 1 s
(`SCHEDULE_WAIT_HOLDING_LOCK`), since no live table updates while it waits. The contracts a
symbol table holds when the stream opens, and those it adds later, wait the same 15 s on the
session's `symbol changes` worker (section 18.1), never on the table's listener, which only
starts their lookups: the verb returns at once, and each contract subscribes once its schedule
loads. That worker runs each
session's futures and crypto changes one at a time, in order, so a removal never overtakes an
addition still waiting, and a contract the table no longer holds is never subscribed again. A wait
that runs out logs one WARNING; those rows get a null `Date` until the lookup ends. A row in
the gap between one loaded session's close and the next one's first event gets the next
session's date; a row with no readable symbol or no instant gets a null `Date` too; the 17:00
rule's date is never kept. An
answer the calendar cannot read, or any other failure of a lookup, is a miss of its contracts'
rows too, and the worker carries on. A miss past the loaded sessions asks for a reload of its
product, at most once per 10 minutes (`RELOAD_SPACING`, so a stream of misses never spends a
Basic key's 5 requests a minute), and a product's sessions are reloaded once a day as the dates
move on. Subscribing never waits on the calendar and never refuses a product it lacks.

**Price adjustment.** Massive adjusts stock and option prices and volumes for splits (forward
and reverse; whether it also adjusts for `stock_dividend` events is
[section 23](23-open-verification-items.md#23-open-verification-items) item 56) when a request says `adjusted=true`, and never
for cash dividends. Its flat files are "unadjusted… not adjusted for stock splits, dividends,
or other corporate actions" (`vendor/massive/flat-files/stocks/overview.md`) and carry no
factor; its WebSocket bars are as traded and never re-adjusted. Index, forex, crypto, and
futures bars have no corporate actions and take no `adjusted`.

- **One basis per `history()`.** With `adjusted=True`, the default of the stock and option bars
  and daily open-close, a call is split-adjusted, as Massive stands at the time of the call,
  and served by REST only: `AUTO` never chooses flat files for adjusted bars
  (`_flat_file_problems`), and an explicit `FLAT_FILES` raises. With `adjusted=False` it is as
  traded, from REST or the flat files. One call never splits its range across sources
  ([section 16](16-historysourceauto.md#16-historysourceauto)).
- **Every page is checked.** The aggregates (range, grouped daily, `/prev`) echo
  `adjusted` on every response, `next_url` pages included (`Page.adjusted`). A page whose echo
  differs from the request raises `MassiveError` naming both; a page without one logs a
  WARNING once per dataset, path, and session. The indicators and daily open-close echo
  nothing, so their basis cannot be checked; the indicators' `adjusted=None` default sends
  nothing, and Massive's default is adjusted.
- **What stays open, documented in every place below.** `bars(dm.ALL, MINUTE).history()` with
  the default `adjusted=True` raises, since the flat files serve only `adjusted=False` and REST
  needs explicit symbols; pass `adjusted=False`. A download of many requests that runs while
  Massive applies a split overnight can mix bases, which nothing can detect. Live bars are as
  traded, so `merge([history, live])` of adjusted history joins its live part seamlessly until
  the next split's execution date (pre-market included: "on the execution date, all trading is
  already adjusted"), and from then on the history is on the old basis: build it again after a
  split. The jump at a split in a live table, or in such a merge, is the data, not an error.
- **Adjusting as-traded data yourself.** The splits dataset gives each split's cumulative
  `HistoricalAdjustmentFactor`; Massive's rule is "for a price on date D, find the first split
  whose `execution_date` is after date D and multiply the unadjusted price by the
  `historical_adjustment_factor`". Since `Date` and `ExecutionDate` are both ISO text, a
  reverse as-of join does it: `bars.raj(splits, on=["Symbol", "Date<ExecutionDate"],
  joins=["F=HistoricalAdjustmentFactor"])`, then `Close * F` and `Volume / F`, a null `F` (no
  later split) meaning 1. A bar with a null `Date` (several days or longer) finds no match.
- **Where it is documented.** `bars()` of stocks and options says all of it, and the other
  classes' `bars()` say they are never adjusted; the `adjusted` argument; `history()` (the
  guarantee and what stays open); the stream verbs (the jumps at splits); `splits()` (the
  recipe); the description of every price and volume column of `stocks.bars`, `options.bars`,
  and both daily open-close datasets, through a library note (`DatasetSpec.column_notes`), so
  `columns()`, the tooltips, and the column reference show it; and the README's "Split
  adjustment" section, whose examples run in `tests/test_docs.py`.

**One-day parameters.** A dataset parameter that names one calendar day (`daily_open_close`'s
`date`, `options.contracts`' `as_of`, `reference.tickers`' and `ticker_details`' `date`)
takes a date only, like a calendar-date axis: a `datetime` raises `ValueError` ("`<name>` is a
calendar date; pass a date", `dates.calendar_day`).

The same kinds bound the time axis of a `…T` dataset on an instant axis: a timestamp
parameter, or a date-typed parameter over an instant field (`published_utc`, benzinga
`published`). A date `start` is sent as that day's 00:00 in the request's day zone and a date
`end` as the next day's 00:00, with `.gte` and `.lt`, so Massive never decides where a day
starts. A date-typed parameter takes the instant as
ISO-8601 text in UTC (`2025-06-02T04:00:00Z`); since Massive documents no
precision for that text, such an axis cannot be declared with `.gt` or `.lte` (the request
fails with `ValueError`). A calendar-date axis (`period_end`, `ex_dividend_date`, the futures
`schedules`' `session_end_date`) keeps whole-day date bounds with `.gte` and `.lte` and takes
dates only: a datetime bound there raises `ValueError` at construction ("`<axis> is a calendar
date; pass dates`"). An open bound of a `…T` dataset is not sent.

**The request's day zone** (`dates.day_class`). A dataset with an asset class follows its
class. A dataset without one uses New York days, with one exception: a dataset that declares a
path per symbol class (`fetch_paths_by_class`, the indicators, whose crypto bars have UTC
days) serves class-specific data, so each REST request, and `history()`, follows the class its
own symbols share (`symbol_class`): a crypto or forex ticker's days are UTC days, a stock,
option, or index ticker's New York days, and a request with symbols of several classes uses
New York. A filter on a publication time does not depend on which ticker the articles
mention, so `m.reference.news(symbols="X:BTCUSD", start=...)` asks for a New York window.

**Day zone per class.**

- **America/New_York** for stocks, options, and indices. Tick endpoints get ns bounds;
  flat-file partition dates are the New York dates of the first and last instant kept.
- **UTC** for forex and crypto.
- **Sessions** for futures. A date means the trading session whose `session_end_date` equals
  it. The futures trading day rolls at 17:00 America/Chicago (the weekly open is Sunday 17:00
  CT; 16:00 to 17:00 CT is the daily maintenance break), so the session of an instant `t` is
  `(t in America/Chicago + 7 h).date()`. An exchange holiday folds into the next session,
  which then opens early: CME traded Martin Luther King Day (Monday 2025-01-20) from Sunday
  17:00 CT under the trade date 2025-01-21, and after Good Friday 2024 the 2024-04-01 session
  opened on Thursday 03-28 at 17:00 CT.
  - **Trades and quotes named by dates** are requested one session at a time:
    `session_end_date=<date>` per weekday session and symbol, following `next_url`, with no
    time window, which is exact however early a holiday opened the session. Saturdays and
    Sundays end no session and are skipped. The rows come in session order and, within a
    session, in symbol order. A query that also filters `session_end_date` raises
    `ValueError`.
  - **Bars named by dates** are requested on `window_start` to 18:00 CT on the last session
    date (`bar_periods.futures_bar_request`), from: for intraday and 1-session bars, the
    first session's regular open less `FUTURES_HOLIDAY_MARGIN` (4 days: the longest known
    fold, Good Friday's, opens three days early); for bars of several sessions, 00:00 UTC on
    the first date, moved back over the weekend when it is a Monday
    (`bar_periods.futures_first_bucket_day`); for week, month, quarter, and year bars,
    00:00 UTC on the first day of the period holding the first date. They are kept as
    above. There is no probe for earlier sessions.
  - **Datetimes** are exact: `timestamp.gte`/`.lt` for trades and quotes, `window_start.gte`/
    `.lt` for bars, with the bars kept by their own window afterwards, since Massive may
    match a candle by its start.
  - **Flat files** are read one file per session date (**UNVERIFIED** that a file holds only
    its own session: the futures flat-file overview says only "standardized in Central Time").
    Each row keeps its own `session_end_date` in `Date`, whichever file it came from, so no row
    is ever labeled with a session it does not have; a call named by dates keeps the rows whose
    own session it requested, and one named by datetimes keeps rows by time. A file that holds
    rows of another session logs one WARNING per dataset per session, naming the file and the
    session, and saying that rows of a requested session may then sit in a neighbouring day's
    file, which a call by dates does not read, and be missing. The call then names those
    sessions on the table: when a file held an earlier session's rows, the range's last
    session may be incomplete, and when one held a later session's, its first; the table's
    description and a WARNING name them (`FlatFileRead.uncertain`); a row whose `session_end_date` is empty or does not parse is dropped and counted
    ([section 19.5](19-wire-clients-and-the-spec-snapshot.md#195-runtime-drift)). Dates read one file per session date and never roll forward:
    a date names its own session exactly. Datetimes read the sessions from that of `start` to
    that of `end - 1 µs`, so an intraday window reads one file and an evening window only the
    next session's file. Under `AUTO` with REST able to serve the request, a missing weekday
    session is read from REST, whose instants hold a holiday's hours
    ([section 16](16-historysourceauto.md#16-historysourceauto)). Otherwise, when the run of missing files that ends
    such a range (per venue) holds a weekday (a holiday, alone or followed by a weekend, such
    as Good Friday and the Saturday after it), the next existing session file within 7 days of
    the run's first day is read too, because the holiday's hours lie in it; its rows are
    filtered on `Timestamp` like the others. A run of weekend days after an existing Friday
    file reads nothing more. When no later session file exists yet (before the current
    session, within 7 days), the run's first missing weekday may not be published yet, and the
    call raises: the range's last hours may lie there, so it never returns a silently short
    table.

**Long downloads.** Grouped daily (`dm.ALL` 1-day bars over REST) makes one request per day,
skipping the days without trading (`dates.is_trading_day`): Saturdays and Sundays for stocks,
options, and indices, Saturdays for forex, which trades from Sunday evening to Friday evening
UTC; crypto keeps every day.
A per-session or grouped-daily download of more than five requests first logs one INFO line
with its number of requests and, when its subscription is not paced, the minutes it would take
were the subscription on the Basic tier (the first five go at once); a paced subscription's
line gives the least time the pacing makes it take ([section 13.4](13-host-resolution-and-connection-protocol.md#134-rest)). A per-session
download counts one request per session and symbol, more where a session spans several pages,
and its line says so.
