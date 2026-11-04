# Troubleshooting

When something goes wrong, find the message or symptom you see in the tables here, then follow
its fix. If no row matches, look at the [diagnostics tables](#diagnostics), turn on the
[debug log](#the-log), and [report the bug](#report-a-bug). Paste each example into the console
of the Deephaven IDE.

<a id="error-messages"></a>

## Find your message or symptom

Where a message in these tables shows `...`, the text you see has more words in that place.

### Setup and keys

These messages come from a server or a session that can't start:

| You see | Why | What to do |
|---|---|---|
| `no Massive API key` | The server can't see `MASSIVE_API_KEY`. | Set the key in the terminal that starts the server, then restart the server, as [Install](../README.md#install) shows. |
| `RuntimeError: The Deephaven Server has not been initialized` | The code ran in plain Python, outside the server. | Paste it into the console of the Deephaven IDE, or start Deephaven's [embedded server](https://deephaven.io/core/docs/getting-started/pip-install/) first. |
| `MassiveSession must be created inside a Deephaven server` | The code ran on a thread with no Deephaven execution context, such as a thread you started yourself. | Create the session in the console or an application script, or enter a context first, as the message shows. |
| `flat files need both s3_access_key_id and s3_secret_access_key` | Only one of the two S3 keys is set. | Set both [S3 keys](../README.md#environment-variables), or neither. |
| `S3 rejected the flat-file credentials` | Your S3 keys are wrong. With `source=dm.HistorySource.AUTO`, the read falls back to REST and this text arrives as a WARNING. | Copy both keys from Massive's dashboard again, then restart the server. |

### Settings file

These messages come from `dm.MassiveConfig.from_file()`:

| You see | Why | What to do |
|---|---|---|
| `no settings file: pass its path ...` | You called it with no path, and `DEEPHAVEN_MASSIVE_CONFIG_FILE` isn't set where the server runs. | Pass the path, or set the variable before you start the server. `help(dm.MassiveConfig.from_file)` shows an example file. |
| `settings file ..., line ..., column ...: ...` | The file holds something the library refuses, such as an unknown setting, a word in the wrong case, a key, or a YAML tag. | Fix that line. For an unknown setting, the message suggests the closest name. |

### Arguments and queries

These rows cover a call written in a way the library refuses, and a table that's missing a
symbol or a value:

| You see | Why | What to do |
|---|---|---|
| `timespan must be dm.Timespan, e.g. ...` | An argument that picks from a fixed set takes an enum member, not text. | Write `dm.Timespan.DAY`, not `"day"`. |
| `ValueError` that names a symbol | The symbol isn't spelled as Massive spells it. | Fix the spelling with the [symbol spellings in Example 2](../README.md#compare-stocks), or convert it with `dm.canonical_symbol`. |
| `AttributeError` that names a verb, the call that ends a query, such as `history()` | The dataset doesn't have that verb. The message lists the verbs it has, and often what to call instead. | Call what the message suggests, such as `history()` for intraday bars, or another dataset, such as `m.stocks.last_trade(...)`. |
| `... poll() needs a start or a limit` | A news or filings poll without either would read the whole archive every cycle. | Give the query a `start` or a `limit`. |
| A symbol is missing from a table that follows a watchlist, a live table of symbols you edit | The symbol's row isn't spelled as Massive spells the ticker, or it's a crypto pair Massive doesn't list. The library skips that row and names it in the table's `Error` in `m.streams()`. | Fix or delete the row. To make the call fail instead, change its [skip setting](#on-error). |
| A null cell | Massive sent a value that doesn't fit the column's type. The log has one WARNING, and the table's `SchemaDrift` in [`m.streams()`](#streams-columns) counts it. | If a column you need stays null, [report it](#report-a-bug) with the log. |
| A row missing | Massive sent the row without a symbol or a time, so the library dropped it. The log has one WARNING, and `SchemaDrift` counts it. | `help(m.streams)` lists each `SchemaDrift` mark. [Report it](#report-a-bug) if rows you need are missing. |
| A null `Symbol`, with Massive's text in `RawSymbol` | Massive spelled a symbol in a way the library can't read. The row stays, and the log has one ERROR. | [Report it](#report-a-bug), because it's a gap in the library. |
| A symbol returns no rows | Massive has no data for it in that range, or its ticker changed. | List a company's past tickers with `m.reference.ticker_events("META").fetch()`. To make a read fail when Massive answers `HTTP 404` for a symbol, which an empty answer never does, change its [skip setting](#on-error). |

<a id="on-error"></a>

### Settings for skipped symbols and rows

When one symbol or row has a problem, the library skips it, and the rest of the table keeps
running. Each kind of problem has its own `dm.MassiveConfig` setting:

| Setting | The problem |
|---|---|
| `noncanonical_symbols` | A row of a watchlist isn't spelled as Massive spells the ticker |
| `unlisted_symbols` | A crypto pair of a watchlist isn't in Massive's list of crypto tickers |
| `unavailable_symbols` | Massive answers `HTTP 404` for a symbol of a `fetch()`, `history()`, or `poll()` |
| `unreadable_symbols` | Massive sends a symbol spelled in a way the library can't read |

Each setting takes one of three values:

| Value | What happens | What the log shows |
|---|---|---|
| `dm.OnError.LOG`, the default | The symbol is left out, or an unreadable one keeps its row with a null `Symbol` | One WARNING for each value, or one ERROR for an unreadable symbol |
| `dm.OnError.IGNORE` | The same as `LOG` | Only DEBUG lines |
| `dm.OnError.RAISE` | The read or the live table fails, naming the value | The failure |

This session fails a read when Massive answers `HTTP 404` for one of its symbols:

```python
import deephaven_massive as dm

config = dm.MassiveConfig(unavailable_symbols=dm.OnError.RAISE)
m = dm.MassiveSession(config)
```

With this session, a `fetch()` or `history()` that meets such a symbol raises an error that
names it, and a `poll()` table stops with that error. Without the setting, the symbol is left
out and the rest of the read goes on.

<a id="plan-messages"></a>

### Plan limits and refusals

These messages mean your Massive plan doesn't include what you asked for, or limits how fast you
can ask. Each dataset's page in the [dataset reference](datasets/README.md) shows which plans
include it.

| You see | Why | What to do |
|---|---|---|
| `... would be delayed (Massive reports ...)` | Your plan's data for that market is 15 minutes behind, and you didn't ask for delayed data. | Add the line the message shows, as in [settings in code](../README.md#settings-in-code), or upgrade the plan. |
| `... refused the subscription to ...` | Your plan doesn't stream this data, though it can stream other data of the same market. | Stream bars instead of trades, or read with `history()` or `poll()`. When the message names `delayed_subscriptions`, add that market to it, such as `dm.Subscription.STOCKS`. |
| `delayed_subscriptions cannot hold dm.Subscription.CURRENCIES` | Forex and crypto, which Massive sells together as Currencies, have no delayed feed. | Remove `CURRENCIES` from the set. |
| `AuthenticationError` from a live table that mentions WebSocket streams | Massive's streaming API gives the same answer for a wrong key and for a plan without streams of that market. | Try `fetch()` with the same key. If it works, your plan doesn't stream this data, so read it with `history()` or `poll()`. |
| `HTTP 403` from `fetch()`, `poll()`, or `history()` | Your plan doesn't include this dataset. | Use the dataset the message names, where it names one, as [what your plan allows](../README.md#what-your-plan-allows) explains. |
| `Your plan doesn't include this data timeframe` | The range starts before your plan's history begins. | Ask for a later start. |
| `WAITING_SLOT` in `m.connections()` | Another program or computer is using your key's one connection for this market. | Stop the other program. The library connects as soon as the connection frees. |
| `rate limited: ...; this read ran inside a Deephaven console command` | Massive kept refusing requests for coming too fast (`HTTP 429`), and a read run in the console gives up after 90 seconds of those pauses. Another program that uses the same key is the usual cause. | Stop the other program, or pass `wait=False`, which waits up to 10 minutes. On a Basic plan, list the market in `rest_basic_subscriptions`. |
| A download is slow | Each Basic plan allows five requests a minute. `m.rate_limits()` shows the pace. | On a paid plan with [S3 keys](../README.md#environment-variables), `history()` of 50 or more symbols reads flat files. Pass `wait=False` to reads that take more than a few seconds. |

### History reads

These rows cover `history()` and the flat files it reads:

| You see | Why | What to do |
|---|---|---|
| `the plan does not include these flat files` | You asked for flat files your plan lacks. | Pass `source=dm.HistorySource.REST`, or leave `source` at `AUTO`. |
| `the plan does not include ... futures flat files: ...`, naming an exchange such as CME | Your futures plan lacks one exchange's flat files. | Pass only the exchanges your plan includes, in `venues=`, as the [futures trades page](datasets/futures.trades.md#arguments) shows. |
| `the flat file for <date> is not published yet` | Massive publishes a day's flat files the next day, usually by 11:00 New York time. | Retry later, or read with `source=dm.HistorySource.REST`. |
| A WARNING that a read leaves some columns empty | The source `AUTO` chose doesn't fill those columns. | Pass the `source=` the WARNING names. `help(dm.HistorySource)` describes each source. |
| `... which is not a stamp Massive is known to use` | Massive timed a bar at a moment the library doesn't expect. The library keeps the bar at Massive's time and leaves its `Date` empty. | Use the bar as it is, and [report it](#report-a-bug). |
| Prices jump at a split | Live bars and flat files are as traded, while adjusted history is restated in today's shares. | Read history with `adjusted=True`, the default, or adjust the prices yourself, as [adjust prices for splits](../README.md#split-adjustment) shows. |

<a id="live-table-messages"></a>

### Live tables

These rows cover tables that a stream or a poll keeps updating. The columns they name, such as
`Error`, are columns of `m.streams()`, which [the streams table](#streams-columns) describes.

| You see | Why | What to do |
|---|---|---|
| A live table stays empty while the market is closed | No data arrives until the market opens. | Check `m.reference.market_status().fetch()`. |
| A live table stays empty for the first 15 minutes after the open | Your plan's feed is delayed. | Wait. `Recency` in `m.streams()` shows `DELAYED`. |
| A live table stays empty while a console command runs | Every live table on the server pauses until the command ends. | Pass `wait=False` to long reads. |
| A live table stays empty, and its `State` is `FAILED` | The stream or poll stopped. | Read why in its `Error`. |
| `MassiveSession #1 was closed; this table no longer updates` | The session was closed. | Take `table.snapshot()` to keep the rows, and create a new session for new tables. |
| `MassiveSession #1 is closed` from a verb or `m.streams()` | You called the session after `m.close()`. | Create a new session. |
| `Can not listen to failed table` | Deephaven can't build a live table on one that has failed. | Work on `table.snapshot()`. |
| `... inside a Deephaven console command, which holds the update graph's lock` | A long command paused every live table on the server. | Pass `wait=False` to long reads, as [start with today's history, then continue live](../README.md#backfill-then-live) shows. |
| `connection down since <time>; no data` in `Error` | The connection to Massive dropped. | Wait for the library to reconnect. `Gaps` keeps the span whose rows may be missing. |
| `... longer than MassiveConfig.max_outage` | The library stopped the table because its connection stayed down, its poll cycles kept failing, or a late cycle received nothing, for longer than `max_outage`. | Fix any error the message names, then call the verb again. To wait longer than the default 300 seconds, raise `max_outage`. |
| `no data since <time> during ... trading hours` in `Error` | Nothing arrived for 5 minutes during the market's regular hours. The library has no holiday calendar, so a holiday or a quiet symbol shows this note. | Nothing. The table keeps running. |
| `cycle running since <time>; last good cycle at <time>` in `Error` | A poll cycle is running late, as it does when rate limits pace its requests. | Check `m.rate_limits()`. The note clears when the cycle ends. |
| An error, then `last good cycle at <time>`, in `Error` | A poll cycle failed with that error. | Fix the error if it names your input. The next cycle runs on schedule. |
| `no rows this cycle for ...` in `Error` | Massive has no data for those symbols (`HTTP 404`), or their endpoint doesn't take an argument you passed, such as `adjusted=False` for a crypto pair. | Fix or remove the symbols named. The other symbols keep polling. |
| `skipping ... symbol-table values this dataset does not take: ...` in `Error` | Rows of your watchlist aren't spelled as Massive spells the ticker, or the dataset doesn't take them. | Fix or delete those rows. The stream subscribes to a row once you fix it. |
| `skipped crypto pairs ...` in `Error` | Massive's list of crypto tickers doesn't hold those pairs, or the list couldn't be read. | Check the spelling, then delete each pair's row and add it back to try it again. |
| `... symbols retrying after a failure that may pass: ...` in `Error` | Reads of those watchlist symbols failed for a reason that can clear, such as a rate limit. | Wait. The library reads each one again 1, 5, and 15 minutes later. |
| `... symbols failed (delete and re-add a row to read one again): ...` in `Error` | Reads of those watchlist symbols failed for good, such as on a plan refusal. | Fix the cause, then delete each symbol's row and add it back. |
| `no acknowledgement from Massive for ...` in `Error` | Massive hasn't confirmed the subscription, and the library is sending it again. | If the note stays, rows of those symbols may be missing, so [report it](#report-a-bug). |
| `over capacity: <n>/1000; <k> deferred` in `Error` | Your option streams reached the library's limit of 1,000 on one connection. Each contract's trades count as one stream, and its quotes as another. | Watch fewer contracts. The waiting ones subscribe as others leave, as [the option chain](../README.md#option-chain) shows. |
| `rows for ... are being dropped` in `Error` | Massive sent rows of a `dm.ALL` stream that the library can't read. | Treat those symbols' last values as stale, and [report it](#report-a-bug) with the log. |
| `... the last 100 rows of channel ... were dropped` | The library couldn't read 100 rows in a row, so it stopped the stream. Massive has most likely changed its format. | [Report it](#report-a-bug) with the log. |
| Memory keeps growing | `append_only()` keeps every row. | Use `ring(n)`, `last_by()`, or `blink()` with an aggregation. |
| A table keeps running after `del` | Python hasn't reclaimed the table yet. | Call `m.release(table)`. |
| The server exits after `m.close()` | `m.close()` fails every live table of the session in one update cycle. If both tables of a `where_in` or `where_not_in` fail in the same cycle, Deephaven can stop the server. | Filter with a join instead: `latest.natural_join(marks, on="Symbol", joins="Watched").where("Watched == true")`, where `marks` is the other table's `select_distinct("Symbol")` with `Watched = true` added. |

<a id="diagnostics"></a>

## Read the diagnostics tables

Four live tables show what a session is doing, and `m.streams()` is the place to start when a
table looks wrong. Call each one on the session whose tables you're checking, such as
`m.streams()` for the session `m`. This example polls Apple's news, which works on every Stocks
plan, then makes all four tables:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
news = m.reference.news("AAPL", limit=10).poll(300)
streams = m.streams()
connections = m.connections()
limits = m.rate_limits()
reads = m.symbol_reads()
```

`streams` has one row, for the news poll, and `limits` shows the pace of the poll's requests.
`connections` and `reads` stay empty, because a poll needs no streaming connection and follows
no watchlist. Each table tracks a different part of the session:

| Table | It has one row for each |
|---|---|
| `m.streams()` | Stream, poll, background read (`wait=False`), and `history()` or `fetch()` of a [watchlist](../README.md#drive-with-a-watchlist) of this session |
| `m.connections()` | Streaming connection to Massive, shared by every session on the server that uses the same key, plus a `REST` row for each market and kind of data that says whether its streams use Massive's real-time or delayed feed |
| `m.rate_limits()` | Subscription whose REST requests the library paces, shared the same way, plus `other` for the datasets Massive assigns to no subscription |
| `m.symbol_reads()` | Symbol of each `history()` or `fetch()` of a watchlist of this session |

<a id="streams-columns"></a>

### Read the streams table

The `State` column of `m.streams()` shows where each table stands:

| State | Meaning |
|---|---|
| `RUNNING` | The stream or poll is delivering rows. A `history()` or `fetch()` of a watchlist reaches `RUNNING` once every symbol has been read or has failed. |
| `LOADING` | A background read is downloading, or a `history()` or `fetch()` of a watchlist has a symbol queued, being read, or waiting to retry. |
| `DONE` | A background read finished. |
| `RELEASED` | You released the table, but a derived table or an open panel still uses it. |
| `FAILED` | The table stopped, and `Error` says why. |
| `CLOSED` | The session was closed. The diagnostics tables keep their final rows. |

The other columns say what each table reads and what went wrong:

| Column | What it shows |
|---|---|
| `Query` | The call that opened it, such as `m.stocks.trades('AAPL')`. |
| `Symbols`, `SymbolCount` | The symbols it reads now, updated as a watchlist changes. For a `poll()`, `SymbolCount` counts the symbols its latest cycle read. |
| `Deferred` | How many option streams are waiting under the limit of 1,000 on one connection. |
| `Loading` | How many symbols a `history()` or `fetch()` of a watchlist still has to read. |
| `Recency` | Whether its rows are real-time (`REALTIME`), delayed (`DELAYED`), or not yet known (`UNKNOWN`). [Settings in code](../README.md#settings-in-code) says how to accept delayed data. |
| `RowsPublished` | How many rows it has delivered so far. |
| `Error` | Why it stopped, or the warnings that leave it running, joined with `;`. Each warning has a row in [live tables](#live-table-messages). |
| `Gaps` | Each span over which rows may be missing, such as a recovered outage. It stays after the `Error` note clears. |
| `SchemaDrift` | Differences between what Massive sent and what Massive's documentation says, such as a field Massive added or stopped sending, a value that didn't fit its column, or rows dropped. `help(m.streams)` lists each mark. |

### Read the connections and rate-limits tables

These states of the two tables tell you what to do, if anything:

| Table | State | Meaning | What to do |
|---|---|---|---|
| `m.connections()` | `WAITING_SLOT` | Another program or computer is using your key's one connection for this market, such as stocks or crypto. | Stop the other program. The library connects as soon as the connection frees. |
| `m.connections()` | `WAITING_SLOT`, with no other program running | Sessions on this server use the same key with different connection settings, such as `max_outage`, so they compete for one connection. | Give every session on the server the same key and the same connection settings. |
| `m.connections()` | `FAILED` | The connection stopped, and `LastError` says why. | Fix what `LastError` names, then call the verb again. |
| `m.rate_limits()` | `PACED` | The library spaces requests to fit a Basic plan's five a minute. | Nothing. For a long read in the console, pass `wait=False`, so live tables keep updating. |
| `m.rate_limits()` | `PAUSED` | Massive refused a request for coming too fast, and requests wait until `PausedUntil`. | Wait. Programs elsewhere that use your key count against the same limit. |

<a id="symbol-reads"></a>

### Read the symbol-reads table

`m.symbol_reads()` shows where the read of each watchlist symbol stands, so you can find the
ones that failed. Its `State` column takes these values:

| State | Meaning |
|---|---|
| `QUEUED` | The symbol is waiting to be read. |
| `LOADING` | The symbol is being read. |
| `RETRYING` | The read failed for a reason that can clear, and the library tries again at `NextAttemptAt`. |
| `DONE` | The symbol's rows are in the table. `ReadAt` is when they were downloaded, and `Rows` counts them. |
| `FAILED` | The read failed for good, and `Error` says why. Delete the symbol's row and add it back to read it again. |
| `REMOVED`, `STOPPED`, `CLOSED` | The symbol's row left the watchlist, you released the table, or the session was closed. |

This example reads a week of daily bars for a two-symbol watchlist, which works on every Stocks
plan, then keeps the symbols whose reads failed:

```python
import deephaven_massive as dm
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

m = dm.MassiveSession()

watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
watchlist.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))
bars = m.stocks.bars(watchlist, dm.Timespan.DAY).history(
    "2026-10-26", "2026-10-30"
)
reads = m.symbol_reads()
failed = reads.where("State == `FAILED`")
```

`reads` has a row for AAPL and one for MSFT, each `DONE` once its bars are in `bars`. `failed`
stays empty unless a read fails, and then it has that symbol's row, with the reason in `Error`.

<a id="the-log"></a>

## Turn on the debug log

The library writes its warnings and errors to Deephaven's log, which the Deephaven IDE shows in
its **Log** panel. The debug log adds INFO and DEBUG lines that say what the library decided
and why. Each level asks something different of you:

| Level | Meaning | What to do |
|---|---|---|
| INFO | A decision, or progress. | Nothing. |
| WARNING | A live table may be stale or wrong, or something needs you. It's logged once when it starts, and an INFO line says when it recovers. | Read the message. |
| ERROR | A defect in your code or input, whose message names the fix, or a bug in the library, whose message says `This is a bug in deephaven-massive.` | Fix your code, or [report the bug](#report-a-bug). |

To record everything the library does while some code runs, turn the debug log on around it.
This example reads a week of Apple's daily bars, which works on every Stocks plan:

```python
import deephaven_massive as dm

# Pass a file path to write the log to a file instead of the Log panel.
dm.enable_debug_logging()
m = dm.MassiveSession()
week = m.stocks.bars("AAPL", dm.Timespan.DAY).history(
    "2025-06-02", "2025-06-06"
)
dm.disable_debug_logging()
```

The **Log** panel then shows a line for the session's settings, each request the read sent, and
what came back. To capture your own problem, replace the read with the code that fails.

To capture a problem from the first session on, set `DEEPHAVEN_MASSIVE_LOG` to `DEBUG`, or to
`INFO` for a long run. To write the log to a file, set `DEEPHAVEN_MASSIVE_LOG_FILE` to its path.
Set both in the terminal that starts the server, as the README's
[environment variables](../README.md#environment-variables) show. The library reads them when
you create the first session. The log never holds your API key or S3 secret.

The log has no size limit and grows with every request, so turn it on right before the failing
code and off after it.

<a id="report-a-bug"></a>

## Report a bug

Open an [issue](https://github.com/chipkent/deephaven-massive/issues) with three things:

1. What you ran, and what you expected.
2. The debug log file. GitHub cuts an issue's text at 65,536 characters, so attach the file,
   and zip a larger one.
3. The diagnostics tables of the session that failed, printed right after the failure.

This function prints a session's four diagnostics tables, up to 1,000 rows each. Paste it into
the console where the failure happened, and call it with the session that failed, such as
`print_diagnostics(m)`:

```python
def print_diagnostics(session):
    for table in (
        session.streams(),
        session.connections(),
        session.rate_limits(),
        session.symbol_reads(),
    ):
        print(table.to_string(num_rows=1000))
```
