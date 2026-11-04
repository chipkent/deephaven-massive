# Troubleshooting

When something goes wrong, find the message or symptom you see in the tables here, then follow
its fix. If no row matches, look at the [diagnostics tables](#diagnostics), turn on the
[debug log](#the-log), and [report the bug](#report-a-bug). Paste each example into the console
of the Deephaven IDE.

<a id="error-messages"></a>

## Messages and symptoms

Where a message shows `...`, Massive's or the library's text has more words in that place. A
verb is the call at the end of a query that returns a table, such as `fetch()`, `history()`,
`poll()`, or `last_by()`. A subscription is what Massive sells for one asset class, such as
Stocks or Options, and each subscription has its own plan.

### Setup and keys

| You see | Why | What to do |
|---|---|---|
| `no Massive API key` | The server can't see `MASSIVE_API_KEY`. | Set the key in the terminal that starts the server, then restart the server (see [install](../README.md#install)). |
| `MassiveSession must be created inside a Deephaven server` | The code ran in a plain Python process. | Run it in the Deephaven console, or start Deephaven's [embedded server](https://deephaven.io/core/docs/getting-started/pip-install/) first. |
| `flat files need both s3_access_key_id and s3_secret_access_key` | Only one of the two S3 keys is set. | Set both keys, or neither (see [environment variables](../README.md#environment-variables)). |
| `S3 rejected the flat-file credentials` | Your S3 keys are wrong. With `source=dm.HistorySource.AUTO`, the read falls back to REST and this text arrives as a WARNING. | Copy both keys from Massive's dashboard again, then restart the server. |

### The settings file

| You see | Why | What to do |
|---|---|---|
| `no settings file: pass its path ...` | `dm.MassiveConfig.from_file()` got no path, and `DEEPHAVEN_MASSIVE_CONFIG_FILE` isn't set where the server runs. | Pass the path, or set the variable before you start the server. `help(dm.MassiveConfig.from_file)` shows an example file. |
| `settings file ..., line ..., column ...: ...` | The file holds something the library refuses, such as an unknown setting, a word in the wrong case, a key, or a YAML tag. | Fix that line. For an unknown setting, the message suggests the closest name. |

### Arguments and queries

| You see | Why | What to do |
|---|---|---|
| `timespan must be dm.Timespan, e.g. ...` | An argument that picks from a fixed set takes an enum member, not text. | Write `dm.Timespan.DAY`, not `"day"`. |
| `ValueError` that names a symbol | The symbol isn't spelled as Massive spells it. | Fix the spelling, or convert it with `dm.canonical_symbol` (see [`symbols`](datasets/stocks.bars.md#arguments)). |
| `AttributeError` that names a verb | The dataset doesn't have that verb. The message lists the verbs it has, and often what to call instead. | Call what the message suggests: another verb of the same dataset, such as `history()` for intraday bars, or another dataset, such as `m.stocks.last_trade(...)`. |
| `... poll() needs a start or a limit` | A news or filings poll without either would read the whole archive every cycle. | Give the query a `start` or a `limit`. |
| A symbol is missing from a table of a watchlist | Its row isn't spelled as Massive spells the ticker, or it's a crypto pair Massive doesn't list. The library skips the row, logs it once, and names it in the table's `Error` in `m.streams()`; the rest of the table keeps running. | Fix or delete the row. To make the call fail instead, set `noncanonical_symbols` or `unlisted_symbols` in `dm.MassiveConfig` to `dm.OnError.RAISE`. |
| A null cell, or a row missing | Massive sent a value that doesn't fit its column, which becomes a null with one WARNING, or a row without a symbol or a time, which is dropped. `SchemaDrift` in `m.streams()` counts both. A symbol Massive spells in a way the library can't read keeps its row, with a null `Symbol`, Massive's text in `RawSymbol`, and one ERROR. | Report an unreadable symbol, because it's a gap in the library. Each such problem has a `dm.MassiveConfig` setting: `noncanonical_symbols`, `unlisted_symbols`, `unavailable_symbols`, and `unreadable_symbols`. Each takes `dm.OnError.LOG` (the default), `IGNORE` (logged at DEBUG only), or `RAISE` (fail). |
| A symbol returns no rows | Massive has no data for it in that range, or its ticker changed. | List a company's past tickers with `m.reference.ticker_events("META").fetch()`. When Massive answers `HTTP 404` for the symbol, `dm.MassiveConfig(unavailable_symbols=dm.OnError.RAISE)` makes a `fetch()`, `history()`, or `poll()` fail instead. An empty answer never fails. |

<a id="plan-messages"></a>

### Plan limits and refusals

Each dataset's page in the [dataset reference](datasets/README.md) shows which plans include
it and which of its streams each plan has. Massive's free plan is called Basic, and it allows
five REST requests a minute for each subscription.

| You see | Why | What to do |
|---|---|---|
| `... would be delayed (Massive reports ...)` | Your plan's data for that subscription is 15 minutes behind, and you didn't ask for delayed data. | Add the line the message shows, or upgrade the plan (see [configuration](../README.md#configuration)). |
| `... refused the subscription to ...` | Your plan doesn't stream this data, though it can stream other data of the same asset class. | Stream bars instead of trades, or read with `history()` or `poll()`. When the message suggests `delayed_subscriptions`, add the subscription to it. |
| `delayed_subscriptions cannot hold dm.Subscription.CURRENCIES` | Forex and crypto have no delayed feed. | Remove `CURRENCIES` from the set. |
| `AuthenticationError` from a live table that mentions WebSocket streams | Massive's streaming API gives a wrong key and a plan without streams of that asset class the same answer. | Try `fetch()` with the same key. If it works, read the data with `history()` or `poll()`, because your plan doesn't stream it. |
| `HTTP 403` from `fetch()`, `poll()`, or `history()` | Your plan doesn't include this dataset. | Use the dataset the message names, where it names one (see [what your plan allows](../README.md#what-your-plan-allows)). |
| `Your plan doesn't include this data timeframe` | The range starts before your plan's history begins. | Ask for a later start. |
| `WAITING_SLOT` in `m.connections()` | Another program or computer is using your key's one connection for this asset class. | Stop the other program. The library connects as soon as the connection frees. |
| `rate limited: ...; this read ran inside a Deephaven console command` | A Basic plan's five requests a minute held a console read for more than 90 seconds. | Pass `wait=False`, and list each subscription you hold on Basic in `rest_basic_subscriptions`. |
| A download is slow | Basic plans allow five requests a minute, and `m.rate_limits()` shows the pace. | On a paid plan with [S3 keys](../README.md#environment-variables), `history()` of 50 or more symbols reads flat files. Pass `wait=False` to long reads. |

### Reading history

| You see | Why | What to do |
|---|---|---|
| `the plan does not include these flat files` | You asked for flat files your plan lacks. | Pass `source=dm.HistorySource.REST`, or leave `source` at `AUTO`. |
| `the plan does not include ... futures flat files: ...`, naming an exchange such as CME | Your futures plan lacks one exchange's flat files. | Pass `venues=` with only the exchanges your plan includes (see [`venues`](datasets/futures.trades.md#arguments)). |
| `the flat file for <date> is not published yet` | Massive publishes a day's files the next day, often by about 11:00 New York time, and sometimes later. | Retry later, or read with `source=dm.HistorySource.REST`. |
| A WARNING that a read leaves some columns empty | The source `AUTO` chose doesn't fill those columns. | Pass the `source=` the WARNING names (`help(dm.HistorySource)` describes each source). |
| `... which is not a stamp Massive is known to use` | Massive stamped a bar at an unexpected time. | Use the bar at Massive's stamp, with no `Date`, and [report it](#report-a-bug). |
| Prices jump at a split | Live bars and flat files are as traded, while adjusted history is restated in today's shares. | Read REST history with `adjusted=True`, the default, or adjust the prices yourself with `m.stocks.splits`. |

<a id="live-table-messages"></a>

### Live tables

The `State`, `Recency`, `Error`, and `Gaps` columns named in these rows are columns of
`m.streams()`, described in [streams](#streams-columns). A warning in `Error` leaves the table
running, while a table that stops shows only why it stopped. A watchlist is a live table of
symbols that a verb follows as you add and delete rows, and `dm.ALL` asks for every symbol in
the market.

| You see | Why | What to do |
|---|---|---|
| A live table stays empty | The market is closed, a console command is still running, the table reads Massive's delayed feed, or the stream failed. | Check `m.reference.market_status().fetch()`, then the table's `State`, `Recency`, and `Error` in `m.streams()`. |
| `MassiveSession #1 was closed; this table no longer updates` | The session was closed. | Take `table.snapshot()` to keep the rows, and create a new session for new tables. |
| `MassiveSession #1 is closed` from a verb or `m.streams()` | You called the session after `m.close()`. | Create a new session. |
| `Can not listen to failed table` | Deephaven can't build a live table on one that has failed. | Work on `table.snapshot()`. |
| `... inside a Deephaven console command, which holds the update graph's lock` | A long command paused every live table on the server. | Pass `wait=False` to long reads (see [backfill, then go live](../README.md#backfill-then-live)). |
| `connection down since <time>; no data` in `Error` | The connection to Massive dropped. | Wait for the library to reconnect. `Gaps` keeps the span whose rows may be missing. |
| `... longer than MassiveConfig.max_outage` | An outage, or failing or silent poll cycles, lasted longer than `max_outage`, so the table stopped. | Fix any error the message names, then call the verb again. To wait longer, raise `max_outage`. |
| `no data since <time> during ... trading hours` in `Error` | Nothing arrived for 5 minutes while the market was open, on a holiday or for quiet symbols. | Nothing. The table keeps running. |
| `cycle running since <time>; last good cycle at <time>` in `Error` | A poll cycle is running late, often because rate limits are pacing its requests. | Check `m.rate_limits()`. The note clears when the cycle ends. |
| An error, then `last good cycle at <time>`, in `Error` | A poll cycle failed with that error. | Fix the error if it names your input. The next cycle runs on schedule. |
| `no rows this cycle for ...` in `Error` | Massive has no data for those symbols (`HTTP 404`), or their endpoint doesn't take an argument you passed, such as `adjusted=False` for a crypto pair. | Fix or remove the symbols named. The other symbols keep polling. |
| `skipping ... symbol-table values this dataset does not take: ...` in `Error` | Rows of your watchlist aren't spelled as Massive spells the ticker, or the dataset doesn't take them. | Fix or delete those rows. The stream subscribes to a row once you fix it. |
| `skipped crypto pairs ...` in `Error` | Massive's list of crypto tickers doesn't hold those pairs, or the list couldn't be read. | Check the spelling, then delete each pair's row and add it back to try it again. |
| `... symbols retrying after a failure that may pass: ...` in `Error` | Reads of those watchlist symbols failed for a reason that can clear, such as a rate limit. | Wait. The library reads each one again 1, 5, and 15 minutes later. |
| `... symbols failed (delete and re-add a row to read one again): ...` in `Error` | Reads of those watchlist symbols failed for good, such as on a plan refusal. | Fix the cause, then delete each symbol's row and add it back. |
| `no acknowledgement from Massive for ...` in `Error` | Massive hasn't confirmed the subscription, and the library is sending it again. | If the note stays, rows of those symbols may be missing, so [report it](#report-a-bug). |
| `over capacity: <n>/1000; <k> deferred` in `Error` | Your option streams reached Massive's limit of 1,000 on one connection. Each contract's trades count as one, and its quotes as another. | Watch fewer contracts. The waiting ones subscribe as others leave (see [the option chain](../README.md#option-chain)). |
| `rows for ... are being dropped` in `Error` | Massive sent rows of a `dm.ALL` stream that the library can't read. | Treat those symbols' last values as stale, and [report it](#report-a-bug) with the log. |
| `... the last 100 rows of channel ... were dropped` | The library couldn't read 100 rows in a row, so it stopped the stream. Massive has most likely changed its format. | [Report it](#report-a-bug) with the log. |
| Memory keeps growing | `append_only()` keeps every row. | Use `ring(n)`, `last_by()`, or `blink()` with an aggregation. |
| A table keeps running after `del` | Python hasn't reclaimed the table yet. | Call `m.release(table)`. |
| The server exits after `m.close()` | A `where_in` or `where_not_in` between two live tables can make the server exit when both fail at once, and `m.close()` fails every live table of the session together. | Filter with a join instead: `latest.natural_join(marks, on="Symbol", joins="Watched").where("Watched == true")`, where `marks` is the other table's `select_distinct("Symbol")` with `Watched = true` added. |

<a id="diagnostics"></a>

## Diagnostics tables

Four live tables show what a session is doing, and `m.streams()` is the place to start when a
table looks wrong. Call them on the session whose tables you're checking. `m.streams()` and
`m.symbol_reads()` show only that session's tables, while `m.connections()` and `m.rate_limits()`
also show what it shares with other sessions that use the same API key. Each call returns a new live
table, which keeps its final rows after `m.close()`. This example polls Apple's news, which works
on every Stocks plan, and then makes all four tables:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
news = m.reference.news("AAPL", limit=10).poll(300)
streams = m.streams()
connections = m.connections()
limits = m.rate_limits()
reads = m.symbol_reads()
```

`streams` has one row, for the news poll, and `limits` shows the pace of the REST requests it
sends. `connections` and `reads` have no rows for it, because the poll opens no WebSocket
connection and follows no watchlist. `m.streams()` tracks every table a verb returns, and the
other three track what feeds those tables:

| Table | It has one row for each |
|---|---|
| `m.streams()` | Stream, poll, background read (`wait=False`), and `history()` or `fetch()` of a [watchlist](../README.md#drive-with-a-watchlist) |
| `m.connections()` | WebSocket connection, plus a `REST` row for each asset class and kind of data that records which Massive server the library chose to stream from, and why |
| `m.rate_limits()` | Subscription whose REST requests the library paces, plus `other` for the datasets Massive assigns to no subscription |
| `m.symbol_reads()` | Symbol of each `history()` or `fetch()` of a watchlist |

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
| `CLOSED` | The session was closed. |

These are the columns you'll read most:

| Column | What it shows |
|---|---|
| `Query` | The call that opened it, such as `m.stocks.trades('AAPL')`. |
| `Symbols`, `SymbolCount` | The symbols it reads now, updated as a watchlist changes. For a `poll()`, `SymbolCount` counts the symbols its latest cycle read. |
| `Deferred` | How many option contracts are waiting under Massive's limit of 1,000 on one connection. |
| `Loading` | How many symbols a `history()` or `fetch()` of a watchlist still has to read. |
| `Recency` | Whether its rows are real-time (`REALTIME`), delayed (`DELAYED`), or not yet known (`UNKNOWN`). [Configuration](../README.md#configuration) says how to accept delayed data. |
| `RowsPublished` | How many rows it has delivered so far. |
| `Error` | Why it stopped, or the warnings that leave it running, joined with `;`. Each warning has a row in [live tables](#live-table-messages). |
| `Gaps` | Each span over which rows may be missing, such as a recovered outage. It stays after the `Error` note clears. |
| `SchemaDrift` | Differences between what Massive sent and what Massive's documentation says, such as a field Massive added or stopped sending, a value that didn't fit its column, or rows dropped. `help(m.streams)` lists each mark. |

### Read the connections and rate-limits tables

Most of the time these two tables need nothing from you. These states do:

| Table | State | Meaning | What to do |
|---|---|---|---|
| `m.connections()` | `WAITING_SLOT` | Another program or computer is using your key's one connection for this group of streams, such as stocks or crypto. | Stop the other program. The library connects as soon as the connection frees. |
| `m.connections()` | `FAILED` | The connection stopped, and `LastError` says why. | Fix what `LastError` names, then call the verb again. |
| `m.rate_limits()` | `PACED` | The library spaces requests to fit a Basic plan's five a minute. | Nothing, or pass `wait=False` to long reads. |
| `m.rate_limits()` | `PAUSED` | Massive refused a request for coming too fast, and requests wait until `PausedUntil`. | Wait. Programs elsewhere that use your key count against the same limit. |

Give every session on a server the same key and the same connection settings, such as
`max_outage`, so that they share their connections. Sessions with different settings compete for
one connection.

### Read the symbol-reads table

The `State` column of `m.symbol_reads()` shows where each symbol's read stands:

| State | Meaning |
|---|---|
| `QUEUED` | The symbol is waiting to be read. |
| `LOADING` | The symbol is being read. |
| `RETRYING` | The read failed for a reason that can clear, and the library tries again at `NextAttemptAt`. |
| `DONE` | The symbol's rows are in the table. |
| `FAILED` | The read failed for good, and `Error` says why. Delete the symbol's row and add it back to read it again. |
| `REMOVED`, `STOPPED`, `CLOSED` | The symbol's row left the watchlist, you released the table, or the session was closed. |

`ReadAt` is when the rows you see were downloaded, and `Rows` counts them. To list the symbols
whose reads failed, filter the table of the session that follows the watchlist. In a fresh
console, as pasted here, the session follows nothing, so the table is empty:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
failed = m.symbol_reads().where("State == `FAILED`")
```

On the session that follows the watchlist, `failed` has a row for each failed symbol, with the
reason in `Error`.

<a id="the-log"></a>

## Turn on the debug log

The library writes its warnings and errors to Deephaven's log, which the Deephaven IDE shows in
its **Log** panel. The debug log adds INFO and DEBUG lines that say what the library decided
and why. A WARNING or ERROR line asks something of you, and INFO lines need nothing:

| Level | Meaning | What to do |
|---|---|---|
| INFO | A decision, or progress. | Nothing. |
| WARNING | A live table may be stale or wrong, or something needs you. It's logged once when it starts, and an INFO line says when it recovers. | Read the message. |
| ERROR | A defect in your code or input, whose message names the fix, or a bug in the library, whose message says `This is a bug in deephaven-massive.` | Fix your code, or [report the bug](#report-a-bug). |

To record everything the library does while some code runs, turn the debug log on around it.
This example reads a week of Apple's daily bars, which works on every Stocks plan:

```python
import deephaven_massive as dm

dm.enable_debug_logging()  # to the Log panel; pass a file path to write to a file instead
m = dm.MassiveSession()
week = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2025-06-02", "2025-06-06")
dm.disable_debug_logging()
```

The **Log** panel then shows a line for the session's settings, each request the read sent, and
what came back. To capture your own problem, replace the read with the code that fails.

To capture a problem from the first session on, set `DEEPHAVEN_MASSIVE_LOG` to `DEBUG`, or to
`INFO` for a long run. To write the log to a file, set `DEEPHAVEN_MASSIVE_LOG_FILE` to its path.
Set them in the terminal that starts the server, as the README's
[configuration](../README.md#configuration) shows. The library reads them when you create the
first session. The log never holds your API key or S3 secret.

The log has no size limit and is never rotated, and it grows with the number of requests, so
turn it on right before the failing code and off after it, and use `INFO` for long runs.

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
