# Troubleshooting

When something goes wrong, find the message or symptom you see in the tables here, then follow
its fix. Most messages name their own fix, and the tables add the cause and a link to the
details. If nothing fits, look at the [diagnostics tables](#diagnostics), turn on the
[debug log](#the-log), and [report a bug](#report-a-bug).

<a id="error-messages"></a>

## Messages and symptoms

The tables group the messages by where you meet them, from setting up to live tables. A message shown with `...` has more text in place of the
dots.

### Setup and keys

| You see | Why | What to do |
|---|---|---|
| `no Massive API key` | The server can't see `MASSIVE_API_KEY` | Set it in the terminal that starts the server, then restart the server (see [give the server your API key](install.md#api-key)) |
| `MassiveSession must be created inside a Deephaven server` | The code ran in a plain Python process | Run it in the Deephaven console, or start an [embedded server](install.md#embedded) first |
| `flat files need both s3_access_key_id and s3_secret_access_key` | Only one of the two S3 keys is set | Set both, or neither (see [S3 keys](install.md#s3-keys)) |
| `S3 rejected the flat-file credentials` | Your S3 keys are wrong. With `source=dm.HistorySource.AUTO`, the read falls back to REST and this arrives as a WARNING. | Copy both keys from Massive's dashboard again and restart the server |

### The settings file

| You see | Why | What to do |
|---|---|---|
| `no settings file: pass its path ...` | `dm.MassiveConfig.from_file()` got no path, and `DEEPHAVEN_MASSIVE_CONFIG_FILE` isn't set where the server runs | Pass the path, or set the variable before you start the server (see [settings file](install.md#settings-file)) |
| `settings file ... does not exist` | The path is wrong, or, in Docker, the file isn't mounted where the variable says | Check the path, or the `-v` and `-e DEEPHAVEN_MASSIVE_CONFIG_FILE` options |
| `Is a directory` from `from_file()` | Docker created a folder at the path, because the file didn't exist on your computer | Create the file first, then start the container again |
| `settings file ..., line ..., column ...: ...` | The file holds something the library refuses, such as an unknown setting, a word in the wrong case, a key, or a YAML tag | Fix that line. The message suggests the closest setting name. |

### Calls

| You see | Why | What to do |
|---|---|---|
| `timespan must be dm.Timespan, e.g. ...` | An argument that picks from a set takes an enum member, not text | Write `dm.Timespan.DAY`, not `"day"` |
| `ValueError` that names a symbol | The symbol isn't spelled as Massive spells it | Fix the spelling, or convert it (see [symbols](arguments.md#symbols)) |
| `AttributeError` that names a verb | The dataset doesn't have that verb | Use the dataset the message names |
| `... poll() needs a start or a limit` | A news or filings poll without either would read the whole archive every cycle | Give the query a `start` or a `limit` |
| A symbol returns no rows | Massive has no data for it in that range, or its ticker changed | List a company's past tickers with `m.reference.ticker_events("META").fetch()` |

### Your plan

| You see | Why | What to do |
|---|---|---|
| `... would be delayed (Massive reports ...)` | Your plan's data for that subscription is delayed, and you didn't ask for delayed data | Add the line the message shows, or upgrade the plan (see [real-time or delayed](plans.md#real-time-or-delayed)) |
| `... refused the subscription to ...` | Your plan doesn't stream this data, though it may stream other data of the asset class | Stream bars instead of trades, or read with `history()` or `poll()`. If the message suggests `delayed_subscriptions`, your plan may be delayed. |
| `delayed_subscriptions cannot hold dm.Subscription.CURRENCIES` | Forex and crypto have no delayed feed | Remove `CURRENCIES` from the set |
| `AuthenticationError` on a live table, saying `this can also mean the ... plan includes no WebSocket streams for ...` | Massive's streaming API gives the same answer for a wrong key and for a plan without streams of that asset class | If `fetch()` or `history()` works with the same key, the key is right, so read with `history()` or `poll()` instead |
| `HTTP 403` from `fetch()`, `poll()`, or `history()` | Your plan doesn't include this dataset | Use the dataset the message names, where there is one (see [what your plan allows](plans.md#plans-and-errors)) |
| `Your plan doesn't include this data timeframe` | The range starts before your plan's history begins | Ask for a later start |
| `WAITING_SLOT` in `m.connections()` | Another program or computer is using your key's one connection for this asset class | Stop the other program. The library connects when the connection frees. |
| `rate limited: ...; this read ran inside a Deephaven console command` | A Basic plan's pace held a console read for more than 90 seconds | Pass `wait=False`, and list your Basic subscriptions in `rest_basic_subscriptions` |
| A download is slow | Basic plans allow five requests a minute, and `m.rate_limits()` shows the pace | Use flat files for many symbols, and `wait=False` for long reads |

Each dataset's page in the [dataset reference](../datasets/README.md) shows which plans include
it and which of its streams each plan has.

### History

| You see | Why | What to do |
|---|---|---|
| `the plan does not include these flat files` | You asked for flat files your plan lacks | Pass `source=dm.HistorySource.REST`, or leave `source` at `AUTO` |
| `the flat file for <date> is not published yet` | Massive usually publishes a day's files by about 11:00 New York time the next day, but the time varies | Retry later, or read with `source=dm.HistorySource.REST` |
| A WARNING that a read leaves some columns empty | The source `AUTO` chose doesn't fill those columns | Pass the `source=` the WARNING names (see [where history comes from](history.md#history-sources)) |
| `... which is not a stamp Massive is known to use` | Massive stamped a bar at an unexpected time | Read the bar at Massive's stamp, with no `Date`, and report it (see [bars with unexpected stamps](history.md#odd-stamps)) |
| Prices jump at a split | Live bars and flat files are as traded, while adjusted history is restated | Read [split adjustment](history.md#split-adjustment) |

### Live tables

| You see | Why | What to do |
|---|---|---|
| A live table stays empty | The market is closed, a console command is still running, the table reads Massive's delayed feed, or the stream failed | Check `m.reference.market_status().fetch()`, and the table's `State`, `Recency`, and `Error` in `m.streams()` |
| `MassiveSession #1 was closed; this table no longer updates` | The session was closed | Take `table.snapshot()` to keep the rows, and create a new session for new tables (see [stop everything](live-tables.md#live-tables)) |
| `Can not listen to failed table` | Deephaven can't build a live table on one that has failed | Work on `table.snapshot()` |
| `... inside a Deephaven console command, which holds the update graph's lock` | A long command paused every live table on the server | Pass `wait=False` to long reads (see [long reads in the console](live-tables.md#live-tables)) |
| `connection down since <time>; no data` in `m.streams()` | The connection to Massive dropped | Nothing. The library reconnects, the note clears once data flows, and `Gaps` keeps the span whose rows may be missing. |
| `cycle running since <time>` or `last good cycle at <time>` in `m.streams()` | A poll cycle is running late, often paced by rate limits, or one failed with the error before the note | Check `m.rate_limits()`. The poll retries by itself. |
| `... longer than MassiveConfig.max_outage` | An outage, or failing or silent poll cycles, lasted longer than `max_outage`, so the table stopped | Fix any error the message names, then call the verb again. Raise `max_outage` to wait longer (see [when the connection drops](live-tables.md#live-tables)). |
| `no data since <time> during ... trading hours` in `m.streams()` | The connection is up, but nothing arrived for 5 minutes while the market was open | Nothing, usually. A holiday or quiet symbols explain it, and the table keeps running. |
| `no rows this cycle for ...` in `m.streams()` | Massive has no data for those symbols (`HTTP 404`), or their endpoint doesn't take an argument you passed, such as `adjusted=False` for a crypto pair | Fix or remove the symbols named. The other symbols keep polling. |
| `no acknowledgement from Massive for ...` in `m.streams()` | Massive hasn't confirmed the subscription, and the library is sending it again | If the note stays, those symbols' rows may be missing, so [report it](#report-a-bug) |
| `over capacity: <n>/1000; <k> deferred` in `m.streams()` | The options connection is full | Watch fewer contracts. The extra ones subscribe when others leave (see [option contracts](arguments.md#symbols)). |
| `rows for ... are being dropped` in `m.streams()`, or `... the last 100 rows of channel ... were dropped` | Massive sent rows the library can't read, most likely because Massive changed its format | [Report it](#report-a-bug) with the log. A stream stops after 100 such rows in a row with none delivered. |
| Memory keeps growing | `append_only()` keeps every row | Use `ring(n)`, `last_by()`, or `blink()` with an aggregation |
| A table keeps running after `del` | Python hasn't reclaimed the table yet | Call `m.release(table)` (see [stop one table](live-tables.md#live-tables)) |

To fail a poll on a symbol Massive has no data for, instead of leaving it out, set
`dm.MassiveConfig(unavailable_symbols=dm.OnError.RAISE)`. `dm.OnError.IGNORE` leaves it out
without a WARNING.

<a id="diagnostics"></a>

## Diagnostics tables

Four live tables show what the library is doing, and `m.streams()` is the place to start when
a table looks wrong. This makes all four, on any plan:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
streams = m.streams()
connections = m.connections()
limits = m.rate_limits()
reads = m.symbol_reads()
```

Each table has one row for each thing it tracks:

| Table | One row for each |
|---|---|
| `m.streams()` | Stream, poll, background download, and `history()` or `fetch()` of a [watchlist](watchlists.md) |
| `m.connections()` | WebSocket connection, plus one for each choice of which Massive host to stream from |
| `m.rate_limits()` | REST subscription, plus `other` for the datasets Massive assigns to no subscription |
| `m.symbol_reads()` | Symbol of each `history()` or `fetch()` of a watchlist |

Each call returns a new live table. After `m.close()`, each keeps its final rows.

### Streams

The `State` column of `m.streams()` shows where each table stands:

| State | Meaning |
|---|---|
| `RUNNING` | Running. A `history()` or `fetch()` of a watchlist is `RUNNING` once every symbol has been read or has failed. |
| `LOADING` | A background download is running, or a `history()` or `fetch()` of a watchlist is reading a symbol |
| `DONE` | A background download finished |
| `RELEASED` | You released the table, but a derived table or an open panel still uses it |
| `FAILED` | It stopped, and `Error` says why |
| `CLOSED` | The session was closed |

These columns tell you the rest:

| Column | What it shows |
|---|---|
| `Query` | The call that opened it, such as `m.stocks.trades('AAPL')` |
| `Symbols`, `SymbolCount` | The symbols it reads now, updated as a watchlist changes. For a `poll()`, `SymbolCount` counts the symbols its latest cycle read. |
| `Deferred` | How many option contracts are waiting for room under the [options cap](arguments.md#symbols) |
| `Loading` | For a `history()` or `fetch()` of a watchlist, how many symbols are still to read |
| `Recency` | `REALTIME`, `DELAYED`, or `UNKNOWN` (see [real-time or delayed](plans.md#real-time-or-delayed)) |
| `RowsPublished` | How many rows it has delivered so far |
| `Error` | Why it stopped, or warnings that don't stop it, joined with `;` |
| `Gaps` | Each span over which rows may be missing, such as a recovered outage, kept after the `Error` note clears |
| `SchemaDrift` | Differences between what Massive sent and what Massive's documentation says |

A warning in `Error` doesn't stop the table. Each one appears in
[messages and symptoms](#error-messages), and these are the kinds:

- a watchlist row not spelled as Massive spells it
- option contracts waiting for room
- a subscription Massive hasn't confirmed
- a `dm.ALL` stream's symbols whose rows are being dropped
- a connection that's down or quiet
- a poll's symbols left out of its latest cycle

Each `SchemaDrift` entry starts with `WebSocket:` or `REST:`, then names a field with a mark:

| Entry | Meaning |
|---|---|
| `zz+` | Massive sent a field its documentation doesn't list |
| `size-` | Massive stopped sending a field |
| `exchange?` | A value didn't fit its column, so the cell is null |
| `s~` | A whole number was too large for its decimal column, and is kept rounded |
| `3 rows dropped` | Rows were dropped because their symbol or time couldn't be read. A `poll()` that reads the same bad row every cycle counts it every cycle. |

### Connections

The `State` column of `m.connections()` shows each WebSocket connection's state:

| State | Meaning |
|---|---|
| `RESOLVING` | The library is choosing which Massive host to stream from |
| `CONNECTING` | The connection is opening |
| `CONNECTED` | Connected and streaming |
| `WAITING_SLOT` | Another program is using your key's one connection, and the library connects when it frees |
| `RECONNECTING` | The connection dropped, and the library is reconnecting |
| `FAILED` | It stopped, and `LastError` says why |
| `CLOSED` | Closed, because no stream needs it or the session closed |

Each row also shows its `Host` and `Recency`, how many topics it carries in `Topics`, how many
Massive has confirmed in `AckedTopics`, and how many are waiting in `PendingTopics`.

Massive's individual plans allow one connection for each WebSocket cluster, such as stocks or
crypto, per account. Sessions of one server with the same key and the same connection settings
share their connections. Sessions with different settings, such as another `max_outage`,
compete for the one connection.

### Rate limits

The `State` column of `m.rate_limits()` shows how each subscription's requests are going out:

| State | Meaning |
|---|---|
| `READY` | Requests go out at once |
| `PACED` | The library spaces requests to fit a Basic plan's five a minute |
| `PAUSED` | Massive refused a request for coming too fast, and requests wait until `PausedUntil` |

Programs elsewhere that use your key count against the same limit, but they don't appear here.

### Symbol reads

The `State` column of `m.symbol_reads()` shows where each symbol's read stands:

| State | Meaning |
|---|---|
| `QUEUED` | Waiting to be read |
| `LOADING` | Being read |
| `RETRYING` | A passing failure. It's read again at `NextAttemptAt`. |
| `DONE` | Its rows are in the table |
| `FAILED` | It failed, and `Error` says why. Delete its row and add it back to read it again. |
| `REMOVED` | Its row left the watchlist |
| `STOPPED` | You released the table |
| `CLOSED` | The session was closed |

`ReadAt` is when the rows you see were downloaded, and `Rows` counts them. This lists the
symbols whose reads failed:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
failed = m.symbol_reads().where("State == `FAILED`")
```

<a id="the-log"></a>

## The debug log

The library writes its warnings and errors to Deephaven's log, which the Deephaven IDE shows in
its **Log** panel. The debug log adds INFO and DEBUG lines that say what the library decided
and why. Each level asks something different of you:

| Level | Meaning | What to do |
|---|---|---|
| INFO | A decision, or progress | Nothing |
| WARNING | A live table may be stale or wrong, or something needs you. It's logged once when it starts, and an INFO line says when it recovers. | Read the message |
| ERROR | A defect in your code or input, whose message names the fix, or a bug in the library, whose message says `This is a bug in deephaven-massive.` | Fix your code, or [report the bug](#report-a-bug) |

To see everything the library does while some code runs, turn the debug log on around it:

```python
import deephaven_massive as dm

dm.enable_debug_logging()  # to the Log panel; pass a file path to write to a file instead
m = dm.MassiveSession()
week = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2025-06-02", "2025-06-06")
dm.disable_debug_logging()
```

The log then holds every request the read sent and what came back. Replace the read with the
code that fails.

To capture a problem from the first session on, set `DEEPHAVEN_MASSIVE_LOG` to `DEBUG`, or to
`INFO` for a long run, or set `DEEPHAVEN_MASSIVE_LOG_FILE` to a file path. Set them in the
terminal that starts the server, as the README's [configuration](../../README.md#configuration)
shows. The library reads them when the first session is created. The log never holds your API
key or S3 secret.

### Keep the log small

The debug log grows with the number of requests, not with the amount of market data. A live
table writes about 60 KB an hour for each connection, however busy the market is. Each REST
request or flat-file read writes up to 1 KB, so a fast `poll()` of many symbols grows quickly.
`poll(5)` of 100 requests a cycle writes about 35 MB an hour.

The log has no size limit and is never rotated, and a log file is added to, not replaced, each
time you turn the log on. Turn it on right before the failing code and off after it, use `INFO`
for long runs, and delete the file between runs.

<a id="report-a-bug"></a>

## Report a bug

Open an [issue](https://github.com/chipkent/deephaven-massive/issues) with three things:

1. What you ran, and what you expected.
2. The debug log file. GitHub cuts an issue's text at 65,536 characters, so attach the file,
   and zip a larger one.
3. The diagnostics tables, printed right after the failure, from the session that failed.

This function prints a session's four tables in full. Paste it into the console where the
failure happened, and call it with the session you used:

```python
import deephaven_massive as dm


def print_diagnostics(session):
    for table in (
        session.streams(),
        session.connections(),
        session.rate_limits(),
        session.symbol_reads(),
    ):
        print(table.to_string(num_rows=1000))


m = dm.MassiveSession()
print_diagnostics(m)
```
