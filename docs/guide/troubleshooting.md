# Troubleshooting

Examples on this page assume `import deephaven_massive as dm` and `m = dm.MassiveSession()`.

Find the message or symptom you see, then its fix. When nothing fits, look at the
[diagnostics tables](#diagnostics), then turn on the [debug log](#the-log).

<a id="error-messages"></a>

## Messages and symptoms

### Setup and keys

| Message or symptom | Cause and fix |
|---|---|
| `no Massive API key` | The server cannot see `MASSIVE_API_KEY`. Export it in the shell that starts the server, then restart the server. Don't type the key into the console. See [your API key](install.md#api-key). |
| `MassiveSession must be created inside a Deephaven server` | The code ran in a plain Python process. Run it in the Deephaven console, in a script the server runs, or after starting an [embedded server](install.md#embedded). |
| `flat files need both s3_access_key_id and s3_secret_access_key` | Only one of the two S3 keys is set. Set both, or neither: see [S3 keys](install.md#s3-keys). |
| `S3 rejected the flat-file credentials` | The S3 keys are wrong. With `source=dm.HistorySource.AUTO`, the default, the read falls back to REST and this text arrives as a WARNING; it fails only with `source=dm.HistorySource.FLAT_FILES`, or when REST cannot serve the read. Set the keys from Massive's dashboard and restart the server. |

### The settings file

| Message or symptom | Cause and fix |
|---|---|
| `no settings file: pass its path ...` | `dm.MassiveConfig.from_file()` was called without a path, and `DEEPHAVEN_MASSIVE_CONFIG_FILE` is not set where the server runs. Pass the path, or set the variable before you start the server. See [settings file](install.md#settings-file). |
| `settings file ... does not exist` | The path is wrong, or, in Docker, the file is not mounted at the path the variable names. Check the `-v` and `-e DEEPHAVEN_MASSIVE_CONFIG_FILE` options. |
| `Is a directory` from `from_file()` | The path is a folder: Docker created it because the file did not exist on your computer. Create the file first. |
| `settings file ..., line ..., column ...: ...` | The file holds something the library refuses: an unknown setting (the message suggests the closest), a word in the wrong case, a key, or a YAML feature such as a tag or an anchor. Fix that line. |

### Calls

| Message or symptom | Cause and fix |
|---|---|
| `timespan must be dm.Timespan` | Arguments that pick from a set take an enum member: `dm.Timespan.DAY`, not `"day"`. |
| `ValueError` naming a symbol | The symbol is not spelled as Massive spells it. See [spelling and converters](arguments.md#symbols). |
| `AttributeError` naming a verb | The dataset cannot serve that verb; the message names the dataset that can. |
| `... poll() needs a start or a limit` | A news or filings poll would read the whole archive every cycle. Give the query a `start` or a `limit`. |
| a symbol returns no rows | No data in that range, or the ticker changed: `m.reference.ticker_events("META").fetch()` lists a company's past tickers. |

### Your plan

| Message or symptom | Cause and fix |
|---|---|
| `... would be delayed (Massive reports ...)` | Your plan's data for that subscription is delayed, and the library serves real-time data unless you ask for delayed. Add the line the message shows, or upgrade the plan. See [real-time or delayed](plans.md#real-time-or-delayed). |
| `... refused the subscription to ...` | Your plan does not stream this data: it may include other streams of the asset class, or REST only. Stream bars instead of trades, or read with `history()` or `poll()`; each dataset's [reference page](../datasets/README.md) has the plan table. When the message suggests `delayed_subscriptions`, your plan may be delayed: see [real-time or delayed](plans.md#real-time-or-delayed). |
| `delayed_subscriptions cannot hold dm.Subscription.CURRENCIES` | Forex and crypto have no delayed feed. Remove `CURRENCIES` from the set. |
| `AuthenticationError` on a live table, ending `can also mean the ... plan includes no WebSocket streams` | Massive's live connection gives the same answer for a wrong key and, possibly, for a plan without streams of that asset class. If `fetch()` or `history()` works with the same key, the key is right: read the data with `history()` or `poll()`, or check which streams your plan includes on the dataset's [reference page](../datasets/README.md). |
| `HTTP 403` from `fetch()`, `poll()`, or `history()` | Your plan does not include this dataset; the message names another where one exists. See [what your plan allows](plans.md#plans-and-errors). |
| `Your plan doesn't include this data timeframe` | The range starts before your plan's history begins. Ask for a later start. |
| `WAITING_SLOT` in `m.connections()` | Another program or computer is using your key's one connection for this asset class. Stop it; the library connects when the connection frees. |
| `rate limited: ...; this read ran inside a Deephaven console command` | A Basic subscription's pace held a console read for more than 90 seconds. Pass `wait=False`, which waits up to ten minutes, and list your Basic subscriptions in `rest_basic_subscriptions` so the library paces from the start. |
| a download is slow | Basic plans allow five requests a minute; `m.rate_limits()` shows the pace. Use flat files for many symbols, and `wait=False` for long reads. |

### History

| Message or symptom | Cause and fix |
|---|---|
| `the plan does not include these flat files` | Pass `source=dm.HistorySource.REST`, or leave `source` at `AUTO`, which reads REST when REST can serve the read. |
| `the flat file for <date> is not published yet` | Massive usually publishes by about 11:00 New York time the next day, but the time varies. Retry later, or read with `source=dm.HistorySource.REST`. |
| a WARNING that a read leaves some columns empty | Pass the `source=` it names. See [where history comes from](history.md#history-sources). |
| `... which is not a stamp Massive is known to use` | Massive stamped a bar at an unexpected time. You get it at Massive's stamp with no `Date`. See [bars with unexpected stamps](history.md#odd-stamps), and please report it. |
| prices jump at a split | Live bars are as traded; adjusted history is restated. See [split adjustment](history.md#split-adjustment). |

### Live tables

| Message or symptom | Cause and fix |
|---|---|
| a live table stays empty | The market is closed (`m.reference.market_status().fetch()`); a console command is still running; the table reads Massive's delayed feed (`Recency` `DELAYED` in `m.streams()`), which starts 15 minutes behind; or the stream failed (`m.streams()`). |
| `MassiveSession #1 was closed; this table no longer updates` | The session was closed. Take `table.snapshot()` to keep the rows, and create a new session for new tables. See [stop everything](concepts.md#live-tables). |
| `Can not listen to failed table` | Deephaven cannot build a live table on one that has failed. Work on `table.snapshot()`. |
| `... inside a Deephaven console command, which holds the update graph's lock` | A long command paused every live table on the server. Pass `wait=False` to long reads: see [long reads in the console](concepts.md#live-tables). |
| `connection down since <time>; no data`, `cycle running since <time>`, or `last good cycle at <time>` in `m.streams()` | The connection dropped, a poll cycle is running late (often paced by rate limits: see `m.rate_limits()`), or a poll cycle failed with the error before the note. The library reconnects and retries by itself. The note clears once data flows, and `Gaps` keeps the span whose rows may be missing. |
| `... longer than MassiveConfig.max_outage`, from a stream or from `... poll(): ...` | The outage, or the failing or silent poll cycles, lasted longer than `max_outage`, so the table stopped. Fix any error it names, then call the verb again; raise `max_outage` to wait longer. See [when the connection drops](concepts.md#live-tables). |
| `no data since <time> during ... trading hours` in `m.streams()` | The connection is up, but nothing arrived for 5 minutes while the market is open. A holiday or quiet symbols explain it; the table keeps running. |
| `no rows this cycle for ...` in `m.streams()` | A poll left those symbols out: Massive has no data for them (`HTTP 404`), or their endpoint does not take an argument you passed (such as `adjusted=False` for a crypto pair). The other symbols keep polling. Fix or remove the symbols named. `dm.MassiveConfig(unavailable_symbols=dm.OnError.RAISE)` stops the table instead, and `IGNORE` leaves them out without a WARNING. |
| `no acknowledgement from Massive for ...` in `m.streams()` | Massive has not confirmed the subscription; the library sends it again until Massive does. If the note stays, those symbols' rows may be missing: report it. |
| `over capacity: <n>/1000; <k> deferred` in `m.streams()` | The options connection is full; the extra contracts subscribe when others leave. Watch fewer contracts. See [the options cap](arguments.md#symbols). |
| `rows for ... are being dropped` in `m.streams()`, or `... the last 100 rows of channel ... were dropped` | Massive sent rows the library cannot read. A `dm.ALL` stream warns and keeps running; any other stream stops after 100 such rows in a row. Massive has most likely changed its format: report it with the log. |
| memory keeps growing | `append_only()` keeps every row. Use `ring(n)`, `last_by()`, or `blink()` with an aggregation. |
| a table keeps running after `del` | Call `m.release(table)`. See [stop one table](concepts.md#live-tables). |

<a id="diagnostics"></a>

## Diagnostics tables

Four live tables show what the library is doing. Start with `m.streams()` when a table looks
wrong.

```python
streams = m.streams()
connections = m.connections()
limits = m.rate_limits()
reads = m.symbol_reads()
```

| Table | One row per |
|---|---|
| `m.streams()` | Stream, poll, background download, and `history()` or `fetch()` of a [watchlist](watchlists.md) |
| `m.connections()` | WebSocket connection, plus one row per choice of which Massive host to stream from |
| `m.rate_limits()` | REST subscription, plus `other` for reference data |
| `m.symbol_reads()` | Symbol of each `history()` or `fetch()` of a watchlist |

Each call returns a new live view. After `m.close()`, each keeps its final rows.

### `m.streams()`

`State`:

| State | Meaning |
|---|---|
| `RUNNING` | Running. A `history()` or `fetch()` of a watchlist is `RUNNING` once every symbol has been read or has failed. |
| `LOADING` | A background download is running, or a `history()` or `fetch()` of a watchlist is reading a symbol |
| `DONE` | A background download finished |
| `RELEASED` | You released the table, but a derived table or an open panel still uses it |
| `FAILED` | It stopped; `Error` says why |
| `CLOSED` | The session was closed |

Other columns:

| Column | What it shows |
|---|---|
| `Query` | The call that opened it, such as `m.stocks.trades('AAPL')` |
| `Symbols`, `SymbolCount` | The symbols it reads now. For a watchlist, updated as you add and delete rows; for a `poll()`, `SymbolCount` counts the symbols its latest cycle read. |
| `Deferred` | Option contracts waiting for room under the [options cap](arguments.md#symbols) |
| `Loading` | For a `history()` or `fetch()` of a watchlist: the symbols still to read |
| `Recency` | `REALTIME`, `DELAYED`, or `UNKNOWN` (not yet shown to be either): see [real-time or delayed](plans.md#real-time-or-delayed) |
| `RowsPublished` | Rows delivered so far |
| `Error` | Why it stopped, or warnings that do not stop it, joined with `;` |
| `Gaps` | Each span over which rows may be missing, such as a recovered outage or a run of failed poll cycles, kept after the `Error` note clears |
| `SchemaDrift` | Differences between what Massive sent and what its documentation says; below |

Warnings in `Error` that do not stop the table:

- a watchlist row not spelled as Massive spells it;
- option contracts waiting for room: `over capacity: <n>/1000; <k> deferred`;
- a subscription Massive has not confirmed: `no acknowledgement from Massive for ...` (the
  library sends it again);
- a `dm.ALL` stream's symbols whose rows are being dropped;
- a connection that is down or quiet;
- a poll's symbols left out of its latest cycle: `no rows this cycle for ...`.

The messages are explained in [messages and symptoms](#error-messages).

`SchemaDrift` entries, each prefixed with `WebSocket:` or `REST:`:

| Entry | Meaning |
|---|---|
| `zz+` | Massive sent a field its documentation does not list |
| `size-` | Massive stopped sending a field |
| `exchange?` | A value did not fit its column, so the cell is null |
| `s~` | A whole number too large for its decimal column, kept rounded |
| `3 rows dropped` | Rows dropped because their symbol or time could not be read. A `poll()` that reads the same bad row every cycle counts it every cycle. |

After 100 dropped streamed rows in a row, with none delivered, the stream stops: Massive has
most likely changed its format.

### `m.connections()`

`State`:

| State | Meaning |
|---|---|
| `RESOLVING` | The library is choosing which Massive host to stream from |
| `CONNECTING` | Opening the connection |
| `CONNECTED` | Connected and streaming |
| `WAITING_SLOT` | Another program is using your key's one connection; the library connects when it frees |
| `RECONNECTING` | The connection dropped; the library is reconnecting |
| `FAILED` | It stopped; `LastError` says why |
| `CLOSED` | Closed, because no stream needs it or the session closed |

Each row also shows its `Host`, its `Recency`, the subscriptions Massive has confirmed and those
pending, and `LastError`.

Massive's individual plans allow one connection per WebSocket cluster, such as stocks or
crypto, per account. Sessions of one server with the same key and the same connection settings
share their connections; sessions with different settings, such as another `max_outage`,
compete for the one connection.

### `m.rate_limits()`

| State | Meaning |
|---|---|
| `READY` | Requests go out at once |
| `PACED` | The library spaces requests to fit a Basic plan's five a minute |
| `PAUSED` | Massive refused a request for coming too fast; requests wait until `PausedUntil` |

Programs elsewhere that use your key count against the same limit without appearing here.

### `m.symbol_reads()`

| State | Meaning |
|---|---|
| `QUEUED` | Waiting to be read |
| `LOADING` | Being read |
| `RETRYING` | A passing failure; read again at `NextAttemptAt` |
| `DONE` | Its rows are in the table |
| `FAILED` | It failed; `Error` says why. Delete its row and add it back to read it again. |
| `REMOVED` | Its row left the watchlist |
| `STOPPED` | You released the table |
| `CLOSED` | The session was closed |

`ReadAt` is when the rows you see were downloaded, and `Rows` counts them. To list the symbols
that failed:

```python
failed = m.symbol_reads().where("State == `FAILED`")
```

<a id="the-log"></a>

## The debug log

The library writes to Deephaven's log, which the Deephaven IDE shows in its **Log** panel. By
default only WARNING and ERROR lines appear there; INFO and DEBUG lines appear once you turn on
the debug log.

| Level | Meaning | What to do |
|---|---|---|
| INFO | A decision or progress | Nothing |
| WARNING | A live table may be stale or wrong, or something needs you. Logged once when it starts; an INFO line says when it recovers. | Read the message |
| ERROR | A defect in your code or input (the message names the fix), or in the library (the message ends "This is a bug in deephaven-massive") | Fix your code, or [report the bug](#report-a-bug) |

To see everything the library does while code runs:

```python
dm.enable_debug_logging()  # to the Log panel; pass a file path to write a file instead
# ... run the code that fails ...
dm.disable_debug_logging()
```

To capture a problem from the moment the server starts, set one of these in the shell that
starts the server:

| Variable | Effect |
|---|---|
| `DEEPHAVEN_MASSIVE_LOG=DEBUG` | The debug log, from startup |
| `DEEPHAVEN_MASSIVE_LOG=INFO` | Decisions and progress without a line per request: use it for long runs |
| `DEEPHAVEN_MASSIVE_LOG_FILE=/path/to/massive.log` | The debug log, written to that file |

The log never holds your API key or S3 secret.

### Keep the log small

The debug log grows with the number of requests, not with the amount of market data:

- A live table writes about 60 KB an hour per connection, however busy the market.
- Each REST request or flat-file read writes up to 1 KB, so a fast `poll()` of many symbols grows
  quickly: `poll(5)` of 100 requests a cycle writes about 35 MB an hour.

The log has no size limit and is never rotated, and a log file is added to, not replaced, each
time you turn the log on. Turn it on right before the failing code and off after it, use
`DEEPHAVEN_MASSIVE_LOG=INFO` for long runs, and delete the file between runs.

<a id="report-a-bug"></a>

## Report a bug

Open an [issue](https://github.com/chipkent/deephaven-massive/issues) with:

1. what you ran, and what you expected;
2. the debug log file (GitHub cuts an issue's text at 65,536 characters, so attach the file, and
   zip a larger one);
3. the diagnostics tables, printed after the failure:

```python
for table in (m.streams(), m.connections(), m.rate_limits(), m.symbol_reads()):
    print(table.to_string(num_rows=1000))
```
