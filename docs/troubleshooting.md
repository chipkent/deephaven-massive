# Troubleshooting

Most error messages from the library name their fix. For the rest, and for symptoms with no
message, find what you see in these tables and follow its fix. If nothing matches, check the
[diagnostics tables](#diagnostics), [turn on the debug log](#the-log), and
[report a bug](#report-a-bug).

<a id="error-messages"></a>

## Find your error or symptom

A `...` in a message stands for more words in the text you see.

<a id="setup-messages"></a>

### Setup

| You see | Why | What to do |
|---|---|---|
| `Repository not found`, or a password prompt, while you install | Git isn't using your GitHub sign-in. | Run `gh auth login` and `gh auth setup-git`, then install again. |
| `command not found` for `deephaven` | The terminal hasn't activated the virtual environment. | Run `source ~/dh-massive/bin/activate`, then start the server again. |
| The server stops at startup with an error about Java | Java 17 or later isn't installed, or `JAVA_HOME` isn't set. | Install Java 17 or later and set `JAVA_HOME`, as the prerequisites of Deephaven's [pip install guide](https://deephaven.io/core/docs/getting-started/pip-install/) say. |
| `AuthenticationError: no Massive API key: ...` | The server's terminal doesn't have `MASSIVE_API_KEY`. | Add the key to your shell's startup file, as [Set your API keys](../README.md#set-your-api-keys) shows. Then restart the server, as [Start the server](../README.md#start-the-server) shows. |
| `RuntimeError: The Deephaven Server has not been initialized` | The code imported the library in plain Python, without a Deephaven server. | Run it in the console of the Deephaven IDE. In a Python program, start a server before the import: `from deephaven_server import Server`, then `Server(port=10000, jvm_args=["-Xmx4g"]).start()`, then `import deephaven_massive as dm`. |

<a id="read-messages"></a>

### Reads

These come from `fetch()` and `history()`. By default a read runs in the background: Massive's
error fails its table, and shows in the `Error` column of its row in `m.streams()`. A call with
`wait=True` raises the error instead.

| You see | Why | What to do |
|---|---|---|
| A `fetch()` or `history()` table has no rows | The download is still running, it failed, or Massive had no rows. | Find the read's row in `m.streams()`. `LOADING` means its rows are on their way, `FAILED` has the reason in `Error`, and `DONE` means Massive had no rows, as "No rows for a symbol" explains. A watchlist's read shows `RUNNING` once each symbol has been read, and `m.symbol_reads()` shows each symbol's result. |
| `try`/`except` around `fetch()` or `history()` doesn't catch Massive's error | The call returned before Massive answered, and the error went to the table. | Pass `wait=True` to wait for Massive's answer, so its error raises in the call. |
| No rows for a symbol | Massive has no data for it in that range, or the company changed its ticker. | List the ticker's changes with `m.reference.ticker_events("META").fetch()`. |
| `... Your plan doesn't include this data timeframe ...` | You asked for data more recent than your plan covers. | End the range earlier. The plan table on the dataset's [reference page](datasets/README.md) shows how recent each plan's data is. |
| `... object has no attribute 'history'` | The dataset reads what Massive has now, so its query has `fetch()` and `poll()` instead. | If the dataset method takes `start` and `end`, pass the range there and call `fetch()`, such as `m.stocks.dividends("AAPL", start="2024-01-01", end="2024-12-31").fetch()`. |
| `... object has no attribute 'fetch'` | The dataset streams or reads a past range, so its query has no `fetch()`. | For each symbol's latest row, use the dataset's one-row counterpart, such as `m.stocks.last_trade(...)` for trades, or the asset class's `snapshots(...)`. For bars, call `history(start, end)`, or, outside futures, `previous_day_bar(...)` for the last session's bar. `dm.datasets()` lists each dataset's methods. |
| A read is slow | A Basic plan allows five requests a minute, and the read waits between them. | Read fewer symbols. |
| A `history()` is slow, and its table's description says it read flat files | Each flat file holds a whole day of data, and `history()` downloads all of it, however few symbols or short a range you ask for. The `m.indices.values` file for 2026-11-09 was 2.9 GB. | Read fewer days. For a few symbols, pass `source=dm.HistorySource.REST` if the dataset's page lists REST under "Read from". |
| Prices jump at a stock split | Live bars, flat files, and bars read with `adjusted=False` are as traded. | Read bars with `history()` and leave `adjusted` at `True`, the default, for split-adjusted prices. The [stock bars page](datasets/stocks.bars.md#arguments) explains `adjusted`. |

<a id="live-table-messages"></a>

### Live tables

Each live table has a row in `m.streams()`, whose `Error` column says what's wrong. The
[diagnostics tables](#diagnostics) section shows how to read it.

| You see | Why | What to do |
|---|---|---|
| A stock table stays empty | No stock trades while the market is closed. | Check whether the market is open with `m.reference.market_status().fetch()`. |
| Live tables stop updating while a console command runs | Deephaven updates no live table until the command ends. | Keep console commands short. Leave out `wait=True` from `fetch()` and `history()`: the call returns at once, and its table fills in the background. |
| `AuthenticationError` from a live table that mentions WebSocket streams | Massive gives this answer for a wrong key, and may give it for a plan that doesn't stream this asset class. | Run `m.reference.market_status().fetch()` with the same key. If it works, your key is right, so read this data with `history()` or `poll()`. |
| A symbol on your watchlist never appears | The table skips a row that isn't Massive's exact ticker, or a crypto pair Massive doesn't list. `Error` lists the skipped rows. | Fix or delete the row. [`symbols`](datasets/README.md#symbols) shows Massive's spelling for each asset class, and `help(dm.canonicalize_symbols)` shows how to convert a table of other spellings to it. |
| `... another connection with this API key took the WebSocket connection ...` | A newer connection with your key took this one's place: another program, or a session on this server with different streaming settings. Massive allows your account one connection per asset class. | Stop the other program, give both sessions the same `MassiveConfig`, or set `connection_taken=dm.ConnectionTaken.RECONNECT` in [`MassiveConfig`](configuration.md#settings-reference) to take the connection back. Then run the line that made the table again. Under `RECONNECT`, if the other program also reconnects, the two take turns, and each loses data while the other holds the connection. |
| `... longer than MassiveConfig.max_outage ...` | The table got no data for longer than `max_outage`, which defaults to 300 seconds. | To allow longer outages, raise `max_outage` in [`MassiveConfig`](../README.md#massive-config). |
| `over capacity: <n>/1000; <k> deferred` in `Error` | Massive allows 1,000 option contracts on one connection, and the library counts a contract's trades and its quotes as two. | Watch fewer contracts. The waiting ones subscribe as others leave. |
| `... Can not listen to failed table ...` | You built a table on a live table that failed or whose session was closed. | Make the live table again, or build on a static copy from `table.snapshot()`. |
| `ValueError: ... only 1-second and 1-minute bars at multiplier=1 stream ...` | You called a stream method, such as `last_by()`, on bars Massive doesn't stream. | Pass `dm.Timespan.SECOND` or `dm.Timespan.MINUTE` and leave `multiplier` at 1, or read these bars with `history(start, end)`. |
| Memory keeps growing | An `append_only()` table keeps every row. | Keep the last rows with `ring(n)`, or each symbol's latest row with `last_by()`. |
| A table keeps updating after you delete its variable | A panel or a table made from it still uses it, or Python hasn't garbage-collected it yet. | Close its panels and delete the tables made from it. Before you delete a live table's variable, call `m.release(table)`, as [Stopping live tables](../README.md#stop-live-tables) shows, so it stops without waiting for Python. |

<a id="handle-errors"></a>

## Handle errors in code

In a program, read with `wait=True`, so that Massive's errors raise in the call instead of
failing the table. `dm.MassiveError` covers every error Massive reports and every failure
inside a query method such as `fetch()` or `last_by()`. A wrong argument raises `TypeError`
or `ValueError` at the call. These attributes of a `dm.MassiveError` tell your code what to
do:

| Attribute | What it says |
|---|---|
| `is_retryable` | `True` when the same call may succeed later, such as after a rate limit (HTTP 429), a failure on Massive's side (HTTP 500, 502, 503, or 504), a connection to Massive that kept failing, or a flat file Massive has not published yet. `False` when it can't, such as for a plan that lacks the data, a refused key, a closed session, or a bug. |
| `is_bug` | `True` when a bug in the library or in your code caused the error. A library bug's message says how to report it, and yours names the fix. |
| `source` | What stopped the call, as a `dm.ErrorSource`: `MASSIVE` (Massive refused or failed), `NETWORK` (no answer came), `USER` (your code or settings, and the message names the fix), or `LIBRARY` (a bug in the library). Later versions may add sources, so handle values you don't know. |
| `status` | The HTTP status of the answer, such as Massive's 404 or a proxy's 407, or `None` when there was none. |

Wait before you call again, since the library has already retried, as
`help(dm.MassiveError.is_retryable)` explains. After `m.close()`, a query method such as
`fetch()`, or `m.streams()`, raises `dm.SessionClosedError`. Create a new session to read
again.

A `fetch()` or `history()` of a watchlist can fail for some symbols and not others, such as
when Massive refuses one symbol's request. Those symbols show their errors in
`m.symbol_reads()`, the log reports them, and the read fails only when every symbol that was
on the watchlist at the call failed.

This example reads Apple's dividends, up to three times, a minute apart:

```python
import time

import deephaven_massive as dm

m = dm.MassiveSession()
for attempt in range(3):
    try:
        dividends = m.stocks.dividends("AAPL").fetch(wait=True)
        break
    except dm.MassiveError as e:
        if e.is_bug:
            print(f"Please report or fix this: {e}")
        if not e.is_retryable or attempt == 2:
            raise
        time.sleep(60)
```

Branch on `is_retryable` and `is_bug`, never on the message.

<a id="diagnostics"></a>

## Read the diagnostics tables

Four live tables show what a session is doing. Call each on the session whose tables you're
checking:

| Table | One row for each | Look here when |
|---|---|---|
| `m.streams()` | Live table, and each `fetch()` or `history()` that runs in the background or follows a watchlist | A live table looks wrong |
| `m.connections()` | Streaming connection, and each real-time or delayed feed the session chose | A stream gets no data |
| `m.rate_limits()` | Massive subscription, with its rate-limit state | A read is slow |
| `m.symbol_reads()` | Symbol of a watchlist read with `history()` or `fetch()` | A watchlist symbol has no rows |

Start with `m.streams()`. On any Stocks plan, this example polls Apple's news in a new session,
`demo`, and shows the poll's row:

```python
import deephaven_massive as dm

demo = dm.MassiveSession()
news = demo.reference.news("AAPL", limit=10).poll(300)
streams = demo.streams()
```

`streams` has one row, for the news poll. `Query` shows the call that made it, and `State` is
`RUNNING` while the poll delivers rows. `help(m.streams)` describes every column.

Act on these states:

| Table | `State` | Why | What to do |
|---|---|---|---|
| `m.streams()` | `FAILED` | The table stopped, and `Error` says why. | Fix what `Error` names, then run the line that made the table again. |
| `m.connections()` | `WAITING_SLOT` | Massive refused this connection at login: your account has no free connection for this asset class. | Stop any other program that uses your key. The library keeps trying, and its live tables fail after `max_outage`. |
| `m.symbol_reads()` | `FAILED` | The symbol's read failed, and `Error` says why. | Fix what `Error` names, then delete the symbol's row from the watchlist and add it back. |

<a id="the-log"></a>

## Turn on the debug log

By default, the library's warnings and errors appear in the server's terminal and in the
Deephaven IDE's **Log** panel. The debug log adds INFO and DEBUG lines:

| Level | What it means |
|---|---|
| ERROR | A defect in your code or input, whose message names the fix, or a bug in the library, whose message says `This is a bug in deephaven-massive.` |
| WARNING | A live table may be stale or wrong, or something needs your action. With the debug log on, an INFO line says when it recovers. |
| INFO | A decision the library made, or its progress. |
| DEBUG | The details of a decision, such as each request sent. |

To record what the library does while some code runs, turn the debug log on before the code and
off after it, in the console where the problem happened:

```python
import deephaven_massive as dm

dm.enable_debug_logging()
# Run the code that failed here.
dm.disable_debug_logging()
```

The server's terminal and the **Log** panel then show the settings of each session the code
makes, each request it sends, and what comes back. To write the lines to a file for a bug
report, pass its path, as in `dm.enable_debug_logging("/tmp/massive.log")`.

To log from the first session on, set `DEEPHAVEN_MASSIVE_LOG` or `DEEPHAVEN_MASSIVE_LOG_FILE`,
as [Environment variables](../README.md#environment-variables) describes.

<a id="report-a-bug"></a>

## Report a bug

Open an [issue](https://github.com/chipkent/deephaven-massive/issues) with:

1. What you ran, and what you expected.
2. The debug log, saved to a file as [Turn on the debug log](#the-log) shows. The log never
   holds your API key or S3 secret.
3. The four diagnostics tables of the session that failed, printed right after the failure and before you close the session.

This function prints them. Paste it into the console where the failure happened, then call it
with that session, such as `print_diagnostics(m)`:

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
