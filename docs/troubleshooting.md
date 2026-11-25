# Troubleshooting

When a table looks wrong, start with [`m.problems()`](#problems). Most error messages from the
library name their fix. For the rest, and for symptoms with no message, find what you see in
[Find your error or symptom](#error-messages) and follow its fix. If nothing matches, check
the [diagnostics tables](#diagnostics), [turn on the debug log](#the-log), and
[report a bug](#report-a-bug).

<a id="problems"></a>

## Read the problems table

`m.problems()` is a live table with one row for each problem of the session and its tables.
Some problems happen once, such as a row left out. Others last, such as an outage. A lasting
problem has a `SpanStart`, and its `EndedAt` stays null until it clears. Valid data, such as a
null Massive sent or a symbol with no rows in the range, has no row.

A problem that happens again adds to its row: `Rows` and `Occurrences` grow.

Call `problems()` on the session whose tables you're checking, here `m`:

```python
import deephaven_massive as dm

m = dm.MassiveSession()  # or the session you already have
problems = m.problems()
lasting = problems.where("!isNull(SpanStart) && isNull(EndedAt)")
```

`lasting` holds the problems that haven't cleared.

To see which tables have problems, count them beside each table's row of `m.streams()`:

```python continues
per_table = m.streams().natural_join(problems.count_by("Problems", "StreamId"), "StreamId")
```

Each row says what happened in these columns:

| Column | What it holds |
|---|---|
| `StreamId` | The table's row in `m.streams()`, or the id of a `wait=True` call. Null for a rate-limit pause. |
| `Query` | The call that made the table. |
| `Dataset` | The table's dataset. |
| `Symbol` | The symbol the problem concerns, when it concerns one. |
| `Field` | The column the problem concerns, when it concerns one. For a flat-file line, the file. For a rate-limit pause, its `Bucket` in `m.rate_limits()`. For a refused exchange, its name. |
| `Cause` | Why. [Each cause](#problem-causes) has a row of its own. |
| `Action` | What the library did. |
| `Source` | Whose problem it is: `massive`, `user` for your input, or `library`. |
| `Policy` | The [`on_error`](configuration.md#on-error) setting that applied: `log`, `ignore`, or `raise`. It comes from the call or from `MassiveConfig`. For a value `dm.canonicalize_symbols` refused, it's that function's `on_error`. Null for a cause with no setting. |
| `FirstSeen`, `LastSeen` | When the problem first and last happened. |
| `SpanStart`, `EndedAt` | When a lasting problem began and cleared. |
| `Rows`, `Occurrences` | How many rows it covers, and how many times it happened. |
| `Detail` | What happened, with the value, Massive's answer, or the reason. |

`Action` is one of:

| `Action` | What the library did |
|---|---|
| `WITHHELD` | It left out rows of Massive's data. |
| `SKIPPED` | It left out a symbol, an exchange, or a file line, and read the rest. |
| `GAP` | Rows may be missing. |
| `DELAYED` | Rows come late. |
| `FAILED` | A read or a table failed. |
| `PUBLISHED` | It published the rows but left out a field Massive's documentation doesn't list. |
| `OVERFLOW` | There were too many different problems to list each one. This row counts the rest. |

<a id="problem-causes"></a>

Each cause, and what to do about it:

<!-- The causes below are generated from problem_events.CAUSES by scripts/render_reference.py. Edit the causes, not this part of the page. -->

| `Cause` | `Source` | `Action` | Lasts | What happened | What to do |
|---|---|---|---|---|---|
| `noncanonical_symbols` | `user` | `SKIPPED` | yes | A value of a watchlist or other table you pass isn't Massive's exact ticker, or `dm.canonicalize_symbols` couldn't convert it, so its row is skipped. A `fetch()`, `history()`, or stream of a static table lists it once. | Fix or delete the row. Massive's lists, such as `m.reference.tickers()`, show its spelling. |
| `unreadable_symbols` | `massive` | `WITHHELD` | no | Massive sent a row whose symbol the library can't read as Massive's ticker. The row is left out. | The library withheld the row because Massive sent it in a form the library can't read. `Detail` gives what Massive sent and why, for a report to Massive. Set `unreadable_symbols` to `dm.OnError.IGNORE` to quiet it, or to `dm.OnError.RAISE` to fail the table. |
| `unavailable_symbols` | `massive` | `SKIPPED` | no | Massive has no data for the symbol. It answered HTTP 404, or the pair isn't on its crypto ticker list. | Look the symbol up in Massive's lists, such as `m.reference.tickers()`. |
| `unrequested_symbols` | `massive` | `WITHHELD` | no | Massive answered a watchlist read's request with a row of a symbol the read didn't ask for. The row is left out. | Nothing, if the row is Massive's answer about another instrument. To read that symbol, add it to the watchlist. Set `unrequested_symbols` to `dm.OnError.IGNORE` to quiet it, or to `dm.OnError.RAISE` to fail the table. |
| `unreadable_keys` | `massive` | `WITHHELD` | no | Massive sent a row whose symbol, time, or other key the library can't read. The row is left out. | The library withheld the row because Massive sent it in a form the library can't read. `Detail` gives what Massive sent and why, for a report to Massive. Set `unreadable_keys` to `dm.OnError.IGNORE` to quiet it, or to `dm.OnError.RAISE` to fail the table. |
| `unconvertible_values` | `massive` | `WITHHELD` | no | Massive sent a row with a value the library can't read as its column's type. The row is left out. | The library withheld the row because Massive sent it in a form the library can't read. `Detail` gives what Massive sent and why, for a report to Massive. Set `unconvertible_values` to `dm.OnError.IGNORE` to quiet it, or to `dm.OnError.RAISE` to fail the table. |
| `malformed_rows` | `massive` | `SKIPPED` | no | A line of a flat file isn't a row the library can read. The line is left out. | The library skipped the line because Massive sent it in a form the library can't read. `Detail` names the file and its first such line, for a report to Massive. Set `malformed_rows` to `dm.OnError.IGNORE` to quiet it, or to `dm.OnError.RAISE` to fail the read. |
| `refused_venues` | `massive` | `SKIPPED` | no | Massive refused the flat files of a futures exchange the call named (HTTP 403). | Name in `venues` only the exchanges your plan includes. |
| `unlisted_contracts` | `massive` | `WITHHELD` | no | Massive streamed rows of a futures contract missing from its snapshot and contracts list for 30 minutes. The library can't date them, so it leaves them out. | Look the contract up with `m.futures.contracts()`. If Massive lists it, report a [bug](#report-a-bug) with the row's `Detail`. |
| `unscheduled_products` | `massive` | `WITHHELD` | no | Massive's futures schedules give no trading session for rows it streamed, so the library can't date them. The rows are left out. | Report it as a [bug](#report-a-bug) with the row's `Detail`, so it can go to Massive. |
| `undated_rows` | `library` | `DELAYED` | yes | A futures contract's rows have waited over 15 seconds for its trading session from Massive's schedules. They publish, dated, once it loads. | Nothing, unless it lasts. The table fails once the wait passes [`max_outage`](configuration.md#settings-reference). |
| `stale_symbol_data` | `library` | `GAP` | yes | The last 100 rows of a symbol were all left out, so its latest values may be stale. It ends when a row of the symbol is published. | The symbol's `WITHHELD` rows in this table say why its rows were left out. Do what their cause says. |
| `schema_drift` | `massive` | `PUBLISHED` | no | Massive sent a field its documentation lacks. The rows are published without it. | Report it as a [bug](#report-a-bug), so the library can add the field. The rows are fine. |
| `outage` | `library` | `GAP` | yes | The connection was down, so rows may be missing. | Nothing. The library reconnects, and the table fails only after [`max_outage`](configuration.md#settings-reference). If `Detail` says another program took the connection, see [Live tables](#live-table-messages). |
| `quiet_feed` | `massive` | `DELAYED` | yes | The connection received nothing for a while during trading hours. | Nothing, unless it lasts. `m.connections()` shows the connection. |
| `options_cap` | `massive` | `DELAYED` | yes | The table has more contracts than Massive's cap of 1,000 per connection, counting a contract's trades and quotes as two. The contracts past the cap wait, and `Rows` counts them. | Pass fewer contracts. The waiting ones subscribe as others leave. |
| `unacknowledged_topics` | `massive` | `DELAYED` | yes | Massive has not confirmed some of the table's subscriptions, so the library keeps sending them. | Nothing, unless it lasts. Then report a [bug](#report-a-bug) with the row's `Detail`. |
| `rate_limited` | `massive` | `DELAYED` | yes | Massive refused a request for coming too fast (HTTP 429), so the requests of that Massive subscription wait until the pause ends. | Nothing: the requests go on after the pause. `m.rate_limits()` shows it. |
| `poll_cycle_failed` | `library` | `GAP` | yes | `poll()` cycles failed, so rows may be missing. It ends with the next good cycle. | Fix what `Detail` names. The poll keeps trying, and the table fails once its cycles have failed for [`max_outage`](configuration.md#settings-reference). |
| `poll_cycle_late` | `library` | `DELAYED` | yes | `poll()` cycles take longer than the interval, so the table updates late. | Poll fewer symbols, or pass `poll()` a longer interval. |
| `symbol_read_retrying` | `library` | `DELAYED` | yes | A symbol's read failed in a way that may pass, so the library tries it again. A read that waits on Massive's crypto ticker list is one of these. | Nothing, unless it lasts. Then fix what `Detail` names. |
| `symbol_read_failed` | `library` | `FAILED` | yes | A watchlist symbol's read failed and won't be tried again. | Fix what `Detail` names. Then delete the symbol's watchlist row and add it back to read it again. |
| `stream_failed` | `library` | `FAILED` | no | The table failed. `Detail` and its `Error` in `m.streams()` say why. | Fix what `Detail` names. Then run the line that made the table again. |

<!-- End of the generated causes. -->

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
| `AuthenticationError: No Massive API key is set. ...` | The server's terminal doesn't have `MASSIVE_API_KEY`. | Add the key to your shell's startup file, as [Set your API keys](../README.md#set-your-api-keys) shows. Then restart the server, as [Start the server](../README.md#start-the-server) shows. |
| `RuntimeError: The Deephaven Server has not been initialized` | The code imported the library in plain Python, without a Deephaven server. | Run it in the console of the Deephaven IDE. In a Python program, start a server before the import: `from deephaven_server import Server`, then `Server(port=10000, jvm_args=["-Xmx4g"]).start()`, then `import deephaven_massive as dm`. |

<a id="read-messages"></a>

### Reads

A read is a call to `fetch()` or `history()`. By default it runs in the background. Massive's
error then fails its table and shows in `Error` in `m.streams()`. With `wait=True`, the call
raises it instead.

| You see | Why | What to do |
|---|---|---|
| A `fetch()` or `history()` table without `wait=True` has no rows | The read runs in the background, and its `State` in `m.streams()` says how it went. | Find that `State` in [the states table](#diagnostics) and follow its fix. |
| `try`/`except` around `fetch()` or `history()` doesn't catch Massive's error | The call returned before Massive answered, and the error went to the table. | Pass `wait=True` to wait for Massive's answer, so its error raises in the call. |
| A symbol that used to trade has no rows | The company changed its ticker. | List its changes with `m.reference.ticker_events("META").fetch()`. |
| A symbol gets no rows, and the log names `unavailable_symbols` | Massive doesn't serve that ticker. | Look it up in Massive's lists: `m.reference.tickers()`, `m.options.contracts()`, or `m.futures.contracts()`. |
| `... Your plan doesn't include this data timeframe ...` | You asked for data more recent than your plan covers. | End the range earlier. The Plans table on the dataset's [reference page](datasets/README.md) shows how recent each plan's data is. |
| `EntitlementError: futures.trades: Massive refused the flat files of NYMEX. ...` | You named an exchange in `venues` whose flat files Massive refuses for your key. | Leave the exchange the message names out of `venues`, such as `dm.FuturesVenue.NYMEX`. To skip every refused exchange instead of failing, pass `on_error=dm.ErrorPolicy(refused_venues=dm.OnError.LOG)` ([`on_error.refused_venues`](configuration.md#setting-details)). |
| `EntitlementError: ...: Massive refused the flat files of every venue the read asked for, ...` | Massive refused the flat files of every exchange the read asked for, usually because your plan includes none of them. | To read from REST instead, pass `source=dm.HistorySource.REST` and contract tickers such as `ESH5` in place of `dm.ALL`. The Plans table on each futures page of the [dataset reference](datasets/README.md) shows what your plan includes. |
| `... object has no attribute 'history'` | The dataset reads what Massive has now, so its query has `fetch()` and `poll()` instead. | If the dataset method takes `start` and `end`, pass the range there and call `fetch()`, such as `m.stocks.dividends("AAPL", start="2024-01-01", end="2024-12-31").fetch()`. |
| `... object has no attribute 'fetch'` | The dataset streams or reads a past range, so its query has no `fetch()`. | Call `history(start, end)` for a range. For each symbol's latest row, call the dataset's one-row counterpart, such as `m.stocks.last_trade(...)`. `dm.datasets()` lists each dataset's methods. |
| Rows are missing, and the log names `unreadable_keys`, `unreadable_symbols`, or `unconvertible_values` | Massive sent a row whose value, symbol, time, or other key the library can't read. The library leaves the row out of the table and lists it in [`m.problems()`](#problems) with its cause. | Nothing in your code: the row is Massive's data, and the row's `Detail` in `m.problems()` gives what Massive sent and why, for a report to Massive. To fail the read instead, pass the dataset method `on_error=dm.ErrorPolicy(unreadable_keys=dm.OnError.RAISE, unreadable_symbols=dm.OnError.RAISE, unconvertible_values=dm.OnError.RAISE)`, as [Bad symbols and rows](configuration.md#on-error) shows. |
| A read is slow | Massive limits the subscription's requests per minute, and the read waits between them. | Read fewer symbols. If you know your subscription's limit, set it in [`rest_requests_per_minute`](configuration.md#setting-details), so reads keep to it from the start instead of pausing after Massive refuses one. `m.rate_limits()` shows each limit. |
| A `history()` is slow, and its table's description says it read flat files | Each flat file holds a whole day of data, and `history()` downloads all of it, however few symbols or short a range you ask for. The `m.indices.values` file for 2026-11-09 was 2.9 GB. | Read fewer days. For a few symbols, pass `source=dm.HistorySource.REST` if the dataset's page lists REST under "Read from". |
| Prices jump at a stock split | Live bars, flat files, and bars read with `adjusted=False` are as traded. | Read bars with `history()` and leave `adjusted` at `True`, the default, for split-adjusted prices. The [stock bars page](datasets/stocks.bars.md#arguments) explains `adjusted`. |

<a id="live-table-messages"></a>

### Live tables

[`m.problems()`](#problems) lists what's wrong with a live table while it runs. If the table
failed, `Error` in its `m.streams()` row says why.

| You see | Why | What to do |
|---|---|---|
| A stock table stays empty | No stock trades while the market is closed. | Check whether the market is open with `m.reference.market_status().fetch()`. |
| Live tables stop updating while a console command runs | Deephaven updates no live table until the command ends. | Keep console commands short. Leave out `wait=True` from `fetch()` and `history()`: the call returns at once, and its table fills in the background. |
| `AuthenticationError` from a live table that mentions WebSocket streams | Massive gives this answer for a wrong key, and may give it for a plan that doesn't stream this asset class. | Run `m.reference.market_status().fetch()` with the same key. If it works, your key is right. Read this data with `history()`, or poll its REST counterpart, such as `m.stocks.last_trade(...)`. |
| A watchlist row never appears, and `m.problems()` names its value | The value isn't Massive's exact ticker, so the table skips the row. | Fix or delete the row. [`symbols`](datasets/README.md#symbols) shows Massive's spelling for each asset class. |
| A row of a table you converted never appears | `dm.canonicalize_symbols` couldn't convert it, so its `Symbol` is null, and every dataset skips the row. | Fix the rows in the table you started from. `.where("isNull(Symbol)")` on the converted table lists them. [Symbols from another source](../README.md#convert-symbols) shows the conversion. |
| `Another connection with this API key took the WebSocket connection ...` | Massive allows your account one connection per asset class, and a newer connection with your key took this one's place. It came from another program, or from a session on this server with different streaming settings. | Stop the other program, or give both sessions the same `MassiveConfig`. Then run the line that made the table again. To take the connection back automatically, see [`connection_taken`](configuration.md#setting-details). |
| `... longer than max_outage= ...` or `... reaching max_outage= ...` | The table had no data, or its polls kept failing, for `max_outage` seconds. The default is 300. The message says what the table waited for. | To allow longer outages, raise [`max_outage`](configuration.md#setting-details). Then run the line that made the table again. |
| `... as long as max_outage= ... allows ...` in `Error` of `m.symbol_reads()` | A watchlist symbol's reads kept failing with a temporary error, such as a rate limit. They failed for `max_outage` seconds. | Delete the symbol's row from the watchlist and add it back to read it again. To keep trying longer, raise [`max_outage`](configuration.md#setting-details). |
| `... MB the library keeps for one connection ...` | Deephaven fell behind the stream, or stopped while a console command ran. The waiting rows reached the library's 1,024 MB limit for one connection, so every live table on that connection stopped. | Keep console commands short, and stream fewer symbols. Then run the line that made the table again. |
| `... with HTTP 429 ... The next request goes at ... UTC, past ...` in a table's `Error` or in a `Detail` of `m.problems()` | Massive paused your requests for longer than the read waits, which for a live table is `max_outage`. The usual cause is another program using the same Massive account. | Stop the other program. If the table failed, run the line that made it again. A `poll()` table that is still `RUNNING` tries again at its next poll. To keep your requests under the limit, set [`rest_requests_per_minute`](configuration.md#setting-details). |
| The server exits with `AssertionFailure: Assertion failed: asserted isFailed == false` from `MergedListener.propagateErrorDownstream` | Your code filtered a live table with `where_in` by a table made from it. When the live table failed, for example because its session closed, a bug in Deephaven 42.9 and earlier failed the filtered table twice. | For a `fetch()` or `history()` table, read with `wait=True`, so the table is static. Otherwise replace `table.where_in(keys, "Key")` with `table.join(keys.select_distinct("Key"), "Key")`, which keeps the same rows and columns. |
| `... Can not listen to failed table ...` | You built a table on a live table that failed or whose session was closed. | Make the live table again, or build on a static copy from `table.snapshot()`. |
| `'HistoryQuery' object has no attribute ...`, such as `'last_by'` | Massive streams only 1-second and 1-minute bars at `multiplier=1`. For any other bars, `bars()` returns a `HistoryQuery`, which has no stream methods. | Pass `dm.Timespan.SECOND` or `dm.Timespan.MINUTE` with `multiplier` at 1. To read any other bars, call `history(start, end)`. |
| mypy or pyright says `HistoryQuery` has no attribute `last_by` on 1-second or 1-minute bars | A `bars()` result can be a `StreamHistoryQuery` or a `HistoryQuery`, so a type checker allows its stream methods only after your code checks the class. | Keep the query in a variable, such as `minute`, and check it with `assert isinstance(minute, StreamHistoryQuery)` before the stream method. `StreamHistoryQuery` imports from `deephaven_massive.queries`. |
| Memory keeps growing | An `append_only()` table keeps every row. | Keep the last rows with `ring(n)`, or each symbol's latest row with `last_by()`. |
| A table keeps updating after you delete its variable | A panel or a table made from it still uses it, or Python hasn't garbage-collected it yet. | Close its panels and delete the tables made from it. Before you delete a live table's variable, call `m.release(table)`, as [Stopping live tables](../README.md#stop-live-tables) shows, so it stops without waiting for Python. |

<a id="underlyings-in-another-spelling"></a>

### Tickers from another source

You see `The symbol table has no Symbol column. ...`, or `A table of underlyings holds ...,
which is not a stock or index ticker ...`. A table of tickers must hold Massive's exact
tickers in a column named `Symbol`. If your column has another name, rename it:
`t.rename_columns("Symbol = Ticker")`. Convert tickers from another source, such as
Yahoo's `BRK-B`, with `dm.canonicalize_symbols`. Pass the source as `convention=`, one of
`dm.SymbolConvention.YAHOO`, `NASDAQ`, and `CMS`. [Symbols from another
source](../README.md#convert-symbols) shows the conversion.

A table you pass as `underlying=` to `m.options.chain` or `m.options.contracts` may mix stocks
and indices. Converting it with `dm.AssetClass.STOCKS` makes an index ticker such as `I:SPX`
null, so convert only the stock rows and keep the index rows as they are. Here `t` stands for
your table:

```python
from deephaven import merge, new_table
import deephaven_massive as dm

t = new_table({"Symbol": ["BRK-B", "AAPL", "I:SPX"]})
stocks = dm.canonicalize_symbols(
    t.where("isNull(Symbol) || !Symbol.startsWith(`I:`)"),
    dm.AssetClass.STOCKS,
    convention=dm.SymbolConvention.YAHOO,
)
underlying = merge([stocks, t.where("!isNull(Symbol) && Symbol.startsWith(`I:`)")])
```

`underlying` holds `BRK.B`, `AAPL`, and `I:SPX`, ready for
`m.options.chain(underlying=underlying)`.

<a id="handle-errors"></a>

## Handle errors in code

In a program, read with `wait=True`, so that Massive's errors raise in the call instead of
failing the table. `dm.MassiveError` covers every error Massive reports and every failure
inside a method of a query, such as `fetch()` or `last_by()`. A wrong argument raises
`TypeError` or `ValueError` at the call. Branch on these attributes of a `dm.MassiveError`,
never on its message:

| Attribute | What it says |
|---|---|
| `is_retryable` | `True` when the same call may succeed later, such as after a rate limit (HTTP 429), a failure on Massive's side (HTTP 500, 502, 503, or 504), a connection that kept failing, or a flat file Massive has not published yet. `False` when it can't, such as for a plan that lacks the data, a refused key, a closed session, or a bug. |
| `is_bug` | `True` for a defect: a bug in the library, or a problem the library caught in your code, such as a watchlist table that failed. The message says how to report the bug, or how to fix your code. |
| `source` | What stopped the call, as a `dm.ErrorSource`: `MASSIVE` (Massive refused or failed), `NETWORK` (no answer came, such as from a connection that kept failing), `USER` (your code or settings), or `LIBRARY` (a library bug). Later versions may add sources, so handle values you don't know. |
| `status` | The HTTP status of the answer, such as Massive's 404 or a proxy's 407, or `None` when there was none. |

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
            print(f"Report or fix this: {e}")
        if not e.is_retryable or attempt == 2:
            raise
        time.sleep(60)
```

Before it raises a rate-limit error, a server failure, or a connection that kept failing, the library
has already retried, so wait before you call again.

`fetch()` and every other query method raise `dm.SessionClosedError` once you call
`m.close()`, and so does `m.streams()`. Create a new session to read again.

A `fetch()` or `history()` of a watchlist fails when Massive rejects the API key, or when every
symbol on the watchlist at the call failed. When only some fail, such as when Massive refuses one
symbol's request, the log reports them and `m.symbol_reads()` shows each one's error.

<a id="diagnostics"></a>

## Read the diagnostics tables

Five live tables show what a session is doing. Call each on the session whose tables you're
checking:

| Table | One row for each | Look here when |
|---|---|---|
| `m.problems()` | Problem of the session or its tables, as [Read the problems table](#problems) describes | A table looks wrong |
| `m.streams()` | Live table, and each `fetch()` or `history()` that runs in the background or follows a watchlist | A table failed or stays empty |
| `m.connections()` | Streaming connection, and a `REST` row for each kind of data, saying whether it's real-time or delayed | A stream gets no data |
| `m.rate_limits()` | Massive subscription, with its rate-limit state | A read is slow |
| `m.symbol_reads()` | Symbol of a watchlist read with `history()` or `fetch()` | A watchlist symbol has no rows |

To see a table's state, read `m.streams()`. This example polls Apple's news and shows the
poll's row:

```python
import deephaven_massive as dm

m = dm.MassiveSession()  # or the session you already have
news = m.reference.news("AAPL", limit=10).poll(300)
streams = m.streams()
```

`streams` has a row for the news poll. `Query` shows the call that made it, and `State` is
`RUNNING` while the poll delivers rows. `help(m.streams)` describes every column.

Act on these states:

| Table | `State` | Why | What to do |
|---|---|---|---|
| `m.streams()` | `LOADING` | A read's rows are on their way. | Wait. A watchlist read turns `RUNNING` once each symbol has been read. `m.symbol_reads()` shows each symbol's result. |
| `m.streams()` | `DONE` | The read finished. With no rows, Massive had none for it. | Nothing, unless you expected rows. Then check the range, and look the symbol up in Massive's lists, such as `m.reference.tickers()`. |
| `m.streams()` | `FAILED` | The table stopped, and `Error` says why. | Fix what `Error` names, then run the line that made the table again. |
| `m.connections()` | `WAITING` | The library hasn't connected: no live table on this connection has a symbol it can stream yet. | Add a symbol to your watchlist. If it already has symbols, find them in `m.problems()`, whose `Cause` says why each is skipped. |
| `m.connections()` | `WAITING_SLOT` | Massive refused this connection at login: your account has no free connection for this asset class. | Stop any other program that uses your key. The library keeps trying, and its live tables fail after `max_outage`. |
| `m.symbol_reads()` | `FAILED` | The symbol's read failed, and `Error` says why. | Fix what `Error` names, then delete the symbol's row from the watchlist and add it back. |

`LastError` in `m.connections()` shows a connection's latest error.

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
3. The five diagnostics tables of the session that failed, printed right after the failure and before you close the session.

This function prints them. Paste it into the console where the failure happened, then call it
with that session, such as `print_diagnostics(m)`:

```python
def print_diagnostics(session):
    for table in (
        session.problems(),
        session.streams(),
        session.connections(),
        session.rate_limits(),
        session.symbol_reads(),
    ):
        print(table.to_string(num_rows=1000))
```
