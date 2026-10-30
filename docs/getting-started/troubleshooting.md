# Troubleshooting

Each entry names a symptom, its usual cause, and the fix. When none fits, turn on the debug
log (at the end of this page) and look at what the library did.

## Setting up

**`AuthenticationError: no Massive API key`.** The server cannot see `MASSIVE_API_KEY`.
Environment variables reach the server only from the shell that started it: export the key
there and restart the server, or pass `dm.MassiveConfig(api_key=...)`.

**`MassiveError: MassiveSession must be created inside a Deephaven worker`.** The code ran in
a plain Python process. Run it in the Deephaven console, or in a script the server runs.

**`TypeError: timespan must be dm.Timespan, e.g. dm.Timespan.DAY`.** Options that pick
from a fixed set take an enum member, not text. Write `dm.Timespan.DAY`, not `"day"`. To
convert text you read from elsewhere, use `dm.Timespan("day")`.

## Symbols

**`ValueError` naming a symbol.** Every symbol must be Massive's exact ticker: `C:EURUSD`, not
`EUR/USD`; `X:BTCUSD`, not `BTC-USD`; `BRK.B`, not `BRK-B`; `I:SPX`, not `SPX`. The
converters translate other spellings: `dm.canonical_symbol_forex("EUR/USD")` returns
`"C:EURUSD"`, and `dm.canonicalize_symbols` converts a whole table. See
[Reactive watchlists](watchlists.md).

**A watchlist symbol never shows up.** Look at the stream's `Error` in `m.streams()`. A row in
another spelling is skipped and listed there. Fix the row in the watchlist and it subscribes.

**A symbol returns no rows.** Massive has no data for it in that range, or the ticker changed.
The library logs a WARNING for a symbol Massive does not know.
`m.reference.ticker_events("META").fetch()` lists a ticker's past names, and `m.reference.tickers(search="Meta").fetch()` finds a ticker by name.

## Live tables

**A live table stays empty.** In order of likelihood:

1. The market is closed. Stock and option streams are quiet outside trading hours; crypto
   trades around the clock. `m.reference.market_status().fetch()` says what is open.
2. The command that created it is still running, or another long command is. The console holds
   the lock live tables need until a command ends; see "Every live table froze" below.
3. Your plan sees the data 15 minutes late (`Recency` is `DELAYED` in `m.connections()`), so
   the first rows of the day arrive 15 minutes after the open.
4. The stream failed. `m.streams()` shows `FAILED` and the reason in `Error`.
5. The connection is waiting. `m.connections()` shows `WAITING_SLOT` when another program
   uses your key's connection, or `RECONNECTING` after a network drop.

**The table shows an error and stops updating.** It failed. The error in the web UI, and the
`Error` column of `m.streams()`, say why. A plan refusal is explained in
[Plans and entitlement errors](plans.md). After fixing the cause, call the verb again.

**`WAITING_SLOT` in `m.connections()`.** Massive's individual plans allow one connection
per asset class at a time, for all your programs together. Another program, a notebook, or a second
computer is using your key. Stop it; the library connects as soon as the slot frees.

**Every live table froze for a while.** A long command ran in the console. The console holds
the lock every live table needs until the command returns, so a `history()` that downloads for
ten minutes pauses every live table for ten minutes; the library logs a WARNING when this
happens. Pass `wait=False` to long `history()` and `fetch()` calls: they return at once, and
the table fills when the download is done.

**Memory keeps growing.** An `append_only()` table keeps every row. Use `ring(n)` for a
rolling window, `last_by()` for current state, or `blink()` with an aggregation. A
`last_by()` remembers every symbol it has ever seen, so it grows with a watchlist that keeps
changing.

**A table keeps running after `del`.** Deleting a variable stops its stream only when the
Java side next reclaims memory, which can take a while. Release it at once with
`m.release(table)`. See [Releasing tables and closing a session](releasing.md).

**`Can not listen to failed table`.** The table failed, often because the session was closed,
and Deephaven builds nothing new on a failed table. Take `table.snapshot()` and work with the
copy.

## History

**A download is slow.** On a free plan each subscription allows five REST requests a minute,
and the library spaces its requests to fit: a long range, or many symbols, takes many minutes.
`m.rate_limits()` shows the pace. With flat-file keys, many symbols or `dm.ALL` read from the
flat files, which are much faster. Run long reads with `wait=False`.

**Today's bars are missing.** A bar appears only once it has ended, so today's daily bar
arrives tomorrow and the current minute's bar arrives when the minute ends.

**`... is not yet published; flat files land ~11:00 ET next day`.** Massive publishes a day's
flat file the next morning. Read that day with `source=dm.HistorySource.REST`, or wait.

**A WARNING says the read leaves some columns empty.** The library chose one source and the
other would have filled those columns. Pass the `source=` the WARNING names.

**`history()` refuses `dm.ALL`.** REST takes explicit symbols, except for daily bars. Use
flat-file keys, or name the symbols.

**Prices jump at a split.** Historical stock and option bars are split-adjusted by default;
live bars are as traded. A table that joins the two shows a step on a split's date. Build it
again after a split. Pass `adjusted=False` to `bars()` for as-traded history.

## Warnings and errors in the log

The library writes to Deephaven's log, which the web UI shows in its **Log** panel. Each level
tells you what, if anything, to do.

- **INFO** reports a decision or progress: which source a history read chose, how long a paced
  download will take, a connection that came back. Nothing to do.
- **WARNING** means a live table may be stale or wrong, or there is something you should act
  on. The message says which. Examples: a WebSocket connection is down and its tables get no
  data until it reconnects; no data has arrived for a while during trading hours; a poll's
  cycles take longer than its interval; a long console command is pausing every live table.
  Each is logged once when it starts, not every second while it lasts, and an INFO line says
  when it has recovered.
- **ERROR** means a defect broke a result, and the message says whose defect it is:
  - **Yours**, in your code or input that the library caught, such as a callback that raised
    or a watchlist table that failed upstream. The message names what to fix.
  - **The library's**: the message ends with "This is a bug in deephaven-massive" and says how
    to report it. Please do, with the debug log described next and the contents of
    `m.streams()`.

Expected conditions are never logged at ERROR: a refusal from your plan, a rate limit, a
network failure, or your own `close()`. When one of them reaches you as an exception, it is
logged only at DEBUG, since the exception already tells you.

Every `dm.MassiveError` says the same thing in code, through its `cause`:

| `cause` | Meaning | Logged at |
|---|---|---|
| `dm.ErrorCause.EXPECTED` | A condition the library expects and reports: a refusal, a plan or rate limit, a network failure, a closed session. | below ERROR |
| `dm.ErrorCause.USER_DEFECT` | A defect in your code or input that the library caught. The message names what to fix. | ERROR |
| `dm.ErrorCause.LIBRARY_DEFECT` | A bug in the library, or data from Massive it cannot read. The message says how to report it. | ERROR |

```python
import deephaven_massive as dm

m = dm.MassiveSession()

try:
    bars = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2026-01-02", "2026-10-30")
except dm.MassiveError as error:
    if error.cause is dm.ErrorCause.LIBRARY_DEFECT:
        print(f"please report this: {error}")
    else:
        print(f"failed: {error}")
```

## The debug log

To see everything the library does, turn on its debug log, run the failing code again, and
turn the log off:

```python
import deephaven_massive as dm

dm.enable_debug_logging()  # to the Log panel; pass a file path to write a file instead
# ... run the code that fails ...
dm.disable_debug_logging()
```

To capture a problem from the moment the server starts, set `DEEPHAVEN_MASSIVE_LOG=DEBUG`
before starting it, or set `DEEPHAVEN_MASSIVE_LOG_FILE=/path/to/massive.log` to write the
debug log to a file (it logs at DEBUG unless `DEEPHAVEN_MASSIVE_LOG` names another level).
The log never holds your API key or S3 secret, so you can attach it to a bug report as it is.

A DEBUG log grows quickly: a long history read or a busy live table can write megabytes in a
few minutes. Attach the file to the issue instead of pasting it, because GitHub cuts an issue's
text at 65,536 characters, and accepts attached files up to 25 MB. If the file is larger, zip
it, or keep its first lines (the library's version and each session's settings) and every line
from a few minutes before the failure to a minute after it. The times in the log are in UTC.
With the log, attach the output of `m.rate_limits()`, `m.streams()`, and `m.connections()`,
printed after the failure with `to_string(num_rows=1000)`.
