# Watchlists

Examples on this page assume `import deephaven_massive as dm` and `m = dm.MassiveSession()`.

A *watchlist* is a live Deephaven table with a `Symbol` column, passed as `symbols`. A call given
one reads or streams the symbols it holds, and keeps up as you add and delete rows: add `NVDA`,
and its trades start arriving; delete it, and they stop. You never restart the stream.

<a id="edit-a-watchlist"></a>

## Make and edit a watchlist

An input table is a Deephaven table you can edit, in code or by typing in the Deephaven IDE.
With a `Symbol` key column it makes a watchlist:

```python
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
watchlist.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))
board = m.stocks.quotes(watchlist).last_by()

watchlist.add(new_table([string_col("Symbol", ["NVDA"])]))
watchlist.delete(new_table([string_col("Symbol", ["MSFT"])]))
```

`board` shows the latest quote of AAPL and NVDA. MSFT's row left when you deleted it.

To edit the watchlist in the Deephaven IDE, open the `watchlist` panel:

- **Add a symbol:** click the empty row at the bottom, type the symbol, press **Enter**, and
  click **Commit**. Rows you type are not added until you commit them.
- **Delete a symbol:** select its row, right-click, and choose **Delete Selected Rows**.

In code, delete rows by passing a static table of the keys, as above. Passing a live view of the
watchlist itself can hang the server.

<a id="what-each-verb-does"></a>

## What each verb does with a watchlist

| Verb | When you add a symbol | When you delete one |
|---|---|---|
| A stream (`blink()`, `append_only()`, `ring()`, `last_by()`) | Subscribes to it | Unsubscribes |
| `poll()` | Reads it from the next cycle on | Stops reading it from the next cycle on |
| `fetch()`, `history()` | Reads it; its rows appear all at once when the read finishes | Its rows leave at once |

`fetch()` and `history()` of a watchlist return a live table.

- `history()` reads every symbol over the same range, fixed when you call it. An open `end`
  means through that moment.
- `fetch()` has no range: each symbol's rows are Massive's answer when that symbol was read. A
  snapshot of a symbol added at 14:00 is the 14:00 snapshot.

A static table, such as one from `new_table()` or `snapshot()`, is a fixed list, read once at
the call.

<a id="choose-symbols-with-a-table"></a>

## Choose symbols with another dataset

Any table with a `Symbol` column can choose symbols, so one dataset can choose another's:

```python
gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
hot = m.stocks.trades(gainers).last_by()

contracts = m.futures.contracts(product_code="ES", filters={"active": True}).fetch()
es = m.futures.trades(contracts).last_by()
```

- `hot` streams the trades of the day's top gainers. `gainers` is live, so the stream follows
  it as the list changes each minute.
- `es` streams the trades of every active E-mini S&P 500 futures contract. `contracts` is
  static, so the list is fixed at the call.

<a id="stream-a-watchlist-as-one-table"></a>

## Stream a watchlist as one table

Put many symbols in one stream, through one watchlist or one list, rather than opening a stream
per symbol. Hundreds of one-symbol streams of busy symbols make every live table on the server
fall behind; one stream of the same symbols does not. To see one symbol, filter the shared
table:

```python
aapl = board.where("Symbol == `AAPL`")
```

`aapl` is the row of `board` for AAPL, updating with it.

<a id="followed-reads"></a>

## `history()` and `fetch()` of a watchlist

A `history()` or `fetch()` of a watchlist reads each symbol on its own, so one bad symbol never
stops the table:

| What happens to one symbol | What you see |
|---|---|
| Massive has no data for it | No rows for it. Under `MassiveConfig(unavailable_symbols=dm.OnError.RAISE)`, it is marked failed instead. |
| A passing failure: a rate limit, a dropped connection | It is read again 1, 5, and 15 minutes later |
| Your plan refuses it | It is marked failed |

`m.symbol_reads()` has one row per symbol, with its state and, for a failure, the reason. To read
a failed symbol again, delete its row and add it back.

Only a problem that affects every symbol fails the whole table: a rejected API key, a closed
session, a failed watchlist, or a bug in the library.

- **With `wait=True`, the default,** the call returns once the watchlist's current symbols are
  read, and raises only if every one of them failed.
- **A symbol you delete and add back** is downloaded again by `fetch()`, and by `history()`
  unless you set its `reread` argument: see [`reread`](arguments.md#verb-arguments).
- **`limit`** is sent on each symbol's request, as Massive's cap on its rows.
- **A news article, filing, or financial statement** appears once under each named ticker it
  lists, as in every read.

<a id="arguments-that-take-a-table"></a>

## Other arguments that take a table

Arguments that take one value also take a table:

| Argument | Datasets | Table column |
|---|---|---|
| `underlying=` | `m.options.chain`, `m.options.contracts` | `Symbol` |
| `product_code=` | Every futures dataset that takes it | `ProductCode`, as the rows of `m.futures.products()` carry it |

Each value is read as if you had passed it alone.

- **A live table** works as a watchlist does. `fetch()` reads a value when its row appears
  and drops its rows when the row leaves; `poll()` reads the table at every cycle; and
  `m.symbol_reads()` lists each value.
- **A static table** is a fixed list, read once. One value that fails fails the call.
- **A value the dataset does not take**, such as an option symbol among the underlyings, is
  handled as a [wrongly spelled watchlist row](#skipped-rows).
- **A `fetch()` follows one live table.** When `symbols` is live too, use `poll()`, which reads
  both at every cycle, or pass `product_code=` a list or a `snapshot()`.

```python
codes = input_table({"ProductCode": dht.string}, key_cols="ProductCode")
codes.add(new_table([string_col("ProductCode", ["ES", "NQ"])]))
active = m.futures.contracts(product_code=codes, filters={"active": True}).fetch()
```

`active` lists the active contracts of the E-mini S&P 500 (`ES`) and E-mini Nasdaq-100 (`NQ`)
futures, and follows `codes` as you edit it.

<a id="large-live-results"></a>

## Large live results

Every table you derive from a large live result updates when a symbol arrives or leaves, and
sorting tens of millions of rows takes seconds each time. When you need a fixed copy, derive
from `snapshot()` of the result instead.

<a id="last-by-and-watchlists"></a>

## `last_by()` and a changing watchlist

`last_by()` keyed by `Symbol`, the default:

- shows a symbol's row as soon as its first trade or quote arrives after you add it;
- drops a symbol's row when you delete it;
- shows nothing for a symbol you add back until it trades again.

With keys that span symbols, such as `last_by("Exchange")`, a group keeps its row after the
symbol behind it leaves. `append_only()` and `ring()` keep the rows a deleted symbol already
produced.

To replace the list, add the new list first, then delete only the symbols that left. A symbol on
both lists never leaves the watchlist, so its row stays; one deleted and added back waits for
its next trade.

```python
watchlist.add(new_table([string_col("Symbol", ["AAPL", "AMD"])]))  # the new list: AAPL, AMD
watchlist.delete(new_table([string_col("Symbol", ["NVDA"])]))  # only the symbol that left
```

<a id="seed-a-watchlist-board"></a>

## Show every symbol before it trades

`last_by()` starts empty and fills in as each symbol trades. To start with a row for every
symbol, put a snapshot under the stream, with a snapshot poll that follows the same watchlist,
as in the README's [seed a live board](../../README.md#seed-a-live-board):

```python
from deephaven import merge

seed = (
    m.stocks.snapshots(watchlist)
    .poll(60)
    .view(["Symbol", "Price = LastTradePrice", "Timestamp = LastTradeSipTimestamp"])
)
trades = m.stocks.trades(watchlist).last_by().view(["Symbol", "Price", "Timestamp"])
live_board = merge([seed, trades]).last_by("Symbol")
```

`live_board` has one row per symbol: its snapshot price until its first trade, then its latest
trade. The stream's rows come after the snapshot's in the merge, so `last_by("Symbol")` keeps
the trade once there is one.

<a id="skipped-rows"></a>

## Rows the library skips

One bad row never stops a watchlist's table. Each kind of bad row has its own
[`dm.OnError` setting](concepts.md#columns); set it to `dm.OnError.RAISE` to fail instead.

| Row | What happens by default | Setting |
|---|---|---|
| Not spelled as Massive spells the ticker | Skipped, logged once, and listed in the stream's `Error` in `m.streams()`. Fix the row and it subscribes. | `noncanonical_symbols` |
| A crypto pair missing from Massive's list of crypto tickers | Skipped and listed the same way; the stream keeps its other pairs. It stays skipped until you delete its row and add it back. | `unlisted_symbols` |

- **A static table of symbols** logs each bad row with one WARNING and skips it, but its
  stream's `Error` does not list them.
- **A watchlist whose every row is bad** is refused at the call.
- **Massive's crypto list unreadable:** when the list cannot be read as a pair is added, the
  library tries twice more over about ten seconds, and the `Error` says so.
- **Crypto `fetch()` and `poll()`:** `unlisted_symbols` also covers a crypto pair's last trade
  and daily open and close. A pair Massive does not list is left out: a `poll()` lists it in its
  `Error`, and a `fetch()` of a watchlist marks it failed in `m.symbol_reads()`. Under `RAISE`,
  the call raises and a live table stops.
- **Futures contracts:** a contract subscribes once its trading schedule has loaded, up to 15
  seconds after you add it, so its first rows carry a `Date`.

The options connection carries at most 1,000 contract streams. A watchlist that grows past
that defers the extra contracts until room frees up: see
[the options cap](arguments.md#symbols).

<a id="schedules"></a>

## Schedules: stream only during market hours

A schedule is a watchlist that is empty outside the hours you care about. While it is empty,
the stream stops, and a poll sends no requests. This one holds its symbols only during New
York's regular session, 09:30 to 16:00 on weekdays (holidays not excluded):

```python
from deephaven import time_table

symbols = new_table([string_col("Symbol", ["AAPL", "MSFT", "NVDA"])])
session_open = (
    time_table("PT1m")
    .tail(1)
    .update_view(
        [
            "Minute = minuteOfDay(Timestamp, timeZone(`America/New_York`), false)",
            "Day = dayOfWeekValue(Timestamp, timeZone(`America/New_York`))",
        ]
    )
    .where(["Day <= 5", "Minute >= 570", "Minute < 960"])
)
scheduled = symbols.join(session_open, on=[], joins=[]).view(["Symbol"])
trades = m.stocks.trades(scheduled).last_by()
```

- `session_open` has one row while the session is open and none otherwise. 570 and 960 are 09:30
  and 16:00 as minutes of the day.
- `scheduled` holds the three symbols while `session_open` has its row, and nothing otherwise.
- `trades` streams the three stocks during the session, and stops between sessions.
