# Watchlists

A watchlist is a live Deephaven table with a `Symbol` column, passed as `symbols`. A call given
one reads or streams the symbols it holds, and keeps up as you add and delete rows. Add `NVDA`
and its trades start arriving, delete it and they stop, and you never restart the stream. Look
here to build and edit a watchlist, to let one dataset choose another's symbols, and to see
what happens to each symbol along the way.

Most examples here stream stock trades. As written, they need a Stocks Advanced plan. On Stocks
Developer, whose streams run 15 minutes behind, create each session with
`dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS})`. Paste each example into the
console of the Deephaven IDE.

<a id="edit-a-watchlist"></a>

## Make and edit a watchlist

An [input table](https://deephaven.io/core/docs/how-to-guides/input-tables/) is a Deephaven table
you can edit, in code or by typing into it in the Deephaven IDE. One keyed on a `Symbol` column
makes a watchlist. This example makes one, streams the latest trade of each symbol on it, and
then edits it:

```python
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession()
watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
watchlist.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))
board = m.stocks.trades(watchlist).last_by()

watchlist.add(new_table([string_col("Symbol", ["NVDA"])]))
watchlist.delete(new_table([string_col("Symbol", ["MSFT"])]))
```

`board` shows the latest trade of AAPL and NVDA. MSFT's row left when you deleted it.

To edit the watchlist by hand, open the `watchlist` panel in the Deephaven IDE:

1. To add a symbol, click the empty row at the bottom, type the symbol, and press Enter. Then
   click **Commit**, because the rows you type aren't added until you commit them.
2. To delete a symbol, right-click its row and choose **Delete Selected Rows**.

In code, delete rows by passing a static table of the keys, as the example does. Passing a live
view of the watchlist itself can hang the server.

<a id="what-each-verb-does"></a>

## What each verb does with a watchlist

Every verb follows a watchlist, but each in its own way:

| Verb | When you add a symbol | When you delete one |
|---|---|---|
| A stream: `blink()`, `append_only()`, `ring()`, or `last_by()` | Subscribes to it | Unsubscribes |
| `poll()` | Reads it from the next cycle on | Stops reading it from the next cycle on |
| `fetch()` or `history()` | Reads it, and its rows appear all at once when the read finishes | Its rows leave at once |

`fetch()` and `history()` of a watchlist return a live table. `history()` reads every symbol
over the same range, fixed when you call it, and an open `end` means through that moment.
`fetch()` has no range, so each symbol's rows are Massive's answer when that symbol was read. A
snapshot of a symbol you added at 14:00 is the 14:00 snapshot. A static table of symbols, such
as one from `new_table()` or `snapshot()`, is a fixed list that's read once, at the call.

<a id="choose-symbols-with-a-table"></a>

## Choose symbols with another dataset

Any table with a `Symbol` column can choose symbols, so one dataset can choose another's. The
top gainers can choose which stocks to stream, for example. This needs a Stocks plan that
includes both movers and trade streams:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
hot = m.stocks.trades(gainers).last_by()
```

`hot` streams the trades of the day's top gainers. `gainers` is live, so the stream follows it as
the list changes each minute.

A static table chooses once. This streams the trades of every active E-mini S&P 500 futures
contract, on a Futures Advanced plan, or Futures Developer with
`delayed_subscriptions={dm.Subscription.FUTURES}`:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
contracts = m.futures.contracts(product_code="ES", filters={"active": True}).fetch()
es = m.futures.trades(contracts).last_by()
```

`contracts` is static, so `es` streams the contracts that were active when you called it.

A watchlist can also choose whole option chains. Pass it as `underlying`, and each symbol's
chain is read as if you had passed it alone. This reads the calls of each underlying on the
watchlist, on an Options Advanced plan, or Options Starter or Developer with
`delayed_subscriptions={dm.Subscription.OPTIONS}`:

```python
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession()
underlyings = input_table({"Symbol": dht.string}, key_cols="Symbol")
underlyings.add(new_table([string_col("Symbol", ["SPY", "QQQ"])]))
chains = m.options.chain(underlyings, contract_type=dm.ContractType.CALL).fetch()
```

`chains` has a row for each call contract of SPY and QQQ. Add an underlying and its chain
appears once it's read, and delete one and its contracts leave. `m.options.contracts` takes the
same table as its `underlying`.

<a id="arguments-that-take-a-table"></a>

## Other arguments that take a table

Two arguments that take one value also take a table of values:

| Argument | Datasets | Table column |
|---|---|---|
| `underlying` | `m.options.chain`, `m.options.contracts` | `Symbol` |
| `product_code` | Every futures dataset that takes it | `ProductCode`, as the rows of `m.futures.products()` have it |

Each value is read as if you had passed it alone. A live table works as a watchlist does. A
`fetch()` reads a value when its row appears and drops its rows when the row leaves, a `poll()`
reads the table at every cycle, and `m.symbol_reads()` lists each value. A static table is a
fixed list that's read once, and one value that fails fails the call. A value the dataset
doesn't take, such as an option symbol among the underlyings, is handled as a
[wrongly spelled watchlist row](#skipped-rows).

A `fetch()` follows only one live table. When `symbols` is live too, use `poll()`, which reads
both at every cycle, or pass `product_code` a list or a `snapshot()`.

This lists the active contracts of two futures products and follows a table of product codes,
on any Futures plan:

```python
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession()
codes = input_table({"ProductCode": dht.string}, key_cols="ProductCode")
codes.add(new_table([string_col("ProductCode", ["ES", "NQ"])]))
active = m.futures.contracts(product_code=codes, filters={"active": True}).fetch()
```

`active` lists the active contracts of the E-mini S&P 500 (`ES`) and E-mini Nasdaq-100 (`NQ`)
futures, and changes as you edit `codes`.

<a id="stream-a-watchlist-as-one-table"></a>

## Stream a watchlist as one table

Put many symbols in one stream, through one watchlist or one list, and filter that one table
when you want a single symbol. Hundreds of one-symbol streams of busy symbols make every live
table on the server fall behind, while one stream of the same symbols doesn't. This streams
three stocks and picks out Apple:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
board = m.stocks.trades(["AAPL", "MSFT", "NVDA"]).last_by()
aapl = board.where("Symbol == `AAPL`")
```

`aapl` is the row of `board` for AAPL, and it updates with it.

<a id="last-by-and-watchlists"></a>

## Latest rows of a changing watchlist

A `last_by()` keyed by `Symbol`, the default, follows the watchlist row by row. It shows a
symbol's row once its first trade or quote arrives after you add it, and drops the row when you
delete the symbol. A symbol you add back shows nothing until it trades again. With keys that span
symbols, such as `last_by("Exchange")`, a group keeps its row after the symbol behind it leaves.
`append_only()` and `ring()` keep the rows a deleted symbol already produced.

To replace the whole list, add the new list first, then delete only the symbols that left. A
symbol on both lists then never leaves the watchlist, so its row stays:

```python
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession()
watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
watchlist.add(new_table([string_col("Symbol", ["AAPL", "NVDA"])]))
latest = m.stocks.trades(watchlist).last_by()

# Switch to AAPL and AMD. AAPL never leaves, so its row stays.
watchlist.add(new_table([string_col("Symbol", ["AAPL", "AMD"])]))
watchlist.delete(new_table([string_col("Symbol", ["NVDA"])]))
```

`latest` keeps AAPL's row throughout, loses NVDA's, and shows AMD once it trades.

<a id="seed-a-watchlist-board"></a>

## Show every symbol before it trades

`last_by()` starts empty and fills in as each symbol trades. To start with a row for every
symbol, merge a snapshot poll that follows the same watchlist under the stream, as the README's
[seed a live board](../../README.md#seed-a-live-board) does. This needs a Stocks plan that
includes both snapshots and trade streams:

```python
from deephaven import dtypes as dht
from deephaven import input_table, merge, new_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession()
watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
watchlist.add(new_table([string_col("Symbol", ["AAPL", "MSFT", "NVDA"])]))

seed = (
    m.stocks.snapshots(watchlist)
    .poll(60)
    .view(["Symbol", "Price = LastTradePrice", "Timestamp = LastTradeSipTimestamp"])
)
trades = m.stocks.trades(watchlist).last_by().view(["Symbol", "Price", "Timestamp"])
live_board = merge([seed, trades]).last_by("Symbol")
```

`live_board` has one row per symbol. It shows the snapshot's price until the symbol's first
trade, then its latest trade. The stream's rows come after the snapshot's in the merge, so
`last_by("Symbol")` keeps the trade once there is one.

<a id="followed-reads"></a>

## History and fetch of a watchlist

A `history()` or `fetch()` of a watchlist reads each symbol on its own, so one bad symbol never
stops the table. A symbol whose read goes wrong ends up in one of three ways:

| What happens to one symbol | What you see |
|---|---|
| Massive has no data for it | No rows for it. With `MassiveConfig(unavailable_symbols=dm.OnError.RAISE)`, it's marked failed instead. |
| A passing failure, such as a rate limit or a dropped connection | It's read again 1, 5, and 15 minutes later |
| Your plan refuses it | It's marked failed |

`m.symbol_reads()` has one row per symbol, with its state and, for a failure, the reason. To read
a failed symbol again, delete its row and add it back. Only a problem that affects every symbol
fails the whole table, such as a rejected API key, a closed session, a failed watchlist, or a bug
in the library.

With `wait=True`, the default, the call returns once the watchlist's current symbols are read,
and raises only if every one of them failed. A symbol you delete and add back is downloaded
again by `fetch()`, and by `history()` unless you set its
[`reread` argument](arguments.md#verb-arguments). A `limit` is sent with each symbol's request,
as Massive's cap on that symbol's rows. A news article, filing, or financial statement appears
once under each named ticker it lists, as in every read.

<a id="large-live-results"></a>

## Large live results

Every table you derive from a large live result updates when a symbol arrives or leaves. Sorting
tens of millions of rows takes seconds each time, so when you need a fixed copy, derive from
`snapshot()` of the result instead.

<a id="skipped-rows"></a>

## Rows the library skips

One bad row never stops a watchlist's table. Each kind of bad row has its own setting in
`dm.MassiveConfig`, described in [data the library can't read](arguments.md#columns). Set it to
`dm.OnError.RAISE` to fail instead.

| Row | What happens by default | Setting |
|---|---|---|
| Not spelled as Massive spells the ticker | Skipped, logged once, and listed in the stream's `Error` in `m.streams()`. Fix the row and it subscribes. | `noncanonical_symbols` |
| A crypto pair missing from Massive's list of crypto tickers | Skipped and listed the same way, while the stream keeps its other pairs. It stays skipped until you delete its row and add it back. | `unlisted_symbols` |

A static table of symbols logs each bad row with one WARNING and skips it, but its stream's
`Error` doesn't list them. A watchlist whose every row is bad is refused at the call. When
Massive's list of crypto tickers can't be read as a pair is added, the library tries twice more
over about ten seconds, and the stream's `Error` says so.

`unlisted_symbols` also covers a crypto pair's last trade and its daily open and close. A
`fetch()` or `poll()` leaves out a pair Massive doesn't list. A `poll()` lists it in its
`Error`, and a `fetch()` of a watchlist marks it failed in `m.symbol_reads()`. Under `RAISE`, the
call raises and a live table stops.

A futures contract subscribes once its trading schedule has loaded, up to 15 seconds after you
add it, so that its first rows carry a `Date`. An options watchlist can hold at most 1,000
topics, one for each contract and kind of data, and holds back the contracts past that (see
[option contracts](arguments.md#symbols)).

<a id="schedules"></a>

## Stream only during market hours

A schedule is a watchlist that's empty outside the hours you care about. While it's empty, its
stream stops and its poll sends no requests. This one holds three stocks only during New York's
regular session, 09:30 to 16:00 on weekdays, though it doesn't skip holidays:

```python
from deephaven import new_table, time_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession()
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

`session_open` has one row while the session is open and none otherwise, and 570 and 960 are
09:30 and 16:00 as minutes of the day. `scheduled` holds the three symbols while `session_open`
has its row. `trades` streams the three stocks during the session and stops between sessions.
