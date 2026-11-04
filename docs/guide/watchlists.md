# Watchlists

A watchlist is a live Deephaven table with a `Symbol` column, passed as `symbols`. Use one when
the symbols you follow change during the day. You edit one table, and every stream and read
built on it follows. Add `NVDA` and its trades start arriving, and delete it and they stop.

Most examples here stream stock trades. As written, they need a Stocks Advanced plan. On Stocks
Developer, whose streams run 15 minutes behind, create each session with
`dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS})`. Paste each example into the
console of the Deephaven IDE. A streamed table fills in during market hours, as each symbol
trades.

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

During market hours, `board` gains a row for AAPL and one for NVDA as soon as each trades. MSFT
has no row, because you deleted it from the watchlist. In code, delete rows by passing a static
table of the keys, as the example does. A live view of the watchlist itself, passed to
`delete`, can hang while Deephaven refreshes the watchlist.

To edit the watchlist by hand, open the `watchlist` panel in the Deephaven IDE:

- To add a symbol, click the empty row at the bottom, type the symbol, and press Enter. Then
  click **Commit**. The rows you type aren't added until you commit them.
- To delete a symbol, right-click its row and choose **Delete Selected Rows**.

Two kinds of symbol take a moment or a limit. A futures contract you add starts streaming up to
15 seconds later. An options watchlist streams at most 1,000 contracts at once, and a contract
whose trades and quotes you both stream counts twice. Contracts past the limit wait until others
leave (see [the 1,000-contract limit](arguments.md#option-contract-limit)).

<a id="what-each-verb-does"></a>

## See what each verb does with a watchlist

When you add or delete a symbol, each verb reacts at a different time:

| Verb | When you add a symbol | When you delete one |
|---|---|---|
| A stream: `blink()`, `append_only()`, `ring()`, or `last_by()` | Subscribes to it | Unsubscribes |
| `poll()` | Reads it from the next cycle on | Stops reading it from the next cycle on |
| `fetch()` or `history()` | Reads it, and its rows appear all at once when the read finishes | Its rows leave at once |

A static table of symbols, such as one from `new_table()` or `snapshot()`, is a fixed list. A
call reads it once, when you make the call, and never again.

<a id="followed-reads"></a>

## Read the history of a watchlist

A `history()` or `fetch()` of a watchlist returns a live table that gains and loses symbols as
you edit the watchlist. `history()` reads every symbol over the same dates. They're fixed when
you call it, and an open `end` stops at that moment. `fetch()` has no range, so each symbol's
rows are Massive's answer at the moment that symbol was read. A snapshot of a symbol you added
at 14:00 is the 14:00 snapshot.

The library reads each symbol separately, and a symbol it can't read affects only its own rows.
A symbol whose read goes wrong ends up in one of three ways:

| What goes wrong | What you see |
|---|---|
| Massive has no data for the symbol | No rows for it. With `MassiveConfig(unavailable_symbols=dm.OnError.RAISE)`, it's marked failed instead. |
| A passing failure, such as a rate limit or a dropped connection | The library reads it again 1, 5, and 15 minutes later |
| Your plan refuses it | It's marked failed |

`m.symbol_reads()` has one row for each symbol, with its state and, for a failure, the reason.
To read a failed symbol again, delete its row and add it back. A rejected API key or a closed
session fails the whole table.

The call returns once it has read every symbol on the watchlist, and raises only if all of them
failed. That's `wait=True`, the default. A symbol you delete and add back is downloaded again by
`fetch()`. `history()` downloads it again too, unless you change its
[`reread` argument](arguments.md#reread).

Each symbol gets its own `limit`, so `limit=100` returns up to 100 rows for each symbol, not 100
in all. Ten symbols can therefore give you up to 1,000 rows.

<a id="large-live-results"></a>

## Take a fixed copy of a large result

A large `history()` of a watchlist can hold tens of millions of rows. Every table you derive
from it updates whenever a symbol arrives or leaves, so a sort of the whole result runs again
each time. When you don't need the derived table to follow the watchlist, derive it from
`snapshot()` of the result instead. The copy holds the rows of that moment and never updates.

<a id="choose-symbols-with-a-table"></a>

## Choose symbols with another dataset

Any table with a `Symbol` column can choose symbols, so one dataset can choose another's. This
streams the trades of the day's top gainers:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
hot = m.stocks.trades(gainers).last_by()
```

`gainers` is live, so `hot` follows it as the list changes each minute. During market hours,
`hot` has a row for each gainer that has traded since it joined the list.

A static table chooses once. This streams the trades of every active E-mini S&P 500 futures
contract. Futures Advanced runs it as written, and Futures Developer needs
`delayed_subscriptions={dm.Subscription.FUTURES}` in its session:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
contracts = m.futures.contracts(product_code="ES", filters={"active": True}).fetch()
es = m.futures.trades(contracts).last_by()
```

`contracts` is static, so `es` streams the contracts that were active when you called it. Each
contract gets a row once it trades.

A watchlist can also choose whole option chains. Pass it as `underlying`, and the library reads
each symbol's chain as if you had passed that symbol alone. This reads the calls of each
underlying on the watchlist. It runs as written on Options Advanced. Options Starter and
Developer serve the chain 15 minutes late, so add `delayed_subscriptions={dm.Subscription.OPTIONS}`
there:

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

`chains` has a row for each call contract of SPY and QQQ. When you add an underlying, its chain
appears once it's read, and when you delete one, its contracts leave. `m.options.contracts`
takes the same table as its `underlying`.

<a id="arguments-that-take-a-table"></a>

## Pass a table to other arguments

Two arguments that take one value also take a table of values, and read each value as if you
had passed it alone:

| Argument | Datasets | Table column |
|---|---|---|
| `underlying` | `m.options.chain`, `m.options.contracts` | `Symbol` |
| `product_code` | Every futures dataset that takes it | `ProductCode`, as the rows of `m.futures.products()` have it |

A live table works as a watchlist does. A `fetch()` reads a value when its row appears, and
drops that value's rows when the row leaves. `m.symbol_reads()` shows how each value's read went. A
`poll()` reads the whole table every cycle and names the values it skipped in its `Error` in
`m.streams()`.

A static table is a fixed list that's read once, and one value that fails fails the call. The
library skips a value the dataset doesn't take, such as an option symbol among the underlyings,
as it skips a misspelled watchlist row.

A `fetch()` follows at most one live table. When both `symbols` and `product_code` are live,
use `poll()`, which reads both every cycle, or pass `product_code` a list or a `snapshot()`.

This lists the active contracts of two futures products and follows a table of product codes.
Every Futures plan includes contracts, the free one too:

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
futures. It changes as you edit `codes`.

<a id="stream-a-watchlist-as-one-table"></a>

## Stream many symbols in one table

Put many symbols in one stream, through one watchlist or one list, and filter that table when
you want a single symbol. One stream carries hundreds of busy symbols more cheaply than hundreds
of one-symbol streams. This streams three stocks and picks out Apple:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
board = m.stocks.trades(["AAPL", "MSFT", "NVDA"]).last_by()
aapl = board.where("Symbol == `AAPL`")
```

`aapl` has Apple's row of `board` once Apple trades, and it updates with each new trade.

<a id="last-by-and-watchlists"></a>

## Keep the latest rows as the watchlist changes

A `last_by()` keyed by `Symbol`, the default, follows the watchlist row by row. A symbol's row
appears when its first trade or quote arrives after you add it, and leaves when you delete the
symbol. A symbol you add back has no row until it trades again. With keys that span symbols,
such as `last_by("Exchange")`, a group keeps its row after the symbol behind it leaves.
`append_only()` and `ring()` keep the rows a deleted symbol already produced.

To replace the whole list, add the new list first, then delete only the symbols that left. A
symbol on both lists then never leaves the watchlist, and its row stays:

```python
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession()
watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
watchlist.add(new_table([string_col("Symbol", ["AAPL", "NVDA"])]))
latest = m.stocks.trades(watchlist).last_by()

# Add the new list first: AAPL never leaves the watchlist, so its row isn't lost.
watchlist.add(new_table([string_col("Symbol", ["AAPL", "AMD"])]))
watchlist.delete(new_table([string_col("Symbol", ["NVDA"])]))
```

`latest` keeps AAPL's row throughout and loses NVDA's. AMD's row appears once AMD trades.

<a id="seed-a-watchlist-board"></a>

## Show every symbol before it trades

`last_by()` starts empty and fills in as each symbol trades. To start with a row for every
symbol, merge a snapshot poll that follows the same watchlist under the stream, as the README's
[seed a live board](../../README.md#seed-a-live-board) does:

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

`live_board` has a row for each of the three symbols within a minute, even outside market hours.
Each row shows the snapshot's price until the symbol trades, then its latest trade. The merge
puts the stream's rows after the snapshot's, and `last_by("Symbol")` keeps the last row of each
symbol, so a trade replaces the snapshot.

<a id="skipped-rows"></a>

## Find the rows the library skipped

When a watchlist row is bad, the library skips it and logs it once. The rest of the table keeps
running. A watchlist whose every row is bad is the exception, and the call refuses it at once.
Each kind of bad row has its own `dm.MassiveConfig` setting, listed in the table, and
[data the library can't read](arguments.md#unreadable-data) has the full list. Set one to
`dm.OnError.RAISE` to make the call fail instead:

| Bad row | What you see by default | Setting |
|---|---|---|
| A misspelled row, one not spelled as Massive spells the ticker | The row is skipped and named in the stream's `Error` in `m.streams()`. Once you correct the row, the stream subscribes to the symbol. | `noncanonical_symbols` |
| A crypto pair missing from Massive's list of crypto tickers | The pair is skipped and named the same way, and the stream keeps its other pairs. The pair stays skipped until you delete its row and add it back. | `unlisted_symbols` |

A static table of symbols skips its bad rows too, but the stream's `Error` doesn't name them,
so look for them in the log. If Massive's list of crypto tickers can't be read for about ten
seconds after you add a pair, the stream skips the pair and names it in `Error`.

`unlisted_symbols` also covers a crypto pair's last trade and its daily open and close. Each
verb leaves out a pair Massive doesn't list, and shows you where:

| Verb | What happens to the pair | Where you see it |
|---|---|---|
| `fetch()` | Left out of the table | The log |
| `fetch()` of a watchlist | Marked failed | `m.symbol_reads()` |
| `poll()` | Left out of each cycle | The poll's `Error` in `m.streams()` |

Under `dm.OnError.RAISE`, the call raises instead, and a live table stops.

<a id="schedules"></a>

## Stream only during market hours

To stream only during the hours you care about, give the stream a watchlist that's empty the
rest of the time. While the watchlist is empty, its stream stops and a poll of it sends no
requests. This watchlist holds three stocks only during New York's regular session, 09:30 to
16:00 on weekdays, though it doesn't skip holidays:

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
has its row. During the session, `trades` holds the latest trade of each stock. At 16:00 the
watchlist empties, the stream stops, and each stock's row leaves, so the table stays empty until
09:30 the next weekday.
