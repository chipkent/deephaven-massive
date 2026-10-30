# Reactive watchlists

Wherever a dataset takes symbols, it also takes a Deephaven table with a `Symbol` column. When
that table is live, the library follows it: add a row and the stream subscribes to the new
symbol; delete a row and it unsubscribes. Such a table is a *reactive watchlist*. It lets you
change what a running table watches without rebuilding it, and lets one table drive another.

## A watchlist you edit

An *input table* is a Deephaven table you can add rows to and delete rows from, by code or by
typing in the web UI. Keyed by `Symbol`, it holds each symbol once:

```python
from deephaven import dtypes as dht
from deephaven import input_table, new_table
from deephaven.column import string_col

import deephaven_massive as dm

m = dm.MassiveSession()

watchlist = input_table({"Symbol": dht.string}, key_cols="Symbol")
watchlist.add(new_table([string_col("Symbol", ["AAPL", "MSFT"])]))

board = m.stocks.quotes(watchlist).last_by()
```

`board` shows the latest quote of AAPL and MSFT. Add a symbol, and its row appears once it
quotes:

```python
watchlist.add(new_table([string_col("Symbol", ["NVDA"])]))
```

Delete one, and its row leaves `board`:

```python
watchlist.delete(new_table([string_col("Symbol", ["MSFT"])]))
```

Delete rows by passing a new static table of the keys, as above. Passing a live view of the
watchlist itself can hang the server.

You can also edit the input table in the web UI: open `watchlist`, type a symbol into the
empty row at the bottom, and press Enter.

## How each verb follows the table

- **Streams** (`blink()`, `append_only()`, `ring(n)`, `last_by()`) subscribe and unsubscribe
  as rows come and go. Rows a removed symbol already produced stay in an append-only or ring
  table, since they did happen. `last_by()` removes the symbol's row. If the symbol comes back,
  its old row reappears until it ticks again.
- **`poll()`** reads the table at the start of every cycle, so a new symbol appears in the
  next cycle and a removed one leaves with it.
- **`fetch()` and `history()`** read the table once, when you call them, and never again.

A table that never changes, such as one made with `new_table`, is a fixed list: every verb
reads it once.

## One table driving another

A watchlist does not have to be typed in. Any live table with a `Symbol` column works, so one
dataset can choose the symbols of another. This streams the trades of whichever stocks are
the day's top gainers, re-checked every minute:

```python
gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
trades = m.stocks.trades(gainers.view("Symbol")).last_by()
```

As stocks join and leave the gainers list, `trades` subscribes and unsubscribes on its own.
Filters work the same way: ``watchlist.where("Symbol != `TSLA`")`` is a watchlist too.

News reads one request per ticker, so it follows a watchlist like any other poll. An article
about several tickers in the list appears once:

```python
from deephaven.time import dh_today

news = m.reference.news(symbols=watchlist, start=dh_today()).poll(300)
```

Each cycle sends one request per symbol. On a free plan, which allows five requests a minute,
a long watchlist cannot keep up with a short interval; the library logs a WARNING when cycles
run late.

## Spelling the symbols

Every row must hold Massive's exact ticker: `C:EURUSD`, not `EUR/USD`; `BRK.B`, not Yahoo's
`BRK-B`. The library never rewrites a watchlist, so what the table says is exactly what is
streamed and matched. When the current rows include one that is not Massive's spelling, the
verb raises `ValueError` and names it.

A row added later that is not Massive's spelling is skipped. The table keeps running, the
library logs the value once, and `m.streams()` lists it in the stream's `Error` column. Fix or
delete the row and the problem clears; a fixed row subscribes. A watchlist whose every row is
bad is refused with `ValueError`, rather than starting an empty stream. To fail the table on
a bad row instead, create the session with
`dm.MassiveConfig(noncanonical_symbols=dm.OnError.RAISE)`, or set the environment variable
`DEEPHAVEN_MASSIVE_NONCANONICAL_SYMBOLS=raise`.

To accept other spellings, convert the table first. `dm.canonicalize_symbols` returns a table
whose `Symbol` column holds Massive's tickers, converted row by row as rows arrive:

```python
pairs = input_table({"Symbol": dht.string}, key_cols="Symbol")
pairs.add(new_table([string_col("Symbol", ["EUR/USD", "GBP-USD"])]))

converted = dm.canonicalize_symbols(pairs, dm.AssetClass.FOREX, on_error=dm.OnError.LOG)
rates = m.forex.quotes(converted).last_by()
```

With `on_error=dm.OnError.LOG`, a value that cannot be converted becomes a null symbol, which
every dataset ignores, and is logged once. The default, `dm.OnError.RAISE`, fails the
converted table instead. For stocks spelled in another vendor's convention, pass
`convention=dm.SymbolConvention.YAHOO` (or `NASDAQ`, or `CMS`).

The converters also work on single values, in Python or inside a query string:
`dm.canonical_symbol_forex("EUR/USD")` returns `"C:EURUSD"`, and `help(dm.canonical_symbol)`
lists them all.

## Options

One WebSocket connection carries at most 1,000 options contracts. When a watchlist of
contracts grows past that, the extra contracts wait: `m.streams()` counts them in its
`Deferred` column and says so in `Error`, and they subscribe as room frees up. A fixed list
longer than the cap is refused with `ValueError`.

Next: [Plans and entitlement errors](plans.md).
