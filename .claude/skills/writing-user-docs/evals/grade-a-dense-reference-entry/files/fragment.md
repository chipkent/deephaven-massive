#### unlisted_symbols

Set `raise` when a stream must not run without every pair you asked for.

- `log` skips the pair and logs a warning naming it.
- `ignore` does the same with no warning: only the `DEBUG` log names it.
- `raise` makes a stream fail with `MassiveError` at the call, and fails
  a `fetch()` or `poll()` table, or a live table when such a pair is added later.
  `fetch(wait=True)` raises it at the call.

It applies to streams and to calls for a pair's last trade or daily open and close. A
pair that isn't Massive's exact ticker, such as `BTC-USD` for `X:BTCUSD`, falls under
`noncanonical_symbols` instead, or raises when passed in code.

Under `log` or `ignore`, a pair the list doesn't hold is one
Massive has no data for: a stream keeps its other pairs and lists the skipped ones
in its `m.streams()` row's `Error`, until a live table's rows are deleted; a
`poll()` lists the pair in `Error`; and a `fetch()` of a live table shows it `DONE`
with 0 rows in `m.symbol_reads()`. A stream or `fetch()` whose pairs are all skipped
opens and gives no rows. A pair the library couldn't look up, because the list
couldn't be read, is not skipped when you pass it in code: a stream raises that
read's error at the call. From a table, it stays skipped in a stream, which looks it
up again when its row is added again, and a `fetch()` of a live table marks it
`FAILED`. When no pair of a
stream or static `fetch()` can be split because the list can't be read, a stream
raises that read's error at the call, and a `fetch()` fails its table, or raises
with `wait=True`.

## Symptoms

| Symptom | Cause | Fix |
|---|---|---|
| A symbol on your watchlist never appears | The table skips a row that isn't Massive's exact ticker and lists it in `Error`. A crypto stream does the same with a pair Massive doesn't list, and a `poll()` or `fetch()` gives that pair no rows. In a watchlist made by `dm.canonicalize_symbols`, a row that didn't convert has a null `Symbol`: every dataset skips it, and `Error` doesn't list it. | Fix or delete the row. For a converted watchlist, `watchlist.where("isNull(Symbol)")` lists the rows that didn't convert, and the log names their values. Fix them in the table you converted. [`symbols`](datasets/README.md#symbols) shows Massive's spelling for each asset class, and [Symbols from another source](../README.md#convert-symbols) shows how to convert a table of Yahoo tickers. |
