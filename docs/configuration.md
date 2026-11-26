# Configuration

Every `dm.MassiveConfig` setting a settings file holds is here: what it does, the values it
takes, its default, and when to change it. Keep your choices in a settings file, and every
console and script loads the same ones. [Every setting in one file](#example-settings-file)
shows each setting in place.

<a id="write-a-settings-file"></a>

## Write a settings file

A settings file is YAML. Each line names one setting at the left margin, then a colon and its
value. Leave a setting out to keep its default. A comment starts with `#`. A value takes one
of three forms:

- A single value: a word, a number, or a URL, such as `raise`, `600`, or
  `https://api.massive.com`. Write a word in lower case, exactly as
  [Values and defaults](#settings-table) shows it.
- A list, in brackets: `[stocks, options]`, or `[]` for none. `delayed_subscriptions` is the
  only list.
- A mapping, one `key: value` line per entry, indented under the setting's name. A short
  mapping also fits in braces on one line, such as `{stocks: 5}`, or `{}` for none. The
  mappings are:
  - `websocket_hosts`: an asset class to a hostname
  - `rest_requests_per_minute`: a subscription to its requests a minute
  - `on_error`: a cause to `raise`, `log`, or `ignore`

This file uses all three forms:

```yaml
# A list, a single value, and a mapping.
delayed_subscriptions: [stocks, options]
max_outage: 600
websocket_hosts:
  indices: business.massive.com
```

Save the file anywhere, such as `~/massive/settings.yaml`. A setting name or value the library
doesn't accept makes loading fail with a message that says what's wrong.

<a id="load-a-settings-file"></a>

## Load a settings file

`dm.MassiveConfig.from_file` reads the file, and `dm.MassiveSession` takes the result:

```python
import deephaven_massive as dm

config = dm.MassiveConfig.from_file("~/massive/settings.yaml")
m = dm.MassiveSession(config)
```

`from_file()` with no path reads the file that the `DEEPHAVEN_MASSIVE_CONFIG_FILE` environment
variable names. `dm.MassiveConfig()` never reads a settings file or that variable.

A setting in code wins over the file, and the file wins over the default. To change one setting
of a file in code, use `dataclasses.replace` on the result:

```python
import dataclasses

import deephaven_massive as dm

config = dm.MassiveConfig.from_file("~/massive/settings.yaml")
m = dm.MassiveSession(dataclasses.replace(config, max_concurrency=4))
```

<a id="settings-in-the-environment"></a>

## Keep keys out of the file

A settings file can't hold your keys: loading a file that names one fails. With a settings file,
set your keys as environment variables, since `from_file()` reads the API key from
`MASSIVE_API_KEY` and fails without it. Without a file, you can pass them in code as the
`api_key`, `s3_access_key_id`, and `s3_secret_access_key` arguments of `dm.MassiveConfig`.
[Environment variables](../README.md#environment-variables) names and describes each one.

<a id="settings-reference"></a>

## Settings

Most files start with your plans: `delayed_subscriptions` and `rest_requests_per_minute`
match the session to what you've bought. A file without `delayed_subscriptions` treats every
subscription as real-time.

Each setting is a name in the settings file and an argument of `dm.MassiveConfig`. In a file,
write the values shown here. In code, pass the matching Python value:

| In the file | In code |
|---|---|
| `delayed_subscriptions: [stocks]` | `delayed_subscriptions={dm.Subscription.STOCKS}`, a set |
| `on_error: {malformed_rows: raise}` | `on_error=dm.ErrorPolicy(malformed_rows=dm.OnError.RAISE)` |
| `websocket_hosts: {indices: business.massive.com}` | `websocket_hosts={dm.AssetClass.INDICES: "business.massive.com"}` |

<a id="on-error"></a>

### Bad symbols and rows

`on_error` decides what one bad symbol or row does. It holds one setting per kind of problem,
called a cause. By default the library skips the problem and keeps going, so one problem never
stops a table. The exception is `refused_venues`, which raises by default. Each cause takes one
of three values:

| Value | What it does |
|---|---|
| `log` | Skips the symbol or row, keeps the rest, and logs a warning for the first problem of each cause on each table. |
| `ignore` | Does the same, logging only at `DEBUG`. |
| `raise` | Fails the call, or the live table, that met the problem. |

`m.problems()` lists each problem a table met, with its cause and the setting that applied.

Whatever the setting, the library never publishes a row of Massive's data it can't read: one
whose symbol, time, or values don't convert to the table's columns.

So under `log` a result can be partial. Suppose this call gets 5 rows from Massive, and the
library can't read one of them:
`m.stocks.bars("AAPL", dm.Timespan.DAY).history("2026-10-26", "2026-10-30")`. The table gets 4
rows. `m.problems()` lists the fifth, and the log warns once.

Set every cause to `raise` when a call must return complete data:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
bars = m.stocks.bars("AAPL", dm.Timespan.DAY, on_error=dm.ErrorPolicy(dm.OnError.RAISE))
apple = bars.history("2026-10-26", "2026-10-30")
```

To do the same for every call, pass that policy to `MassiveConfig(on_error=...)`.

A policy can also set one cause, such as `dm.ErrorPolicy(noncanonical_symbols=dm.OnError.RAISE)`.
In a call's policy, a cause it leaves unset follows the session's. A cause the session's policy
leaves unset follows the default.

Under `raise`, a `fetch()` or `history()` with `wait=True` raises an error for a problem it
finds while it waits. A problem found after a call returns fails the table, and `Error` in
`m.streams()` says why.

<a id="failed-symbols"></a>

#### Symbols that fail

In a `fetch()`, `history()`, `poll()`, or stream of several symbols, one symbol's failure is
its own. By default, the other symbols keep their rows, and `m.problems()` lists the failed one
with its cause. The symbols can be a list or a watchlist, a live table of symbols.

Symbols that share one request or one flat file fail together when it fails with an error such
as HTTP 503. Snapshots read several symbols in one request, and a `history()` of many symbols
can read them all from the same flat files. When Massive refuses a shared request, see
[`on_error.refused_symbols`](#setting-details).

A symbol fails when:

- Massive refuses it.
- Its request keeps failing with a temporary error, such as HTTP 503. See
  [`max_outage`](#setting-details).
- In a `poll()`, or a `fetch()` or `history()` of a watchlist, the library can't read any row
  Massive sent for it.

By default, a symbol Massive has no data for doesn't fail. It gets no rows.

A failed symbol isn't read again. To read it again, delete its watchlist row and add it back.
For symbols you pass in code, call `fetch()`, `history()`, or `poll()` again. For a stream of
symbols in code, release each of its tables with `m.release()`, then call the method again.

When a symbol of a `poll()` fails, a `dm.PollMode.REPLACE` table drops its rows. A
`dm.PollMode.APPEND` or `dm.PollMode.UPSERT` table keeps them.

<a id="read-failures"></a>

#### Problems that fail the whole table

Whatever the setting, each of these problems fails the whole table of a `fetch()`, `history()`,
`poll()`, or stream:

- Massive rejects your API key.
- Massive serves current data delayed, and [`delayed_subscriptions`](#setting-details) doesn't
  list its subscription.
- Massive refuses a call for `dm.ALL`.
- Every symbol the table started with fails. A table whose watchlist was empty when you called
  the method never fails this way.
- A `fetch()` or `history()` of `dm.ALL`, or of symbols in code, gets rows from Massive, and
  the library can't read any of them.
- A stream's or `poll()` table's outage outlasts [`max_outage`](#setting-details).

<!-- The settings below are generated from MassiveConfig by scripts/render_reference.py. Edit the class's docstring, not this part of the page. -->

<a id="example-settings-file"></a>

### Every setting in one file

This file sets every setting, and a line marked `# the default` can be left out. Change the other values to match your account:

```yaml
# Subscriptions whose data Massive serves you delayed, 15 minutes late.
delayed_subscriptions: [options, stocks]

# REST requests a minute Massive allows each subscription it limits.
rest_requests_per_minute:
  currencies: 5
  futures: 5

# Seconds a live table waits out an outage before it fails, or 0 to fail at once.
max_outage: 600

# Seconds between the batches of streamed rows added to live tables.
flush_interval: 0.05  # the default

# Symbols in one history() call that make it read flat files, once your S3 keys are set.
flat_file_symbol_threshold: 50  # the default

# REST requests at a time, and separately flat files read at a time.
max_concurrency: 8  # the default

# What each kind of problem does: raise, log, or ignore.
on_error:
  # A watchlist value that isn't Massive's exact ticker.
  noncanonical_symbols: raise
  # A symbol in Massive's data that the library can't read.
  unreadable_symbols: log  # the default
  # A symbol Massive has no data for.
  unavailable_symbols: log  # the default
  # A symbol Massive refuses.
  refused_symbols: log  # the default
  # A row Massive sent for a symbol the call didn't ask for.
  unrequested_symbols: log  # the default
  # A row of Massive's data whose key the library can't read.
  unreadable_keys: log  # the default
  # A row of Massive's data with a value the library can't read.
  unconvertible_values: log  # the default
  # A flat-file line that isn't a readable row.
  malformed_rows: log  # the default
  # A futures ticker Massive streams that its lists don't hold.
  unlisted_contracts: log  # the default
  # A futures row Massive's schedules give no session for.
  unscheduled_products: log  # the default
  # A futures exchange you name that the plan lacks.
  refused_venues: raise  # the default

# The WebSocket host per asset class: index values stream only from business.massive.com.
websocket_hosts:
  indices: business.massive.com

# Seconds Massive has to confirm a stream's channel before the library asks again.
subscription_confirmation_timeout: 5  # the default

# Shortest wait, in seconds, before reconnecting a dropped stream: 15 or more.
reconnect_backoff: 30  # the default

# Longest wait, in seconds, before reconnecting a dropped stream.
max_reconnect_backoff: 120  # the default

# When another program with this key takes a stream's connection: fail, or reconnect.
connection_taken: fail  # the default

# Where REST requests go: change it only for another server.
rest_base_url: https://api.massive.com  # the default

# Where flat files are read from: change it only for another server.
s3_endpoint_url: https://files.massive.com  # the default

# The flat files' S3 bucket: change it only with s3_endpoint_url.
s3_bucket: flatfiles  # the default
```

<a id="settings-table"></a>

### Values and defaults

A key inside the `websocket_hosts` or `on_error` mapping is named by its full path: `websocket_hosts.indices` is the `indices` key under `websocket_hosts`.

| Setting | What it's for | Values | Default |
|---|---|---|---|
| `delayed_subscriptions` | Lists the subscriptions whose data Massive serves you delayed, so their streams use Massive's delayed feed and their snapshots accept its `DELAYED` label. | A list of any of `stocks`, `options`, `indices`, or `futures`, such as `[stocks]`, or `[]` for none | `[]` |
| `rest_requests_per_minute` | Gives each subscription's REST request limit on your plan, so its requests keep to it from the first one. | A mapping of any of `stocks`, `options`, `indices`, `currencies`, or `futures` to a positive whole number, such as `{options: 5}`, or `{}` for none | `{}` |
| `max_outage` | Sets how many seconds a live table waits out an outage or Massive's rate limits before it fails. | A number of seconds, `0` or more, such as `600` | `300` |
| `flush_interval` | Sets how many seconds pass between the batches of streamed rows added to live tables. | A positive number of seconds, such as `0.1` | `0.05` |
| `flat_file_symbol_threshold` | Sets how many symbols in a list make a `history()` call read Massive's flat files instead of a request per symbol, when both can serve it. | A positive whole number, such as `100` | `50` |
| `max_concurrency` | Sets how many REST requests the session runs at a time, and separately how many flat files it reads at a time. | A positive whole number, such as `4` | `8` |
| `on_error.noncanonical_symbols` | Decides what happens to a row of a watchlist or other table you pass whose value isn't Massive's exact ticker or futures product code, such as `SPX` for `I:SPX`. | `log`, `ignore`, or `raise` | `log` |
| `on_error.unreadable_symbols` | Decides whether a row of Massive's data whose symbol the library can't read as Massive's ticker fails the table; the row is left out either way. | `log`, `ignore`, or `raise` | `log` |
| `on_error.unavailable_symbols` | Decides what happens to a symbol Massive has no data for. | `log`, `ignore`, or `raise` | `log` |
| `on_error.refused_symbols` | Decides what happens to a symbol Massive refuses, usually because your plan doesn't serve it. | `log`, `ignore`, or `raise` | `log` |
| `on_error.unrequested_symbols` | Decides whether a row Massive returns for a symbol the call didn't ask for fails the table; the row is left out either way. | `log`, `ignore`, or `raise` | `log` |
| `on_error.unreadable_keys` | Decides whether a row of Massive's data whose key the library can't read fails the table; the row is left out either way. | `log`, `ignore`, or `raise` | `log` |
| `on_error.unconvertible_values` | Decides whether a row of Massive's data holding a value the library can't read as its column's type fails the table; the row is left out either way. | `log`, `ignore`, or `raise` | `log` |
| `on_error.malformed_rows` | Decides what happens to a line of a Massive flat file that isn't a readable row: it doesn't split into the header's fields, or it isn't UTF-8 text. | `log`, `ignore`, or `raise` | `log` |
| `on_error.unlisted_contracts` | Decides what happens to the rows of a futures ticker Massive streams that neither its futures snapshot nor its contracts list has held for 30 minutes. | `log`, `ignore`, or `raise` | `log` |
| `on_error.unscheduled_products` | Decides what happens to the rows of a futures product that Massive's schedules give no session for: none at all, or none that holds the row's time. | `log`, `ignore`, or `raise` | `log` |
| `on_error.refused_venues` | Decides what a futures `history()` does when Massive refuses the flat files of an exchange the call names in `venues`, as it does for an exchange the plan doesn't include. | `log`, `ignore`, or `raise` | `raise` |
| `websocket_hosts.stocks` | Sets the WebSocket host stocks streams come from. | A lower-case hostname, such as `socket.massive.com`; a `delayed.` host only when `delayed_subscriptions` lists `stocks` | `delayed.massive.com` when `delayed_subscriptions` lists `stocks`, else `socket.massive.com` |
| `websocket_hosts.options` | Sets the WebSocket host options streams come from. | A lower-case hostname, such as `socket.massive.com`; a `delayed.` host only when `delayed_subscriptions` lists `options` | `delayed.massive.com` when `delayed_subscriptions` lists `options`, else `socket.massive.com` |
| `websocket_hosts.futures` | Sets the WebSocket host futures streams come from. | A lower-case hostname, such as `socket.massive.com`; a `delayed.` host only when `delayed_subscriptions` lists `futures` | `delayed.massive.com` when `delayed_subscriptions` lists `futures`, else `socket.massive.com` |
| `websocket_hosts.indices` | Sets the WebSocket host indices streams come from. | A lower-case hostname, such as `business.massive.com`; a `delayed.` host only when `delayed_subscriptions` lists `indices` | `delayed.massive.com` when `delayed_subscriptions` lists `indices`, else `socket.massive.com` |
| `websocket_hosts.forex` | Sets the WebSocket host forex streams come from. | A lower-case hostname, such as `socket.massive.com`; never a `delayed.` host | `socket.massive.com` |
| `websocket_hosts.crypto` | Sets the WebSocket host crypto streams come from. | A lower-case hostname, such as `socket.massive.com`; never a `delayed.` host | `socket.massive.com` |
| `subscription_confirmation_timeout` | Sets how many seconds Massive has to confirm a stream's channel before the library asks again. | A positive number of seconds below 60, such as `10` | `5` |
| `reconnect_backoff` | Sets the shortest wait, in seconds, before reconnecting a dropped stream. | A number of seconds from 15 to `max_reconnect_backoff`, such as `60` | `30` |
| `max_reconnect_backoff` | Sets the longest wait, in seconds, before reconnecting a dropped stream. | A number of seconds, at least `reconnect_backoff`, such as `300` | `120` |
| `connection_taken` | Decides what a live table does when another connection with the same API key takes over its own. | `fail` or `reconnect` | `fail` |
| `rest_base_url` | Sets the URL REST requests go to. | An `https` URL of a host alone, or `http` for your own machine, such as `https://api.massive.com` | `https://api.massive.com` |
| `s3_endpoint_url` | Sets the URL flat files are read from. | An `http` or `https` URL, such as `https://files.massive.com` | `https://files.massive.com` |
| `s3_bucket` | Sets the S3 bucket flat files are read from. | A bucket name, such as `flatfiles` | `flatfiles` |

<a id="setting-details"></a>

### When to change each setting

`help(dm.MassiveConfig)` shows the same text in the console, written for Python code.

#### delayed_subscriptions

Massive labels such data `DELAYED` in its answers, and its delayed feed runs 15
minutes late, or 10 for futures. Never list
`currencies`, the one subscription for forex and crypto: Massive has
no delayed feed for it.

Leave a delayed subscription out, and a call for its current data fails with
`EntitlementError`. The message gives the line of Python to add. A stream and
`fetch(wait=True)` raise it at the call. A `fetch()` or `poll()` table fails with it.
A stream from a host you set in `websocket_hosts` is the exception. It opens, and its
`Recency` in `m.connections()` shows Massive's label, `DELAYED`.

`history()` and other past data don't depend on this setting.

#### rest_requests_per_minute

List each subscription Massive limits, with its limit in requests a minute, such as
5 for `stocks`. Leave out a subscription Massive doesn't limit.
Massive's article on its request limits gives each limit:
https://massive.com/knowledge-base/article/what-is-the-request-limit-for-massives-restful-apis.

Leave a limited subscription out, and the session learns its limit from Massive's
first rate-limit error, after a pause. `m.rate_limits()` shows the limit a session
learned. Copy it here to skip the pause next time. A limit you list wins over a
learned one.

Every session in this server that uses your key shares one limit per subscription.
When sessions list different limits, the lowest applies.

#### max_outage

Set a larger value when your tables should ride out longer outages, or `0` when a
table must fail at the first one. A live table fails once its outage lasts
`max_outage`:

- A stream's outage is the time its connection is down.
- A `poll()` table's outage is the time its polls keep failing or waiting for
  Massive's answer.

In a `poll()`, or a `fetch()` or `history()` of a watchlist, a symbol that keeps
failing with a temporary error isn't read again after `max_outage`. A rate limit
is one such error. `m.problems()` lists the symbol.

A failed table stays failed, so call the method again for a new one. See
[Live table messages](troubleshooting.md#live-table-messages).

#### flush_interval

A shorter interval shows each row sooner, and a longer one adds more rows in each
update.

#### flat_file_symbol_threshold

Lower it to read flat files for shorter lists, or raise it to keep them on requests.
Each flat file holds a whole day of data, and a flat-file read downloads all of
it, however few symbols you ask for.
A call for this many symbols or more reads flat files when all of these hold:

- `MASSIVE_S3_ACCESS_KEY_ID` and `MASSIVE_S3_SECRET_ACCESS_KEY` are set.
- Massive publishes flat files for the dataset.
- The call names no `source`.
- For bars that take `adjusted`, the call passes `adjusted=False`.

A `history()` of `dm.ALL` reads flat files under the same conditions. A `history()`
of a live table reads REST whenever REST can serve it.

#### max_concurrency

Raise it to run more at once, or lower it to open fewer connections. Both limits
cover all the session's calls together. It also sets how many downloads of
`fetch()` and `history()` run at a time, counting each symbol of a watchlist read:
the others wait their turn, and `m.streams()` shows them `LOADING`. A subscription
with a request limit still keeps to it.

#### on_error

Set a cause to `raise` when a table must not run past it.
`help(dm.ErrorPolicy)` lists the causes. By default each logs, except
`refused_venues`, which raises.

A dataset method's `on_error=` overrides this policy for one call. A call that
leaves it out, or passes None, uses this policy for every cause, and each cause
unset here falls back to `dm.ErrorPolicy.DEFAULTS`.

#### on_error.noncanonical_symbols

Set `raise` to catch a typo in a watchlist at once. A bad row added
later then fails the whole live table, even a watchlist read by `fetch()` or
`history()`. Under the other values, a row you fix is read like a new one.

This setting covers only tables. A symbol you pass in code must be Massive's exact
ticker, or the call raises `ValueError`. A table whose every row is bad raises
`ValueError` too, whatever this setting says.

#### on_error.unreadable_symbols

Set `raise` to fail the table at the first such row. Such a symbol is
Massive's data: `m.problems()` gives what Massive sent, for a report to Massive.

A listed crypto pair is unreadable too when its ticker isn't its two currencies
written together. Under `log` or `ignore` it gets no rows, and
`m.problems()` lists it. A watchlist read by `fetch()` also names it in `Error` in
`m.symbol_reads()`.

#### on_error.unavailable_symbols

Set `raise` when a missing symbol means your list is wrong. Under
`log` and `ignore`, the call skips the symbol and reads the
others, and `m.problems()` lists it. Massive has no data for a symbol when it
gives one of these answers:

- HTTP 404 for the symbol's request;
- a snapshot row whose `error` is `NOT_FOUND`;
- a crypto ticker list without the pair.

A symbol Massive answers with no rows and no error has no rows, whatever this
setting says.

#### on_error.refused_symbols

Set `raise` when a `fetch()`, `history()`, `poll()`, or stream must not
run without every symbol you named. Under `log` and `ignore`,
the symbol gets no rows, the others are read, and `m.problems()` lists it with
Massive's answer.

Massive can refuse a request for several symbols at once, such as a snapshot of a
list. A symbol your plan serves can then be listed as refused too. To find which
symbols Massive refuses:

1. Read each listed symbol by itself, such as
   `m.stocks.snapshots("AAPL").fetch(wait=True)`. The call raises
   `dm.EntitlementError` when Massive refuses that symbol.
2. Remove the symbols whose call raised.
3. Call the method again. For a watchlist, delete the rows of the other listed
   symbols and add them back.

#### on_error.unrequested_symbols

Set `raise` to fail the call or table when Massive sends a row of a
symbol the call didn't ask for. `m.problems()` lists each such row with its symbol
and count. Massive sends such rows in these cases:

- `m.options.open_interest_by_underlying()` asked for `SPX` gets rows of `I:SPX`.
- A dataset whose rows list their tickers, such as `m.partners.benzinga.news()`,
  sends a row that lists none of the tickers the call named.
- An options dataset sends rows of another underlying. For example,
  `m.options.contracts("I:DJT")` leaves out the stock `DJT`'s contracts.
- A futures read by a table of product codes gets a row of another code.

#### on_error.unreadable_keys

A key can't be read when it is one of these:

- the row's symbol, its time, or another column its dataset needs, missing or of
  the wrong type;
- a time of 0, or one outside 1990 to 2100;
- a `poll()` table's key column, null or NaN, which would merge unrelated rows.

A symbol that is present but isn't Massive's ticker is `unreadable_symbols`. Set
`raise` to fail the table at the first such row. Such a row is a sign
that Massive's format changed.

#### on_error.unconvertible_values

A value the library can't read is one of these:

- a value of another type, or a whole number that a `double` column would round;
- a time outside 1990 to 2100 in any column but the row's own time;
- the start of a bar of a day or longer stamped at a time Massive isn't known to
  use.

In those other columns a time of 0 is a null, as Massive means it, and the row is
kept.

Set `raise` when a table must fail rather than leave out a row with a
value it can't read.

#### on_error.malformed_rows

Set `raise` when a `history()` read from flat files must have every row
of each file it reads.

- `log` skips the line, keeps the file's other rows, and logs one
  warning. `m.problems()` gives each file's count of such lines and the first
  one's row number, never the line itself.
- `ignore` does the same, logging only at `DEBUG`.
- `raise` makes `history()` raise `MassiveError`, naming the file and the
  first such line's row. A `history()` of a live table fails the table instead.

Only a `history()` that reads flat files meets such a line. Under any setting, a
file whose every line is malformed fails the read.

#### on_error.unlisted_contracts

A futures stream dates each row from its product's trading schedule, which it finds
through those lists, and holds the rows until then. Massive's lists sometimes lag
its stream, so a ticker missing from both waits and is asked about again, at 2, 8,
and 30 seconds and then every minute. After 30 minutes its rows are left out,
then and later, until a list holds it.

- `raise` fails the stream at that point.
- `log` leaves the rows out and logs one warning per stream.
- `ignore` does the same, logging only at `DEBUG`.

`m.problems()` lists the rows left out under `unlisted_contracts`.

#### on_error.unscheduled_products

A futures stream dates each row from its product's trading schedule. A row no
session holds waits while the library loads the schedule again. If Massive's answer
still has no session for it, the library can't date the row, so it leaves the row
out. A product with no session at all is asked about again each day, and once
Massive publishes its sessions, its rows are dated and published again.

- `raise` fails the stream at the first row left out.
- `log` leaves the rows out and logs one warning per stream.
- `ignore` does the same, logging only at `DEBUG`.

`m.problems()` lists the rows left out under `unscheduled_products`.

#### on_error.refused_venues

It applies only to exchanges a call names. A call that leaves `venues` out reads
every exchange the plan allows: where only flat files can serve it, it skips each
one Massive refuses with a warning, whatever this setting says. Set
`log` when your calls name exchanges your plan may not cover and should
read the ones it does.

- `raise` makes `history()` raise `EntitlementError`, naming the
  exchange and the setting.
- `log` skips every file of each refused exchange over the whole range and
  logs one warning. `m.problems()` lists the exchanges with Massive's status and text.
- `ignore` does the same, logging only at `DEBUG`.

It applies only when REST can't serve the call: `dm.ALL`, or
`source=dm.HistorySource.FLAT_FILES`. Otherwise `history()` reads REST when Massive
refuses the flat files. Under any setting, a call whose every exchange is refused
raises `EntitlementError`.

#### websocket_hosts

Set `indices` to `business.massive.com` to stream index values. Massive's
documentation lists no other host for them, delayed or real-time. Set nothing
else unless Massive tells you to.

A host you set moves every stream of its asset class. Fair market value streams
are the exception: they always use `business.massive.com`.

The host you set is used as given, whatever Massive's label says. When Massive
doesn't serve your key there, the stream fails with Massive's refusal.

`Recency` in `m.connections()` shows how Massive labels the data it serves you
there:

- `DELAYED`: Massive labels the data `DELAYED`.
- `REALTIME`: Massive labels the data `REAL-TIME`.
- `UNKNOWN`: Massive gave no label the library knows.

Write the lower-case hostname alone: `business.massive.com`, not
`wss://business.massive.com/indices` or `Business.massive.com`. The table above
says when a class may take a `delayed.` host.

#### subscription_confirmation_timeout

Raise it on a slow network, where confirmations arrive late. It must be below 60
seconds.

#### reconnect_backoff

Lower it to reconnect sooner after a brief drop. It must be at least 15 seconds,
which limits reconnect churn, and at most `max_reconnect_backoff`.

Each failure in a row doubles the wait, up to `max_reconnect_backoff`, and the count
resets once a connection stays up for 60 seconds. A failure no retry can fix, such as a
rejected key, isn't retried.

#### max_reconnect_backoff

Lower it to retry more often during a long outage, or raise it to retry less often.
It must be at least `reconnect_backoff`.

#### connection_taken

Massive allows your account one WebSocket connection per asset class, and keeps the
newest. When another program, or a session with different streaming settings,
connects with the same key, Massive closes this session's connection.

- `fail` fails the live tables at once, with an error that names
  the time and the fix.
- `reconnect` reconnects after the reconnect backoff, which takes
  the connection back. If the other program does the same, the two take turns, and
  each loses data while the other holds the connection.

Sessions with the same key and streaming settings share one connection whatever
this setting. See
[Live table messages](troubleshooting.md#live-table-messages).

#### rest_base_url

Change it only to reach a server other than Massive's, such as a local test server.
The config refuses a URL with a path, a query, a fragment, a user and password, or
port 0. It also refuses an `http` URL of a host that isn't on your own machine. The
error shows `***` in place of the URL's query and password.

#### s3_endpoint_url

Change it only to reach a server other than Massive's.

#### s3_bucket

Change it only with `s3_endpoint_url`.

<!-- End of the generated settings. -->
