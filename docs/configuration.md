# Configuration

Every `dm.MassiveConfig` setting is here: what it does, the values it takes, its default, and
when to change it. Keep your choices in a settings file, and every console and script loads the
same ones. [Every setting in one file](#example-settings-file) shows each setting in place;
change its plan lists to match your own plans.

<a id="write-a-settings-file"></a>

## Write a settings file

A settings file is YAML. Each line names one setting at the left margin, then a colon and its
value. Leave a setting out to keep its default, and start a comment with `#`. A value takes one
of three forms:

- A single value: a word, a number, or a URL, such as `raise`, `600`, or
  `https://api.massive.com`. Write a word in lower case, exactly as
  [Values and defaults](#settings-table) shows it.
- A list, in brackets: `[stocks, options]`, or `[]` for none. `delayed_subscriptions` and
  `rest_basic_subscriptions` are the two lists.
- A mapping, one `key: value` line per entry, indented under the setting's name.
  `websocket_hosts` is the only mapping. Its keys are asset classes, and its values are
  hostnames.

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

To change one setting of a file in code, replace it in the result:

```python
import dataclasses

import deephaven_massive as dm

config = dm.MassiveConfig.from_file("~/massive/settings.yaml")
m = dm.MassiveSession(dataclasses.replace(config, max_concurrency=4))
```

<a id="settings-in-the-environment"></a>

## Keep keys out of the file

A settings file can't hold your keys: loading a file that names one fails. Set them as
environment variables, or pass them in code as the `api_key`, `s3_access_key_id`, and
`s3_secret_access_key` arguments of `dm.MassiveConfig`. A proxy is an environment variable
too, `HTTPS_PROXY`, and so are your company's certificate authorities and the log.
[Environment variables](../README.md#environment-variables) names and describes each one.

<a id="settings-reference"></a>

## Settings

Most files start with your plans: `delayed_subscriptions` and `rest_basic_subscriptions`
match the session to what you've bought. A file that leaves both out treats every
subscription as bought on a real-time plan.

Each setting is a name in the settings file and an argument of `dm.MassiveConfig`. In a file,
write the values shown here. In code, pass the matching Python value: `{dm.Subscription.STOCKS}`
for `[stocks]`, `dm.OnError.RAISE` for `raise`, and
`{dm.AssetClass.INDICES: "business.massive.com"}` for the `websocket_hosts` mapping above.

<!-- The settings below are generated from MassiveConfig by scripts/render_reference.py. Edit the class's docstring, not this part of the page. -->

<a id="example-settings-file"></a>

### Every setting in one file

This file sets every setting, and a line marked `# the default` can be left out. Its other values suit an account with Stocks Starter, Options Developer, and Indices Business, and the free Basic plan of Currencies and Futures:

```yaml
# Plans that stream 15 minutes late: Stocks Starter and Options Developer.
delayed_subscriptions: [options, stocks]

# Subscriptions on the free Basic plan, held to five requests a minute.
rest_basic_subscriptions: [currencies, futures]

# Seconds a stream or poll() table rides out an outage before it fails; 0 fails at once.
max_outage: 600

# Seconds between the batches of streamed rows added to live tables.
flush_interval: 0.05  # the default

# Symbols in one history() call that make it read flat files, once your S3 keys are set.
flat_file_symbol_threshold: 50  # the default

# REST requests at a time, and separately flat files read at a time.
max_concurrency: 8  # the default

# A watchlist value that isn't Massive's exact ticker: log, ignore, or raise.
noncanonical_symbols: raise

# A watchlist crypto pair that Massive doesn't list: log, ignore, or raise.
unlisted_symbols: log  # the default

# A symbol in Massive's data that the library can't read: log, ignore, or raise.
unreadable_symbols: log  # the default

# A symbol Massive answers as not found: log, ignore, or raise.
unavailable_symbols: log  # the default

# A flat-file line that isn't a readable row: log, ignore, or raise.
malformed_rows: log  # the default

# A futures exchange whose flat files the plan lacks: log, ignore, or raise.
refused_venues: raise  # the default

# The WebSocket host per asset class: index values stream only from business.massive.com.
websocket_hosts:
  indices: business.massive.com

# Seconds Massive has to confirm a stream's channel before the library asks again.
subscription_confirmation_timeout: 5  # the default

# Shortest wait, in seconds, before reconnecting a dropped stream; at least 15.
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

A key inside the `websocket_hosts` mapping is named by its full path: `websocket_hosts.indices` is the `indices` key under `websocket_hosts`.

| Setting | What it's for | Values | Default |
|---|---|---|---|
| `delayed_subscriptions` | Lists the subscriptions whose plan gets Massive's delayed data, as Stocks Starter and Developer do, so their streams and snapshots work. | A list of any of `stocks`, `options`, `indices`, or `futures`, such as `[stocks]`, or `[]` for none | `[]` |
| `rest_basic_subscriptions` | Lists the subscriptions on Massive's free Basic plan, so their requests keep to Basic's five a minute from the start. | A list of any of `stocks`, `options`, `indices`, `currencies`, or `futures`, such as `[options]`, or `[]` for none | `[]` |
| `max_outage` | Sets how many seconds a stream or `poll()` table may stay cut off from Massive before it fails. | A number of seconds, `0` or more, such as `600` | `300` |
| `flush_interval` | Sets how many seconds pass between the batches of streamed rows added to live tables. | A positive number of seconds, such as `0.1` | `0.05` |
| `flat_file_symbol_threshold` | Sets how many symbols in a list make a `history()` call read Massive's flat files instead of a request per symbol, when both can serve it. | A positive whole number, such as `100` | `50` |
| `max_concurrency` | Sets how many REST requests the session runs at a time, and separately how many flat files it reads at a time. | A positive whole number, such as `4` | `8` |
| `noncanonical_symbols` | Decides what happens when a watchlist or other table you pass holds a value that isn't Massive's exact ticker or futures product code: `SPX` for `I:SPX`, say. | `log`, `ignore`, or `raise` | `log` |
| `unlisted_symbols` | Decides what happens when a watchlist or other table you pass holds a crypto pair that isn't on Massive's crypto ticker list, or one the library couldn't look up there. | `log`, `ignore`, or `raise` | `log` |
| `unreadable_symbols` | Decides what happens to a row of Massive's data whose symbol the library can't read. | `log`, `ignore`, or `raise` | `log` |
| `unavailable_symbols` | Decides what happens to a symbol Massive answers as not found, or one a `poll()` can't request with the arguments you gave it. | `log`, `ignore`, or `raise` | `log` |
| `malformed_rows` | Decides what happens to a line of a Massive flat file that isn't a readable row: it doesn't split into the header's fields, or it isn't UTF-8 text. | `log`, `ignore`, or `raise` | `log` |
| `refused_venues` | Decides what a futures `history()` does when Massive refuses the flat files of some of the exchanges in its `venues`, as it does for an exchange the plan doesn't include. A call's own `refused_venues` argument overrides it. | `log`, `ignore`, or `raise` | `raise` |
| `websocket_hosts.stocks` | Sets the WebSocket host stocks streams come from. | A lower-case hostname, such as `socket.massive.com`; a `delayed.` host exactly when `delayed_subscriptions` lists `stocks` | `delayed.massive.com` when `delayed_subscriptions` lists `stocks`, else `socket.massive.com` |
| `websocket_hosts.options` | Sets the WebSocket host options streams come from. | A lower-case hostname, such as `socket.massive.com`; a `delayed.` host exactly when `delayed_subscriptions` lists `options` | `delayed.massive.com` when `delayed_subscriptions` lists `options`, else `socket.massive.com` |
| `websocket_hosts.futures` | Sets the WebSocket host futures streams come from. | A lower-case hostname, such as `socket.massive.com`; a `delayed.` host exactly when `delayed_subscriptions` lists `futures` | `delayed.massive.com` when `delayed_subscriptions` lists `futures`, else `socket.massive.com` |
| `websocket_hosts.indices` | Sets the WebSocket host indices streams come from. | A lower-case hostname, such as `business.massive.com`; a `delayed.` host exactly when `delayed_subscriptions` lists `indices` | `delayed.massive.com` when `delayed_subscriptions` lists `indices`, else `socket.massive.com` |
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

List each subscription on a Starter or Developer plan of stocks, options, or futures,
or on Indices Starter. Those plans stream data 15 minutes late, or 10 for futures.
Leave out Advanced and Business plans, which are real-time. Never list
`currencies`, the one subscription for forex and crypto: Massive has
no delayed feed for it.

Leave a delayed subscription out, and a stream of its current data raises
`EntitlementError`. Its message gives the line of Python to add. A `fetch()` or
`poll()` table of it fails with the same message, and `fetch(wait=True)` raises it.
`history()` and other past data don't depend on this setting.

#### rest_basic_subscriptions

Each of the five subscriptions has a free Basic plan, and every subscription you
haven't bought is on it: list each of those. Leave out each one you
pay for, on any plan, Starter included: a listed subscription's requests are held to
five a minute.

A Basic subscription you leave out runs at full speed until Massive first refuses
one of its requests for the rate limit. From then on, it keeps to five a minute. The
limit is shared by every session in this server that uses your key.
`m.rate_limits()` shows each subscription's pace.

#### max_outage

Raise it to ride out longer outages, or set `0` to fail at the first one. A stream
counts the time its connection is down, and a `poll()` table counts the time its
polls keep failing or stop arriving. Until the limit passes, the table's
`m.streams()` row shows the problem in `Error`. A failed table stays failed, so call
the method again for a new one. See
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
the others wait their turn, and `m.streams()` shows them `LOADING`. A Basic
subscription still keeps to five requests a minute.

#### noncanonical_symbols

Set `raise` to catch a typo in a watchlist at once.

- `log` skips the row and logs a warning naming the value.
- `ignore` does the same with no warning: only the `DEBUG` log names it.
- `raise` raises `ValueError` for a bad row at the call, and fails a live
  table when one is added later.

Under `log` or `ignore`, a stream or `poll()` table lists the
skipped values in its `m.streams()` row's `Error` until you fix or delete the row. A
fixed row is read like a new one.

Under any setting, a table whose rows are all bad raises `ValueError` at the call. So
does a symbol passed in code, as text or a list, that isn't Massive's exact ticker.

#### unlisted_symbols

Set `raise` when a stream must not run without every pair you asked for.

- `log` skips the pair and logs a warning naming it.
- `ignore` does the same with no warning: only the `DEBUG` log names it.
- `raise` makes a stream fail with `MassiveError` at the call, and fails
  a `fetch()` or `poll()` table, or a live table when such a pair is added later.
  `fetch(wait=True)` raises it at the call.

It applies to streams and to calls for a pair's last trade or daily open and close. A
pair that isn't Massive's exact ticker, such as `BTC-USD` for `X:BTCUSD`, falls under
`noncanonical_symbols` instead.

Under `log` or `ignore`, a stream keeps its other pairs. A
stream of a live table lists the skipped ones in its `m.streams()` row's `Error`
until their rows are deleted. A `poll()` lists the pair in `Error`, and a `fetch()`
of a live table marks it `FAILED` in `m.symbol_reads()`. A stream whose pairs are all
skipped raises `MassiveError` at the call, and a `fetch()` whose pairs are all
skipped fails its table, or raises with `wait=True`.

#### unreadable_symbols

Leave it at `log` so the row stays and shows Massive's text, or set
`raise` to stop at the first one. Such a symbol is a library bug; to
report it, see
[Report a bug](troubleshooting.md#report-a-bug).

- `log` keeps the row with a null `Symbol`, puts Massive's text in a
  `RawSymbol` column, and logs an error naming it.
- `ignore` does the same without the error: only the `DEBUG` log names it.
- `raise` fails the call or stream that received it, naming the text, and
  the table has no `RawSymbol` column.

#### unavailable_symbols

Set `raise` when a missing symbol means your list is wrong and the read
should stop.

- `log` leaves the symbol out, keeps the others, and logs a warning
  naming it.
- `ignore` does the same with no warning: only the `DEBUG` log names it.
- `raise` fails a `poll()`, `fetch()`, or `history()` table with
  `MassiveError`, and makes `fetch(wait=True)` or `history(..., wait=True)` raise it.
  A `fetch()` or `history()` of a live table instead marks the symbol `FAILED` in
  `m.symbol_reads()` and reads the others.

Such a symbol is one Massive answers with HTTP 404, in any call. In a `poll()`, it's
also one the poll can't read, such as a crypto pair in an indicator poll with
`adjusted=False`. Under `log` or `ignore`, a `poll()` table
lists it in its `m.streams()` row's `Error` until it's readable again or removed, and
a `dm.PollMode.REPLACE` poll drops its rows.

#### malformed_rows

Set `raise` when a `history()` read from flat files must have every row
of each file it reads.

- `log` skips the line, keeps the file's other rows, and logs one warning
  per file with the count of such lines and the first one's row number. The warning
  never quotes the line.
- `ignore` does the same with no warning: only the `DEBUG` log names it.
- `raise` makes `history()` raise `MassiveError`, naming the file and the
  first such line's row. A `history()` of a live table instead marks every symbol
  of the read `FAILED` in `m.symbol_reads()`.

Only a `history()` that reads flat files meets such a line. Under any setting, a
file whose every line is malformed fails the read.

#### refused_venues

Set `log` when your plan covers only some exchanges and a read of
`dm.ALL` should return the contracts of the exchanges it does cover.

- `raise` makes `history()` raise `EntitlementError`, naming the
  exchange and `venues=`.
- `log` skips every file of each refused exchange over the whole range,
  logs one warning per exchange with Massive's status and text, and names the
  exchanges in the table's description. A table that follows a live symbol table
  names them in each symbol's `Transport` in `m.symbol_reads()` instead.
- `ignore` does the same with no warning: only the `DEBUG` log and the
  description or `m.symbol_reads()` name them.

It applies only when REST can't serve the call: `dm.ALL`, or
`source=dm.HistorySource.FLAT_FILES`. Otherwise `history()` reads REST when Massive
refuses the flat files. Under any setting, a call whose every exchange is refused
raises `EntitlementError`.

#### websocket_hosts

Set `indices` to `business.massive.com` to stream index values on a real-time Indices
plan. Massive's documentation lists no other host for them, delayed or real-time. Set
nothing else unless Massive tells you to.

Without an entry, a subscription listed in `delayed_subscriptions` streams from
`delayed.massive.com`, and every other from `socket.massive.com`. Massive documents
those two hosts for every other stream. An entry moves every stream of its asset
class, except Fair Market Value (FMV) streams, which always use
`business.massive.com`.

An entry must agree with `delayed_subscriptions`. Give a class a host whose name
starts with `delayed.` exactly when its subscription is listed there, and never give
one to forex or crypto. Write a hostname alone, with no scheme, port, or path.

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
