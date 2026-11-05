# Configuration

A settings file holds your `dm.MassiveConfig` settings, so every console and script uses the same
ones. It can't hold your API and S3 keys. Set them in the environment, as
[Environment variables](../README.md#environment-variables) shows.

<a id="write-a-settings-file"></a>

## Write a settings file

A settings file maps each setting's name to its value. Leave a setting out to keep its default.
This file is for an account with Stocks Starter and Options Basic:

```yaml
# Stocks Starter streams Massive's delayed feed, 15 minutes late.
delayed_subscriptions: [stocks]
# Options Basic allows five requests a minute.
rest_basic_subscriptions: [options]
# A stream or poll() table waits through 10 minutes of outage before it fails.
max_outage: 600
# A watchlist row that isn't Massive's exact ticker is an error, not a skipped row.
noncanonical_symbols: raise
```

Save it anywhere, such as `~/massive/settings.yaml`.

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

<a id="settings-reference"></a>

## Settings

Each setting is a name in the settings file and an argument of `dm.MassiveConfig`. In a file,
write the values shown here. In code, pass the matching Python value, such as
`{dm.Subscription.STOCKS}` for `[stocks]` or `dm.OnError.RAISE` for `raise`.

<!-- The settings below are generated from MassiveConfig by scripts/render_reference.py. Edit the class's docstring, not this part of the page. -->

`help(dm.MassiveConfig)` gives each setting's full behavior.

| Setting | What it's for | Values | Default |
|---|---|---|---|
| `delayed_subscriptions` | Lists the subscriptions whose plan gets Massive's delayed data, as Stocks Starter and Developer do, so their streams and current data work. | A list of any of `stocks`, `options`, `indices`, or `futures`, such as `[stocks]`, or `[]` for none | `[]` |
| `rest_basic_subscriptions` | Lists the subscriptions on Massive's free Basic plan, so their requests keep to Basic's five a minute from the start. | A list of any of `stocks`, `options`, `indices`, `currencies`, or `futures`, such as `[options]`, or `[]` for none | `[]` |
| `max_outage` | Sets how many seconds a stream or `poll()` table may stay cut off from Massive before it fails. | A number of seconds, `0` or more, such as `600` | `300` |
| `flush_interval` | Sets how many seconds pass between the batches of streamed rows added to live tables. | A positive number of seconds, such as `0.1` | `0.05` |
| `flat_file_symbol_threshold` | Sets how many symbols in a list make a `history()` call read Massive's flat files instead of a request per symbol, when both can serve it. | A positive whole number, such as `100` | `50` |
| `max_concurrency` | Sets how many REST requests the session runs at a time, and separately how many flat files it reads at a time. | A positive whole number, such as `4` | `8` |
| `noncanonical_symbols` | Decides what happens to a row of a table you pass, such as a watchlist, whose value isn't Massive's exact ticker or code for the dataset. | `log`, `ignore`, or `raise` | `log` |
| `unlisted_symbols` | Decides what happens to a crypto pair in a table you pass that the library can't match to Massive's crypto ticker list. | `log`, `ignore`, or `raise` | `log` |
| `unreadable_symbols` | Decides what happens to a row of Massive's data whose symbol the library can't read. | `log`, `ignore`, or `raise` | `log` |
| `unavailable_symbols` | Decides what happens to a symbol Massive answers as not found, or one a `poll()` can't ask for with its arguments. | `log`, `ignore`, or `raise` | `log` |
| `websocket_hosts` | Streams an asset class from a WebSocket host you name instead of the one the library picks, such as `business.massive.com` for index values. | A mapping from any of `stocks`, `options`, `futures`, `indices`, `forex`, or `crypto` to a lower-case hostname, such as `{indices: business.massive.com}`. A `delayed.` host goes only with a subscription in `delayed_subscriptions`, and such a subscription takes only a `delayed.` host | `{}` |
| `subscription_confirmation_timeout` | Sets how many seconds Massive has to confirm a stream's channel before the library asks again. Raise it on a slow network. | A positive number of seconds below 60, such as `10` | `5` |
| `reconnect_backoff` | Sets the shortest wait, in seconds, before reconnecting a dropped stream. | A number of seconds from 15 to `max_reconnect_backoff`, such as `60` | `30` |
| `max_reconnect_backoff` | Sets the longest wait, in seconds, before reconnecting a dropped stream. | A number of seconds, at least `reconnect_backoff`, such as `300` | `120` |
| `rest_base_url` | Sets the address REST requests go to. | An `https` URL of a host alone, or `http` for your own machine, such as `https://api.massive.com` | `https://api.massive.com` |
| `s3_endpoint_url` | Sets the address flat files are read from. | An `http` or `https` URL, such as `https://files.massive.com` | `https://files.massive.com` |
| `s3_bucket` | Sets the S3 bucket flat files are read from. | A bucket name, such as `flatfiles` | `flatfiles` |

<!-- End of the generated settings. -->
