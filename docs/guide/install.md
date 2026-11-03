# Install and configure

Examples on this page assume `import deephaven_massive as dm` and `m = dm.MassiveSession()`.

The README's [install](../../README.md#install) covers the main path: a Python virtual
environment and `deephaven server`. This page covers platform details, the other ways to run
Deephaven, your keys, and the settings file.

<a id="platforms"></a>

## Platform details

The library runs wherever a pip-installed Deephaven server runs: macOS, Linux, and Windows 10 or
11 through WSL 2. Deephaven's [pip install guide](https://deephaven.io/core/docs/getting-started/pip-install/)
is the reference.

| Platform | What to do |
|---|---|
| Every platform | Install Java 17 or later (the latest long-term-support release is best, such as one from [Adoptium](https://adoptium.net)), and set `JAVA_HOME` to it. On macOS: `export JAVA_HOME=$(/usr/libexec/java_home)`. |
| Mac with an Apple M2 chip | Add `-Dprocess.info.system-info.enabled=false` to the server's `--jvm-args` |
| Windows (WSL 2) | WSL 2's default clock sync can jump the clock 10 to 20 seconds, which stalls live tables. Apply one of Deephaven's [clock-drift workarounds](https://deephaven.io/core/docs/reference/community-questions/wsl2-clock-drift/) first. |

Give the server at least 4 GB of memory (`-Xmx4g` in `--jvm-args`), on a computer with at least
8 GB.

<a id="docker"></a>

## Run Deephaven in Docker

Deephaven publishes a server image, `ghcr.io/deephaven/server`. You build an image from it with
the library installed, then run that image. Install [Docker](https://www.docker.com) first, and
the GitHub CLI, `gh`, signed in with `gh auth login` while the library's repository is private.

1. Download the newest release's wheel and write a Dockerfile, in a new folder:

   ```bash
   mkdir deephaven-massive-image && cd deephaven-massive-image
   TAG=$(gh release list --repo chipkent/deephaven-massive --limit 1 --json tagName --jq '.[0].tagName')
   gh release download "$TAG" --repo chipkent/deephaven-massive --pattern '*.whl'
   cat > Dockerfile <<'EOF'
   FROM ghcr.io/deephaven/server:latest
   COPY *.whl /tmp/
   RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl
   EOF
   ```

2. Build the image:

   ```bash
   docker build -t deephaven-massive .
   ```

   The first build downloads Deephaven's image, about 1.3 GB.

3. Run it, in a terminal where `MASSIVE_API_KEY` is set:

   ```bash
   docker run --rm -p 10000:10000 \
     -e MASSIVE_API_KEY \
     -e START_OPTS="-Xmx4g -Dauthentication.psk=choose-a-password" \
     deephaven-massive
   ```

   `-e MASSIVE_API_KEY` passes your key into the container without showing it. `START_OPTS`
   holds the server's Java options; `-Dauthentication.psk` sets the password of the Deephaven
   IDE.

4. Open <http://localhost:10000/ide/>, enter the password, and use the console as the README
   shows.

To pass a settings file into the container, see [settings file](#settings-file).

<a id="embedded"></a>

## Run Deephaven inside Python or Jupyter

Deephaven can run inside your own Python process, such as a script or a Jupyter notebook,
instead of as a separate server. Install the library as the README shows, in the environment
your script or notebook uses, then start the server before anything else:

```python skip-test
from deephaven_server import Server

server = Server(port=10000, jvm_args=["-Xmx4g", "-Dauthentication.psk=choose-a-password"])
server.start()

import deephaven_massive as dm

m = dm.MassiveSession()
week = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2025-06-02", "2025-06-06")
print(week.to_string())
```

This prints Apple's five daily bars of that week.

- Start the `Server` before importing `deephaven` or creating a session; otherwise Deephaven
  raises a `RuntimeError`.
- The server runs only as long as the Python process. Run a script with `python -i` to keep it
  running after the script's last line.
- To see live tables, open the Deephaven IDE at <http://localhost:10000/ide/> while the process
  runs.

<a id="api-key"></a>

## Your API key

Massive's dashboard shows your API key on its keys page. The library reads it from the
`MASSIVE_API_KEY` environment variable.

- Set it in the shell that starts the server, before you start it:
  `export MASSIVE_API_KEY="your-api-key"`.
- Never type it into the console: every user of the server can read the console's variables.
- In Docker, pass it with `-e MASSIVE_API_KEY`, as above.

A session with no key raises `no Massive API key` when you create it.

<a id="s3-keys"></a>

## S3 keys for flat files

Massive's paid plans include flat files: one compressed file per day holding every trade, quote,
or bar of a whole market, the fastest way to read a lot of history. They live in Amazon S3 and
have their own keys, shown beside your API key on Massive's dashboard.

Set both keys, or neither, in the shell that starts the server:

```bash
export MASSIVE_S3_ACCESS_KEY_ID="your-access-key-id"
export MASSIVE_S3_SECRET_ACCESS_KEY="your-secret-access-key"
```

With only one of them set, creating a session raises `ValueError`. With neither, `history()`
reads REST only: see [where history comes from](history.md#history-sources).

<a id="settings-file"></a>

## Settings file

Settings you use everywhere can live in one YAML file instead of in each script. The file sets
any of `dm.MassiveConfig`'s settings by name; `help(dm.MassiveConfig)` lists them all. A setting
the file leaves out keeps its default.

```yaml
# ~/massive/settings.yaml: settings for deephaven-massive.
# The API key and the S3 keys never go here: set MASSIVE_API_KEY (and, for flat files,
# MASSIVE_S3_ACCESS_KEY_ID and MASSIVE_S3_SECRET_ACCESS_KEY) in the environment.

# Subscriptions whose live data you accept from Massive's delayed feed (Starter, Developer).
delayed_subscriptions: [options]

# Subscriptions you hold on Massive's free Basic plan: five REST requests a minute.
rest_basic_subscriptions: [stocks]

# Seconds a live table rides out a dropped connection before it stops.
max_outage: 600

# How many REST requests or flat-file reads run at once.
max_concurrency: 4

# A row of a symbol table that is not Massive's exact ticker: raise, log, or ignore.
noncanonical_symbols: raise

# A crypto pair of a symbol table that Massive's crypto ticker list does not hold, in a
# stream, fetch(), or poll(): raise, log (leave it out), or ignore (the same, quietly).
unlisted_symbols: log

# A symbol Massive has no data for, in fetch(), history(), or poll(), or one a poll's endpoint
# cannot take an argument for: log (leave it out), ignore (the same, quietly), or raise (fail).
unavailable_symbols: log

# Pin the WebSocket host of an asset class; the library chooses when you leave this out.
websocket_hosts:
  options: delayed.massive.com
```

### Read the file

Nothing reads the file until you call `dm.MassiveConfig.from_file`: `dm.MassiveConfig()` and
`dm.MassiveSession()` use only the defaults and the environment's keys. Name the file one of
three ways:

| Where you run | How |
|---|---|
| A notebook or script | Pass the path: `dm.MassiveConfig.from_file("~/massive/settings.yaml")` |
| The Deephaven console | Before you start the server, set `export DEEPHAVEN_MASSIVE_CONFIG_FILE=~/massive/settings.yaml` in its shell. Then `dm.MassiveConfig.from_file()` needs no path. |
| Docker | Add `-v "$HOME/massive/settings.yaml:/massive/settings.yaml:ro"` and `-e DEEPHAVEN_MASSIVE_CONFIG_FILE=/massive/settings.yaml` to the `docker run` line. `:ro` mounts the file read-only. |

Code wins over the file, and the file wins over the defaults. To override one setting of the
file in code, use `dataclasses.replace`:

```python
import dataclasses

# A notebook or script names the file.
config = dm.MassiveConfig.from_file("~/massive/settings.yaml")
# A console or container started with DEEPHAVEN_MASSIVE_CONFIG_FILE needs no path.
config = dm.MassiveConfig.from_file()
# Code overrides the file.
m = dm.MassiveSession(dataclasses.replace(config, max_concurrency=2))
```

`m` is a session with the file's settings, except `max_concurrency`, which is 2.

### The file's rules

The library reads the file strictly, so a typo fails at once instead of being ignored:

- Every value is text until its setting converts it, so a ticker such as `ON` or `NO` stays a
  ticker.
- Words must be spelled exactly as shown: `raise`, not `Raise`.
- An unknown setting is refused, and the error names the closest one.
- A setting with nothing after its colon is refused: leave the setting out to get its default.
- The API key and the S3 keys are refused.
- YAML's tags (`!!python/...`), anchors and aliases (`&name`, `*name`), repeated keys, and
  several documents in one file are refused.

Every error names the file, the line, and the setting; an error about an environment variable
names the variable. With the [debug log](troubleshooting.md#the-log) on, an INFO line records
the file's name and the settings it set.
