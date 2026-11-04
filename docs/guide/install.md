# Install and configure

The README's [install](../../README.md#install) starts a Deephaven server from a terminal on
macOS or Linux. Its [configuration](../../README.md#configuration) lists every environment
variable the library reads. If your setup differs, start with the section that matches it:

| Your setup | Start with |
|---|---|
| Windows, or a Mac with an Apple M2 chip | [Check your platform](#platforms) |
| Deephaven in a container | [Run Deephaven in Docker](#docker) |
| A Python script or a Jupyter notebook | [Run Deephaven inside Python](#embedded) |
| Years of history for many symbols | [Add the S3 keys for flat files](#s3-keys) |
| The same settings in every script | [Keep your settings in a file](#settings-file) |

<a id="platforms"></a>

## Check your platform

The library runs wherever Deephaven's pip-installed server runs: macOS, Linux, and Windows 10 or
11 through WSL 2. Give the server at least 4 GB of memory, on a computer with at least 8 GB. Some
platforms need one more step before the server starts:

| Platform | What to do |
|---|---|
| Every platform | Install Java 17 or later, such as the latest long-term-support release from [Adoptium](https://adoptium.net), and set `JAVA_HOME` to it. On macOS, run `export JAVA_HOME=$(/usr/libexec/java_home)`. |
| Mac with an Apple M2 chip | Add `-Dprocess.info.system-info.enabled=false` to the server's `--jvm-args`. |
| Windows, through WSL 2 | Apply one of Deephaven's [clock-drift workarounds](https://deephaven.io/core/docs/reference/community-questions/wsl2-clock-drift/). WSL 2 can jump its clock 10 to 20 seconds, and live tables stall when it does. |

The server's memory is its `-Xmx` Java option, as in `--jvm-args "-Xmx4g"`. Deephaven's
[pip install guide](https://deephaven.io/core/docs/getting-started/pip-install/) has the rest of
the details for each platform.

<a id="docker"></a>

## Run Deephaven in Docker

Deephaven publishes a server image, `ghcr.io/deephaven/server`. You build your own image from it
with the library installed, then run that image with your API key. You need
[Docker](https://www.docker.com), and, while the library's repository is private, the GitHub CLI
signed in with `gh auth login`.

1. Download the newest release of the library and write a Dockerfile, in a new folder:

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

   The folder now holds the library's wheel file and a three-line `Dockerfile`.

2. Build the image:

   ```bash
   docker build -t deephaven-massive .
   ```

   The first build downloads Deephaven's image, about 1.3 GB, so it takes a few minutes.

3. Run the image, in a terminal where `MASSIVE_API_KEY` is set:

   ```bash
   docker run --rm -p 10000:10000 \
     -e MASSIVE_API_KEY \
     -e START_OPTS="-Xmx4g -Dauthentication.psk=choose-a-password" \
     deephaven-massive
   ```

   `-e MASSIVE_API_KEY` copies your key from the terminal into the container, so the key never
   appears in the command. `START_OPTS` holds the server's Java options. Its
   `-Dauthentication.psk` sets the password you'll type into the Deephaven IDE.

4. Open <http://localhost:10000/ide/> in your browser and enter that password.

The Deephaven IDE opens with its console, where you use the library as the README shows. To
give the container a settings file as well, see [read the file](#read-the-settings-file).

<a id="embedded"></a>

## Run Deephaven inside Python or Jupyter

Deephaven can run inside your own Python process, such as a script or a Jupyter notebook, so
you don't start a separate server. Install the library as the README shows, into the
environment your script or notebook uses. Set `MASSIVE_API_KEY` in the terminal that starts
Python or Jupyter, because the session reads it from the process's environment.

This script starts the server, then reads Apple's daily bars for one week:

```python skip-test
from deephaven_server import Server

server = Server(port=10000, jvm_args=["-Xmx4g", "-Dauthentication.psk=choose-a-password"])
server.start()

import deephaven_massive as dm

m = dm.MassiveSession()
week = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2025-06-02", "2025-06-06")
print(week.to_string())
```

It prints five rows, one daily bar for each trading day from June 2 to June 6, 2025. It works on
every Stocks plan, the free one included.

Start the server before you import `deephaven` or create a session, because both need the
running server. The server lives only as long as the Python process. Run a script with
`python -i` to keep the server running after the script's last line, and open the Deephaven IDE
at <http://localhost:10000/ide/> to watch its live tables.

<a id="api-key"></a>

## Give the server your API key

Massive's dashboard shows your API key on its keys page. The library reads it from the
`MASSIVE_API_KEY` environment variable when you create a session, so the key has to be in the
environment of the process that runs Deephaven:

| How you run Deephaven | Where to set the key |
|---|---|
| `deephaven server` | `export MASSIVE_API_KEY="your-api-key"` in the terminal, before you start the server |
| Docker | The same `export`, then `-e MASSIVE_API_KEY` on the `docker run` line |
| Python or Jupyter | The same `export`, in the terminal that starts Python or Jupyter |

A key kept in the environment stays out of the scripts and notebooks you save and share. A
running server doesn't see a key you set later, so restart it after you change one. If the
session finds no key, `dm.MassiveSession()` raises `dm.AuthenticationError` with the message
`no Massive API key`.

<a id="s3-keys"></a>

## Add the S3 keys for flat files

Massive's paid plans include flat files. Each one is a compressed file that holds a whole
market's trades, quotes, or bars for one day, and together they're the fastest way to read a
lot of history. They're stored in Amazon S3 and have their own pair of keys, which Massive's
dashboard shows next to your API key.

Set both keys in the terminal that starts the server, before you start it:

```bash
export MASSIVE_S3_ACCESS_KEY_ID="your-access-key-id"
export MASSIVE_S3_SECRET_ACCESS_KEY="your-secret-access-key"
```

Once both are set, `history()` reads flat files whenever they're the better source (see
[where history comes from](history.md#history-sources)). Without them, it reads Massive's REST
API only. If you set one key and not the other, creating a session raises `ValueError`, so set
both or neither.

<a id="settings-file"></a>

## Keep your settings in a file

A settings file holds the `dm.MassiveConfig` settings you'd otherwise repeat in every script.
It's a YAML file that sets any setting by name, and a setting it leaves out keeps its default.
`help(dm.MassiveConfig)` lists every setting and what it does.

This example sets one of each kind of value, with a comment on what each line does:

```yaml
# ~/massive/settings.yaml: settings for deephaven-massive.
# Your API key and S3 keys never go here. Set them in the environment.

# Subscriptions whose live data you accept from Massive's delayed feed (Starter, Developer).
delayed_subscriptions: [options]

# Subscriptions you hold on Massive's free Basic plan, which allows five REST requests a minute.
rest_basic_subscriptions: [stocks]

# Seconds a live table waits out a dropped connection before it stops.
max_outage: 600

# How many REST requests or flat-file reads run at once.
max_concurrency: 4

# A row of a symbol table that isn't spelled as Massive spells the ticker: raise, log, or ignore.
noncanonical_symbols: raise

# A crypto pair that Massive's list of crypto tickers doesn't hold: log, ignore, or raise.
unlisted_symbols: log

# A symbol Massive has no data for, or one a poll can't read with your arguments.
unavailable_symbols: log

# The WebSocket host for one asset class. Leave this out, and the library chooses.
websocket_hosts:
  options: delayed.massive.com
```

<a id="read-the-settings-file"></a>

### Read the file

The file takes effect only when you call `dm.MassiveConfig.from_file()`. A plain
`dm.MassiveConfig()` or `dm.MassiveSession()` uses the defaults and your environment's keys, and
never looks for a file. You tell `from_file()` where the file is in one of three ways:

| Where you run | How to name the file |
|---|---|
| A notebook or script | Pass the path: `dm.MassiveConfig.from_file("~/massive/settings.yaml")`. |
| The Deephaven console | Set `export DEEPHAVEN_MASSIVE_CONFIG_FILE=~/massive/settings.yaml` in the terminal before you start the server. Then call `dm.MassiveConfig.from_file()` with no path. |
| Docker | Add `-v "$HOME/massive/settings.yaml:/massive/settings.yaml:ro"` and `-e DEEPHAVEN_MASSIVE_CONFIG_FILE=/massive/settings.yaml` to the `docker run` line. `:ro` mounts the file read-only. |

Settings in code win over the file, and the file wins over the defaults. To change one setting
of the file for a single session, copy the configuration with `dataclasses.replace`:

```python
import dataclasses

import deephaven_massive as dm

# In a notebook or script, name the file.
config = dm.MassiveConfig.from_file("~/massive/settings.yaml")

# In a console or container started with DEEPHAVEN_MASSIVE_CONFIG_FILE, leave the path out.
config = dm.MassiveConfig.from_file()

# Change one setting of the file for this session.
m = dm.MassiveSession(dataclasses.replace(config, max_concurrency=2))
```

`m` is a session with every setting from the file, except `max_concurrency`, which is 2.

### Write the file so it reads

The library reads the file strictly, so a typo stops `from_file()` with an error that names the
file, the line, and the setting. These rules catch most mistakes:

- Every value is text until its setting converts it, so a ticker such as `ON` or `NO` stays a
  ticker.
- Words are spelled exactly as shown, so `raise` works and `Raise` doesn't.
- An unknown setting is refused, and the error suggests the closest name.
- A setting with nothing after its colon is refused. Leave the setting out to get its default.
- Your API key and S3 keys are refused, and the error names the environment variable to use.
- YAML's tags (`!!python/...`), anchors and aliases (`&name`, `*name`), repeated settings, and
  several documents in one file are refused.

With the [debug log](troubleshooting.md#the-log) on, an INFO line records the file's name and
the settings it set.
