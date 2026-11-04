# Install and configure

Come here when your setup differs from the README's install: Windows, an Apple M2 Mac, Docker,
or Deephaven inside a Python script or notebook. Come here too for years of history through flat
files, or the same settings in every script. The README's
[install](../../README.md#install) covers the terminal setup, and its
[configuration](../../README.md#configuration) lists every environment variable the library
reads. Find your setup and start with its section:

| Your setup | Start with |
|---|---|
| Windows, or a Mac with an Apple M2 chip | [Check your platform](#platforms) |
| Docker, or a Python script or notebook | [Give the server your API key](#api-key), then [run Deephaven in Docker](#docker) or [inside Python or Jupyter](#embedded) |
| Years of history for many symbols | [Add the S3 keys for flat files](#s3-keys) |
| The same settings in every script | [Keep your settings in a file](#settings-file) |

<a id="platforms"></a>

## Check your platform

The library runs wherever Deephaven's pip-installed server runs: macOS, Linux, and Windows 10 or
11 through WSL 2. Give the server at least 4 GB of memory with `--jvm-args "-Xmx4g"`, on a
computer with at least 8 GB. Before the server starts, every platform needs Java, and two need
one more step:

| Platform | What to do |
|---|---|
| Every platform | Install Java 17 or later, such as the latest long-term-support release from [Adoptium](https://adoptium.net), and set `JAVA_HOME` to it. On macOS, run `export JAVA_HOME=$(/usr/libexec/java_home)`. |
| Mac with an Apple M2 chip | Add `-Dprocess.info.system-info.enabled=false` to the server's `--jvm-args`. |
| Windows, through WSL 2 | Apply one of Deephaven's [clock-drift workarounds](https://deephaven.io/core/docs/reference/community-questions/wsl2-clock-drift/). WSL 2's clock can jump, and live tables stall when it does. |

For the rest of the details on each platform, see Deephaven's
[pip install guide](https://deephaven.io/core/docs/getting-started/pip-install/). It covers the
same server the library runs in, so everything there applies here too.

<a id="api-key"></a>

## Give the server your API key

Set `MASSIVE_API_KEY` in the terminal that starts Deephaven, before you start it. The library
reads the key when you create a session. Massive's dashboard shows the key on its keys page.
Where you set it depends on how you run Deephaven:

| How you run Deephaven | Where to set the key |
|---|---|
| `deephaven server` | `export MASSIVE_API_KEY="your-api-key"` in the terminal, before you start the server |
| Docker | The same `export`, then `-e MASSIVE_API_KEY` on the `docker run` line |
| Python or Jupyter | The same `export`, in the terminal that starts Python or Jupyter |

A key kept in the environment stays out of the scripts and notebooks you save and share. A
running server doesn't see a key you set later, so restart the server after you change one. If
the session finds no key, `dm.MassiveSession()` raises `dm.AuthenticationError` with the message
`no Massive API key`.

<a id="docker"></a>

## Run Deephaven in Docker

Deephaven publishes a server image, `ghcr.io/deephaven/server`. You build your own image from it
with the library installed, then run that image with your API key. You need
[Docker](https://www.docker.com) and, while the library's repository is private, the GitHub CLI,
signed in with `gh auth login`.

1. Download the newest release of the library into a new folder:

   ```bash
   mkdir deephaven-massive-image && cd deephaven-massive-image
   TAG=$(gh release list --repo chipkent/deephaven-massive --limit 1 --json tagName --jq '.[0].tagName')
   gh release download "$TAG" --repo chipkent/deephaven-massive --pattern '*.whl'
   ```

   The folder now holds the library's wheel file.

2. Write a Dockerfile next to it:

   ```bash
   cat > Dockerfile <<'EOF'
   FROM ghcr.io/deephaven/server:latest
   COPY *.whl /tmp/
   RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl
   EOF
   ```

   The Dockerfile starts from Deephaven's image and installs the wheel into it.

3. Build the image:

   ```bash
   docker build -t deephaven-massive .
   ```

   The first build downloads Deephaven's image, about 1.3 GB, and takes a few minutes.

4. Run the image, in a terminal where you've set `MASSIVE_API_KEY`:

   ```bash
   docker run --rm -p 10000:10000 \
     -e MASSIVE_API_KEY \
     -e START_OPTS="-Xmx4g -Dauthentication.psk=choose-a-password" \
     deephaven-massive
   ```

   `-e MASSIVE_API_KEY` copies your key from the terminal into the Docker container, and the key
   never appears in the command. `START_OPTS` holds the server's Java options. Its
   `-Dauthentication.psk` sets the password you'll type into the Deephaven IDE. To load a
   settings file too, add the two options in [load the file in a session](#read-the-settings-file).

5. Open <http://localhost:10000/ide/> in your browser and enter that password. The Deephaven IDE
   opens with its console, where you can run the examples in the [README](../../README.md).

<a id="embedded"></a>

## Run Deephaven inside Python or Jupyter

Deephaven can run inside your own Python process, such as a script or a Jupyter notebook, with
no separate server to start. Install the library into the environment your script or notebook
uses, with the README's [install](../../README.md#install) command. Then set `MASSIVE_API_KEY`
in the terminal that starts Python or Jupyter.

This script starts the server, then reads Apple's daily bars for one week. It works on every
Stocks plan, the free one included. Save it as `week.py` and run it with `python -i week.py`:

```python skip-test
from deephaven_server import Server

server = Server(port=10000, jvm_args=["-Xmx4g", "-Dauthentication.psk=choose-a-password"])
server.start()

import deephaven_massive as dm

m = dm.MassiveSession()
week = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2025-06-02", "2025-06-06")
print(week.to_string())
```

It prints five rows, one daily bar for each trading day from June 2 to June 6, 2025. The server
lives only as long as the Python process, and `-i` keeps the process open after the script's
last line. While it runs, open the Deephaven IDE at <http://localhost:10000/ide/> to watch its
live tables.

Start the server before you import `deephaven` or create a session. Both need the running
server, and they fail without it.

<a id="s3-keys"></a>

## Add the S3 keys for flat files

Set your two S3 keys, and `history()` can read Massive's flat files, the fastest way to read
years of history for many symbols. Each flat file is a compressed file that holds a whole
market's trades, quotes, or bars for one day. Massive stores them in Amazon S3, behind their own
pair of keys, which its dashboard shows next to your API key. Each dataset's page in the
[dataset reference](../datasets/README.md) shows which plans include its flat files.

Set both keys in the terminal that starts the server, before you start it:

```bash
export MASSIVE_S3_ACCESS_KEY_ID="your-access-key-id"
export MASSIVE_S3_SECRET_ACCESS_KEY="your-secret-access-key"
```

With both keys set, `history()` reads flat files when you ask for 50 or more symbols, or for
`dm.ALL`. It does so only for datasets that have flat files, as
[where history comes from](history.md#history-sources) explains. Without the keys, `history()`
reads Massive's REST API only. Set both keys or neither, because one key alone makes
`dm.MassiveSession()` raise `ValueError`.

<a id="settings-file"></a>

## Keep your settings in a file

A settings file holds the `dm.MassiveConfig` settings you'd otherwise repeat in every script.
It's a YAML file that sets any setting by name, and a setting it leaves out keeps its default.
`help(dm.MassiveConfig)` lists every setting and what it does.

This file fits an account with Stocks on the free Basic plan and Options on the Starter plan.
Each Massive product for one asset class, such as Stocks or Options, is a subscription, and the
file names them in lower case. Save it as `~/massive/settings.yaml`, and keep only the lines
your account needs:

```yaml
# ~/massive/settings.yaml: settings for deephaven-massive.
# Your API key and S3 keys never go here. Set them in the environment.

# Options on Starter stream 15 minutes late. Listing them accepts the delayed feed.
delayed_subscriptions: [options]

# Stocks on Basic allow five REST requests a minute. Listing them paces reads from the start.
rest_basic_subscriptions: [stocks]

# Wait up to 10 minutes for a dropped connection before a live table or poll fails.
max_outage: 600

# Keep at most four REST requests or flat-file reads running at one time.
max_concurrency: 4

# Stop at a mistyped ticker in a symbol table, instead of skipping it: raise, log, or ignore.
noncanonical_symbols: raise

# Skip a crypto pair Massive doesn't list, and log it once: log, ignore, or raise.
unlisted_symbols: log

# Skip a symbol Massive has no data for, and log it once: log, ignore, or raise.
unavailable_symbols: log

# Stream options from Massive's delayed host. Without this line, the library chooses.
websocket_hosts:
  options: delayed.massive.com
```

<a id="read-the-settings-file"></a>

### Load the file in a session

The file takes effect only when you call `dm.MassiveConfig.from_file()`. A plain
`dm.MassiveConfig()` or `dm.MassiveSession()` uses the defaults and your environment's keys, and
never looks for a file. You give `from_file()` the file in one of three ways:

| Where you run | How to name the file |
|---|---|
| A notebook or script | Pass the path: `dm.MassiveConfig.from_file("~/massive/settings.yaml")`. |
| The Deephaven console | Set `export DEEPHAVEN_MASSIVE_CONFIG_FILE=~/massive/settings.yaml` in the terminal before you start the server. Then call `dm.MassiveConfig.from_file()` with no path. |
| Docker | Add `-v "$HOME/massive/settings.yaml:/massive/settings.yaml:ro"` and `-e DEEPHAVEN_MASSIVE_CONFIG_FILE=/massive/settings.yaml` to the `docker run` line. `:ro` mounts the file read-only. |

Settings in code win over the file, and the file wins over the defaults. To change one setting
of the file for a single session, copy the configuration with `dataclasses.replace`. Run this in
a notebook or the console, once the file is saved:

```python
import dataclasses

import deephaven_massive as dm

config = dm.MassiveConfig.from_file("~/massive/settings.yaml")
m = dm.MassiveSession(dataclasses.replace(config, max_concurrency=2))
```

`m` is a session with every setting from the file except `max_concurrency`, which is 2. The file
itself doesn't change, so your other scripts still get 4. With the
[debug log](troubleshooting.md#the-log) on, each load records an INFO line with the file's name
and the settings it set.

### Fix a file the library refuses

The library refuses a file that breaks any of these rules, and its error names the file, the
line, and the setting:

- Every value is text until its setting converts it. A ticker such as `ON` or `NO` stays a
  ticker.
- Spell each word as shown: `raise`, never `Raise`.
- Use only the settings `help(dm.MassiveConfig)` lists. The error for an unknown one suggests
  the closest name.
- Give every setting a value. To get a setting's default, leave the setting out.
- Keep your API key and S3 keys out of the file. The error names the environment variable to
  use instead.
- Leave out YAML's tags (`!!python/...`), anchors and aliases (`&name`, `*name`), repeated
  settings, and extra documents.
