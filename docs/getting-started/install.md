# Install and configure

This page installs Deephaven and `deephaven-massive` into one Python environment, starts a
Deephaven server, and gives the library your Massive credentials. It takes about ten minutes.

## What you need

- **Python 3.11 or later.**
- **Java 17 or later.** Deephaven's engine runs on the Java virtual machine. Check your
  version with `java -version`; if it is missing or older, install a current JDK, for example
  from [Adoptium](https://adoptium.net).
- **A Massive account.** A free account works for the historical examples. The live examples
  need a paid plan for the asset class you stream; [Plans and entitlement errors](plans.md)
  explains which.

## Install

Create a virtual environment so the packages stay apart from the rest of your system, then
install the Deephaven server and the library into it:

```bash
python3 -m venv ~/dh-massive
source ~/dh-massive/bin/activate
pip install --upgrade pip
pip install deephaven-server "deephaven-massive @ git+https://github.com/chipkent/deephaven-massive.git"
```

Until the library is published on PyPI, it installs from its Git repository, as above. The
optional extra `deephaven-massive[orjson]` adds a faster JSON decoder; the library works the
same without it.

## Your API key

Massive's dashboard shows your API key on its keys page. The library reads it from the
environment variable `MASSIVE_API_KEY`, Massive's own name for it, so a key set up for
Massive's other tools works unchanged:

```bash
export MASSIVE_API_KEY="your-api-key"
```

Set it in the shell that starts the server, before you start it. To keep it across shells,
put the line in your shell's startup file (`~/.zshrc` or `~/.bashrc`).

## Your flat-file keys (optional)

Flat files are Massive's daily bulk files: one file per day holding every trade, quote, or
bar of every symbol. They are the fast way to read a whole market, or a long history of many
symbols. They are stored in Amazon S3, so they have credentials of their own, separate from
the API key: an access key id and a secret access key. Massive's keys page issues them beside
the API key. Flat files come with Massive's paid plans; a free plan has none.

```bash
export MASSIVE_S3_ACCESS_KEY_ID="your-access-key-id"
export MASSIVE_S3_SECRET_ACCESS_KEY="your-secret-access-key"
```

Set both or neither: one without the other is an error. Without them, `history()` reads
everything over REST, which works for any range but takes one request per page of rows.

## Start Deephaven

```bash
deephaven server --port 10000
```

The server prints the address of its web interface; open it in a browser
(<http://localhost:10000/ide> by default). If the server asks for a password, the server's
startup log printed a link that carries one. The **Console** panel runs Python. Every example
in these pages runs there.

## Check the setup

Paste this into the console:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
print(m, dm.__version__)
```

It prints `<MassiveSession #1 open>` and the library's version. Creating the session makes no
request to Massive, so it succeeds even with a wrong key; the first table you ask for checks
the key. If it fails instead:

- `AuthenticationError: no Massive API key` means the server cannot see `MASSIVE_API_KEY`.
  Stop the server, export the variable in the same shell, and start the server again.
- `MassiveError: MassiveSession must be created inside a Deephaven worker` means the code ran
  in a plain Python process rather than in the Deephaven console. Run it in the console.

## Configure in code instead

Everything the environment variables set can also be passed to `dm.MassiveConfig`. A key
passed this way takes the place of `MASSIVE_API_KEY`:

```python
import os

import deephaven_massive as dm

config = dm.MassiveConfig(
    api_key=os.environ["MY_MASSIVE_KEY"],
    rest_basic_subscriptions={dm.Subscription.STOCKS},
)
m = dm.MassiveSession(config)
```

Avoid typing the key itself into the console: every user of a Deephaven server can read the
console's variables. Read it from somewhere private, as above.

Two settings are worth knowing from the start:

- `rest_basic_subscriptions` names the subscriptions you hold on Massive's free Basic plan,
  which allows five REST requests a minute. The library then spaces its requests from the
  first one, instead of learning the limit from Massive's first refusal. The members are
  `dm.Subscription.STOCKS`, `OPTIONS`, `INDICES`, `CURRENCIES` (forex and crypto), and
  `FUTURES`.
- `max_concurrency` (default 8) is how many requests or file reads run at once.

`help(dm.MassiveConfig)` in the console lists every field with its default.

## One session is enough

A session holds your connections to Massive. Create one, at the top of your script or
console, and use it for everything. Massive's individual plans allow one WebSocket connection
per asset class at a time, so two sessions that each opened their own would knock each other
off. Sessions in one Deephaven server that use the same key share their connections, so a
second session is harmless, but it buys nothing.

Next: [Your first tables](first-tables.md).
