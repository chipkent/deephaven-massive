## 5. `MassiveConfig`

`MassiveConfig` is a frozen dataclass. `MassiveConfig()` reads the credentials from the
environment and nothing else: it never reads a file. Defaults are defined in exactly one place:
the field defaults of this class. `MassiveConfig.from_file` builds one from a settings file
(below).

| Field | Default / environment variable | Meaning |
|---|---|---|
| `api_key: str` | `MASSIVE_API_KEY` | Required. Absent or blank raises `AuthenticationError` at construction; a key with surrounding whitespace raises `ValueError` (the message never shows the key). No other variable is read: `POLYGON_API_KEY`, the vendor's former name, is ignored. |
| `s3_access_key_id: str \| None` | `MASSIVE_S3_ACCESS_KEY_ID` | Enables flat-file history. |
| `s3_secret_access_key: str \| None` | `MASSIVE_S3_SECRET_ACCESS_KEY` | The two S3 credentials are both set or both absent; exactly one raises `ValueError`, and so does a credential passed as blank text or with surrounding whitespace (None means absent). |
| `ws_hosts: Mapping[AssetClass, str]` | `{}` | Pins the hostname of the TICK family for a class, for example `{AssetClass.STOCKS: "socket.massive.com"}`. A bare lower-case hostname, taken exactly as written: a scheme, port, path, upper-case letter, or surrounding whitespace raises `ValueError`. The FMV family is never pinned ([section 13](13-host-resolution-and-connection-protocol.md#13-host-resolution-and-connection-protocol)). |
| `ack_timeout: float` | `5.0` seconds | Subscribe-acknowledgement watchdog, per topic, and how long a round of re-sends awaits its answer. Must be below `NO_ACK_RESEND` (60 s), the spacing of those rounds; a larger value raises `ValueError`. |
| `reconnect_backoff: float` | `30.0` seconds | The first wait before the WebSocket client tries again after any failure that is not fatal: a connection that opened and closed (cleanly or not, including a `max_connections` refusal), a TCP or TLS failure, a handshake or authentication timeout, or a retryable HTTP status during the upgrade (500, 502, 503, 504). Later waits double up to `max_reconnect_backoff` ([section 19.2](19-wire-clients-and-the-spec-snapshot.md#192-the-websocket-client)). Must be at least 15 s, else `ValueError` naming Massive's guidance: a shorter wait can be refused for holding the allowance the dropped connection still holds, and on an individual plan that wastes the one slot there is. Tests never need a shorter value: they drive `WsClient` directly, with its backoff as constructor arguments and an injected clock. The default waits out the window Massive's knowledge base describes: the server takes "roughly 10 to 30 seconds" to release a dropped connection, and an earlier retry "can be refused for holding the allowance it just lost", which wastes the one slot an individual plan has. |
| `max_outage: float` | `300.0` seconds | The longest outage a live table survives ([section 13.3](13-host-resolution-and-connection-protocol.md#133-connection-and-subscription-protocol), "Outages"). Once a WebSocket connection has been down (`RECONNECTING` or `WAITING_SLOT`) this long (from its last frame, or 40 s before the drop when that is later, or from the drop when no frame ever arrived), each stream on it that has not recovered fails, so no live table keeps showing rows as current while the rows since the outage began are missing. A `poll()` table whose cycles have failed this long, from the last progress of the first failed cycle since its last good cycle, or whose late cycle has received no page this long, fails the same way ([section 13.4](13-host-resolution-and-connection-protocol.md#134-rest)). `0` fails at the first disconnect or failed cycle. Zero is allowed, unlike the other durations; a negative or non-finite value raises `ValueError`, and a non-number `TypeError`. No environment variable: no duration setting reads one. |
| `max_reconnect_backoff: float` | `120.0` seconds | Cap of the doubling backoff, so a connection comes back within this long of a long outage ending. Must not be below `reconnect_backoff`. Jitter only adds up to 20% and is clamped, so every wait lies in `[reconnect_backoff, max_reconnect_backoff]` ([section 19.2](19-wire-clients-and-the-spec-snapshot.md#192-the-websocket-client)). |
| `flat_file_symbol_threshold: int` | `50` | The `HistorySource.AUTO` switch point when both sources are possible. |
| `max_concurrency: int` | `8` | REST fan-out and S3 reads: the threads of one fan-out or `history()` call, and the most REST requests, and the most flat files being read, that the session has at once however many calls overlap. Each client's connection pool holds this many connections, and a request waits for a free one (`_massive/net/slots.py`), so overlapping calls never open and discard extra connections. |
| `rest_basic_subscriptions: AbstractSet[Subscription]` | `frozenset()` | The subscriptions the key's account holds on Massive's free Basic tier, whose REST requests are paced at 5 per minute from the first one ([section 13.4](13-host-resolution-and-connection-protocol.md#134-rest)). Stored as a `frozenset`. Anything but a set of `Subscription` raises `TypeError`; forex or crypto, as text or as an `AssetClass`, raises `TypeError` naming `Subscription.CURRENCIES`, the one subscription of both. |
| `delayed_subscriptions: AbstractSet[Subscription]` | `frozenset()` | The subscriptions whose streams use Massive's delayed feed (`delayed.massive.com`: 15 minutes behind, 10 for futures), even when the plan is real-time. Every subscription not listed uses the real-time feed, and a stream verb of one raises `EntitlementError` when Massive shows that the plan's data is delayed, naming the evidence and this setting ([section 13.2](13-host-resolution-and-connection-protocol.md#132-host-decision)); `fetch()` and `poll()` of its current data fail the same way when Massive labels a response or row `DELAYED` ([section 13.4](13-host-resolution-and-connection-protocol.md#134-rest)). REST cannot ask for delayed data, so a listed subscription's REST reads return whatever the plan serves, unchecked. The choice is deterministic: no stream ever moves between feeds. `history()` is past data and is not affected. Stored as a `frozenset` and validated like `rest_basic_subscriptions` (`TypeError`); `Subscription.CURRENCIES` raises `ValueError`, since Massive has no delayed forex or crypto feed. A `ws_hosts` pin that contradicts it raises `ValueError`: a delayed feed (any host whose name starts with `delayed.`) for forex, crypto, or a class whose subscription is not listed, or any other host for one that is. A stream of the FMV family on a listed subscription raises `ValueError`: FMV is real-time only. |
| `flush_interval: float` | `0.05` seconds | WebSocket micro-batch flush into `TablePublisher.add`. |
| `rest_base_url: str` | `https://api.massive.com` | Rarely changed. Must be an `http` or `https` URL with a host and no whitespace (`ValueError`, showing the URL without its user and password). |
| `s3_endpoint_url: str` | `https://files.massive.com` | Rarely changed. Validated like `rest_base_url`. |
| `s3_bucket: str` | `flatfiles` | Rarely changed. A blank bucket raises `ValueError`. |
| `noncanonical_symbols: OnError` | `LOG` | What a row of a symbol table that is not Massive's exact ticker does, for every verb and every row, present at the call or added later ([section 10](10-symbols-and-all.md#10-symbols-and-all)). `RAISE` raises `ValueError` for such a row present at the call and fails the result for one added later. `LOG` skips the row and logs each distinct value once at WARNING (at most 100 per process); `IGNORE` skips it with nothing logged above DEBUG. Under both, a stream's or poll's `m.streams()` row lists the skipped values in `Error` (their count and the first five), and a table whose rows are all bad raises `ValueError` rather than start an empty result. A `str` or a sequence passed as `symbols` in code always raises, whatever the setting. |
| `unlisted_symbols: OnError` | `LOG` | What a crypto pair of a symbol table does in a live stream (`blink()`, `append_only()`, `ring()`, `last_by()`) when Massive's crypto ticker list does not hold it, or the list cannot be read after the bounded retries, so the pair cannot be split into the topic Massive streams it on ([section 10](10-symbols-and-all.md#10-symbols-and-all)); present at the call or added later. `RAISE` raises `MassiveError` for such a pair present at the call and fails the stream for one added later. `LOG` skips the pair and logs each distinct value once at WARNING (at most 100 per process); `IGNORE` skips it with nothing logged above DEBUG. Under both, the stream keeps its other pairs, its `m.streams()` row lists a reactive table's skipped pairs in `Error` (saying when the list could not be read), and a table whose pairs are all unlisted raises `MassiveError` rather than start an empty result. A skipped pair stays skipped until its row is deleted and added again. The two crypto REST paths that need the split (the last trade, the daily open and close) follow it too (decision 90): under `LOG` and `IGNORE`, `fetch()` and each `poll()` cycle leave such a pair out (a `poll()` lists it in `Error`, and a followed `fetch()` marks it `FAILED` in `m.symbol_reads()`), and a static table whose every pair is unlisted raises `MassiveError`; under `RAISE`, `fetch()` raises and a `poll()` or followed table fails. A failed read of the list fails them as before. A `str` or a sequence passed as `symbols` in code always raises, whatever the setting. |
| `unreadable_symbols: OnError` | `LOG` | What a symbol Massive sends in a spelling the library cannot read does ([section 10](10-symbols-and-all.md#10-symbols-and-all)). `RAISE` fails the request or stream that received it, naming the spelling, this setting, and where to report the gap. `LOG` keeps the row with a null `Symbol` and the text in the `RawSymbol` column, and logs each spelling once at ERROR with the library-bug text (at most 100 per process): the library's grammar lacks a spelling Massive uses, which is a library defect, yet every table keeps running. `IGNORE` keeps the row the same way with nothing logged above DEBUG. Every dataset that reads its symbol from rows has `RawSymbol` under `LOG` and `IGNORE`, so the log choice never changes a schema; under `RAISE` it has none. |
| `unavailable_symbols: OnError` | `LOG` | What a `poll()` table does with a symbol it cannot read from Massive: one Massive answers with HTTP 404 (no data), or one whose path does not take a parameter the poll sends (a reactive table's crypto pair on an indicator polled with `adjusted=False`). Owner standard, 2026-11-02: a problem with one symbol is logged and the table continues by default, with an `OnError` setting for other behavior. `LOG` leaves the symbol out of each cycle, logs it once per session at WARNING (then DEBUG), and lists it in the table's `m.streams()` `Error` (`no rows this cycle for ...`) until it is readable again or removed; a `REPLACE` table drops its rows; the other symbols keep polling. `IGNORE` does the same with nothing logged above DEBUG, and `Error` still lists it. `RAISE` fails the table with an error naming the symbol, the reason, and this setting (`ErrorCause.EXPECTED` for a 404, `USER_DEFECT` for a refused parameter). `fetch()` and `history()` always leave a symbol with no data out, with a WARNING. |

Not configurable:

- The options contract cap (1,000 per connection; [section 15](15-options-contract-cap.md#15-options-contract-cap)).
- The JSON codec. `orjson` is used when it is installed (optional extra
  `deephaven-massive[orjson]`); otherwise the standard library. The wire package's
  `net.codec` owns the choice and logs it once at INFO, when it is first imported
  ([section 19](19-wire-clients-and-the-spec-snapshot.md#19-wire-clients-and-the-spec-snapshot)).
- The evidence endpoints of [section 13.2](13-host-resolution-and-connection-protocol.md#132-host-decision).
- The shared retry policy (REST and S3), the REST client's timeouts and pool size, and the
  WebSocket client's handshake timeout and stable-uptime rule
  ([section 19](19-wire-clients-and-the-spec-snapshot.md#19-wire-clients-and-the-spec-snapshot)).
- Proxies and CA certificates: all three clients read the standard environment variables
  (`HTTPS_PROXY`, `NO_PROXY`, `SSL_CERT_FILE`, `SSL_CERT_DIR`), as section 19 specifies, except
  that `SSL_CERT_DIR` does not apply to S3.

There is no `tier` field. No entitlement endpoint exists, so a tier field would be a claim the
server corrects. A key holds up to five tiers, one per subscription, and the only REST fact a
tier carries is its request limit (Basic: 5 per minute; every paid tier: none), so the one
tier input is `rest_basic_subscriptions`, which the library also learns from Massive's first
refusal; the 5 per minute is a wire constant, not configurable.

Environment variables have one name each and no fallbacks. An unset or blank variable is
absent; a value with surrounding whitespace raises `ValueError` naming the variable (never a
credential's value), as an argument would. The log levels accept any case, as their table
says: a keyword names a choice, not an identifier Massive matches. The only settings read from
the environment are the credentials, the log's two variables, and the settings file's path,
`DEEPHAVEN_MASSIVE_CONFIG_FILE`. The credentials keep Massive's
names (`MASSIVE_API_KEY`, `MASSIVE_S3_ACCESS_KEY_ID`, `MASSIVE_S3_SECRET_ACCESS_KEY`), so a key
set up for Massive's own tools works unchanged.

**The settings file.** `MassiveConfig.from_file(path=None)` returns a configuration built from a
YAML file, so a notebook, a console, and a container can share one set of settings. With no
path it reads the path from `DEEPHAVEN_MASSIVE_CONFIG_FILE`, and raises `ValueError` naming that
variable when it is unset; `~` in either is expanded. Nothing else reads a file: there is no
default location and no discovery, so `MassiveConfig()` and `MassiveSession()` behave the same
wherever they run. Precedence is code, then the file, then the defaults: code overrides a file's
setting with `dataclasses.replace(MassiveConfig.from_file(), ...)`. The call logs one INFO line
naming the file and the keys it set.

- **Keys** are the field names, and a file sets any subset. The credentials (`api_key`,
  `s3_access_key_id`, `s3_secret_access_key`) come only from code or their `MASSIVE_*`
  variables: a credential key in the file raises `ValueError` naming the variable to use, never
  the value. An unknown key raises `ValueError` with the closest key ("did you mean"), showing
  at most the key's first 40 characters, so a secret pasted as a key is cut short.
- **Parsing** uses a pure-Python PyYAML `BaseLoader` subclass, never `yaml.load` with a full
  loader or the C loader, so every scalar is text: YAML 1.1's type guessing never turns the
  ticker `ON` or `NO` into a boolean or `0700` into a number. The file is refused when it holds
  a duplicate key, any explicit tag (`!!python/...` included), an anchor or alias, more than one
  document, a top level that is not a mapping (an empty file included), or lists and mappings
  nested more than 10 deep (a setting needs two levels; deeper nesting would overflow the
  parser's recursion), each refusal naming the line.
- **Conversion** goes through one table that maps every field but the credentials to a
  converter: a list of subscription words (`[stocks, options]`), an enum word of `OnError`
  written exactly as its value (`raise`, `log`, `ignore`; `Raise` and `" raise"` are refused
  with the valid spellings), a number (an `int` field takes whole numbers only; a non-finite
  value is refused, and so is a whole number too long for Python to convert), a mapping of
  asset-class word to host (`ws_hosts`), or text (the URLs and the bucket; an empty value, such
  as `rest_base_url:` with nothing after it, `~`, `null`, or `''`, is refused: leaving the key
  out gives the default). The constructor then validates as it does for code, so it stays the only
  validator of values.
- **Every error** is a `ValueError` that names the file, the key, and the line and column where
  known, except one about an environment variable (a credential with surrounding whitespace, or
  one S3 credential without the other), which names the variable and not the file; a missing file raises `FileNotFoundError` naming it (and the variable, when the path
  came from it).

The log is not configured through `MassiveConfig`: it belongs to the process, not to a session.
Two environment variables turn it on, read once per process when the first session is created
([section 17.1](17-diagnostics.md#171-the-log)):

| Variable | Meaning |
|---|---|
| `DEEPHAVEN_MASSIVE_LOG` | A level name (`DEBUG`, `INFO`, `WARN`, `WARNING`, `ERROR`, `CRITICAL`, any case). Unset or blank changes nothing; any other value raises `ValueError` naming the variable from `MassiveSession()`. |
| `DEEPHAVEN_MASSIVE_LOG_FILE` | Append the lines to this file (UTF-8) instead of stderr. Alone, without `DEEPHAVEN_MASSIVE_LOG`, it turns the log on at DEBUG: naming a file asks for a log in it, and silently ignoring it would leave a tester with an empty file. A file that cannot be opened raises `OSError` naming the variable. |
