## 17. Diagnostics

Each diagnostics table is a blink table that the diagnostics writer adds rows to through its
`blink_publisher` publisher, and `last_by` of it on the table's keys, which keeps the latest
row of each key: a later row of a key replaces the earlier one as a modify, also when two adds
of one key land in one cycle, and a new key is appended in first-seen order. The tables are not
input tables, so nobody can edit them in the web UI. **Memory:** rows are never deleted, so
each table keeps one row per key ever written, copied out of the blink rows, plus `last_by`'s
hash state: a few dozen `connections()` rows, six `rate_limits()` rows, one `streams()` row per
stream, poll, and background read the session ever opened, and one `symbol_reads()` row per
symbol of each followed read. That is the same as the keyed input tables the library used
before, which never deleted rows either; a session that ever needs pruning would add a
tombstone filter, as `poll(mode=REPLACE)` does. They live in one session-owned
`LivenessScope` that `m.close()` releases. `m.connections()`, `m.streams()`, and
`m.symbol_reads()` each return a new view of one of them, built in its own result scope and tracked like any live result: while the view is held it
keeps the session alive and current, and after `close()` it keeps showing the final rows. They
are written by one diagnostics writer thread per session, which
adds immutable row snapshots from a queue in order; other threads only enqueue, never wait on
the update graph for diagnostics, and never write from UG callbacks. Each time it wakes, the
writer takes everything queued, keeps the latest row per key, and adds it with one Arrow
conversion and one `publisher.add` per table; it starts its next batch no sooner than
`WRITE_INTERVAL` (0.1 s) after the last, so a burst of rows costs each table one add per
interval and the queue stays bounded however fast rows arrive. An add returns at once; the
rows reach the tables on the next update cycle. Only the writer touches the publishers, so they
need no lock of their own. `Diagnostics.wait_until_shown(timeout)` waits for the writer to add
every row queued before the call and then for an update cycle that started after that; tests
use it to read rows.

**`m.connections()`**: one row per connection plus one `Transport=REST` row per resolved
(class, family).

| Column | Meaning |
|---|---|
| `Transport` | `WebSocket` or `REST`: the spelling `columns()` and the column reference use for transports |
| `AssetClass` | enum value |
| `Family` | `TICK` or `FMV` |
| `Host`, `Path` | resolved host and cluster path |
| `Recency` | `REALTIME`, `DELAYED`, `UNKNOWN` |
| `Evidence` | the evidence string of [section 13.2](13-host-resolution-and-connection-protocol.md#132-host-decision) |
| `State` | `RESOLVING`, `CONNECTING`, `CONNECTED`, `WAITING_SLOT`, `RECONNECTING`, `FAILED`, `CLOSED` |
| `Topics`, `AckedTopics`, `PendingTopics`, `NoAckTopics` | counts; rewritten after every frame of acknowledgements and refusals and every `NO_ACK`, also while a subscription step is in flight |
| `BufferedRows` | WebSocket connections only (0 on `REST` rows): rows received that no flush of the connection has taken yet; every family row of a connection shows the same count. The row is rewritten at least once a second while the count is above zero. Rows are never dropped; above `BACKLOG_WARNING_ROWS` (1,000,000) a WARNING says the flushes are falling behind, logged again only after the backlog has fallen below half of it |
| `Reconnects` | count |
| `LastMessageAt`, `NextReconnectAt`, `UpdatedAt` | `Instant`; `NextReconnectAt` is set only while the connection waits for its next attempt (`RECONNECTING`, `WAITING_SLOT`) |
| `LastError` | text; for a REST failure it names the HTTP status, the path, and the `request_id` |
| `SchemaDrift` | text; WebSocket connections only: the drift seen on the connection, as `channel:field+` (extra), `channel:field-` (missing), `channel:field?` (a value that does not fit), `channel:field~` (a whole number beyond 2<sup>53</sup> served rounded), and `channel:<n> rows dropped` (each read of a row whose key could not be read, counted), the counts first, then the fields, each sorted, truncated at 200 characters |

**`m.streams()`**: one row per stream, per `poll()` table, per background read
(`wait=False`), and per followed read (a `fetch()` or `history()` that follows a refreshing
symbol table, [section 7.8](07-query-classes-and-verbs.md#78-following-a-live-symbol-table)), told apart by `Kind`. A row is written when the stream has opened and on
every change until the stream leaves the registry (its holds released and its blink table shut
down), so a released stream still kept alive by a derived table keeps reporting
`RowsPublished` and a late failure; its last row (`RELEASED`, or `FAILED` if the stream failed)
stays in the table while the session is open. At `m.close()` every stream
still in the registry is written `CLOSED`, with the close's error text, and then
the diagnostics scope is released; a view obtained before the close keeps showing those rows.

| Column | Meaning |
|---|---|
| `StreamId` | key; a read's is its call id |
| `Kind` | `stream` (a WebSocket stream), `poll` (a `poll()` table), or `read` (a background or followed `fetch()` or `history()`) |
| `Dataset` | `"stocks.trades"` |
| `Query` | the `repr` of the query whose verb opened the stream or `poll()`, the call that built it, such as `m.stocks.trades(['AAPL', 'MSFT'])`, so two streams of one dataset can be told apart, truncated at 200 characters like `SchemaDrift`; a stream reused by an equal query keeps the opener's text |
| `SymbolCount`, `Symbols` | count and a truncated list |
| `HeldResults` | results currently held by the library (result records not yet released) |
| `Deferred` | options contracts deferred by the cap |
| `Transport`, `Host`, `Path` | `Transport` is `WebSocket` for a stream, `REST` for a `poll()`, and for a read the source it reads, `REST` or `flat files`; a read's `Host` and `Path` are null |
| `Recency` | `REALTIME` or `DELAYED`: the feed a stream reads (`UNKNOWN` on a pinned host of no known feed); for a `poll()` table of current data, what the labels of its latest labelled cycle showed (`REALTIME`, or `DELAYED` for a subscription listed in `MassiveConfig.delayed_subscriptions`, since a delayed label on any other fails the table), `UNKNOWN` until one does for the data of one subscription, and null until one does for the cross-class snapshot; null for reference, economy, and partner data and for a background read. `UNKNOWN` is never shown as real-time |
| `RowsPublished` | count; a background read's is set once, when its rows land; a followed read's is the rows now in the result |
| `Loading` | a followed read's symbols that are queued, loading, or waiting to retry; null on every other row |
| `RequestsPerCycle` | `poll()` fan-out |
| `State` | `RUNNING` (a stream or poll; a followed read whose every current symbol has landed or failed), `LOADING` (a read still downloading; a followed read with a symbol queued, loading, or waiting to retry), `DONE` (a background read whose rows have landed), `FAILED`, `RELEASED`, `CLOSED` |
| `Error` | text: why a `FAILED` or `CLOSED` stream stopped, and nothing else; while it runs, every current warning that leaves it running, joined with `"; "` in a fixed order: its connection's outage or silence ([section 13.3](13-host-resolution-and-connection-protocol.md#133-connection-and-subscription-protocol), "Outages" and "Silence"), the options cap's deferral, a futures stream's schedule miss ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)), a `dm.ALL` stream's symbols whose rows are dropped, a `poll()` cycle's error, the symbols the latest good `poll()` cycle left out, then the skipped symbol-table values. Each warning is one named part (`NotePart`) that only its source sets or clears (`StreamUpdate.notes`), so no source replaces or clears another's warning. |
| `Gaps` | text: every span over which the table's rows may be missing, a lasting record of the warnings `Error` shows only while they last: a WebSocket outage the stream recovered from, from the last frame before the drop (or the stream's open, when it opened during the outage) to the data's return, and a run of failed `poll()` cycles, from the start of its first failed cycle (which published nothing, whatever pages it received) to the next good cycle's start. Each is `<start> to <end>` in UTC to the second, oldest first, joined by `"; "`; the latest five are shown, after a count of the earlier ones (`"2 earlier gaps; ..."`). Kept on a stopped stream; null while there is none. An outage that fails the stream is named by its failure instead. |
| `CreatedAt` | `Instant` |
| `SchemaDrift` | text: the stream's drift, `poll()` rows included, as `transport:field+` (extra), `transport:field-` (missing), `transport:field?` (a value that does not fit), `transport:field~` (a whole number beyond 2<sup>53</sup> served rounded), and `transport:<n> rows dropped` (each read of a row whose key could not be read, so a `poll()` that reads the same bad row every cycle counts it every cycle), with the transport spelled as in `Transport` (`WebSocket:zz+`), the counts first, then the fields, each sorted, truncated at 200 characters |

**`m.rate_limits()`**: one row per REST rate-limit bucket ([section 13.4](13-host-resolution-and-connection-protocol.md#134-rest)), keyed by
`Bucket`. The bucket's state lives in a governor that sessions of the same API key share, so
the writer pulls a fresh snapshot (`_ingest/rate_limits.rate_limit_pull`) whenever the
governor signals a change, at most once per batch, instead of applying queued rows; a row can
never be overtaken by an older one, and every sharing session shows the same rows. The state
also changes with time alone (a pause ends, a slot frees), so each pull names when the rows
next change, and the writer waits on its queue no longer than that before it pulls them
again; a batch handled after that moment pulls them too, so a busy queue cannot hold them back. No other thread or timer is involved, so nothing outlives `release()` or `stop()`.

| Column | Meaning |
|---|---|
| `Bucket` | key: a `Subscription` value (`stocks`, `options`, `indices`, `currencies`, `futures`), or `other` for reference data and the other datasets Massive assigns to no subscription, which it counts on their own |
| `State` | `READY` (a request starts at once), `PACED` (a request would wait for a slot: some wait, or every slot is held), `PAUSED` (a 429's pause is active; a read that will not wait until `PausedUntil` fails) |
| `RequestsPerMinute` | 5 while paced; null while unpaced |
| `LimitSource` | `DECLARED` (`rest_basic_subscriptions` of a sharing session) or `LEARNED` (Massive's refusal named the per-minute maximum); null while unpaced |
| `InFlight`, `Waiting`, `RecentRequests` | counts: requests on the wire; waiting for a slot or a pause; holding a slot (in flight, or answered within 61 s) |
| `Attempts`, `Refusals` | totals since the governor was created; every retry and refused attempt counts |
| `PausedUntil`, `UpdatedAt` | `Instant` |
| `LastRefusal` | text of the latest 429: status, path, `request_id`, and Massive's message |

**`m.symbol_reads()`**: one row per symbol of each followed read, keyed by `StreamId` and
`Symbol`, so a user sees which symbols are loading, which failed, and why
([section 7.8](07-query-classes-and-verbs.md#78-following-a-live-symbol-table)). The rows are kept for the session's life,
also after their result is released; at `m.close()` every symbol not in a final state is
written `CLOSED`. It is written by the diagnostics writer like `m.streams()`, from row
snapshots the session's follow coordinator queues.

| Column | Meaning |
|---|---|
| `StreamId` | key; the followed read's row in `m.streams()` |
| `Symbol` | key; the canonical symbol |
| `Dataset` | `"stocks.bars"` |
| `State` | `QUEUED` (waiting for a read), `LOADING` (being read), `RETRYING` (a transient failure; read again at `NextAttemptAt`), `DONE` (its rows are in the result; 0 rows for Massive's 404), `FAILED` (a permanent failure or the last retry's; `Error` says why), `REMOVED` (its row left the symbol table, and its rows the result), `STOPPED` (the result was released and nothing uses it), `CLOSED` (`m.close()`) |
| `Transport` | `REST` or `flat files`: the source of its latest read |
| `AddedAt` | `Instant`: when its row last entered the symbol table |
| `ReadAt` | `Instant`: when the download whose rows the result holds began; the rows are Massive's answer as of then |
| `Rows` | its rows in the result |
| `Attempts` | reads started since it was last added |
| `NextAttemptAt` | `Instant`; set only while `RETRYING` |
| `Error` | text of its latest failure, as in `m.streams()`; cleared when a read succeeds |
| `UpdatedAt` | `Instant` |

`RELEASED` means "no library-held results; still publishing until the blink table dies"
([section 6.1](06-massivesession-and-lifecycle.md#61-how-a-live-table-is-kept-alive-and-released)). `FAILED` can follow `RUNNING`
or `RELEASED`; a failed stream's row keeps `FAILED` when its last result is released. `CLOSED`
is final: no later failure replaces it. A read's `RELEASED` means its result was released
while it was loading; it becomes `DONE` if a derived table kept it going until its rows landed.
`DONE` is final like `FAILED`: neither a release nor `m.close()` changes it.

`Error` also carries warnings that do not fail the stream: the options cap's deferral
([section 15](15-options-contract-cap.md#15-options-contract-cap)) and the wait for a WebSocket slot
([section 13.3](13-host-resolution-and-connection-protocol.md#133-connection-and-subscription-protocol)).

Symbol problems show in `Error` with their own text. Under `noncanonical_symbols=LOG` or
`IGNORE` (`LOG` is the default), the rows of a stream's or poll's symbol table that are skipped
because they are not Massive's exact tickers are listed as a warning (`skipping 1 symbol-table
value that is not Massive's ticker: 'msft'; fix or delete that row`), cleared once none is
left. That text is kept apart from the transport's own warning (a wait for a connection slot,
deferred options contracts, a poll cycle's error), and `Error` shows both, joined by `; `, so
neither hides or clears the other. Under `RAISE`, such a
later row fails the result (`a symbol table holds 'msft' (...). Fix the row, or pass the table
through dm.canonicalize_symbols(table, dm.AssetClass.STOCKS), which ...`),
and under `unreadable_symbols=RAISE` a symbol Massive sent in a spelling the library cannot
read does (`stocks.trades: Massive sent the symbol 'BRK/B', ... This is a bug in
deephaven-massive. ...`). Under `unreadable_symbols=LOG` an unreadable spelling is logged once
at ERROR and its rows keep the text in `RawSymbol`; it is not shown here, since a WebSocket row
with no symbol belongs to no single symbol stream.

`SchemaDrift` is separate from `Error` and `LastError`, so drift never overwrites a failure. The
session's `DriftRegistry` collects it ([section 19.5](19-wire-clients-and-the-spec-snapshot.md#195-runtime-drift)). Static verbs
(`fetch()`, `history()`) report drift only in the log.

Every failure is logged, at the level [section 17.1](#171-the-log) gives its cause, with the text
the exception carries: hosts, the HTTP status and
`request_id` of a REST failure, the server message verbatim, the decision evidence, and an
alternative where the library has one. A plan refusal (`EntitlementError`) states the
subscription, the transport, the request that was refused (marked as the library's own when it
was the host check's snapshot request, which a stream needs before it subscribes), and
Massive's text. The library does not model plans, so it names another way to read the data only
where one exists: a refused flat-file `history()`, under `FLAT_FILES` or `AUTO`, names
`source=dm.HistorySource.REST` when REST can serve the read. `AUTO` raises the refusal rather
than switch to REST, since the two sources fill different columns.

### 17.1 The log

The requirement: a log from a failing run, with the issue text, is enough for an agent to
reproduce and fix the bug. It uses only Python's standard `logging`.

**Loggers.** Every module logs through `logging.getLogger(__name__)`, so each library logger is a
child of `deephaven_massive` and names the file that wrote the record.

**Off by default.** Nothing is configured at import. WARNING and ERROR records reach
`sys.stderr` through Python's last-resort handler, which Deephaven captures; lower levels are
dropped.

**Two switches on, one off.** Each switch adds one handler to the `deephaven_massive`
logger and sets that logger's level; turning the log on again replaces the handler and closes
the old one:

- `DEEPHAVEN_MASSIVE_LOG=<level>` (and `DEEPHAVEN_MASSIVE_LOG_FILE=<path>`), read when the first
  `MassiveSession` is created ([section 5](05-massiveconfig.md#5-massiveconfig)). Not at import: a script that
  starts `deephaven_server.Server` may import the library before the server replaces
  `sys.stderr`, and a handler made then would keep the old stream. A session needs the server,
  so by then `sys.stderr` is final. The variables are skipped once `enable_debug_logging` has
  run. The file variable alone turns the log on at DEBUG.
- `dm.enable_debug_logging(output=LogOutput.STDERR)`, for a running console, where a new variable
  takes effect only after a worker restart. It sets DEBUG; `output` is `LogOutput.STDERR` or a
  file path to append to, an unopenable file raises its `OSError` and keeps the previous
  handler, and anything else raises `TypeError`. `LogOutput` is an `Enum`, not a `StrEnum`:
  its value is no Massive wire string.

`dm.disable_debug_logging()` turns off whatever either switch turned on, at any level: it logs
one INFO line saying so, removes and closes the library's handler, and sets the logger's level
back to `NOTSET`, so the library is as it was at import (WARNING and ERROR through the
last-resort handler). The variables then count as read, so a later first session does not turn
the log back on, and a read of them already under way on another thread installs nothing, as
after `enable_debug_logging`; only `enable_debug_logging` turns it on again. With nothing
turned on it changes nothing. It leaves a handler the user added to the logger alone, but sets
the logger's level to `NOTSET` whoever set it last. It takes no argument:
the switch exists to undo the other two, and a level is what `DEEPHAVEN_MASSIVE_LOG` is for.

Setting the level alone (`logging.getLogger("deephaven_massive").setLevel(logging.DEBUG)`) shows
nothing below WARNING, because the last-resort handler drops lower levels; that is why the
function exists. A user who configures logging already only sets the level.

**The handler.** A `StreamHandler` on `sys.stderr` (Deephaven copies stderr into its log, the
container log, and the web IDE's log panel), or a `FileHandler(path, encoding="utf-8")`. The
format is `%(asctime)sZ %(levelname)s [%(threadName)s] %(name)s: %(message)s` with times in UTC
(`formatter.converter = time.gmtime`). `propagate` stays on, so `caplog` and any root handler
still see every record; Deephaven installs no root handler, so nothing is printed twice. There
is no queue, rotation, JSON, custom field, or context variable: each line costs one locked write.

**Correlation.** Every library thread is named after what owns it. A session's own threads are
`massive #<session> <role>`: `reaper`, `diagnostics`, `poll-<stream id>` and its pool
`poll-<stream id> rest_<n>`, `poll watch`, `symbol changes`, and the call pools below. The threads of a
connection group, which serve every session sharing its connections, are
`massive group #<group> <role>`: `ws <host>/<cluster>`, `flush <host>/<cluster>`,
`start <host>/<cluster>`, `move <host>/<cluster>`, and `futures schedules`. Each static
verb (`fetch()`, `history()`) is one call: it takes an id from the stream-id counter, so call
ids never collide with the `StreamId`s of `m.streams()`, logs `call <id> start: <query>.<verb>`
and then `call <id> done: <rows> rows in <s> s` or `call <id> failed after <s> s: <error>` at
DEBUG, and its worker threads are `call-<id> rest_<n>` or `call-<id> files_<n>`. The wire
clients cannot see the group, so `WsClient` takes the prefix as a constructor argument.

**The environment line.** Each time the log is turned on, by a variable or by the function,
it first logs one INFO line naming the level, where the lines go (a file by its absolute path,
symbolic links resolved), and what turned it on. Then it logs one INFO line describing the
process: the library's version and the git commit it was installed from (`vcs_info.commit_id`
of the distribution's PEP 610 `direct_url.json`, or "not recorded"); Python's, with whether
the GIL is enabled; `platform.platform()`; the local time's offset from UTC and zone name, so
the log's UTC times map to the tester's clock; the version of every dependency the library
declares (an absent `orjson` means the standard library decodes JSON); the date of the Massive
spec snapshot; and whether `HTTPS_PROXY` and `SSL_CERT_FILE` are set, never their values. Each
`MassiveSession()` then logs its label and its configuration's fields at INFO, with each
credential (the API key, the S3 access key id, and the S3 secret) shown only as set or unset.

**Secrets.** The API key travels only in the REST client's header and the WebSocket
authentication frame; the S3 secret goes only to boto3; the proxy password is already
stripped (`net.http`). A server can still echo a secret, so each client cleans what it
receives in one place, before anything reads it, through `net.redaction`: the REST client every
response body and the WebSocket client every frame, the handshake's included
(`Redactor.received`), and the flat-file client S3's error words (`redact`), leaving out of the
error chain a botocore error that quotes them. Each client builds one `Redactor`, which
compiles, per secret, a pattern in which every character is itself or any JSON escape that
spells it (hex digits in either case), and a cheaper pattern of only those escapes.
`received` replaces every match in the raw text, so an echo spelled with escapes is caught
whether the body is valid JSON, cut short, or a proxy's HTML page, and nothing is decoded or
re-encoded. Every row, status, error, excerpt, and log line built from them is then free of the
secret, wherever it is built. A body holding neither the secret nor such an escape costs a
substring search per secret and one pattern search, and is passed on uncopied. The library
never enables DEBUG on urllib3 or botocore, and the websockets frame log stays off
([section 19.2](19-wire-clients-and-the-spec-snapshot.md#192-the-websocket-client)). The planted-secret test
(`tests/massive/net/test_redaction.py`) runs the fakes with distinctive secrets and DEBUG on
every logger through a 401, a 429, bodies, rows, and a status that echo the key, plainly and
spelled with JSON escapes, `auth_failed`, and S3 errors, and finds neither secret in any record
or error; the logging rules of
`tests/test_layout.py` are a cheap backstop (no secret name in a logging call, every thread
and pool named, no logging call under a leaf lock).

**What the wire clients log.** Each layer logs what it knows, and repeats are fine:

- The REST client logs every attempt at DEBUG with the full URL, the status, the `request_id`,
  and the time it took: `GET https://api.massive.com/v3/trades/AAPL?limit=50000 -> 200
  (request_id 6a1f): 1950 rows in 0.410 s`. Any other status adds the body, up to 2,000
  characters with the key replaced. A retry logs at INFO and giving up at DEBUG; the call
  that fails is what the user sees.
- A row the decoder drops, or one holding a value it reads as null, is logged whole at DEBUG
  (its JSON, up to 2,000 characters) by the decoder that read it: `rest.batch` for a REST page,
  `ws.protocol` for a WebSocket frame, whose `Reject` also carries the row as `raw`. Every
  reason and drift sample quotes the offending
  value's JSON text up to 200 characters.
- The WebSocket client logs each status object Massive sends (acknowledgements, refusals) and
  each subscription frame it sends at DEBUG, its connections and closes at INFO, and each
  backoff, refused handshake, and reconnect attempt at DEBUG. A frame it cannot decode is
  logged at ERROR with the library-bug text, at most 20 times per connection, and at DEBUG
  after that.
- The flat-file client logs each object request at DEBUG: the object, `200` with its size or
  `404 NoSuchKey`, and S3's `request_id`.
- Each `DriftReport` carries a sample per drifted key ([section 19.5](19-wire-clients-and-the-spec-snapshot.md#195-runtime-drift)).

**What the library logs.** Above the wire, each layer adds what it knows:

- A schema-drift WARNING names, after the dataset, transport, and field, where the payload came
  from (the REST path, the WebSocket URL and channel, or the flat file), the symbol of the first
  row that showed it (a WebSocket sample's, else the per-symbol REST request's), and an extra
  field's first value: `stocks.trades: WS field 'zz' is not in the schema; it is dropped
  (schema drift) at wss://delayed.massive.com/stocks channel T; symbol AAPL; first value 1`.
- A failed `fetch()` or `history()` logs `call <id> failed after <s> s: <message>` once, from
  the call itself, whatever its source: at DEBUG for an expected cause, which the caller
  receives as the exception, and at ERROR with the owner's text for a defect.
- A stream's open, reuse, each released hold, and its shutdown (with its route and topics) log
  at DEBUG with its stream id. The open names the session, the query and verb, the channel,
  the host and path with the evidence for that host, and the symbols (their count and the
  first 20). So do a reactive symbol table's changes, each subscription step (with the
  desired, acknowledged, and deferred counts), a `poll()`'s start (query, interval, the mode
  asked and the mode chosen, symbols) and end (why, and after how many cycles), and each
  `poll()` cycle (requests, rows read and published, time, and the seconds of rate limit
  pauses the cycle waits out). A `poll()` failure's record, and a skipped symbol-table row's,
  names its stream id.
- A REST request's rate governor bucket, and why, logs at DEBUG once per process for each
  dataset, path, and bucket, not once per request.
- A WebSocket row the client drops because a key cannot be read (a `Reject`) logs at DEBUG
  with its reason and its whole raw text, and counts toward its dataset's one WARNING and
  running count ([section 19.5](19-wire-clients-and-the-spec-snapshot.md#195-runtime-drift)); it fails no stream.
- Each WebSocket connection logs a DEBUG summary every 60 s from its loop: data batches and rows
  per channel, rows dropped for a key, the rows each stream got and the streams that got none, the
  acknowledged topics (their count, the first 20, and those added and removed since the last
  summary), pending and unacknowledged counts, and how long ago each channel last had data, so
  a stall is visible though nothing failed. Every list is bounded to 20 items. The timer
  always runs; the line is built only when DEBUG is on.
- A flat-file `history()` whose every trading day's file is missing (two or more) logs a
  WARNING that the bucket, prefix, or venue may be wrong, before the per-day holiday lines.
- Nothing is logged while `registry_lock` or another leaf lock is held, and nothing in a blink
  table's shutdown callback: the shutdown line comes from the connection's loop, which releases
  the route.

**Levels.** Users see WARNING and above by default, so each record's level says what the
reader must do, not how bad the event feels:

- **ERROR: a defect broke a result, and the message says whose it is.** A *library defect* (a
  broken internal invariant, an unexpected exception on a library thread, or vendor data the
  library cannot convert or read) ends with one shared text: "This is a bug in
  deephaven-massive. Turn on dm.enable_debug_logging(), reproduce it, and report it with the
  log and m.streams() at https://github.com/chipkent/deephaven-massive/issues." A *user defect*
  the library catches (a symbol table that fails upstream, a bad symbol-table row under
  `RAISE`, a parameter the chosen path does not take) names the user's table or code and what
  to fix, and never asks for a bug report. A defect logs at ERROR even when it is also raised,
  so a broad `except` never hides one. Expected conditions never log at ERROR: refusals, plan
  and rate limits, network failures, the user's own `m.close()`.
- **WARNING: live data is stale or wrong, or the user must act**, once per episode, with the
  recovery at INFO: a connection lost (one per outage, per connection, naming its live tables);
  a connection with acknowledged subscriptions that receives no frame for 5 minutes during its
  asset class's regular trading hours (`_datasets/trading_hours.py`; there is no holiday
  calendar, so the message says a holiday explains it; the streams' `Error` says so too); a stream backlog over its threshold; a
  `poll()` whose cycles run late; a static read inside a console command that will pause every
  live table; a live table failing for an expected reason, once per cause rather than once per
  table; data the user would misread (vendor drift, a bad symbol under the log setting); and
  configuration to fix.
- **INFO: decisions and progress**: the host chosen, a long read's estimate, the first 429,
  each rate-limit pause with its length and reason, recoveries, session open and close, and a
  session garbage-collected without `close()`.
- **DEBUG: everything else**, including every expected error that is also raised to the
  caller, each reconnect attempt, each retry and give-up, and per-request detail.

The common path of each WebSocket frame, row decode, and flush makes no log call at any level;
its exceptional branches log through the bounded helpers. A live table's failure is logged by
`log_failure` once per failure object, at ERROR with the owner's text for a defect and at
WARNING otherwise, so one connection failure that fails a hundred streams logs once.

**The rules every change follows** live in the `_debug_log` module docstring, beside the switch,
with the checks that enforce them: ruff's `G` and `LOG` rules, the logging rules of
`tests/test_layout.py`, the planted-secret test, and the thread-name tests.

**Cost.** With the log off, a level check fails and nothing is formatted (`%` arguments,
enforced by ruff's `G` rules); a value that costs a call to compute is computed only under
`isEnabledFor`. Nothing is logged per row or per frame except a failure.

**Bug reports.** `.github/ISSUE_TEMPLATE/bug_report.yml` asks for the code that was run, the
log of a run with `DEBUG` on as an attached file (GitHub cuts an issue's text at 65,536
characters), with its first lines and the minutes around the failure, `m.rate_limits()`,
`m.streams()`, and `m.connections()` printed, and what was expected, with a recipe for a hang.
