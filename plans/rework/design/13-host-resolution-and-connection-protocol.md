## 13. Host resolution and connection protocol

### 13.1 Invariants

1. One connection per `wss://host/path` per API key and connection settings in a process,
   shared by every session that has them ([section 6](06-massivesession-and-lifecycle.md#6-massivesession-and-lifecycle)). A
   class may therefore have up to two connections: one for the TICK family and one for the
   FMV family. (Business accounts have three connection slots per cluster; individual plans
   have no FMV.) A shared connection is reference-counted by the streams of every session on
   it: closing one session releases only that session's streams and never closes a connection
   another session leases. Three consequences are by design:
   - The options cap of [section 15](15-options-contract-cap.md#15-options-contract-cap) is the connection's, so the
     sessions on one `/options` connection share its 1,000 topics, and a static symbol list
     can raise `ValueError` because of another session's streams; the message counts the
     topics in use.
   - A refusal or a fatal failure acts on the connection, not on a session: a refused topic
     fails every stream that wants it, in every session, and a rejected key or a fatal
     handshake error fails every stream on the connection, in every session.
   Diagnostics stay per session: each session's `m.connections()` shows the rows of the
   connections its streams use, and its `m.streams()` only its own streams.
2. The host of a (class, family) is decided before its first connection, from configuration or
   REST evidence, and cached. A connection never moves to another host: the feed is the user's
   choice (`MassiveConfig.delayed_subscriptions`), and a refusal from Massive fails the refused
   streams.
3. No connection is ever opened to test anything.
4. Connects are single-flight. Every retry, whether after a connection that opened has closed
   (cleanly or not, including a `max_connections` refusal) or after a failed attempt (TCP or TLS
   failure, handshake or authentication timeout, HTTP 500/502/503/504 during the upgrade), waits
   at least `reconnect_backoff` (30 s by default, the upper end of the 10–30 s the knowledge base
   gives the server to release a dropped connection); the wait doubles up to
   `max_reconnect_backoff`, with upward-only jitter clamped to that range. A handshake error that
   retrying cannot fix (for example HTTP 400 or 404) and a rejected key are fatal and fail the
   connection's streams ([section 19.2](19-wire-clients-and-the-spec-snapshot.md#192-the-websocket-client)).
5. Futures streams always use `/futures`. `/futures/{venue}` is undocumented and unsupported.
6. A live table never shows stale data as if it were current. While a connection is down, its
   streams say so in `m.streams()` `Error`, and an outage longer than `max_outage` fails them
   ([section 13.3](#133-connection-and-subscription-protocol), "Outages").

### 13.2 Host decision

Resolution runs synchronously on the calling thread at the first stream verb for a
(class, family) and is cached in the session. The seam is
`RestTransport.resolve_host(asset_class, family) -> HostDecision`, where
`HostDecision(host, recency, evidence)` is a frozen record. The WebSocket transport calls it.
The REST transport is the one place the decision is cached, per session, for the session's
life, and resolution is **single-flight**: the cache holds one future per (class, family),
created under the transport's own leaf lock (never `registry_lock`); the first caller makes
the REST call and completes the future, and later callers wait on it outside every lock, each
within its own patience (section 13.4, Giving up). A failed resolution completes the future
with the error and removes it, so nothing is cached; every waiting caller raises a copy of it.
Each new decision writes the pair's `Transport=REST` row of `m.connections()`. A decision that
raises (a delayed plan for a subscription not listed in
`MassiveConfig.delayed_subscriptions`) is not cached, so the next verb decides again and sees a
plan upgraded meanwhile.

Recency follows the host: `socket.massive.com` and `business.massive.com` serve real-time data
(`REALTIME`), `delayed.massive.com` serves delayed data (`DELAYED`), and any other pinned host is
`UNKNOWN`, which is never shown as real-time.

```
resolve(asset_class, family) -> HostDecision(host, recency, evidence)

FMV family (any class):
  subscription listed in MassiveConfig.delayed_subscriptions
       -> ValueError: FMV is real-time only; Massive has no delayed FMV feed
  otherwise
       -> business.massive.com, REALTIME, evidence="FMV is Business-only; fixed host (docs)"

TICK family:
  1. websocket_hosts[asset_class] present (checked against MassiveConfig.delayed_subscriptions at
     construction)
       -> that host, the host's recency, evidence="websocket_hosts"
  2. subscription listed in MassiveConfig.delayed_subscriptions
       -> delayed.massive.com, DELAYED, evidence="MassiveConfig.delayed_subscriptions"
          (no REST call: the user chose the delayed feed, so no answer changes the host)
  3. FOREX, CRYPTO
       -> socket.massive.com, REALTIME,
          evidence="single documented host; Starter is real-time"      (no REST call)
  4. One REST call through the library's REST client (section 19.1), its envelope status and
     raw JSON rows:
       STOCKS  : GET /v3/snapshot?ticker.any_of=SPY,AAPL
       OPTIONS : GET /v3/snapshot/options/SPY?limit=1
       INDICES : GET /v3/snapshot/indices?ticker.any_of=I:SPX
       FUTURES : GET /futures/v1/snapshot?product_code=ES&limit=100
     Per-ticker rows carrying "error"/"message" (NOT_ENTITLED) count as absent. The futures
     snapshot lists a product's contracts by ticker, far-dated ones that do not trade yet
     first, and only a traded contract carries a timeframe (item 78 of section 23), so the
     probe reads up to 100 contracts and every row's labels.
  5. Classify:
     a. HTTP 403 / NOT_AUTHORIZED
          -> socket.massive.com, REALTIME,
             evidence="the host check's snapshot (<path>) was refused: <Massive's text>";
             WARNING naming the evidence. A refused REST snapshot says nothing certain about
             the WebSocket plan; a refusal from Massive fails the refused streams with
             Massive's text (a Basic plan's streams fail that way)
     b. Envelope status DELAYED, or a timeframe field DELAYED, read in this order (the same
        fields as the REST check of 13.4), never underlying_asset.timeframe:
          timeframe              (indices, top level; documented DELAYED | REAL-TIME)
          last_trade.timeframe, last_quote.timeframe   (stocks, options, futures)
          last_minute.timeframe  (futures; in the OpenAPI spec, absent from the documentation
                                 page)
          -> EntitlementError naming the evidence and the fix:
             "options streams would be delayed (Massive reports last_trade.timeframe=DELAYED),
             and dm.Subscription.OPTIONS is not in MassiveConfig.delayed_subscriptions, so the
             library serves only real-time options data. To use Massive's delayed options feed:
             dm.MassiveConfig(delayed_subscriptions={dm.Subscription.OPTIONS})".
             Not cached.
     c. A timeframe value other than DELAYED or REAL-TIME
          -> socket.massive.com, REALTIME, evidence="<field>='<value>', neither DELAYED nor
             REAL-TIME, in the snapshot"; WARNING naming the field and the value
     d. A REAL-TIME timeframe -> socket.massive.com, REALTIME, evidence="<field>=REAL-TIME"
     e. No timeframe found
          -> socket.massive.com, REALTIME, evidence="no timeframe in the <class> snapshot";
             INFO (WARNING for indices, whose field is documented)
     f. Transport error or rejected key -> MassiveError or AuthenticationError; nothing
        cached; the verb raises it
  6. Record the decision in m.connections().
```

Why these rules hold (verified plan tables): the unified snapshot is not included on Stocks
Basic, is delayed on Starter and Developer, and is real-time on Advanced and Business.
`last_trade` and `last_quote` are "only returned if your plan includes trades/quotes". Stocks and
Options trades over WebSocket are Developer and above (delayed) and Business plus expansion
(real-time), on `socket.` and `delayed.`. Options `AM` is Starter and above (delayed). Options
Basic has no chain snapshot (403, rule 5a) and no WebSocket. The option-chain sample shows
`underlying_asset.timeframe: "DELAYED"` beside `last_trade.timeframe: "REAL-TIME"`, which is why
`underlying_asset` is excluded. The forex and crypto WebSocket pages list only
`socket.massive.com`, with Starter real-time, so no REST call is made. The futures snapshot
documentation lists no `timeframe` (only the OpenAPI spec has it), so its envelope status is
the evidence there (**UNVERIFIED**; a live check records it). The live run of 2026-11-02 found
`last_minute`, `last_trade`, and `last_quote` timeframes `REAL-TIME` on a traded ES contract,
and none on the far-dated contracts listed before it.

The feed is the user's choice, never the library's: a subscription not listed in
`MassiveConfig.delayed_subscriptions` always streams from the real-time feed, and when the snapshot shows
that the plan's data is delayed, the verb raises at once rather than open a connection Massive
would refuse. Nothing else is guessed from the snapshot: a refused snapshot, an undocumented
`timeframe` value, or no timeframe at all connects to the real-time feed, and a refusal from
Massive then fails the refused streams with its text and the same fix. A listed subscription
always streams from the delayed feed, even on a real-time plan; whether Massive accepts a
real-time key there is **UNVERIFIED** (a live check records it), and a refusal says so.

**Base Business key, TICK family.** A Business key without a trades/quotes expansion has no
`timeframe` in its snapshot, so it streams from `socket.massive.com` like any unlabelled
snapshot (rule 5e); no host is guessed from the snapshot's `fmv` field. If Massive serves that
plan's TICK channels elsewhere, the server refuses the topic (an explicit `error` that fails
that stream and names `websocket_hosts[AssetClass.STOCKS]`), and the user pins the host there.

**Indices `V`.** The Value page lists only `wss://business.massive.com/indices`, while its plan
table says Starter delayed and Advanced real-time, and the indices `A`/`AM` pages list
`socket.` and `delayed.`. **UNVERIFIED.** Conservative rule: `V` is in the TICK family and uses
the class's TICK host. A `not authorized` reply for a `V` topic fails that stream with a message
naming `websocket_hosts[AssetClass.INDICES]` and `business.massive.com`. A fixture test covers both
replies; the live integration test must retire this note before 1.0.

### 13.3 Connection and subscription protocol

Each connection is one `_massive.WsClient` ([section 19.2](19-wire-clients-and-the-spec-snapshot.md#192-the-websocket-client)) bound to
one `wss://host/path`. The client runs on its own daemon thread with its own asyncio loop, and
owns everything about the wire: the socket, authentication, frame encoding and decoding,
reconnecting with backoff, and closing. `_ingest/ws_connection.py` owns everything the library
decides: the
desired topics, the acknowledgements, the watchdogs, the routing, and the connection's
diagnostics. Both run on the client's loop thread; other threads reach a connection only through
`loop.call_soon_threadsafe` with plain data, and through `WsClient.close()`.

The client reports to `_ingest/ws_connection.py` through four callbacks, each called on the loop thread, in order:

- `on_open(generation)`: Massive accepted the key (`auth_success`) on a new socket. A
  **generation** is one authenticated socket; the number increases by one per `on_open`.
- `on_status(generation, status)`: one decoded status object (`Status`: `kind` `ACK`,
  `REFUSED`, `MAX_CONNECTIONS`, or `OTHER`; `topics`; `message`).
- `on_data(generation, channel, rows, rejects, drift)`: one channel's data from one frame,
  decoded against `ws.json`. Each row is a tuple of the values of the channel's source's fields,
  in the order of `Source.fields`, whose long-form columns name them (the library renames
  them), with symbols as the wire spells them (`EUR/USD`, `BTC-USD`). `rejects` are the rows
  dropped because a required key is absent, null, or does not fit, one
  `Reject(channel, symbol or None, WireError, raw)` each, whose `symbol` is the raw wire value
  of the channel's declared symbol column and `raw` the whole row's JSON text (up to 2,000
  characters); `drift` is the frame's `DriftReport` ([section 19.5](19-wire-clients-and-the-spec-snapshot.md#195-runtime-drift)).
- `on_state(state, detail)`: `CONNECTING`, `RECONNECTING` (with the next attempt's time),
  `WAITING_SLOT` (with the next attempt's time; the auth reply was `max_connections`, or the
  server sent a `max_connections` status before closing an authenticated socket), `FAILED`
  (with the `WireError`; also any internal failure, such as a callback that raised, with the
  original exception as its `__cause__`), or `CLOSED` (after `close()`). `FAILED` and
  `CLOSED` are terminal, and exactly one of them is reported. The client is the only owner of
  `WAITING_SLOT`; the library displays what it reports.

`_ingest/ws_connection.py` parses no frame text. Before it first subscribes a channel on a connection, it calls
the client's loop-thread method `set_required(channel, keys, *, symbol=None)`: `keys` is a
`frozenset` of long-form key columns, built by `_ingest/ws_connection.py` from the `KeySpec`s that the channel's
dataset declares for that channel's source, excluding `from_request` keys, and `symbol` is the
long-form column of the channel's `symbol` key (`None` when it declares none). The client keeps
both across generations and hands them to the decoder, so a row missing one of those keys, or
holding a value there that does not fit, is a `Reject` carrying the row's raw symbol, while any
other absent field, or value that does not fit, becomes null. A channel with no entry has no
required keys and no symbol. Wire symbols are never rewritten in `_massive`: the flush thread
reads the rows' symbols with `WireSymbols` (`_datasets/wire_symbols.py`) when it routes them,
and topics use the wire's own form, as the subscribe frames do. It
then calls the loop-thread methods `subscribe(generation, topics)` and
`unsubscribe(generation, topics)`. Each sends at once, in
call order, at most 1,000 topics per frame (the frame-size limit is **UNVERIFIED**; the chunk
size is conservative). A call for a generation that is no longer current is discarded: the next
generation re-sends the whole desired set. Rows from `on_data` go to the per-channel row buffer;
rejects never enter a buffer and fail no stream: they are counted in the frame's drift, which
goes to the flush thread, which reports it to the session's `DriftRegistry` and counts the
drops toward `DROPPED_RUN_LIMIT` ([section 19.5](19-wire-clients-and-the-spec-snapshot.md#195-runtime-drift)).

**Topics are exact strings.** `A.*` and `AM.AAPL` are unrelated topics; nothing in the library
matches one topic as a prefix of another. On one channel, while that channel's acknowledged
wildcard is in the desired set, the channel's explicit topics are not on the wire: the wildcard
already delivers their events, and routing filters rows per stream. Whether the server would
deliver an event twice for `T.*` plus `T.AAPL` is **UNVERIFIED**. Transitions **make before
they break**, so a stream never loses data because of another stream's wildcard:

- **Adding a wildcard.** `T.*` is subscribed while the explicit `T.` topics stay on the wire.
  Only once `T.*` is acknowledged are the covered explicit topics unsubscribed. If `T.*` is
  refused, they stay, and only the wildcard's own streams fail.
- **Removing a wildcard.** The explicit `T.` topics still desired are subscribed first. `T.*`
  stays on the wire, as long as it is still subscribed, until each of them is acknowledged,
  refused, or `NO_ACK`, including the ones not yet sent (they left the wire while the
  wildcard covered them). Only then is `T.*` unsubscribed.
- During an overlap, an event may arrive twice for one round trip. That brief duplicate is
  accepted; a gap would be silent and unrecoverable.
- Covered explicit topics count toward the options cap ([section 15](15-options-contract-cap.md#15-options-contract-cap))
  even while the wildcard keeps them off the wire, so removing the wildcard never pushes a
  contract past the cap.

Responsibilities:

| Step | Owner |
|---|---|
| Open the socket (TLS, proxy, keepalive pings), read `connected`, send auth, read the auth reply, all within the handshake timeout | client |
| `auth_failed` | The client reports `FAILED` with `WireAuthError` and stops; it never retries a rejected key. The library fails every stream on the path with `AuthenticationError` (`_errors.from_wire`), whose message quotes Massive's text and adds that it can also mean the class's plan includes no WebSocket streams, not only a bad key: Massive may answer `auth_failed` to such a key too. |
| `max_connections` in the auth reply | The client reports `WAITING_SLOT`, lets the server close the socket, and retries after its backoff; streams stay alive until the outage passes `max_outage` ("Outages" below). |
| `max_connections` as a status after authentication | The client reports `Status(MAX_CONNECTIONS)`, remembers it for the generation, and when that generation's socket closes reports `WAITING_SLOT` instead of `RECONNECTING`. The library only displays the state. |
| Reconnect after every close or failure that `close()` did not request and that is not fatal, after the backoff of [section 19.2](19-wire-clients-and-the-spec-snapshot.md#192-the-websocket-client) | client |
| Classify a failed upgrade as fatal or retryable (section 19.2) | client; a fatal one is reported as `FAILED` with `WireError(status=<http>)`, which the library turns into a `MassiveError` naming the HTTP status and the host |
| Decode frames against `ws.json`; report rows, rejects (the rows whose key cannot be read), and drift | client |
| Read raw wire symbols as canonical symbols (`WireSymbols` in `_datasets/wire_symbols.py`) when routing rows; fail or keep an unreadable one (`unreadable_symbols`) | library |
| Desired topic set, reference counts, single in-flight step, wildcard transitions | library |
| Sending a step's subscribe and unsubscribe frames, and the one single-topic re-send | library decides; the client sends at once |
| Per-topic acknowledgement bookkeeping, watchdogs, the refusals | library, from `Status` events |
| Routing rows to streams; Deephaven publishing | library |

**Reconcile loop.** It runs on the connection's loop thread and is entered through
`loop.call_soon_threadsafe`, through `on_open`, and through `on_status`.

```
desired D = topics with ref-count > 0, minus deferred options contracts (section 15)
sent    S = topics subscribed in the current generation (acknowledged or not)
acked   A = topics the server acknowledged in the current generation
wire    W = D, minus the explicit topics of each channel whose wildcard is in D and in A,
            plus each wildcard that has left D but is still in S while any explicit topic
            of its channel in D is unresolved: not yet acknowledged, refused, or NO_ACK,
            whether or not it has been sent (make before break)
a step is in flight from its send until each of its topics is acknowledged, refused, or NO_ACK

no generation open (connecting, waiting for a slot, backing off)
   -> nothing is sent; reference-count changes only update D
on_open(generation)
   -> cancel the old generation's watchdogs; S = A = {}; State=CONNECTED; reconcile, which
      subscribes all of W: the full desired set is re-sent after every authentication
when a generation is open, no step is in flight, and W != S:
   adds    = W - S  -> client.subscribe, one step; S |= adds; one watchdog per added topic,
                       bound to the generation
   removes = S - W  -> client.unsubscribe after the adds: removed from S and A at once, their
                       watchdogs cancelled (unsubscribe acknowledgements are UNVERIFIED and
                       not awaited)

Status(ACK, topics)
   -> those topics are ACKED; W is recomputed (a newly acknowledged wildcard releases its
      covered explicit topics). The knowledge base shows one status object per topic, in one
      array.
Status(ACK, no topics: the message could not be parsed)
   -> WARNING once; treated as an acknowledgement of, in turn: the in-flight step's unresolved
      topics; else the topics of the re-send round awaiting its answer; else the latest
      step's topics now in NO_ACK (a late answer to a large step whose watchdogs gave up),
      until a re-send round goes out. A wrong credit only clears a NO_ACK note early.
Status(REFUSED, topic)
   -> that topic's streams fail with EntitlementError (publish_failure); the topic leaves D
Status(REFUSED, no topic)
   -> fails a stream only when exactly one topic on the connection awaits an answer: one
      unresolved topic in the in-flight step, no topic in NO_ACK, and no re-send round
      outstanding; that topic is refused. Every other case (a step of several topics, or any
      NO_ACK topic or round whose late answer it may be) is logged at INFO and fails nothing,
      since it could fail a healthy stream. The unanswered topics keep their notes, and a
      stream with rows still fails at max_outage after a reconnect, so nothing stale goes
      unmarked. See "Refusals name the feed" below.
Status(MAX_CONNECTIONS) after authentication
   -> nothing for the library to do: the client reports WAITING_SLOT once its socket closes
no acknowledgement within subscription_confirmation_timeout, the topic still in S and W, same generation
   -> client.subscribe(generation, [topic]): that topic alone, once
still none after another subscription_confirmation_timeout, same check
   -> topic state NO_ACK; it counts in NoAckTopics of m.connections(), and each stream that
      wants it shows "no acknowledgement from Massive for <topic>; its rows may be missing"
      in its Error without failing, beside its other warnings (each its own part); after a
      drop, though, it keeps its stream down until data comes ("Outages" below). One
      WARNING per step, once the step has resolved, counts its NO_ACK topics. The topic
      stays desired and subscribed. Every 60 s (NO_ACK_RESEND) one round re-sends all NO_ACK
      topics on the wire together, in one subscribe frame, until each is acknowledged, leaves
      the wire, or the generation ends. A round due while a step is in flight, or while the
      previous round still awaits its answer, waits subscription_confirmation_timeout and tries again; a sent round awaits its answer for subscription_confirmation_timeout, and no
      step goes out meanwhile. A
      late acknowledgement or a new generation clears the state and the streams' Error.
      Streams never fail on it, and the connection is never reconnected for it: a reconnect
      would cut every other stream on the shared connection over one late answer
watchdog fires but its topic left S or W, or its generation is no longer current
   -> cancelled without escalating
socket closed or failed (not by close())
   -> the generation ends; its watchdogs are cancelled; State is what the client reports,
      RECONNECTING or WAITING_SLOT, with NextReconnectAt; on the next on_open,
      W is re-sent
fatal handshake error, or auth_failed
   -> every stream on the connection fails through publish_failure (MassiveError naming the
      HTTP status and the host, or AuthenticationError); State=FAILED
shutdown callback (section 6.1)
   -> ref-count decrement; topics at zero leave D, and each cancels its watchdog
a connection with no stream attached for 30 s (no lease, so no stream opening either)
   -> closed by the connection loop (idle grace); the row is kept in m.connections() with
      State=CLOSED. A reactive stream whose watchlist is empty keeps its connection open:
      its next symbol must not reach a closed loop. The cost is an idle socket holding a slot
```

A socket that the server stops answering is detected by websockets' keepalive pings and closed,
which starts a reconnect. A server that goes silent after authentication leaves the topics of the
in-flight step unacknowledged, so they reach `NO_ACK` within 2 × `subscription_confirmation_timeout` of their send. A
server that goes silent before `auth_success` is cut off by the handshake timeout.

**Closing.** `m.close()` and the idle close call `WsClient.close()`, which is safe from any
thread and does not wait: it cancels the client's task on its loop, which interrupts a backoff
wait, a handshake, or a receive at once, and the socket, if open, is closed with code 1000. The
client thread is a daemon; `m.close()` joins it with a bounded timeout and logs a WARNING if it
has not ended. `m.close()` closes only the connections no other session leases; on a shared
one it removes the session's streams (their routes at once, their topics through the loop) and
leaves the connection to the other sessions. Removing the routes also breaks the cycle between
Python and Java that a stream's publisher, its shutdown callback, and the connection's routes
would otherwise form. A connection's loop-confined state is never touched from another thread:
the client's terminal state (`CLOSED` or `FAILED`), its last callback on the loop, clears the
connection's streams, and a call posted after it is dropped or ignored, so a post that lands
just before the loop closes cannot leave a stream behind. The flush thread clears the routes
when it ends, after its final flush.

**Outages.** An outage starts when a connection reports `RECONNECTING` or `WAITING_SLOT`. It
dates from the last frame the connection received (when data stopped, which on a half-open
socket is well before the drop is noticed), or from that report when none ever arrived; the
note and the failure name that time. The outage holds every stream on the connection *down*,
and decides recovery per stream: once the connection is `CONNECTED` again, a down stream
recovers when rows have reached it since the new socket opened, or when each of its topics
the options cap admits is acknowledged, directly or through its channel's acknowledged
wildcard. A topic in `NO_ACK` keeps down a stream that has received rows, since Massive never
confirmed it: a `last_by` on that stream would otherwise show the rows from before the drop as
current, so with neither confirmation nor data it fails at `max_outage`. A stream that has
never received a row shows no stale data: once each of its topics is acknowledged or in
`NO_ACK`, it recovers and keeps running with its no-acknowledgement note, after a reconnect as
before one. One such topic never holds the connection's other streams down. The outage ends once no stream
is down and the socket has proved itself (a topic acknowledged, or data received); a server
that accepts the key but never answers is still down, so a stream that opens on it is held
down too. A drop and a reconnect that drops again before the outage ends are one outage, with
one start; a stream that recovered in between is held down again. Throughout, every down
stream shows in its `Error`: "connection down since <time>; no data", the time in UTC as
`yyyy-MM-ddTHH:mm:ssZ`. While the connection is `WAITING_SLOT` the text goes on: "; waiting for
a WebSocket slot on <host><path>: another connection with this API key, perhaps from another
process, holds it; the plan allows one connection per cluster". Sessions in one process share
connections, so the other holder is another process, or a session whose connection settings
differ. A stream's note is cleared when it recovers; its other warnings (an options-cap note, stale
symbols) stay throughout, since each is its own part of the `Error` (section 17).

Holding a stream down arms a timer on the connection's loop for it. For the streams on the
connection at a drop, it fires `max_outage` seconds after the later of the last frame and the
drop less `KEEPALIVE_WINDOW` (40 s: websockets' default `ping_interval` plus `ping_timeout`,
which the client keeps); keepalive pings are no frames, so after a quiet night, weekend, or
illiquid symbol the last frame can be hours old while the connection was healthy until moments
before the drop. A connection that never received a frame (it never connected) counts from
the drop itself, so its streams get the full `max_outage`. A stream that opens during the
outage gets a full `max_outage` from its open. Recovery, release, or the connection's end
cancels the timer, and a stream the outage has failed is never held down again by the
outage's later attempts or by a later outage. When it fires, the stream fails
through `publish_failure` (`Flusher.fail`, so through `BlinkPublisher`) with a `MassiveError`
(`ErrorCause.EXPECTED`), one per outage so it logs once: "the WebSocket connection to
<host><path> has been down since <time>, longer than MassiveConfig.max_outage (<limit> s); the
rows from <time> on are missing, so this table stopped; call the verb again for a new live
table". Tables derived from the stream fail with it, as with every stream failure.
`max_outage=0` fails them at the drop: the timers fire on the loop's next turn. The
connection itself keeps reconnecting. So that a held-down stream always gets one full attempt
before its timer fires, the client asks the connection before each reconnect wait for its
deadline: the earliest held-down stream's time left, less `subscription_confirmation_timeout`. A wait that would start
the next attempt later than that deadline less `HANDSHAKE_TIMEOUT` is shortened to fit, but never
below `reconnect_backoff` or a 429's `Retry-After`. Without it, the default waits (30, 60, then
100 to 120 seconds) would put the fourth attempt at 290 to 348 seconds, after a 300-second
`max_outage`.

**Silence.** A `CONNECTED` connection with acknowledged topics that receives no frame for
`NO_FRAMES_WARNING` (5 minutes) during its asset class's regular trading hours logs its WARNING
([section 17.1](17-diagnostics.md#171-the-log)) and shows in the `Error` of every stream on it: "no data since
<time> during <class> trading hours; quiet symbols or a market holiday may explain it", the
time being the last frame's. The
streams keep running: a holiday or a quiet symbol list is no failure. The first frame clears
the note and logs the recovery at INFO. An outage ends the silence episode and clears its note.

**Refusals name the feed.** A refusal never moves a connection. It fails the refused streams
with `EntitlementError`, whose message quotes Massive and names the fix for the connection's
feed: on the real-time feed of an unpinned class, how to use Massive's delayed feed
(`dm.MassiveConfig(delayed_subscriptions={dm.Subscription.<X>})`), since a delayed plan's subscription is
refused there, unless the host decision's snapshot labelled the class's data `REAL-TIME`: that key's
plan is real-time, so the refused data (an add-on such as stocks imbalances, or a higher tier) is
something its plan or add-ons may not include, and the delayed feed is not suggested; when
that snapshot carried no `timeframe` at all (a base Business key's), that the plan may serve the
data on another host, pinned with `MassiveConfig.websocket_hosts[dm.AssetClass.<X>]`, and the delayed
feed only if the plan's data is delayed; on the delayed feed, that the subscription is listed in `MassiveConfig.delayed_subscriptions`
and that removing it uses the real-time feed (Massive acknowledges a real-time key's
subscription on the delayed feed; whether rows then flow is **UNVERIFIED**); on a pinned host,
`MassiveConfig.websocket_hosts`.

`publish_failure` is used only for `AuthenticationError`, `EntitlementError` (an explicit server
`error` for the stream's topic, or for `poll()` REST 403 or a `DELAYED` label on a subscription not in `MassiveConfig.delayed_subscriptions`), a fatal WebSocket handshake error
(`MassiveError` with the HTTP status and host), and the data failures of
[section 19.5](19-wire-clients-and-the-spec-snapshot.md#195-runtime-drift) (a run of `DROPPED_RUN_LIMIT` dropped rows, a symbol
spelling the library cannot read under `RAISE`). `max_connections`, closes, and `NO_ACK` are
diagnostic states.

### 13.4 REST

One host, one REST client per session ([section 19.1](19-wire-clients-and-the-spec-snapshot.md#191-the-rest-client)), shared by every
caller thread, poller, and fan-out worker. Every REST dataset reaches its paths through it. The
REST transport turns each `WireError` into a public exception with `_errors.from_wire`, which
keeps the message and fills `status` and `request_id` ([section 3](03-exports.md#3-exports)). What a user
sees:

- **HTTP 401** raises `AuthenticationError`. **HTTP 403**, and a 200 response whose envelope
  `status` is `NOT_AUTHORIZED`, raise `EntitlementError` from `fetch()` and `history()` and fail
  the table from `poll()`. The message names the dataset and the subscription the request is
  charged to ("your account" for data of none), then Massive's answer, and, where the dataset
  declares one (`DatasetSpec.refused_alternative`), the dataset to try instead: the class's
  snapshots for its last trade or last quote.
- **Delayed data on a real-time subscription.** A `fetch()` or `poll()` of a dataset of current
  data (`DatasetSpec.serves_current_data`: the snapshots, the last trade and quote, the movers,
  the option chain, the unified snapshot, the forex conversion) is checked. So is an
  indicator's latest value, which Massive computes from the latest bars
  (`DatasetSpec.latest_is_current`): its `poll()`, and a `fetch()` whose window reaches the
  present (no `end`, or an `end` on or after the current day or instant), are checked; a
  `fetch()` of a past range is not. Datasets of past data (`previous_day_bar`, a
  `daily_open_close`, dividends, splits) are not checked, since Massive labels them `DELAYED`
  on a Basic or Starter key though their rows are final. Each
  label Massive puts on current data is held against the subscription of the data it labels. A
  page charged to a subscription (its rate-limit bucket names one) is that subscription's
  data. A page of the `other` bucket that is not a reference endpoint, such as the unified
  snapshot of several classes, is each row's own: a row's subscription is its ticker's class's
  (`symbol_class`, then `subscription_of`). A page whose envelope `status` is `DELAYED` is
  refused unless every class the request names is listed in
  `MassiveConfig.delayed_subscriptions` (a request that names none, such as economy data, has
  nothing to refuse); a row whose `timeframe`, `last_trade.timeframe`, `last_quote.timeframe`,
  or `last_minute.timeframe` is `DELAYED` is refused unless its subscription is listed.
  Forex and crypto are never held to a label: Massive has no delayed currencies feed, and
  `delayed_subscriptions` cannot list `CURRENCIES`, so a refusal could name no remedy. A
  refusal raises `EntitlementError` from `fetch()` and fails the table from `poll()`, naming
  the dataset, the tickers of the refused rows (the first five, then a count), the field that
  said `DELAYED`, and the fix (`dm.MassiveConfig(delayed_subscriptions={dm.Subscription.<X>})`).
  `underlying_asset.timeframe` is never read: Massive's option-chain sample shows the
  underlying delayed beside a real-time contract. Reference endpoints are never checked.
  REST cannot ask for delayed data, so a listed subscription's labels pass and it returns what
  the plan serves, which the table's description says. Nothing is shown as real-time without
  proof: a `poll()` table's `Recency` is `UNKNOWN` (null for data of no single subscription)
  until a cycle's page carries a label, then `REALTIME` or `DELAYED` (section 17), and no
  description claims real-time data. A `fetch()` and a `poll()` table of the same current data
  carry the same recency sentence in their descriptions. `history()` reads past data and is never checked.
  Futures snapshots document no `timeframe`, so their envelope status is the evidence
  (**UNVERIFIED**, item 69 of [section 23](23-open-verification-items.md#23-open-verification-items)), and so is that of an
  option chain on a delayed plan (item 71).
- **HTTP 429** pauses the request's subscription and resumes it, as the rate limits below
  specify; only when a pause is longer than the read's patience still covers (10 minutes of
  pauses, or 90 s while the read holds the update graph's lock) does the client raise
  `WireError(status=429)`, which becomes `MassiveError("rate limited: Massive is refusing
  stocks requests ...")` with `status=429`.
- **HTTP 5xx** and connection failures are retried; if they persist they raise `MassiveError`
  naming the host and the cause. A TLS certificate failure is not retried and names
  `SSL_CERT_FILE`. Every other status, and a 200 envelope whose `status` is `ERROR`, raises
  `MassiveError` with the server's `message`.
- **A body that is not valid JSON** (an HTML page from a proxy, a truncated body, bytes that are
  not UTF-8) raises `MassiveError` on any call or page, so a malformed response never becomes an
  empty or truncated dataset.
- **A missing result key** in an envelope whose `status` is `OK` or `DELAYED` is an empty page
  (Massive omits `results` when a range has no data); with any other `status` it is an error, as
  above.
- **Fan-out and missing symbols.** A symbol with no data returns no rows, never an error and
  never an option: in a per-symbol request series (`ticker_details`, `daily_open_close` on a
  holiday, `/prev`, the "last" endpoints, a symbol's `history()`), HTTP 404 on the first
  request of a request series drops that series, logged at DEBUG; one WARNING per read lists
  the dropped symbols not warned about before in the session, through `bounded_list` (the
  aggregates and tick endpoints answer an unknown ticker with an empty 200 instead, and a flat
  file simply has no rows for it). "First request" means each series's own: futures trades and
  quotes named by dates are one series per symbol and session, so a 404 on a later session,
  after earlier sessions returned rows, drops only that session, named on its own in that
  WARNING ("ESH5 for the session 2025-01-22"), since a session can have no data. HTTP 404 on a
  later page of one series means its data was cut short and raises `MassiveError` naming the
  symbol, the path, and the pages received. Every other error fails the call.
- **`poll()` cycles.** A cycle that raises `EntitlementError` or `AuthenticationError`, or hits
  a data failure of [section 19.5](19-wire-clients-and-the-spec-snapshot.md#195-runtime-drift) (a page whose every row was dropped,
  or a null or NaN `poll_keys` value, in every mode), fails the table through
  `publish_failure`. By default one symbol never fails the whole table
  (`MassiveConfig.unavailable_symbols`, `LOG`): a reactive symbol table's symbol routed to a
  path that lacks a parameter the caller set (`adjusted=False` on a crypto indicator) is left
  out of each cycle, with one WARNING per session naming the path and the parameter, and a
  symbol Massive answers with HTTP 404 has no rows (a `REPLACE` table drops its rows); under
  `IGNORE` they log only at DEBUG, and under `RAISE` either fails the table. Each good cycle
  lists both kinds in the stream's `Error` ("no rows this cycle for
  <n> symbols: X:BTCUSD (/v1/indicators/sma/{cryptoTicker} does not take ['adjusted']); MSFT
  (HTTP 404: no data)", the first ten and a count), and clears the list once none is left out,
  so a table never loses rows without saying why.
  `poll()` checks synchronously whatever does not depend on the symbols: with static symbols
  every path, with a reactive table the parameters against the dataset's fetch paths, at
  least one of which must take every one of them. An emptied symbol table plans no request.
  Any other error sets the stream's `Error` in `m.streams()` to the error followed by "; last
  good cycle at <time>" (or "; no good cycle since the poll started at <time>"), keeps the
  previous rows, and the next cycle runs as scheduled. The first good cycle after a run of
  failures clears that `Error` and records the run in the stream's `Gaps`, from its first
  failed cycle's start (that cycle published nothing, even when pages arrived before it
  failed) to the good cycle's start, the span over which rows may be missing, and logs it at
  INFO. A run of failing cycles begins with
  the first failed cycle after a good cycle, at that cycle's start, or at its latest page when
  it received one; a failure that comes `max_outage` seconds or more after the run began
  fails the table instead, in every mode, with a `MassiveError` of the latest
  error's cause: "<dataset> poll(): last good cycle at <time>; every cycle since <time> has
  failed, for <n> s, longer than MassiveConfig.max_outage (<limit> s); the rows since then may
  be missing, so this table stopped; call the verb again for a new live table. The latest
  error: <error>". Counting from the run's start, not the last good cycle, keeps one failure
  of an hourly poll, or of a slow cycle that kept receiving pages, from failing it. A cycle
  that succeeds with no new row is a good cycle, so a quiet dataset never fails.
  A running cycle blocks its poller, so the session's `poll watch` thread checks the running
  cycles every second (section 18.1). A cycle still running past its interval sets the
  stream's `Error` to "cycle running since <start>; last good cycle at <time>" (or "; no good
  cycle since the poll started at <time>") until it ends, whose own update replaces the note.
  A cycle past its interval that has made no progress (received no page) for more than
  `max_outage` seconds, counted from its start, or from the start of the run of failing
  cycles it continues, until its latest page, fails the table with a `MassiveError`
  (`ErrorCause.EXPECTED`): "<dataset> poll(): last good cycle at <time>; the cycle running
  since <time> has received nothing for <n> s, longer than MassiveConfig.max_outage (<limit>
  s); the rows since then may be missing, so this table stopped; call the verb again for a
  new live table". The stall must also exceed the one-second spacing of the checks, and with
  `max_outage=0` no running cycle fails this way (a failed cycle still fails at once). A cycle
  that keeps receiving pages, however slowly its requests are paced or paused, never fails
  this way, so a Basic fan-out of twenty minutes keeps running, marked
  late.
  (an expected cause) or ERROR (a defect, with its trace), later ones log at DEBUG, and the
  first cycle that succeeds again logs the recovery at INFO. A cycle that ends later than its
  interval, because its requests are paced or held by a rate-limit pause, logs one WARNING per
  run of late cycles, naming `m.rate_limits()`, and the first cycle on time again logs at INFO.
- **Drift.** `to_batch` returns each page's `DriftReport`; the transport routes it to the
  session's `DriftRegistry` (one WARNING per (dataset, transport, field), and `SchemaDrift` in
  `m.streams()` for `poll()`). Static verbs report drift only in the log.
- **`adjusted`.** The bars `adjusted` flag travels in its own typed field, `Request.adjusted`,
  which the REST transport sends, when not None, as the `adjusted` query parameter (`true` or
  `false`); `params` never holds `adjusted` (`build_query` rejects it there), so a request for
  unadjusted bars cannot silently fall back to Massive's adjusted default. Each page of bars
  sent with an explicit `adjusted` must echo it: a different echo raises `MassiveError`, and a
  page without one logs a WARNING once per dataset, path, and session
  ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries), "Price adjustment").
- **Time columns.** Every page's rows get the library's time columns (`row_times.TimeRule`),
  with `TimestampInsert` the instant the page arrived, after the stamps are checked and the rows
  outside the window dropped. The day zone of each call follows its own symbols' class, so a
  crypto indicator's `Date` is a UTC date and a stock indicator's a New York date.
- Every error message names the HTTP status, the path, and the response's `request_id`, and is
  logged with the same text at the level its cause gives ([section 17.1](17-diagnostics.md#171-the-log)); the API
  key never appears in a message or a log.

**Rate limits.** Massive limits REST requests per subscription, not per account or per key: it
sells five subscriptions (`Subscription`: stocks, options, indices, currencies for forex and
crypto, futures), each on a tier of its own; the free Basic tier allows 5 requests per minute
and every paid tier has no request limit, and every API key of the account shares the
counters. A 429 whose message says "You've exceeded the maximum requests per minute" marks a
Basic subscription. There is no way to ask which tier a subscription is on.

- **Buckets.** Every REST request is charged to one bucket: the subscription of the class it
  reads (`subscription_of(dates.day_class(spec, its symbols' classes))`: the dataset's class,
  or for a dataset with a path per symbol class the class its request's symbols share), and a
  host-decision probe to the subscription of the class it decides, whatever path it reads.
  The futures calendar's contract and schedule lookups go to `futures`, each in a lane of its
  own under a token the calendar's close sets. Reference data and the other datasets Massive
  assigns to no subscription (economy, partner data, the unified snapshot) go to the `other`
  bucket, whatever their filters (the unified snapshot's delayed-data check still holds each
  row to its own class's subscription, as above): Massive's knowledge base ("What is the request limit for
  Massive's RESTful APIs?", vendored as
  `vendor/massive/knowledge-base/what-is-the-request-limit-for-massives-restful-apis.md`)
  says reference and cross-asset endpoints are counted in a bucket of their own. So `m.reference.tickers(market=dm.Market.CRYPTO)` goes to `other`, as does
  the session's own read of Massive's crypto ticker list, its pages in one lane of their own.
  `other` cannot be declared, since no setting names it, but it is paced like a subscription
  once a 429 names the per-minute limit.
- **Sharing.** One `RequestGovernor` ([section 19.1](19-wire-clients-and-the-spec-snapshot.md#191-the-rest-client)) per (SHA-256 of the
  API key, REST scheme, host, and port) serves every session of the process that uses them
  (`_ingest/rate_limits.py`, `GOVERNORS`). The registry keeps the digest, never the key. A
  bucket is paced when any live session sharing it declares the subscription in
  `rest_basic_subscriptions` (the stricter wins), or once it is learned; two sessions that
  each declare a non-empty, different set log one WARNING naming both and the union. A
  governor without a lease is kept until it holds no slot and no pause, so a session created
  again soon after does not start with an empty window; what it learned goes with it, so a
  governor created again starts unpaced and relearns a Basic subscription from its first
  refusal (one refused request and one WARNING).
- **Pacing.** A paced bucket lets a request start only while fewer than 5 slots are held; a
  slot is held by a request in flight, and by a finished one until 61 s (the minute plus one
  second of margin) after its response arrived. Massive's minute is the clock minute, not a
  sliding window (the live check of 2026-10-30, item 67): no clock minute holds more starts
  than a 61 s window, so the hold never draws a refusal for pacing and is at worst a minute
  slower. It does not wait for the next minute instead: that trusts the local clock to agree
  with Massive's to the second, and a fast clock would draw a 429 and a whole window's pause. A request that never reached Massive (a connection
  could not be made, the proxy or its tunnel failed, including a TLS handshake with the proxy
  and HTTP 407, or its read was cancelled just as it was admitted) frees its slot at once; a
  TLS error without a proxy keeps it, since it may come after the request left. Waiting for a slot never times out: a 250-request history at 5 per minute takes about
  50 minutes, and the caller's interrupt, a failed sibling call of a fan-out, a stopped poller,
  or `close()` ends the wait (each read has a `Cancel` token of its own, set when a fan-out
  ends, following its poller's).
- **Lanes.** The requests waiting on a bucket wait in lanes: one per read (a `fetch()`, a
  `history()`, one `poll()` cycle), one per host decision, and one per call that names none.
  Within a lane they start first in, first out; the lanes take turns, round-robin, so the next
  slot goes to the next lane, whatever its number of threads. A host decision or a `poll()`
  cycle behind two long histories of eight workers each waits for one request of each, not
  sixteen.
- **Pauses.** A 429 pauses its bucket for every session sharing it: for its `Retry-After`
  (seconds, or an HTTP date measured against the response's own `Date`), up to 10 minutes;
  without one, or with one of zero or in the past, a paced bucket for one whole window (61 s) and an unpaced one 15,
  30, then 60 s by the refusals of the run, so a request is never sent again at once. A pause
  only grows. When it ends, one request of the bucket goes first as a probe while the others
  wait for its answer: an answer other than 429 releases them, and a 429 pauses again. So a
  subscription whose 429s do not say it is Basic still sends one refused request per pause,
  never a burst. Only the first 429 to requests started after the latest pause counts toward
  the run (a burst in flight is one refusal); a 429 to an earlier request that lengthens the
  pause starts a run if none is going. The run ends with the first answer other than 429 to a
  request started after the latest pause, or once more than a window (61 s) has passed since
  its pause ended with no refusal and no probe in flight (the read was cancelled or the
  session closed), so a 429 hours later starts a new run while a slow probe's 429 stays in
  its run. A 429 is never counted as a retry.
- **Giving up.** The bucket never gives up: it keeps pausing and probing for as long as
  Massive refuses it. Each read has a *patience*: the most time it waits out pauses, shared by
  all its requests, on every thread of a fan-out and every path of a read that spans several.
  It is 10 minutes (`READ_PATIENCE`), or 90 s (`CONSOLE_PATIENCE`) when the read's thread
  holds the Deephaven update graph's lock, as a console command does, since no table of the
  process updates meanwhile; the lock is checked on the read's thread when the read starts,
  outside every library lock. Only pause time counts, each stretch of a pause once however
  many of the read's requests wait it out: a read paced at 5 requests a minute for an hour
  spends none of it on slot waits. A request whose pause the patience cannot cover fails at
  once with `MassiveError("rate limited: ...")`, status 429: it names the subscription's
  requests and the latest refusal; says when the next one goes, in UTC and in local time;
  gives the patience and how much the read has spent; says the usual cause is another
  program or process using the same Massive account (the sessions of one process share one
  governor); and gives the remedies: retry after that time, stop the other program, and, for
  a subscription, declare it Basic or upgrade it (none of these for `other`). A read with the
  console's patience adds one more, the patience's `advice`: "this read ran inside a Deephaven
  console command, which waits out only 90 s of pauses; pass wait=False to fetch() or
  history() to read in the background, which waits out 10 min". Only that read
  stops; the requests of other reads, a stream's host decision and other sessions included,
  keep their own patience. Every lookup the session shares between the callers that need it
  at once (the crypto ticker list and each WebSocket host decision) follows the same rule. A
  caller that joins one another caller is making sends no request of its own: it charges the
  pauses of the lookup's bucket to its own patience (`RequestGovernor.charge_pause`, woken by
  a listener of that bucket alone, `subscribe_bucket`) and gives up with the same error, while
  the lookup goes on for the caller making it. If that caller stops for a reason of its own
  (its patience ran out, or it was interrupted, as by Ctrl-C), no waiter fails with it: each
  tries again with the patience it has left, so one of them makes the lookup and the others
  wait on that one. Any other failure reaches every waiter, each raising its own copy. No
  setting changes the patience.
- **Learning.** A 429 whose message names the per-minute maximum makes its bucket learned
  Basic, paced from then on, for the governor's life (see Sharing); any other 429 only
  pauses. `other` learns as a subscription does.
- **Warnings and notices.** The first 429 of each bucket in a session logs one INFO line, naming
  the session and the refusal (status, path, `request_id`, Massive's message) and saying what
  it means: for a declared subscription, that other sessions, processes, or keys of the account
  use the same limit; for a learned one, that the Basic tier allows 5 requests per minute, that
  its requests are now paced, and the `rest_basic_subscriptions` declaration that paces them
  from the first request; for any other, that requests pause and resume and that a paid
  subscription that keeps getting 429s should give Massive's support the request id. A
  static read logs one INFO line per bucket before its first request when its paced requests
  cannot all start within a minute ("stocks.trades fetch: 250 requests; paced at 5 requests a
  minute (stocks); at that pace they take at least 49 min", a lower bound from the governor's
  state). Made on a thread that holds the UG lock (a console command), which pauses every live
  table of the worker while the read runs, that line is a WARNING instead: "stocks.bars
  history: 250 requests; paced at 5 requests a minute (stocks); this command will pause every
  live table in the worker for at least 49 min: pass wait=False to read in the background".
  And
  a grouped-daily or per-session download of more than five requests of an unpaced
  subscription names what the Basic tier would cost ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)).
  A `poll()` whose cycle makes more paced requests than its interval allows (each request
  holding its slot 61 s) logs one WARNING for the table ("each cycle makes 3 stocks requests,
  but they are paced at 5 per minute, so a cycle takes at least 37 s, longer than the 30 s
  interval; cycles run late").
- **Diagnostics.** `m.rate_limits()` shows every bucket ([section 17](17-diagnostics.md#17-diagnostics)).

How a request is built from a `DatasetSpec` and a `Request`:

- **Path parameters.** The symbol fills its own segment (`{stocksTicker}`, or `{from}` and
  `{to}` of a pair). Every other path parameter (`direction`, `from`, `to`, `amount`,
  `precision`, `date`) travels in `Request.params` under its wire name and fills the path's
  `{name}` segment; it is not sent again as a query parameter. A one-day path parameter is
  turned into a calendar date by `calendar_day` in `_datasets/dates.py`: a date or
  `"YYYY-MM-DD"` as given; a `datetime` raises `ValueError`, since a calendar date is a whole
  day ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)).
- **History order.** REST `history()` returns each request's rows, so each symbol's, oldest
  first; the rows of several requests follow one another in request order (futures sessions
  named by date: session by session, and the symbols of one session in symbol order).
  Wherever the history path takes a direction and the caller set none, REST sends the ascending one on
  the time axis's field when the path lists it (`order=asc` with `sort=timestamp`;
  `sort=asc` on the aggregates; `sort=window_start.asc` or, where `sort` lists no values,
  `<time axis field>.asc` on the futures paths, which default to newest first). The order of
  rows inside a flat file is Massive's and is not documented.
- **Library-fixed parameters.** A parameter the library sets itself, with no argument of its
  own (the indicators' `expand_underlying=false`), is declared on the dataset as
  `DatasetSpec.fixed_params`. The catalog's `wire parameters` gate requires every REST path of
  the dataset to take it as a query parameter, so it is never blamed on the caller as a
  parameter a path lacks. `build_query` sends it like a typed parameter, and its whole
  family is reserved from `filters` ("the library
  sets type itself; it is not a filter") and left out of the valid keys a refused filter
  lists. An equality form of the symbol parameter in `filters` (`ticker_any_of`) is refused
  with a message naming `symbols=`.
- **Path values.** A path value is percent-encoded whole (`I:SPX` is `I%3ASPX`) and may not
  contain `/`: Massive answers 404 for a `%2F` segment, and no canonical symbol has one, so
  `request_target` raises `ValueError`. A `NUMBER` value is sent as an `int`'s digits or a
  `float`'s shortest exact decimal in positional notation (`1e-7` is `0.0000001`, `1e20` is
  `100000000000000000000`), never with an exponent; a non-finite value raises.
- **Time axis.** `start` and `end` go to the `time_axis` pair by the rules of
  [section 9](09-filters.md#9-filters) and [section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries): instants
  on an instant axis (decided by the generated row field the parameter filters, so a
  date-typed `published_utc.gte` gets an instant), whole-day dates on a calendar-date axis.
  Each request's dates, its axis and its date filters on instant parameters, follow the
  request's day zone ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)): the dataset's
  class, or for a dataset with a path per symbol class the class of the request's own symbols,
  so a crypto indicator's `start="2025-03-03"` is UTC midnight and a stock indicator's New
  York midnight.
- **Currency pairs.** A per-symbol request on a path whose `{from}` and `{to}` nothing else
  fills (crypto's daily open-close, whose rows carry their own `symbol`) takes them from the
  request's symbol, split into its two currencies, wherever the dataset's symbol key comes
  from; the rows' symbol is made canonical as usual.
- **Bars ranges.** A bars spec whose range path has `{from}` and `{to}` segments declares no
  `time_axis`, and `history()` fills those segments as epoch milliseconds: `{from}` is the first
  instant that belongs to the first bar whose whole window fits the range (a week's Sunday, the
  1st of a month) and `{to}` the last whole millisecond before the
  range's end ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)); the bars that do not fit
  wholly, or have not ended, are dropped afterwards.
  `futures.bars`, whose path `/futures/v1/aggs/{ticker}` has no such segments, declares the
  instant pair `window_start.gte`/`window_start.lt` instead.
- **Futures sessions.** Futures trades and quotes named by session dates are requested one
  weekday session at a time, `session_end_date=<date>` with no time window; futures bars named
  by session dates are requested on `window_start` and kept by their session label and window
  ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)). Grouped daily skips weekends for the
  classes that do not trade then. A long download is announced once, at INFO, before the
  first request (the rate limits below).
- **Day stamps.** Bars of a day or longer from `/prev`, the range, and grouped daily, of every
  class but futures, start at 00:00 of their day in the class day zone: every known stamp is
  moved there, and one Massive is not known to use for the class is kept as sent and logs one
  WARNING per form of stamp per session naming the path, the symbol, and the stamp
  ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries), decisions 70 and 85). Bars are stamped before
  `keep_window` drops the rows outside the window, so an unknown stamp outside it still warns.
- **Symbol batching.** `DatasetSpec.symbols_per_request: int | None = None` caps the symbols
  of one request when the symbol parameter is list-valued (`ticker.any_of`). When it is set
  (250 on the unified snapshot, the indices snapshot, and the stocks, forex, and crypto
  snapshots), the transport splits the symbol list into batches of at most that size, issues
  them in order, and concatenates the rows; None sends every symbol in one request.
- **A path per symbol class.** `DatasetSpec.fetch_paths_by_class: Mapping[AssetClass, str]`
  (empty by default) names the fetch path that serves symbols of one class, for a dataset that
  spans classes whose vendor templates differ (the indicators:
  `{AssetClass.CRYPTO: "/v1/indicators/sma/{cryptoTicker}"}`). Each symbol's class is
  `symbol_class(symbol)` in `_datasets/symbols.py`; a class without an entry uses
  `fetch_path`. Each such path is a source of the dataset and declares its `keys` like any
  other.
- **Only declared parameters are sent.** A request carries only the parameters its chosen
  path declares in its generated parameters. A typed parameter the chosen path lacks raises
  `ValueError` naming the parameter and the path, rather than being dropped silently.
- **Per-symbol detail paths.** A dataset with both a list path (`fetch_path`) and a per-symbol
  detail path (`DatasetSpec.detail_path`; `options.contracts`:
  `/v3/reference/options/contracts` and `/v3/reference/options/contracts/{o}`) calls the
  detail path once per symbol when symbols are given and the list path when they are not. The choice follows from the spec's role
  paths, never from the dataset's name.
- **Row symbols are canonical.** A `symbol` key read from the rows, not from the request,
  goes through `WireSymbols` (`canonical_from_wire(spec.asset_class, value)`), exactly as
  WebSocket rows do, so forex conversion's `AUD/USD` becomes `C:AUDUSD` and matches the symbol
  the caller passed; a spelling it cannot read follows `unreadable_symbols`
  ([section 10](10-symbols-and-all.md#10-symbols-and-all)).
- **Error rows in multi-ticker snapshots.** A snapshot of several tickers answers a ticker it
  cannot serve (`NOT_FOUND`, `NOT_ENTITLED`) with a row carrying `error` and `message`.
  `snapshots()` keeps such rows, with their `Error` and `Message` columns, and logs each at
  DEBUG; one WARNING per page lists the tickers not warned about before in the session, with
  Massive's message, through `bounded_list`.
