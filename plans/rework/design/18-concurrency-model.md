## 18. Concurrency model

Free-threaded CPython (`PYTHON_GIL=0`) is a gating target. The library's locking must be correct
without the GIL.

### 18.1 Threads

| Thread | Created by | Does | Must not |
|---|---|---|---|
| caller (console, script, user thread); a console command holds the UG's exclusive lock throughout | user | verbs; the host-resolution REST call; Deephaven table creation inside the result scope; registry mutation; waits, outside every library lock and only when it does not hold the UG lock, for another thread's open of the same stream key; fails a new stream whose connection failed while it opened (`publish_failure`, outside every library lock) | hold the registry lock while calling Deephaven or waiting; while holding the UG lock, wait for an update cycle or on a thread that may need the lock (the diagnostics writer, a poller, another caller's stream open), or join a closing connection |
| WS client, one per connection | library | `asyncio.run` of the connection's `WsClient` (section 19.2): socket, handshake, frames, backoff; decode each frame once against `ws.json`; route status events, rejects, and drift; append rows to per-channel `SimpleQueue`s; the reconcile loop and watchdogs; idle close | any JVM call; blocking waits |
| flush, one per connection | library | every `flush_interval`: drain queues, Arrow to Java once per channel, `where_in` and `publisher.add` per stream, `publish_failure` of failed streams; enqueue diagnostics row snapshots | touch loop-owned state directly |
| futures schedules, one per connection group with a futures stream (`FuturesCalendar`) | library | REST requests of `/futures/v1/contracts` and `/futures/v1/schedules` through the REST client of any session of the group still open, charged to the `futures` bucket, each lookup in a lane of its own that the calendar's close ends; swaps in each new map whole under its leaf lock; closed when the group's last session leaves | call Deephaven; hold its lock while it waits on the network |
| poller, one per `poll()` table, plus a `ThreadPoolExecutor(max_concurrency)` for fan-out | library | REST calls, waiting in the governor for a slot or a pause (ended by its stop token); `Table.snapshot()` of a reactive symbol table; `publisher.add` | run inside a listener |
| poll watch, one per session while it has a live `poll()` table | library | every second, checks each running cycle: marks one past its interval in its row (`update_streams`), and fails a late one that has received no page for `max_outage` (sets the poller's stop token, then `publish_failure` through the poller's publish lock) | wait on a poller; hold a poller's watch lock across anything but `update_streams` |
| background read, one per `fetch(wait=False)` or `history(..., wait=False)`, ended when the read lands, fails, or is cancelled | library | the transport call `wait=True` makes (REST requests, with their fan-out pool, or the flat-file reader pool), listening to the read's stop token; the static conversion; `publisher.add` of one row that holds the static table, or `publish_failure`, under the read's publish lock; `SessionCore.finish_read` | hold the UG lock; take `registry_lock` across a Deephaven call |
| REST fan-out, a `ThreadPoolExecutor(min(max_concurrency, calls))` per static read that makes several calls (`fetch()` per symbol, REST `history()`), ended before the read returns | library | one call each: its pages through the session's `RestClient`, waiting in the governor for a slot or a pause, then for one of the client's `max_concurrency` connections; each page first checks the read's `Cancel` token, which the read sets on its first failure or an interrupt, so the other calls stop after the page already on the wire | take `registry_lock`; call Deephaven; outlive the read |
| flat-file reader, a `ThreadPoolExecutor(min(max_concurrency, keys))` per `history()` call, ended before the call returns | library | S3 requests through the session's `FlatFileClient`, each waiting for one of its `max_concurrency` connections, which an open file keeps until it is closed; decompressing the file and cutting it into chunks, which a pool of three parse threads per file parses with Arrow (no Deephaven call there); `deephaven.arrow.to_table` of static tables, inside the session's execution context; checks the call's cancel event and the client's closed flag before each request and after each block | take `registry_lock`; outlive the call |
| follow coordinator, one per session from its first followed read (`fetch()` or `history()` of a refreshing symbol table) until the session closes or is collected | library | owns all per-symbol state of the session's followed reads: takes the symbol tables' changes, the reads' outcomes, and the stops from one `SimpleQueue`; starts reads on the follow read pool (flat-file scans of one result one at a time); times the retries; publishes each symbol's cell (`publisher.add`) and a failure of the whole table (`publish_failure`, after `SessionCore.fail_stream`) under the result's publish lock, checking its failed flag; queues `m.streams()` and `m.symbol_reads()` row snapshots | any I/O; wait for an update cycle or on a read; take `registry_lock` across a Deephaven call; let another thread touch per-symbol state |
| follow read pool, a `ThreadPoolExecutor(max_concurrency)` per session, made and ended with the follow coordinator | library | one followed read each: the transport call `wait=True` makes, in the session's execution context, listening to its result's stop token; the static conversion; enqueues the outcome for the coordinator | publish; touch per-symbol state; take `registry_lock` across a Deephaven call |
| reaper, one per session | library | scope releases (`release_scope`, under the UG shared lock, outside every library lock): results, streams, and failure pins; stream held-result counts; for a session freed without `close()`, the session's closers, which close the connections no other session leases and wait up to `JOIN_TIMEOUT` (5 s) for each one's threads; stopping a shut-down stream's watchlist listener; failing a stream whose watchlist failed; usually the final release of a core that left the live set | anything else |
| diagnostics writer, one per session | library | adds row snapshots from its queue to the blink tables under `connections()`, `streams()`, and `symbol_reads()` through their publishers: each batch coalesced to the latest row per key, one `publisher.add` per table, batches at least `WRITE_INTERVAL` (0.1 s) apart, never waiting for an update cycle; pulls the `rate_limits()` rows from the session's governor when told it changed, and when its queue wait times out at the rows' next time-driven change; at exit, releases the tables' scope under the UG shared lock | anything else |
| symbol changes, one per session, started by the first futures or crypto watchlist change | library | for futures, waits up to 15 s for the schedules of the contracts a symbol table adds (`SCHEDULE_WAIT`); for crypto, reads Massive's crypto ticker list (REST requests, once per session, charged to the `currencies` bucket) for each crypto ticker a symbol table adds that the session's connection group has not looked up (the group keeps the pairs, vendor facts shared by its sessions), retrying a failed read for the change's whole batch after 2 s and 8 s, one read per try, and skipping a ticker it cannot split (its `Error` part in `m.streams()`); a lookup error whose cause is `LIBRARY_DEFECT` is neither retried nor skipped; any unexpected exception of a change is logged with its trace and fails the stream (`Flusher.fail`), a `LIBRARY_DEFECT` `MassiveError` as it is and any other wrapped in one; `m.close()` ends a wait for schedules or for a retry at once, and the change is dropped (DEBUG); then posts the change to the connection's loop; changes run one at a time, in order, so a removal never overtakes an addition still waiting | hold `registry_lock` or the group's lock across the request; call Deephaven |
| UG thread (Deephaven) | Deephaven | watchlist listeners (`listen`): compute symbol adds and removes and hand them to a worker; a watchlist row that is not canonical, or a failed watchlist, queues the stream's failure on the reaper | Deephaven table operations; blocking I/O; waiting on library threads |
| Java-originated (shutdown callback, UI export release, liveness cleanup) | Deephaven | `SessionCore.stream_shut_down` and `SessionCore.worker_shut_down` (under the registry lock: unmap the key; delete the record, or mark it for the last hold to delete); queuing work on the reaper; `call_soon_threadsafe` | sends; JVM calls; joins; blocking |

Every thread and pool the library creates is named `massive #<session> <role>`, or
`massive group #<group> <role>` for a connection group's, so each log line names its session or
group and its call or stream ([section 17.1](17-diagnostics.md#171-the-log)); `tests/test_layout.py`
checks that every `threading.Thread` has a `name` and every `ThreadPoolExecutor` a prefix.

### 18.2 Shared state and ownership

- `SessionCore.registry_lock` (a `threading.RLock`) guards every mutable registry of a
  session: streams by key, results by id, polls, diagnostics holds. It is a
  **leaf lock**: no Deephaven call, no network call, no blocking wait, and no wait on a
  `call_soon_threadsafe` result while holding it. Per-stream state is a frozen dataclass replaced
  atomically under the lock; readers copy references out and release the lock before working.
- Two locks of the WebSocket flush are not leaf locks, and are never taken under
  `registry_lock`. A caller holding the UG lock (a console command) takes the publish lock
  when it fails a stream (`close()`, or a connection that failed while the stream opened),
  which is safe because nothing done under either lock takes the UG lock or waits for an
  update cycle: `where_in` of static tables, `publisher.add` of a static table (a snapshot
  that needs no lock), `publish_failure`, and `SessionCore.fail_stream`:
  - each connection's **flush lock** serializes its flushes (the flush thread's, and any a test
    calls); a flush calls Deephaven under it;
  - each stream's **publish lock** serializes `publisher.add` and `publish_failure` for the
    stream, which two flush threads can briefly share when a connection moves, the caller
    thread when a stream's connection failed while it opened, the closing thread at
    `m.close()`, and the reaper when a stream's watchlist failed. Failing a stream takes
    `registry_lock` under it (`SessionCore.fail_stream`).
- The WebSocket connections shared by the sessions of one API key and settings have their own
  lock (a `threading.RLock`, one per group of sessions), which guards the connections by URL,
  the connection of each stream, each connection's leases and sessions, its failure, and its
  options-cap bookkeeping. It is a leaf lock like `registry_lock`, and the two are never held
  together.
- The other locks are leaf locks too, each guarding one thing and making no Deephaven,
  network, or blocking call while held:
  - the live set of session cores ([section 6.1](06-massivesession-and-lifecycle.md#61-how-a-live-table-is-kept-alive-and-released)),
    taken only under `registry_lock`;
  - the process's registry of shared connection groups, taken to find or create a group when
    a session opens its first stream and to drop it when its last session leaves;
  - the REST transport's host-decision cache, which holds one future per (class, family);
    callers wait on a future outside every lock (§ 13.2);
  - `FlatFileClient`'s lock, one per session, which guards the creation and the close of its
    boto3 S3 clients, one per connection slot, and the list of idle ones (creation reads local
    AWS configuration but makes no request, since the credentials are given); its closed flag
    is a `threading.Event`, read without the lock by the reader threads. The only lock taken
    under it is the model-loader lock below, while a client is built. The session's `SessionClients` creates the `FlatFileClient` on the
    session's first flat-file read, under `registry_lock` (the constructor only stores
    settings), and registers its close after releasing the lock;
  - the flat-file model-loader lock, one per process, held while an S3 client is built. Every
    S3 client shares one botocore model loader, created when the module is imported, filled by
    the first build, and kept for the life of the process (about 12 MB of parsed S3, endpoint,
    and retry models), so the files are parsed once rather than once per client (about 100 ms
    each). botocore's loader fills its cache without a lock, merges S3's extra definitions
    into a cached model in place, and has boto3 append its data directory to the search paths
    on every session, so no two clients are built at once, and each build removes the
    duplicate search path it added;
  - the `net.http` TLS-context cache, one per process, which builds each consumer's context
    for a certificate setting once (reading the bundle from local disk) and looks it up
    afterwards; the certificate file and directory are examined before the lock is taken;
  - the `_massive.spec` loaders, which load each generated file once;
  - each `RequestGovernor`'s lock (section 19.1), which guards its buckets; a request waits
    for its slot outside it, on a wake event of its own, and the governor's listeners are
    called after it is released;
  - the process's governor registry (`_ingest/rate_limits.py`), which calls into a governor
    (`declare`, `idle`) while held, so its order is the registry's lock, then the governor's,
    then the diagnostics queue's lock a listener takes; none of them is taken under
    `registry_lock`;
  - each `Cancel` token's lock, which orders its `set` against its callbacks; the callbacks
    run after it is released;
  - the session's `RateLimitNotices`, which remembers the buckets already reported;
  - the session's `DriftRegistry`, whose WARNINGs are logged after the lock is released;
  - the `dataset_schema` cache of the `Catalog`;
  - the futures schedule calendar's lock, one per connection group, which guards its immutable maps of
    contracts to products and products to sessions, replaced whole by its worker; the flush
    thread and the update-graph thread of a watchlist change only read them and queue work.
    A condition on the same lock counts the lookups served, for `wait_idle`, which tests call
    and no flush or update-graph thread ever does.
- Per-connection mutable state (desired, acknowledged, and pending sets; the in-flight step;
  watchdogs; the `WsClient` and its socket) is **loop-confined**: only the connection's client
  thread touches it. Other threads reach it through `loop.call_soon_threadsafe` with plain data
  (tuples of topic strings), and through `WsClient.close()`, which any thread may call: it only
  sets an `Event` and schedules a cancel on the loop (section 19.2). Nothing reads loop state
  across threads; the loop thread enqueues diagnostics row snapshots on the diagnostics writer's
  queue, and the writer is the only thread that writes the diagnostics tables.
- The session's `RestClient` is shared by every thread that makes REST calls. It is safe per
  call: its shared mutable state is urllib3's internally locked pool, the governor (shared with
  the other sessions of its API key), and its closed `Cancel` token (section 19.1). The first-429
  WARNING of each bucket belongs to the session's `RateLimitNotices`, the client's refusal
  hook, behind its leaf lock.
- Row buffers are one `queue.SimpleQueue` per (connection, channel). The WS thread is the only
  producer; the flush thread is the only consumer.
- The reaper queue is a `queue.SimpleQueue`. The producers are the result finalizers,
  `m.release`, the collection finalizer of an unclosed session (`_collect_session`), the
  shutdown callbacks (a watchlist listener's stop), a failed watchlist listener, and the live
  set (a core's final release); `m.close` runs its closers on the calling thread.
- No code relies on the atomicity of `dict`, `set`, or `list` operations. Every shared container
  is behind the lock or has a single owner.
- `ExecutionContext` is captured at session construction. Every library thread enters it before
  any Deephaven call, each time through a fresh Python wrapper of the captured Java context
  (`entered` in `_tables/execution_context.py`). The `deephaven` wrapper stores its restore handle on the wrapper object,
  so two threads entering one shared wrapper at once make the first to exit install the other
  thread's previous (empty) context on itself.
- `LivenessScopeStack` is thread-local, so result scopes are opened and closed on the caller
  thread. `release()` runs on the reaper thread under the UG shared lock, which is allowed: the
  tracker is synchronized and drains on the releasing thread.

### 18.3 Lock order

The order is **UG lock, then `registry_lock`**. Listeners run on the UG thread while the UG
lock is held and may take `registry_lock`. Therefore no library thread acquires the UG lock,
directly or through auto-locking in `Table.snapshot()`, `where_in`, `last_by`, or
`deephaven.arrow.to_table`, while holding `registry_lock`. That single rule prevents the UG and
registry deadlock. Verbs called on the UG thread raise `ValueError`. `blink_publisher` takes
the UG shared lock on the caller thread while it creates a blink table, never under
`registry_lock` or a publish lock. `wait_for_update_cycle`, which waits for a cycle that
started after the call to end, refuses to run on a UG thread or under either UG lock.

**A caller holding the UG lock never waits on a thread that may need the UG lock or an update
cycle** (the diagnostics writer, a poller, another caller's stream open, the reaper). The
caller may hold the UG lock without being a UG thread: the console holds the exclusive lock
for every command, and a user thread may hold the shared lock. No update cycle runs meanwhile,
and any other thread that needs the lock blocks, so a wait on such a thread could never end.
Which library threads can need the lock: the diagnostics writer (its exit releases the
tables' scope under the shared lock; its adds never wait for a cycle), a poller (`Table.snapshot()` of a
watchlist), another caller's stream open (starting a watchlist's listener, reading a crypto
watchlist), and the reaper (every scope release takes the shared lock; nothing joins the reaper
or waits for its jobs). The flush threads, the WebSocket client threads, the REST and flat-file
reader pools, and the crypto-pairs and futures-schedule workers never need it: they call no
Deephaven operation that locks (static tables, `publisher.add` of a static table,
`publish_failure`, which hands every scope it would release to the reaper, listener removal).
So on a thread that holds the lock (`holds_update_graph_lock`, checked outside `registry_lock`,
since it is a Java call), a verb takes over the key of a stream still opening on another thread
instead of waiting for it ([section 6.1](06-massivesession-and-lifecycle.md#61-how-a-live-table-is-kept-alive-and-released)), and `close()` waits for
neither the diagnostics writer nor the cycle that applies the final rows. It does not join the
connections it closes or the futures-schedule worker either, though none of those threads
needs the lock: while it waits no live table updates, and against an unresponsive server each
join could last its full `JOIN_TIMEOUT`. They are daemons and end on their own, and the
closing thread writes each closing connection's final `CLOSED` row itself
([section 6.1](06-massivesession-and-lifecycle.md#61-how-a-live-table-is-kept-alive-and-released), `close()` step 4); a caller
without the lock still joins them, each for a bounded time. No library lock is ever held by a
thread that waits for the UG lock, so a caller holding the UG lock may take any of them in the
usual order.

The reverse direction is latency, not a deadlock: a thread without the lock whose verb needs
it (a stream open that starts a watchlist's listener, a `fetch()` that reads a watchlist)
waits for the whole console command that holds it, as any Deephaven operation that locks
does.

The WebSocket flush adds two locks above `registry_lock`: a connection's **flush lock**, then a
stream's **publish lock**, then `registry_lock`. Nothing takes the flush or the publish lock
while holding `registry_lock`, and nothing done under them takes the UG lock or waits for a
cycle, so a caller that holds the UG lock may take a publish lock (to fail a stream) and the
order has no cycle.

Two leaf locks complete the order. The **live set's lock** is taken only under
`registry_lock`, never the other way round. A **connection group's lock** (the registries of
the WebSocket connections that sessions with one key share) is a leaf of its own: a watchlist
listener takes it on a UG thread, so the order there is UG lock, then the group's lock; it is
never held together with `registry_lock`, and nothing blocks, sends, or calls Deephaven under
it.

`m.close()` takes the shared UG lock once, last, after every closer has run and the diagnostics
are released, with no library lock held, so a held `m.streams()` view reads the final rows as
it returns. It never runs on a UG thread (it raises `ValueError` there). On a thread that
already holds the UG lock it takes the lock again at once, without waiting for anything, and
it does not wait for the diagnostics writer (above).
The wait lasts as long as the current update cycle, without bound if a cycle hangs; a caller
holding a lock of its own that a listener needs waits as it would on any auto-locking
`t.snapshot()`.

### 18.4 Free-threaded specifics

`PYTHON_GIL=0` jobs run the full suite, including the Deephaven integration tests (the embedded
JVM starts in `pytest_configure`). Stress tests exercise concurrent subscribe and unsubscribe
from 16 threads against a fake server, concurrent `release()` and verb calls, 16 threads
opening and releasing results of two sessions on one shared connection while one of them
closes, and 16 threads
paging through one `RestClient` against the fake REST server (including a 429 with
`Retry-After` and a reset in the middle of paging), and 16 threads taking 160 permits of one
paced governor bucket on a virtual clock, of which no six ever start within one 61 s window. The only free-threaded target is 3.14t,
because pyarrow, numpy, and pandas no longer publish cp313t wheels. Wheel availability on 3.14t:
`jpy 2.2.0`, `pyarrow`, `numpy`, and `pandas` ship cp314t wheels; `deephaven-core` is
`py3-none-any`; `websockets 17.3` ships cp314t wheels; `urllib3`, `certifi`, `boto3`, and
`botocore` are pure Python. `orjson 3.13.0` publishes no free-threaded wheels, so `orjson` is an
optional extra and
the standard-library codec is the default. `PythonDeephavenSession.setVariable` carries the
comment "relying on the GIL being kind to us"; the library never mutates script globals, so this
does not affect it, but it is why the Deephaven integration tests must run on the free-threaded
builds rather than unit tests alone.

**Deferred frees across threads.** A free-threaded build counts references with biased
reference counting: a reference dropped on a thread other than the one that created the
referencing object's count is queued to that object's creating thread, which merges it only
when it next runs Python code, or when a cyclic collection runs. The creating thread is often
the user's (the core), but can be a library or a Java thread (a record version the reaper or a
shutdown callback made). So when the last reference to a core goes on another thread, the core
is freed, and its connections, clients, and threads close, only once that creating thread next
runs Python code, or at the next cyclic collection, whichever comes first; in a console whose
commands run on the thread that created the session, that is the next command. A thread
blocked in a C call such as `Thread.join()` or `Event.wait()` merges nothing. This is a
property of the runtime, not a race: nothing is lost, only delayed, and `m.close()` stays
immediate. The library keeps the delay short by holding a core strongly only from the live
set and the user's objects. Tests that wait for a session to be freed therefore poll in Python,
never inside one C-level wait.
