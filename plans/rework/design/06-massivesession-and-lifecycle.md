## 6. `MassiveSession` and lifecycle

```python
m = MassiveSession(config: MassiveConfig | None = None)   # None -> MassiveConfig()

m.stocks  m.options  m.futures  m.indices  m.forex  m.crypto
m.reference  m.indicators  m.economy
m.partners.benzinga  m.partners.etf_global  m.partners.tmx  m.partners.fable

m.snapshots(symbols: Symbols, *, sort=None, order=None, limit=None, filters=None) -> Query
    # GET /v3/snapshot; mixed asset classes; 250 tickers per request
m.connections() -> Table      # live diagnostics: one row per connection and per resolved (class, family)
m.streams() -> Table          # live diagnostics: one row per stream
m.rate_limits() -> Table      # live diagnostics: one row per REST rate-limit bucket (section 17)
m.symbol_reads() -> Table     # live diagnostics: one row per symbol of each followed history() or
                              # fetch() (section 7.8)
m.release(table: Table) -> None   # deterministic teardown of one result
m.close() -> None             # stops every stream and poller now; every live table fails
with MassiveSession() as m: ...   # __exit__ calls close(); for scripts whose results are static
repr(m)  # "<MassiveSession #1 open>" or "<MassiveSession #1 closed>"; the label names the session in logs and errors
repr(m.stocks)  # "<MassiveSession #1 stocks>"; vendors "<MassiveSession #1 partners.benzinga>"
```

`MassiveSession` exposes nothing beyond the names above (§ 22, “Sessions and queries expose no
introspection attributes”). In particular it has no `name` or `config` attribute: the session's
label (`MassiveSession #1`, a per-process counter) appears in its repr, in the same-key WARNING,
and in error messages, and nowhere else; the configuration is the `MassiveConfig` the caller passed
(or `MassiveConfig()`), which the caller already holds. Not exposing the configuration keeps the
API key one step further from console globals, which every user of a Deephaven worker can read.

The session must be created inside a Deephaven worker: an `ExecutionContext` must be active.
The library captures it at construction and enters it on every library thread before any
Deephaven call. Construction without an active context raises `MassiveError`. The context's
query scope must also be the Python script session's own (`ScriptSessionQueryScope`), because
library formulas such as `canonicalize_symbols`'s `update` call Python functions through it; a
context from `make_user_exec_ctx(freeze_vars=[...])` holds a frozen `StandaloneQueryScope`
instead, so construction under it raises `MassiveError` naming the requirement. The library
never starts a JVM.

Sessions in one process that use the same API key **share their WebSocket connections**
([section 13.1](13-host-resolution-and-connection-protocol.md#131-invariants)). Individual plans allow one connection per cluster per
account, so two sessions with connections of their own would knock each other off and both
stall in `WAITING_SLOT`. Sessions share only when the settings that shape a connection are
equal: the API key, `websocket_hosts`, `subscription_confirmation_timeout`, `reconnect_backoff`, `max_reconnect_backoff`,
`max_outage`, `flush_interval`, and `rest_base_url` (the group's futures schedule calendar reads through any
of its sessions' REST clients, which must read the same API); they must also run on the same Deephaven update graph, because a
connection's flush thread enters one execution context for all its streams (a worker normally
has one graph, so this rarely matters and is not warned about). A connection carries one value
of each, so a session whose settings differ never joins another session's connection, and the
library never silently applies one session's settings to another session's streams. Such a session opens connections of its own,
and its construction logs a WARNING naming the other session and the settings that differ,
because on an individual plan the two then compete for the slot. A second session with equal
settings logs nothing. Sharing is per process: a session in another process that uses the key
still takes the slot, and each affected stream's `Error` then says so
([section 17](17-diagnostics.md#17-diagnostics)).

Sessions with the same API key and REST origin share one REST `RequestGovernor`, because
Massive counts their requests together ([section 13.4](13-host-resolution-and-connection-protocol.md#134-rest)). `SessionClients` takes the
session's lease on it from the process's registry when it is created and registers the lease's
release as a session closer, so `close()` and the collection of an unclosed session both
release it; the lease refers to the registry, never to the core.

After `close()`: `close()` itself is idempotent; verbs, `m.connections()`, `m.streams()`,
`m.rate_limits()`, and `m.symbol_reads()` raise `MassiveError("MassiveSession #1 is closed")`; `m.release(table)` keeps working, and so
does garbage-collection release of results; building queries and reaching namespaces never fail.
Every live table of the session, and every table derived from one, **fails** with
`MassiveError("MassiveSession #1 was closed; this table no longer updates")`, so a closed
session never leaves a table that looks live but never changes. A failed table keeps its rows:
`t.snapshot()`, `deephaven.pandas.to_pandas(t)`, and `deephaven.arrow.to_arrow(t)` still return
them, and `t.snapshot()` is the way to keep working with the data after `close()`. A new
operation derived from a failed table (`t.where(...)`, `t.update(...)`) raises Deephaven's
"Can not listen to failed table". A verb that passed the closed check just before `close()` ran
also raises `MassiveError`: the stream registry is checked again under `registry_lock`, and the
verb checks once more after it has tracked its result, so it never returns a table whose
failure would land a cycle later. A `connections()`, `streams()`, `rate_limits()`, or
`symbol_reads()` table obtained before `close()` keeps working and shows the final rows, `CLOSED` included
([section 17](17-diagnostics.md#17-diagnostics)).

Every verb raises `ValueError` when called on a UG thread (for example from inside a table
listener), checked before anything else, so also on a closed session; `close()` likewise. The
closed check comes next, still before any argument is validated, so `q.ring(0)` on a closed
session raises `MassiveError`. Verbs create tables and take the registry lock; both are
forbidden on the UG thread ([section 18](18-concurrency-model.md#18-concurrency-model)).

A thread that only holds the UG lock is not a UG thread, and every call works there:
`MassiveSession()`, every verb, `columns()`, `connections()`, `streams()`, `symbol_reads()`,
`release()`, and `close()`. This is the main deployment path, not an edge case: Deephaven's console runs every
command holding the UG's exclusive lock (`PythonDeephavenSession.evaluate` runs the script
inside `exclusiveLock().doLockedInterruptibly`), so no update cycle runs until the command
returns. A call on such a thread never waits on a thread that may need the UG lock or an
update cycle to finish: the diagnostics writer, a poller, or another caller's stream open
([section 18.3](18-concurrency-model.md#183-lock-order)). What a call writes to a live table (stream rows,
diagnostics rows, a poll's first cycle) therefore shows once the command is over, which is
Deephaven's normal behavior for anything done in a console command. A long blocking call in a
command, such as a `history()` of many requests on a rate-limited key, holds every live table
of the worker still for its duration, as any long console command does. A verb that runs 5
seconds or longer there logs one WARNING after it returns, "MassiveSession #1: stocks.bars
history() ran for 12 s inside a Deephaven console command, which holds the update graph's lock:
live tables do not update until the command ends; pass wait=False to fetch() or history() to
read in the background", once per pause: a later long verb in the same pause (no update cycle
has run since the warning) logs it at DEBUG, so a command of several long verbs warns once and
the next command that pauses the tables warns again, and a paced REST
read warns before its first request how long it will pause them
([section 13.4](13-host-resolution-and-connection-protocol.md#134-rest)). An application script runs under the lock too, so the remedy is
`wait=False`, a read on a library thread of its own ([section 7](07-query-classes-and-verbs.md#7-query-classes-and-verbs)),
not a script.

**How long a session lives.** The rule: a live table keeps alive everything it needs, and
nothing else keeps anything alive. A session lives while the user holds it (`m`, a query, a
namespace), and while it has a running stream, a running `poll()` table, a background read
(`wait=False`) or a followed read whose result is held or still in use, or a diagnostics table someone holds ([section 6.1](#61-how-a-live-table-is-kept-alive-and-released)). Only those hold
the session strongly; nothing else in the library does, and the library forms no reference
cycle through it, so an unused session is freed by reference counting, even with
`gc.disable()`. (A caller can still form one: an exception raised out of a call keeps the
frames of that call, and with them the session they had in hand, until the cyclic collector
breaks the cycle between the exception and its traceback; that is ordinary Python.) So
`t = dm.MassiveSession().stocks.trades("AAPL").append_only().where("Price > 100")` keeps
ticking for as long as `t` is in use, although the session object and the intermediate result
were dropped at once.

A session freed without `close()` is cleaned up by a finalizer on its core (not run at
interpreter exit). By the rule above it has no live table left. The finalizer takes no lock
and makes no Deephaven call: it hands the registered closers (connections, pollers) to the
reaper, which logs at INFO ("MassiveSession #1 was garbage-collected without close(); closing
its connections and pollers") and runs them, and then queues the diagnostics writer's release
behind them, so the rows they write are applied. That job waits for the writer to end, and the
reaper's retirement is queued after it, so the reaper outlives the writer: a writer that dies
hands work to the reaper (below), which must still be there to run it. The reaper keeps running
while results are still tracked and stops once the last one has been collected and released. `close()` empties
the closer list, so a closed session's closers never run twice. A closer must not refer to the
core strongly, or the core could never be freed.

### 6.1 How a live table is kept alive and released

These facts are verified in `deephaven-core` `main`:

- `AbstractScriptSession.evaluateScript` runs every console command inside
  `LivenessScopeStack.open(queryScope, false)`. `queryScope` is one
  `ScriptSessionQueryScope` per session, released only when the session is destroyed.
- `PythonDeephavenSession.setVariable` only pops or assigns the Python global. Nothing in it
  calls `unmanage` or `release`. Therefore deleting or reassigning a Python variable does not by
  itself release a table that a console command created.
- `StreamToBlinkTableAdapter`: the blink table's `destroy()` calls `adapter.close()`, which
  runs `streamPublisher.shutdown()`, which runs the `on_shutdown_callback` synchronously on the
  calling thread. The adapter holds the table only weakly.
- `StreamToBlinkTableAdapter` (42.7) delivers a published failure on its next run and then
  closes itself; the close takes the adapter off the update graph. It never clears its queue
  of failures, so it relies on running no more once the close has started. A `destroy()` on
  another thread can claim the close (`alive.compareAndSet`) while a failure is queued: the
  next run delivers the failure, its own close does nothing, and if the destroying thread has
  not yet called `removeSource` when the cycle after that starts, the adapter runs again and
  delivers the same failure a second time. `BaseTable.notifyListenersOnError` asserts the
  table has not failed yet, and the fatal-error handler calls `System.exit(1)`. A failure the
  adapter queues itself (`deliverUpdate` when `notifyListeners` throws) races a destroy the same
  way. With a debugger pausing the destroying thread before `removeSource`, the process exits
  on the first or second failure. Unforced, under heavy load (eight test processes beside 20 CPU
  burners), 3 in 100,000 failures whose result is released at once ended the process.
- `LivenessScopeStack` is thread-local. `LivenessScope.release()` decrements the scope's count,
  and referents are dropped on the thread that releases.
- `deephaven.liveness_scope.LivenessScope` exposes `open()`, `manage()`, `unmanage()`, and
  `release()`. `deephaven.table.Table` defines no `__slots__`, so wrappers are
  weak-referenceable.

**Mechanism, per result:**

```python
scope = LivenessScope()                       # library-owned, one per result
with scope.open():                            # top of the thread-local stack during creation
    result = describe(shape(stream.blink), spec, schema)
                                              # shape: append_only / ring / last_by, or for blink()
                                              #   the blink table itself; describe() returns a new,
                                              #   described table (section 7.4)
m._core.results.track(result, scope, on_released)
    # ResultRegistry.track: under registry_lock, a ResultRecord(scope, on_released,
    # finalizer=weakref.finalize(result, <enqueue release>), released=False) keyed by
    # id(result); the key is checked against finalizer.peek() on use
return result
```

- **Every result is a distinct Python object.** `blink()` never returns the stream's own blink
  wrapper. The stream's blink table is never described, so `describe()` always copies it: each
  call returns a new Java table, created while the result scope is open and therefore managed by
  it. So `a = q.blink(); b = q.blink()` yields two records with distinct `id(result)` values,
  and the stream never holds a wrapper it has handed out, so each result's weakref can fire.
  The described copy depends on the blink table, as any derived table does, so the blink table
  and its `on_shutdown_callback` live until the copy is gone.
- Because `scope` is at the top of the stack, `result` is managed by `scope`, not by the
  session's `queryScope`. Releasing `scope` drops the last liveness reference unless a
  user-derived table (`result.where(...)`), an open UI panel, or a user liveness scope also
  holds it.
- The stream's blink table and publisher live in one stream scope. The stream scope is released
  when the last result record of that stream is released. The blink table itself dies only when
  nothing refers to it any more; at that moment the adapter closes and the shutdown callback runs.
- **Result records control scope release only.** Topic reference counts and flush routing are
  bound exclusively to the blink table's `on_shutdown_callback`. A derived table, an open UI
  panel, or a user scope therefore keeps the stream alive and subscribed, even after every
  library-held result has been released.
- **Tracking is all or nothing.** `ResultRegistry.track` builds the record and its finalizer
  before it registers the record; if anything raises, even `KeyboardInterrupt`, it marks the
  record released and removes it, and the verb releases the result scope and the stream hold
  itself (also when the interrupt lands between `make_result` and `track`). A result is never
  released twice and never leaked, within the best-effort boundary below (see **Opening a stream**).
- **Collection finalizer.** A `weakref.finalize` on the result fires when the Python wrapper
  is collected. It holds the result registry and the record strongly, and through them the
  reaper, for exactly as long as the result lives. The record's `on_released` callback holds
  the session core only weakly, so no cycle runs from the core through its results back to
  itself; while the record is unreleased its hold keeps the core in the live set (below), so
  the callback always finds it. The finalizer takes
  no lock and makes no Java call: it only puts a job on the reaper queue (`queue.SimpleQueue`).
  Garbage collection can run it on any thread, including one that holds another library lock,
  so taking `registry_lock` there could deadlock. The reaper job then flips the record's
  once-only `released` flag under `registry_lock` and, if it was not already set, releases the
  result.
- **Reaper thread.** One daemon thread per session. It takes records off the queue, releases
  the result scope (under the UG lock, below), decrements the stream's held-result count, and at
  zero releases the stream scope. Each job runs inside `try`/`except Exception`; a failure is
  logged at ERROR and the thread continues. It also runs the closers of a session freed without
  `close()` ([section 6](#6-massivesession-and-lifecycle)), the jobs the shutdown callback and a
  failed watchlist queue (below), the pin releases (`SessionCore.release_later`), and the final
  release of a session that leaves the live set.
- **Every release holds the UG lock.** Releasing a scope can drop the last reference to a
  refreshing table, and Deephaven destroys the table on the releasing thread. The library
  releases every liveness scope through `release_scope` (`_tables/liveness.py`), which enters
  the session's execution context and holds the UG shared lock for the release: no update
  cycle runs meanwhile, so a destroy never lands between a blink table's two cycles (below).
  The release runs outside `registry_lock` and every other library lock, never on a UG thread,
  and never under a stream's or a poller's publish lock, which a console command may take while
  it holds the exclusive lock. A thread that may not take the lock (a shutdown callback, a
  listener, the holder of a publish lock) hands the scope to `SessionCore.release_later`, which
  queues the release on the reaper. The reaper and the diagnostics writer may now wait for a
  console command to end, which is not a deadlock: no thread that holds the UG lock waits for
  them (section 18.3). The cost is latency. A result released during a long console command
  (by `m.release` or by garbage collection) is released only when the command ends: until then
  a released `poll()` table keeps polling, a released stream stays subscribed, and every later
  reaper job (other releases, a session's closers, a failed watchlist's failure) waits behind
  the blocked release. None of it deadlocks. The test suite wraps `LivenessScope.release` and fails any test in which
  library code releases a scope without the lock; the suite runs every branch of the library.
- **The live set.** A module-level dictionary of session cores, under its own leaf lock (taken
  only under `registry_lock`, never the other way round). A core is in it exactly while its
  stream registry, its worker-table registry (`poll()` tables, background reads, and followed reads), or its count
  of held diagnostics tables is non-empty;
  every method that changes one of them updates the set in the same `registry_lock` section.
  The set is what keeps a session with a running stream or poll alive after the user dropped
  every reference to it: nothing else in the library holds a core strongly (connections,
  flushers, stream sinks, pollers, and the clients hold it weakly). A sink can outlive its
  stream's record in a container cleaned up later (the routes of a connection, which its
  flush thread clears when it ends), so a strong reference there
  would keep a finished session alive; a sink whose core is gone reports nothing. A core that leaves the set is handed
  to the reaper as a job argument, so its last reference is not dropped under the lock. The
  thread that took it out still holds it while it returns, so the final release may also land
  there, on whatever thread that is; that is safe because a core's finalizer only queues work
  (its closers and the diagnostics writer's release go to the reaper) and never blocks or calls
  Java.
- **Opening a stream.** The session mints the stream id (a process-wide counter) and
  registers the stream's record, `OPENING`, with one hold for the opener, before it calls
  `StreamTransport.open_stream(..., stream_id=)`; so a failure or a shutdown the transport
  reports while the open is still running finds the record. No row is written until the open
  returns (`m.streams()` has no `OPENING` state). A second verb on the same key finds the
  record still opening and waits for the open to finish, on the caller thread and outside every
  lock, then reuses the stream; one open per key, never a wasted publisher. The exception is a
  verb on a thread that holds the UG lock (a console command): the open may need that lock (it
  starts a watchlist's listener, and reads a crypto watchlist), so waiting could never end.
  Such a verb takes the key over: it registers a record of its own, maps the key to it, and
  opens a second stream. The first stream finishes opening for its own caller and is never
  reused; the verbs that were waiting for it wake when it is ready, claim again, and reuse the
  new one. Every unmapping checks that the key still maps to the stream being unmapped, so the
  first stream's end leaves the key alone. The lock is checked once per verb, before the claim,
  since the check is a Java call and `registry_lock` admits none. A take-over logs one INFO
  line after the lock is released, so the key's second row in `m.streams()` is explained:
  "MassiveSession #1: stocks.quotes stream 2 opens alongside stream 1 for AAPL, which another
  thread is still opening: this call holds the update graph's lock (a console command) and
  cannot wait for that open, so m.streams() shows both; later calls reuse stream 2". The opener's claim
  and open run inside one `try`/`except`, so whatever interrupts them (even a
  `KeyboardInterrupt` landing between the two, or after the open returned) the record is
  readied and, unless the verb got the stream, the opener's hold is released. Asynchronous
  exceptions such as `KeyboardInterrupt` are handled on a best-effort basis: every window
  between the library's own steps (claim, open, build, track) is covered, and the windows that
  remain are inside the cleanup code itself. If the open raises
  anything, even `KeyboardInterrupt`, the record is deleted, its key is unmapped, and the
  waiters wake and open the stream themselves. `m.close()` wakes the waiters, which raise
  `MassiveError`; the opener releases the stream when its open returns. A failure reported
  during the open leaves the stream `FAILED`, and the open returns the stream as is, exactly as
  if the failure had arrived just after it: every shape, `blink()` included, raises
  `MassiveError` naming the stream's failure, with the hold released. A new stream subscribes
  before its first result exists, so its sink holds the rows routed to it until the verb that
  opened it has built that result, and `SessionCore.stream_result` then adds them
  (`StreamHandle.publish_held`, also after a failed build), so an aggregation such as
  `last_by` sees every row the stream received; a reused stream holds nothing.
  `SessionCore.stream_result` checks the stream's recorded failure before it builds the shape, and wraps in the same
  `MassiveError` the `DHError` ("Can not listen to failed table") Deephaven raises when the
  failure lands while the shape derives from the blink table. A stream's failure is recorded
  (`SessionCore.fail_stream`) before its table is failed (`publish_failure`), so a verb that
  meets the failed table always finds the failure to name.
- **A failure pins its table.** Every blink table the library feeds (a stream's, a `poll()`
  table's, a background read's, a diagnostics table's) is created by `blink_publisher`
  (`_tables/publisher.py`), whose
  `BlinkPublisher.publish_failure` first makes a new liveness scope manage the table, then
  publishes the failure. While the failure is queued nothing can destroy the table, so only
  the delivery closes the adapter, on the update-graph thread inside the cycle, and no second
  run follows. The shutdown callback that close runs queues the pin's release on the session's
  reaper (a Deephaven call, which the callback must not make itself). A table that is already
  being destroyed, already shut down, or already pinned by an earlier failure gets no failure;
  its unused scope, which manages nothing, goes to the reaper too, because `publish_failure`
  runs under the owner's publish lock and so releases nothing itself. The pin cannot cover a
  failure the adapter queues itself (`deliverUpdate`), which only a Deephaven fix can.
  The library's own releases hold the UG lock (above), so they never destroy a table between
  two cycles, but the pin stays: Deephaven releases without the lock where the library cannot
  reach. A gRPC thread drops a released export's reference when a user closes a UI panel
  (`SessionState.ExportObject` calls `dropReference` with no lock), the liveness-cleanup thread
  drops a collected table's references, and a user's thread may release a derived table. Any
  of them can drop the last reference to the blink table while its failure is queued: a panel
  showing `result.where(...)`, closed after `m.release(result)` just as the stream fails, does.
- **Record lifetime.** Holds and the shutdown callback run on different clocks (a
  `publish_failure` shuts the blink table down at once, while results are still held), so
  either may come last. A stream's registry record is deleted only once both have happened:
  its holds have reached zero and its blink table has shut down. Until then `m.streams()` keeps
  showing its row count and a late failure, even for a released stream a derived table keeps
  alive. Its key maps to it only while it is opening or held, not failed, and not shut down,
  so no verb ever reuses a dead table.
- **Shutdown callback.** It usually runs on the reaper thread (under the UG shared lock that the
  reaper's release holds), but it may run on a UG thread, a
  gRPC thread if the UI held the last reference, or Deephaven's liveness-cleanup thread when
  the JVM collected the last derived table. It does exactly two things:
  `SessionCore.stream_shut_down(stream_id)`, which under `registry_lock` unmaps the stream's
  key and deletes its record if no hold is left, or marks it shut down so that the last hold
  deletes it (and queues the stop of a reactive stream's watchlist listener on the reaper), and
  `loop.call_soon_threadsafe(...)` to the connection's loop so the topic reference counts are
  decremented there. Never a send, never a Deephaven call. It may drop the last reference to
  a core, whose finalizer only queues work on the reaper.
  The JVM holds the callback until it collects the publisher, so the callback reaches the
  session's connections only through a weak reference, as a watchlist listener's callbacks
  reach the stream: a strong one would form a cycle across Python and Java that neither
  collector can break, keeping a finished session in memory until a JVM collection.
- **Reactive watchlists.** The stream's record holds its `SymbolWatch` strongly until the
  stream shuts down. Deephaven keeps a table's listeners only weakly; the blink table that
  manages the watch keeps it reachable today, but the stream's correctness does not rest on
  how a liveness manager holds its referents, because a lost listener would silently stop the
  stream following its watchlist. The listener's `on_error`, which runs on a UG
  thread, queues the stream's failure on the reaper: a watchlist that fails fails its stream,
  with a `MassiveError` naming the watchlist's error, instead of leaving a stream that no
  longer follows its symbols.
- **`m.release(table)`** delegates to `m._core.results.release(table)`, which looks up
  `id(table)` in the registry, verifies that the weakref resolves to
  that exact object (so a recycled id cannot match), flips the `released` flag under the lock,
  and enqueues the release. `m.release` raises `ValueError` if the table is not a verb result.
  A release always notifies the owner (the stream's held-result count, or the poller), even if
  releasing the result scope raised. It is
  deterministic: the table keeps ticking only while other holders (derived tables, open panels)
  exist.
- **`m.close()`** stops everything the session runs and fails every live table:
  1. On a UG thread it raises `ValueError`. On a thread that only holds the UG lock it works.
  2. Under `registry_lock` it only collects: it sets the closed flag, takes the closers, marks
     every stream, `poll()`, and background-read record `CLOSED` with the error text below (a
     released stream that a derived table or panel still keeps alive included; a `FAILED` or
     `DONE` row keeps its state), gathers the streams, polls, and loading reads whose tables
     have not shut down, and wakes the verbs waiting for an open.
  3. Outside every lock it fails each gathered table with
     `MassiveError("MassiveSession #1 was closed; this table no longer updates")`: a stream
     through its sink, under the stream's publish lock and once only, so a connection failure
     queued meanwhile cannot replace `CLOSED`; a poll or a read by stopping its thread first
     and then failing its publisher. `SessionCore.fail_stream` never changes a `CLOSED` row's
     state.
  4. It runs the closers: the session's own streams leave their connections, and every
     connection no other session leases is closed and joined for a bounded time; the pollers
     stop. On a thread that holds the UG lock it joins nothing (neither the connections nor
     the futures-schedule worker): no live table updates while it would wait, and against an
     unresponsive server each join could last its full bound. Those threads are daemons that
     never need the lock, so they end on their own. A closing connection's own last row,
     `CLOSED`, would then reach the session only after its diagnostics are released, and be
     dropped, so the closing thread writes it instead: under the group's lock, which every row
     write holds, the session gets the last row written for each family it shows, marked
     `CLOSED` with no topics and no buffered rows (a `FAILED` row keeps its state and counts),
     and leaves the connection's users, so no later row replaces it.
  5. It has the writer add the final rows, release the diagnostics scope under the shared
     update-graph lock, and end, and waits for it. The rows reach the tables on the next update
     cycle, so `close()` then waits for a cycle that started after the writer ended
     (`wait_for_update_cycle`, at most `CLOSE_SHOWN_TIMEOUT`, 30 s, then a WARNING), and a held
     `m.streams()` view, or a table derived from one, shows them when it returns. A held view
     keeps its table, the blink table under it, and the publisher alive, so the release does
     not drop the pending rows. `close()` waits even when it counts no view held: in
     `x = m.streams().where(...)` the view's wrapper is collected at once while `x` keeps the
     table, and the wait is usually shorter than one cycle. On a thread that holds the UG lock it waits for neither: the writer's release
     cannot take the lock until the caller lets it go, so `close()` asks the writer to add the
     rows, release the scope, and end, and returns at once; the rows, and a held view, follow
     once the console command is over.
  6. It logs "MassiveSession #1 closed; N live tables failed, streams N [...]", at WARNING
     when N > 0 and at INFO when N is 0. N counts the tables that are failed once the close is
     done, including one a racing failure failed first; the line names their stream ids (the
     first 20) rather than logging the tables one by one.

  The failures shut the blink tables down on the next update cycle, whose callbacks then
  release the records; a session whose failed tables are still held stays in the live set
  until they are dropped, holding nothing but its registries. No `add` or `publish_failure`
  ever runs under `registry_lock`: the publisher calls the shutdown callback inside its own
  synchronized block, and the callback takes `registry_lock`.
- **Library thread death.** If the reaper, the diagnostics writer, or a connection's flush
  thread dies on an exception it does not handle (a `KeyboardInterrupt` or `SystemExit` raised
  inside it), it fails the live tables it served before it ends, naming the thread: otherwise
  those tables would be frozen without an error. A dead reaper can no longer release results,
  so its session stays in the live set until the process ends; that leak is accepted, because
  only an interrupt raised inside the reaper kills it, and its tables are failed, never left
  looking live. A dying diagnostics writer also fails the four diagnostics tables through their
  publishers, so a view a user holds, and every table derived from one, fails instead of showing
  frozen rows as current; a new view then raises a `MassiveError` that says the writer died. The
  writer hands their scope, and the failures' pins, to the reaper, since it will never release
  them itself. The WebSocket client thread reports any internal failure as `FAILED`, which fails
  the connection's streams ([section 13.3](13-host-resolution-and-connection-protocol.md#133-connection-and-subscription-protocol)).

Three documented behaviours that users will notice:

1. `del t` in the console releases a library result at the end of the same console command.
   The script session reconciles its variable snapshot when the command finishes, so the
   wrapper is collected then, not at the next command.
2. A table derived in the console (`t = q.append_only().where(...)`) is held by the console's
   query scope only weakly. After `del t` its stream keeps running until the JVM next collects
   the derived table: usually seconds, but unbounded on an idle heap. Until then `m.streams()`
   shows the stream `RELEASED` with `RowsPublished` still rising; then the connection closes
   `IDLE_GRACE` after its last stream and the session cleans up. The library never calls
   `System.gc()` to shorten the delay, because a collection pauses every query in the worker;
   `m.release(t)` before deriving in a script, or `m.close()`, stops it at once. The same holds
   for a `poll()` table derived in the console, which until then keeps making its REST requests:
   on the Basic tier those come out of the five a minute that every session of the account
   shares.
3. A table open in a UI panel is exported and stays live until the panel closes, regardless of
   `del`.

`poll()` results use the same record, scope, and reaper path, and the same rule as streams: a
poll keeps polling while its table is in use, by the result or anything derived from it.
Releasing the result marks its row `RELEASED`; the poller stops when the poll's blink table
shuts down, because nothing uses the table any more.

Background reads (`fetch(wait=False)`, `history(..., wait=False)`) follow the same rule and the
same registry as `poll()` tables; the session core calls both *worker tables*, a table one
library thread of its own fills. The result is created on the caller thread inside its own
result scope, from a blink table whose one column holds Deephaven tables
(`_tables/background_read.py`):

```python
blink, publisher = blink_publisher(name, {"Constituent": <io.deephaven.engine.table.Table>}, ...)
parts = PartitionedTable.from_partitioned_table(
    blink_to_append_only(blink), key_cols=[], unique_keys=False,
    constituent_column="Constituent", constituent_table_columns=<the dataset's columns>,
    constituent_changes_permitted=True)
result = describe(parts.merge(), spec, schema)     # refreshing, no rows
```

The read's thread enters the session's execution context and runs the same transport call as
`wait=True`, so the complete static table is built off the UG lock (the static conversion may
take the shared lock briefly, as any `to_table` does). It then publishes one row that holds the
table. On the next cycle the append-only table gains that row and the merge adds the static table
as a constituent, which redirects row keys and copies no rows: a result of a billion rows lands
in one cycle as fast as one of ten. A test fills a billion-row result whose column is a lazy
formula (copying it would take seconds and gigabytes) and checks that it lands within one
second of the publish. The result shows every row in that one update and never changes again;
it is never half loaded. Its row in `m.streams()` is `Kind=read`, `LOADING` until then and
`DONE` after.

- **Releasing cancels.** Releasing the result (`m.release`, garbage collection) marks the row
  `RELEASED`; the blink table's shutdown, once nothing uses the result, sets the read's stop
  token, which its REST requests and flat-file reads listen to (`IngestPorts.read_stop`), so a
  request waiting for the rate governor stops at once and one on the wire is the last. A read
  that stops publishes nothing. A table derived from the result keeps the read going, as it
  keeps a poll polling, and gets the rows when they land.
- **Failures.** A download that fails fails the table (the normal failed-table state carrying
  the full message, through the publisher's failure, which reaches the merged result through
  the append-only and partitioned tables) and marks the row `FAILED` with the message, after
  logging it once at ERROR (`call 7 failed after 3.2 s: ...`). A defect that is not a
  `MassiveError` is logged with its traceback and fails the table with a `MassiveError` naming
  it. `m.close()` fails a read still `LOADING` like any live table and marks its row `CLOSED`;
  a `DONE` read is never failed, and its row stays `DONE`, a final state like `FAILED`.
- **The race of the last publish and a failure** is settled under the read's publish lock: the
  publish and the failure check a flag set under it, so a table either gets its rows (and later
  failures are skipped) or fails (and the publish is skipped), never both.

**Followed reads** (`fetch()` and `history()` given a refreshing symbol table,
[section 7.8](07-query-classes-and-verbs.md#78-following-a-live-symbol-table)) are worker tables too, kept alive and
released by the same rule. Everything is created on the caller thread inside the result's own
scope:

```python
# One partition per symbol. Deephaven has no call that adds or removes a partition: the rows of
# the partition table define them, so a blink publisher plus last_by stands in for a keyed
# input table (the library never uses one), and watched_rows removes a symbol's partition.
blink, publisher = blink_publisher(name, {"Symbol": str, "Seq": long, "Constituent": TABLE,
                                          "DecidedAtStep": long}, ...)
latest_per_symbol = blink.last_by("Symbol")          # one partition row per symbol: its newest
with_seed = latest_per_symbol                        # wait=False
with_seed = merge([seed.natural_join(latest_per_symbol, on="Symbol", joins="NewerSeq=Seq")
                       .where("isNull(NewerSeq)").drop_columns("NewerSeq"),  # wait=True: the
                   latest_per_symbol])               # static seed until a newer row is published
in_watchlist = watched_rows(with_seed.where("!isNull(Constituent)"), symbol_table)
# reread=ALWAYS: also hide a row whose DecidedAtStep precedes its symbol's last join cycle
parts = PartitionedTable.from_partitioned_table(in_watchlist, key_cols=["Symbol"],
            unique_keys=True, constituent_column="Constituent",
            constituent_table_columns=<the dataset's columns>, constituent_changes_permitted=True)
result = describe(parts.merge(), spec, schema)      # [list-column datasets: the dedupe]
```

A partition row holds one symbol's complete static rows, built off the update-graph lock; `Seq`
numbers a result's partition rows, and the seed's join copies it only because it is never null.
A symbol lands, is replaced, or leaves as one partition, so no rows are copied in the update
cycle. A removed symbol's rows leave in the cycle that removes its row from the user's table,
because `watched_rows` drops its partition there. With `wait=True` the seed is a static table of
the call-time symbols' partition rows, so their rows are visible at once, also inside a console
command; a static table never drops a row, so the seed keeps those rows until the result is
released, under either `reread`. A partition row with a null `Constituent` (an empty partition row) frees a symbol's
rows: under `reread=ALWAYS` a removal publishes one, and `DecidedAtStep`, the latest symbol-table
cycle the coordinator had applied when it decided to publish a row, hides a download decided
before the symbol's last join; `NEVER` keeps the row, so a re-add shows it at once. There is no
`input_table` and no `add_async`: `add_async` runs on the JVM's shared
`ForkJoinPool.commonPool()`, and on an input table already destroyed it never completes and
keeps a pool thread (measured: 20 calls left all 9 threads busy, 11 more queued, and no
callback). `publisher.add` returns without waiting for an update cycle, also under the
exclusive lock (measured).

- **One follow coordinator per session** (`massive #1 follow`, section 18.1) owns all
  per-symbol state of every followed read of the session and does no I/O. It takes the symbol
  tables' changes, the reads' outcomes, and the stops from one `SimpleQueue`, starts reads on
  the session's read pool, times the retries, and publishes each partition row. The `SymbolWatch`
  listener only enqueues.
- **One read pool per session**, of `max_concurrency` threads, runs every followed read: the
  transport call, in the session's execution context, and the static conversion. It hands the
  outcome back to the coordinator through the queue.
- **Per result**: a publish lock and a failed flag. A partition row's `publisher.add`
  and the failure check the flag under the lock, so a failed result never gets another row.
- **Releasing stops it.** The blink table's shutdown, once nothing uses the result, sets the
  result's stop token and enqueues a stop; reads in flight stop at their next request, and the
  watch's stop is queued on the reaper. Every symbol's row in `m.symbol_reads()` becomes
  `STOPPED`. A table derived from the result keeps it following.
- **Failures.** A failure of the whole table is recorded first (`SessionCore.fail_stream`) and
  then delivered only through the publisher's `publish_failure`, under the result's publish
  lock, by the coordinator, never from the listener. `m.close()` fails a followed read like
  any live table and marks its symbols `CLOSED`.

There is no `stop()` on queries. Release is automatic (garbage collection) or explicit
(`m.release`); `m.close()` stops anything, whatever still holds it.
