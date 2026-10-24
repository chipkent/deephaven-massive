# deephaven-massive: Design Specification

This document is the approved design for the clean rework of `deephaven-massive`, a Python
library that exposes Massive.com (formerly Polygon.io) market data as live and static
[Deephaven](https://deephaven.io) tables. It is the source of truth for every implementation
work package in `PLAN.md`. It changes only by owner decision.

The design was checked against `massive==2.9.0` (the official Python SDK), `deephaven-core`
42.7 (Python package; Java behaviour from the `deephaven-core` `main` branch), and the
massive.com documentation as of 2026-10-23. Facts that could not be verified from those
sources are marked **UNVERIFIED**, carry the conservative handling that applies, and are
collected in [section 23](#23-open-verification-items).

## Table of contents

1. [Purpose and scope](#1-purpose-and-scope)
2. [Principles and terms](#2-principles-and-terms)
3. [Exports](#3-exports)
4. [Package layout](#4-package-layout)
5. [`MassiveConfig`](#5-massiveconfig)
6. [`MassiveSession` and lifecycle](#6-massivesession-and-lifecycle)
7. [Query classes and verbs](#7-query-classes-and-verbs)
8. [Namespace tree](#8-namespace-tree)
9. [Filters](#9-filters)
10. [Symbols and `ALL`](#10-symbols-and-all)
11. [Enums and types](#11-enums-and-types)
12. [Timestamp units and day boundaries](#12-timestamp-units-and-day-boundaries)
13. [Host resolution and connection protocol](#13-host-resolution-and-connection-protocol)
14. [Publishers, fan-out, and failure semantics](#14-publishers-fan-out-and-failure-semantics)
15. [Options contract cap](#15-options-contract-cap)
16. [`HistorySource.AUTO`](#16-historysourceauto)
17. [Diagnostics](#17-diagnostics)
18. [Concurrency model](#18-concurrency-model)
19. [SDK fork: fix list and responsibility split](#19-sdk-fork-fix-list-and-responsibility-split)
20. [Quality gates](#20-quality-gates)
21. [Examples](#21-examples)
22. [Decisions](#22-decisions)
23. [Open verification items](#23-open-verification-items)

---

## 1. Purpose and scope

`deephaven-massive` turns Massive's three transports into Deephaven tables:

- **WebSocket** streams (`wss://<host>/<cluster path>`) become live tables.
- **REST** endpoints (`https://api.massive.com`) become static tables, or live tables that
  re-run a request on an interval.
- **Flat files** (gzip CSV on S3 at `files.massive.com`) become static tables.

The library runs inside an existing Deephaven worker. It never starts a JVM and never embeds a
Deephaven server. HTTP, retries, pagination, the WebSocket socket, authentication, and reconnects
belong to the official `massive` SDK (from the owner's fork, see
[section 19](#19-sdk-fork-fix-list-and-responsibility-split)). Everything Deephaven-specific
belongs here.

There are no existing users. Backward compatibility with the previous code base is not a goal.

## 2. Principles and terms

### 2.1 Principles

1. **Datasets are named by what they are, not how they travel.** One method per dataset. One
   schema per dataset kind: the union of the SDK model fields across asset classes and
   transports (WebSocket, REST, and flat files), flattened to PascalCase. A column a class or
   transport does not supply is null (crypto trades have no `Tape`; stock bars have no
   `SettlementPrice`). `Timestamp` is
   the primary time column on every tick, bar, and value row. `Symbol` is the instrument on every
   instrument row.
2. **Shape is the user's choice; transport and host are the library's.** Verbs name the
   Deephaven table shape. No feed, host, channel, or S3 prefix appears in user code.
3. **Tier is a property of the key.** Identical code runs on every plan. The library learns
   recency (real-time or delayed) from REST responses, never by opening a connection it intends
   to abandon. A dataset the plan lacks fails loudly and names the alternative.
4. **Tab completion is the contract.** A verb that cannot work for a dataset does not exist on
   that dataset's query class. Where the capability depends on a parameter value (the bar
   timespan), typed `@overload`s pick the class statically. `ValueError` at verb time is reserved
   for parameter values (`ALL` where a per-symbol endpoint is required, out-of-range dates). It is
   always raised synchronously on the calling thread.
5. **One canonical name, no aliases, no fallbacks.** Officially deprecated endpoints are
   unsupported. Undocumented endpoints and paths are unsupported, with no path to them.
6. **One connection per WebSocket URL per session; one publisher per stream.** A failure in one
   stream never fails another. A connection is never opened as a tactic.
7. **Failures land where Deephaven users look.** Live tables fail through
   `TablePublisher.publish_failure`. Static verbs raise. Both log at ERROR and appear in the
   diagnostics tables.
8. **Thin over the official SDK.** `massive.RESTClient` does HTTP, retries, and pagination.
   `massive.websocket.WebSocketClient` (the fork) does the socket, authentication, reconnects, and
   subscription frames. SDK model classes define columns. Bugs in the SDK are fixed in the fork,
   not worked around here.
9. **No live testing as design input.** Unverifiable facts get the conservative choice and a
   fixture test. Live integration tests are a later work package.
10. **Closed categorical sets are enums.** Every closed set of values in a public signature is a
    `StrEnum`. No public signature accepts a string in place of an enum member.

### 2.2 Terms

| Term | Meaning |
|---|---|
| dataset | One method under a namespace, such as `m.stocks.trades`. |
| query | The immutable object a dataset method returns. |
| verb | A method on a query that returns a Deephaven `Table`. |
| result | A `Table` a verb returned. |
| stream | One live subscription for one (dataset, channel, symbol set). It owns one `TablePublisher`. |
| channel | A WebSocket event prefix: `T`, `Q`, `A`, `AM`, `FMV`, and so on. |
| channel family | `TICK` (every channel except `FMV`) or `FMV`. |
| cluster path | The WebSocket URL path: `/stocks`, `/options`, `/futures`, `/indices`, `/forex`, `/crypto`. |
| connection | One WebSocket to one `wss://host/path`. |
| topic | One subscription parameter, `<channel>.<symbol>` or `<channel>.*`. |
| UG thread | A thread of Deephaven's update graph (UG), which refreshes live tables while holding the UG lock. |
| blink table | A Deephaven table whose rows exist for exactly one update cycle. `TablePublisher` produces one. |
| result scope | A library-owned `LivenessScope` that holds exactly one result. |
| stream scope | A library-owned `LivenessScope` that holds one stream's blink table and publisher. |
| registry lock | `SessionCore.registry_lock`, the one lock that guards every session-level registry. |

## 3. Exports

`deephaven_massive/__init__.py` exports exactly these 21 names and nothing else:

```python
import deephaven_massive as dm

# Session and configuration
dm.MassiveSession
dm.MassiveConfig

# Exceptions
dm.MassiveError            # base class
dm.AuthenticationError     # subclass of MassiveError: bad key
dm.EntitlementError        # subclass of MassiveError: the plan lacks the dataset, host, or timeframe

# Sentinel
dm.ALL                     # every symbol on the stream, in the flat file, or in the snapshot

# Enums (all StrEnum; values are Massive's wire strings)
dm.AssetClass
dm.Timespan
dm.PollMode
dm.HistorySource
dm.FuturesVenue
dm.Direction
dm.ContractType
dm.SeriesType
dm.Timeframe
dm.Order
dm.Market
dm.Locale
dm.ConditionDataType
dm.Sip

# Version
dm.__version__             # from importlib.metadata
```

The six query classes (`Query`, `StreamQuery`, `HistoryQuery`, `StreamHistoryQuery`,
`StreamHistoryLatestQuery`, `HistoryLatestQuery`) are importable from the private module
`deephaven_massive._query` for type annotations only. Users never construct them. The type
aliases `Symbols`, `DateLike`, and `FilterValue` are importable from `deephaven_massive._types`
for the same purpose.

A test asserts that `dm.__all__` equals this list and that no public signature in the package
contains a `Literal` of strings ([section 11](#11-enums-and-types)).

## 4. Package layout

### 4.1 Source modules

Every module below `deephaven_massive/` other than `__init__.py` is private. The rule: the
first path component below the package has a leading underscore. A module inside a private
subpackage needs no underscore of its own.

Inside the private modules, a name used by other modules is bare (`QueryBase`, `SessionCore`,
`check_filters`), and a leading underscore marks a helper local to its module. The exceptions are
names this document fixes with an underscore: `_All`, `_require_enum`, and the namespace classes
(`_Stocks`, `_Partners`, ...), whose underscore keeps them out of users' completion lists.

| Module | Contents |
|---|---|
| `__init__.py` | The public names of [section 3](#3-exports), `__all__`, `__version__`. |
| `_config.py` | `MassiveConfig` ([section 5](#5-massiveconfig)). |
| `_session.py` | `MassiveSession`: namespaces, `snapshots`, `release`, `close`, the registry lock, the reaper thread, the stream-id allocator ([section 6](#6-massivesession-and-lifecycle)). The host-decision cache belongs to the REST transport ([section 13.2](#132-host-decision)). |
| `_query.py` | The six query classes and their verbs ([section 7](#7-query-classes-and-verbs)). |
| `_enums.py` | The fourteen public enums and the internal `Recency`, `ChannelFamily`, and `Transport` enums; the `_require_enum` runtime check. |
| `_types.py` | `Symbols`, `ALL` and its type `_All`, `DateLike`, `FilterValue`, the `bars` overload aliases, and `override` (`typing_extensions.override` for the type checker, a no-op at run time, since `typing.override` needs Python 3.12). |
| `_errors.py` | `MassiveError`, `AuthenticationError`, `EntitlementError`. |
| `_catalog.py` | The dataset catalog: one frozen record per dataset (namespace, name, query class, channel, REST method, `start`/`end` parameter pair, poll default, flat-file prefix, how symbols reach a REST request (`SymbolParam`: `ticker`, `tickers`, `ticker.any_of`, path segment, `from`/`to` pair)). A REST dataset gives its SDK model (`rest_model`) and the record derives its schema from it at construction, so the poll-key and time-unit checks run at import for every dataset; exactly one of a hand-written schema and a model is given, and a WebSocket dataset always has a hand-written schema and an asset class. Also the generated `SDK_PARAMS` table ([section 9](#9-filters)). |
| `_schema.py` | The schema registry: SDK-model flattening, PascalCase naming, the dtype override table, the four hand-written tick schemas (trades, quotes, bars, index values) with per-transport key maps. |
| `_timeunits.py` | The `(dataset, transport, field) -> unit` table and the conversion rules ([section 12](#12-timestamp-units-and-day-boundaries)). |
| `_symbols.py` | Symbol normalization and the `Symbol`/`Underlying` rule ([section 10](#10-symbols-and-all)). |
| `_dates.py` | `DateLike` parsing, class day zones, session-date rules, `[start, end)` normalization. |
| `_liveness.py` | Result scopes, stream scopes, result records, the reaper queue and thread. |
| `_rest.py` | `RESTClient` construction from config, pagination into bounded Arrow batches, 403/429 mapping, the two documented private-call shims. |
| `_hosts.py` | Host resolution from REST evidence and the host-decision record ([section 13.2](#132-host-decision)). |
| `_ws.py` | One connection per URL over the forked `WebSocketClient(raw=True)`: client thread, frame decoding, status routing, the reconcile loop, acknowledgement watchdogs, the explicit-verdict rule, the options contract cap. |
| `_stream.py` | Streams, per-stream publishers, the per-connection flush thread, shared-batch fan-out ([section 14](#14-publishers-fan-out-and-failure-semantics)). |
| `_watchlist.py` | Reactive symbol tables: the listener, the diff, and dispatch off the UG thread. |
| `_poll.py` | `poll()` tables: `REPLACE`, `UPSERT`, and `APPEND` modes, fan-out under `max_concurrency`. |
| `_flatfiles.py` | S3 streaming of `.csv.gz` keys, pure-Arrow per-batch transforms, symbol pushdown, partition-date rules ([section 16](#16-historysourceauto)). |
| `_diagnostics.py` | The `connections()` and `streams()` input tables ([section 17](#17-diagnostics)). |
| `_domains/__init__.py` | Empty. |
| `_domains/base.py` | The shared dataset factory: builds a query from a catalog record, validates symbols and filters, applies the `bars` overload selection. |
| `_domains/stocks.py`, `options.py`, `futures.py`, `indices.py`, `forex.py`, `crypto.py`, `reference.py`, `indicators.py`, `economy.py`, `partners.py` | One namespace class per module ([section 8](#8-namespace-tree)). `partners.py` holds the four vendor classes. |

Supporting files outside the package:

| File | Purpose |
|---|---|
| `scripts/check.sh [fast\|full] [--no-fix]` | The single verification entry point (lint, format, type check; `full` adds the test suite with coverage). |
| `scripts/refresh_massive_catalog.py` | Regenerates `SDK_PARAMS` in `_catalog.py` and the vendored docs index under `tests/data/` from the installed SDK and the documentation index. |
| `tests/data/` | Vendored documentation index and payload fixtures. |
| `tests/_fakes/` | Fake REST transport, WebSocket server, and S3 opener used by the unit tests ([section 4.2](#42-test-layout)). |

Deephaven is imported at module level where it is used at runtime. Type-only imports go under
`TYPE_CHECKING`. The library imports nothing from `deephaven_server`.

### 4.2 Test layout

Tests mirror the package. The test file for `deephaven_massive/<path>/<name>.py` is
`tests/<path>/test_<name>.py`, where `<name>` has its leading underscore removed and `<path>`
is copied as is (a private subpackage such as `_domains/` keeps its underscore under `tests/`).
A package's `__init__.py` is mirrored like any other module, by `tests/<path>/test_init.py`, so
the rule stays mechanical: every source module has exactly one mirror. `tests/test_init.py`
therefore holds the public-surface tests (`dm.__all__` equals [section 3](#3-exports) and
`__version__` equals the installed distribution's version), and `tests/_domains/test_init.py`
mirrors the empty `_domains/__init__.py`. Examples:

| Source | Unit test | Live integration test |
|---|---|---|
| `__init__.py` | `tests/test_init.py` | |
| `_config.py` | `tests/test_config.py` | |
| `_hosts.py` | `tests/test_hosts.py` | `tests/test_hosts_integration.py` |
| `_domains/__init__.py` | `tests/_domains/test_init.py` | |
| `_domains/stocks.py` | `tests/_domains/test_stocks.py` | `tests/_domains/test_stocks_integration.py` |

Integration tests end in `_integration`. They are skipped unless `MASSIVE_API_KEY` is set and
run only in the later live-testing work package. The owner approved exactly these exceptions to
the mirror rule, and no others:

- `tests/conftest.py`: starts the embedded JVM in `pytest_configure`, before collection, and
  provides fixtures: thin fixtures that build the fake servers from `tests/_fakes/`, plus the
  table-to-Python and fixture-loading helpers. Test modules never import from it.
- `tests/test_layout.py`: walks `src/deephaven_massive/**/*.py` and asserts every module has its
  mirrored test file, and that every test file maps to a module or is one of these exceptions.
- `tests/test_catalog.py`: asserts 100% Massive coverage against the SDK and the documentation
  index, with a reviewed exclusion list ([section 8.12](#812-excluded-endpoints)). Under the
  naming rule it is also the mirror of `_catalog.py`, so the unit tests of the catalog records
  and of the generated `SDK_PARAMS` table live in the same file.
- `tests/test_readme.py`: runs every example of [section 21](#21-examples) in the embedded JVM
  against the fake servers. It mirrors `README.md`, not a source module; the lowercase name
  follows PEP 8 module naming (ruff `N999`).
- `tests/_fakes/`: a package of protocol-faithful fake servers, one module per transport
  (`rest.py`, `ws.py`, `s3.py`, plus an empty `__init__.py`). `tests/test_layout.py` ignores the
  whole directory. Only `tests/conftest.py` imports it; test modules receive fakes through
  fixtures. A test module may import from `tests/_fakes/` directly only when a fixture cannot
  provide what it needs (for example a second, independently scripted server), and says why
  in a comment.

## 5. `MassiveConfig`

`MassiveConfig` is a frozen dataclass. `MassiveConfig()` reads the environment. Defaults are
defined in exactly one place: the field defaults of this class.

| Field | Default / environment variable | Meaning |
|---|---|---|
| `api_key: str` | `MASSIVE_API_KEY`, then `POLYGON_API_KEY` | Required. Absent raises `AuthenticationError` at construction. |
| `s3_access_key_id: str \| None` | `MASSIVE_S3_ACCESS_KEY_ID` | Enables flat-file history. |
| `s3_secret_access_key: str \| None` | `MASSIVE_S3_SECRET_ACCESS_KEY` | |
| `ws_hosts: Mapping[AssetClass, str]` | `{}` | Pins the hostname of the TICK family for a class, for example `{AssetClass.STOCKS: "fullmarket-business.massive.com"}`. Hostname only; validated. The FMV family is never pinned ([section 13](#13-host-resolution-and-connection-protocol)). |
| `ack_timeout: float` | `5.0` seconds | Subscribe-acknowledgement watchdog, per topic. |
| `reconnect_backoff: float` | `30.0` seconds | The first wait before reconnecting after a connection that opened has closed, including a `max_connections` refusal (it arrives after a successful handshake); later waits double up to `max_reconnect_backoff`. Before a connection opens, a TCP failure, a timeout, or a retryable handshake failure (HTTP 500/502/503/504) retries on the websockets library's own schedule (up to 5 s at first, growing to 90 s); any other handshake error is fatal and fails the connection's streams (§ 13.3, `raise_fatal_errors=True`). Must be positive (a base of 0 would never grow and retry in a hot loop); any positive value is accepted, so tests can use milliseconds. The default waits out the window Massive's knowledge base describes: the server takes "roughly 10 to 30 seconds" to release a dropped connection, and an earlier retry "can be refused for holding the allowance it just lost", which wastes the one slot an individual plan has. |
| `max_reconnect_backoff: float` | `120.0` seconds | Cap of the doubling backoff, so a connection comes back within this long of a long outage ending. Must not be below `reconnect_backoff`. Jitter only adds up to 20% and is clamped, so every wait lies in `[reconnect_backoff, max_reconnect_backoff]` (§19, F2.3). |
| `flat_file_symbol_threshold: int` | `50` | The `HistorySource.AUTO` switch point when both sources are possible. |
| `flat_file_publish_cutoff: datetime.time` | `time(11, 30)`, America/New_York | A partition for day D counts as published from the first occurrence of this New York time after day D ends ([section 16](#16-historysourceauto)). The documentation says files land "by approximately 11:00 AM ET the following day". |
| `max_concurrency: int` | `8` | REST fan-out and S3 reads. |
| `flush_interval: float` | `0.05` seconds | WebSocket micro-batch flush into `TablePublisher.add`. |
| `rest_base_url: str` | `https://api.massive.com` | Rarely changed. |
| `s3_endpoint_url: str` | `https://files.massive.com` | Rarely changed. |
| `s3_bucket: str` | `flatfiles` | Rarely changed. |

Not configurable:

- The options contract cap (1,000 per connection; [section 15](#15-options-contract-cap)).
- The JSON codec. `orjson` is used when it is installed (optional extra
  `deephaven-massive[orjson]`); otherwise the standard library. The choice is logged once at
  INFO.
- The evidence endpoints of [section 13.2](#132-host-decision).

There is no `tier` field. No entitlement endpoint exists, so a tier field would be a claim the
server corrects.

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
m.release(table: Table) -> None   # deterministic teardown of one result
m.close() -> None             # closes every connection, stops every poller; live tables go static
with MassiveSession() as m: ...   # __exit__ calls close(); for scripts whose results are static
repr(m)  # "<MassiveSession #1 open>" or "<MassiveSession #1 closed>"; the label names the session in logs and errors
```

`MassiveSession` exposes nothing beyond the names above (§ 22, decision 21). In particular it
has no `name` or `config` attribute: the session's label (`MassiveSession #1`, a per-process counter) appears in
its repr, in the same-key WARNING, and in error messages, and nowhere else; the configuration is
the `MassiveConfig` the caller passed (or `MassiveConfig()`), which the caller already holds. Not
exposing the configuration keeps the API key one step further from console globals, which every
user of a Deephaven worker can read.

The session must be created inside a Deephaven worker: an `ExecutionContext` must be active.
The library captures it at construction and enters it on every library thread before any
Deephaven call. Construction without an active context raises `MassiveError`. The context's
query scope must also be the Python script session's own (`ScriptSessionQueryScope`), because
library formulas such as `canonical_view`'s `update_view` call Python functions through it; a
context from `make_user_exec_ctx(freeze_vars=[...])` holds a frozen `StandaloneQueryScope`
instead, so construction under it raises `MassiveError` naming the requirement. The library
never starts a JVM.

A second session with the same key in one process logs a WARNING naming the first. Individual
plans allow one WebSocket connection per cluster per account.

After `close()`: `close()` itself is idempotent; verbs, `m.connections()`, and `m.streams()`
raise `MassiveError("MassiveSession #1 is closed")`; `m.release(table)` keeps working, and so
does garbage-collection release of results; building queries and reaching namespaces never fail;
live results stop updating and keep their rows. A verb that passed the closed check just before
`close()` ran also raises `MassiveError`: the stream registry is checked again under
`registry_lock`, so a closed stream is never reused. The diagnostics tables are the exception: their
scope is released, so a `connections()` or `streams()` table obtained before `close()` becomes
unusable unless the caller holds it in a liveness scope of their own.

Every verb raises `ValueError` when called on a UG thread (for example from inside a table
listener), checked before anything else, so also on a closed session; `close()` likewise. The
closed check comes next, still before any argument is validated, so `q.ring(0)` on a closed
session raises `MassiveError`. Verbs create tables and take the registry lock; both are
forbidden on the UG thread ([section 18](#18-concurrency-model)).

A session that is garbage-collected without `close()` is cleaned up by a finalizer on its core
(not run at interpreter exit). The finalizer takes no lock and makes no Deephaven call: it hands
the registered closers (connections, pollers) to the reaper, which logs a WARNING ("MassiveSession
#1 was garbage-collected without close(); closing its connections and pollers") and runs them,
and stops the diagnostics writer. The reaper keeps running while results are still tracked (a
result whose release path does not hold the core, such as a `poll()` table, can outlive its
session) and stops once the last one has been collected and released. `close()` empties the closer list, so a closed
session's closers never run twice. A closer must not refer to the core strongly, or the core
could never be collected. Even so, an unclosed session's core is collected by the cyclic garbage
collector, not promptly by reference counting: each result record's `on_released` callback (a
`functools.partial` of the core's release method) refers back to the core.

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
- `LivenessScopeStack` is thread-local. `LivenessScope.release()` decrements the scope's count,
  and referents are dropped on the thread that releases.
- `deephaven.liveness_scope.LivenessScope` exposes `open()`, `manage()`, `unmanage()`, and
  `release()`. `deephaven.table.Table` defines no `__slots__`, so wrappers are
  weak-referenceable.

**Mechanism, per result:**

```python
scope = LivenessScope()                       # library-owned, one per result
with scope.open():                            # top of the thread-local stack during creation
    result = shape(stream.blink)              # append_only / ring / last_by; for blink(), a fresh wrapper:
                                              #   Table(stream.blink.j_table), then scope.manage(result)
m._core.results.track(result, scope, on_released)
    # ResultRegistry.track: under registry_lock, a ResultRecord(scope, on_released,
    # finalizer=weakref.finalize(result, <enqueue release>), released=False) keyed by
    # id(result); the key is checked against finalizer.peek() on use
return result
```

- **Every result is a distinct Python object.** `blink()` never returns the stream's own blink
  wrapper: each call wraps the same Java table in a new `Table(stream.blink.j_table)` and manages
  it explicitly in the result scope (`scope.manage(result)`), because creating a wrapper creates
  no liveness reference by itself. So `a = q.blink(); b = q.blink()` yields two records with
  distinct `id(result)` values, and the stream never holds a wrapper it has handed out, so each
  result's weakref can fire.
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
  before it registers the record; if anything raises, even `KeyboardInterrupt`, it detaches
  the finalizer and registers nothing, and the verb releases the result scope and the stream
  hold itself. A result is never released twice and never leaked.
- **Collection finalizer.** A `weakref.finalize` on the result fires when the Python wrapper
  is collected. It holds the result registry and the record strongly, and through them the
  reaper and the session core, for exactly as long as the result lives, so a result that
  outlives every reference to its session and query is still released. The finalizer takes
  no lock and makes no Java call: it only puts a job on the reaper queue (`queue.SimpleQueue`).
  Garbage collection can run it on any thread, including one that holds another library lock,
  so taking `registry_lock` there could deadlock. The reaper job then flips the record's
  once-only `released` flag under `registry_lock` and, if it was not already set, releases the
  result.
- **Reaper thread.** One daemon thread per session. It takes records off the queue, calls
  `scope.release()`, decrements the stream's held-result count, and at zero releases the stream
  scope. Each job runs inside `try`/`except Exception`; a failure is logged at ERROR and the
  thread continues. The reaper does nothing else, except run the closers of a session collected
  without `close()` ([section 6](#6-massivesession-and-lifecycle)).
- **Opening a stream.** The session mints the stream id (a process-wide counter) and
  registers the stream's record, `OPENING`, with one hold for the opener, before it calls
  `StreamTransport.open_stream(..., stream_id=)`; so a failure or a shutdown the transport
  reports while the open is still running finds the record. No row is written until the open
  returns (`m.streams()` has no `OPENING` state). A second verb on the same key finds the
  record still opening and waits for the open to finish, on the caller thread and outside every
  lock, then reuses the stream; one open per key, never a wasted publisher. The opener's claim
  and open run inside one `try`/`finally`, so whatever interrupts them (even a
  `KeyboardInterrupt` landing between the two, or after the open returned) the record is
  readied and, unless the verb got the stream, the opener's hold is released. If the open raises
  anything, even `KeyboardInterrupt`, the record is deleted, its key is unmapped, and the
  waiters wake and open the stream themselves. `m.close()` wakes the waiters, which raise
  `MassiveError`; the opener releases the stream when its open returns. A failure reported
  during the open leaves the stream `FAILED`, and the open returns the stream as is, exactly as
  if the failure had arrived just after it: `blink()` returns the failed table, while
  `append_only()`, `ring()` and `last_by()` raise Deephaven's `DHError` ("Can not listen to
  failed table") as they derive from it, with the hold released. P4 wraps that error in a
  `MassiveError` naming the stream's failure.
- **Record lifetime.** Holds and the shutdown callback run on different clocks (a
  `publish_failure` shuts the blink table down at once, while results are still held), so
  either may come last. A stream's registry record is deleted only once both have happened:
  its holds have reached zero and its blink table has shut down. Until then `m.streams()` keeps
  showing its row count and a late failure, even for a released stream a derived table keeps
  alive. Its key maps to it only while it is opening or held, not failed, and not shut down,
  so no verb ever reuses a dead table.
- **Shutdown callback.** It usually runs on the reaper thread, but it may run on a UG thread or a
  gRPC thread if the UI held the last reference. It does exactly two things:
  `SessionCore.stream_shut_down(stream_id)`, which under `registry_lock` unmaps the stream's
  key and deletes its record if no hold is left, or marks it shut down so that the last hold
  deletes it, and `loop.call_soon_threadsafe(...)` to the connection's loop so the topic
  reference counts are decremented there. Never a send, never a Deephaven call.
- **`m.release(table)`** delegates to `m._core.results.release(table)`, which looks up
  `id(table)` in the registry, verifies that the weakref resolves to
  that exact object (so a recycled id cannot match), flips the `released` flag under the lock,
  and enqueues the release. `m.release` raises `ValueError` if the table is not a verb result.
  A release always notifies the owner (the stream's held-result count, or the poller), even if
  releasing the result scope raised. It is
  deterministic: the table keeps ticking only while other holders (derived tables, open panels)
  exist.
- **`m.close()`** closes every connection and poller and releases the session-owned diagnostics
  scope. Publishers are not failed; live tables go static. Before the release, the streams
  table flips every stream still in the registry to `State=CLOSED`, including a released
  stream that a derived table or panel still keeps alive (a stream leaves the registry only
  once its holds are released and its blink table has shut down), and one message is logged:
  "session closed; N live tables are now static", at WARNING when N > 0 and at INFO when N
  is 0.

Two documented behaviours that users will notice:

1. `del t` in the console releases the result at the end of the same console command. The script
   session reconciles its variable snapshot when the command finishes, so the wrapper is
   collected then, not at the next command.
2. A table open in a UI panel is exported and stays live until the panel closes, regardless of
   `del`.

`poll()` results use the same record, scope, and reaper path. Releasing a `poll()` result stops
its poller.

There is no `stop()` on queries. Release is automatic (garbage collection) or explicit
(`m.release`, `m.close`).

## 7. Query classes and verbs

Six classes, defined in `_query.py`. Nothing touches the network until a verb is called. Queries
are immutable, hashable, and reusable. A query's repr names its class, its dataset, and the
arguments that differ from their defaults, for example
`<StreamHistoryLatestQuery stocks.trades symbols=('AAPL', 'MSFT')>`; `ALL` shows as `ALL` and a
reactive table as `<reactive table>`. Queries have no other attributes: the verbs are their whole
API (§ 22, decision 21).

```python
class Query:                               # REST list and snapshot datasets
    def fetch(self) -> Table: ...          # static; every row the endpoint returns, fully paginated
    def poll(self, interval: float | timedelta, *, mode: PollMode | None = None) -> Table: ...
                                           # live; re-runs fetch() every interval

class StreamQuery:                         # WebSocket datasets
    def blink(self) -> Table: ...
    def append_only(self) -> Table: ...
    def ring(self, capacity: int) -> Table: ...
    def last_by(self, by: str | Sequence[str] | None = None) -> Table: ...

class HistoryQuery:                        # datasets with a historical retrieval path
    def history(self, start: DateLike, end: DateLike, *,
                source: HistorySource = HistorySource.AUTO) -> Table: ...   # static

class StreamHistoryQuery(StreamQuery, HistoryQuery): ...

class StreamHistoryLatestQuery(StreamHistoryQuery):   # adds a REST "last" endpoint
    def fetch(self) -> Table: ...          # static; exactly one row per symbol in the stream schema;
                                           # ALL raises ValueError naming snapshots()

class HistoryLatestQuery(HistoryQuery):   # daily bars only
    def fetch(self) -> Table: ...          # static; one row per symbol: the previous completed session's bar
```

**`fetch()`.** Returns exactly what the dataset's REST endpoint returns for the request. On
`Query` that is a list or snapshot endpoint. On `StreamHistoryLatestQuery` it is the dataset's
"last" endpoint: `/v2/last/trade`, `/v2/last/nbbo`, `/v1/last_quote/currencies`,
`/v1/last/crypto`, or `/v3/snapshot/indices` projected to the values schema. On
`HistoryLatestQuery` it is `/v2/aggs/ticker/{t}/prev`, documented as "the prior session's open,
high, low, close, and volume"; the docstring says "previous completed session". No streamed
dataset has `poll()`. Current state on every tier is `m.<class>.snapshots(symbols).poll(n)`, one
request per cycle.

**`poll()`.** `mode=None` uses the catalog default: `PollMode.APPEND` for `reference.news`,
`stocks.ipos`, every `stocks.filings.*`, and `partners.benzinga.news`; `PollMode.REPLACE`
otherwise. `REPLACE` is a keyed table from which keys absent in the latest cycle are deleted.
`UPSERT` is keyed and never deletes. `APPEND` is append-only. A reactive `Table` passed as
`symbols` is read once per cycle with `Table.snapshot()` on the poller thread, never inside a
listener. Fan-out endpoints issue one request per symbol per cycle under `max_concurrency`;
`m.streams()` shows `RequestsPerCycle`.

**`last_by()`.** Returns `blink.last_by(keys)` with the default key `Symbol`. With a reactive
`Table` as symbols and `Symbol` among the keys, the result is
`blink.last_by(keys).where_in(symbol_table, "Symbol")`, so rows of removed symbols are evicted
from the result. The underlying `last_by` state keeps every symbol ever seen: a symbol that is
removed and re-added shows its stale row until its next tick. With keys that span symbols (for
example `by="Exchange"`) the filter comes first, `blink.where_in(symbol_table,
"Symbol").last_by(keys)`: filtering after the aggregation would drop a whole group whenever its
latest row came from a removed symbol. Each group then shows the latest row of a symbol that was
watched when the row arrived, and keeps it after that symbol is removed until a watched symbol
ticks in the group.
`last_by()` never seeds from REST. The "last" endpoints are absent from Starter and base Business
plans, so a seed would behave differently per tier; `snapshots(symbols).poll(n)` is the
tier-identical way to get current state.

**`history()`.** Static. `start` and `end` follow the day rules of
[section 12](#12-timestamp-units-and-day-boundaries). There is no `limit`: the library pages
internally. (The aggregates endpoint's `limit` counts base aggregates, not rows, so it cannot
serve as a row cap.)

**Backfill plus live.** There is no seeding verb. The documented recipe is:

```python
q = m.stocks.trades(["AAPL"])
t = merge([q.history("2026-10-20", "2026-10-23"), q.append_only()])
```

The history range is half-open and the live stream starts when `append_only()` is called, so
rows between the two can be missing or duplicated. The user owns that boundary.

### 7.1 `bars()` overloads

The bar timespan decides which transport exists: `SECOND` and `MINUTE` have WebSocket channels,
`DAY` has the `/prev` endpoint, and everything else is history only. Typed overloads pick the
class statically so no verb-time error is needed for the timespan.

```python
# _types.py
StreamSpan = Literal[Timespan.SECOND, Timespan.MINUTE]
DaySpan = Literal[Timespan.DAY]
FuturesSpan = Literal[Timespan.SECOND, Timespan.MINUTE, Timespan.HOUR, Timespan.SESSION,
                      Timespan.WEEK, Timespan.MONTH, Timespan.QUARTER, Timespan.YEAR]   # no DAY

class _Stocks:   # same shape on m.options, m.forex, m.crypto; m.indices has no `adjusted`
    @overload
    def bars(self, symbols: Symbols, timespan: StreamSpan = Timespan.MINUTE, *,
             multiplier: Literal[1] = 1, adjusted: bool = True) -> StreamHistoryQuery: ...
    @overload
    def bars(self, symbols: Symbols, timespan: DaySpan, *,
             multiplier: Literal[1] = 1, adjusted: bool = True) -> HistoryLatestQuery: ...
    @overload
    def bars(self, symbols: Symbols, timespan: Timespan = Timespan.MINUTE, *,
             multiplier: int = 1, adjusted: bool = True) -> HistoryQuery: ...

class _Futures:
    @overload
    def bars(self, symbols: Symbols, timespan: StreamSpan = Timespan.MINUTE, *,
             multiplier: Literal[1] = 1, venue: FuturesVenue | None = None) -> StreamHistoryQuery: ...
    @overload
    def bars(self, symbols: Symbols, timespan: FuturesSpan = Timespan.MINUTE, *,
             multiplier: int = 1, venue: FuturesVenue | None = None) -> HistoryQuery: ...
```

The implementation returns the most specific class for the actual values. The general overload
returns the base class, so the overlapping overloads type-check (the narrower return type is a
subtype). `bars(syms)` is stream plus history. `bars(syms, Timespan.DAY)` is history plus
`/prev`. `bars(syms, Timespan.MINUTE, multiplier=5)` is history only.

`Timespan.DAY` on futures is a static type error and a synchronous `ValueError` at construction:
the futures aggregates endpoint accepts the units `sec`, `min`, `hour`, `session`, `week`,
`month`, `quarter`, and `year` only. `Timespan.SESSION` outside futures is likewise rejected at
construction. The futures `resolution` parameter is `f"{multiplier}{unit}"`; per-unit multiplier
maxima are validated by the server (HTTP 400 becomes `MassiveError`).

`multiplier: Literal[1]` is an integer literal, not a categorical, and is allowed.

### 7.2 Synchronous `ValueError`s for parameter values

- `fetch()` with `ALL` on a `StreamHistoryLatestQuery` or a `HistoryLatestQuery` (the "last"
  and `/prev` endpoints take one ticker per request).
- `history()` with `ALL` over REST, except daily bars (the grouped-daily endpoint).
- `history(source=HistorySource.REST)` on `indices.values` (flat files only).
- `history(source=HistorySource.FLAT_FILES)` when a flat-file constraint is violated
  ([section 16](#16-historysourceauto)).
- A static symbol list that would exceed the options cap ([section 15](#15-options-contract-cap)).
- An exhausted generator as `symbols`.
- Any verb called on a UG thread.

### 7.3 Runtime enum check

Every enum parameter is checked at construction with `_require_enum(value, EnumType, name)`.
Because a `StrEnum` member is also a `str`, the check tests `isinstance(value, EnumType)`, not
`isinstance(value, str)`. A plain string raises
`TypeError("timespan must be dm.Timespan, e.g. dm.Timespan.MINUTE")`. `Timespan("minute")` is
the documented way to convert text.

## 8. Namespace tree

Notation:

- `Symbols = str | Sequence[str] | Table | _All` ([section 10](#10-symbols-and-all)).
- `DateLike = str | date | datetime` ([section 11](#11-enums-and-types)).
- `…F` is the trailing keyword-only block
  `sort: str | None = None, order: Order | None = None, limit: int | None = None, filters: Mapping[str, FilterValue] | None = None`.
- `…T` is `start: DateLike | None = None, end: DateLike | None = None` followed by `…F`. It
  exists only on datasets whose row has a named time axis (shown as "axis"). `start` and `end`
  address that axis with the day rules of [section 12](#12-timestamp-units-and-day-boundaries).
- `limit` is a total-row cap. Page size is internal. Every docstring says so. The cap is
  stored apart from the SDK parameters, in `Request.limit`, so it can never be forwarded as
  the SDK's page-size `limit`; the page size is `DatasetSpec.page_size`.
- The REST column names the path and, in parentheses, the SDK `RESTClient` method.

### 8.1 `m.stocks`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols)` | StreamHistoryLatestQuery | `T` | fetch `/v2/last/trade/{t}` (`get_last_trade`); history `/v3/trades/{t}` (`list_trades`) | `us_stocks_sip/trades_v1` |
| `quotes(symbols)` | StreamHistoryLatestQuery | `Q` | fetch `/v2/last/nbbo/{t}` (`get_last_quote`); history `/v3/quotes/{t}` (`list_quotes`) | `us_stocks_sip/quotes_v1` |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1, adjusted=True)` | overloads ([7.1](#71-bars-overloads)) | `A` (1 s), `AM` (1 min) | fetch `/v2/aggs/ticker/{t}/prev` (`get_previous_close_agg`, DAY only); history `/v2/aggs/ticker/{t}/range/{m}/{span}/{from}/{to}` (`list_aggs`); `ALL` with DAY: `/v2/aggs/grouped/locale/us/market/stocks/{date}` per day (`get_grouped_daily_aggs`) | `us_stocks_sip/minute_aggs_v1`, `us_stocks_sip/day_aggs_v1` (`adjusted=False` only) |
| `luld(symbols)` | StreamQuery | `LULD` | | |
| `imbalances(symbols)` | StreamQuery | `NOI` | | |
| `fmv(symbols)` | StreamQuery | `FMV` (FMV family, `business.massive.com/stocks`) | | |
| `snapshots(symbols, *, include_otc: bool = False)` | Query | | `/v2/snapshot/locale/us/markets/stocks/tickers` (`get_snapshot_all`, `tickers=`; `ALL` is the full market) | |
| `movers(direction: Direction, *, include_otc: bool = False)` | Query | | `/v2/snapshot/locale/us/markets/stocks/{gainers\|losers}` (`get_snapshot_direction`) | |
| `daily_open_close(symbols, date: DateLike, *, adjusted: bool = True)` | Query | | `/v1/open-close/{t}/{date}` per symbol (`get_daily_open_close_agg`) | |
| `dividends(symbols=None, …T)`, axis `ex_dividend_date` | Query | | `/stocks/v1/dividends` (`list_stocks_dividends`) | |
| `splits(symbols=None, …T)`, axis `execution_date` | Query | | `/stocks/v1/splits` (`list_stocks_splits`) | |
| `ipos(symbols=None, …T)`, axis `listing_date` | Query | | `/vX/reference/ipos` (`vx.list_ipos`) | |
| `short_interest(symbols=None, …T)`, axis `settlement_date` | Query | | `/stocks/v1/short-interest` (`list_short_interest`) | |
| `short_volume(symbols=None, …T)`, axis `date` | Query | | `/stocks/v1/short-volume` (`list_short_volume`) | |
| `share_float(symbols=None, …T)`, axis `date` | Query | | `/stocks/vX/float` (`list_stocks_floats`) | |
| `financials.balance_sheets`, `financials.income_statements`, `financials.cash_flow_statements`, `financials.ratios` — each `(symbols=None, *, timeframe: Timeframe \| None = None, …T)`, axis `period_end` | Query | | `/stocks/financials/v1/{balance-sheets,income-statements,cash-flow-statements,ratios}` (`list_financials_*`) | |
| `filings.index(symbols=None, *, form_type: str \| None = None, …T)`, axis `filing_date` | Query | | `/stocks/filings/vX/index` (`list_stocks_filings_index`) | |
| `filings.sections_10k`, `filings.text_8k`, `filings.disclosures_8k`, `filings.risk_factors` — each `(symbols=None, …T)`, axis `filing_date` | Query | | `/stocks/filings/10-K/vX/sections`, `/stocks/filings/8-K/vX/text`, `/stocks/filings/8-K/vX/disclosures`, `/stocks/filings/vX/risk-factors` (`list_stocks_filings_10k_sections`, `_8k_text`, `_8k_disclosures`, `_risk_factors`) | |
| `filings.institutional_holdings(*, filer_cik: str \| None = None, …T)`, axis `filing_date` (Form 13-F) | Query | | `/stocks/filings/vX/13-F` (`list_stocks_filings_13f`) | |
| `filings.insider_holdings(symbols=None, *, issuer_cik: str \| None = None, owner_cik: str \| None = None, …T)` (Form 3); `filings.insider_transactions(...)` with the same signature (Form 4); axis `filing_date` | Query | | `/stocks/filings/vX/form-3`, `/stocks/filings/vX/form-4` (`list_stocks_filings_form_3`, `_form_4`) | |
| `filings.risk_factor_taxonomy(…F)`, `filings.disclosure_taxonomy(…F)` | Query | | `/stocks/taxonomies/vX/risk-factors`, `/stocks/taxonomies/vX/disclosures` (`list_stocks_taxonomies_risk_factors`, `_disclosures`) | |

### 8.2 `m.options`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols)` | StreamHistoryLatestQuery | `T` | fetch `/v2/last/trade/{o}` (`get_last_trade`); history `/v3/trades/{o}` (`list_trades`) | `us_options_opra/trades_v1` |
| `quotes(symbols)` | StreamHistoryQuery | `Q` | history `/v3/quotes/{o}` (`list_quotes`) | `us_options_opra/quotes_v1` |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1, adjusted=True)` | overloads | `A`, `AM` | `/prev` (DAY); range | `us_options_opra/{minute,day}_aggs_v1` |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/options`) | | |
| `chain(underlying: str, *, contract_type: ContractType \| None = None, …F)`; `filters` takes `strike_price_*`, `expiration_date_*` | Query | | `/v3/snapshot/options/{u}` (`list_snapshot_options_chain`) | |
| `snapshots(symbols, *, …F)`; OCC contracts; `Underlying` parsed; `ALL` is every contract, paginated (slow, documented) | Query | | `/v3/snapshot?ticker.any_of=O:…` (`list_universal_snapshots`), 250 per request. Same fields as the per-contract endpoint, including greeks. `/v3/snapshot/options/{u}/{o}` is listed in the catalog as covered by this dataset. | |
| `contracts(*, symbols=None, underlying: str \| None = None, contract_type: ContractType \| None = None, as_of: DateLike \| None = None, expired: bool \| None = None, …F)`; `filters` takes `expiration_date_*`, `strike_price_*` | Query | | `/v3/reference/options/contracts` (`list_options_contracts`); with `symbols`: `/v3/reference/options/contracts/{o}` per contract (`get_options_contract`) | |
| `daily_open_close(symbols, date: DateLike)` | Query | | `/v1/open-close/{o}/{date}` per contract (`get_daily_open_close_agg`) | |

### 8.3 `m.futures`

Streams always use the `/futures` cluster path. `venue` exists on the three factory methods
below and selects the flat-file prefix only: it is not part of the stream key
([section 14](#14-publishers-fan-out-and-failure-semantics)). `venue=None` reads all four prefixes.

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols, *, venue: FuturesVenue \| None = None)` | StreamHistoryQuery | `T` | history `/futures/v1/trades/{t}` (`list_futures_trades`; `session_end_date` filters) | `us_futures_{venue}/trades_v1` |
| `quotes(symbols, *, venue: FuturesVenue \| None = None)` | StreamHistoryQuery | `Q` | history `/futures/v1/quotes/{t}` (`list_futures_quotes`) | `us_futures_{venue}/quotes_v1` |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1, venue: FuturesVenue \| None = None)` | overloads | `A`, `AM` | history `/futures/v1/aggs/{t}` (`list_futures_aggregates`; `resolution`; `window_start` in ns) | `us_futures_{venue}/minute_aggs_v1`, `us_futures_{venue}/session_aggs_v1` |
| `snapshots(symbols, *, product_code: str \| None = None, …F)`; `ALL` with `product_code` is every contract of a product | Query | | `/futures/v1/snapshot` (`get_futures_snapshot`) | |
| `contracts(*, product_code: str \| None = None, …F)`, `products(…F)`, `exchanges()`, `market_status(…F)` | Query | | `/futures/v1/{contracts,products,exchanges,market-status}` (`list_futures_contracts`, `_products`, `_exchanges`, `_market_statuses`) | |
| `schedules(…T)`, axis `session_end_date` | Query | | `/futures/v1/schedules` (`list_futures_schedules`) | |

No futures dataset has a "last" endpoint, so no futures stream query has `fetch()`.
`snapshots(symbols).fetch()` is the one-request answer. There is no futures `day_aggs` flat
file; `Timespan.DAY` is rejected ([section 7.1](#71-bars-overloads)).

### 8.4 `m.indices`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `values(symbols)` | StreamHistoryLatestQuery | `V` | fetch `/v3/snapshot/indices` (`list_snapshot_indices`) projected to `Value`, with `Timestamp` from `last_updated`; history flat files only | `us_indices/values_v1` |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1)` (no `adjusted`: the endpoint has none) | overloads | `A`, `AM` | `/prev` (DAY); range | `us_indices/{minute,day}_aggs_v1` |
| `snapshots(symbols, *, …F)` | Query | | `/v3/snapshot/indices` (`list_snapshot_indices`) | |
| `daily_open_close(symbols, date: DateLike)` | Query | | `/v1/open-close/{t}/{date}` (`get_daily_open_close_agg`) | |

### 8.5 `m.forex`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `quotes(symbols)` | StreamHistoryLatestQuery | `C` | fetch `/v1/last_quote/currencies/{from}/{to}` (`get_last_forex_quote`); history `/v3/quotes/{C:…}` (`list_quotes`) | `global_forex/quotes_v1` |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1, adjusted=True)` | overloads | `CAS` (1 s), `CA` (1 min) | `/prev`; range; `ALL` with DAY: `/v2/aggs/grouped/locale/global/market/fx/{date}` | `global_forex/{minute,day}_aggs_v1` |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/forex`) | | |
| `snapshots(symbols)`; `ALL` is all pairs | Query | | `/v2/snapshot/locale/global/markets/forex/tickers` (`get_snapshot_all`) | |
| `movers(direction: Direction)` | Query | | `/v2/snapshot/locale/global/markets/forex/{gainers\|losers}` (`get_snapshot_direction`) | |
| `conversion(from_currency: str, to_currency: str, *, amount: float = 1.0, precision: int = 2)` | Query | | `/v1/conversion/{from}/{to}` (`get_real_time_currency_conversion`) | |

### 8.6 `m.crypto`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols)` | StreamHistoryLatestQuery | `XT` | fetch `/v1/last/crypto/{from}/{to}` (`get_last_crypto_trade`); history `/v3/trades/{X:…}` (`list_trades`) | `global_crypto/trades_v1` |
| `quotes(symbols)` | StreamQuery | `XQ` | | |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1, adjusted=True)` | overloads | `XAS` (1 s), `XA` (1 min) | as forex; grouped `/v2/aggs/grouped/locale/global/market/crypto/{date}` | `global_crypto/{minute,day}_aggs_v1` |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/crypto`) | | |
| `snapshots(symbols)`, `movers(direction: Direction)` | Query | | `/v2/snapshot/locale/global/markets/crypto/{tickers,gainers,losers}` | |
| `daily_open_close(symbols, date: DateLike)` | Query | | `/v1/open-close/crypto/{from}/{to}/{date}` (fork method `get_daily_open_close_crypto`) | |

### 8.7 `m.reference`

All return `Query`.

| Method | REST |
|---|---|
| `tickers(*, symbols=None, market: Market \| None = None, type: str \| None = None, exchange: str \| None = None, cusip: str \| None = None, cik: str \| None = None, date: DateLike \| None = None, active: bool = True, search: str \| None = None, …F)` | `/v3/reference/tickers` (`list_tickers`). The library types `cusip` as `str`; the SDK's `int` hint is wrong and is a fork fix. |
| `ticker_details(symbols, *, date: DateLike \| None = None)` | `/v3/reference/tickers/{t}` per symbol (`get_ticker_details`) |
| `ticker_types(*, asset_class: AssetClass \| None = None, locale: Locale \| None = None)` | `/v3/reference/tickers/types` (`get_ticker_types`) |
| `ticker_events(symbols, *, types: Sequence[str] \| None = None)` | `/vX/reference/tickers/{t}/events` per symbol (`get_ticker_events`) |
| `related_companies(symbols)` | `/v1/related-companies/{t}` per symbol (`get_related_companies`); rows are `{"ticker": peer}`, so `Symbol` is the peer and the requested symbol is `Underlying` |
| `news(*, symbols=None, …T)`, axis `published_utc` | `/v2/reference/news` (`list_ticker_news`) |
| `exchanges(*, asset_class: AssetClass \| None = None, locale: Locale \| None = None)` | `/v3/reference/exchanges` (`get_exchanges`) |
| `conditions(*, asset_class: AssetClass \| None = None, data_type: ConditionDataType \| None = None, id: int \| None = None, sip: Sip \| None = None, …F)` | `/v3/reference/conditions` (`list_conditions`) |
| `market_status()` | `/v1/marketstatus/now` (`get_market_status`); one row; nested maps flattened |
| `market_holidays()` | `/v1/marketstatus/upcoming` (`get_market_holidays`) |

`AssetClass.FOREX` is sent as `fx` on these endpoints. `AssetClass.FUTURES` has no value on
these endpoints and raises `ValueError`.

### 8.8 `m.indicators`

All return `Query`. The symbol prefix selects the asset class. The axis is `timestamp`.

```python
sma(symbols, window: int, *, timespan: Timespan = Timespan.DAY, multiplier: int = 1,
    series_type: SeriesType = SeriesType.CLOSE, adjusted: bool = True,
    expand_underlying: bool = False, …T)
ema(...)   # same signature as sma
rsi(...)   # same signature as sma
macd(symbols, *, short_window: int, long_window: int, signal_window: int,
     timespan: Timespan = Timespan.DAY, multiplier: int = 1,
     series_type: SeriesType = SeriesType.CLOSE, adjusted: bool = True,
     expand_underlying: bool = False, …T)
```

REST: `/v1/indicators/{sma,ema,rsi,macd}/{t}` per symbol (`get_sma`, `get_ema`, `get_rsi`,
`get_macd`). `Timespan.SESSION` is rejected here.

### 8.9 `m.economy`

All return `Query`; axis `date`; each takes `…T`.

| Method | REST |
|---|---|
| `treasury_yields` | `/fed/v1/treasury-yields` (`list_treasury_yields`) |
| `inflation` | `/fed/v1/inflation` (`list_inflation`) |
| `inflation_expectations` | `/fed/v1/inflation-expectations` (`list_inflation_expectations`) |
| `labor_market` | `/fed/v1/labor-market` (`list_labor_market_indicators`) |
| `funding_conditions` | `/fed/v1/funding-conditions` (fork method `list_funding_conditions`; the released SDK has only four `/fed/v1/*` methods) |

### 8.10 `m.partners`

All return `Query`.

| Vendor | Methods | REST |
|---|---|---|
| `benzinga` | `analysts`, `analyst_insights`, `ratings`, `bulls_bears_say`, `guidance`, `earnings`, `firms` — `(symbols=None, …F)` where the endpoint takes tickers, `(…F)` otherwise | `/benzinga/v1/{analysts,analyst-insights,ratings,bulls-bears-say,guidance,earnings,firms}` (`list_benzinga_*`) |
| `benzinga` | `consensus_ratings(symbols)` | `/benzinga/v1/consensus-ratings/{t}` per symbol (`list_benzinga_consensus_ratings`; the ticker is a path segment) |
| `benzinga` | `news(*, symbols=None, …T)`, axis `published` | `/benzinga/v2/news` (`list_benzinga_news_v2`) |
| `etf_global` | `analytics`, `constituents`, `fund_flows`, `profiles`, `taxonomies` — `(symbols=None, …F)` | `/etf-global/v1/{analytics,constituents,fund-flows,profiles,taxonomies}` (`get_etf_global_*`) |
| `tmx` | `corporate_events(symbols=None, …T)`, axis `date` | `/tmx/v1/corporate-events` (`list_tmx_corporate_events`) |
| `fable` | `merchant_aggregates(…T)`, `merchant_hierarchy(…F)` | `/consumer-spending/eu/v1/{merchant-aggregates,merchant-hierarchy}` (`list_eu_merchant_aggregates`, `list_eu_merchant_hierarchy`) |

### 8.11 `m.snapshots`

`m.snapshots(symbols, *, …F) -> Query` calls the unified `/v3/snapshot`
(`list_universal_snapshots`) with `ticker.any_of`, 250 tickers per request, any mix of asset
classes. Rows are the union schema of the unified snapshot. `ALL` is not accepted here (the
endpoint requires tickers).

### 8.12 Excluded endpoints

Each exclusion appears in `tests/test_catalog.py` with a `# reason:` comment.

| Endpoint | Reason |
|---|---|
| `/v3/reference/dividends`, `/v3/reference/splits` | Deprecated; replaced by `/stocks/v1/*`. |
| `/benzinga/v1/news` | Sunset; replaced by `/benzinga/v2/news`. |
| `/vX/reference/financials` | Sunset; replaced by `/stocks/financials/v1/*`. |
| `/v1/summaries` | Not in the documentation index. |
| Crypto `/book` REST endpoint; WebSocket `XL2` | Deprecated and sunset. |
| WebSocket `LV`; Launchpad hosts and paths | Product retired. |
| WebSocket `/futures/{venue}` | Only in the SDK `Market` enum; the futures WebSocket pages list `/futures` only. |
| `/v1/historic/*`, `/v2/ticks/*` | Deprecated 2022; replaced by `/v3/trades` and `/v3/quotes`. |
| Form 5, per-asset `/v1/exchanges`, `/options/v3/*`, `/v1/reference/ipos` | Present only in the OpenAPI spec; not announced or documented. |
| `/benzinga/v1/transcripts` | Never existed. |
| Single-ticker v2 snapshots `/v2/snapshot/.../tickers/{t}` | Covered by `snapshots(symbols)` with the `tickers` filter. |
| `/v3/snapshot/options/{u}/{o}` | Covered by `m.options.snapshots`; identical fields. |

**Private SDK calls.** Exactly two documented endpoints that the released SDK lacks use
`RESTClient._paginate_iter` or `_get` until the fork methods land: `/fed/v1/funding-conditions`
and `/v1/open-close/crypto/{from}/{to}/{date}`. A test pins each shim to the fork commit and
fails when the fork method exists, so the shim is removed on the next upgrade.

## 9. Filters

Parameters are hybrid: typed primaries plus one validated `filters` mapping.

Typed keyword-only parameters exist for:

- identity (`symbols`, `underlying`, `product_code`, `filer_cik`, and so on);
- the primary time axis (`start`, `end`) where the row has one;
- closed categoricals, as enums;
- shape parameters (`timeframe`, `contract_type`, `window`);
- `sort`, `order`, `limit`.

Every other SDK filter (the `_gt`, `_gte`, `_lt`, `_lte`, and `_any_of` variants; the SDK has
over a thousand such parameter lines) goes through `filters: Mapping[str, FilterValue]`, where
`FilterValue = str | int | float | bool | date | datetime | Sequence[str]`, using the SDK's
parameter names:

```python
m.options.contracts(underlying="NVDA",
                    filters={"strike_price_gte": 100, "expiration_date_lte": "2026-01-16"})
```

Validation is synchronous at query construction. `_catalog.py` holds a generated table
`SDK_PARAMS[method] = {name: kind}`, keyed by SDK `RESTClient` method name and produced by
`scripts/refresh_massive_catalog.py` from `inspect.signature(RESTClient.<method>)`; a dataset's
accepted filters are the union over its methods plus its `extra_params`. An unknown key or a
value of the wrong kind raises `ValueError` naming the valid keys. A test asserts that the vendored table equals the installed
SDK's signatures, so an SDK upgrade that adds a filter fails CI until the table is regenerated.

`start` and `end` map to the SDK parameter pair recorded per dataset in the catalog
(`timestamp_gte`/`timestamp_lt`, `published_utc_gte`/`published_utc_lte`,
`ex_dividend_date_gte`/`ex_dividend_date_lte`, `filing_date_gte`/`filing_date_lte`,
`session_end_date_gte`/`session_end_date_lte`, and so on). Passing the same axis through
`filters` raises `ValueError`.

## 10. Symbols and `ALL`

```python
Symbols = str | Sequence[str] | Table | _All      # dm.ALL is the only _All instance
```

- A `str` is one symbol. A `Sequence[str]` is materialized to a tuple at construction. An
  exhausted generator raises `ValueError("no symbols")`.
- **`symbols` is required on every market-data dataset, including every `snapshots()`.** `ALL`
  is accepted by stream verbs (the `<channel>.*` topic), by every per-class `snapshots(ALL)`
  (full market, all pairs, all contracts), and by `history()` when a flat file or a grouped-daily
  REST path exists. Everywhere else `ALL` raises `ValueError` naming the alternative.
- Input forms are normalized: `"EUR/USD"` and `"C:EURUSD"`; `"AAPL250117C00150000"` and
  `"O:AAPL250117C00150000"`; `"BTC-USD"` and `"X:BTCUSD"`; `"I:SPX"` and `"SPX"` (an index
  symbol without its prefix gets `I:`); `"ESZ5"`. The `Symbol` column always holds the canonical
  Massive form. A crypto pair must end in a known quote currency, so its WebSocket form
  (`BTC-USD`) can always be recovered; others raise `ValueError` at construction. A symbol
  with another class's prefix raises `ValueError` saying which prefix, if any, the class uses
  (`'X:BTCUSD' carries the prefix of another asset class; stocks symbols have no prefix`).
- **Datasets that span asset classes** (indicators, reference data, the unified
  `m.snapshots()`) declare no asset class. Each symbol's prefix then selects its class (`O:`
  options, `I:` indices, `C:` forex, `X:` crypto), a symbol without one is a stock, and that
  class's rules apply unchanged: `["X:BTCUSD", "i:spx", "aapl"]` normalizes to
  `("AAPL", "I:SPX", "X:BTCUSD")`. A WebSocket dataset always declares its class.
- A `Table` must have a `Symbol` column. Streams listen to it and subscribe or unsubscribe as
  rows are added or removed. Polls re-read it each cycle. Its rows may use any accepted input
  form: every consumer reads the table through one canonicalized view
  (`_symbols.canonical_view`), so `last_by()`'s `where_in` filter and the subscribed topics
  agree on `C:EURUSD` whether the row says `EUR/USD` or `C:EURUSD`. A row that is not a valid
  symbol for the class matches nothing and is logged once at WARNING.
- `Symbol` is always the instrument the row describes. When the fan-out key is a different
  instrument, it is `Underlying` on every options row (from `underlying_ticker` or
  `underlying_asset.ticker`) and on `related_companies` rows. Non-instrument fan-out keys use
  the SDK field name in PascalCase (`FilerCik`).

## 11. Enums and types

**Python floor:** `requires-python = ">=3.11"`. `StrEnum`, `datetime.UTC`, `typing.Self`,
`tomllib`, and exception groups are native. The CI matrix is 3.11, 3.12, 3.13, 3.14, and 3.14t;
the free-threaded 3.14t job runs with `PYTHON_GIL=0` and is required. 3.14t is the only
free-threaded target: pyarrow, numpy, and pandas no longer publish cp313t wheels.

**Criterion.** Every closed set of values in a public signature is a `StrEnum` whose values are
Massive's wire strings. Signatures take only the enum. There are no string `Literal`
categoricals and no `Enum | Literal` unions. Open sets (`sort`, `form_type`, `type`) are
`str`. The one spelling per value gives tab completion in every IDE, including the Deephaven web
console, and typo safety at both type-check time and run time.

**Runtime.** Passing a `str` raises `TypeError` naming the expected member
([section 7.3](#73-runtime-enum-check)).

**Enforcement.** `tests/test_enums.py` asserts that no public signature in the package contains a
`Literal` of strings, and that every enum that mirrors an SDK enum has the same value set.

| Enum | Members | Wire values | Used by |
|---|---|---|---|
| `AssetClass` | STOCKS, OPTIONS, FUTURES, INDICES, FOREX, CRYPTO | `stocks`, `options`, `futures`, `indices`, `forex`, `crypto` (the cluster paths; `FOREX` is sent as `fx` on REST reference endpoints) | `ws_hosts`, diagnostics, `reference.ticker_types`, `reference.exchanges`, `reference.conditions` |
| `Timespan` | SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR, SESSION | `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`, `session` | `bars`, `indicators`; selects channel, REST path or resolution, flat-file prefix, and overload |
| `PollMode` | REPLACE, UPSERT, APPEND | `replace`, `upsert`, `append` | `poll()` |
| `HistorySource` | AUTO, REST, FLAT_FILES | `auto`, `rest`, `flat_files` | `history()` |
| `FuturesVenue` | CME, CBOT, NYMEX, COMEX | `cme`, `cbot`, `nymex`, `comex` | futures factory methods; flat-file prefix only |
| `Direction` | GAINERS, LOSERS | `gainers`, `losers` | `movers()` |
| `ContractType` | CALL, PUT, OTHER | `call`, `put`, `other` | `options.chain`, `options.contracts` |
| `SeriesType` | OPEN, HIGH, LOW, CLOSE | `open`, `high`, `low`, `close` | `indicators.*` |
| `Timeframe` | ANNUAL, QUARTERLY | `annual`, `quarterly` | `stocks.financials.*` |
| `Order` | ASC, DESC | `asc`, `desc` | the `…F` block |
| `Market` | STOCKS, OTC, INDICES, FX, CRYPTO | `stocks`, `otc`, `indices`, `fx`, `crypto` | `reference.tickers` |
| `Locale` | US, GLOBAL | `us`, `global` | `reference.ticker_types`, `reference.exchanges` |
| `ConditionDataType` | TRADE, BBO, NBBO | `trade`, `bbo`, `nbbo` | `reference.conditions` |
| `Sip` | CTA, UTP, OPRA | `CTA`, `UTP`, `OPRA` | `reference.conditions` |

Internal, not exported: `Recency {REALTIME, DELAYED, UNKNOWN}`, `ChannelFamily {TICK, FMV}`,
and `Transport {WS, REST, FLAT_FILES}`.

**Dates.** Parameters accept an ISO-8601 `str`, a `date`, or a timezone-aware `datetime`. A
naive `datetime`, or an ISO string with a time but no offset, raises a `ValueError` that names
the parameter, the value, and the fix, for example `start=datetime.datetime(2025, 1, 2, 9, 30)
needs a timezone: pass an aware datetime or a date`. The same rule and message apply to a naive
`datetime` passed as a `filters` value or a typed parameter.

**ISO-8601 text.** A timestamp the wire carries as ISO-8601 text (for example `published_utc`)
becomes an `Instant`; text without a UTC offset is read as UTC. SDK fields named `*_utc` map to
`Instant` automatically; any other text timestamp needs an explicit override.

**Column types.** `Instant` for timestamps, `LocalDate` for calendar dates, `double`, `long`,
`int`, `String`, `boolean`, and `int[]` for condition lists. Each union column has exactly one
dtype, fixed in the schema registry with a test per dataset: sequence numbers are `long`, sizes
are `double` (fractional sizes exist), condition codes are `int[]`, exchange ids are `int`.

## 12. Timestamp units and day boundaries

`_timeunits.py` is a `(dataset, transport, field) -> unit` table. The normalizer refuses to
convert an unregistered field. The catalog test asserts that every timestamp-like field of every
registered SDK model and flat-file header has an entry.

| Transport and fields | Unit |
|---|---|
| WebSocket stocks `T`/`Q` (`t`, `pt`, `trft`); `A`/`AM` (`s`, `e`); `LULD`/`NOI`; options `T`/`Q`; futures `T`/`Q`; indices `V` (`t`) and `A`/`AM`; forex `C`/`CA`/`CAS`; crypto `XT`/`XQ`/`XA`/`XAS` (`t`; `XT`/`XQ` received time `r`) | ms (verified on each documentation page) |
| WebSocket `FMV` `t` | Documented as ns; the documentation sample is `1678220098130` (13 digits, ms). Rule, FMV only: `t >= 10**17` means ns, otherwise ms. Every ns value after 1973-03-03 is at least 10^17, and an ms value reaches 10^17 only in the year 3,170,000, so the rule cannot misclassify. Logged once; tested. |
| REST `/v3/trades`, `/v3/quotes`, `/v2/last/trade`, `/v2/last/nbbo`; snapshot `last_updated`, `fmv_last_updated`; futures `/trades` and `/quotes` (`timestamp`, `bid_timestamp`, `ask_timestamp`); futures aggregates `window_start` (documented as "nanosecond Unix timestamp") | ns |
| REST aggregates `t`; `/prev` `t`; indicators `timestamp`; futures snapshot `last_minute.last_updated` | ms |
| REST forex `/v1/last_quote` `timestamp`; crypto `/v1/last` `timestamp` | **UNVERIFIED**. Registered as ms with a fixture test, plus the defensive rule below. |
| Flat files: all tick files; `*_aggs_v1.window_start`; `us_indices/values_v1.timestamp` (the documentation says ms, the sample is 19 digits) | ns |

**Defensive rule for UNVERIFIED entries.** After conversion the `Instant` must fall within
[1990, 2100]; otherwise `MassiveError("timestamp unit mismatch for <dataset>.<field>")`. Wrong
units fail loudly, never silently.

**Canonical time columns:** `Timestamp`, `ParticipantTimestamp`, `TrfTimestamp`,
`BidTimestamp`, `AskTimestamp` (futures quotes), `ReceivedTimestamp` (crypto trades and
quotes), `EndTimestamp` (bars), `LastUpdated` (snapshots). Calendar dates such as the futures
`SessionEndDate` are `LocalDate`.

**`history(start, end)` normalization.** Each bound converts independently to an `Instant`. The
range is half-open: `[start, end)`. A `date` or `"YYYY-MM-DD"` `start` is that day's 00:00 in
the class day zone. A `date` `end` is the next day's 00:00, so whole days are inclusive. A
timezone-aware `datetime` is used as is.

**Day zone per class.**

- **America/New_York** for stocks, options, and indices. Aggregates `from`/`to` are passed as ET
  dates; tick endpoints get ns bounds; flat-file partition dates are the ET dates of `start` and
  `end - 1 ns`.
- **UTC** for forex and crypto.
- **Sessions** for futures. A date means the trading session whose `session_end_date` equals
  it. `trades` and `quotes` use `session_end_date.gte`/`.lte`. `bars` request `window_start`
  widened by one day on each side in America/Chicago and post-filter
  `SessionEndDate` within `[start, end]`. Flat-file keys are assumed to be keyed by session date
  (**UNVERIFIED**: the futures flat-file overview says only "standardized in Central Time"; the
  implementation reads one file and asserts that the date column matches the key, failing
  loudly otherwise). With `datetime` bounds, futures filter on `Timestamp`.

## 13. Host resolution and connection protocol

### 13.1 Invariants

1. One connection per `wss://host/path` per session. A class may therefore have up to two
   connections: one for the TICK family and one for the FMV family. (Business accounts have
   three connection slots per cluster; individual plans have no FMV.)
2. The host of a (class, family) is decided before its first connection, from configuration or
   REST evidence, and cached. The single exception is the explicit-verdict rule of
   [section 13.3](#133-connection-and-subscription-protocol), which acts on a server statement,
   never on silence.
3. No connection is ever opened to test anything.
4. Connects are single-flight. A reconnect after a connection that opened has closed
   (including a `max_connections` refusal, which arrives after a successful handshake) waits at
   least `reconnect_backoff` (30 s by default, the upper end of the 10–30 s the knowledge base
   gives the server to release a dropped connection); the wait doubles up to
   `max_reconnect_backoff`, with upward-only jitter clamped to that range. Before a connection
   opens, a TCP failure, a timeout, or a retryable handshake failure (HTTP 500/502/503/504)
   retries on the websockets library's own schedule (up to 5 s at first, growing to 90 s); any
   other handshake error is fatal and fails the connection's streams (§ 13.3,
   `raise_fatal_errors=True`).
5. Futures streams always use `/futures`. `/futures/{venue}` is undocumented and unsupported.

### 13.2 Host decision

Resolution runs synchronously on the calling thread at the first stream verb for a
(class, family) and is cached in the session. The seam is
`RestTransport.resolve_host(asset_class, family) -> HostDecision` (a protocol method defined in
P2 and implemented in P3), where `HostDecision(host, recency, evidence)` is a frozen record.
The WebSocket transport calls it. The REST transport is the one place the decision is
cached: per session, for the session's life, under its own leaf lock (never `registry_lock`),
released before the REST call. Each new decision writes the pair's `Transport=REST` row of
`m.connections()`.

```
resolve(asset_class, family) -> HostDecision(host, recency, evidence)

FMV family (any class):  business.massive.com, REALTIME,
                         evidence="FMV is Business-only; fixed host (docs)"

TICK family:
  1. ws_hosts[asset_class] present
       -> that host, UNKNOWN, evidence="ws_hosts"
  2. FOREX, CRYPTO
       -> socket.massive.com, REALTIME,
          evidence="single documented host; Starter is real-time"      (no REST call)
  3. One REST call through the SDK RESTClient, raw results:
       STOCKS  : GET /v3/snapshot?ticker.any_of=SPY,AAPL        (list_universal_snapshots)
       OPTIONS : GET /v3/snapshot/options/SPY?limit=1            (list_snapshot_options_chain)
       INDICES : GET /v3/snapshot/indices?ticker.any_of=I:SPX     (list_snapshot_indices)
       FUTURES : GET /futures/v1/snapshot?product_code=ES&limit=1 (get_futures_snapshot)
     Per-ticker rows carrying "error"/"message" (NOT_ENTITLED) count as absent.
  4. Classify:
     a. HTTP 403 / NOT_AUTHORIZED
          -> raise EntitlementError("<class> plan has no WebSocket access")   # Basic plans
     b. Read the class's ordered timeframe fields, excluding underlying_asset:
          STOCKS, OPTIONS: last_trade.timeframe, last_quote.timeframe
          INDICES:         timeframe (top level; documented DELAYED | REAL-TIME)
          FUTURES:         last_trade.timeframe, last_quote.timeframe, last_minute.timeframe
                           (SDK model fields; absent from the documentation page)
        If any value is DELAYED  -> delayed.massive.com, DELAYED, evidence="<field>=DELAYED"
                                    (DELAYED wins conflicts; the field used is logged)
        elif any is REAL-TIME    -> socket.massive.com,  REALTIME, evidence="<field>=REAL-TIME"
     c. No timeframe found:
          STOCKS, OPTIONS with a top-level "fmv" field (Business key without a trades/quotes
          expansion)
              -> business.massive.com, REALTIME,
                 evidence="Business key without trades/quotes; base plan streams FMV-based
                 aggregates (UNVERIFIED host)"; WARNING once
          STOCKS, OPTIONS otherwise
              -> delayed.massive.com, DELAYED,
                 evidence="no trades/quotes in snapshot => below Developer (plan tables)"; INFO
          INDICES
              -> delayed.massive.com, UNKNOWN; WARNING (the field is documented; its absence
                 is anomalous)
          FUTURES
              -> socket.massive.com, UNKNOWN, evidence="no timeframe in futures snapshot";
                 the explicit-verdict rule of 13.3 applies
     d. Transport error -> MassiveError; nothing cached; the table fails with that error
  5. Record the decision in m.connections().
```

Why these rules hold (verified plan tables): the unified snapshot is not included on Stocks
Basic, is delayed on Starter and Developer, and is real-time on Advanced and Business.
`last_trade` and `last_quote` are "only returned if your plan includes trades/quotes". Stocks and
Options trades over WebSocket are Developer and above (delayed) and Business plus expansion
(real-time), on `socket.` and `delayed.`. Options `AM` is Starter and above (delayed). Options
Basic has no chain snapshot (403, rule 4a) and no WebSocket. The option-chain sample shows
`underlying_asset.timeframe: "DELAYED"` beside `last_trade.timeframe: "REAL-TIME"`, which is why
`underlying_asset` is excluded. The forex and crypto WebSocket pages list only
`socket.massive.com`, with Starter real-time, so no REST call is made. The futures snapshot
documentation lists no `timeframe` (only the SDK model has it), so absence proves nothing, and a
real-time customer must never be routed to delayed data by inference.

**Base Business key, TICK family.** A Business key without a trades/quotes expansion is routed
to `business.massive.com` with a WARNING. A wrong guess cannot misbehave silently: the server
either refuses the topic (an explicit `error` that fails that stream and names
`ws_hosts[AssetClass.STOCKS]`) or serves what the plan includes. A live integration test
(`tests/test_hosts_integration.py`, skipped without a key) verifies the host.

**Indices `V`.** The Value page lists only `wss://business.massive.com/indices`, while its plan
table says Starter delayed and Advanced real-time, and the indices `A`/`AM` pages list
`socket.` and `delayed.`. **UNVERIFIED.** Conservative rule: `V` is in the TICK family and uses
the class's TICK host. A `not authorized` reply for a `V` topic fails that stream with a message
naming `ws_hosts[AssetClass.INDICES]` and `business.massive.com`. A fixture test covers both
replies; the live integration test must retire this note before 1.0.

### 13.3 Connection and subscription protocol

The library drives the forked `WebSocketClient` from one client thread per connection:

```python
client = WebSocketClient(api_key, feed=host, market=path, raw=True,
                         max_reconnects=None,
                         reconnect_backoff=cfg.reconnect_backoff,
                         max_reconnect_backoff=cfg.max_reconnect_backoff,
                         max_size=None)                     # websockets frame cap off
thread.run(lambda: asyncio.run(client.connect(conn.on_frame)))
```

In raw mode the SDK consumes only the handshake frames (`connected`, the auth reply) and passes
every later text frame to the processor unparsed. That includes subscribe acknowledgements,
refusals, and `max_connections`. `conn.on_frame` decodes each frame exactly once with the
library's JSON codec, routes `ev == "status"` objects to the status handler, and appends data
objects to the per-channel row buffer. The client itself keeps the standard-library codec for its
handshake and subscribe frames (`orjson.dumps` returns bytes, which would produce binary frames).

Responsibilities:

| Step | Owner |
|---|---|
| Open the socket, read `connected`, send auth, read the auth reply | fork client |
| `auth_failed` | The fork raises `AuthError` out of `connect()`. The library marks the session failed and fails every stream on the path with `AuthenticationError`. |
| `max_connections` status before a server close | Received as a raw frame after the handshake; during the handshake the fork surfaces it to the caller (fork item F2.6), so the library can tell it from a network drop. The fork reconnects after backoff; the library shows `State=WAITING_SLOT`; streams stay alive. |
| Reconnect after any close unless `request_close()` was called; merge pending subscriptions; after a connection that opened closes (including a `max_connections` refusal), a backoff of at least `reconnect_backoff`, doubling to `max_reconnect_backoff`; before a connection opens, a TCP failure, a timeout, or a retryable handshake failure (HTTP 500/502/503/504) retries on the websockets library's own schedule (up to 5 s at first, growing to 90 s); any other handshake error is fatal and fails the connection's streams (§ 13.3, `raise_fatal_errors=True`) | fork |
| Desired topic set, reference counts, single in-flight step | library |
| Sending the subscribe and unsubscribe frames for a step | fork `set_subscriptions(desired, resend=...)` |
| Per-topic acknowledgement parsing, watchdog, re-send | library, from the status frames |
| Routing data frames to streams; Deephaven publishing | library |

**Reconcile loop.** It runs on the connection's loop thread and is entered through
`loop.call_soon_threadsafe`.

```
desired D = topics with ref-count > 0, minus deferred options contracts (section 15)
acked   A = topics the server acknowledged

when no step is in flight and D != A:
   removes = A - D  -> client.set_subscriptions(D): one unsubscribe frame; removed immediately
                       (unsubscribe acknowledgements are UNVERIFIED and not awaited)
   adds    = D - A  -> one subscribe frame per step, at most 1,000 topics per frame
                       (the frame-size limit is UNVERIFIED; the chunk size is conservative);
                       start a watchdog per topic

status "success", message "subscribed to: <p1>[,<p2>...]"
   -> those topics are ACKED. Cluster-agnostic text match; tolerates a comma list. The
      knowledge base shows one status object per topic, in one array.
status "success" with an unparseable message
   -> WARNING once; treated as an acknowledgement of the whole in-flight step
status "error" naming a topic
   -> that topic's streams fail with EntitlementError (publish_failure); the topic leaves D
status "error" with no topic
   -> applies to every topic of the in-flight step (exact attribution, because one step is in
      flight). See the explicit-verdict rule below.
no acknowledgement within ack_timeout
   -> client.set_subscriptions(D, resend={topic}) once
still none
   -> topic state NO_ACK; ERROR logged; reconnect scheduled under the backoff rules; streams
      are not failed
server close or network error
   -> the fork reconnects; the library shows State=RECONNECTING; after auth_success the fork
      re-sends D; watchdogs restart
shutdown callback (section 6.1)
   -> ref-count decrement; topics at zero leave D
a connection whose D stays empty for 30 s
   -> closed by the connection loop (idle grace); the row is kept in m.connections() with
      State=CLOSED
```

**Explicit-verdict rule.** When a status `error` with no topic arrives on a `socket.` TICK
connection and its message contains "real-time" (case-insensitive; the exact text is
**UNVERIFIED**, the knowledge base quotes only "not authorized"), the library re-resolves the
host only if all three hold:

1. the connection's cached `Recency` is `UNKNOWN` (the futures default);
2. every topic of the in-flight step was refused;
3. the host is not pinned by `ws_hosts`.

Then: close, wait the backoff, reopen on `delayed.`, cache `DELAYED` with
`evidence="server verdict: <message>"`, log a WARNING, and do not fail the streams. This happens
at most once per (class, family) per session. In every other case the verdict is an
`EntitlementError` for that step's streams.

`publish_failure` is used only for `AuthenticationError`, `EntitlementError` (an explicit server
`error` for the stream's topic, or REST 403 for `poll()`), and unrecoverable library errors such
as a schema mismatch. `max_connections`, closes, and `NO_ACK` are diagnostic states.

### 13.4 REST

One host. HTTP 403 becomes `EntitlementError` from `fetch()` and `history()` and fails the table
from `poll()`. HTTP 429 is retried by the SDK's urllib3 `Retry` (which honours `Retry-After`) and
then becomes `MassiveError("rate limited ...")`; the library logs once at WARNING that Basic keys
allow 5 requests per minute and that fan-out `poll()` multiplies requests. `BadResponse` carries
the HTTP status after the fork fix; until then the library parses the status from the body.
Per-ticker `NOT_ENTITLED` rows in snapshots keep their `Error` and `Message` columns and are
logged once.

## 14. Publishers, fan-out, and failure semantics

- **One stream per unique (dataset, channel, symbol key).** The symbol key is the sorted tuple
  of canonical symbols, `ALL`, or `id(table)` for a reactive table. A second verb on the same
  query, or an equal query, reuses the stream: `m.stocks.quotes(s).last_by()` twice costs one
  publisher and two derived tables. A futures `venue` is not part of the key.
- **Reactive symbol tables** are held strongly by the stream and managed in the stream scope.
  Two Python wrappers of one Java table have different ids and do not share a stream.
- **Library threads never create refreshing tables outside an explicit scope.** Refreshing
  tables are created only on the caller thread inside a result scope or a stream scope, and in
  the session's diagnostics scope. The flush thread creates only static tables.
- Each stream owns one
  `table_publisher(name=f"massive.{dataset}#{stream_id}", col_defs=<dataset schema>, on_shutdown_callback=...)`,
  created inside the stream scope under the session's captured `ExecutionContext` by
  `StreamTransport.open_stream(spec, request, channel, *, stream_id)`. The session mints
  `stream_id` and has registered the stream before the call, so the transport may call
  `SessionCore.fail_stream(stream_id, ...)` as soon as the publisher exists; the callback calls
  `SessionCore.stream_shut_down(stream_id)` before it releases the topics; the returned
  `StreamHandle` carries `stream_id`. `blink()`
  returns a new wrapper of the blink table's Java object, managed in its own result scope
  ([section 6.1](#61-how-a-live-table-is-kept-alive-and-released)); `append_only()` is
  `blink_to_append_only(blink)`; `ring(n)` is `ring_table(blink, n)`; `last_by()` is as in
  [section 7](#7-query-classes-and-verbs).
- **Fan-out per flush.** Per connection and per channel, the flush thread drains the channel's
  row buffer once, builds one Arrow batch in the dataset schema, and converts it once with
  `deephaven.arrow.to_table`: one Python-to-Arrow-to-Java conversion per channel per flush.
  Then, for each stream subscribed to that channel: `ALL` streams get `publisher.add(batch)`;
  symbol-list streams get `publisher.add(batch.where_in(stream.filter_table, "Symbol"))`, where
  `filter_table` is a cached static one-column table rebuilt only when the stream's symbol set
  changes. Target: Python work is O(rows) per channel per flush and independent of the number of
  streams; per-stream work is one Java filter and one `add`, O(rows) in Java with no Python
  per-row cost. A non-gating benchmark test records rows per second for `T.*` with 1, 10, and
  100 streams and compares `where_in` fan-out against per-stream Python grouping.
- `TablePublisher.add` is safe from the flush thread: the Java side snapshots the static batch
  and guarantees non-interleaved adds. `on_flush_callback` is not used (it blocks the update
  cycle).
- After its `add` calls, each flush reports to the session once:
  `SessionCore.update_streams({stream_id: StreamUpdate(rows_published=..., ...)})`, where
  `StreamUpdate` is a frozen keyword-only record whose fields left as None keep their values.
  One short critical section under `registry_lock`, about 20 times a second per connection at
  the default `flush_interval`; it updates the rows of every stream still in the registry
  (writing a row only once the stream has opened), ignores ids that have left it, and never
  changes `State` (only `fail_stream` does). An `error` there is a warning, and
  `StreamUpdate(clear_error=True)` sets `Error` back to null, for example once the options
  contract cap no longer defers anything. `poll()` rows take their `StreamId` from the same
  allocator, `SessionCore.next_stream_id()`, so the two kinds never collide.
- A failure fails exactly the streams whose topics the server rejected. Nothing tears down a
  whole URL on an entitlement error. A failed stream's key is unregistered at once
  (`SessionCore.fail_stream`), so the next verb with the same key opens a fresh stream rather
  than returning the failed table. A stream's registry record lives until its holds are
  released and its blink table's `on_shutdown_callback` (`SessionCore.stream_shut_down`) has
  run ([section 6.1](#61-how-a-live-table-is-kept-alive-and-released)), so a failure is shown
  whenever it is reported: during the open, after the library's last result was released
  while a derived table or an open panel still keeps the publisher alive (the row moves from
  `RELEASED` to `FAILED` with its `Error`), or after the blink table shut down while results
  are still held. A failure reported for a stream that has left the registry is only logged.
- REST `poll()` tables use a per-table publisher (`APPEND`) or a keyed `input_table`
  (`REPLACE`, `UPSERT`), created in the result scope.

## 15. Options contract cap

Massive allows 1,000 option contracts per `/options` connection. The library counts
**topics** per connection: `T.X` and `Q.X` count as two. Whether the server counts contracts or
topics is **UNVERIFIED**, so the stricter count is used (fixture test). Wildcards count zero.

Because the library knows the count before sending, a frame that would cross the cap is never
sent:

- **Static input.** `len(new topics) + in use > 1000` raises `ValueError` at verb time,
  synchronously.
- **Reactive `Table` input.** Adds are subscribed in sorted order up to the cap. The rest are
  deferred: not subscribed, not failed. `m.streams()` shows `State=RUNNING`, `Deferred=<n>`, and
  `Error="over capacity: 1234/1000; 234 deferred"`, with a WARNING on each transition. When
  removals free capacity, deferred contracts subscribe in the same order. Rows for deferred
  contracts are absent, never stale.

No all-or-nothing server refusal can occur, so frame chunking has no interaction with the cap.

## 16. `HistorySource.AUTO`

```
S3 possible   iff S3 credentials are configured
              and the dataset has a prefix (futures: one per venue, or all four when venue=None)
              and (not bars, or ((adjusted is False or the dataset has no adjusted parameter)
                                  and (multiplier, timespan) in {(1, MINUTE), (1, DAY)}
                                      or, for futures, (1, SESSION)))
              and every partition date is published:
                    now >= publish_time(date)
REST possible iff the dataset has a REST history endpoint
              and (symbols is not ALL or (bars and (1, DAY)))        # grouped daily
none possible -> ValueError listing, per source, the violated constraint
one possible  -> use it
both          -> S3 if symbols is ALL or len(symbols) >= flat_file_symbol_threshold, else REST
```

`publish_time(date)` is the first `flat_file_publish_cutoff` (New York time) strictly after the
start of the next day, `date + 1 day`, in the class day zone of
[section 12](#12-timestamp-units-and-day-boundaries). For stocks, options, and indices that is
11:30 New York time on the following day. For forex and crypto, whose days are UTC, it is the
first 11:30 New York time after the following UTC midnight. A partition is published once `now`
reaches that instant; comparing full instants, not a time of day, keeps the rule correct for
every day zone. `indices.values` is flat-file only.
`indices.bars` has no `adjusted`, so the predicate is satisfied.

Each `history()` logs `source=S3 files=<n> skipped=<dates>` or `source=REST requests=<n>`.
Missing keys on non-trading dates are skipped. A missing weekday key logs a WARNING. A missing
key for the last requested date raises
`MassiveError("... not yet published; flat files land ~11:00 ET next day")`. Futures
`venue=None` reads four prefixes per date; a 403 on any prefix raises `EntitlementError` naming
the venue and the `venue=` parameter.

Flat files are streamed: `pyarrow.csv.open_csv` reads each `.csv.gz` key in chunks; every chunk
is renamed to PascalCase, cast, and filtered by `Symbol` (`pyarrow.compute.is_in`) as pure Arrow
operations; list columns (`Conditions`, `Indicators`) are parsed from strings only on surviving
rows. No chunk is converted to Python objects. Row filtering applies the half-open `[start, end)`
bound on `Timestamp` after partition selection.

## 17. Diagnostics

Both tables are keyed `input_table`s. They live in one session-owned `LivenessScope` that
`m.close()` releases. They are written by one diagnostics writer thread per session, which
applies immutable row snapshots from a queue in order; other threads only enqueue, never wait on
the update graph for diagnostics, and never write from UG callbacks. Each time it wakes, the
writer takes everything queued, keeps the latest row per key, and applies it with one `add` per
table, so the queue stays bounded however fast rows arrive.

**`m.connections()`**: one row per connection plus one `Transport=REST` row per resolved
(class, family).

| Column | Meaning |
|---|---|
| `Transport` | `WS` or `REST` |
| `AssetClass` | enum value |
| `Family` | `TICK` or `FMV` |
| `Host`, `Path` | resolved host and cluster path |
| `Recency` | `REALTIME`, `DELAYED`, `UNKNOWN` |
| `Evidence` | the evidence string of [section 13.2](#132-host-decision) |
| `State` | `RESOLVING`, `CONNECTING`, `CONNECTED`, `WAITING_SLOT`, `RECONNECTING`, `FAILED`, `CLOSED` |
| `Topics`, `AckedTopics`, `PendingTopics`, `NoAckTopics` | counts |
| `Reconnects` | count |
| `LastMessageAt`, `NextReconnectAt`, `UpdatedAt` | `Instant` |
| `LastError` | text |

**`m.streams()`**: one row per stream. A row is written when the stream has opened and on
every change until the stream leaves the registry (its holds released and its blink table shut
down), so a released stream still kept alive by a derived table keeps reporting
`RowsPublished` and a late failure; its last row (`RELEASED`, or `FAILED` if the stream failed)
stays in the table while the session is open. At `m.close()` every stream
still in the registry is written `CLOSED` and then
the diagnostics scope is released, so those `CLOSED` rows are readable only through a table the
caller kept alive in a liveness scope of their own.

| Column | Meaning |
|---|---|
| `StreamId` | key |
| `Dataset` | `"stocks.trades"` |
| `SymbolCount`, `Symbols` | count and a truncated list |
| `HeldResults` | results currently held by the library (result records not yet released) |
| `Deferred` | options contracts deferred by the cap |
| `Transport`, `Host`, `Path` | |
| `RowsPublished` | count |
| `RequestsPerCycle` | `poll()` fan-out |
| `State` | `RUNNING`, `FAILED`, `RELEASED`, `CLOSED` |
| `Error` | text |
| `CreatedAt` | `Instant` |

`RELEASED` means "no library-held results; still publishing until the blink table dies"
([section 6.1](#61-how-a-live-table-is-kept-alive-and-released)). `FAILED` can follow `RUNNING`
or `RELEASED`; a failed stream's row keeps `FAILED` when its last result is released.

Every failure is logged at ERROR with the text the exception carries: hosts, the server message
verbatim, the decision evidence, and the nearest entitled alternative. For example: "Quotes are
not available on 15-minute delayed stock plans; `m.stocks.snapshots(symbols).poll(15)` provides
delayed NBBO via REST".

## 18. Concurrency model

Free-threaded CPython (`PYTHON_GIL=0`) is a gating target. The library's locking must be correct
without the GIL.

### 18.1 Threads

| Thread | Created by | Does | Must not |
|---|---|---|---|
| caller (console, script, user thread) | user | verbs; the host-resolution REST call; Deephaven table creation inside the result scope; registry mutation; waits, outside every lock, for another thread's open of the same stream key | hold the registry lock while calling Deephaven or waiting |
| WS client, one per connection | library | `asyncio.run(client.connect(on_frame))`; decode each frame once; route status frames; append rows to per-channel `SimpleQueue`s; the reconcile loop; idle close | any JVM call; blocking waits |
| flush, one per connection | library | every `flush_interval`: drain queues, Arrow to Java once per channel, `where_in` and `publisher.add` per stream; enqueue diagnostics row snapshots | touch loop-owned state directly |
| poller, one per `poll()` table, plus a `ThreadPoolExecutor(max_concurrency)` for fan-out | library | REST calls; `Table.snapshot()` of a reactive symbol table; `publisher.add` or `input_table` operations | run inside a listener |
| reaper, one per session | library | `scope.release()`; stream held-result counts | anything else |
| diagnostics writer, one per session | library | applies row snapshots from its queue to the `connections()` and `streams()` input tables: each batch coalesced to the latest row per key, one `add` per table | anything else |
| UG thread (Deephaven) | Deephaven | watchlist listeners (`listen`): compute symbol adds and removes and hand them to a worker | Deephaven table operations; blocking I/O; waiting on library threads |
| Java-originated (shutdown callback, UI export release) | Deephaven | `SessionCore.stream_shut_down` (under the registry lock: unmaps the key; deletes the record, or marks it for the last hold to delete); `call_soon_threadsafe` | sends; JVM calls; blocking |

### 18.2 Shared state and ownership

- `SessionCore.registry_lock` (a `threading.RLock`) guards every mutable registry: streams
  by key, results by id, connections by URL, topic reference counts, host decisions. It is a
  **leaf lock**: no Deephaven call, no network call, no blocking wait, and no wait on a
  `call_soon_threadsafe` result while holding it. Per-stream state is a frozen dataclass replaced
  atomically under the lock; readers copy references out and release the lock before working.
- Per-connection mutable state (desired, acknowledged, and pending sets; the in-flight step;
  watchdogs; the client object) is **loop-confined**: only the connection's client thread touches
  it. Other threads reach it through `loop.call_soon_threadsafe` with plain data (tuples of topic
  strings). Nothing reads loop state across threads; the loop thread enqueues diagnostics row
  snapshots on the diagnostics writer's queue, and the writer is the only thread that writes the
  diagnostics tables.
- Row buffers are one `queue.SimpleQueue` per (connection, channel). The WS thread is the only
  producer; the flush thread is the only consumer.
- The reaper queue is a `queue.SimpleQueue`. The producers are the result finalizers,
  `m.release`, and the collection finalizer of an unclosed session (`_collect_session`);
  `m.close` runs its closers on the calling thread and queues nothing.
- No code relies on the atomicity of `dict`, `set`, or `list` operations. Every shared container
  is behind the lock or has a single owner.
- `ExecutionContext` is captured at session construction. Every library thread enters it before
  any Deephaven call, each time through a fresh Python wrapper of the captured Java context
  (`_liveness.entered`). The `deephaven` wrapper stores its restore handle on the wrapper object,
  so two threads entering one shared wrapper at once make the first to exit install the other
  thread's previous (empty) context on itself.
- `LivenessScopeStack` is thread-local, so result scopes are opened and closed on the caller
  thread. `release()` runs on the reaper thread, which is allowed: the tracker is synchronized and
  drains on the releasing thread.

### 18.3 Lock order

The order is **UG lock, then `registry_lock`**. Listeners run on the UG thread while the UG
lock is held and may take `registry_lock`. Therefore no library thread acquires the UG lock,
directly or through auto-locking in `Table.snapshot()`, `where_in`, `last_by`, or
`deephaven.arrow.to_table`, while holding `registry_lock`. That single rule prevents the UG and
registry deadlock. Verbs called on the UG thread raise `ValueError`.

### 18.4 Free-threaded specifics

`PYTHON_GIL=0` jobs run the full suite, including the Deephaven integration tests (the embedded
JVM starts in `pytest_configure`). Stress tests exercise concurrent subscribe and unsubscribe
from 16 threads against a fake server, concurrent `release()` and verb calls, and concurrent
`RESTClient` use. The only free-threaded target is 3.14t, because pyarrow, numpy, and pandas no
longer publish cp313t wheels. Wheel availability on 3.14t: `jpy 2.2.0`, `pyarrow`, `numpy`, and
`pandas` ship cp314t wheels; `deephaven-core` is `py3-none-any`; `websockets 17.3` ships cp314t
wheels. `orjson 3.13.0` publishes no free-threaded wheels, so `orjson` is an optional extra and
the standard-library codec is the default. `PythonDeephavenSession.setVariable` carries the
comment "relying on the GIL being kind to us"; the library never mutates script globals, so this
does not affect it, but it is why the Deephaven integration tests must run on the free-threaded
builds rather than unit tests alone.

## 19. SDK fork: fix list and responsibility split

Fork: `github.com/chipkent/massive-client-python`. The fixes are grouped into seven topic
branches, F1 to F7. Each branch is opened as one pull request to `massive-com/client-python`.
The fork's `deephaven-fixes` branch is assembled by direct merges of the seven branches (no
pull requests into it); CI runs on every push to it. This project tracks the branch:
`[tool.uv.sources] massive = { git = "<fork>", branch = "deephaven-fixes" }`, with `[project]`
keeping `massive>=2.9.0`. `uv.lock` records the exact commit, so every install is reproducible,
and `uv lock --upgrade-package massive` moves the lock to the branch's latest commit. The branch
only moves forward: when upstream moves, the new upstream `master` is merged into it. It is
never reset or force-pushed, so every commit a lock has recorded stays reachable; a
fork ruleset blocks deletion and force pushes. Agents may push topic branches, and may merge
into `deephaven-fixes` and push it when the fork's CI is green; opening an upstream pull
request needs explicit owner approval each time. Evidence paths are relative to `site-packages/massive` in `massive==2.9.0`.

### F1 `fix/ws-subscriptions`: thread-safe, prompt, isolated subscription handling

| Item | Problem | Evidence | API change | Test |
|---|---|---|---|---|
| F1.1 thread-safe subscribe | `subscribe()` and `unsubscribe()` mutate `scheduled_subs` on the caller thread while `connect()` reads it on the loop thread; no lock | `websocket/__init__.py:273-312` vs `:136-146` | a `threading.Lock` around `scheduled_subs` and `schedule_resub`; the loop copies the set under the lock | 16 threads × 10,000 subscribe/unsubscribe calls against a fake server; the final server set equals the desired set; runs on 3.13t |
| F1.2 loop wake-up | pending subscriptions are applied only after the 1 s `recv` timeout | `:148-154` | an `asyncio.Event` set through `loop.call_soon_threadsafe` from `subscribe()`; the loop waits on `recv()` or the event | subscribe latency under 50 ms in a fake-server test |
| F1.3 wildcard isolation | `subscribe("T.*")` discards every scheduled topic whose string starts with the prefix, and `unsubscribe("T.*")` likewise; the prefix test `t.startswith(topic)` makes `"A"` match `"AM.AAPL"` | `:285-289`, `:306-310` | match on the exact channel (`t.split(".", 1)[0] == topic`); a wildcard add no longer removes explicit topics; the collapse becomes opt-in `collapse=True` | `subscribe("A.X"); subscribe("AM.*")` keeps `A.X`; `unsubscribe("T.*")` keeps `T.AAPL` |
| F1.4 `set_subscriptions` | no way to re-send an unacknowledged topic (`difference()` is empty when the topic is already in `subs`) and no declarative reconcile | `:140-145` | `set_subscriptions(desired: Iterable[str], *, resend: Iterable[str] = ())`: atomically replaces the scheduled set, emits one unsubscribe frame for removals and one subscribe frame for additions plus `resend` | frames captured by a fake server; `resend` of an acknowledged topic emits a frame |

### F2 `fix/ws-reconnect`: reconnect semantics

| Item | Problem | Evidence | API change | Test |
|---|---|---|---|---|
| F2.1 keep the desired set across reconnects | on `ConnectionClosedError` the client runs `scheduled_subs = set(self.subs)`, dropping subscribes and resurrecting unsubscribes that were scheduled but not yet applied | `:178` | treat `scheduled_subs` as the authoritative desired set and leave it untouched; reset only `subs = set()` and set `schedule_resub = True`, so the new connection subscribes exactly the desired set (a union with `subs` would resurrect pending unsubscribes) | during an outage, schedule one subscribe and one unsubscribe; after reconnect the server holds the new topic and not the removed one |
| F2.2 reconnect after a clean close | `ConnectionClosedOK` returns; a server-initiated clean close (after `max_connections` or a slow consumer) ends the client permanently | `:168-172` | reconnect unless close was requested (F2.5 sets the flag) | the fake server closes with 1000; the client reconnects; after `request_close()` it stops |
| F2.3 configurable backoff | reconnects are immediate (the `connect()` iterator's delay resets after a success); the knowledge base requires at least 15 s | `:111-117`, `:183` | `reconnect_backoff: float = 15.0`, `max_reconnect_backoff: float = 120.0`, doubling; the n-th consecutive wait is `uniform(b, min(1.2 * b, max_reconnect_backoff))` with `b = min(reconnect_backoff * 2**n, max_reconnect_backoff)`, so jitter only lengthens a wait and never leaves `[reconnect_backoff, max_reconnect_backoff]`; the client sleeps before re-entering the iterator | every measured wait lies in `[reconnect_backoff, max_reconnect_backoff]`, including at the cap |
| F2.4 `max_reconnects` semantics | `max_reconnects=5` is a lifetime total, so a long-running client eventually raises | `:106`, `:177-182` | count consecutive failed attempts, reset on `auth_success`; `None` means unlimited; docstring updated | six outages with successes between them do not raise |
| F2.5 thread-safe close | `close()` is a coroutine that touches `self.websocket` from any thread | `:321-332` | `request_close()` (synchronous, thread-safe, through `call_soon_threadsafe`) sets the close flag; `close()` kept | called from another thread; the loop exits |
| F2.6 surface handshake `max_connections` (the last commit of F2, separable so the upstream PR can drop it) | a `max_connections` status in the auth reply is only logged; the library sees a disconnect and a reconnect | `:132-133` | surface the status to the caller (for example a distinct exception or a flag) so the library can show `WAITING_SLOT` immediately | the fake server sends `max_connections` in the auth reply; the caller observes it |

### F3 `fix/ws-logging-format`

| Item | Problem | Evidence | API change | Test |
|---|---|---|---|---|
| F3.1 | `logger.warning("invalid subscription:", s)` passes `s` as a format argument with no placeholder | `:268` | `logger.warning("invalid subscription: %s", s)` | caplog |

### F4 `feat/typing`: `py.typed` and hint fixes

| Item | Problem | Evidence | API change | Test |
|---|---|---|---|---|
| F4.1 `py.typed` | no `py.typed`; mypy strict needs `ignore_missing_imports` | `massive/` has no `py.typed` | add `py.typed` | mypy strict passes on a sample consumer |
| F4.2 model hints | wrong hints: `CryptoQuote.bid_price`, `ask_price`, `ask_size` as `int` and `timestamp` as `float`; `IndexValue.timestamp: str`; `PreviousCloseAgg.timestamp: float`; `LastTrade.sequence_number: float` | `websocket/models/models.py:220-224`, `:331`; `rest/models/aggs.py:107`; `rest/models/trades.py:34` | `float`, `float`, `float`, `int`; `int`; `int`; `int` | round-trip fixtures |
| F4.3 `cusip` | `list_tickers(cusip: int)` | `rest/reference.py:95` | `cusip: Optional[str]` | the parameter accepts a 9-character string |

### F5 `fix/rest-bad-response-status`

| Item | Problem | Evidence | API change | Test |
|---|---|---|---|---|
| F5.1 | `BadResponse(resp.data.decode())` loses the HTTP status | `rest/base.py:154-155` | `BadResponse(message, *, status: int, body: str)`; `__str__` unchanged | 403 and 429 fixtures |

### F6 `feat/rest-missing-endpoints`

| Item | Problem | Evidence | API change | Test |
|---|---|---|---|---|
| F6.1 funding conditions | `/fed/v1/funding-conditions` is documented but absent | `rest/economy.py` has only four `/fed/v1/*` URLs | `list_funding_conditions(...)` mirroring `list_treasury_yields` | signature parity with the documentation |
| F6.2 crypto daily open-close | `/v1/open-close/crypto/{from}/{to}/{date}` is absent (only `/v1/open-close/{ticker}/{date}` exists) | `rest/aggs.py:154` | `get_daily_open_close_crypto(from_, to, date, ...)` | fixture |

### F7 `ci/free-threaded`

| Item | Problem | Evidence | API change | Test |
|---|---|---|---|---|
| F7.1 | no free-threaded CI | `.github/workflows` | jobs on 3.13t and 3.14t with `PYTHON_GIL=0`; the stress tests from F1.1 and concurrent `RESTClient` use | CI green |

Work package P4 (the WebSocket layer) depends on F1 and F2. The crypto and economy packages
depend on F6; until it lands, the two endpoints use the documented private-call shims of
[section 8.12](#812-excluded-endpoints). The typing package F4 removes the `massive.*` mypy
override.

**Stays in this library, never a PR:** the connection registry keyed by URL with single-flight
creation; topic reference counts and the desired-set reconcile with one in-flight step;
acknowledgement parsing and watchdogs; explicit-verdict host re-resolution; host resolution from
REST evidence; routing rows to streams; Deephaven publishers, flush, liveness scopes, and the
reaper; reactive watchlists; the options cap; schema mapping and time units; flat files;
diagnostics tables.

## 20. Quality gates

Every change must pass `scripts/check.sh full`, and every CI job including the free-threaded
3.14t job. The gates, in brief ([section 20.1](#201-tooling-configuration) gives the exact configuration):

- **Coverage:** 100% statement and branch coverage. `pragma: no cover` needs a written reason
  and owner approval.
- **Ruff:** `select = ["ALL"]` with a documented ignore list, latest version, Google docstring
  convention, `E501` at 100 columns, complexity limits enforced.
- **Mypy:** `strict` plus `warn_unreachable` and the extra error codes; the only override is
  `ignore_missing_imports` for `pyarrow.*`. Pyright runs as a non-gating second checker.
- **Pytest 9:** `strict = true`, `filterwarnings = ["error"]`, `--import-mode=importlib`,
  deadlock detection through `faulthandler_timeout` and `pytest-timeout`.
- **Layout:** `tests/test_layout.py` and `tests/test_catalog.py` pass.
- **Process:** conventional-commit titles; every change lands through a pull request; only the
  owner merges.

### 20.1 Tooling configuration

This is the exact configuration the tooling work package applies. Every lint ignore carries a
reason comment in `pyproject.toml`; adding one needs owner approval.

- **Ruff.**
  - `select = ["ALL"]`. Linters and type checkers track their latest releases: the dev group
    declares floors only (`ruff>=…`, `mypy>=…`, `pyright>=…`), `uv.lock` pins the exact versions
    for reproducible runs, and Dependabot's weekly `uv` update moves the lock to the newest
    releases. Because `ALL` enables new rules as they ship, an update PR may surface new
    findings; that PR fixes them (or proposes an ignore for owner approval).
  - `preview = true` with `explicit-preview-rules = true`, enabling only `DOC201`, `DOC501`, and
    `DOC502` (docstring Returns and Raises sections must match the code).
  - Global ignores: the rules that conflict with `ruff format` (`COM812`, `COM819`, `W191`,
    `E111`, `E114`, `E117`, `D206`, `D300`, `Q000`–`Q003`), plus `TRY003`, `EM101`, `EM102`,
    `PLR0913`, `FBT003`, and `CPY001`.
  - Enforced on purpose: `E501` at 100 columns, `ANN401` (no `Any`), the `TC` import rules,
    `SLF001` in `src/`, `C901` at a maximum of 12, the `PLR091x` limits, and `PLR0917` with
    `max-positional-args = 3`.
  - Ignored in `tests/` only: `S101`, `PLR2004`, `INP001`, `SLF001`, `ARG001`, `ARG002`,
    `ARG005`, `FBT001`, `FBT002`, and `D100`–`D107` (test docstrings are optional, but any that
    exist must be well formed).
  - Google docstring convention; `docstring-code-format = true`.
- **Mypy.** `strict`, `warn_unreachable`, `warn_unused_configs`, and `enable_error_code =
  ["ignore-without-code", "redundant-expr", "truthy-bool", "truthy-iterable",
  "possibly-undefined", "unused-awaitable", "deprecated", "explicit-override"]`. The dev group
  adds `types-boto3[s3]`. The only override is `ignore_missing_imports` for `pyarrow.*`, which
  ships no type information; the fork's `massive` ships `py.typed` (F4). No other override is
  allowed. Pyright runs as a non-gating second checker.
- **Pytest 9** (native `[tool.pytest]` table). `strict = true`, `filterwarnings = ["error"]`,
  `addopts = ["-ra", "--import-mode=importlib"]`, `faulthandler_timeout = 120` with
  `faulthandler_exit_on_timeout = true`, and `pytest-timeout` at 60 seconds. Coverage is not in
  `addopts`, so single-test runs and debuggers work.
- **Coverage.** `branch = true`, `fail_under = 100`, `exclude_also = ["if TYPE_CHECKING:",
  "@overload"]`.
- **Deephaven imports.** Module-level wherever used at run time; imports used only in
  annotations go under `TYPE_CHECKING` (enforced by `TC`). `tests/conftest.py` starts the
  embedded JVM in `pytest_configure`, before collection.
- **One entry point.** `scripts/check.sh [fast|full] [--no-fix]`. The fast tier (about 2 s) runs
  ruff fix, ruff format, and mypy; the full tier adds pytest with coverage. Local hooks and CI call
  the same script; CI passes `--output-format github` to ruff.
- **Agent hooks** (committed `.claude/settings.json`). After an agent edits a `.py` file, ruff
  fixes and formats it and reports remaining diagnostics to the agent. Before an agent ends its
  turn, `scripts/check.sh fast --no-fix` runs. Bare `python`, `pytest`, `ruff`, `mypy`, and `pip`
  commands not prefixed by `uv run` are blocked.
- **Git hooks** (`.pre-commit-config.yaml`). The commit stage runs the fast tier plus
  `no-commit-to-branch` (main), `check-yaml`, `end-of-file-fixer`, `trailing-whitespace`, and
  `uv-lock`. The full tier runs at the pre-push stage.
- **Packaging.** `uv.lock` is committed and CI runs `uv sync --locked`. Development dependencies
  use a PEP 735 `[dependency-groups]` table; license metadata follows PEP 639. `__version__`
  comes from `importlib.metadata`. `deephaven-server` is a development dependency only.
  `websockets>=14`. `[project]` declares `massive>=2.9.0`, and `[tool.uv.sources]` tracks the fork's
  `deephaven-fixes` branch, with `uv.lock` recording the exact commit. `.gitignore` lists only the files this project
  produces.
- **Metadata.** `authors = [{ name = "Chip Kent", email = "chipkent@illumon.com" }]`; repository
  `https://github.com/chipkent/deephaven-massive`. Not published to PyPI until the fork fixes are
  released upstream.

## 21. Examples

Every example below runs in the embedded test JVM against fake REST and WebSocket servers, as
a test in `tests/test_readme.py` ([section 4.2](#42-test-layout)).

**1. Stocks Starter (15-minute delayed).**

```python
import deephaven_massive as dm

m = dm.MassiveSession()                                     # MASSIVE_API_KEY from the environment
syms = ["AAPL", "MSFT", "NVDA"]
bars = m.stocks.bars(syms, dm.Timespan.MINUTE).append_only()   # snapshot has no trades/quotes
                                                               # -> delayed.massive.com/stocks
snap = m.stocks.snapshots(syms).poll(30)                    # REST; current state on every tier
daily = m.stocks.bars(syms, dm.Timespan.DAY).history("2025-01-01", "2025-06-30")   # 3 symbols -> REST
prev = m.stocks.bars(syms, dm.Timespan.DAY).fetch()         # previous completed session (/prev)
divs = m.stocks.dividends(syms, start="2025-01-01",
                          filters={"pay_date_lte": "2025-12-31"}).fetch()
conns = m.connections()                                     # Host=delayed.massive.com Recency=DELAYED
```

**2. Real-time multi-asset trader (Advanced plans).** No `with`: live tables must outlive the
statement.

```python
import deephaven_massive as dm

m = dm.MassiveSession()
syms = ["AAPL", "MSFT", "NVDA"]
bars = m.stocks.bars(syms, dm.Timespan.MINUTE).append_only()   # last_trade.timeframe=REAL-TIME
                                                               # -> socket.massive.com/stocks
quotes = m.stocks.quotes(syms).append_only()                # full NBBO history for as-of joins
trades = m.stocks.trades(syms).ring(100_000)
nbbo_at_trade = trades.aj(quotes, on=["Symbol", "Timestamp"], joins=["BidPrice", "AskPrice"])
current = m.stocks.quotes(syms).last_by()                   # shares the quotes stream; one row per symbol
chain = m.options.chain("NVDA", contract_type=dm.ContractType.CALL,
                        filters={"expiration_date_lte": "2026-01-16"}).poll(60)
otrades = m.options.trades(chain.where("Delta > 0.4")).blink()   # reactive; over 1,000 contracts
                                                                 # -> deferred, never fails
es = m.futures.trades(["ESZ5"]).last_by()                   # /futures on every plan
fx = m.forex.quotes(["EUR/USD", "USD/JPY"]).last_by()       # socket.massive.com/forex, no REST call
```

**3. Business key.**

```python
import deephaven_massive as dm

m = dm.MassiveSession()
fmv = m.stocks.fmv(dm.ALL).last_by()                        # FMV family -> business.massive.com/stocks
bars = m.stocks.bars(["AAPL"], dm.Timespan.MINUTE).append_only()
    # TICK family: a base Business key -> business.massive.com with a WARNING (UNVERIFIED host).
    # With a Full Market expansion the snapshot carries last_trade.timeframe=REAL-TIME
    # -> socket.massive.com.
xfmv = m.crypto.fmv(["BTC-USD", "ETH-USD"]).append_only()   # business.massive.com/crypto
es = m.futures.quotes(["ESZ5"]).last_by()                   # /futures; per-venue plans receive
                                                            # their venue on the same path

# If your expansion is served from a different host, pin the TICK family. FMV is never pinned.
m2 = dm.MassiveSession(dm.MassiveConfig(ws_hosts={dm.AssetClass.STOCKS: "fullmarket-business.massive.com"}))
```

**4. Flat-file history (static results only, so `with` is right).**

```python
import deephaven_massive as dm
from deephaven.parquet import write

with dm.MassiveSession() as m:
    for year in range(2020, 2026):
        t = m.stocks.bars(dm.ALL, dm.Timespan.DAY, adjusted=False).history(
            f"{year}-01-01", f"{year}-12-31")                # AUTO -> S3 day_aggs_v1
        write(t, f"/data/us_day_bars_{year}.parquet")
    q = m.stocks.quotes(["AAPL", "SPY"]).history("2025-03-03", "2025-03-03",
                                                 source=dm.HistorySource.REST)
    es = m.futures.bars(dm.ALL, dm.Timespan.SESSION, venue=dm.FuturesVenue.CME).history(
        "2024-01-01", "2024-12-31")                          # us_futures_cme/session_aggs_v1
    vals = m.indices.values(["I:SPX", "I:VIX"]).history("2025-06-01", "2025-06-30")
                                                             # flat files are the only source
```

**5. Reactive watchlist dashboard.**

```python
import deephaven_massive as dm
from deephaven import input_table, dtypes as dht

m = dm.MassiveSession()
wl = input_table(col_defs={"Symbol": dht.string}, key_cols=["Symbol"])   # edited in the UI
wl_quotes = m.stocks.quotes(wl).last_by()        # subscriptions follow edits; removed rows are evicted
wl_trades = m.stocks.trades(wl).ring(10_000)
movers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
mover_quotes = m.stocks.quotes(movers).last_by()                          # follows the top 20
mover_ratios = m.stocks.financials.ratios(movers).poll(600)               # 20 requests per cycle
m.release(wl_trades)        # deterministic: T topics unsubscribed when no other table holds the stream
del wl_quotes               # released at the end of this console command, unless a UI panel shows it
```

**6. Backfill plus live.**

```python
import deephaven_massive as dm
from deephaven import merge

m = dm.MassiveSession()
q = m.stocks.trades(["AAPL"])
t = merge([q.history("2026-10-20", "2026-10-23"), q.append_only()])
```

## 22. Decisions

The owner settled each of the following. They are recorded here as rationale; the body of this
document already reflects them.

1. **Official SDK.** REST goes through `RESTClient` `list_*` and `get_*` methods. WebSocket goes
   through `WebSocketClient(raw=True)` from the owner's fork, inside a thin wrapper that holds
   only Deephaven-specific logic. SDK bugs are fixed in the fork, not worked around here. Private
   `_get` calls are allowed only for documented endpoints the SDK lacks, each listed explicitly.
2. **All SDK fields as columns.** REST datasets expose every SDK model field, flattened to
   PascalCase, with an explicit, tested override table for wrong hints and timestamp units. The
   four WebSocket tick schemas keep hand-written per-transport key maps. Fallback chains are gone.
3. **No seeding.** `last_by()` never seeds from REST, and there is no `seed_since`. The
   documented recipe is `merge([q.history(start, end), q.append_only()])`.
4. **Reactive Deephaven-table watchlists are kept.**
5. **No embedded server.** The library requires an existing `ExecutionContext` and fails fast
   without one. `deephaven-server` is a development dependency only.
6. **Lifecycle is automatic plus explicit.** A garbage-collected result releases its scope
   through a weakref and the reaper thread. `m.release(table)` and `m.close()` give deterministic
   control. Topic reference counts are bound to the blink table's real shutdown, so derived
   tables and UI panels keep a stream alive. Safeguards: a once-only release flag and a guarded
   reaper. There is no `stop()` on queries.
7. **Configuration is `MassiveSession(MassiveConfig(...))` only.** Defaults live in one place.
8. **Flat files are reachable only through `history(source=...)`.** There is no public
   `flat_files` namespace.
9. **100% statement and branch coverage.** `pragma: no cover` needs a written reason and owner
   approval.
10. **Partners stay nested** under `m.partners.<vendor>`.
11. **Entitlement refusals fail the affected table** through `publish_failure`, are logged, and
    appear in the diagnostics tables.
12. **Endpoint policy.** Everything documented is supported, plus endpoints that are new but lag
    in the documentation or SDK. Deprecated and sunset endpoints are excluded with a reason.
    Undocumented endpoints are unsupported.
13. **Python 3.11 and above.** The CI matrix includes 3.14t as a required job; it is the only
    free-threaded target because pyarrow, numpy, and pandas no longer publish cp313t wheels.
14. **Enums only.** Every closed categorical set is a `StrEnum` with wire-string values; a plain
    string raises `TypeError`. This gives one spelling per value, tab completion everywhere
    including the Deephaven web console, and typo safety at both type-check time and run time.
15. **Base Business key, TICK family:** `business.massive.com` with a WARNING, verified by a
    live integration test. A wrong guess yields an explicit server refusal that fails one stream
    and names `ws_hosts`; it cannot misbehave silently, and it spares every base Business user a
    configuration line.
16. **WebSocket status handling needs no SDK API change.** In raw mode every frame after the
    handshake reaches the processor, so the library parses frames once and routes status frames
    itself. One fork item, F2.6, surfaces a `max_connections` status received during the
    handshake, so `WAITING_SLOT` is accurate from the first attempt.
17. **The context manager is kept**, documented as static-results-only.
18. **`m.options.snapshots(dm.ALL)` is allowed.** It paginates the whole OPRA universe, which is
    slow and documented as such. The rule "`ALL` is accepted by every per-class `snapshots()`"
    stays uniform.
19. **No live testing as design input.** Unverifiable facts get the conservative choice and a
    fixture test; live tests come later and retire the UNVERIFIED notes.
20. **All merges into `main` happen through pull requests, and only the owner merges.** In the
    fork, each topic branch goes upstream as one pull request; the fork's `deephaven-fixes`
    branch is assembled by direct merges of those branches, with CI on every push, is never
    reset or force-pushed, and is tracked here by branch, with `uv.lock` recording the exact commit.
21. **Sessions and queries expose no introspection attributes; their reprs are defined.**
    `MassiveSession` has no `name` or `config`, and queries have no `dataset`: a session's label
    appears only in its repr (`<MassiveSession #1 open>`), the same-key WARNING, and error
    messages, and a query's dataset and arguments only in its repr. The public surface stays
    exactly the verbs and namespaces, and the API key stays out of objects users routinely
    leave in console globals.

## 23. Open verification items

Each item is handled conservatively now, covered by a fixture test in its unit-test module, and
retired by a live integration test (`*_integration.py`, skipped without `MASSIVE_API_KEY`) in the
later live-testing work package. Each item that a live test refutes becomes an owner decision
before 1.0.

| # | Item | Conservative handling | Fixture test | Live integration test |
|---|---|---|---|---|
| 1 | Unit of REST forex `/v1/last_quote` `timestamp` and crypto `/v1/last` `timestamp` | Registered as ms; the [1990, 2100] range check raises `MassiveError` on a mismatch | `tests/test_timeunits.py`: ms and ns fixtures; the out-of-range path raises | `tests/test_timeunits_integration.py`: one request each; the converted `Instant` is within 1 day of now |
| 2 | Unit of WebSocket `FMV` `t` (documented ns, sample ms) | Magnitude rule for FMV only; logged once | `tests/test_timeunits.py`: both magnitudes convert correctly | `tests/_domains/test_stocks_integration.py` (Business key): one FMV frame converts to a current `Instant` |
| 3 | Futures flat files keyed by session date | Read one file and assert the date column matches the key; fail loudly otherwise | `tests/test_flatfiles.py`: matching and mismatching fixtures | `tests/test_flatfiles_integration.py` (S3 credentials): one key per venue |
| 4 | Indices `V` host (`business.` on the page; `socket.`/`delayed.` in the plan table) | `V` uses the class TICK host; a refusal names `ws_hosts[AssetClass.INDICES]` and `business.massive.com` | `tests/test_hosts.py`: both replies | `tests/test_hosts_integration.py`: subscribe `V.I:SPX` on the resolved host |
| 5 | TICK host for a base Business key | `business.massive.com` with a WARNING | `tests/test_hosts.py`: the snapshot shape with `fmv` and no trades/quotes resolves to `business.` | `tests/test_hosts_integration.py` (Business key): `AM.AAPL` is acknowledged on the resolved host |
| 6 | Futures snapshot `timeframe` fields (SDK model only) | Absence yields `socket.` with `Recency=UNKNOWN` and the explicit-verdict rule | `tests/test_hosts.py`: with and without the fields | `tests/test_hosts_integration.py`: record which fields the live payload carries |
| 7 | Exact text of the delayed-key verdict message ("real-time") | Case-insensitive substring match; limited to `Recency=UNKNOWN`, whole-step refusal, unpinned host | `tests/test_ws.py`: verdict and non-verdict messages | `tests/test_ws_integration.py` (delayed key): subscribe on `socket.` and capture the message |
| 8 | Unsubscribe acknowledgements | Not awaited; topics are removed immediately | `tests/test_ws.py`: removal without an acknowledgement | `tests/test_ws_integration.py`: record whether an acknowledgement arrives |
| 9 | Subscribe frame size limit | At most 1,000 topics per frame | `tests/test_ws.py`: 2,500 topics produce three frames | `tests/test_ws_integration.py`: a 1,000-topic frame is acknowledged |
| 10 | Options cap counted in contracts or topics | Topics (the stricter count) | `tests/test_ws.py`: `T.X` plus `Q.X` counts as two | `tests/test_ws_integration.py` (Options Advanced): 501 contracts on `T` and `Q` |
| 11 | Exact `max_connections` status text and timing | Any status `max_connections`, after the handshake or surfaced by the fork during it (F2.6), shows `WAITING_SLOT` | `tests/test_ws.py`: both placements | `tests/test_ws_integration.py`: two sessions with one individual key |
| 12 | `orjson` free-threaded wheels | Optional extra; standard-library codec by default | `tests/test_config.py`: codec selection with and without `orjson` importable | CI: the 3.14 job installs the extra (`--extra orjson`) and runs the suite with it; the 3.14t job does not, because `orjson` publishes no cp314t wheel |
| 13 | Flat-file publish time (documented "approximately 11:00 AM ET") | `flat_file_publish_cutoff` defaults to 11:30 ET | `tests/test_flatfiles.py`: AUTO before and after the cutoff | `tests/test_flatfiles_integration.py`: yesterday's key exists after the cutoff |
