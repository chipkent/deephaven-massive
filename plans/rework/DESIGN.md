# deephaven-massive: Design Specification

This document is the approved design for the clean rework of `deephaven-massive`, a Python
library that exposes Massive.com (formerly Polygon.io) market data as live and static
[Deephaven](https://deephaven.io) tables. It is the source of truth for every implementation
work package in `PLAN.md`. It changes only by owner decision.

The design was checked against Massive's OpenAPI spec (`https://api.massive.com/openapi`,
147 paths), its WebSocket spec (`https://api.massive.com/specs/websocket.json`, 37 channels),
`llms.txt`, and the flat-file dataset and overview pages, all as of 2026-10-26;
`deephaven-core` 42.7 (Python package; Java behaviour from the `deephaven-core` `main` branch);
and the massive.com documentation as of 2026-10-23. Facts that could not be verified from those
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
19. [Wire clients and the spec snapshot](#19-wire-clients-and-the-spec-snapshot)
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
Deephaven server. It does not use Massive's Python SDK. It speaks Massive's documented REST,
WebSocket, and S3 flat-file protocols directly, through its own clients, and it generates every
schema from vendored, pinned vendor specs. All wire access lives in one private package,
`deephaven_massive._massive`, behind one small interface
([section 19](#19-wire-clients-and-the-spec-snapshot)). The runtime dependencies are
`deephaven-core`, `pyarrow`, `boto3`, `botocore`, `urllib3`, `websockets`, and `certifi`.

There are no existing users. Backward compatibility with the previous code base is not a goal,
and nothing in this design exists to avoid breaking a user: no compatibility shim, no
deprecation path, and no rule that rests on the chance of breaking someone.

## 2. Principles and terms

### 2.1 Principles

1. **Datasets are named by what they are, not how they travel.** One method per dataset. One
   schema per dataset kind: the union of the fields Massive documents for the kind's sources
   across asset classes and transports, merged at run time from the generated tables
   ([section 19.4](#194-generated-schemas)). Column names come from the generated tables
   (the lexicons and `NAMES`) and from each dataset's declared key columns, never from a field
   description. A column a class or transport does not supply is null (crypto trades have no
   `Tape`; stock bars have no `SettlementPrice`). `Timestamp` is the primary time column on
   every tick, bar, and value row. `Symbol` is the instrument on every instrument row.
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
8. **Direct over Massive's documented APIs, in one package.** The library owns its REST,
   WebSocket, and S3 flat-file clients. They live with the vendored spec snapshot, the
   generator, and the generated schemas in the private package `_massive`, split into six
   subpackages (`net/`, `spec/`, `rest/`, `ws/`, `flatfiles/`, `refresh/`). `_massive` imports
   nothing from Deephaven or the rest of the library and is reached only through its interface
   ([section 19](#19-wire-clients-and-the-spec-snapshot)). A parity test keeps the generated
   files in step with the snapshot, and an advisory weekly job reports drift in the live specs.
9. **No live testing as design input.** Unverifiable facts get the conservative choice and a
   fixture test. Live integration tests are a later work package.
10. **Closed categorical sets are enums.** Every closed set of values in a public signature is a
    `StrEnum`. No public signature accepts a string in place of an enum member.
11. **Schemas are generated at build time and fixed before data.** No schema is hand-written,
    and none is inferred from data. A live table must know its full column set and types before
    its first row, the first message may omit optional fields (stocks `T` omits `pt` on OTC
    trades), and two sessions must agree on a dataset's shape.

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
| source | One generated wire schema: a REST path template, a WebSocket `(cluster, channel)`, or a flat-file prefix. |
| kind | A dataset kind: the datasets whose sources share one merged schema (trades, quotes, bars, …), defined in `KINDS` and recorded on each generated source as a `Membership`. |
| shape | An object shape in a payload (an aggregate row, a snapshot `day` object, a WebSocket channel), which selects a lexicon. |
| drift | A difference between a payload on the wire and its committed generated schema. |
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
dm.AuthenticationError     # subclass of MassiveError: bad key (API key or S3 credentials)
dm.EntitlementError        # subclass of MassiveError: the plan lacks the dataset, host, or timeframe
                           # every MassiveError has .status (HTTP status or None) and .request_id (or None)

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

`MassiveError` and its two subclasses carry two read-only attributes, set when the error came
from an HTTP response and `None` otherwise: `status`, the HTTP status (`int | None`), and
`request_id`, the `request_id` of Massive's response (`str | None`). Users branch on `status`
(for example 404 or 429) instead of parsing messages.

`__init__.py` resolves these names lazily, through a module `__getattr__` (PEP 562), with the
eager imports under `TYPE_CHECKING` mirroring `__all__`. Importing
`deephaven_massive._massive` therefore never imports Deephaven.

A test asserts that `dm.__all__` equals this list, that `getattr(dm, name)` resolves every name
in it through the lazy `__getattr__`, and that no public signature in the package
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

```
src/deephaven_massive/
__init__.py          public names of section 3, lazy (PEP 562); TYPE_CHECKING imports mirror __all__
_config.py           MassiveConfig (section 5)
_session.py          MassiveSession, SessionCore, closers, reaper, stream ids (section 6)
_query.py            the six query classes and their verbs; HostDecision; transport protocols (section 7)
_enums.py            the fourteen public enums, Recency, ChannelFamily, _require_enum
_types.py            Symbols, ALL/_All, DateLike, FilterValue, bars overload aliases, override
_errors.py           MassiveError (.status, .request_id), AuthenticationError, EntitlementError, from_wire
_catalog.py          DatasetSpec (role paths, kind, per-source key columns, channels, prefixes,
                     poll defaults, page size), datasets(), members(kind), filter validation
_namespaces/
  __init__.py        empty
  base.py            the shared dataset factory
  stocks.py options.py futures.py indices.py forex.py crypto.py
  reference.py indicators.py economy.py partners.py      namespace class + DATASETS
_schema.py           ColumnType, Schema, WireType -> ColumnType, rename(), dataset_schema() (section 19.4)
_timeunits.py        the one timestamp routine (section 12), the FMV magnitude rule
_symbols.py          normalization, canonical_view, the Symbol/Underlying rule (section 10)
_dates.py            DateLike parsing, day zones, sessions, publish_time, [start, end)
_liveness.py         result and stream scopes, records, reaper queue (section 6.1)
_diagnostics.py      connections() and streams() with SchemaDrift; DriftRegistry (section 17)
_rest.py             REST transport: requests from a DatasetSpec, batches renamed and converted,
                     403/429 policy, drift routing, host-decision cache (single-flight)
_hosts.py            host resolution from REST evidence (section 13.2)
_ws.py               connections over WsClient: reconcile, wildcards, watchdogs, the NO_ACK rule,
                     the verdict rule, the options cap, rejects and drift routing (section 13.3)
_stream.py           streams, publishers, flush thread, fan-out, bound-failure routing (section 14)
_watchlist.py        reactive symbol tables
_poll.py             poll(): REPLACE/UPSERT/APPEND, fan-out, data failures -> publish_failure
_flat_files.py       flat-file transport: read plans, reader pool, cancellation, conversion,
                     merge to tables, missing-file policy, drift routing (section 16)
_history_source.py   partition dates, publication grace, REST capability, the AUTO choice; pure, no I/O
_massive/            the wire package (section 19)
py.typed
```

`_namespaces/` holds plain modules, one namespace class each; it is not a PEP 420 namespace
package. Each namespace module also declares its datasets (`DATASETS: tuple[DatasetSpec, ...]`).
`_history_source.py` decides where `history()` reads from; `_flat_files.py` does flat-file I/O
through `_massive` and never chooses a source.

Supporting files outside the package:

| File | Purpose |
|---|---|
| `scripts/check.sh [fast\|full] [--no-fix]` | The single verification entry point (lint, format, type check; `full` adds the test suite with coverage). |
| `python -m deephaven_massive._massive.refresh [--check \| --fetch]` | The refresh command, run from a source checkout ([section 19.6](#196-refresh-and-drift)). |
| `.github/workflows/spec-drift.yml` | The advisory weekly drift job ([section 20](#20-quality-gates)). |
| `.github/workflows/release.yml` | Publishes to PyPI from a `v*` tag; created only when the owner turns publishing on ([section 20.1](#201-tooling-configuration)). |
| `tests/data/` | Payload fixtures. |
| `tests/_fakes/` | Fake REST server, WebSocket server, S3, and stream helpers ([section 4.2](#42-test-layout)). |

Deephaven is imported at module level where it is used at runtime. Type-only imports go under
`TYPE_CHECKING`. The library imports nothing from `deephaven_server`. Only the two lazy
`__init__.py` files (the package's and `_massive`'s) defer imports.

### 4.2 Test layout

Tests mirror the package. The test file for `deephaven_massive/<path>/<name>.py` is
`tests/<path>/test_<name>.py`, where **every path component drops one leading underscore**
(`_namespaces/stocks.py` → `tests/namespaces/test_stocks.py`, `_massive/rest/client.py` →
`tests/massive/rest/test_client.py`). A package's `__init__.py` is mirrored by
`tests/<path>/test_init.py`, and a `__main__.py` by `tests/<path>/test_main.py`, so every source
module has exactly one mirror. `tests/test_init.py` holds the public-surface tests (`dm.__all__`
equals [section 3](#3-exports) and `__version__` equals the installed distribution's version).
`tests/test_catalog.py` is the plain mirror of `_catalog.py`. A directory directly under
`tests/` whose name starts with `_` is a helper package outside the mirror rule; a test module
(`test_*.py`, `*_test.py`, `conftest.py`) inside one is still checked. Fixtures live in
`tests/data/<test owner path>/` (for example `tests/data/massive/rest/`), so no fixture
directory starts with `_`. Examples:

| Source | Unit test | Live integration test |
|---|---|---|
| `__init__.py` | `tests/test_init.py` | |
| `_config.py` | `tests/test_config.py` | |
| `_hosts.py` | `tests/test_hosts.py` | `tests/test_hosts_integration.py` |
| `_flat_files.py` | `tests/test_flat_files.py` | `tests/test_flat_files_integration.py` |
| `_namespaces/__init__.py` | `tests/namespaces/test_init.py` | |
| `_namespaces/stocks.py` | `tests/namespaces/test_stocks.py` | `tests/namespaces/test_stocks_integration.py` |
| `_massive/__init__.py` | `tests/massive/test_init.py` | |
| `_massive/rest/client.py` | `tests/massive/rest/test_client.py` | |
| `_massive/refresh/__main__.py` | `tests/massive/refresh/test_main.py` | |
| `_massive/refresh/generate.py` | `tests/massive/refresh/test_generate.py` (also the parity test) | |

**Import roots.** `[tool.pytest] pythonpath = ["."]` and `[tool.mypy] mypy_path = ["src"]`;
test code imports helpers as `tests._fakes…`. So `tests/massive/` can never shadow a top-level
`massive` package.

Integration tests end in `_integration`. They are skipped unless `MASSIVE_API_KEY` is set and
run only in the later live-testing work package. The exceptions to the mirror rule, and no
others:

- `tests/conftest.py`: starts the embedded JVM in `pytest_configure`, before collection, and
  provides fixtures: thin fixtures that build the fakes from `tests/_fakes/`, plus the
  table-to-Python and fixture-loading helpers. Test modules never import from it.
- `tests/test_layout.py`: walks `src/deephaven_massive/**/*.py` and asserts every module has its
  mirrored test file and that every test file maps to a module or is one of these exceptions.
  It also holds the import-boundary and layer tests of `_massive`
  ([section 19](#19-wire-clients-and-the-spec-snapshot)), with a negative test per rule:
  `tests/massive/` may import any `_massive` module, and the rest of `tests/` only the
  interface.
- `tests/test_readme.py`: runs every example of [section 21](#21-examples) in the embedded JVM
  against the fakes. It mirrors `README.md`, not a source module.
- `_` helper directories, of which there is one: `tests/_fakes/` (`__init__.py`, `rest.py`,
  `ws.py`, `s3.py`, `stream.py`), protocol-faithful fakes. Only `tests/conftest.py` imports it;
  test modules receive fakes through fixtures, and import a fake directly only when a fixture
  cannot provide it, saying why in a comment.

## 5. `MassiveConfig`

`MassiveConfig` is a frozen dataclass. `MassiveConfig()` reads the environment. Defaults are
defined in exactly one place: the field defaults of this class.

| Field | Default / environment variable | Meaning |
|---|---|---|
| `api_key: str` | `MASSIVE_API_KEY`, then `POLYGON_API_KEY` | Required. Absent raises `AuthenticationError` at construction. |
| `s3_access_key_id: str \| None` | `MASSIVE_S3_ACCESS_KEY_ID` | Enables flat-file history. |
| `s3_secret_access_key: str \| None` | `MASSIVE_S3_SECRET_ACCESS_KEY` | The two S3 credentials are both set or both absent; exactly one raises `ValueError`. |
| `ws_hosts: Mapping[AssetClass, str]` | `{}` | Pins the hostname of the TICK family for a class, for example `{AssetClass.STOCKS: "fullmarket-business.massive.com"}`. Hostname only; validated. The FMV family is never pinned ([section 13](#13-host-resolution-and-connection-protocol)). |
| `ack_timeout: float` | `5.0` seconds | Subscribe-acknowledgement watchdog, per topic. |
| `reconnect_backoff: float` | `30.0` seconds | The first wait before the WebSocket client tries again after any failure that is not fatal: a connection that opened and closed (cleanly or not, including a `max_connections` refusal), a TCP or TLS failure, a handshake or authentication timeout, or a retryable HTTP status during the upgrade (500, 502, 503, 504). Later waits double up to `max_reconnect_backoff` ([section 19.2](#192-the-websocket-client)). Must be at least 15 s, else `ValueError` naming Massive's guidance: a shorter wait can be refused for holding the allowance the dropped connection still holds, and on an individual plan that wastes the one slot there is. Tests never need a shorter value: they drive `WsClient` directly, with its backoff as constructor arguments and an injected clock. The default waits out the window Massive's knowledge base describes: the server takes "roughly 10 to 30 seconds" to release a dropped connection, and an earlier retry "can be refused for holding the allowance it just lost", which wastes the one slot an individual plan has. |
| `max_reconnect_backoff: float` | `120.0` seconds | Cap of the doubling backoff, so a connection comes back within this long of a long outage ending. Must not be below `reconnect_backoff`. Jitter only adds up to 20% and is clamped, so every wait lies in `[reconnect_backoff, max_reconnect_backoff]` ([section 19.2](#192-the-websocket-client)). |
| `flat_file_symbol_threshold: int` | `50` | The `HistorySource.AUTO` switch point when both sources are possible. |
| `flat_file_publish_cutoff: datetime.time` | `time(11, 30)`, America/New_York | A partition for day D counts as published from the first occurrence of this New York time after day D ends ([section 16](#16-historysourceauto)). The documentation says files land "by approximately 11:00 AM ET the following day". A naive `time` (a New York wall-clock time); one with `tzinfo` raises `ValueError`. |
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
- The shared retry policy (REST and S3), the REST client's timeouts and pool size, and the
  WebSocket client's handshake timeout and stable-uptime rule
  ([section 19](#19-wire-clients-and-the-spec-snapshot)).
- Proxies and CA certificates: all three clients read the standard environment variables
  (`HTTPS_PROXY`, `NO_PROXY`, `SSL_CERT_FILE`, `SSL_CERT_DIR`), as section 19 specifies, except
  that `SSL_CERT_DIR` does not apply to S3.

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
repr(m.stocks)  # "<MassiveSession #1 stocks>"; vendors "<MassiveSession #1 partners.benzinga>"
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
  before it registers the record; if anything raises, even `KeyboardInterrupt`, it marks the
  record released and removes it, and the verb releases the result scope and the stream hold
  itself (also when the interrupt lands between `make_result` and `track`). A result is never
  released twice and never leaked, within the best-effort boundary below (see **Opening a stream**).
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
`/v1/last/crypto`, or `/v3/snapshot/indices` projected to the values schema through its
membership of the values kind ([section 19.4](#194-generated-schemas)). Each "last" path is a
member of its dataset's kind, so its rows come out in the stream schema. On
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
serve as a row cap.) A reactive `Table` passed as `symbols` is read once, when the verb is
called, on the caller thread inside the session's execution context: its valid rows become the
sorted canonical symbol tuple that every source receives (`REST`, `FLAT_FILES`, and the `AUTO`
choice), rows that are not valid symbols of the class are ignored (logged once per process
per class, at WARNING), and a table with no valid symbol raises `ValueError`, as an empty
symbol list does.

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
  A range that ends before it starts raises `ValueError` at construction, as `history()` does
  (two dates may be equal: whole days; two datetimes are half-open; a date and a datetime
  compare by day).
- `limit` is a total-row cap. Page size is internal. Every docstring says so. The cap is
  stored apart from the wire parameters, in `Request.limit`, so it can never be sent as the
  endpoint's page-size `limit`; the page size is `DatasetSpec.page_size`.
- The REST column names the path template as the OpenAPI spec writes it (the spec's own
  placeholder names, such as `{stocksTicker}`, are abbreviated here to `{t}`, `{o}`, `{u}`).
  Every path is a generated REST source (`_massive.source(Transport.REST, template)`,
  [section 19.4](#194-generated-schemas)). Each dataset names its paths by role (`fetch_path`,
  `history_path`, `grouped_daily_path`, `extra_paths`).

### 8.1 `m.stocks`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols)` | StreamHistoryLatestQuery | `T` | fetch `/v2/last/trade/{t}`; history `/v3/trades/{t}` | `us_stocks_sip/trades_v1` |
| `quotes(symbols)` | StreamHistoryLatestQuery | `Q` | fetch `/v2/last/nbbo/{t}`; history `/v3/quotes/{t}` | `us_stocks_sip/quotes_v1` |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1, adjusted=True)` | overloads ([7.1](#71-bars-overloads)) | `A` (1 s), `AM` (1 min) | fetch `/v2/aggs/ticker/{t}/prev` (DAY only); history `/v2/aggs/ticker/{t}/range/{m}/{span}/{from}/{to}`; `ALL` with DAY: `/v2/aggs/grouped/locale/us/market/stocks/{date}` per day | `us_stocks_sip/minute_aggs_v1`, `us_stocks_sip/day_aggs_v1` (`adjusted=False` only) |
| `luld(symbols)` | StreamQuery | `LULD` | | |
| `imbalances(symbols)` | StreamQuery | `NOI` | | |
| `fmv(symbols)` | StreamQuery | `FMV` (FMV family, `business.massive.com/stocks`) | | |
| `snapshots(symbols, *, include_otc: bool = False)` | Query | | `/v2/snapshot/locale/us/markets/stocks/tickers` (`tickers=`; `ALL` is the full market) | |
| `movers(direction: Direction, *, include_otc: bool = False)` | Query | | `/v2/snapshot/locale/us/markets/stocks/{gainers\|losers}` | |
| `daily_open_close(symbols, date: DateLike, *, adjusted: bool = True)` | Query | | `/v1/open-close/{t}/{date}` per symbol | |
| `dividends(symbols=None, …T)`, axis `ex_dividend_date` | Query | | `/stocks/v1/dividends` | |
| `splits(symbols=None, …T)`, axis `execution_date` | Query | | `/stocks/v1/splits` | |
| `ipos(symbols=None, …T)`, axis `listing_date` | Query | | `/vX/reference/ipos` | |
| `short_interest(symbols=None, …T)`, axis `settlement_date` | Query | | `/stocks/v1/short-interest` | |
| `short_volume(symbols=None, …T)`, axis `date` | Query | | `/stocks/v1/short-volume` | |
| `share_float(symbols=None, …T)`, axis `date` | Query | | `/stocks/vX/float` | |
| `financials.balance_sheets`, `financials.income_statements`, `financials.cash_flow_statements`, `financials.ratios` — each `(symbols=None, *, timeframe: Timeframe \| None = None, …T)`, axis `period_end` | Query | | `/stocks/financials/v1/{balance-sheets,income-statements,cash-flow-statements,ratios}` | |
| `filings.index(symbols=None, *, form_type: str \| None = None, …T)`, axis `filing_date` | Query | | `/stocks/filings/vX/index` | |
| `filings.sections_10k`, `filings.text_8k`, `filings.disclosures_8k`, `filings.risk_factors` — each `(symbols=None, …T)`, axis `filing_date` | Query | | `/stocks/filings/10-K/vX/sections`, `/stocks/filings/8-K/vX/text`, `/stocks/filings/8-K/vX/disclosures`, `/stocks/filings/vX/risk-factors` | |
| `filings.institutional_holdings(*, filer_cik: str \| None = None, …T)`, axis `filing_date` (Form 13-F) | Query | | `/stocks/filings/vX/13-F` | |
| `filings.insider_holdings(symbols=None, *, issuer_cik: str \| None = None, owner_cik: str \| None = None, …T)` (Form 3); `filings.insider_transactions(...)` with the same signature (Form 4); axis `filing_date` | Query | | `/stocks/filings/vX/form-3`, `/stocks/filings/vX/form-4` | |
| `filings.risk_factor_taxonomy(…F)`, `filings.disclosure_taxonomy(…F)` | Query | | `/stocks/taxonomies/vX/risk-factors`, `/stocks/taxonomies/vX/disclosures` | |

### 8.2 `m.options`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols)` | StreamHistoryLatestQuery | `T` | fetch `/v2/last/trade/{o}`; history `/v3/trades/{o}` | `us_options_opra/trades_v1` |
| `quotes(symbols)` | StreamHistoryQuery | `Q` | history `/v3/quotes/{o}` | `us_options_opra/quotes_v1` |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1, adjusted=True)` | overloads | `A`, `AM` | `/prev` (DAY); range | `us_options_opra/{minute,day}_aggs_v1` |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/options`) | | |
| `chain(underlying: str, *, contract_type: ContractType \| None = None, …F)`; `filters` takes `strike_price_*`, `expiration_date_*` | Query | | `/v3/snapshot/options/{u}` | |
| `snapshots(symbols, *, …F)`; OCC contracts; `Underlying` parsed; `ALL` is every contract, paginated (slow, documented) | Query | | `/v3/snapshot?ticker.any_of=O:…`, 250 per request. Same fields as the per-contract endpoint, including greeks. `/v3/snapshot/options/{u}/{o}` is listed in the catalog as covered by this dataset. | |
| `contracts(*, symbols=None, underlying: str \| None = None, contract_type: ContractType \| None = None, as_of: DateLike \| None = None, expired: bool \| None = None, …F)`; `filters` takes `expiration_date_*`, `strike_price_*` | Query | | `/v3/reference/options/contracts`; with `symbols`: `/v3/reference/options/contracts/{o}` per contract | |
| `daily_open_close(symbols, date: DateLike)` | Query | | `/v1/open-close/{o}/{date}` per contract | |

### 8.3 `m.futures`

Streams always use the `/futures` cluster path. `venue` exists on the three factory methods
below and selects the flat-file prefix only: it is not part of the stream key
([section 14](#14-publishers-fan-out-and-failure-semantics)). `venue=None` reads all four prefixes.

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols, *, venue: FuturesVenue \| None = None)` | StreamHistoryQuery | `T` | history `/futures/v1/trades/{t}` (`session_end_date` filters) | `us_futures_{venue}/trades_v1` |
| `quotes(symbols, *, venue: FuturesVenue \| None = None)` | StreamHistoryQuery | `Q` | history `/futures/v1/quotes/{t}` | `us_futures_{venue}/quotes_v1` |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1, venue: FuturesVenue \| None = None)` | overloads | `A`, `AM` | history `/futures/v1/aggs/{t}` (`resolution`; `window_start` in ns) | `us_futures_{venue}/minute_aggs_v1`, `us_futures_{venue}/session_aggs_v1` |
| `snapshots(symbols, *, product_code: str \| None = None, …F)`; `ALL` with `product_code` is every contract of a product | Query | | `/futures/v1/snapshot` | |
| `contracts(*, product_code: str \| None = None, …F)`, `products(…F)`, `exchanges()`, `market_status(…F)` | Query | | `/futures/v1/{contracts,products,exchanges,market-status}` | |
| `schedules(…T)`, axis `session_end_date` | Query | | `/futures/v1/schedules` | |

No futures dataset has a "last" endpoint, so no futures stream query has `fetch()`.
`snapshots(symbols).fetch()` is the one-request answer. There is no futures `day_aggs` flat
file; `Timespan.DAY` is rejected ([section 7.1](#71-bars-overloads)).

### 8.4 `m.indices`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `values(symbols)` | StreamHistoryLatestQuery | `V` | fetch `/v3/snapshot/indices` projected to `Value`, with `Timestamp` from `last_updated`; history flat files only | `us_indices/values_v1` |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1)` (no `adjusted`: the endpoint has none) | overloads | `A`, `AM` | `/prev` (DAY); range | `us_indices/{minute,day}_aggs_v1` |
| `snapshots(symbols, *, …F)` | Query | | `/v3/snapshot/indices` | |
| `daily_open_close(symbols, date: DateLike)` | Query | | `/v1/open-close/{t}/{date}` | |

### 8.5 `m.forex`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `quotes(symbols)` | StreamHistoryLatestQuery | `C` | fetch `/v1/last_quote/currencies/{from}/{to}`; history `/v3/quotes/{C:…}` | `global_forex/quotes_v1` |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1, adjusted=True)` | overloads | `CAS` (1 s), `CA` (1 min) | `/prev`; range; `ALL` with DAY: `/v2/aggs/grouped/locale/global/market/fx/{date}` | `global_forex/{minute,day}_aggs_v1` |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/forex`) | | |
| `snapshots(symbols)`; `ALL` is all pairs | Query | | `/v2/snapshot/locale/global/markets/forex/tickers` | |
| `movers(direction: Direction)` | Query | | `/v2/snapshot/locale/global/markets/forex/{gainers\|losers}` | |
| `conversion(from_currency: str, to_currency: str, *, amount: float = 1.0, precision: int = 2)` | Query | | `/v1/conversion/{from}/{to}` | |

### 8.6 `m.crypto`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols)` | StreamHistoryLatestQuery | `XT` | fetch `/v1/last/crypto/{from}/{to}`; history `/v3/trades/{X:…}` | `global_crypto/trades_v1` |
| `quotes(symbols)` | StreamQuery | `XQ` | | |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1, adjusted=True)` | overloads | `XAS` (1 s), `XA` (1 min) | as forex; grouped `/v2/aggs/grouped/locale/global/market/crypto/{date}` | `global_crypto/{minute,day}_aggs_v1` |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/crypto`) | | |
| `snapshots(symbols)`, `movers(direction: Direction)` | Query | | `/v2/snapshot/locale/global/markets/crypto/{tickers,gainers,losers}` | |
| `daily_open_close(symbols, date: DateLike)` | Query | | `/v1/open-close/crypto/{from}/{to}/{date}` | |

### 8.7 `m.reference`

All return `Query`.

| Method | REST |
|---|---|
| `tickers(*, symbols=None, market: Market \| None = None, type: str \| None = None, exchange: str \| None = None, cusip: str \| None = None, cik: str \| None = None, date: DateLike \| None = None, active: bool = True, search: str \| None = None, …F)` | `/v3/reference/tickers`. `cusip` and `cik` are `str`, as the spec types them. |
| `ticker_details(symbols, *, date: DateLike \| None = None)` | `/v3/reference/tickers/{t}` per symbol |
| `ticker_types(*, asset_class: AssetClass \| None = None, locale: Locale \| None = None)` | `/v3/reference/tickers/types` |
| `ticker_events(symbols, *, types: Sequence[str] \| None = None)` | `/vX/reference/tickers/{t}/events` per symbol |
| `related_companies(symbols)` | `/v1/related-companies/{t}` per symbol; rows are `{"ticker": peer}`, so `Symbol` is the peer and the requested symbol is `Underlying` |
| `news(*, symbols=None, …T)`, axis `published_utc` | `/v2/reference/news` |
| `exchanges(*, asset_class: AssetClass \| None = None, locale: Locale \| None = None)` | `/v3/reference/exchanges` |
| `conditions(*, asset_class: AssetClass \| None = None, data_type: ConditionDataType \| None = None, id: int \| None = None, sip: Sip \| None = None, …F)` | `/v3/reference/conditions` |
| `market_status()` | `/v1/marketstatus/now`; one row; nested maps flattened |
| `market_holidays()` | `/v1/marketstatus/upcoming` |

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

REST: `/v1/indicators/{sma,ema,rsi,macd}/{t}` per symbol. `Timespan.SESSION` is rejected
here.

### 8.9 `m.economy`

All return `Query`; axis `date`; each takes `…T`.

| Method | REST |
|---|---|
| `treasury_yields` | `/fed/v1/treasury-yields` |
| `inflation` | `/fed/v1/inflation` |
| `inflation_expectations` | `/fed/v1/inflation-expectations` |
| `labor_market` | `/fed/v1/labor-market` |
| `funding_conditions` | `/fed/v1/funding-conditions` |

### 8.10 `m.partners`

All return `Query`.

| Vendor | Methods | REST |
|---|---|---|
| `benzinga` | `analysts`, `analyst_insights`, `ratings`, `bulls_bears_say`, `guidance`, `earnings`, `firms` — `(symbols=None, …F)` where the endpoint takes tickers, `(…F)` otherwise | `/benzinga/v1/{analysts,analyst-insights,ratings,bulls-bears-say,guidance,earnings,firms}` |
| `benzinga` | `consensus_ratings(symbols)` | `/benzinga/v1/consensus-ratings/{t}` per symbol (the ticker is a path segment) |
| `benzinga` | `news(*, symbols=None, …T)`, axis `published` | `/benzinga/v2/news` |
| `etf_global` | `analytics`, `constituents`, `fund_flows`, `profiles`, `taxonomies` — `(symbols=None, …F)` | `/etf-global/v1/{analytics,constituents,fund-flows,profiles,taxonomies}` |
| `tmx` | `corporate_events(symbols=None, …T)`, axis `date` | `/tmx/v1/corporate-events` |
| `fable` | `merchant_aggregates(…T)`, `merchant_hierarchy(…F)` | `/consumer-spending/eu/v1/{merchant-aggregates,merchant-hierarchy}` |

### 8.11 `m.snapshots`

`m.snapshots(symbols, *, …F) -> Query` calls the unified `/v3/snapshot` with `ticker.any_of`,
250 tickers per request, any mix of asset classes. Rows are the union schema of the unified
snapshot. `ALL` is not accepted here (the endpoint requires tickers). Its catalog record is
named `reference.snapshots` and is declared in `_namespaces/reference.py` with
`asset_class=None` (each ticker's prefix selects its class,
§ 10), so `_catalog.datasets()` finds it; `m.streams()` and messages show that name.

### 8.12 Excluded endpoints

Excluded items fall into two lists.

- **Executable exclusions** are entries `(kind, item, reason)` of `EXCLUSIONS` in
  `_massive/refresh/exclusions.py`, the single list for REST paths, WebSocket channels, and
  flat-file prefixes. Each names an item present in a vendor source at the pinned snapshot
  (the OpenAPI spec, `websocket.json`, `llms.txt`, or the flat-file pages). An entry that names
  an item the snapshot lacks fails the refresh as stale (G4,
  [section 19.4](#194-generated-schemas)).
- **Documentation-only exclusions** name endpoints that never existed or are absent from every
  vendor source. They are recorded here so nobody re-adds them, and never appear in
  `EXCLUSIONS`. A row of the first table that the snapshot shows absent from every vendor
  source moves to the second.

| Endpoint | Reason |
|---|---|
| `/v3/reference/dividends`, `/v3/reference/splits` | Deprecated; replaced by `/stocks/v1/*`. |
| `/benzinga/v1/news` | Sunset; replaced by `/benzinga/v2/news`. |
| `/vX/reference/financials` | Sunset; replaced by `/stocks/financials/v1/*`. |
| `/v1/summaries` | Not in the documentation index. |
| Crypto `/book` REST endpoint; WebSocket `XL2` | Deprecated and sunset. |
| WebSocket `LV`; Launchpad hosts and paths | Product retired. |
| WebSocket `/futures/{venue}` | Not documented: the futures WebSocket pages list `/futures` only. |
| `/v1/historic/*`, `/v2/ticks/*` | Deprecated 2022; replaced by `/v3/trades` and `/v3/quotes`. |
| Form 5, per-asset `/v1/exchanges`, `/options/v3/*`, `/v1/reference/ipos`, `/stocks/dev/*`, `vX_0` paths, and every path marked `x-polygon-draft` or `x-polygon-ignore` | Present only in the OpenAPI spec; not announced or documented in `llms.txt`. |
| Single-ticker v2 snapshots `/v2/snapshot/.../tickers/{t}` | Covered by `snapshots(symbols)` with the `tickers` filter. |
| `/v3/snapshot/options/{u}/{o}` | Covered by `m.options.snapshots`; identical fields. |

Documentation only:

| Endpoint | Reason |
|---|---|
| `/benzinga/v1/transcripts` | Never existed. |

**Coverage.** Everything documented is generated or excluded, and everything generated is
used or listed as still to be declared:

- **documented = generated ⊎ excluded ⊎ (deprecated ∪ draft ∪ ignored)**, tested against the
  pinned snapshot in `tests/massive/refresh/test_generate.py`;
- **generated ⊆ used ∪ undeclared**, tested in `tests/test_catalog.py`. Every generated item
  is used by a declared dataset or listed in some undeclared file. The file
  `tests/data/catalog/undeclared/<namespace>.txt` lists the sources that namespace will
  declare, so a shared source (the aggregate range path, `/prev`) appears in the file of every
  namespace that will use it, and the files may overlap. A namespace's file must not list a
  source that namespace already uses. Each namespace's dataset package deletes only its own
  file, so those packages land in any order; when all are gone, generated = used.

`/fed/v1/funding-conditions` and `/v1/open-close/crypto/{from}/{to}/{date}` are ordinary
generated paths: every path reaches the same REST client in the same way.

## 9. Filters

Parameters are hybrid: typed primaries plus one validated `filters` mapping.

Typed keyword-only parameters exist for:

- identity (`symbols`, `underlying`, `product_code`, `filer_cik`, and so on);
- the primary time axis (`start`, `end`) where the row has one;
- closed categoricals, as enums;
- shape parameters (`timeframe`, `contract_type`, `window`);
- `sort`, `order`, `limit`.

Every other query parameter the spec documents for the dataset's paths (the `.gt`, `.gte`, `.lt`,
`.lte`, and `.any_of` variants, of which the spec lists over a thousand, and plain filters such
as `pay_date`) goes through `filters: Mapping[str, FilterValue]`, where
`FilterValue = str | int | float | bool | date | datetime | Sequence[str]`. A key is the wire
name with its dot written as an underscore, so every key is a Python identifier:

```python
m.options.contracts(underlying="NVDA",
                    filters={"strike_price_gte": 100, "expiration_date_lte": "2026-01-16"})
# sends strike_price.gte=100&expiration_date.lte=2026-01-16
```

The mapping is mechanical and one to one: a key ending in `_gt`, `_gte`, `_lt`, `_lte`, or
`_any_of` is sent as `<field>.<suffix>` exactly when the spec lists that dotted parameter for the
path; every other key is sent unchanged. `Sequence[str]` values of an `.any_of` parameter are
joined with commas; dates are ISO-8601; an aware `datetime` becomes the parameter's documented
form (exact integer nanoseconds for a nanosecond timestamp parameter, ISO-8601 otherwise), and a
naive one raises `ValueError` (§ 11).

Validation is synchronous at query construction, against the generated parameters of the
dataset's paths (`RestPath.params` of `spec/rest.json`,
[section 19.4](#194-generated-schemas)), which carry each
parameter's kind and, for an enum parameter, its values. A dataset's accepted filters are the
union over its paths, minus the parameters the library sets itself (path segments, the symbol
parameter, the axis pair, `limit`, `sort`, `order`, and the typed keyword parameters). An unknown
key, a value of the wrong kind, or a value outside a parameter's enum raises `ValueError` naming
the valid keys or values. When Massive adds a filter, refreshing the snapshot adds it; no code
changes.

`start` and `end` map to the wire parameter pair recorded per dataset in the catalog
(`timestamp.gte`/`timestamp.lt`, `published_utc.gte`/`published_utc.lte`,
`ex_dividend_date.gte`/`ex_dividend_date.lte`, `filing_date.gte`/`filing_date.lte`,
`session_end_date.gte`/`session_end_date.lte`, and so on). Passing the same axis through
`filters` (as `timestamp_gte`, for example) raises `ValueError`.

## 10. Symbols and `ALL`

```python
Symbols = str | Sequence[str] | Table | _All      # dm.ALL is the only _All instance; repr "dm.ALL", str "ALL"
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
  `underlying_asset.ticker`) and on `related_companies` rows. A non-instrument fan-out key
  keeps the generated column the dataset declares as its `fanout` key (`filer_cik` becomes
  `FilerCik`). Where a source does not carry the instrument in its rows (it takes the symbol
  as a path parameter, as `/v3/trades/{t}` and `/v2/aggs/ticker/{t}/…` do, or holds it outside
  the result key, as `/v1/last/crypto` and `/v1/last_quote` do), `Symbol` is filled from the
  request ([section 19.4](#194-generated-schemas)). A dataset whose rows describe no instrument
  (`reference.exchanges`, `economy.*`) has no `Symbol` column.

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
`Literal` of strings, and that every enum that mirrors a parameter enum of `spec/rest.json`
has the same value set as that parameter.

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

Internal, not exported: `Recency {REALTIME, DELAYED, UNKNOWN}` and `ChannelFamily {TICK, FMV}`.
`Transport {REST, WS, FLAT_FILES}` is defined once, in `_massive.spec.types`, and used on both
sides of the boundary.

**Dates.** Parameters accept an ISO-8601 `str`, a `date`, or a timezone-aware `datetime`. A
naive `datetime`, or an ISO string with a time but no offset, raises a `ValueError` that names
the parameter, the value, and the fix, for example `start=datetime.datetime(2025, 1, 2, 9, 30)
needs a timezone: pass an aware datetime or a date`. The same rule and message apply to a naive
`datetime` passed as a `filters` value or a typed parameter.

**ISO-8601 text.** A timestamp the wire carries as ISO-8601 text (for example `published_utc`)
becomes an `Instant`; text without a UTC offset is read as UTC. The generator's
representation rules ([section 19.4](#194-generated-schemas)) decide which fields are ISO
`Instant`s and which are dates; any other text timestamp needs a `TYPE_FIXES` entry.

**Column types.** Every column's type comes from the type lattice of
[section 19.4](#194-generated-schemas): each generated field has a `WireType`, the merge joins
the `WireType`s of one column across a kind's sources, and `_schema` maps the result to a
Deephaven column type (`long`, `double`, `boolean`, `String`, `LocalDate`, `Instant`, `int[]`).
Sizes come out as `double` wherever any source says `number`; condition codes come out as
`int[]`; exchange and tape ids come out as `long`.

## 12. Timestamp units and day boundaries

The unit of every timestamp-like wire field is a fact about Massive's wire format. It is the
hand-verified `UNITS` table in `_massive/refresh/overrides.py`, and the generator writes it onto
each generated field, so callers read it as `source(...).field(column).unit`. A timestamp-like
field without a `UNITS` entry fails the refresh (gate G3, [section 19.4](#194-generated-schemas)).
The table is complete for every generated source, so no dataset package edits it.

The OpenAPI spec tags some integer fields with a unit type in `x-polygon-go-type`
(`INanoseconds`, `IMicroseconds`, `IMilliseconds`, `IDaysPolygonDateString`), but the tags are
not reliable: all 20 `IMicroseconds` tags sit on indicator `timestamp` fields whose descriptions
say "Unix Msec", and the indicator responses carry milliseconds. The tags are a cross-check: a
`UNITS` entry that differs from its tag is listed, with its reason, in the tag-conflict list
beside the table, and the indicator `timestamp` fields are its first entries.

| Transport and fields | Unit |
|---|---|
| WebSocket stocks `T`/`Q` (`t`, `pt`, `trft`); `A`/`AM` (`s`, `e`); `LULD`/`NOI`; options `T`/`Q`; futures `T`/`Q`; indices `V` (`t`) and `A`/`AM`; forex `C`/`CA`/`CAS`; crypto `XT`/`XQ`/`XA`/`XAS` (`t`; `XT`/`XQ` received time `r`) | ms (verified on each documentation page) |
| WebSocket `FMV` `t` | Documented as ns; the documentation sample is `1678220098130` (13 digits, ms). Rule, FMV only: `t >= 10**17` means ns, otherwise ms. Every ns value after 1973-03-03 is at least 10^17, and an ms value reaches 10^17 only in the year 3,170,000, so the rule cannot misclassify. Logged once; tested. |
| REST `/v3/trades`, `/v3/quotes`, `/v2/last/trade`, `/v2/last/nbbo`; snapshot `last_updated`, `fmv_last_updated`; futures `/trades` and `/quotes` (`timestamp`, `bid_timestamp`, `ask_timestamp`); futures aggregates `window_start` (documented as "nanosecond Unix timestamp") | ns |
| REST aggregates `t`; `/prev` `t`; indicators `timestamp`; futures snapshot `last_minute.last_updated` | ms |
| REST forex `/v1/last_quote` `timestamp`; crypto `/v1/last` `timestamp` | **UNVERIFIED** (`unverified-ms`). The spec tags both `last.timestamp` fields `IMilliseconds`, which agrees; registered as ms with a fixture test. |
| Flat files: all tick files; `*_aggs_v1.window_start`; `us_indices/values_v1.timestamp` (the documentation says ms, the sample is 19 digits) | ns |

**The timestamp routine.** One routine in `_timeunits.py` converts every epoch-integer
`Instant` on every transport, in this order:

1. **Epoch 0 means absent.** In every epoch `Instant` column except the dataset's declared time
   column, a `0` becomes null. Massive's flat files write `0` where a timestamp has no value
   (the documented stocks-trades sample has `trf_timestamp` `0` on every row without a TRF);
   WebSocket and REST are expected to omit the field, and the rule applies on every transport
   regardless. The time column keeps its value, so a `0` there fails
   step 3. Prices, sizes, and every other numeric column keep `0` (a one-sided quote has
   `ask_price` `0`).
2. **Checked multiply.** The integer is multiplied to nanoseconds from its unit (`s`, `ms`,
   `us`, or `ns`) with an overflow check.
3. **Bound.** The result must lie in [1990, 2100]; otherwise
   `MassiveError("timestamp unit mismatch for <dataset>.<field>")` names the dataset, the
   column, the unit, and the value.

The bound applies to epoch-integer `Instant`s only, never to ISO-8601 text, so a pre-1990 ISO
date stays valid. The overflow check and the bound together catch every unit confusion. For
FMV `t`, the magnitude rule above picks the unit per value first, then the routine runs.

**Examples of generated time column names:** `Timestamp`, `ParticipantTimestamp`,
`TrfTimestamp`, `BidTimestamp`, `AskTimestamp` (futures quotes), `ReceivedTimestamp` (crypto
trades and quotes), `EndTimestamp` (bars), `LastUpdated` (snapshots). Calendar dates such as
the futures `SessionEndDate` are `LocalDate`.

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
  implementation checks every block of every futures file against its key, and a
  `session_end_date` that differs from the key's date, or is empty, fails loudly). With
  `datetime` bounds, futures filter on `Timestamp`, and flat files read the sessions the
  instants belong to. The futures trading day rolls at 17:00 America/Chicago (the weekly open is
  Sunday 17:00 CT; 16:00 to 17:00 CT is the daily maintenance break), so the session of an
  instant `t` is `(t in America/Chicago + 7 h).date()`, and the files read are the sessions
  from the session of `start` to the session of `end - 1 ns`. An intraday window reads one
  file; an evening window reads only the next session's file.

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
4. Connects are single-flight. Every retry, whether after a connection that opened has closed
   (cleanly or not, including a `max_connections` refusal) or after a failed attempt (TCP or TLS
   failure, handshake or authentication timeout, HTTP 500/502/503/504 during the upgrade), waits
   at least `reconnect_backoff` (30 s by default, the upper end of the 10–30 s the knowledge base
   gives the server to release a dropped connection); the wait doubles up to
   `max_reconnect_backoff`, with upward-only jitter clamped to that range. A handshake error that
   retrying cannot fix (for example HTTP 400 or 404) and a rejected key are fatal and fail the
   connection's streams ([section 19.2](#192-the-websocket-client)).
5. Futures streams always use `/futures`. `/futures/{venue}` is undocumented and unsupported.

### 13.2 Host decision

Resolution runs synchronously on the calling thread at the first stream verb for a
(class, family) and is cached in the session. The seam is
`RestTransport.resolve_host(asset_class, family) -> HostDecision`, where
`HostDecision(host, recency, evidence)` is a frozen record. The WebSocket transport calls it.
The REST transport is the one place the decision is cached, per session, for the session's
life, and resolution is **single-flight**: the cache holds one future per (class, family),
created under the transport's own leaf lock (never `registry_lock`); the first caller makes
the REST call and completes the future, and later callers wait on it outside every lock. A
failed resolution completes the future with the error and removes it, so nothing is cached.
Each new decision writes the pair's `Transport=REST` row of `m.connections()`.

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
  3. One REST call through the library's REST client (section 19.1), the raw JSON rows:
       STOCKS  : GET /v3/snapshot?ticker.any_of=SPY,AAPL
       OPTIONS : GET /v3/snapshot/options/SPY?limit=1
       INDICES : GET /v3/snapshot/indices?ticker.any_of=I:SPX
       FUTURES : GET /futures/v1/snapshot?product_code=ES&limit=1
     Per-ticker rows carrying "error"/"message" (NOT_ENTITLED) count as absent.
  4. Classify:
     a. HTTP 403 / NOT_AUTHORIZED
          -> raise EntitlementError("<class> plan has no WebSocket access")   # Basic plans
     b. Read the class's ordered timeframe fields, excluding underlying_asset:
          STOCKS, OPTIONS: last_trade.timeframe, last_quote.timeframe
          INDICES:         timeframe (top level; documented DELAYED | REAL-TIME)
          FUTURES:         last_trade.timeframe, last_quote.timeframe, last_minute.timeframe
                           (in the OpenAPI spec; absent from the documentation page)
        If any value is DELAYED  -> delayed.massive.com, DELAYED, evidence="<field>=DELAYED"
                                    (DELAYED wins conflicts; the field used is logged)
        elif any is REAL-TIME    -> socket.massive.com,  REALTIME, evidence="<field>=REAL-TIME"
        Any other value          -> raise MassiveError naming the field, the value, and
                                    MassiveConfig.ws_hosts; nothing is cached
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
documentation lists no `timeframe` (only the OpenAPI spec has it), so absence proves nothing, and a
real-time customer must never be routed to delayed data by inference. For the same reason, a
`timeframe` value other than the documented `DELAYED` and `REAL-TIME` fails loudly at the first
stream verb for that class instead of being guessed at.

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

Each connection is one `_massive.WsClient` ([section 19.2](#192-the-websocket-client)) bound to
one `wss://host/path`. The client runs on its own daemon thread with its own asyncio loop, and
owns everything about the wire: the socket, authentication, frame encoding and decoding,
reconnecting with backoff, and closing. `_ws.py` owns everything the library decides: the
desired topics, the acknowledgements, the watchdogs, the routing, and the connection's
diagnostics. Both run on the client's loop thread; other threads reach a connection only through
`loop.call_soon_threadsafe` with plain data, and through `WsClient.close()`.

The client reports to `_ws.py` through four callbacks, each called on the loop thread, in order:

- `on_open(generation)`: Massive accepted the key (`auth_success`) on a new socket. A
  **generation** is one authenticated socket; the number increases by one per `on_open`.
- `on_status(generation, status)`: one decoded status object (`Status`: `kind` `ACK`,
  `REFUSED`, `MAX_CONNECTIONS`, or `OTHER`; `topics`; `message`; `realtime_verdict`).
- `on_data(generation, channel, rows, rejects, drift)`: one channel's data from one frame,
  decoded against `ws.json`. `rows` are keyed by **long-form** column (the library renames
  them), with symbols as the wire spells them (`EUR/USD`, `BTC-USD`). `rejects` are the rows
  that failed validation, one `Reject(channel, symbol or None, WireError)` each, whose
  `symbol` is the raw wire value of the channel's declared symbol column; `drift` is the frame's
  `DriftReport` ([section 19.5](#195-runtime-drift)).
- `on_state(state, detail)`: `CONNECTING`, `RECONNECTING` (with the next attempt's time),
  `WAITING_SLOT` (the auth reply was `max_connections`), `FAILED` (with the `WireError`), or
  `CLOSED` (after `close()`).

`_ws.py` parses no frame text. Before it first subscribes a channel on a connection, it calls
the client's loop-thread method `set_required(channel, keys, *, symbol=None)`: `keys` is a
`frozenset` of long-form key columns, built by `_ws.py` from the `KeySpec`s that the channel's
dataset declares for that channel's source, excluding `from_request` keys, and `symbol` is the
long-form column of the channel's `symbol` key (`None` when it declares none). The client keeps
both across generations and hands them to the decoder, so a row missing one of those keys is a
`Reject` carrying the row's raw symbol, while any other absent field becomes null. A channel
with no entry has no required keys and no symbol. Wire symbols are never rewritten in
`_massive`: `_ws.py` maps the raw values of rows and rejects to canonical symbols with
`_symbols` when it routes them, and topics use the wire's own form, as the subscribe frames
do. It
then calls the loop-thread methods `subscribe(generation, topics)` and
`unsubscribe(generation, topics)`. Each sends at once, in
call order, at most 1,000 topics per frame (the frame-size limit is **UNVERIFIED**; the chunk
size is conservative). A call for a generation that is no longer current is discarded: the next
generation re-sends the whole desired set. `reconnect()` closes the current socket so the client
reconnects under its backoff rules. Rows from `on_data` go to the per-channel row buffer;
rejects fail their streams ([section 19.5](#195-runtime-drift)) and never enter a buffer; drift
goes to the session's `DriftRegistry`.

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
- Covered explicit topics count toward the options cap ([section 15](#15-options-contract-cap))
  only while they are on the wire; when a wildcard is removed, they re-enter through the cap's
  deferral in sorted order.

Responsibilities:

| Step | Owner |
|---|---|
| Open the socket (TLS, proxy, keepalive pings), read `connected`, send auth, read the auth reply, all within the handshake timeout | client |
| `auth_failed` | The client reports `FAILED` with `WireAuthError` and stops; it never retries a rejected key. The library fails every stream on the path with `AuthenticationError` (`_errors.from_wire`). |
| `max_connections` in the auth reply | The client reports `WAITING_SLOT`, lets the server close the socket, and retries after its backoff; streams stay alive. |
| `max_connections` as a status after authentication | The client reports `Status(MAX_CONNECTIONS)`; the library records it, and when that generation's socket closes, the connection shows `WAITING_SLOT` instead of `RECONNECTING`. |
| Reconnect after every close or failure that `close()` did not request and that is not fatal, after the backoff of [section 19.2](#192-the-websocket-client) | client |
| Classify a failed upgrade as fatal or retryable (section 19.2) | client; a fatal one is reported as `FAILED` with `WireError(status=<http>)`, which the library turns into a `MassiveError` naming the HTTP status and the host |
| Decode frames against `ws.json`; report rows, rejects, and drift | client |
| Map raw wire symbols to canonical symbols (`_symbols`) when routing rows and rejects | library |
| Desired topic set, reference counts, single in-flight step, wildcard transitions | library |
| Sending a step's subscribe and unsubscribe frames, and the one single-topic re-send | library decides; the client sends at once |
| Per-topic acknowledgement bookkeeping, watchdogs, the explicit-verdict rule | library, from `Status` events |
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
   -> WARNING once; treated as an acknowledgement of the whole in-flight step
Status(REFUSED, topic)
   -> that topic's streams fail with EntitlementError (publish_failure); the topic leaves D
Status(REFUSED, no topic)
   -> applies to every topic of the in-flight step (exact attribution, because one step is in
      flight). See the explicit-verdict rule below.
Status(MAX_CONNECTIONS) after authentication
   -> recorded for the generation; State=WAITING_SLOT once its socket closes
no acknowledgement within ack_timeout, the topic still in S and W, same generation
   -> client.subscribe(generation, [topic]): that topic alone, once
still none after another ack_timeout, same check
   -> topic state NO_ACK; ERROR logged; the first NO_ACK of this topic in the session calls
      client.reconnect(), so the backoff rules apply; a later NO_ACK of the same topic only
      logs at ERROR and counts in NoAckTopics. The topic stays desired; streams never fail
      on it
watchdog fires but its topic left S or W, or its generation is no longer current
   -> cancelled without escalating
socket closed or failed (not by close())
   -> the generation ends; its watchdogs are cancelled; State=RECONNECTING with
      NextReconnectAt, or WAITING_SLOT after a max_connections status; on the next on_open,
      W is re-sent
fatal handshake error, or auth_failed
   -> every stream on the connection fails through publish_failure (MassiveError naming the
      HTTP status and the host, or AuthenticationError); State=FAILED
shutdown callback (section 6.1)
   -> ref-count decrement; topics at zero leave D, and each cancels its watchdog
a connection whose D stays empty for 30 s
   -> closed by the connection loop (idle grace); the row is kept in m.connections() with
      State=CLOSED
```

A socket that the server stops answering is detected by websockets' keepalive pings and closed,
which starts a reconnect. A server that goes silent after authentication leaves the topics of the
in-flight step unacknowledged, so they reach `NO_ACK` within 2 × `ack_timeout` of their send. A
server that goes silent before `auth_success` is cut off by the handshake timeout.

**Closing.** `m.close()` and the idle close call `WsClient.close()`, which is safe from any
thread and does not wait: it cancels the client's task on its loop, which interrupts a backoff
wait, a handshake, or a receive at once, and the socket, if open, is closed with code 1000. The
client thread is a daemon; `m.close()` joins it with a bounded timeout and logs a WARNING if it
has not ended.

**Explicit-verdict rule.** When a `Status(REFUSED)` with no topic arrives on a `socket.` TICK
connection with `realtime_verdict` set (its message contains "real-time", case-insensitive; the exact text is
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
`error` for the stream's topic, or REST 403 for `poll()`), a fatal WebSocket handshake error
(`MassiveError` with the HTTP status and host), and the data failures of
[section 19.5](#195-runtime-drift) (a missing required key, an unconvertible value, a
timestamp out of bounds). `max_connections`, closes, and `NO_ACK` are diagnostic states.

### 13.4 REST

One host, one REST client per session ([section 19.1](#191-the-rest-client)), shared by every
caller thread, poller, and fan-out worker. Every REST dataset reaches its paths through it. The
REST transport turns each `WireError` into a public exception with `_errors.from_wire`, which
keeps the message and fills `status` and `request_id` ([section 3](#3-exports)). What a user
sees:

- **HTTP 401** raises `AuthenticationError`. **HTTP 403**, and a 200 response whose envelope
  `status` is `NOT_AUTHORIZED`, raise `EntitlementError` from `fetch()` and `history()` and fail
  the table from `poll()`.
- **HTTP 429** is retried as section 19.1 specifies (the `Retry-After` value, or 15, 30, then
  60 s; a `Retry-After` above 60 s fails at once); if it persists the client raises
  `WireError(status=429)`, which becomes `MassiveError("rate limited ...")` with `status=429`. The REST transport (`_rest.py`, not the
  client) then logs once per session at WARNING that Basic keys allow 5 requests per minute and
  that fan-out `poll()` multiplies requests.
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
- **Fan-out.** In a per-symbol `fetch()` or `poll()` cycle (`ticker_details`,
  `daily_open_close` on a holiday, `/prev`, the "last" endpoints), an HTTP 404 for one symbol
  drops that symbol, with one WARNING per symbol; every other error fails the call.
- **`poll()` cycles.** A cycle that raises `EntitlementError` or `AuthenticationError`, or hits
  a data failure of [section 19.5](#195-runtime-drift) (a missing required key, an
  unconvertible value, a timestamp out of bounds), fails the table through `publish_failure`.
  Any other error is logged at ERROR, sets the stream's `Error` in `m.streams()`, keeps the
  previous rows, and the next cycle runs as scheduled.
- **Drift.** `to_batch` returns each page's `DriftReport`; the transport routes it to the
  session's `DriftRegistry` (one WARNING per (dataset, transport, field), and `SchemaDrift` in
  `m.streams()` for `poll()`). Static verbs report drift only in the log.
- **`adjusted`.** The bars `adjusted` flag travels in its own typed field, `Request.adjusted`,
  which the REST transport sends, when not None, as the `adjusted` query parameter (`true` or
  `false`); `params` never holds `adjusted` (`build_query` rejects it there), so a request for
  unadjusted bars cannot silently fall back to Massive's adjusted default.
- Every error message names the HTTP status, the path, and the response's `request_id`, and is
  logged at ERROR with the same text; the API key never appears in a message or a log.

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
  row buffer once. The rows arrive keyed by long-form column; the flush renames them
  (`_schema.rename`), converts them (`_timeunits`), and builds one Arrow batch in the dataset
  schema. The timestamp bound is applied as a vectorized mask: rows that fail it fail only the
  streams that would have received them ([section 19.5](#195-runtime-drift)) and are dropped
  from the batch. Rejected rows never enter a buffer. The batch is converted once with
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
An explicit topic covered by an acknowledged wildcard counts only while it is on the wire
([section 13.3](#133-connection-and-subscription-protocol)); when the wildcard is removed, the
covered topics re-enter through the deferral below, in sorted order.

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
every day zone. Futures files are named by session date, and their clock starts at the next
midnight in America/Chicago, where Massive standardizes futures data. `indices.values` is
flat-file only.
`indices.bars` has no `adjusted`, so the predicate is satisfied.

The choice covers the whole range: one unpublished partition sends all of it to REST, and
nothing is split between the sources. A dataset without instruments counts as `ALL`.
`HistoryQuery.history()` makes the choice (`_history_source.choose_source`) and then calls one
transport; the transports only do I/O. The REST predicate depends only on the dataset's REST
capabilities (`DatasetSpec.has_rest_history`, `has_grouped_daily`), and the `adjusted` check on
the typed `Request.adjusted`.

An `AUTO` call logs its choice and the reason once, for example
`stocks.trades history: AUTO chose REST (2 symbols < flat_file_symbol_threshold 50)`. Each
transport then logs its own work: the flat-file transport `source=S3 files=<n> skipped=<dates>`
(`skipped=none` when nothing was missing), the REST transport `source=REST requests=<n>`.
Missing keys on non-trading dates are skipped. A missing weekday key logs a WARNING. The
source choice has already required every partition to be past its `publish_time`, so a missing
key for the last requested date is either late or a market holiday (there is no holiday
calendar). Within one day after its `publish_time` it raises
`MassiveError("<dataset>: <key> does not exist: <date> is not yet published (flat files land
~11:00 ET next day), or was a market holiday")`. After that window it is treated as a holiday:
a WARNING, and the date is skipped like any other missing weekday, so the rest of the range is
returned. Futures `venue=None` reads four prefixes per date; a 403 on any prefix raises
`EntitlementError` naming the venue and the `venue=` parameter. A 403 whose S3 error code says
the credentials themselves are wrong (`InvalidAccessKeyId`, `SignatureDoesNotMatch`) raises
`AuthenticationError` instead, for every asset class, naming `s3_access_key_id` and
`s3_secret_access_key`.

Flat files are read through `_massive.FlatFileClient` and `FlatFile`
([section 19.3](#193-the-flat-file-client)); `_flat_files.py` builds each file's `ReadPlan`.
`pyarrow.csv.open_csv` reads each `.csv.gz` key in 1 MiB blocks (gzip only: every documented
key is `.csv.gz`). Columns are matched **by name, never by position**: the reader first reads
the header line, parses it with the `csv` module (quotes and `\r\n` included), and passes those
names as `ReadOptions(column_names=…)`, then sets
`include_columns` to the documented headers with `include_missing_columns=True`, so a reordered
file reads identically, an extra header is reported as drift and ignored, a missing header
becomes a null column of its declared type, and a missing declared key header raises at once
([section 19.5](#195-runtime-drift)). The CSV reader parses each header straight to its
column's type (epoch integers for `Instant`s, text for lists), and only an empty field is null:
`NA` and `NULL` are text, since both are valid tickers. Each block then goes through pure Arrow
operations in this order: the futures session check on the whole block, the `Symbol` filter
(`pyarrow.compute.is_in`), the half-open `[start, end)` filter on the time column through the
plan's converter to nanoseconds (not applied to date-bounded futures, whose files hold exactly
the requested sessions), and, on the surviving rows only, the rename to long-form names and
parsing of the list columns (`Conditions`, `Indicators`). `_flat_files.py` then applies
`_schema.rename` and the `_timeunits` routine. No block is converted to Python objects. A
file's surviving rows become one static Deephaven table per 16 blocks of data and at the end of
the file (usually one table per file, since each table carries a fixed JVM-heap cost), and the
tables are merged.

Memory is bounded per concurrent reader, not per range: Arrow reads about 32 blocks ahead of the
consumer, so a reader holds about 32 MiB of decompressed text plus at most 16 blocks of
surviving rows, and at most `max_concurrency` readers run at once.

Rows come in partition-date order, then (futures) venue order (CME, CBOT, NYMEX, COMEX), then
file order. They are not sorted by time across venues, and may differ in order from REST.

Keys are read by a per-call pool of `min(max_concurrency, keys)` threads. A failed read, an
interrupt of the calling thread, or `m.close()` stops every other read at its next block, and
reads not yet started are cancelled; the failure of the earliest file is raised. After a close,
no request starts once a reader has seen the close; a request already in flight is discarded
(its connection pool is closed once the call's reads have drained), and the call raises
`MassiveError("MassiveSession #1 is closed")`, whatever that request returned.

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
| `LastError` | text; for a REST failure it names the HTTP status, the path, and the `request_id` |
| `SchemaDrift` | text; WebSocket connections only: the drift seen on the connection, as `channel:field+` (extra) and `channel:field-` (missing), sorted, truncated at 200 characters |

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
| `SchemaDrift` | text: the stream's drift, `poll()` rows included, as `transport:field+` (extra) and `transport:field-` (missing), sorted, truncated at 200 characters |

`RELEASED` means "no library-held results; still publishing until the blink table dies"
([section 6.1](#61-how-a-live-table-is-kept-alive-and-released)). `FAILED` can follow `RUNNING`
or `RELEASED`; a failed stream's row keeps `FAILED` when its last result is released.

`SchemaDrift` is separate from `Error` and `LastError`, so drift never overwrites a failure. The
session's `DriftRegistry` collects it ([section 19.5](#195-runtime-drift)). Static verbs
(`fetch()`, `history()`) report drift only in the log.

Every failure is logged at ERROR with the text the exception carries: hosts, the HTTP status and
`request_id` of a REST failure, the server message verbatim, the decision evidence, and the
nearest entitled alternative. For example: "Quotes are
not available on 15-minute delayed stock plans; `m.stocks.snapshots(symbols).poll(15)` provides
delayed NBBO via REST".

## 18. Concurrency model

Free-threaded CPython (`PYTHON_GIL=0`) is a gating target. The library's locking must be correct
without the GIL.

### 18.1 Threads

| Thread | Created by | Does | Must not |
|---|---|---|---|
| caller (console, script, user thread) | user | verbs; the host-resolution REST call; Deephaven table creation inside the result scope; registry mutation; waits, outside every lock, for another thread's open of the same stream key | hold the registry lock while calling Deephaven or waiting |
| WS client, one per connection | library | `asyncio.run` of the connection's `WsClient` (section 19.2): socket, handshake, frames, backoff; decode each frame once against `ws.json`; route status events, rejects, and drift; append rows to per-channel `SimpleQueue`s; the reconcile loop and watchdogs; idle close | any JVM call; blocking waits |
| flush, one per connection | library | every `flush_interval`: drain queues, Arrow to Java once per channel, `where_in` and `publisher.add` per stream; enqueue diagnostics row snapshots | touch loop-owned state directly |
| poller, one per `poll()` table, plus a `ThreadPoolExecutor(max_concurrency)` for fan-out | library | REST calls; `Table.snapshot()` of a reactive symbol table; `publisher.add` or `input_table` operations | run inside a listener |
| flat-file reader, a `ThreadPoolExecutor(min(max_concurrency, keys))` per `history()` call, ended before the call returns | library | S3 requests through the session's `FlatFileClient`; Arrow CSV parsing (Arrow's own I/O threads read ahead from the response stream); `deephaven.arrow.to_table` of static tables, inside the session's execution context; checks the call's cancel event and the client's closed flag before each request and after each block | take `registry_lock`; outlive the call |
| reaper, one per session | library | `scope.release()`; stream held-result counts | anything else |
| diagnostics writer, one per session | library | applies row snapshots from its queue to the `connections()` and `streams()` input tables: each batch coalesced to the latest row per key, one `add` per table | anything else |
| UG thread (Deephaven) | Deephaven | watchlist listeners (`listen`): compute symbol adds and removes and hand them to a worker | Deephaven table operations; blocking I/O; waiting on library threads |
| Java-originated (shutdown callback, UI export release) | Deephaven | `SessionCore.stream_shut_down` (under the registry lock: unmaps the key; deletes the record, or marks it for the last hold to delete); `call_soon_threadsafe` | sends; JVM calls; blocking |

### 18.2 Shared state and ownership

- `SessionCore.registry_lock` (a `threading.RLock`) guards every mutable registry: streams
  by key, results by id, connections by URL, topic reference counts. It is a
  **leaf lock**: no Deephaven call, no network call, no blocking wait, and no wait on a
  `call_soon_threadsafe` result while holding it. Per-stream state is a frozen dataclass replaced
  atomically under the lock; readers copy references out and release the lock before working.
- The other locks are leaf locks too, each guarding one thing and making no Deephaven,
  network, or blocking call while held:
  - the REST transport's host-decision cache, which holds one future per (class, family);
    callers wait on a future outside every lock (§ 13.2);
  - `FlatFileClient`'s lock, one per session, which guards the creation and the close of the
    boto3 S3 client (creation reads local AWS configuration but makes no request); its closed
    flag is a `threading.Event`, read without the lock by the reader threads;
  - the `_massive.spec` loaders, which load each generated file once;
  - `RestClient`'s not-before instant (section 19.1);
  - the session's `DriftRegistry`, whose WARNINGs are logged after the lock is released;
  - the `dataset_schema` cache.
- Per-connection mutable state (desired, acknowledged, and pending sets; the in-flight step;
  watchdogs; the `WsClient` and its socket) is **loop-confined**: only the connection's client
  thread touches it. Other threads reach it through `loop.call_soon_threadsafe` with plain data
  (tuples of topic strings), and through `WsClient.close()`, which any thread may call: it only
  sets an `Event` and schedules a cancel on the loop (section 19.2). Nothing reads loop state
  across threads; the loop thread enqueues diagnostics row snapshots on the diagnostics writer's
  queue, and the writer is the only thread that writes the diagnostics tables.
- The session's `RestClient` is shared by every thread that makes REST calls. It is safe per
  call: its shared mutable state is urllib3's internally locked pool and the not-before
  instant (section 19.1). The once-per-session rate-limit WARNING belongs to the REST transport,
  behind a `threading.Event`.
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
from 16 threads against a fake server, concurrent `release()` and verb calls, and 16 threads
paging through one `RestClient` against the fake REST server (including a 429 with
`Retry-After` and a reset in the middle of paging). The only free-threaded target is 3.14t,
because pyarrow, numpy, and pandas no longer publish cp313t wheels. Wheel availability on 3.14t:
`jpy 2.2.0`, `pyarrow`, `numpy`, and `pandas` ship cp314t wheels; `deephaven-core` is
`py3-none-any`; `websockets 17.3` ships cp314t wheels; `urllib3`, `certifi`, `boto3`, and
`botocore` are pure Python. `orjson 3.13.0` publishes no free-threaded wheels, so `orjson` is an
optional extra and
the standard-library codec is the default. `PythonDeephavenSession.setVariable` carries the
comment "relying on the GIL being kind to us"; the library never mutates script globals, so this
does not affect it, but it is why the Deephaven integration tests must run on the free-threaded
builds rather than unit tests alone.

## 19. Wire clients and the spec snapshot

The library talks to Massive through three clients of its own (REST, WebSocket, and S3 flat
files) and generates every wire schema from vendored, pinned vendor specs. It does not depend
on Massive's Python SDK, and nothing here works around a third-party client: each rule below
implements Massive's documented protocol or a requirement of this design.

**One package.** `deephaven_massive._massive` owns all Massive wire access: REST, WebSocket,
and S3 flat files (the bucket client, the key layout, CSV decoding, the S3 error mapping), the
vendored spec snapshot, the generated wire schemas, the units, and the long-form names. What
stays outside is the library's: Deephaven tables, sessions and liveness, dataset semantics
(`Symbol`, `Timestamp`, `Underlying`, the merged dataset schemas, conversion to nanoseconds,
the timestamp bound), the source choice, the missing-file policy, reader pools, cancellation,
public error text, and drift reporting. The clients take plain arguments, never
`MassiveConfig`, so they are tested alone against the fakes of `tests/_fakes/`.

```
src/deephaven_massive/_massive/
__init__.py          lazy interface: __all__ and re-exports only
net/                 shared by every client; imports nothing else in _massive
  __init__.py        USER_AGENT; re-exports
  errors.py          WireError, WireAuthError, WireEntitlementError
  http.py            CA bundle (SSL_CERT_FILE/SSL_CERT_DIR, else certifi); proxy from HTTPS_PROXY/NO_PROXY
  retry.py           the retry schedule (0.5/1/2 s, +20 % jitter; Retry-After; 15/30/60 s for 429),
                     retryable classification; TLS never retried
  codec.py           JSON codec: orjson when installed (logged once), else stdlib
spec/                run-time wire vocabulary; imports nothing else in _massive
  __init__.py        source(transport, key), sources(transport): load once under a lock
  types.py           Transport, Source(key, transport, fields, memberships),
                     Membership(kind, group, position, projection), RestPath(Source + template,
                     result key, params), Field(wire key, column, WireType, unit, required),
                     Param, ParamKind, WireType, WireUnit, DriftReport(extra, missing)
  rest.json  ws.json  flatfiles.json        generated, committed
rest/                imports net, spec
  __init__.py        RestClient, Page, to_batch
  client.py          pool, bearer auth, retries, not-before instant (leaf lock), next_url paging, close
  query.py           path and query encoding by ParamKind
  response.py        strict decoding, envelopes, result keys, error mapping
  batch.py           to_batch(rows, rest_path, *, required) -> (RecordBatch, DriftReport)
ws/                  imports net, spec
  __init__.py        WsClient, WsState, Status, StatusKind, Reject
  client.py          loop thread, connect/reconnect task, sender, close/join, per-channel
                     required keys (set_required)
  protocol.py        handshake and auth frames, subscribe frames, frame decoding against ws.json,
                     rows + rejects + DriftReport, failure classification
  backoff.py         backoff policy, stable-uptime reset
flatfiles/           imports net, spec
  __init__.py        FlatFileClient, FlatFile, ReadPlan, ReadColumn, object_key
  client.py          FlatFileClient: lazy boto3 client under a leaf lock, closed flag,
                     open(key) -> FlatFile | None, S3 error mapping, prefixes(), close()
  layout.py          object_key(prefix, day, venue): <prefix>/<YYYY>/<MM>/<date>.csv.gz; {venue} template
  reader.py          FlatFile.batches(plan) -> (RecordBatch, DriftReport) per block; header peek
refresh/             build time only; excluded from the wheel; may import any of the above
  __init__.py        empty
  __main__.py        [--check | --fetch]; the plain run regenerates spec/*.json
  lexicon.py         SHAPES (object shape -> lexicon), lexicons, identity entries
  overrides.py       NAMES, KINDS, PROJECTIONS, UNITS and tag conflicts, TYPE_FIXES,
                     TRANSPORT_ONLY, RESULT_KEYS, VENUES
  exclusions.py      EXCLUSIONS (kind, item, reason), the executable rows of section 8.12
  docs.py            flat-file "Columns" tables, overview prefix tables, llms.txt
  generate.py        snapshot -> spec/*.json: flatten, representation rules, type lattice, names, gates
  fetch.py           download and store canonically, manifest.json
  drift.py           live-against-snapshot comparison, unaccounted items, deprecations
  snapshot/          openapi.json, websocket.json, llms.txt, flat-file pages, manifest.json
```

The package is spelled `flatfiles` because PEP 8 forbids the underscore in a package name; the
library module is `_flat_files.py`. The docstrings say so.

**The interface.** `_massive/__init__.py` defines `__all__` and re-exports exactly these 26
names, resolving them lazily (PEP 562), so a REST-only session never imports boto3 or
websockets:

- errors: `WireError`, `WireAuthError`, `WireEntitlementError`;
- REST: `RestClient`, `Page`, `to_batch`;
- WebSocket: `WsClient`, `WsState`, `Status`, `StatusKind`, `Reject`;
- flat files: `FlatFileClient`, `FlatFile`, `ReadPlan`, `ReadColumn`, `object_key`;
- spec: `source`, `sources`, `Transport`, `Source`, `RestPath`, `Field`, `ParamKind`,
  `WireType`, `WireUnit`, `DriftReport`.

`Param` is reached as an attribute of `RestPath`. `Transport` (`REST`, `WS`, `FLAT_FILES`) is
the library's one transport enum.

**Wire errors.** There are three: `WireError`, `WireAuthError`, and `WireEntitlementError`,
each carrying `status` (or `None`), `host`, `path`, `request_id` (or `None`), and `message`. A
429 that persists after the retries is `WireError(status=429)`; a fatal WebSocket handshake is
`WireError(status=<http>)`. `_errors.from_wire` turns each into `MassiveError`,
`AuthenticationError`, or `EntitlementError` with the same message, `status`, and
`request_id`.

**The boundary and the layers**, enforced by an AST test in `tests/test_layout.py` with a
negative test per rule:

- Inside `_massive`, a module imports only the standard library, `urllib3`, `websockets`,
  `certifi`, the optional `orjson`, `boto3`, `botocore`, `pyarrow`, and other `_massive`
  modules. It never imports `deephaven`, `deephaven_server`, `jpy`, or any module outside
  `_massive`. The one exception is `typing_extensions` inside `if TYPE_CHECKING:` (for
  `override` on Python 3.11), which never runs.
- **Layers.** `net` and `spec` import nothing else in `_massive`. `rest`, `ws`, and `flatfiles`
  import only `net` and `spec`. `refresh` may import any of them, and only `refresh` and its
  tests import `refresh`.
- **The wire package's own tests.** Modules under `tests/massive/` (the mirror tests of
  `_massive` and the refresh parity tests) may import any `_massive` module, and only those
  under `tests/massive/refresh/` import `refresh`.
- Everywhere else outside `_massive`, the rest of `tests/` included: no module imports
  `urllib3`, `websockets`, `certifi`, `boto3`, or `botocore` (except `tests/_fakes/`); no
  module imports a `_massive` submodule (by `import`, relative import, or
  `importlib.import_module`/`__import__` with a literal name); an attribute read on `_massive`
  names only a member of `_massive.__all__`; and no module names a snapshot file.
- A subprocess import of `deephaven_massive._massive` leaves `deephaven` out of `sys.modules`.

**Not shipped.** The wheel excludes all of `refresh/`, the snapshot included. `spec/*.json` ship
as package data.

### 19.1 The REST client

`_massive.RestClient` is one object per session, created on first use and closed by
`m.close()` (a session closer). It is **safe to call from any number of threads at once**: its
shared mutable state is urllib3's connection pool, which locks internally, and a client-wide
**not-before instant** under a leaf lock. Every call is independent; no call mutates an
argument (the parameters are copied before they are encoded).

```python
client.get(rest_path, path_params, query_params) -> Page             # one request
client.pages(rest_path, path_params, query_params, *, limit=None) -> Iterator[Page]
                                                                    # follows next_url
Page(status: str, request_id: str | None, rows: list[dict], body: dict | list)
to_batch(rows, rest_path, *, required) -> (pyarrow.RecordBatch, DriftReport)
```

`rest_path` is the path's `RestPath` from `spec/rest.json`; the client reads its template,
result key, and parameter kinds from that value, so its tests build `RestPath` values by hand.

- **Transport.** One `urllib3.PoolManager` (or `ProxyManager`, below) with `retries=False` (the
  client retries itself), a 10 s connect and 30 s read timeout, `maxsize` at least
  `max_concurrency` so fan-out never discards connections, and `Accept-Encoding: gzip`
  (urllib3 decodes it). `User-Agent: deephaven-massive/<version>` (`net.USER_AGENT`).
- **Authentication.** `Authorization: Bearer <key>` on every request. The key never appears in
  a URL: an `apiKey` parameter in a `next_url` is removed before the request.
- **Proxies and certificates** come from `net.http`: `urllib.request.getproxies()` for
  `HTTPS_PROXY` and `urllib.request.proxy_bypass` for `NO_PROXY` select a `ProxyManager` for
  the REST host or a plain `PoolManager`, read once at construction. The CA bundle is the
  system default when `SSL_CERT_FILE` or `SSL_CERT_DIR` is set (an
  `ssl.create_default_context()` honours both) and `certifi`'s bundle otherwise.
- **Query encoding.** Path parameters are percent-encoded into the template. Query values, by
  the parameter's `ParamKind`: an enum by its value, `bool` as `true`/`false`, a `date` as
  ISO-8601, an aware `datetime` as exact integer nanoseconds computed from `timedelta` days,
  seconds, and microseconds (never through a float) for a nanosecond timestamp parameter and as
  ISO-8601 otherwise, and a sequence joined with commas (the `.any_of` form). A naive
  `datetime` raises `ValueError`. `None` values are omitted. Names arrive already in wire form
  (`strike_price.gte`); the dot mapping of [section 9](#9-filters) happens before the client.
- **Paging.** `pages` follows each page's `next_url`, which carries the cursor and the query.
  A `next_url` whose scheme and host differ from `rest_base_url` raises `WireError` and is never
  requested. `limit` is the total-row cap: paging stops once that many rows have been yielded,
  and the last page is truncated. Only one page of rows is held at a time.
- **Retries** follow `net/retry.py`. At most 3 retries (4 attempts); every request is a `GET`,
  so a retry is always safe. HTTP 500, 502, 503, 504 and connection errors wait 0.5, 1, then
  2 s, each with up to 20% upward jitter. HTTP 429 waits the `Retry-After` value in either form
  (delta-seconds or HTTP-date), otherwise 15, 30, then 60 s; a `Retry-After` above 60 s raises
  `WireError(status=429)` at once, naming the requested wait. After a 429 the client sets its
  not-before instant, so concurrent workers back off together instead of multiplying 429s. A
  TLS certificate failure (`ssl.SSLCertVerificationError`) is never retried: it raises
  `WireError` at once, naming `SSL_CERT_FILE`. A connection reset in the middle of paging
  retries **that page** (its `next_url`), never the first page.
- **Error mapping.** After the retries, the client raises: 401 as `WireAuthError`; 403, and a
  200 envelope with `status` `NOT_AUTHORIZED`, as `WireEntitlementError`; every other non-200
  status (429 included), a 200 envelope with `status` `ERROR`, and a connection failure as
  `WireError`. Each carries the status, the method and path (query without secrets), the
  `request_id`, and the first 200 characters of the server's `message` or body, and its message
  names them all.
- **Strict decoding.** The body is decoded once with the `net.codec` JSON codec. Bytes that are
  not UTF-8, or text that is not JSON, raise `WireError` with the status and the first 200
  bytes, on any call and on any page. So does a document nested deeper than 512 on the
  standard-library path: the codec counts the depth before decoding, so a hostile body can
  never exhaust the C stack of the calling thread (a JVM thread has 1 MB); orjson enforces its
  own limit of 1024. The page's rows come from the `RestPath`'s result key
  (`results`, or the `RESULT_KEYS` entry). A single object is wrapped as one row; a bare JSON
  list (`/v1/marketstatus/upcoming`) is the rows, with `status=""` and `request_id=None`. A
  missing result key is an empty page only when `status` is `OK` or `DELAYED`. The envelope's
  `status` and `request_id` are kept on the `Page`, so callers can read `DELAYED`.
- **Batches.** `to_batch` turns a page's rows into one Arrow batch of the path's generated
  fields, in long-form column names, and returns its `DriftReport`; it validates every value
  per [section 19.5](#195-runtime-drift). It is the only code that walks a wire path; no row
  object or model class sits between the JSON and Arrow.
- **Logging.** Each request logs at DEBUG: method, path, query (with no secret), status,
  `request_id`, row count, and elapsed time. Retries log at INFO with the wait, and exhausted
  retries at ERROR. The `Authorization` header is never logged.

### 19.2 The WebSocket client

`_massive.WsClient` is one object per connection (one `wss://host/path`), created by `_ws.py`
and run on its own daemon thread under `asyncio.run`. Its interface and callbacks are those of
[section 13.3](#133-connection-and-subscription-protocol). **Apart from `start()` (called once by
its creator, before the loop thread exists), `close()`, and `join(timeout)`, every method must be
called on the client's loop thread.** All its state is confined to that thread except the
`close()` request flag (a `threading.Event`), and `close()` touches nothing else: it hands its
work to the loop. That is what makes the design correct on free-threaded 3.14t without locks.

- **Connecting.** `websockets.asyncio.client.connect(url, ssl=..., max_size=None,
  open_timeout=None, user_agent_header=net.USER_AGENT)`, the whole handshake (below) running
  under `asyncio.timeout(10)`. `max_size=None` lifts websockets' 1 MiB frame cap; keepalive
  pings stay at websockets' defaults; the proxy comes from the environment through websockets'
  own proxy support; the CA bundle comes from `net.http`.
- **Handshake.** Read the `connected` status, send `{"action":"auth","params":"<key>"}`, and read
  the reply, all within the fixed 10 s handshake timeout, or the attempt fails and is retried.
  `auth_success` opens a generation (`on_open`). `auth_failed` is fatal (`WireAuthError`).
  `max_connections` reports `WAITING_SLOT`, and the attempt ends when the server closes the
  socket (or after the handshake timeout), then waits its backoff. The auth frame is never
  logged.
- **Receiving.** After `auth_success`, `protocol.py` decodes each text frame once, with the
  `net.codec` JSON codec, against the channel's generated fields in `ws.json` and the channel's
  required-key set and symbol column from `set_required` (none when none was set), into
  events delivered in
  order:
  - `on_status(generation, Status)` for each `ev == "status"` object. `Status.kind` is `ACK`
    (`topics` parsed from "subscribed to: <p1>[,<p2>...]", cluster-agnostic, a comma list
    tolerated; empty when the message cannot be parsed), `REFUSED` (`topics` holds the topic the
    message names, or is empty when it names none), `MAX_CONNECTIONS`, or `OTHER`. `message` is
    the server's text verbatim, and `realtime_verdict` is true when a refusal's message contains
    "real-time" (case-insensitive).
  - `on_data(generation, channel, rows, rejects, drift)` for each channel's data objects in the
    frame. Rows are keyed by **long-form** column (the library renames them) and hold every
    column of the channel's source, null when absent; values are validated and converted to
    their Python form (an epoch keeps its integer count), and symbols stay as the wire spells
    them. A row that fails validation becomes a `Reject(channel, symbol or None, WireError)`
    instead, with the raw value of the declared symbol column (`None` when none is declared or
    the value is missing); the frame's other rows are delivered. `drift` is the frame's
    `DriftReport` ([section 19.5](#195-runtime-drift)).

  No frame text is parsed outside `_massive`.
- **Sending.** `subscribe` and `unsubscribe` put their frames
  (`{"action":"subscribe","params":"T.AAPL,Q.AAPL"}`, at most 1,000 topics each) on one sender
  coroutine's queue, which sends them in order as soon as it runs: no change waits for a frame
  to arrive. Frames for a generation that is no longer current are dropped.
- **Reconnecting.** After every close and every failed attempt that `close()` did not request
  and that is not fatal (a server's clean close included), the client waits and tries again,
  forever. The first wait is `reconnect_backoff` (at least 15 s, [section 5](#5-massiveconfig));
  each further consecutive failure doubles it up to `max_reconnect_backoff`; jitter adds up to
  20%, clamped, so at the cap the wait is drawn from
  `[max(reconnect_backoff, max_reconnect_backoff / 1.2), max_reconnect_backoff]` and every wait
  lies in `[reconnect_backoff, max_reconnect_backoff]`. The failure count and the backoff reset
  only after a generation has stayed open for 60 s (stable uptime), never on `auth_success`
  alone. The client reports `RECONNECTING` with the next attempt's time before each wait.
- **Fatal and retryable failures.** `protocol.py` classifies an exception from `connect()`:
  - an HTTP 429 at the upgrade is retryable, and its wait is the larger of its `Retry-After`
    and the backoff;
  - a TLS certificate failure (`ssl.SSLCertVerificationError`) is fatal, naming
    `SSL_CERT_FILE`, although it is an `OSError`;
  - everything else follows websockets' public rule, the default `process_exception` of
    `connect()`, read from the `connect` object that raised (the function lives in
    `websockets.asyncio.client` before websockets 17 and in `websockets.client` from 17, and
    the floor is 15): what it returns `None` for (EOF, `OSError`, timeouts, HTTP 500/502/503/504) is retried, and
    anything it returns (for example HTTP 400 or 404, an invalid URI) is fatal.

  A fatal error ends the client and is reported as `FAILED` with `WireError(status=<http>)`
  naming the host.
- **Closing.** `close()` sets the request flag and calls `loop.call_soon_threadsafe` with a
  callback that cancels the task once, so a second `close()` cannot interrupt the socket's
  closing (a no-op once the loop has ended; the task checks the flag when it starts, so a
  `close()` before `start()` also ends it). The loop is created at construction, so
  `WsClient.loop` is valid before `start()`. Cancellation interrupts whatever the task awaits, a backoff
  wait included; a `finally` block closes an open socket with code 1000 and reports `CLOSED`.
  `close()` never waits; `join(timeout)` waits for the thread with a bound.
- **Logging.** INFO on connect, authentication, and close; WARNING on `max_connections` and on
  each reconnect wait (with its length); ERROR on a fatal error.

### 19.3 The flat-file client

`_massive.FlatFileClient(access_key_id, secret_access_key, endpoint_url, bucket, max_pool)` is
one object per session, closed by `m.close()`. It creates its boto3 S3 client lazily, under a
leaf lock, and keeps a closed flag (a `threading.Event`); once closed, it makes no request.

- **`open(key) -> FlatFile | None`.** `None` for a missing key (404). `object_key(prefix, day,
  venue)` builds keys as `<prefix>/<YYYY>/<MM>/<date>.csv.gz`, filling a `{venue}` template.
  The generator folds the four concrete futures overview prefixes into that template using
  `VENUES = ("cme", "cbot", "nymex", "comex")` in `refresh/overrides.py`, which a library test
  checks against `FuturesVenue`.
- **Error mapping.** An S3 error code `InvalidAccessKeyId` or `SignatureDoesNotMatch` is
  `WireAuthError`; any other 403 is `WireEntitlementError`; anything else is `WireError`. The
  messages keep the flat-file transport's texts ([section 16](#16-historysourceauto)).
  `request_id` comes from `x-amz-request-id`. Credentials are never logged.
- **TLS, proxies, retries.** The client is built as `session.client("s3", verify=<the
  net.http CA bundle>, config=Config(proxies=<from net.http>, retries={"total_max_attempts":
  1}))`: the CA bundle is `SSL_CERT_FILE`, else certifi (`SSL_CERT_DIR` does not apply to S3),
  and botocore's own retries are off; the shared
  `net/retry.py` schedule retries HTTP 500, 503 `SlowDown`, `RequestTimeout`, and connection
  errors before the first byte. A TLS failure is never retried, and neither is a failure partway
  through a body.
- **`prefixes()`** lists the bucket's top-level prefixes, for `--check`.

`FlatFile.batches(plan)` yields one `(RecordBatch, DriftReport)` per block, in this order:
(1) the futures session check; (2) the symbol filter (`is_in`); (3) `[start, end)` on
`plan.to_ns(time column)`; (4) on the survivors, the rename to long-form names and list
parsing. Instants stay in their wire unit; the library converts them. The reader first peeks the
header line and reads by name ([section 16](#16-historysourceauto)). `close()` is idempotent and
safe from any thread.

```python
ReadPlan(columns: tuple[ReadColumn, ...], symbol_header, symbols, time_header, window_ns,
         to_ns, session_header, required)
ReadColumn(header, column, read_type: pyarrow.DataType, list_type)
```

`_flat_files.py` builds the plan from `source(Transport.FLAT_FILES, prefix)`. `to_ns` is the
`_timeunits` converter, so a wrongly declared unit still raises; the window is never converted
into wire units.

### 19.4 Generated schemas

No schema is hand-written. The refresh generates three committed, canonical files from the
vendored snapshot: `spec/rest.json` (from `openapi.json`), `spec/ws.json` (from
`websocket.json`), and `spec/flatfiles.json` (from the flat-file "Columns" tables). Each holds,
per source, an ordered list of fields `(wire key, column, WireType, unit, required)`. Every
change shows up as a reviewed diff of a generated file. Hand input is naming, grouping, and
verification, never a column list.

**Sources and flattening.**

- **REST.** Every `openapi.json` path that is not deprecated, draft, or ignored and not in
  `EXCLUSIONS`. The generator resolves `$ref`, merges `allOf`, flattens nested objects into
  dotted wire paths below the result key (`results` unless `RESULT_KEYS` names another), keeps
  fields in spec order, and records the parameters (wire name, `ParamKind`, enum values, path or
  query).
- **WebSocket.** Every `websocket.json` channel not in `EXCLUSIONS`, keyed `(cluster,
  channel)` and spelled `"<cluster>/<channel>"` as its `Source.key` (`"stocks/T"`,
  `"futures/AM"`), since stocks, options, and futures share channel names. Fields are its
  message properties in spec order; `required` is copied where the vendor gives it.
- **Flat files.** Every dataset page with a row in its class's overview table. That row's
  `S3 <prefix>` is the source key, so no hand-written page-to-prefix map exists. A futures kind
  keyed by `{venue}` requires its four venue pages to list identical columns. Fields are the
  page's "Columns" table (name, type) in table order; the example rows are ignored. A dataset
  page with no overview row is reported as unaccounted by `--check`.
- **Representation rules** (generator rules, not hand entries): an array of strings or objects,
  a free-form object, and `oneOf`/`anyOf` become `json-text`; an array of integers becomes
  `int-array`; the WebSocket envelope key `ev` is dropped; a field with a `UNITS` entry becomes
  an `epoch` in that unit; a string with `format: date-time`, or no format and a name ending in
  `_utc`, becomes an `iso-instant`; a string with `format: date`, or named `date` or `*_date`,
  becomes a `date`.
- **Projections.** `PROJECTIONS {(kind, source): {wire key: long name}}` limits a source to
  the listed keys, and renames them, **inside one membership only**. The generator writes each
  long name through the naming rule, so a generated projection maps wire keys to PascalCase
  columns, the same vocabulary as `Field.column`. Its first entry gives
  `/v3/snapshot/indices`'s membership of the values kind
  `{"ticker": "Ticker", "value": "Value", "last_updated": "Timestamp"}`, for
  `indices.values.fetch()`; `indices.snapshots` uses the same source in full.
- **Memberships are written into the generated files.** `KINDS` and `PROJECTIONS` are generator
  inputs in `refresh/`, which is not shipped. The generator writes each source's memberships
  onto its committed entry in `spec/rest.json`, `ws.json`, or `flatfiles.json`, and the `spec`
  loader exposes them as `Source.memberships: tuple[Membership, ...]`, where
  `Membership(kind, group, position, projection: Mapping[wire_key, column] | None)`: the
  kind, the asset-class group, the source's order within the group, and its projection. A
  source in no kind has no memberships; a source may hold several. Everything at run time, the
  merge and gate C4 included, reads memberships from there and never from `refresh/`. Drift
  detection stays per source, against the full fields, so projected-away fields are never
  reported as drift.

**The type lattice.** `WireType` is one of `long`, `double`, `bool`, `string`, `date`, `epoch`
(an integer with a unit), `iso-instant`, `int-array`, and `json-text`. OpenAPI `integer`, and
`number` with `format: integer`, map to `long`, and `number` to `double`. Doc pages map
`integer` to `long`, `number` to `double`, `string` to `string`, and `timestamp - integer` to
`epoch` (with its `UNITS` entry). Two types meet for one column name within one kind by these
joins: `long ⊔ double = double`; `long ⊔ int-array = int-array` (a scalar becomes a
one-element list); `epoch ⊔ epoch = epoch`, each source keeping its own unit. Any other pair is a
**type conflict** (gate G5). `_schema` maps `long` to `long`, `double` to `double`, `bool` to
`boolean`, `string` and `json-text` to `String`, `date` to `LocalDate`, `epoch` and
`iso-instant` to `Instant`, and `int-array` to `int[]`. An `int-array` element outside the
`int32` range is an unconvertible value.

**Hand input.** All of it lives in `refresh/`, every entry carries a reason, and an entry that
names a source or field the snapshot lacks fails the refresh as stale (G4).

| Kind | Where | What | Example |
|---|---|---|---|
| Lexicon | `lexicon.py` | `SHAPES` maps each object shape (aggregate row; snapshot `day`, `min`, `prevDay`, `lastTrade`, `lastQuote`; each WebSocket channel; each futures object) to its lexicon. A lexicon translates a short key to Massive's own long form; identity entries cover short real words (`id`, `cik`, `url`, `low`, `fmv`, `mic`). | trade lexicon `p` → `price`, `t` → `sip_timestamp`; segment `min` → `minute` |
| `NAMES` | `overrides.py` | `{(source, wire key): (long name, reason)}` for a column the lexicon cannot name | stocks `A`/`AM` `dv` → `decimal_volume`, `dav` → `accumulated_decimal_volume`; futures `A`/`AM` `dv` → `dollar_volume`; `av`, `op`, aggregate `a`, aggregate `z` |
| `KINDS` | `overrides.py` | `{kind: {group: [sources]}}`: the sources of one dataset kind, grouped per asset class, in source order. Every dataset with a stream or flat-file source has a kind, WebSocket-only `luld`, `imbalances`, and `fmv` included. | `trades: {stocks: [/v3/trades/{stockTicker}, /v2/last/trade/{stocksTicker}, (stocks, T), us_stocks_sip/trades_v1], crypto: […]}` |
| `PROJECTIONS` | `overrides.py` | see above | indices snapshot → values, `last_updated` → `timestamp` |
| `VENUES` | `overrides.py` | the futures venues, folded into the `{venue}` template | `cme`, `cbot`, `nymex`, `comex` |
| `UNITS` | `overrides.py` | the verified unit per `(source, wire key)`, `unverified-ms` where [section 23](#23-open-verification-items) says so, or "not a timestamp"; with the tag-conflict list | the 20 indicator `IMicroseconds` tags are ms |
| `TYPE_FIXES` | `overrides.py` | vendor type **errors** only: a declared, field-scoped conversion that the decoder applies deliberately (see **Declared conversions** below); each is exercised by a fixture test, and one the vendor's own docs do not show goes into section 23 | flat-file `conditions` `integer` → `int-array`; forex `x` (documented as a quoted decimal string) → `long`; crypto `XT` `i` (`integer`) → `string`; snapshot `lastQuote` sizes → `double` |
| `TRANSPORT_ONLY` | `overrides.py` | `{(kind, column): reason}`: columns legitimately on one transport | `AccumulatedVolume` (WebSocket only) |
| `RESULT_KEYS` | `overrides.py` | the paths whose rows are not under `results` | `/v1/last/crypto` → `last` |
| `EXCLUSIONS` | `exclusions.py` | `(kind, item, reason)`, the executable rows of section 8.12 | `/v1/summaries` |

**The naming rule.** A field's column is derived by splitting its wire path into segments and
dropping the result key, translating each short key through the lexicon of its shape, applying
`NAMES`, and PascalCase-joining the segments (`prevDay.vw` becomes `PrevDayVwap`). Names are
never derived from descriptions. The generator never emits `Symbol` or `Underlying`, and emits
`Timestamp` only where the vendor's long name is `timestamp`. The WebSocket-only
names are `av` → `AccumulatedVolume`, `op` → `OfficialOpenPrice`, aggregate `a` → `DayVwap`,
aggregate `z` → `AverageTradeSize`, `dav` → `AccumulatedDecimalVolume`, stocks `dv` →
`DecimalVolume`, and futures `dv` → `DollarVolume`; the snapshot object `min` gives
`Minute…` columns.

**Refresh gates.** The refresh fails, printing the source, the key, and its description for the
reviewer, on:

- **G1:** a short key (three characters or fewer, no underscore) with no lexicon, identity, or
  `NAMES` entry;
- **G2:** two keys of one source producing one column;
- **G3:** a timestamp-like integer field with no `UNITS` entry. ISO date-time text fields
  (`last_updated` on benzinga and futures products, `timestamp` on futures schedules) are
  excluded; the representation rules decide them. G3 runs on the long name, after the
  lexicon and `NAMES`: an integer field is timestamp-like when it has a unit tag, a doc type of
  `timestamp - integer`, or a long name matching `timestamp`, `*_timestamp`, `updated`,
  `*_updated`, `window_start`, `window_end`, or `*_time` (the bar lexicon gives `s` and `e`
  as `window_start` and `end_timestamp`, while a trade's `s` is `size`);
- **G4:** a stale hand entry: a `NAMES`, `UNITS`, `TYPE_FIXES`, `PROJECTIONS`, `KINDS`,
  `RESULT_KEYS`, or `EXCLUSIONS` entry naming a source or field the snapshot lacks;
- **G5 (type agreement):** within one kind, one column name joins to one `WireType`, unless a
  `TYPE_FIXES` entry resolves it; the message prints both keys' descriptions, because a
  conflict is first treated as a possible naming error (stocks and futures `dv` are exactly
  that);
- **G6 (transport agreement):** within one `KINDS` group whose sources span two or more
  transports, every column appears on at least two transports or is in `TRANSPORT_ONLY`,
  which catches a one-sided lexicon slip (`s` becoming `Shares` while REST says `size`);
- **G7:** a futures kind whose venue pages differ.

Descriptions differ across transports and change about twenty times as often as keys, so G5 and
G6, not description matching, are the mechanical checks.

**Dataset schemas** are merged at run time, after the rename, never inferred from data:

```
dataset_schema(spec):                       # cached per dataset; computed outside the lock,
                                            # published under it (first writer wins)
  members = datasets of spec.kind, ordered by the group and position of the memberships of
            their sources                         (spec alone for a dataset in no kind)
  for each member m, each source s of m (its role paths, channels, prefixes):
      fields = s.fields through the projection of s's membership of (m.kind, m's group), if any
      cols(m, s) = rename(m, s, fields)      # m's keys for s: symbol -> Symbol, time ->
                                             # Timestamp, fanout -> Underlying; a key filled
                                             # from the request adds a constant column;
                                             # nothing else changes
  schema = ordered union by column name of every cols(m, s):
           Symbol, then Timestamp, first where the dataset declares them; then first
           appearance in member, then source, then field order
  each column's WireType = the lattice join over every contribution
```

**Declared conversions.** A `TYPE_FIXES` entry names one field of one source, the wire form,
and the generated type, and it is recorded on the generated `Field`. The decoder applies that
conversion deliberately: forex `x` arrives as a quoted decimal string (`"48"`) and is parsed to
`long`. A value that does not parse (`"4.8"`, `"x"`) is an unconvertible value and fails loudly
like any other ([section 19.5](#195-runtime-drift)). Every entry has a fixture test of both the
conversion and the failure. A conversion that no entry declares is coercion, which never
happens.

**Keys are declared per source.** For each source it uses, a `DatasetSpec` declares its key
columns as `KeySpec(column, from_request)`, one optional declaration per role (`symbol`,
`time`, `fanout`, `session`):

- every source of a dataset in a kind declares `symbol` and `time`;
- every source of any other instrument dataset declares `symbol`;
- any other dataset declares only the keys its rows carry, possibly none: `reference.exchanges`,
  `conditions`, `market_status`, `market_holidays`, and `ticker_types`, `futures.exchanges`,
  and `benzinga.firms` declare none, and `economy.*` declares `time` and no `symbol`;
- `fanout` and (futures) `session` are declared only on the sources that carry them.

A key is `from_request` when the source does not carry it in its rows: the symbol of a source
that takes it as a path parameter or holds it outside the result key
([section 10](#10-symbols-and-all)). The library fills a
`from_request` key as a constant column. `to_batch(..., required=)` and `ReadPlan.required`
(per request) and `WsClient.set_required(channel, keys, *, symbol)` (per channel, before its first
subscription, [section 13.3](#133-connection-and-subscription-protocol)) receive exactly the
source's required keys, so a futures WebSocket row without
`session_end_date` is not a missing key, while a futures flat file without it is. Every member
of a kind gets the identical
column set: crypto's `ParticipantTimestamp` is renamed to `Timestamp`, while stocks keeps its
`ParticipantTimestamp` column, so crypto rows hold `ParticipantTimestamp` null. A non-tick
dataset with several role paths (`options.contracts` list and per-contract) is the union of its
role paths under the same join.

**Catalog gates** (`tests/test_catalog.py`, with negative tests on synthetic `DatasetSpec`s):

- **C1:** every key a dataset declares for a source is either a field of that source after
  the projection of the dataset's membership (the `indices.values` time key on
  `/v3/snapshot/indices` is `timestamp` only after it), or `from_request`; every source of a
  dataset in a kind declares `symbol` and `time`, and every source of an instrument dataset
  declares `symbol`. Its negative tests cover an undeclared kind key, an undeclared instrument
  symbol, a declared key the source lacks, and a reference dataset that declares none and
  passes;
- **C2:** after the rename, no source has a duplicate column;
- **C3:** across members, one column name gets one `WireType`;
- **C4:** each kind dataset's sources equal the sources holding a membership of its
  `(kind, group)`;
- **C5:** every declared wire name (time-axis pair, symbol parameter, path placeholders) is a
  parameter of its `RestPath`;
- **C6:** every path, channel, and prefix a dataset uses is generated; a miss names the dataset
  and says "deprecated or removed in the snapshot";
- **C7:** generated ⊆ used ∪ undeclared, and no namespace's undeclared file lists a source
  that namespace already uses ([section 8.12](#812-excluded-endpoints)).

`KINDS` overlaps the catalog by design: the refresh checks names before any dataset is
declared, and C4 keeps the two in step. A test loads `spec/` with `refresh/` absent, as in an
installed wheel; the CI build job checks that the wheel holds no `refresh/`.

### 19.5 Runtime drift

Data on the wire can differ from the committed schema until the next refresh. **Detection** is in
`_massive`: each decoder compares a payload with its generated source and returns a
`DriftReport(extra, missing)`. A decoder never raises for drift; REST and flat files raise
`WireError` for a bad value, and WebSocket returns `Reject`s. **Policy** is in the library: the
session's `_diagnostics.DriftRegistry` (a leaf lock, WARNINGs logged after release) and the
routing in `_rest.py`, `_poll.py`, `_ws.py`, `_stream.py`, and `_flat_files.py`.

| Case | REST (`rest/batch.py`) | WebSocket (`ws/protocol.py`) | Flat files (`flatfiles/reader.py`) | Library |
|---|---|---|---|---|
| **Extra field** | key not in the path's fields | key not in the channel's fields | header not in the documented headers | Dropped. WARNING once per (dataset, transport, field). Shown in `SchemaDrift`. |
| **Missing required key** (a key the dataset declares for this source, not `from_request`; a dataset that declares no key has none) | `WireError` naming the key | `Reject(channel, symbol or None, WireError)` | `WireError` before the first block | Loud failure. |
| **Missing other field** | null | null | a null column of the declared type | A vendor-`required` field or a CSV header: null, WARNING once, `SchemaDrift`. Any other field: null, silently, because JSON omits absent values and the vendor marks almost nothing required. |
| **Unconvertible value** (the type, the `int64` range, `bool` given as a number, an `int-array` element, `int32` for `int[]`) | `WireError` naming the field, the expected type, and the value (first 50 characters) | `Reject` for that row's topic; the frame's other rows are delivered | `WireError` naming the header, the type, and Arrow's message | Loud failure. Never coerced: no undeclared conversion is applied. A lattice widening and a declared `TYPE_FIXES` conversion ([section 19.4](#194-generated-schemas), for example forex `x` from a decimal string to `long`) are not coercion; a value the declared conversion cannot parse fails here. |
| **Timestamp out of [1990, 2100], or overflow** | in `_timeunits` | in `_timeunits`, in the flush thread as a vectorized mask | in `_timeunits` | Loud failure naming the dataset, the column, the unit, and the value. |
| **Undocumented value of an interpreted field** (`timeframe`) | `_hosts.py` | | | `MassiveError` ([section 13.2](#132-host-decision)). |

**A loud failure** depends on the verb: static verbs (`fetch()`, `history()`) raise
`MassiveError` through `from_wire`; `poll()` fails its table through `publish_failure`; a
WebSocket failure fails, through `publish_failure`, only the streams that would have received
the row: the channel's `ALL` streams and the streams holding that symbol. A row with no
symbol is rejected and fails only the channel's `ALL` streams, since it cannot be routed to a
symbol stream; it is logged at ERROR. Rejected rows are never published, and the rest of the
batch is. A drift failure goes through `SessionCore.fail_stream`, like an entitlement
failure.

**`SchemaDrift`** is a text column of both diagnostics tables ([section 17](#17-diagnostics)),
sorted and truncated at 200 characters: `m.streams()` uses `transport:field+` for extra and
`transport:field-` for missing, and `m.connections()` uses `channel:field+` and
`channel:field-`.

### 19.6 Refresh and drift

```
uv run python -m deephaven_massive._massive.refresh            # regenerate spec/*.json from the snapshot
uv run python -m deephaven_massive._massive.refresh --fetch    # replace the snapshot with the live sources
uv run python -m deephaven_massive._massive.refresh --check    # live: report drift; changes nothing
```

The command runs from a source checkout; `refresh/` is not in the wheel. The parity test in
`tests/massive/refresh/test_generate.py` regenerates `spec/*.json` from the snapshot and
requires it byte for byte, and checks documented = generated ⊎ excluded ⊎ (deprecated ∪ draft
∪ ignored).

**The snapshot is stored canonically.** `fetch.py` writes JSON with `sort_keys`, `indent=1`,
and a final newline, and text with trailing whitespace stripped and a final newline, so refresh
diffs read line by line and no hook exception is needed. `manifest.json` records the fetch date
and the SHA-256 of the fetched bytes and of the stored file.

**`--check`** downloads the live `openapi.json`, `websocket.json`, `llms.txt`, and flat-file
pages and overviews, compares them semantically with the snapshot (`refresh/drift.py`), prints a
report, changes nothing, and exits non-zero on drift. It reports REST paths new, removed, or
newly deprecated (including deprecated paths still in `rest.json`); fields and parameters
added, removed, or retyped; WebSocket channels and keys; flat-file pages, headers, and overview
rows; changed unit tags; the keys the gates would stop on; and **unaccounted items**:
documented REST endpoints, channels, and flat-file pages neither generated nor excluded, plus,
when S3 credentials are set, bucket prefixes from `FlatFileClient.prefixes()` that nothing
names.

**The drift job.** `.github/workflows/spec-drift.yml` runs `--check` weekly and on demand. With
drift it keeps one standing "Massive API drift" issue (found by title and the `spec-drift`
label, which it creates when missing; reopened if closed; never a duplicate) listing each item
with its action; with no drift it does nothing. It has `issues: write` only, never opens a
pull request, and is never a gate.

**Closing the loop.** A person runs `--fetch` and the plain refresh, then reviews one PR holding
the snapshot diff, the `spec/*.json` diff, and any hand-input lines, and `check.sh` runs G1–G7
and C1–C7. What each change does:

| Change at Massive | At run time | In the refresh |
|---|---|---|
| Field added | Dropped, one WARNING, `SchemaDrift`. | It gets a name (G1 asks for a lexicon or `NAMES` line for a short key) and joins its kind (G5 checks the type, G6 the transports). |
| Field removed | Null; a required field or CSV header warns; a required key fails loudly. | It leaves its source; C1 fails if it was a declared key. |
| Field renamed | A removal plus an addition. | The column is renamed in the diff, or stays the same if the long form is unchanged; G6 catches a one-sided rename. |
| Type changed | A value that no longer converts fails loudly. | G5 or C3 fails on disagreement, resolved as a naming error first, then by a vendor fix or a `TYPE_FIXES` entry. |
| Unit changed | The overflow check or the bound fails loudly. | `--check` reports the changed tag, and the `UNITS` entry is re-verified. |
| New endpoint, channel, or file | Nothing. | `--check` reports it as unaccounted, the refresh generates it, and C7 fails until a `DatasetSpec` or an `EXCLUSIONS` entry is added. |
| Endpoint deprecated or removed | `MassiveError` with the HTTP status. | It leaves the generated set, and C6 names every dataset that uses it. |

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
- **Layout:** `tests/test_layout.py` passes: the mirror rule and naming of
  [section 4.2](#42-test-layout), and the `_massive` boundary and layers of
  [section 19](#19-wire-clients-and-the-spec-snapshot).
- **Spec:** `tests/massive/refresh/test_generate.py` passes (parity of `spec/*.json` byte for
  byte, documented = generated ⊎ excluded ⊎ (deprecated ∪ draft ∪ ignored), and refresh gates
  G1–G7), and `tests/test_catalog.py` passes (catalog gates C1–C7, generated ⊆ used ∪
  undeclared) ([section 19.4](#194-generated-schemas)). They read only vendored files, never
  the network.
- **Negative tests:** every gate has a negative test that fails when the gate is deleted.
- **Process:** conventional-commit titles; every change lands through a pull request; only the
  owner merges.

**Spec drift is advisory, never a gate.** `.github/workflows/spec-drift.yml` runs
`python -m deephaven_massive._massive.refresh --check` weekly and on demand, the same command a
person runs locally ([section 19.6](#196-refresh-and-drift)). It runs without S3 credentials,
so flat-file coverage comes from the documentation pages and overviews.

- **No drift:** the job does nothing: no issue, no comment.
- **Drift:** it opens, or updates in place, the single standing issue "Massive API drift"
  (found by that title and the `spec-drift` label, which it creates when missing; reopened if
  closed; never a duplicate). The body lists each item with its action:
  - **unaccounted items** (a documented REST endpoint, WebSocket channel, or flat-file dataset
    neither generated nor excluded): refresh and add a dataset, or add an `EXCLUSIONS` entry
    with a reason;
  - **field, type, and column changes** (fields and parameters added, removed, or retyped;
    WebSocket keys; CSV headers; changed unit tags; keys the gates would stop on): run
    `--fetch` and the plain refresh, adding a lexicon, `NAMES`, or `UNITS` line where a gate
    asks;
  - **deprecations** (a generated path newly deprecated or removed): replace the dataset's path
    or add it to `EXCLUSIONS` with a reason.
- It has `permissions: issues: write` only, never pushes, never opens a pull request, and is
  never a required check, because a change at Massive must not block unrelated pull requests.

### 20.1 Tooling configuration

This is the exact configuration the tooling work package applies. Every lint ignore carries a
reason comment in `pyproject.toml`; adding one needs owner approval.

- **Ruff.**
  - `select = ["ALL"]`. Linters and type checkers track their latest releases: the dev group
    declares floors only (`ruff>=…`, `mypy>=…`, `pyright>=…`), and with no committed `uv.lock`
    every `check.sh` run (`uv sync --upgrade`) and every CI job installs the newest releases; a
    plain `uv sync` or `uv run` keeps the local lock. Because `ALL` enables new rules as they ship, a new
    release may surface new findings; the next PR fixes them (or proposes an ignore for owner
    approval).
  - `preview = true` with `explicit-preview-rules = true`, enabling only the preview rules
    selected by name: `DOC201`, `DOC501`, and `DOC502` (docstring Returns and Raises sections
    must match the code), and `PLR0904`, `PLR0914`, and `PLR0916` (the Pylint limits on public
    methods, local variables, and boolean expressions, which are still preview rules). Preview
    also switches some stable rules to their preview behavior.
  - Global ignores: `COM812`, which conflicts with `ruff format` (it is the only one of ruff's
    documented formatter-overlap rules that reports anything on formatted code; the others stay
    enabled), plus `TRY003`, `EM101`, `EM102`, `PLR0913`, and `CPY001`.
  - Enforced on purpose: `E501` at 100 columns, `ANN401` (no `Any`), the `TC` import rules,
    `SLF001` in `src/`, `C901` at a maximum of 12, the Pylint size limits (`PLR0904` and
    `PLR0911`–`PLR0916`, except `PLR0913` above), and `PLR0917` with `max-positional-args = 3`.
  - Ignored in `tests/` only: `S101`, `PLR2004`, `SLF001`, `ARG001`, `ARG002`, `FBT001`
    (listener stand-ins keep Deephaven's `on_update(update, is_replay)` signature), and
    `D100`–`D103` and `D107` (test docstrings are optional, but any that exist must be well
    formed).
  - `src = ["src", "."]`, the same import roots as mypy and pytest, and
    `namespace-packages = ["tests"]` (`tests/` has no `__init__.py`), so `INP001` needs no
    ignore. `banned-api` bans both `conftest` and `tests.conftest`.
  - Google docstring convention; `docstring-code-format = true`.
- **Mypy.** `strict`, `warn_unreachable`, `warn_unused_configs`, and `enable_error_code =
  ["ignore-without-code", "redundant-expr", "truthy-bool", "truthy-iterable",
  "possibly-undefined", "unused-awaitable", "deprecated", "explicit-override"]`, with
  `mypy_path = ["src"]`. The dev group adds `types-boto3[s3]`. The only override is
  `ignore_missing_imports` for `pyarrow.*`, which ships no type information; `urllib3`,
  `websockets`, and `certifi` ship their own. No other override is allowed. Pyright runs as a
  non-gating second checker.
- **Pytest 9** (native `[tool.pytest]` table). `strict = true`, `filterwarnings = ["error"]`,
  `addopts = ["-ra", "--import-mode=importlib"]`, `pythonpath = ["."]` (test code imports
  helpers as `tests._fakes…`, so `tests/massive/` never shadows a top-level `massive`),
  `faulthandler_timeout = 120` with
  `faulthandler_exit_on_timeout = true`, and `pytest-timeout` at 60 seconds. Coverage is not in
  `addopts`, so single-test runs and debuggers work.
- **Coverage.** `branch = true`, `fail_under = 100`, `exclude_also = ["if TYPE_CHECKING:",
  "@(typing\\.)?overload"]`, each with a reason.
- **Deephaven imports.** Module-level wherever used at run time; imports used only in
  annotations go under `TYPE_CHECKING` (enforced by `TC`). `tests/conftest.py` starts the
  embedded JVM in `pytest_configure`, before collection.
- **One entry point.** `scripts/check.sh [fast|full] [--no-fix]`. Both tiers first run
  `uv sync --upgrade`, so they use the newest versions; offline, a local run warns and keeps
  what is installed, and CI fails. The fast tier then runs ruff fix, ruff format, and mypy; the
  full tier adds pytest with coverage. Local hooks and CI call the same script; CI passes
  `--output-format github` to ruff.
- **Agent hooks** (committed `.claude/settings.json`). After an agent edits a `.py` file, ruff
  fixes and formats it and reports remaining diagnostics to the agent. Before an agent ends its
  turn, `scripts/check.sh fast --no-fix` runs. Bare `python`, `pytest`, `ruff`, `mypy`, and `pip`
  commands not prefixed by `uv run` are blocked.
- **Git hooks** (`.pre-commit-config.yaml`). The commit stage runs the fast tier plus
  `no-commit-to-branch` (main), `check-yaml`, `end-of-file-fixer`, and `trailing-whitespace`.
  The full tier runs at the pre-push stage.
- **Packaging.** `uv.lock` is not committed, and CI resolves afresh on every run. The wheel
  excludes all of `refresh/` (`[tool.hatch.build.targets.wheel] exclude =
  ["src/deephaven_massive/_massive/refresh"]`), the snapshot included; `spec/*.json` ship as
  package data, and the CI build job asserts both. Development dependencies use a PEP 735
  `[dependency-groups]` table; license metadata follows PEP 639. `__version__` comes from
  `importlib.metadata`. `deephaven-server` is a development dependency only. The runtime
  dependencies are declared directly, with floors: `deephaven-core`, `pyarrow`, `boto3`,
  `botocore` (a floor whose `Config` takes `proxies` and `retries={"total_max_attempts": …}`),
  `urllib3`,
  `websockets` (a floor with proxy support in `websockets.asyncio.client.connect`), and
  `certifi`. There is no `massive` dependency and no `[tool.uv.sources]` table: every
  dependency comes from PyPI. `.gitignore` lists only the files this project produces.
- **Banned import.** `[tool.ruff.lint.flake8-tidy-imports.banned-api]` bans `massive` with the
  message "The library has its own clients; see deephaven_massive._massive". A ban is not an
  ignore, so it needs no approval.
- **Release, on the owner's go-ahead only.** Publishing is not turned on. When the owner gives
  the go-ahead, `.github/workflows/release.yml` runs on a `v*` tag: it builds the sdist and the
  wheel, checks that the tag equals `project.version`, and publishes to PyPI through trusted
  publishing (`id-token: write`, no long-lived token). The first version is 0.1.0. Until then
  no publishing workflow, GitHub environment, or pending publisher exists. **Hazard:** a PyPI
  version can never be re-uploaded, so no rehearsal may push a tag that matches the project
  version; the rehearsal method is chosen at the go-ahead.
- **Metadata.** `authors = [{ name = "Chip Kent", email = "chipkent@illumon.com" }]`; repository
  `https://github.com/chipkent/deephaven-massive`.

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

1. **No SDK: own clients over Massive's documented APIs** (revised 2026-10-26; this replaces
   "official SDK" and the fork). The library has its own REST client on `urllib3` and its own
   WebSocket client on `websockets`, and speaks Massive's documented protocols directly
   ([section 19](#19-wire-clients-and-the-spec-snapshot)). Using Massive's SDK would have meant
   about fourteen workaround layers, two reads of undocumented SDK attributes, and permanent
   residual defects: a wildcard subscribe that drops sibling topics, a clean server close that
   ends the client, instant reconnects, up to 1 s of subscribe latency, no single-topic re-send,
   invalid JSON returned as an empty list, unknown fields silently dropped, `datetime` converted
   through a float, and REST decoding 2.5–5 times slower than columnar decoding. The fork that
   fixed them could not be published to PyPI as a dependency. Owning the clients costs about one
   week before 1.0 and permanent ownership of HTTP and WebSocket edge cases.
2. **Every schema is generated from the vendor's specs.** No schema is hand-written and none is
   inferred from data. The refresh generates `spec/rest.json`, `spec/ws.json`, and
   `spec/flatfiles.json` from the vendored, pinned `openapi.json`, `websocket.json`, and
   flat-file pages; dataset schemas are merged from them at run time
   ([section 19.4](#194-generated-schemas)). Hand input is naming, grouping, and verification
   only. Fallback chains are gone.
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
    in the documentation. Deprecated and sunset endpoints are excluded with a reason.
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
16. **WebSocket frames are decoded by the client.** `WsClient` decodes each frame once, against
    `ws.json`, into status events, rows, rejects, and drift; the library parses no frame text.
    `max_connections`, in the auth reply or as a later status, shows `WAITING_SLOT`, and
    streams stay alive (section 13.3).
17. **The context manager is kept**, documented as static-results-only.
18. **`m.options.snapshots(dm.ALL)` is allowed.** It paginates the whole OPRA universe, which is
    slow and documented as such. The rule "`ALL` is accepted by every per-class `snapshots()`"
    stays uniform.
19. **No live testing as design input.** Unverifiable facts get the conservative choice and a
    fixture test; live tests come later and retire the UNVERIFIED notes.
20. **All merges into `main` happen through pull requests, and only the owner merges.**
    `uv.lock` is not committed, and every sync resolves afresh.
21. **Sessions and queries expose no introspection attributes; their reprs are defined.**
    `MassiveSession` has no `name` or `config`, and queries have no `dataset`: a session's label
    appears only in its repr (`<MassiveSession #1 open>`), the same-key WARNING, and error
    messages, and a query's dataset and arguments only in its repr. The public surface stays
    exactly the verbs and namespaces, and the API key stays out of objects users routinely
    leave in console globals.
22. **Names come from the generated tables** (2026-10-26). The generator names every field from
    its wire key, the lexicon of its shape, and `NAMES`; each `DatasetSpec` declares the key
    columns its sources carry, and `_schema.rename` maps them to `Symbol`, `Timestamp`, and
    `Underlying`. Names are never derived from descriptions, and gates G1–G7 and C1–C7 fail on a
    wrong name, type, unit, or merge ([section 19.4](#194-generated-schemas)). The final names:
    `av` → `AccumulatedVolume`, `op` → `OfficialOpenPrice`, aggregate `a` → `DayVwap`,
    aggregate `z` → `AverageTradeSize`, `dav` → `AccumulatedDecimalVolume`, stocks `dv` →
    `DecimalVolume`, futures `dv` → `DollarVolume`, and the snapshot object `min` → `Minute…`.
23. **Spec drift is reported, not gated** (2026-10-26). A weekly workflow runs
    `python -m deephaven_massive._massive.refresh --check` and keeps one standing "Massive API
    drift" issue, creating its label when missing; with no drift it does nothing, and it never
    opens a pull request or becomes a gate ([section 19.6](#196-refresh-and-drift)).
24. **No client object is shared unless it is proven thread-safe** (2026-10-26). The REST client
    is safe per call and shared by the session's threads; each WebSocket client is confined to
    its own loop thread. Free-threaded 3.14t remains a required CI job.
25. **No SDK pull requests; upstream issues are optional** (2026-10-26). The library never
    depends on a fix in `massive-com/client-python`. Real bugs found there may be reported as
    issues, never pull requests, off the path.
26. **One wire package** (2026-10-26). `deephaven_massive._massive` owns all Massive wire
    access (REST, WebSocket, and S3 flat files), the vendored snapshot, the generated schemas,
    the units, and the long-form names, in six subpackages (`net/`, `spec/`, `rest/`, `ws/`,
    `flatfiles/`, `refresh/`). It imports nothing from Deephaven or the rest of the library,
    and the rest of the library imports only its interface; an AST test enforces the boundary
    and the layers ([section 19](#19-wire-clients-and-the-spec-snapshot)).
27. **`MassiveError` carries `status` and `request_id`** (2026-10-26). Both are read-only and
    `None` when the error did not come from an HTTP response ([section 3](#3-exports)).
28. **`reconnect_backoff` is at least 15 s** (2026-10-26). `MassiveConfig` rejects a smaller
    value with `ValueError` ([section 5](#5-massiveconfig)).
29. **`refresh/` is not shipped** (2026-10-26). The wheel excludes all of `refresh/`, the
    snapshot included; `spec/*.json` ship ([section 19](#19-wire-clients-and-the-spec-snapshot)).
30. **Wire behaviour settled in review** (2026-10-26). A 429 without `Retry-After` waits 15, 30,
    then 60 s, and a `Retry-After` above 60 s fails at once; a TLS certificate failure is never
    retried; a 429 at the WebSocket upgrade is retried; wildcard transitions make before they
    break, accepting a brief duplicate rather than a gap
    ([section 13.3](#133-connection-and-subscription-protocol),
    [section 19](#19-wire-clients-and-the-spec-snapshot)).
31. **No hand-written schema anywhere.** There are no hand tick schemas, key maps, column
    overrides, or name lock; every change to a schema is a reviewed diff of a generated file
    ([section 19.4](#194-generated-schemas)).
32. **Dataset schemas are merged at run time, after the rename**, from the committed generated
    files and the catalog, cached per dataset; never inferred from data
    ([section 19.4](#194-generated-schemas)).
33. **Schemas are fixed before data arrives** (principle 11): a live table needs its full column
    set before its first row, the first message may omit optional fields, and two sessions must
    agree on a dataset's shape.
34. **Hand input is naming, grouping, and verification, never a column list.** It lives in
    `refresh/`, every entry carries a reason, and a stale entry fails the refresh. The
    page-to-prefix map is derived from the vendor's overview tables
    ([section 19.4](#194-generated-schemas)).
35. **The runtime drift policy** of [section 19.5](#195-runtime-drift): an extra field is
    dropped and reported; a missing required key, an unconvertible value, or a timestamp
    out of bounds fails loudly (a `poll()` table through `publish_failure`; on WebSocket only
    the affected topic's streams). The table never silently lies.
36. **One timestamp routine** for every transport: epoch 0 to null except in the time column,
    a checked multiply to nanoseconds, and the [1990, 2100] bound on epoch integers only
    ([section 12](#12-timestamp-units-and-day-boundaries)).
37. **An undocumented value of an interpreted field fails loudly.** An unknown `timeframe`
    value raises at the first stream verb for that class and nothing is cached
    ([section 13.2](#132-host-decision)).
38. **A never-acknowledged topic** triggers at most one reconnect per topic per session; later
    `NO_ACK`s only log and count ([section 13.3](#133-connection-and-subscription-protocol)).
39. **S3 follows the shared network rules:** the `net` CA bundle and proxy, botocore retries off
    in favour of the shared schedule, TLS never retried ([section 19.3](#193-the-flat-file-client)).
40. **REST rate limits and host resolution are shared.** A client-wide not-before instant after
    a 429, and single-flight host resolution through one future per (class, family)
    ([section 13.2](#132-host-decision), [section 19.1](#191-the-rest-client)).
41. **Test layout:** every path component drops one leading underscore, `__init__.py` maps to
    `test_init.py` and `__main__.py` to `test_main.py`, `_` directories under `tests/` are
    helpers, and `test_catalog.py` is a plain mirror ([section 4.2](#42-test-layout)).
42. **Import roots:** `pythonpath = ["."]` and `mypy_path = ["src"]`, fakes imported as
    `tests._fakes…`; ruff bans `import massive` once the dependency is gone.
43. **Module names:** `_namespaces/` (plain modules of namespace classes), `_flat_files.py`,
    `_history_source.py`, and the wire package `_massive/flatfiles/` (PEP 8 forbids the
    underscore in a package name).
44. **Release through trusted publishing, on the owner's go-ahead.** When the owner turns
    publishing on, `.github/workflows/release.yml` publishes to PyPI with trusted publishing
    (no long-lived token), and the first version is 0.1.0. Until then no publishing workflow,
    GitHub environment, or pending publisher exists. No rehearsal may push a tag that matches
    the project version, because a PyPI version can never be re-uploaded; the rehearsal method
    is chosen at the go-ahead ([section 20.1](#201-tooling-configuration)).

## 23. Open verification items

Each item is handled conservatively now, covered by a fixture test in its unit-test module, and
retired by a live integration test (`*_integration.py`, skipped without `MASSIVE_API_KEY`) in the
later live-testing work package. Each item that a live test refutes becomes an owner decision
before 1.0.

| # | Item | Conservative handling | Fixture test | Live integration test |
|---|---|---|---|---|
| 1 | Unit of REST forex `/v1/last_quote` `timestamp` and crypto `/v1/last` `timestamp` | Registered as ms (`unverified-ms`); the spec tags both `last.timestamp` fields `IMilliseconds`, which agrees; the overflow check and the [1990, 2100] bound raise `MassiveError` on a mismatch | `tests/test_timeunits.py`: ms and ns fixtures; the out-of-range path raises | `tests/test_timeunits_integration.py`: one request each; the converted `Instant` is within 1 day of now |
| 2 | Unit of WebSocket `FMV` `t` (documented ns, sample ms) | Magnitude rule for FMV only; logged once | `tests/test_timeunits.py`: both magnitudes convert correctly | `tests/namespaces/test_stocks_integration.py` (Business key): one FMV frame converts to a current `Instant` |
| 3 | Futures flat files keyed by session date | Check every block of every futures file against its key; fail loudly otherwise | `tests/massive/flatfiles/test_reader.py`: matching and mismatching fixtures | `tests/test_flat_files_integration.py` (S3 credentials): one key per venue |
| 4 | Indices `V` host (`business.` on the page; `socket.`/`delayed.` in the plan table) | `V` uses the class TICK host; a refusal names `ws_hosts[AssetClass.INDICES]` and `business.massive.com` | `tests/test_hosts.py`: both replies | `tests/test_hosts_integration.py`: subscribe `V.I:SPX` on the resolved host |
| 5 | TICK host for a base Business key | `business.massive.com` with a WARNING | `tests/test_hosts.py`: the snapshot shape with `fmv` and no trades/quotes resolves to `business.` | `tests/test_hosts_integration.py` (Business key): `AM.AAPL` is acknowledged on the resolved host |
| 6 | Futures snapshot `timeframe` fields (in the OpenAPI spec; absent from the documentation page) | Absence yields `socket.` with `Recency=UNKNOWN` and the explicit-verdict rule | `tests/test_hosts.py`: with and without the fields | `tests/test_hosts_integration.py`: record which fields the live payload carries |
| 7 | Exact text of the delayed-key verdict message ("real-time") | Case-insensitive substring match; limited to `Recency=UNKNOWN`, whole-step refusal, unpinned host | `tests/test_ws.py`: verdict and non-verdict messages | `tests/test_ws_integration.py` (delayed key): subscribe on `socket.` and capture the message |
| 8 | Unsubscribe acknowledgements | Not awaited; topics are removed immediately | `tests/test_ws.py`: removal without an acknowledgement | `tests/test_ws_integration.py`: record whether an acknowledgement arrives |
| 9 | Subscribe frame size limit | At most 1,000 topics per frame | `tests/test_ws.py`: 2,500 topics produce three frames | `tests/test_ws_integration.py`: a 1,000-topic frame is acknowledged |
| 10 | Options cap counted in contracts or topics | Topics (the stricter count) | `tests/test_ws.py`: `T.X` plus `Q.X` counts as two | `tests/test_ws_integration.py` (Options Advanced): 501 contracts on `T` and `Q` |
| 11 | Exact `max_connections` status text and timing | Any status `max_connections`, in the auth reply or after it, shows `WAITING_SLOT` | `tests/test_ws.py`: both placements | `tests/test_ws_integration.py`: two sessions with one individual key |
| 12 | `orjson` free-threaded wheels | Optional extra; standard-library codec by default | `tests/massive/net/test_codec.py`: codec selection with and without `orjson` importable | CI: the 3.14 job installs the extra (`--extra orjson`) and runs the suite with it; the 3.14t job does not, because `orjson` publishes no cp314t wheel |
| 13 | Flat-file publish time (documented "approximately 11:00 AM ET") | `flat_file_publish_cutoff` defaults to 11:30 ET | `tests/test_history_source.py`: AUTO before and after the cutoff | `tests/test_flat_files_integration.py`: yesterday's key exists after the cutoff |
| 14 | Whether the server delivers an event twice when a wildcard and an explicit topic of the same channel are both subscribed (`T.*` plus `T.AAPL`) | While a channel's acknowledged wildcard is desired, its explicit topics are off the wire; transitions make before they break, accepting a brief duplicate ([section 13.3](#133-connection-and-subscription-protocol)) | `tests/test_ws.py`: adding, refusing, and removing a wildcard over explicit topics sends the expected frames in the expected order | `tests/test_ws_integration.py`: subscribe both on a test key and record whether duplicates arrive |
| 15 | A missing flat-file key answers HTTP 404, not 403 `AccessDenied` (S3 answers 403 for a missing key when the credentials lack `ListBucket`, which would turn weekends into `EntitlementError`) | A 404 is a missing file; a 403 is an entitlement failure (section 16) | `tests/massive/flatfiles/test_client.py`: 404 and 403 replies | `tests/test_flat_files_integration.py` (S3 credentials): request a weekend key and record the status |
