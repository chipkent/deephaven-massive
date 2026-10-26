# deephaven-massive: Design Specification

This document is the approved design for the clean rework of `deephaven-massive`, a Python
library that exposes Massive.com (formerly Polygon.io) market data as live and static
[Deephaven](https://deephaven.io) tables. It is the source of truth for every implementation
work package in `PLAN.md`. It changes only by owner decision.

The design was checked against Massive's OpenAPI spec (`https://api.massive.com/openapi`) and
WebSocket spec (`https://api.massive.com/specs/websocket.json`) as of 2026-10-26,
`deephaven-core` 42.7 (Python package; Java behaviour from the `deephaven-core` `main` branch),
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
Deephaven server. It does not use Massive's Python SDK. It speaks Massive's
documented REST and WebSocket protocols directly, through its own REST client (on `urllib3`) and
its own WebSocket client (on `websockets`), and it generates its REST parameters and columns
from a pinned snapshot of Massive's OpenAPI spec. All of that lives in one private package,
`deephaven_massive._massive`, behind one small interface
([section 19](#19-wire-clients-and-the-spec-snapshot)). The runtime dependencies are
`deephaven-core`, `pyarrow`, `boto3` (flat files), `urllib3`, `websockets`, and `certifi`.

There are no existing users. Backward compatibility with the previous code base is not a goal.

## 2. Principles and terms

### 2.1 Principles

1. **Datasets are named by what they are, not how they travel.** One method per dataset. One
   schema per dataset kind: the union of the fields Massive documents across asset classes and
   transports (the REST fields of the OpenAPI spec, the WebSocket keys, and the flat-file
   headers), flattened to PascalCase. Column names come from the committed name lock
   ([section 19.3](#193-the-spec-snapshot-the-generator-and-the-name-lock)), never from an
   SDK or a field description. A column a class or transport does not supply is null (crypto
   trades have no `Tape`; stock bars have no `SettlementPrice`). `Timestamp` is the primary time
   column on every tick, bar, and value row. `Symbol` is the instrument on every instrument row.
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
8. **Direct over Massive's documented APIs, in one package.** The library owns one REST client
   and one WebSocket client, kept with the spec snapshot, the generator, and the name lock in
   the private package `_massive`, which imports nothing from Deephaven or the rest of the library
   and is reached only through its interface
   ([section 19](#19-wire-clients-and-the-spec-snapshot)). They implement Massive's
   documented protocols, and nothing works around a third-party client. REST parameters and
   columns are generated from a vendored, pinned snapshot of the OpenAPI spec; a parity test
   keeps the generated code in step with the snapshot, and an advisory weekly job reports
   drift in the live spec.
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
`request_id`, the `request_id` of Massive's response envelope (`str | None`). Users branch on
`status` (for example 404 or 429) instead of parsing messages. The exception set is unchanged.

`__init__.py` resolves these names lazily, through a module `__getattr__` (PEP 562), with the
eager imports under `TYPE_CHECKING` for type checkers. Importing a private submodule, such as
`deephaven_massive._catalog`, therefore never imports Deephaven
([section 4.1](#41-source-modules)).

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

| Module | Contents |
|---|---|
| `__init__.py` | The public names of [section 3](#3-exports), `__all__`, `__version__`, resolved lazily so that importing a private submodule imports no Deephaven module. |
| `_config.py` | `MassiveConfig` ([section 5](#5-massiveconfig)). |
| `_session.py` | `MassiveSession`: namespaces, `snapshots`, `release`, `close`, the registry lock, the reaper thread, the stream-id allocator ([section 6](#6-massivesession-and-lifecycle)). The host-decision cache belongs to the REST transport ([section 13.2](#132-host-decision)). |
| `_query.py` | The six query classes and their verbs ([section 7](#7-query-classes-and-verbs)). |
| `_enums.py` | The fourteen public enums and the internal `Recency`, `ChannelFamily`, and `Transport` enums; the `_require_enum` runtime check. |
| `_types.py` | `Symbols`, `ALL` and its type `_All`, `DateLike`, `FilterValue`, the `bars` overload aliases, and `override` (`typing_extensions.override` for the type checker, a no-op at run time, since `typing.override` needs Python 3.12). |
| `_errors.py` | `MassiveError` (with `status` and `request_id`), `AuthenticationError`, `EntitlementError`; `from_wire`, which turns a `WireError` into the public exception with the same message, `status`, and `request_id`. |
| `_massive/` | **The Massive wire package**: everything that talks to Massive or describes its wire format, behind one interface, `_massive/__init__.py` ([section 19](#19-wire-clients-and-the-spec-snapshot)). Subpackages `rest/` (the REST client), `ws/` (the WebSocket client), `spec/` (the vendored snapshot, the generator, and the generated REST table), and `names/` (the lexicons, the WebSocket key maps, and the name lock). It imports nothing from Deephaven or from the rest of the library. |
| `_catalog.py` | The dataset catalog. One frozen `DatasetSpec` per dataset: namespace, name, `query_kind` (a `QueryKind` member naming one of the six query classes, so a record needs no import of `_query.py`), channel and family, the REST **role paths** `fetch_path` (backs `fetch()` and `poll()`), `history_path` (backs REST `history()`), `grouped_daily_path`, and `extra_paths` (per-symbol variants such as `/v3/reference/options/contracts/{o}`), the `start`/`end` wire parameter pair, poll default and keys, flat-file prefixes, how symbols reach a REST request (`SymbolParam`: `ticker`, `tickers`, `ticker.any_of`, or path segments, where a forex or crypto pair fills `{from}` and `{to}`), the primary time key and fan-out key, overrides, and the page size. `has_rest_history` is `history_path is not None`, and `has_grouped_daily` likewise. `DatasetSpec.__post_init__` validates only the declaration, including that every wire name it declares is a parameter of its `RestPath`; **import never requires the dataset's lock entries** (the catalog test does). Also the aggregation of `_datasets/<domain>.py`, `dataset_wires()` (one `_massive.DatasetWire` per dataset, never reading the lock), and filter validation ([section 9](#9-filters)). Importable without Deephaven. |
| `_datasets/<domain>.py` | One module per domain module, each exporting `DATASETS: tuple[DatasetSpec, ...]`: the declarations only, importable without Deephaven, so `scripts/refresh_massive_spec.py` reads them without a JVM. |
| `_schema.py` | The schema registry: column types and the mapping from `_massive`'s wire types to them, PascalCase naming, the dtype override table (`ColumnOverride`, keyed by wire path), columnar Arrow construction, the four hand-written tick schemas (trades, quotes, bars, index values), and `dataset_schema(spec)`, which derives a dataset's schema lazily (cached) from its lock entries, their types included; verbs and the catalog test call it. Key maps live in `_massive`. |
| `_timeunits.py` | Timestamp conversion ([section 12](#12-timestamp-units-and-day-boundaries)): each field's unit comes from `_massive.wire_unit`; conversion to nanoseconds, the FMV magnitude rule, and the [1990, 2100] guard. |
| `_symbols.py` | Symbol normalization and the `Symbol`/`Underlying` rule ([section 10](#10-symbols-and-all)). |
| `_dates.py` | `DateLike` parsing, class day zones, session-date rules, `[start, end)` normalization. |
| `_liveness.py` | Result scopes, stream scopes, result records, the reaper queue and thread. |
| `_rest.py` | The REST transport: requests built from a `DatasetSpec` and the generated parameters, pages decoded into bounded columnar Arrow batches, 403/429 handling for `fetch()`, `history()`, and `poll()`. |
| `_hosts.py` | Host resolution from REST evidence ([section 13.2](#132-host-decision)). The `HostDecision` record itself lives in `_query.py`, next to the transport protocols that return it (`RestTransport.resolve_host`). |
| `_ws.py` | One connection per URL over `_massive.WsClient`: the reconcile loop over the client's decoded `Status` and data events, wildcard transitions, acknowledgement watchdogs, the explicit-verdict rule, the options contract cap. It parses no frame text. |
| `_stream.py` | Streams, per-stream publishers, the per-connection flush thread, shared-batch fan-out ([section 14](#14-publishers-fan-out-and-failure-semantics)). |
| `_watchlist.py` | Reactive symbol tables: the listener, the diff, and dispatch off the UG thread. |
| `_poll.py` | `poll()` tables: `REPLACE`, `UPSERT`, and `APPEND` modes, fan-out under `max_concurrency`. |
| `_sources.py` | Where `history()` reads from: partition dates, the REST capability check, and the `AUTO` choice ([section 16](#16-historysourceauto)). Pure: no I/O. |
| `_flatfiles.py` | S3 streaming of `.csv.gz` keys, pure-Arrow per-batch transforms, symbol pushdown, the missing-file rules ([section 16](#16-historysourceauto)). Pure S3 I/O: it never chooses a source. Its CSV header maps come from `_massive.flatfile_key_map`. |
| `_diagnostics.py` | The `connections()` and `streams()` input tables ([section 17](#17-diagnostics)). |
| `_domains/__init__.py` | Empty. |
| `_domains/base.py` | The shared dataset factory: builds a query from a catalog record, validates symbols and filters, applies the `bars` overload selection. |
| `_domains/stocks.py`, `options.py`, `futures.py`, `indices.py`, `forex.py`, `crypto.py`, `reference.py`, `indicators.py`, `economy.py`, `partners.py` | One namespace class per module ([section 8](#8-namespace-tree)), whose methods build queries from the matching `_datasets/<domain>.py` records. `partners.py` holds the four vendor classes. |

Supporting files outside the package:

| File | Purpose |
|---|---|
| `scripts/check.sh [fast\|full] [--no-fix]` | The single verification entry point (lint, format, type check; `full` adds the test suite with coverage). |
| `scripts/refresh_massive_spec.py` | A thin entry point that calls `deephaven_massive._massive.spec.generate.main()` with `_catalog.dataset_wires()` and, when S3 credentials are set, the bucket's top-level prefixes; no JVM. Modes `--check` (live drift report), `--fetch`, the plain offline regeneration, and `--verify` ([section 19.3](#193-the-spec-snapshot-the-generator-and-the-name-lock)). |
| `.github/workflows/spec-drift.yml` | The advisory weekly drift job ([section 20](#20-quality-gates)). |
| `tests/data/` | Payload fixtures. |
| `tests/_fakes/` | Fake REST transport, WebSocket server, and S3 opener used by the unit tests ([section 4.2](#42-test-layout)). |

Deephaven is imported at module level where it is used at runtime. Type-only imports go under
`TYPE_CHECKING`. The library imports nothing from `deephaven_server`.

**The Deephaven-free layer.** `_catalog.py`, `_datasets/`, `_enums.py`, `_errors.py`,
`_dates.py`, `_config.py`, and `_massive/` import no Deephaven module, directly or through
another module, and the package `__init__.py` resolves its public names lazily
([section 3](#3-exports)). So `from deephaven_massive import _catalog` and
`_catalog.dataset_wires()` work with no JVM. `tests/test_catalog.py` proves it in a
subprocess that starts no server and asserts that `deephaven` is not in `sys.modules`
afterwards.

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
| `_massive/__init__.py` | `tests/_massive/test_init.py` | |
| `_massive/rest/client.py` | `tests/_massive/rest/test_client.py` | |
| `_massive/spec/generate.py` | `tests/_massive/spec/test_generate.py` (also the parity test of `restspec.json`) | |
| `_domains/stocks.py` | `tests/_domains/test_stocks.py` | `tests/_domains/test_stocks_integration.py` |

Integration tests end in `_integration`. They are skipped unless `MASSIVE_API_KEY` is set and
run only in the later live-testing work package. The owner approved exactly these exceptions to
the mirror rule, and no others:

- `tests/conftest.py`: starts the embedded JVM in `pytest_configure`, before collection, and
  provides fixtures: thin fixtures that build the fake servers from `tests/_fakes/`, plus the
  table-to-Python and fixture-loading helpers. Test modules never import from it.
- `tests/test_layout.py`: walks `src/deephaven_massive/**/*.py` and asserts every module has its
  mirrored test file, and that every test file maps to a module or is one of these exceptions.
  It also holds the import-boundary test of the `_massive` package
  ([section 19](#19-wire-clients-and-the-spec-snapshot)).
- `tests/test_catalog.py`: asserts 100% Massive coverage against the vendored spec and the
  documentation index, with a reviewed exclusion list ([section 8.12](#812-excluded-endpoints)),
  and the name-lock completeness checks that need the declared datasets
  ([section 19.3](#193-the-spec-snapshot-the-generator-and-the-name-lock)). Under the naming rule
  it is also the mirror of `_catalog.py`, so the unit tests of the catalog records live in the
  same file.
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
- The REST client's retry policy, timeouts, and pool size, and the WebSocket client's handshake
  timeout and stable-uptime rule ([section 19](#19-wire-clients-and-the-spec-snapshot)).
- Proxies and CA certificates: both clients read the standard environment variables
  (`HTTPS_PROXY`, `NO_PROXY`, `SSL_CERT_FILE`, `SSL_CERT_DIR`), as section 19 specifies.

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
  A range that ends before it starts raises `ValueError` at construction, as `history()` does
  (two dates may be equal: whole days; two datetimes are half-open; a date and a datetime
  compare by day).
- `limit` is a total-row cap. Page size is internal. Every docstring says so. The cap is
  stored apart from the wire parameters, in `Request.limit`, so it can never be sent as the
  endpoint's page-size `limit`; the page size is `DatasetSpec.page_size`.
- The REST column names the path template as the OpenAPI spec writes it (the spec's own
  placeholder names, such as `{stocksTicker}`, are abbreviated here to `{t}`, `{o}`, `{u}`).
  Every path is in the generated REST table (`_massive.rest_path`,
  [section 19.3](#193-the-spec-snapshot-the-generator-and-the-name-lock)). Each dataset names
  its paths by role (`fetch_path`, `history_path`, `grouped_daily_path`, `extra_paths`).

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
250 tickers per request, any mix of asset classes. Rows are the union schema of the unified snapshot. `ALL` is not accepted here (the
endpoint requires tickers). Its catalog record is named `reference.snapshots` and is declared
in `_datasets/reference.py` with `asset_class=None` (each ticker's prefix selects its class,
§ 10), so `_catalog.datasets()` finds it; `m.streams()` and messages show that name.

### 8.12 Excluded endpoints

Each exclusion is an entry of `EXCLUSIONS` in `_massive/spec/allowlist.py`, the single list for
REST paths, WebSocket channels, and flat-file prefixes, each with its reason
([section 19.3](#193-the-spec-snapshot-the-generator-and-the-name-lock)).

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
| `/benzinga/v1/transcripts` | Never existed. |
| Single-ticker v2 snapshots `/v2/snapshot/.../tickers/{t}` | Covered by `snapshots(symbols)` with the `tickers` filter. |
| `/v3/snapshot/options/{u}/{o}` | Covered by `m.options.snapshots`; identical fields. |

**The allow-list.** The REST paths above are the generator's allow-list
([section 19.3](#193-the-spec-snapshot-the-generator-and-the-name-lock)). Each one is in the
vendored OpenAPI snapshot and in the vendored `llms.txt` index; the catalog test fails on an
allow-listed path missing from either, and on a documented path that is neither allow-listed nor
excluded above. `/fed/v1/funding-conditions` and `/v1/open-close/crypto/{from}/{to}/{date}` are
ordinary entries: every path reaches the same REST client in the same way.

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
dataset's paths (`_massive.rest_path`)
([section 19.3](#193-the-spec-snapshot-the-generator-and-the-name-lock)), which carry each
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
  `underlying_asset.ticker`) and on `related_companies` rows. Non-instrument fan-out keys use
  the column name the name lock gives the wire key (`filer_cik` becomes `FilerCik`).

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
`Literal` of strings, and that every enum that mirrors a parameter enum of the vendored
OpenAPI snapshot has the same value set as that parameter.

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
becomes an `Instant`; text without a UTC offset is read as UTC. A spec string field with
`format: date-time`, or with no format and a name ending in `_utc`, maps to `Instant`
automatically; one with `format: date`, or with no format and named `date` or `*_date`, maps to
`LocalDate`; any other text timestamp needs an explicit override.

**Column types.** `Instant` for timestamps, `LocalDate` for calendar dates, `double`, `long`,
`int`, `String`, `boolean`, and `int[]` for condition lists. Each union column has exactly one
dtype, fixed in the schema registry with a test per dataset: sequence numbers are `long`, sizes
are `double` (fractional sizes exist), condition codes are `int[]`, exchange ids are `int`.

## 12. Timestamp units and day boundaries

The unit of every timestamp-like wire field is a fact about Massive's wire format, so it lives in
the wire package: `_massive/spec/units.py`, keyed by `(transport, source, field)`, where the
source is a REST path template, a `(cluster path, channel)`, or a flat-file prefix, and read
through `_massive.wire_unit`. `_timeunits.py` converts: it refuses a field without a unit, and
it applies the FMV magnitude rule and the [1990, 2100] guard below. Building a dataset's schema
fails on any timestamp-like field without a unit, so the catalog test covers every REST field,
WebSocket key, and flat-file header a dataset uses. The table is complete for everything § 8
uses, so no dataset package edits it.

The table stays hand-verified. The OpenAPI spec tags some integer fields with a unit type in
`x-polygon-go-type` (`INanoseconds`, `IMicroseconds`, `IMilliseconds`, `IDaysPolygonDateString`),
but the tags are not reliable: all 20 `IMicroseconds` tags sit on indicator `timestamp` fields
whose descriptions say "Unix Msec", and the indicator responses carry milliseconds. The generator
copies each tag into the generated table as a cross-check only. A test inside the wire package
asserts that every tagged field has a unit in the table, and that a unit that differs from its
tag is listed, with its reason, in the reviewed conflict list beside the table; the indicator
`timestamp` fields are its first entries.

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
- `on_data(generation, channel, rows)`: one channel's data rows from one frame, keyed by column
  name, symbols already in Massive's REST form.
- `on_state(state, detail)`: `CONNECTING`, `RECONNECTING` (with the next attempt's time),
  `WAITING_SLOT` (the auth reply was `max_connections`), `FAILED` (with the `WireError`), or
  `CLOSED` (after `close()`).

`_ws.py` parses no frame text. It calls the client's loop-thread methods
`subscribe(generation, topics)` and `unsubscribe(generation, topics)`. Each sends at once, in
call order, at most 1,000 topics per frame (the frame-size limit is **UNVERIFIED**; the chunk
size is conservative). A call for a generation that is no longer current is discarded: the next
generation re-sends the whole desired set. `reconnect()` closes the current socket so the client
reconnects under its backoff rules. Rows from `on_data` go to the per-channel row buffer.

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
| Classify a failed upgrade as fatal or retryable (section 19.2) | client; a fatal one is reported as `FAILED` with a `WireHandshakeError`, which the library turns into a `MassiveError` naming the HTTP status and the host |
| Decode frames into `Status` and data rows; apply the key maps; normalize wire symbols | client |
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
   -> topic state NO_ACK; ERROR logged; client.reconnect(), so the backoff rules apply;
      streams are not failed
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
(`MassiveError` with the HTTP status and host), and unrecoverable library errors such as a
schema mismatch. `max_connections`, closes, and `NO_ACK` are diagnostic states.

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
  60 s; a `Retry-After` above 60 s fails at once); if it persists it raises
  `MassiveError("rate limited ...")` with `status=429`. The REST transport (`_rest.py`, not the
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
- **`poll()` cycles.** A cycle that raises `EntitlementError` or `AuthenticationError` fails the
  table through `publish_failure`. Any other error is logged at ERROR, sets the stream's `Error`
  in `m.streams()`, keeps the previous rows, and the next cycle runs as scheduled; it never
  fails the table.
- **Request parameters.** Besides the path, symbols, axis bounds, and `filters`, the transport
  sends the typed `Request.adjusted` as the `adjusted` query parameter (`true` or `false`) on
  every path whose `RestPath` lists it.
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
`HistoryQuery.history()` makes the choice (`_sources.choose_source`) and then calls one
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

Flat files are streamed: `pyarrow.csv.open_csv` reads each `.csv.gz` key in 1 MiB blocks (gzip
only: every documented key is `.csv.gz`). The CSV reader parses each header straight to its
column's type (epoch integers for `Instant`s, text for lists), and only an empty field is null:
`NA` and `NULL` are text, since both are valid tickers. Each block then goes through pure Arrow
operations in this order: the futures session check on the whole block, the `Symbol` filter
(`pyarrow.compute.is_in`), the half-open `[start, end)` filter on `Timestamp` (not applied to
date-bounded futures, whose files hold exactly the requested sessions), and, on the surviving
rows only, the rename to PascalCase, unit conversion, and parsing of the list columns
(`Conditions`, `Indicators`). No block is converted to Python objects. A file's surviving rows
become one static Deephaven table per 16 blocks of data and at the end of the file (usually one
table per file, since each table carries a fixed JVM-heap cost), and the tables are merged.

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
| WS client, one per connection | library | `asyncio.run` of the connection's `WsClient` (section 19.2): socket, handshake, frames, backoff; decode each frame once; route status frames; append rows to per-channel `SimpleQueue`s; the reconcile loop and watchdogs; idle close | any JVM call; blocking waits |
| flush, one per connection | library | every `flush_interval`: drain queues, Arrow to Java once per channel, `where_in` and `publisher.add` per stream; enqueue diagnostics row snapshots | touch loop-owned state directly |
| poller, one per `poll()` table, plus a `ThreadPoolExecutor(max_concurrency)` for fan-out | library | REST calls; `Table.snapshot()` of a reactive symbol table; `publisher.add` or `input_table` operations | run inside a listener |
| reaper, one per session | library | `scope.release()`; stream held-result counts | anything else |
| diagnostics writer, one per session | library | applies row snapshots from its queue to the `connections()` and `streams()` input tables: each batch coalesced to the latest row per key, one `add` per table | anything else |
| UG thread (Deephaven) | Deephaven | watchlist listeners (`listen`): compute symbol adds and removes and hand them to a worker | Deephaven table operations; blocking I/O; waiting on library threads |
| Java-originated (shutdown callback, UI export release) | Deephaven | `SessionCore.stream_shut_down` (under the registry lock: unmaps the key; deletes the record, or marks it for the last hold to delete); `call_soon_threadsafe` | sends; JVM calls; blocking |

### 18.2 Shared state and ownership

- `SessionCore.registry_lock` (a `threading.RLock`) guards every mutable registry: streams
  by key, results by id, connections by URL, topic reference counts. (Host decisions are
  cached by the REST transport under its own leaf lock, § 13.2.) It is a
  **leaf lock**: no Deephaven call, no network call, no blocking wait, and no wait on a
  `call_soon_threadsafe` result while holding it. Per-stream state is a frozen dataclass replaced
  atomically under the lock; readers copy references out and release the lock before working.
- Per-connection mutable state (desired, acknowledged, and pending sets; the in-flight step;
  watchdogs; the `WsClient` and its socket) is **loop-confined**: only the connection's client
  thread touches it. Other threads reach it through `loop.call_soon_threadsafe` with plain data
  (tuples of topic strings), and through `WsClient.close()`, which any thread may call: it only
  sets an `Event` and schedules a cancel on the loop (section 19.2). Nothing reads loop state
  across threads; the loop thread enqueues diagnostics row snapshots on the diagnostics writer's
  queue, and the writer is the only thread that writes the diagnostics tables.
- The session's `RestClient` is shared by every thread that makes REST calls. It is safe per
  call: its only shared mutable state is urllib3's internally locked pool (section 19.1). The
  once-per-session rate-limit WARNING belongs to the REST transport, behind a `threading.Event`.
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
`py3-none-any`; `websockets 17.3` ships cp314t wheels; `urllib3` and `certifi` are pure Python. `orjson 3.13.0` publishes no free-threaded wheels, so `orjson` is an optional extra and
the standard-library codec is the default. `PythonDeephavenSession.setVariable` carries the
comment "relying on the GIL being kind to us"; the library never mutates script globals, so this
does not affect it, but it is why the Deephaven integration tests must run on the free-threaded
builds rather than unit tests alone.

## 19. Wire clients and the spec snapshot

The library talks to Massive through two clients of its own and describes Massive's REST surface
with data generated from a pinned snapshot of Massive's OpenAPI spec. It does not depend on
Massive's Python SDK (`massive`), and nothing here works around a third-party client: each rule
below implements Massive's documented protocol or a requirement of this design.

**One package.** Everything that talks to Massive's REST or WebSocket API or describes its wire
format lives in one private package, `deephaven_massive._massive`, the **wire package**, split
into four subpackages along the boundaries below. Reading flat files from S3 stays in
`_flatfiles.py`; their wire format (the CSV header maps) is here.

```
src/deephaven_massive/_massive/
├── __init__.py        the interface: the only module the rest of the library imports
├── errors.py          WireError and its subclasses
├── net.py             shared plumbing: CA bundle, proxy settings, User-Agent, the JSON codec
├── rest/              the REST client (section 19.1)
│   ├── __init__.py    RestClient, Page
│   ├── client.py      the pool, authentication, retries, next_url paging
│   ├── query.py       path and query encoding (exact datetime nanoseconds, .any_of lists)
│   ├── response.py    strict decoding, envelopes, result keys, the error mapping
│   └── extract.py     wire-path extraction: decoded rows to one list per wire path
├── ws/                the WebSocket client (section 19.2)
│   ├── __init__.py    WsClient, WsState, Status, StatusKind
│   ├── client.py      the loop thread, the connect-and-reconnect task, the sender, close
│   ├── protocol.py    handshake and auth frames, subscribe frames, frame decoding into status
│   │                  and data events, wire-symbol normalization, failure classification
│   └── backoff.py     the backoff policy and the stable-uptime reset
├── spec/              the spec snapshot and the generator (section 19.3)
│   ├── __init__.py    rest_path, rest_paths, wire_unit, unused_allowlisted, the cross-check
│   │                  readers, and the re-exported value types
│   ├── types.py       the value types RestPath, Param, Field, ParamKind, WireType, WireUnit,
│   │                  DatasetWire: frozen, standard library only, safe at run time
│   ├── allowlist.py   the path allow-list, the non-`results` result keys, the type overrides
│   ├── units.py       the hand-verified wire-field unit table and its unit-tag conflict list
│   ├── drift.py       the semantic comparison of a live fetch with the snapshot
│   ├── generate.py    the generator and the refresh command's main(); imports types.py;
│   │                  never imported at run time
│   ├── restspec.json  generated: the REST table, canonical JSON
│   └── snapshot/      data (source tree only): openapi.json, websocket.json, llms.txt, the
│                      flat-file documentation pages, manifest.json
└── names/             column naming (section 19.3)
    ├── __init__.py    column_names, ws_key_map, flatfile_key_map
    ├── lexicon.py     the record families, their object-shape assignments, the lexicons,
    │                  the identity entries, the WebSocket-only names
    ├── wskeys.py      the WebSocket key maps, one per (cluster path, channel)
    ├── flatkeys.py    the flat-file key maps, one per prefix
    ├── lock.py        loading, appending, and comparing the lock; ALIASES; BREAKING_RENAMES
    └── locks/         data: <domain>.json, one append-only lock per domain module
```

**The interface.** `_massive/__init__.py` defines `__all__` and re-exports exactly these names:

| Name | What it is |
|---|---|
| `RestClient`, `Page` | The REST client and one decoded page (section 19.1). |
| `extract(rows, fields)` | One Python list per wire path, read from decoded rows by the generated field paths; the only code that walks a wire path. |
| `WsClient`, `WsState`, `Status`, `StatusKind` | The WebSocket client, the states it reports, and the status events it decodes (section 19.2). |
| `WireError`, `WireAuthError`, `WireEntitlementError`, `WireRateLimitError`, `WireHandshakeError` | What the clients raise or report. Each carries the HTTP status (or `None`), the host, the path, the `request_id` (or `None`), and the server's message. `_errors.from_wire` turns one into the public exception with the same message, `status`, and `request_id` ([section 3](#3-exports)). |
| `rest_path(template) -> RestPath`, `rest_paths()` | The generated REST table: per path template, its result key, its parameters (`Param`: wire name, `ParamKind`, enum values, path or query), and its response fields (`Field`: wire path, `WireType`, unit tag). |
| `ParamKind`, `WireType` | Parameter kinds and wire types (`long`, `double`, `bool`, `string`, `date`, `date-time`, `int-array`, `json-text`). `_schema.py` maps each `WireType` to a column type; `_massive` knows no Deephaven type. |
| `wire_unit(transport, source, field) -> WireUnit`, `WireUnit` | The unit of one timestamp-like wire field (`ns`, `us`, `ms`, `s`, the FMV magnitude rule, or `unverified-ms`), where `source` is a path template, a `(cluster path, channel)`, or a flat-file prefix. `_timeunits.py` converts. |
| `column_names(domain)` | The name lock of one domain: `(dataset, transport, wire path) -> (column, WireType)`, with transports named by the strings `rest`, `ws`, and `flat_files`. |
| `ws_key_map(cluster, channel)`, `flatfile_key_map(prefix)` | The key map of one WebSocket channel or flat-file prefix: wire key or CSV header to `(column, WireType)`, plus the keys deliberately dropped. |
| `unused_allowlisted(wires, namespace=None)` | The allow-listed paths, channels, and prefixes no declared dataset uses ([section 19.3](#193-the-spec-snapshot-the-generator-and-the-name-lock)). |
| `DatasetWire` | The plain record of one dataset's wire facts: name, domain, role paths, primary time key, fan-out key, WebSocket channels, flat-file prefixes. `_catalog.dataset_wires()` builds them. |
| `spec_paths()`, `documented_paths()`, `ws_channels()`, `ws_channel_keys(cluster, channel)`, `flatfile_headers(prefix)` | Read the vendored snapshot for the catalog's cross-checks. They read the source tree; in an installed wheel, which excludes the snapshot, they raise `WireError` naming the missing file. |

Each name is documented in its module to the project's docstring standard. The clients take
plain arguments (key, base URL, timeouts, backoff), never `MassiveConfig`, so they are tested
alone against the fake servers of `tests/_fakes/`.

**The boundary**, enforced by an import-boundary test in `tests/test_layout.py` that reads every
module with `ast`, resolving relative imports:

- Inside `_massive/`, a module imports only the standard library, `urllib3`, `websockets`,
  `certifi`, the optional `orjson`, and other `_massive` modules. It never imports `deephaven`,
  `deephaven_server`, `jpy`, `pyarrow`, or any `deephaven_massive` module outside `_massive`.
- Outside `_massive/` (the rest of `src/`, and `tests/` outside `tests/_massive/` and
  `tests/_fakes/`), no module imports `urllib3`, `websockets`, or `certifi`; no module imports a
  `_massive` submodule, directly, relatively, or through `importlib.import_module` or
  `__import__` with a literal name; an attribute read on the package (`_massive.X`) names only a
  member of `_massive.__all__`; and no module names a snapshot file (`openapi.json`,
  `websocket.json`, `llms.txt`) or the `snapshot/` directory.
- The single exception is `scripts/refresh_massive_spec.py`, which calls
  `deephaven_massive._massive.spec.generate.main()`.
- A run-time check: after importing `deephaven_massive` and every `_domains` module, neither
  `deephaven_massive._massive.spec.generate` nor `deephaven_massive._massive.spec.drift` is in
  `sys.modules`.

Tests mirror the package under `tests/_massive/` (`tests/_massive/rest/test_client.py`,
`tests/_massive/spec/test_generate.py`, and so on), by the rule of
[section 4.2](#42-test-layout).

**The snapshot is not shipped.** `pyproject.toml` excludes it from the wheel:

```toml
[tool.hatch.build.targets.wheel]
exclude = ["src/deephaven_massive/_massive/spec/snapshot"]
```

The sdist keeps it. Nothing reads it at run time; the tests, the refresh command, and the drift
job read it from the source tree. `restspec.json` and the locks are package data and ship.

### 19.1 The REST client

`_massive.RestClient` is one object per session, created on first use and closed by
`m.close()` (a session closer). It is **safe to call from any number of threads at once**: its
only shared mutable state is urllib3's connection pool, which locks internally. Every call is
independent; no call mutates an argument (the parameters are copied before they are encoded).

```python
client.get(rest_path, path_params, query_params) -> Page             # one request
client.pages(rest_path, path_params, query_params, *, limit=None) -> Iterator[Page]
                                                                    # follows next_url
Page(status: str, request_id: str | None, rows: list[dict], body: dict | list)
```

`rest_path` is the path's `RestPath` from the generated table; the client reads its template,
result key, and parameter kinds from that value and imports no generated data, so its tests
build `RestPath` values by hand.

- **Transport.** One `urllib3.PoolManager` (or `ProxyManager`, below) with `retries=False` (the
  client retries itself), a 10 s connect and 30 s read timeout, `maxsize` at least
  `max_concurrency` so fan-out never discards connections, and `Accept-Encoding: gzip`
  (urllib3 decodes it). `User-Agent: deephaven-massive/<version>`.
- **Authentication.** `Authorization: Bearer <key>` on every request. The key never appears in
  a URL: an `apiKey` parameter in a `next_url` is removed before the request.
- **Proxies and certificates.** The proxy comes from the standard environment, read once at
  construction: `urllib.request.getproxies()` for `HTTPS_PROXY` and `urllib.request.proxy_bypass`
  for `NO_PROXY` select a `ProxyManager` for the REST host or a plain `PoolManager`. The CA
  bundle is the system default when `SSL_CERT_FILE` or `SSL_CERT_DIR` is set (an
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
- **Retries.** At most 3 retries (4 attempts). Every request is a `GET`, so a retry is always
  safe. Retried, with these waits:
  - HTTP 500, 502, 503, 504 and connection errors (refused, reset, timed out): 0.5, 1, then
    2 s, each with up to 20% upward jitter;
  - HTTP 429: the `Retry-After` value in either form (delta-seconds or HTTP-date) when present,
    otherwise 15, 30, then 60 s. A `Retry-After` above 60 s raises `WireRateLimitError` at once,
    naming the requested wait.

  A TLS certificate failure (`ssl.SSLCertVerificationError`) is never retried: it raises
  `WireError` at once, naming `SSL_CERT_FILE`. A connection reset in the middle of paging
  retries **that page** (its `next_url`), never the first page, so a long history neither
  restarts nor skips.
- **Error mapping.** After the retries, the client raises: 401 as `WireAuthError`; 403, and a
  200 envelope with `status` `NOT_AUTHORIZED`, as `WireEntitlementError`; 429 as
  `WireRateLimitError`; every other non-200 status, a 200 envelope with `status` `ERROR`, and a
  connection failure as `WireError`. Each carries the status, the method and path (query without
  secrets), the `request_id`, and the first 200 characters of the server's `message` or body,
  and its message names them all. The REST transport turns each into the public exception of
  [section 13.4](#134-rest) with `_errors.from_wire`.
- **Strict decoding.** The body is decoded once with the JSON codec of `_massive/net.py`
  (`orjson` when it is installed, logged once at INFO, otherwise the standard library). Bytes
  that are not UTF-8, or text that is not JSON, raise `WireError` with the status and the first
  200 bytes, on any call and on any page. The page's rows come from the `RestPath`'s result key
  (`results` for most paths; one of the seven others, such as `last` or `ticker`, where the spec
  says so). A single object is wrapped as one row; a bare JSON list
  (`/v1/marketstatus/upcoming`) is the rows, with `status=""` and `request_id=None`. A missing
  result key is an empty page only when `status` is `OK` or `DELAYED`. The envelope's `status`
  and `request_id` are kept on the `Page`, so callers can read `DELAYED`.
- **Logging.** Each request logs at DEBUG: method, path, query (with no secret), status,
  `request_id`, row count, and elapsed time. Retries log at INFO with the wait, and exhausted
  retries at ERROR. The `Authorization` header is never logged.

The REST transport (`_rest.py`) turns pages into columns with `_massive.extract(page.rows,
fields)`, which returns one Python list per wire path the dataset's schema names, and builds one
Arrow array per column. No row object or model class sits between the JSON and Arrow, and no
code outside `_massive` walks a wire path.

### 19.2 The WebSocket client

`_massive.WsClient` is one object per connection (one `wss://host/path`), created by `_ws.py`
and run on its own daemon thread under `asyncio.run`. Its interface and callbacks are those of
[section 13.3](#133-connection-and-subscription-protocol). **Apart from `start()` (called once by
its creator, before the loop thread exists), `close()`, and `join(timeout)`, every method must be
called on the client's loop thread.** All its state is confined to that thread except the
`close()` request flag (a `threading.Event`), and `close()` touches nothing else: it hands its
work to the loop. No other client state is ever reached from two threads, which is what makes
the design correct on free-threaded 3.14t without locks.

- **Connecting.** `websockets.asyncio.client.connect(url, ssl=..., max_size=None,
  open_timeout=None, user_agent_header="deephaven-massive/<version>")`, the whole handshake
  (below) running under `asyncio.timeout(10)`. `max_size=None` lifts websockets' 1 MiB frame
  cap; keepalive pings stay at websockets' defaults; the proxy comes from the environment
  through websockets' own proxy support; the CA bundle follows the REST rule of
  [section 19.1](#191-the-rest-client).
- **Handshake.** Read the `connected` status, send `{"action":"auth","params":"<key>"}`, and read
  the reply. The upgrade, the `connected` status, and the reply together must finish within the
  fixed 10 s handshake timeout, or the attempt fails and is retried. `auth_success` opens a
  generation (`on_open`). `auth_failed` is fatal (`WireAuthError`). `max_connections` reports
  `WAITING_SLOT`, and the attempt ends when the server closes the socket (or after the
  handshake timeout), then waits its backoff. The auth frame is never logged.
- **Receiving.** After `auth_success`, `protocol.py` decodes each text frame once, with the JSON
  codec of `_massive/net.py`, into events delivered in order:
  - `on_status(generation, Status)` for each `ev == "status"` object. `Status.kind` is `ACK`
    (`topics` parsed from "subscribed to: <p1>[,<p2>...]", cluster-agnostic, a comma list
    tolerated; empty when the message cannot be parsed), `REFUSED` (`topics` holds the topic the
    message names, or is empty when it names none), `MAX_CONNECTIONS`, or `OTHER`. `message` is
    the server's text verbatim, and `realtime_verdict` is true when a refusal's message contains
    "real-time" (case-insensitive).
  - `on_data(generation, channel, rows)` for each channel's data objects in the frame, each row
    keyed by **column name** through `ws_key_map`, with the wire symbol normalized to Massive's
    REST form (`EUR/USD` to `C:EURUSD`, `BTC-USD` to `X:BTCUSD`) and the forex `x` given as a
    quoted string turned into an integer. A key the map neither names nor drops is counted and
    logged once at WARNING.

  No frame text is parsed outside `_massive`.
- **Sending.** `subscribe` and `unsubscribe` put their frames
  (`{"action":"subscribe","params":"T.AAPL,Q.AAPL"}`, at most 1,000 topics each) on one sender
  coroutine's queue, which sends them in order as soon as it runs: no change waits for a frame
  to arrive. Frames for a generation that is no longer current are dropped.
- **Reconnecting.** After every close and every failed attempt that `close()` did not request
  and that is not fatal (a server's clean close included), the client waits and tries again,
  forever. The first wait is `reconnect_backoff`; each further consecutive failure doubles it up
  to `max_reconnect_backoff`; jitter adds up to 20%, clamped, so at the cap the wait is drawn
  from `[max(reconnect_backoff, max_reconnect_backoff / 1.2), max_reconnect_backoff]` and every
  wait lies in `[reconnect_backoff, max_reconnect_backoff]`. The failure count and the backoff
  reset only after a generation has stayed open for 60 s (stable uptime), never on
  `auth_success` alone, so a server that accepts and then drops at once still backs off. The
  client reports `RECONNECTING` with the next attempt's time before each wait.
- **Fatal and retryable failures.** `protocol.py` classifies an exception from `connect()`:
  - an HTTP 429 at the upgrade is retryable, and its wait is the larger of its `Retry-After`
    and the backoff;
  - a TLS certificate failure (`ssl.SSLCertVerificationError`) is fatal, naming
    `SSL_CERT_FILE`, although it is an `OSError`;
  - everything else follows websockets' public rule, `websockets.client.process_exception`:
    what it returns `None` for (EOF, `OSError`, timeouts, HTTP 500/502/503/504) is retried, and
    anything it returns (for example HTTP 400 or 404, an invalid URI) is fatal.

  A fatal error ends the client and is reported as `FAILED` with a `WireHandshakeError` naming
  the HTTP status and the host.
- **Closing.** `close()` sets the request flag and calls `loop.call_soon_threadsafe(task.cancel)`
  (a no-op once the loop has ended). Cancellation interrupts whatever the task awaits, a backoff
  wait included; a `finally` block closes an open socket with code 1000 and reports `CLOSED`.
  `close()` never waits; `join(timeout)` waits for the thread with a bound.
- **Logging.** INFO on connect, authentication, and close; WARNING on `max_connections` and on
  each reconnect wait (with its length); ERROR on a fatal error.

### 19.3 The spec snapshot, the generator, and the name lock

**Sources of truth.** The fields of a REST response, the parameters of a REST path, and their
types come from Massive's OpenAPI spec, `https://api.massive.com/openapi`, vendored and pinned
in `_massive/spec/snapshot/` (never `https://api.massive.com/specs/rest.json`, a stale 33-path
subset). The documentation is rendered from the same spec, so it adds no naming information.
The other vendored sources are cross-checks only:

| Source | Use |
|---|---|
| `openapi.json` (`/openapi`) | REST paths, parameters, response fields, types, formats, enums, unit tags. The generator's only input. |
| `websocket.json` (`/specs/websocket.json`, OpenAPI 3.0.3) | A completeness check over the WebSocket key maps: every key of every channel a dataset uses is mapped or listed as dropped, with a reason. |
| `llms.txt` (`https://massive.com/docs/llms.txt`) | A check of the path allow-list: every allow-listed path is documented, and every documented path is allow-listed or excluded ([section 8.12](#812-excluded-endpoints)). |
| The flat-file documentation pages | A cross-check of the flat-file key maps against the documented CSV headers (Massive publishes no machine-readable flat-file spec). |

**Hand-maintained wire data**, reviewed like code:

- `_massive/spec/allowlist.py`: the allow-list (every REST path of § 8), the seven non-`results`
  result keys, the type overrides, and `EXCLUSIONS`, the **single exclusion list** of
  [section 8.12](#812-excluded-endpoints): every documented REST path, WebSocket channel, and
  flat-file prefix the library does not support, each entry `(kind, item, reason)` with
  `kind` one of `rest`, `ws`, `flat_files`.
- `_massive/spec/units.py`: the unit of every timestamp-like wire field the library reads,
  keyed by `(transport, source, field)`, verified against the documentation as § 12 describes,
  and the reviewed list of fields whose verified unit differs from the spec's
  `x-polygon-go-type` tag (the 20 indicator `IMicroseconds` tags first).
- `_massive/names/lexicon.py`, `wskeys.py`, `flatkeys.py`: the lexicons, the WebSocket key maps,
  and the flat-file key maps (each key with its column and `WireType`).
- `_massive/names/lock.py`: `ALIASES` and `BREAKING_RENAMES` (below).

They are complete for every path, channel, and prefix that § 8 uses, so a dataset package never
edits them.

**Everything documented is accounted for.** Every documented REST path (in `openapi.json` or
`llms.txt`), every WebSocket channel (in `websocket.json`), and every flat-file prefix (a
flat-file page in `llms.txt`, each mapped to its prefix in `flatkeys.py`) is **accounted for**:
either allow-listed (a path in `allowlist.py`, a channel in `wskeys.py`, a prefix in
`flatkeys.py`, each entry naming the § 8 namespace that owns it) or on `EXCLUSIONS`. The union
lives entirely in the wire package, so the check needs no declared dataset:
`tests/_massive/spec/test_allowlist.py` enforces it against the pinned snapshot, and
`drift.py` and `--check` use the same union, so drift always shows up as **unaccounted items**,
never as silence.

**Every allow-listed item is used.** A separate check, `_massive.unused_allowlisted(wires,
namespace=None)`, returns the allow-listed paths, channels, and prefixes that no declared
dataset (`DatasetWire`) uses, optionally for one namespace. It is tested on synthetic
declarations from the start, is required to return nothing for a namespace once that
namespace's dataset package has merged, and becomes strict for the whole catalog when the
rework finishes; until then empty namespaces are expected.

**The refresh command.** `scripts/refresh_massive_spec.py` is a thin entry point: it calls
`deephaven_massive._massive.spec.generate.main(argv, _catalog.dataset_wires(), s3_prefixes)`.
No JVM is involved: the catalog is importable without Deephaven
([section 4.1](#41-source-modules)). `s3_prefixes` is `None` unless the S3 credentials of
[section 5](#5-massiveconfig) are set in the environment, in which case the script lists the
bucket's top-level prefixes with one `boto3` `list_objects_v2(Delimiter="/")` call (`boto3` is
outside `_massive`'s allowed imports, so the script does it). Its modes:

| Mode | Network | What it does |
|---|---|---|
| `--check` | yes | Downloads the live `openapi.json`, `websocket.json`, `llms.txt`, and flat-file pages into a temporary directory and runs the semantic comparison of `_massive/spec/drift.py`. Against the vendored snapshot it reports REST paths new, removed, or newly deprecated; response fields and parameters added, removed, or retyped; WebSocket channels and keys added or removed; CSV headers added, removed, or renamed in a known flat file's page; and the keys the naming gate would stop on. Against the allow-list and `EXCLUSIONS` it reports **unaccounted items**: documented REST endpoints (spec or `llms.txt`), WebSocket channels, and flat-file pages that are neither allow-listed nor excluded, plus, when `s3_prefixes` is given, bucket prefixes that no `flatkeys.py` entry or exclusion names (CI has no S3 credentials, so there it relies on the pages). JSON is compared canonically (`sort_keys`) and text normalized, so reordering and volatile fields are never reported. It prints a report, changes nothing, and exits non-zero when it finds drift or unaccounted items. |
| `--fetch` | yes | Replaces the vendored snapshot with the live sources and records their fetch date and SHA-256 in `manifest.json`. |
| (none) | no | Regenerates `restspec.json` from the vendored snapshot and appends new name-lock entries. |
| `--verify` | no | Exits non-zero when the regeneration would change anything (the parity test runs the same comparison). |

**The generator.** For each allow-listed path it:

1. fails, naming the path and the datasets that use it, when the path is missing from the spec
   or marked `x-polygon-ignore`, `x-polygon-draft`, or `deprecated`, so an endpoint Massive
   retires never drops out silently;
2. merges `allOf` and flattens nested objects into dotted wire paths below the path's result
   key;
3. maps each leaf to a `WireType`: `integer` to `long`, `number` to `double`, `boolean`,
   `string` (with the format and name rules of [section 11](#11-enums-and-types)), and an array
   of integers to `int-array`; a leaf with a unit tag keeps it for the cross-check of
   [section 12](#12-timestamp-units-and-day-boundaries);
4. records each parameter: wire name, kind, enum values, and whether it is a path segment.

It writes `restspec.json` canonically (`sort_keys=True`, a one-space indent, a trailing newline),
which `_massive/spec/__init__.py` loads once into frozen dataclasses. Ruff and mypy never see the
generated data, so a formatter release cannot break parity. The generator never guesses. A
construct it has no rule for (`$ref`, `oneOf`, `anyOf`, an untyped leaf, an array of strings or
objects, a free-form map, `number` with an integer format) fails the run with its path, unless
an entry in the override table gives the leaf a type or drops it. An override that names a path
the spec lacks also fails the run. Arrays and maps that a dataset keeps become `json-text`
through such an override.

**The naming rule.** A column name is fixed once per `(dataset, transport, wire path)` and
recorded in the name lock. For a new REST entry the generator proposes it by this rule:

1. Split the wire path into segments and drop the result key (`results[]`, `tickers[]`,
   `ticker`, `last`).
2. Apply the library-wide names: the row's instrument key (`ticker`, `T`, `sym`) becomes
   `Symbol`; the dataset's primary time key (declared in its `DatasetSpec` for tick, bar, and
   value rows) becomes `Timestamp`; the dataset's fan-out key becomes `Underlying`
   ([section 10](#10-symbols-and-all)).
3. Translate an abbreviated key through the lexicon of its **record family**: bar, trade, NBBO
   quote, forex quote, or ticker, assigned per object shape in `lexicon.py` (the row of an
   aggregates response, the snapshot objects `day`, `min`, `prevDay`, `lastTrade`, `lastQuote`,
   and so on). Each lexicon value is Massive's own long-form name for the field, as v3 REST,
   futures v1, and the flat-file headers write it: `o` is `open`, `vw` is `vwap`, `n` is
   `transactions`, `q` is `sequence_number`, `y` is `participant_timestamp`, `f` is
   `trf_timestamp`, `P` is `ask_price`, `X` is `ask_exchange`, `ds` is `decimal_size`, and so on.
   The snapshot object `min` is named `minute`, so its columns are `MinuteOpen`, `MinuteVwap`,
   and so on (`Min…` would read as "minimum").
4. Keep every other segment as it is, and PascalCase-join the segments (`prevDay.vw` becomes
   `PrevDayVwap`, `lastQuote.P` becomes `LastQuoteAskPrice`).

The refresh **fails instead of guessing** in two cases: a key of three characters or fewer, with
no underscore, that has no lexicon entry (short real words and acronyms such as `id`, `cik`,
`url`, `low`, `fmv`, `mic` have explicit identity entries); and two wire keys of one row that
produce the same column. The failure prints the key, its path, its description, and a
suggestion: a readable key elsewhere in the spec with the identical description. The suggestion
is for the reviewer; **names are never derived from descriptions**, which Massive edits about
twenty times as often as it changes keys.

WebSocket names come from the key maps in `wskeys.py`, one per (cluster path, channel), which use
the same long-form vocabulary. The WebSocket-only concepts, which have no long form at Massive,
are named `av` → `AccumulatedVolume`, `op` → `OfficialOpenPrice`, aggregate `a` → `DayVwap`,
aggregate `z` → `AverageTradeSize`, and `dav` → `AccumulatedDollarVolume`. Flat-file names come
from `flatkeys.py`; the headers are already long-form.

**The name lock.** `_massive/names/locks/<domain>.json`, one file per domain module, records for
every transport `(dataset, transport, wire path) -> (column, WireType)`:

```json
{"stocks.trades": {"flat_files": {"price": {"column": "Price", "type": "double"}},
                   "rest": {"price": {"column": "Price", "type": "double"}},
                   "ws": {"p": {"column": "Price", "type": "double"}}}}
```

It is written canonically (sorted keys, a one-space indent, a trailing newline), so every refresh
diff is a pure addition. REST entries are appended from the rule above; WebSocket and flat-file
entries are copied from the key maps. A dataset's schema is derived from its lock entries,
column types included ([section 4.1](#41-source-modules), `_schema.dataset_schema`). Once a name ships,
the lock outranks the spec and the rule:

- The generator never recomputes an existing entry, so a lexicon edit cannot silently rename a
  shipped column.
- The lock is never edited by hand. Two hand-written inputs in `lock.py` change it through the
  refresh, each entry carrying its reason and PR:
  - `ALIASES[(dataset, transport, new wire path)] = old wire path`: a wire key that Massive
    renamed gets an entry naming the old column. An alias is exempt from the collision gate only
    when the old key is absent from the current snapshot.
  - `BREAKING_RENAMES[(dataset, transport, wire path)] = (old, new, "PR #n")`: an owner-approved
    change of a shipped column's name or type.
- A wire key that Massive removes keeps its entry, so its column stays, typed from the lock, and
  is null.
- A new readable field gets its entry automatically when a dataset's path exposes it, and the
  reviewer sees it in the lock diff. A description-only change produces no lock diff.

The tests that keep this honest:

- **Parity** (`tests/_massive/spec/test_generate.py`): regenerating `restspec.json` from the
  vendored snapshot and the hand-maintained tables gives the committed file, byte for byte.
- **Completeness, inside `_massive`**: the WebSocket key maps are complete against
  `websocket.json` (`tests/_massive/names/test_wskeys.py`); the flat-file key maps match the
  documented headers (`tests/_massive/names/test_flatkeys.py`); the allow-list matches
  `llms.txt` (`tests/_massive/spec/test_allowlist.py`); `drift.py` finds each kind of change
  in synthetic before-and-after snapshots (`tests/_massive/spec/test_drift.py`); every tagged
  field in the unit table
  agrees with its tag or is in the conflict list (`tests/_massive/spec/test_units.py`).
- **Completeness, across the boundary** (`tests/test_catalog.py`, through the interface only):
  every declared dataset's schema builds (`_schema.dataset_schema`), so every wire path has a lock
  entry and every timestamp-like field a unit; every REST column of a tick dataset is a column
  of its tick schema with the same type; and every wire name a `DatasetSpec` declares (time-axis
  pair, symbol parameter, path placeholders) is a parameter of its `RestPath`; and
  once the rework finishes, `unused_allowlisted` returns nothing for the whole catalog (until
  then each namespace's own test asserts it once that namespace's datasets are declared).
- **Accounted for, inside `_massive`** (`tests/_massive/spec/test_allowlist.py`): the pinned
  snapshot has no unaccounted items; every documented REST path, WebSocket channel, and
  flat-file prefix is allow-listed or on `EXCLUSIONS`.
- **Compatibility** (`tests/_massive/names/test_lock.py`): the lock at the **last release tag**
  is compared with the current lock, and any changed or removed entry, a changed type included,
  fails the test unless `BREAKING_RENAMES` lists it. The test first runs
  `git rev-parse --is-shallow-repository` and fails on a shallow clone; the baseline is then
  `git describe --tags --abbrev=0 --match 'v[0-9]*'`, and with no such tag (before the first
  release) it passes. CI checks out the full history.

**The refresh flow.** The drift job ([section 20](#20-quality-gates)) runs `--check` and opens an
issue from its report. A person runs `--fetch`, then the plain refresh, reviews three diffs (the
snapshot, `restspec.json`, and the lock), adds a lexicon, alias, or unit line where the
generator asks for one, and lands the result as one PR. A new endpoint additionally needs its
path on the allow-list and a `DatasetSpec`.

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
- **Layout:** `tests/test_layout.py` (the mirror rule and the `_massive` import boundary of
  [section 19](#19-wire-clients-and-the-spec-snapshot)) and `tests/test_catalog.py` pass.
- **Spec:** the parity test (`restspec.json` equals what the vendored snapshot generates), the
  completeness checks (no unaccounted documented endpoint, channel, or flat-file prefix), and the
  name-lock compatibility test
  ([section 19.3](#193-the-spec-snapshot-the-generator-and-the-name-lock)) pass. They read only
  vendored files and git history, never the network.
- **Process:** conventional-commit titles; every change lands through a pull request; only the
  owner merges.

**Spec drift is advisory, never a gate.** `.github/workflows/spec-drift.yml` runs weekly and on
demand (`workflow_dispatch`). It runs `scripts/refresh_massive_spec.py --check`
([section 19.3](#193-the-spec-snapshot-the-generator-and-the-name-lock)), the same semantic
comparison a person runs locally, without S3 credentials, so flat-file coverage comes from the
documentation pages alone.

- **No drift:** the job does nothing: no issue, no comment.
- **Drift:** it opens, or updates in place, the **single standing issue** titled
  "Massive API drift", found by that title and the `spec-drift` label. It never creates a
  second one: when the issue is open it rewrites the body; when it is closed it reopens it. The
  body lists, each item with the action to take:
  - **unaccounted items** (a documented REST endpoint, WebSocket channel, or flat-file dataset
    neither allow-listed nor excluded): allow-list it and add a dataset, refreshing with
    `refresh_massive_spec.py`, or add an entry to `EXCLUSIONS` with a reason;
  - **field, type, and column changes** (response fields and parameters added, removed, or
    retyped; WebSocket keys added or removed; CSV headers changed; keys the naming gate would
    stop on): run `refresh_massive_spec.py --fetch` and the plain refresh, adding a lexicon,
    alias, or unit line where the generator asks;
  - **deprecations** (an allow-listed path newly deprecated or removed): replace the dataset's
    path or move it to `EXCLUSIONS` with a reason.
- It has `permissions: issues: write` only, never pushes, never opens a pull request, and is
  never a required check, because a change at Massive must not block unrelated pull requests.
  The refresh itself is a reviewed PR.

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
  ships no type information; `urllib3`, `websockets`, and `certifi` ship their own. No other
  override is allowed. Pyright runs as a non-gating second checker.
- **Pytest 9** (native `[tool.pytest]` table). `strict = true`, `filterwarnings = ["error"]`,
  `addopts = ["-ra", "--import-mode=importlib"]`, `faulthandler_timeout = 120` with
  `faulthandler_exit_on_timeout = true`, and `pytest-timeout` at 60 seconds. Coverage is not in
  `addopts`, so single-test runs and debuggers work.
- **Coverage.** `branch = true`, `fail_under = 100`, `exclude_also = ["if TYPE_CHECKING:",
  "@overload"]`.
- **Deephaven imports.** Module-level wherever used at run time; imports used only in
  annotations go under `TYPE_CHECKING` (enforced by `TC`). `tests/conftest.py` starts the
  embedded JVM in `pytest_configure`, before collection.
- **One entry point.** `scripts/check.sh [fast|full] [--no-fix]`. Both tiers first run
  `uv sync --upgrade`, so they use the newest versions; offline, a local run warns and keeps
  what is installed, and CI fails. The fast tier then runs ruff fix, ruff format, and mypy; the
  full tier adds pytest with coverage. Local hooks and CI call the same script; CI passes
  `--output-format github` to ruff. CI checks out the full history with tags, which the
  name-lock compatibility test needs.
- **Agent hooks** (committed `.claude/settings.json`). After an agent edits a `.py` file, ruff
  fixes and formats it and reports remaining diagnostics to the agent. Before an agent ends its
  turn, `scripts/check.sh fast --no-fix` runs. Bare `python`, `pytest`, `ruff`, `mypy`, and `pip`
  commands not prefixed by `uv run` are blocked.
- **Git hooks** (`.pre-commit-config.yaml`). The commit stage runs the fast tier plus
  `no-commit-to-branch` (main), `check-yaml`, `end-of-file-fixer`, and `trailing-whitespace`.
  The full tier runs at the pre-push stage.
- **Packaging.** `uv.lock` is not committed, and CI resolves afresh on every run. The wheel
  excludes the spec snapshot (`[tool.hatch.build.targets.wheel] exclude =
  ["src/deephaven_massive/_massive/spec/snapshot"]`); the sdist keeps it, and `restspec.json`
  and the locks ship as package data.
- **Banned import.** `[tool.ruff.lint.flake8-tidy-imports.banned-api]` bans `massive` with the
  message "The library has its own clients; see deephaven_massive._massive". A ban is not an
  ignore, so it needs no approval.
  Development dependencies use a PEP 735 `[dependency-groups]` table; license metadata follows
  PEP 639. `__version__` comes from `importlib.metadata`. `deephaven-server` is a development dependency only.
  The runtime dependencies are declared directly, with floors: `deephaven-core`, `pyarrow`,
  `boto3`, `urllib3`, `websockets` (a floor with proxy support in
  `websockets.asyncio.client.connect`), and `certifi`. There is no `massive` dependency and no
  `[tool.uv.sources]` table: every dependency comes from PyPI. `.gitignore` lists only the files
  this project produces.
- **Metadata.** `authors = [{ name = "Chip Kent", email = "chipkent@illumon.com" }]`; repository
  `https://github.com/chipkent/deephaven-massive`. No dependency blocks publication to PyPI;
  when to publish is an owner decision.

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
2. **All documented fields as columns, generated from the OpenAPI spec** (revised 2026-10-26).
   REST datasets expose every field the spec documents for their paths, flattened to PascalCase,
   from a vendored, pinned snapshot; a parity test keeps the generated code in step with it, and
   an explicit, tested override table covers spec type errors; units are the hand-verified table
   of section 12. The WebSocket and
   flat-file schemas keep hand-written per-transport key maps. Fallback chains are gone.
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
16. **WebSocket status handling.** Every frame after authentication reaches the library
    undecoded; the library decodes it once and routes status frames itself. `max_connections`,
    in the auth reply or as a later status, shows `WAITING_SLOT`, and streams stay alive
    (section 13.3).
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
22. **Column names come from a committed, append-only name lock** (2026-10-26). Fields come
    from the spec; names come from the lock, `(dataset, transport, wire path) -> column`, filled
    by the rule of [section 19.3](#193-the-spec-snapshot-the-generator-and-the-name-lock): drop
    the result key, apply `Symbol`, `Timestamp`, and `Underlying`, translate abbreviated keys
    through per-record-family lexicons of Massive's own long-form names, then PascalCase. The
    refresh fails instead of guessing on an unknown key of three characters or fewer and on a
    PascalCase collision. Names are never derived from descriptions. A compatibility test against
    the lock at the last release tag fails on any changed or removed name unless an
    owner-approved `BREAKING_RENAMES` entry lists it; a renamed wire key is aliased to the old
    column. The snapshot `min` object's columns are `Minute…`. WebSocket-only names: `av` →
    `AccumulatedVolume`, `op` → `OfficialOpenPrice`, aggregate `a` → `DayVwap`, aggregate `z` →
    `AverageTradeSize`, `dav` → `AccumulatedDollarVolume`.
23. **Spec drift is reported, not gated** (2026-10-26). A weekly workflow runs
    `refresh_massive_spec.py --check` and keeps one standing "Massive API drift" issue up to
    date, listing unaccounted items, field, type, and column changes, and deprecations, each with
    its action; with no drift it does nothing, and it never opens a pull request or becomes a
    required check. Every documented REST endpoint, WebSocket channel, and flat-file prefix is
    allow-listed or on the single `EXCLUSIONS` list, and every allow-listed item is used by a
    declared dataset once its namespace is declared. The WebSocket spec checks the WebSocket key maps for completeness,
    `llms.txt` checks the path allow-list, and the flat-file documentation tables check the CSV
    headers.
24. **No client object is shared unless it is proven thread-safe** (2026-10-26). The REST client
    is safe per call and shared by the session's threads; each WebSocket client is confined to
    its own loop thread. Free-threaded 3.14t remains a required CI job.
25. **Upstream SDK issues are optional goodwill** (2026-10-26). Real bugs in
    `massive-com/client-python` may be reported as issues only, never pull requests, off the 1.0
    path, and only after the owner has reviewed the issue text. The fork
    `chipkent/massive-client-python` is no longer used; it is archived once the project has
    fully shipped.
26. **One package for the Massive wire format** (2026-10-26). Everything that talks to Massive's
    REST or WebSocket API or describes its wire format (the two clients, frame decoding, the spec
    snapshots, the generator, the generated REST table, the lexicons, the WebSocket and flat-file
    key maps, the wire-field unit table, and the name lock) lives in `deephaven_massive._massive`,
    split into `rest/`, `ws/`, `spec/`, and `names/`. It imports nothing from Deephaven or the
    rest of the library; the rest of the library imports only its interface,
    `_massive/__init__.py`. An import-boundary test enforces both directions
    ([section 19](#19-wire-clients-and-the-spec-snapshot)). Reading flat files from S3 stays in
    `_flatfiles.py`.
27. **`MassiveError` carries `status` and `request_id`** (2026-10-26). Both are read-only and
    `None` when the error did not come from an HTTP response ([section 3](#3-exports)).
28. **`reconnect_backoff` is at least 15 s** (2026-10-26). `MassiveConfig` rejects a smaller
    value with `ValueError` ([section 5](#5-massiveconfig)).
29. **The spec snapshot is not shipped** (2026-10-26). The wheel excludes it; tests, the refresh
    command, and the drift job read it from the source tree ([section 19](#19-wire-clients-and-the-spec-snapshot)).
30. **Wire behaviour settled in review** (2026-10-26). A 429 without `Retry-After` waits 15, 30,
    then 60 s, and a `Retry-After` above 60 s fails at once; a TLS certificate failure is never
    retried; a 429 at the WebSocket upgrade is retried; wildcard transitions make before they
    break, accepting a brief duplicate rather than a gap
    ([section 13.3](#133-connection-and-subscription-protocol),
    [section 19](#19-wire-clients-and-the-spec-snapshot)).

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
| 6 | Futures snapshot `timeframe` fields (in the OpenAPI spec; absent from the documentation page) | Absence yields `socket.` with `Recency=UNKNOWN` and the explicit-verdict rule | `tests/test_hosts.py`: with and without the fields | `tests/test_hosts_integration.py`: record which fields the live payload carries |
| 7 | Exact text of the delayed-key verdict message ("real-time") | Case-insensitive substring match; limited to `Recency=UNKNOWN`, whole-step refusal, unpinned host | `tests/test_ws.py`: verdict and non-verdict messages | `tests/test_ws_integration.py` (delayed key): subscribe on `socket.` and capture the message |
| 8 | Unsubscribe acknowledgements | Not awaited; topics are removed immediately | `tests/test_ws.py`: removal without an acknowledgement | `tests/test_ws_integration.py`: record whether an acknowledgement arrives |
| 9 | Subscribe frame size limit | At most 1,000 topics per frame | `tests/test_ws.py`: 2,500 topics produce three frames | `tests/test_ws_integration.py`: a 1,000-topic frame is acknowledged |
| 10 | Options cap counted in contracts or topics | Topics (the stricter count) | `tests/test_ws.py`: `T.X` plus `Q.X` counts as two | `tests/test_ws_integration.py` (Options Advanced): 501 contracts on `T` and `Q` |
| 11 | Exact `max_connections` status text and timing | Any status `max_connections`, in the auth reply or after it, shows `WAITING_SLOT` | `tests/test_ws.py`: both placements | `tests/test_ws_integration.py`: two sessions with one individual key |
| 12 | `orjson` free-threaded wheels | Optional extra; standard-library codec by default | `tests/test_config.py`: codec selection with and without `orjson` importable | CI: the 3.14 job installs the extra (`--extra orjson`) and runs the suite with it; the 3.14t job does not, because `orjson` publishes no cp314t wheel |
| 13 | Flat-file publish time (documented "approximately 11:00 AM ET") | `flat_file_publish_cutoff` defaults to 11:30 ET | `tests/test_flatfiles.py`: AUTO before and after the cutoff | `tests/test_flatfiles_integration.py`: yesterday's key exists after the cutoff |
| 14 | Whether the server delivers an event twice when a wildcard and an explicit topic of the same channel are both subscribed (`T.*` plus `T.AAPL`) | While a channel's acknowledged wildcard is desired, its explicit topics are off the wire; transitions make before they break, accepting a brief duplicate ([section 13.3](#133-connection-and-subscription-protocol)) | `tests/test_ws.py`: adding, refusing, and removing a wildcard over explicit topics sends the expected frames in the expected order | `tests/test_ws_integration.py`: subscribe both on a test key and record whether duplicates arrive |
