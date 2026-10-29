# deephaven-massive: Design Specification

This document is the approved design for the clean rework of `deephaven-massive`, a Python
library that exposes Massive.com (formerly Polygon.io) market data as live and static
[Deephaven](https://deephaven.io) tables. It is the source of truth for every implementation
planned PR in `PLAN.md`. It changes only by owner decision.

The design was checked against Massive's OpenAPI spec (`https://api.massive.com/openapi`,
147 paths), its WebSocket spec (`https://api.massive.com/specs/websocket.json`, 37 channels),
`llms.txt` and the REST pages it links, and the flat-file dataset and overview pages, all as
of 2026-10-26;
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
   fixture test. Live integration tests are a later planned PR.
10. **Closed categorical sets are enums.** Every closed set of values in a public signature is a
    `StrEnum`. No public signature accepts a string in place of an enum member.
11. **Schemas are generated at build time and fixed before data.** No schema is hand-written,
    and none is inferred from data. A live table must know its full column set and types before
    its first row, the first message may omit optional fields (stocks `T` omits `pt` on OTC
    trades), and two sessions must agree on a dataset's shape.
12. **Every column says what it means, in the vendor's own words where the vendor has them.**
    The refresh records Massive's description of every field beside the generated schemas,
    and one rule picks each dataset column's description from them; fixed library text
    covers only the cases [section 19.4](#194-generated-schemas) defines (the key columns
    `Symbol`, `Timestamp`, and `Underlying`, and a column that only other members of its kind
    supply). The same text reaches users three ways: the column-header tooltip of every result
    in the Deephaven web UI, `columns()` in the console, and the generated pages under
    `docs/datasets/` ([section 7.4](#74-column-descriptions)). Nobody writes a column list by hand.

### 2.2 Terms

| Term | Meaning |
|---|---|
| dataset | One method under a namespace, such as `m.stocks.trades`. |
| query | The immutable object a dataset method returns. |
| verb | A method on a query that returns a Deephaven `Table` of the dataset's rows. `columns()` returns a table about the columns, not rows, and is not a verb. |
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
| column description | The plain-text meaning of one dataset column: the description of the wire field behind it (the vendor's, or the refresh's correction where the vendor's is clearly wrong), or fixed library text for a key column or a column only other members of the kind supply ([section 19.4](#194-generated-schemas)). |
| column reference | The generated, committed pages `docs/datasets/<namespace>.md`, one section per dataset ([section 7.4](#74-column-descriptions)). |
| UG thread | A thread of Deephaven's update graph (UG), which refreshes live tables while holding the UG lock. |
| blink table | A Deephaven table whose rows exist for exactly one update cycle. `TablePublisher` produces one. |
| result scope | A library-owned `LivenessScope` that holds exactly one result. |
| stream scope | A library-owned `LivenessScope` that holds one stream's blink table and publisher. |
| registry lock | `SessionCore.registry_lock`, the one lock that guards every session-level registry. |

## 3. Exports

`deephaven_massive/__init__.py` exports exactly these 22 names and nothing else:

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
dm.TickerType
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
for the same purpose. Every query also has `columns()`
([section 7](#7-query-classes-and-verbs)), a method rather than an export, so the list above
does not change.

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

**One subject per module.** A module is named for its content and holds one subject, named in
the first line of its docstring. New code goes in the module whose first line already names its
subject; if none does, it gets a new module named for that subject, never a widened summary ("…,
and X") on a neighbor. Module names are subject nouns, never pattern words such as `base`,
`core`, `common`, `helpers` or `util`. A rule or helper has one body, in the module that owns
its concept. An exception is defined in the module that raises it, or in the layer's errors
module when several modules raise it, or when a module in a lower layer must name it:
`_errors.py`, or `_massive/net/errors.py` for the wire clients. A module above about 1,000
lines, or one whose first line lists more than two subjects, is split or says in its docstring
why it is one subject. Review enforces these rules, not a test: checks of placement over source
text would be brittle and costly to keep for little protection.

```
src/deephaven_massive/
__init__.py              public names of section 3, lazy (PEP 562); TYPE_CHECKING imports mirror __all__
_config.py               MassiveConfig (section 5)
_enums.py                the fifteen public enums, Recency, ChannelFamily, _require_enum
_types.py                Symbols, ALL/_All, DateLike, FilterValue, bars overload aliases, override
_errors.py               MassiveError (.status, .request_id), AuthenticationError, EntitlementError,
                         from_wire
_session.py              MassiveSession: builds the catalog, the core, and the session's clients
                         (section 6)
_query.py                the six query classes and their verbs, through the core's ports (section 7)
_namespaces/
  __init__.py            docstring only
  base.py                the shared dataset factory
  stocks.py options.py futures.py indices.py forex.py crypto.py
  reference.py indicators.py economy.py partners.py      namespace class + DATASETS
_datasets/               dataset semantics: no network I/O, no threads, no Deephaven calls;
                         only column_reference.sync writes files
  __init__.py            docstring only
  catalog.py             DatasetSpec (summary, role paths and fetch paths by symbol class,
                         kind, per-source key columns, channels, prefixes, poll defaults,
                         page size, symbols per request), Catalog (the specs by name,
                         members(kind)), filter validation
  schema.py              ColumnType, Column, Schema, WireType -> ColumnType, rename(),
                         dataset_schema() with column descriptions (section 19.4)
  request.py             Request, StreamKey, ParamValue, HostDecision
  timestamps.py          the one timestamp routine (section 12), the FMV magnitude rule
  dates.py               DateLike parsing, day zones, sessions, publish_time, [start, end)
  symbols.py             normalization, symbol_class(), the Symbol/Underlying rule
                         (section 10)
  history_source.py      partition dates, publication grace, REST capability, the AUTO choice
  column_reference.py    the column reference: render(catalog, provenance) -> {file name:
                         Markdown} (pure); sync(directory, pages) owns docs/datasets/
                         (section 7.4)
_tables/                 the Deephaven side: tables, liveness, library threads
  __init__.py            docstring only
  core.py                SessionCore, registry_lock, registries, closers, stream ids, the
                         transport ports, IngestPorts, attach (sections 4.3, 6)
  liveness.py            result and stream scopes, records, reaper queue (section 6.1)
  diagnostics.py         connections() and streams() with SchemaDrift; DriftRegistry (section 17)
  columns.py             ColumnType -> Deephaven type, column definitions; describe() (section 7.4)
  watchlist.py           canonical_view; reactive symbol tables
  stream.py              streams, publishers, flush thread, fan-out, bound-failure routing
                         (section 14)
_ingest/                 the only holder of wire clients
  __init__.py            docstring only
  clients.py             SessionClients: the session's wire clients and connections, the ports
  flatfiles.py           flat-file transport: read plans, reader pool, cancellation, conversion,
                         merge to tables, missing-file policy, drift routing (section 16)
  rest.py                REST transport: requests from a DatasetSpec, batches renamed and
                         converted, 403/429 policy, drift routing, host-decision cache
                         (single-flight)
  hosts.py               host resolution from REST evidence (section 13.2)
  poll.py                poll(): REPLACE/UPSERT/APPEND, fan-out, data failures -> publish_failure
  subscriptions.py       topics and the reconcile loop of one connection: wildcards,
                         watchdogs, the NO_ACK rule, the options cap's admission (section 13.3)
  ws.py                  connections over WsClient: leases, the verdict rule, rejects and drift
                         routing, the options cap's verb check (section 13.3)
_massive/                the wire package (section 19)
py.typed
```

The root holds the vocabulary every layer names (`_config`, `_enums`, `_errors`, `_types`) and
the API users touch (`__init__`, `_session`, `_query`, `_namespaces/`). The rest of the library
is grouped into three packages: `_datasets/` describes datasets (and renders their column
reference from a catalog it is given), `_tables/` is the Deephaven side, and `_ingest/` is the
only code that holds wire clients. The `__init__.py` of each of the three, and of
`_namespaces/`, holds only a docstring. [Section 4.3](#43-layers) orders these groups into
layers.

`_namespaces/` holds plain modules, one namespace and its sub-namespaces each (`m.partners` and
its partner sub-namespaces share a module); it is not a PEP 420 namespace package. Each
namespace module also declares its datasets (`DATASETS: tuple[DatasetSpec, ...]`).
`_datasets/history_source.py` decides where `history()` reads from; `_ingest/flatfiles.py` does
flat-file I/O through `_massive` and never chooses a source.

Supporting files outside the package:

| File | Purpose |
|---|---|
| `scripts/check.sh [fast\|full] [--no-fix]` | The single verification entry point (lint, format, type check; `full` adds the test suite with coverage). |
| `python -m deephaven_massive._massive.refresh [--check \| --fetch]` | The refresh command, run from a source checkout ([section 19.6](#196-refresh-and-drift)). |
| `uv run python scripts/render_reference.py` | Regenerates the column reference, `docs/datasets/`, the whole directory and nothing else ([section 7.4](#74-column-descriptions)). |
| `docs/datasets/<namespace>.md` | The generated column reference, committed, never edited by hand. |
| `vendor/massive/` | The vendored spec snapshot the refresh reads ([section 19](#19-wire-clients-and-the-spec-snapshot)). It is Massive's published content, vendored as generation input: not covered by the repository's license, and in neither the wheel nor the sdist. `vendor/README.md` says so beside it. |
| `.github/workflows/spec-drift.yml` | The advisory weekly drift job ([section 20](#20-quality-gates)). |
| `.github/workflows/release.yml` | Publishes to PyPI from a `v*` tag; created only when the owner turns publishing on ([section 20.1](#201-tooling-configuration)). |
| `tests/data/` | Payload fixtures. |
| `tests/_fakes/` | Fake REST server, WebSocket server, S3, clock, and stream helpers, and the shared streaming session harness ([section 4.2](#42-test-layout)). |

Deephaven is imported at module level where it is used at runtime. Type-only imports go under
`TYPE_CHECKING`. The library imports nothing from `deephaven_server`;
`scripts/render_reference.py` starts the embedded server itself, outside the package, before it
imports the library. Only the two lazy `__init__.py` files (the package's and `_massive`'s)
defer imports.

### 4.2 Test layout

Tests mirror the package. The test file for `deephaven_massive/<path>/<name>.py` is
`tests/<path>/test_<name>.py`, where **every path component drops one leading underscore**
(`_namespaces/stocks.py` → `tests/namespaces/test_stocks.py`, `_massive/rest/client.py` →
`tests/massive/rest/test_client.py`). A package's `__init__.py` is mirrored by
`tests/<path>/test_init.py`, and a `__main__.py` by `tests/<path>/test_main.py`, so every source
module has exactly one mirror. `tests/test_init.py` holds the public-surface tests (`dm.__all__`
equals [section 3](#3-exports) and `__version__` equals the installed distribution's version).
`tests/datasets/test_catalog.py` is the plain mirror of `_datasets/catalog.py`, like every other
module's test. A directory directly under `tests/` whose name starts with `_` is a helper
package outside the mirror rule; a test module (`test_*.py`, `*_test.py`, `conftest.py`) inside
one is still checked. Fixtures live in `tests/data/<test owner path>/` (for example
`tests/data/massive/rest/`, `tests/data/ingest/flatfiles/`), so no fixture directory starts
with `_`. Examples:

| Source | Unit test | Live integration test |
|---|---|---|
| `__init__.py` | `tests/test_init.py` | |
| `_config.py` | `tests/test_config.py` | |
| `_datasets/__init__.py` | `tests/datasets/test_init.py` | |
| `_datasets/catalog.py` | `tests/datasets/test_catalog.py` | |
| `_tables/core.py` | `tests/tables/test_core.py` | |
| `_ingest/hosts.py` | `tests/ingest/test_hosts.py` | `tests/ingest/test_hosts_integration.py` |
| `_ingest/flatfiles.py` | `tests/ingest/test_flatfiles.py` | `tests/ingest/test_flatfiles_integration.py` |
| `_datasets/column_reference.py` | `tests/datasets/test_column_reference.py` (also the parity test of `docs/datasets/`) | |
| `_namespaces/__init__.py` | `tests/namespaces/test_init.py` | |
| `_namespaces/stocks.py` | `tests/namespaces/test_stocks.py` | `tests/namespaces/test_stocks_integration.py` |
| `_massive/__init__.py` | `tests/massive/test_init.py` | |
| `_massive/rest/client.py` | `tests/massive/rest/test_client.py` | |
| `_massive/refresh/__main__.py` | `tests/massive/refresh/test_main.py` | |
| `_massive/refresh/generate.py` | `tests/massive/refresh/test_generate.py` (also the parity test) | |
| `_massive/refresh/rest.py` | `tests/massive/refresh/test_rest.py` | |

**Import roots.** `[tool.pytest] pythonpath = ["."]` and `[tool.mypy] mypy_path = ["src"]`;
test code imports helpers as `tests._fakes…`. So `tests/massive/` can never shadow a top-level
`massive` package.

Integration tests end in `_integration` and carry the `integration` marker, which `addopts`
deselects in every default run (CI's included); `uv run pytest -m integration` runs them, and
each skips unless `MASSIVE_API_KEY` is set (and always under CI).
[`docs/live-tests.md`](../../docs/live-tests.md) is the runbook for them. The live tests that deliberately exceed Massive's subscription and connection
limits also carry `integration_limits`, and the smoke tier (every dataset and verb once,
checked automatically) carries `integration_smoke`: `-m integration` deselects both, and only
`-m integration_limits` or `-m integration_smoke` runs them. `MASSIVE_LIVE_PLANS` declares the
key's plans, and each test's `needs` marker skips it when they fall short. The exceptions to the mirror rule, and no
others:

- `tests/conftest.py`: starts the embedded JVM in `pytest_configure`, before collection, and
  provides fixtures: thin fixtures that build the fakes from `tests/_fakes/`, plus the
  table-to-Python and fixture-loading helpers. Test modules never import from it.
- `tests/test_layout.py`: walks `src/deephaven_massive/**/*.py` and asserts every module has its
  mirrored test file and that every test file maps to a module or is one of these exceptions.
  It also holds the import-boundary and layer tests of `_massive`
  ([section 19](#19-wire-clients-and-the-spec-snapshot)) and the library's layer rules
  ([section 4.3](#43-layers)), with a negative test per rule: `tests/massive/` may import any
  `_massive` module, and the rest of `tests/` only the interface.
- `tests/test_readme.py`: runs every example of [section 21](#21-examples) in the embedded JVM
  against the fakes. It mirrors `README.md`, not a source module.
- `_` helper directories, of which there is one: `tests/_fakes/` (`__init__.py`, `rest.py`,
  `ws.py`, `s3.py`, `stream.py`, `clock.py`, `gh.py`), protocol-faithful fakes, and
  `ws_session.py`, the one streaming harness every WebSocket end-to-end test shares (a real
  session whose connections reach the fake WebSocket server, offered by the `ws_session` and
  `make_ws_session` fixtures). Only `tests/conftest.py` imports it;
  test modules receive fakes through fixtures, and import a fake directly only when a fixture
  cannot provide it, saying why in a comment.

### 4.3 Layers

The library has six layers. A module imports only its own layer or lower ones, and an import
under `if TYPE_CHECKING:` counts: moving an upward import under it does not make it legal.

| Rank | Layer | Home | What justifies the boundary |
|---|---|---|---|
| 0 | wire | `_massive/` | The only network I/O, and no Deephaven ([section 19](#19-wire-clients-and-the-spec-snapshot)). |
| 1 | vocabulary | the root `_config`, `_enums`, `_errors`, `_types`, `_log_limits` | The public value types every layer names. No I/O and no threads; Deephaven appears only as the `Table` in `Symbols`. |
| 2 | datasets | `_datasets/` | Dataset semantics: no I/O, no threads, no Deephaven calls, and `deephaven` imported only for type checking. So any of it is safe to call under `registry_lock` and on any thread. |
| 3 | tables | `_tables/` | The Deephaven side: tables, liveness, the session core, publishers, library threads. It never holds a wire client and imports no ingest code, so it is tested with no fake server. |
| 4 | ingest | `_ingest/` | The only holder of wire clients: `SessionClients` owns them per session. |
| 5 | API | the root `__init__`, `_session`, `_query`, and `_namespaces/` | What users touch. `_session.py` is the composition root. |

**Composition.** `_session.py` is the one module that sees every layer. It imports every
namespace module statically and names each once, in `NAMESPACE_MODULES` (a test compares the
tuple with the modules of `_namespaces/`); at import it builds `CATALOG` from their `DATASETS`,
so the dataset layer never finds the namespaces itself. `MassiveSession.__init__` builds the
`SessionCore` with that catalog, then `SessionClients(core)`, then calls `core.attach(clients)`
once. The core defines the transport ports (`RestTransport`, `StreamTransport`,
`FlatFileTransport`) and `IngestPorts`, the protocol `SessionClients` implements
(`rest()`, `streams()`, `flat_files()`); the core's transport hooks call the attached ports, and
the transports read the catalog as `core.catalog`. A dependency therefore never points from the
tables up to the code that does the I/O. `SessionClients` holds the core through a
`weakref.ref`, so the core and its clients form no reference cycle and an unclosed session is
still collected as soon as its last reference goes. Each of its methods takes a strong
reference for its call and raises `MassiveError("MassiveSession #1 is closed")` if the core is
gone; the transports it builds hold the core itself, never the weak reference.

**The rules**, each enforced by `tests/test_layout.py` with a negative test:

- every module under `src/` is in a known layer; any other root module or package is reported
  as "not in a known layer", so a new one needs a rule change;
- every import, relative import, dynamic import with a resolvable literal name (an absolute
  name, or a relative one with a literal package), and module-name string, type-only or not,
  targets a layer no higher than its own;
- outside the two lazy `__init__.py` files, every import the rules cannot resolve from the
  source is a violation: an `import_module` or `__import__` whose name is not a literal, a
  relative name without a literal package, an `__import__` whose `level` is not a literal 0,
  any use of `import_module` or `__import__` other than a direct call (an alias made by
  assignment, a `getattr`), and a `sys.modules` lookup by a key that is not a literal;
- Deephaven: none in the wire; only `deephaven.table` in the vocabulary; `_datasets` imports
  `deephaven`, and the vocabulary's run-time `_types.Table`, only under `TYPE_CHECKING`
  (`if TYPE_CHECKING:` or `if typing.TYPE_CHECKING:`); nothing imports `deephaven_server`;
- the vocabulary and `_datasets` import none of `queue`, `asyncio`, `concurrent`, `socket`,
  `ssl`, `http`, `urllib`, `subprocess`, `_thread`, `multiprocessing`, `select`, or
  `selectors`, and read neither `threading.Thread` nor `threading.Timer`, by attribute, import,
  or literal `getattr` (`threading` itself stays allowed, for leaf locks);
- `_massive.__all__` splits into vocabulary names (the three errors, the spec names, and the
  pure parameter encoder `encode_value`; a subprocess test checks that reading every one of
  them loads no client and no network module) and
  client names (`RestClient`, `Page`, `to_batch`, `WsClient`, `WsState`, `Status`,
  `StatusKind`, `Reject`, `FlatFileClient`, `FlatFile`, `ReadPlan`, `ReadColumn`,
  `object_key`); under `src/`, only `_ingest` uses a client name (by import, attribute,
  literal `getattr`, or string), nor anything that can reach one (a star import of `_massive`,
  a `getattr` on it by a computed name), and a test asserts that the two sets partition
  `__all__`, so a new name must be classified;
- in the API, only `_session.py` imports `_ingest`, and only `_ingest.clients`;
- the `__init__.py` of `_namespaces`, `_datasets`, `_tables`, and `_ingest` holds a docstring
  and nothing else.

Tests are outside the layers: they may import any module and keep only the wire interface rule
of [section 19](#19-wire-clients-and-the-spec-snapshot). The rules read source, so they cannot
see an import built by `exec`, `importlib.util.find_spec`, or an attribute read through the
root package (`deephaven_massive._ingest`); review keeps those out.

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
  `deephaven-massive[orjson]`); otherwise the standard library. The wire package's
  `net.codec` owns the choice and logs it once at INFO, when it is first imported
  ([section 19](#19-wire-clients-and-the-spec-snapshot)).
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
  if the failure had arrived just after it: every shape, `blink()` included, raises
  `MassiveError` naming the stream's failure, with the hold released. `SessionCore.stream_result`
  checks the stream's recorded failure before it builds the shape, and wraps in the same
  `MassiveError` the `DHError` ("Can not listen to failed table") Deephaven raises when the
  failure lands while the shape derives from the blink table. A stream's failure is recorded
  (`SessionCore.fail_stream`) before its table is failed (`publish_failure`), so a verb that
  meets the failed table always finds the failure to name.
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
reactive table as `<reactive table>`. Queries have no other attributes: the verbs and `columns()`
are their whole API (§ 22, “Sessions and queries expose no introspection attributes” and
“`columns()` on every query”).

```python
class QueryBase:                           # the base of all six classes below
    def columns(self) -> Table: ...        # static; the dataset's columns and what they mean;
                                           # no network, not a verb, not a result
```

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
    def history(self, start: DateLike, end: DateLike | None = None, *,
                source: HistorySource = HistorySource.AUTO) -> Table: ...   # static

class StreamHistoryQuery(StreamQuery, HistoryQuery): ...

class StreamHistoryLatestQuery(StreamHistoryQuery):   # adds a REST "last" endpoint
    def fetch(self) -> Table: ...          # static; exactly one row per symbol in the stream schema;
                                           # ALL raises ValueError naming snapshots()

class HistoryLatestQuery(HistoryQuery):   # daily bars only
    def fetch(self) -> Table: ...          # static; one row per symbol: the previous completed session's bar
```

**The query class of a dataset** is derived from its `DatasetSpec`, never declared on it, so
the dataset layer never names the query classes. With S meaning the spec has channels, H that it
has a history source (a history path, a grouped-daily path, or a flat-file prefix), and F that
it has a fetch path, `query_class(spec)` in `_namespaces/base.py` gives:

| S | H | F | Class (not `bars`) |
|---|---|---|---|
| no | no | yes | `Query` |
| yes | no | no | `StreamQuery` |
| no | yes | no | `HistoryQuery` |
| yes | yes | no | `StreamHistoryQuery` |
| yes | yes | yes | `StreamHistoryLatestQuery` |
| no | no | no | rejected: no verb |
| yes | no | yes | rejected: no class streams and fetches without history, and no streamed dataset has `poll()` |
| no | yes | yes | rejected outside `bars`: `HistoryLatestQuery` is kept for daily bars |

`DatasetSpec.__post_init__` raises `ValueError`, naming the dataset, for the rejected rows. A
`bars` spec must have H; `query_class` gives it `HistoryQuery`, the class of its general
overload, and `bar_query_class(spec, timespan, multiplier)` picks the class of each call
([section 7.1](#71-bars-overloads)). A test in `tests/namespaces/test_base.py` checks that every
factory's return annotation is `query_class(spec)` for its catalog spec, that each `bars`
overload returns `bar_query_class` on a timespan of its `Literal` set and the implementation
returns `HistoryQuery`, and that every public factory method maps to exactly one spec.

**`columns()`.** Lists the columns every verb of the dataset returns, without touching the
network, so a console or notebook user can read what each column means without the web UI or
GitHub. The docstring contract, which the implementation copies:

```python
def columns(self) -> Table:
    """List this dataset's columns, their types, and what each one means.

    Every verb of the dataset returns these columns in this order, whichever transport
    serves it; a column the serving transport does not supply is null. The table is built
    from the committed generated schemas: no request is made, no stream is opened, and it
    works on a closed session.

    Returns:
        A static table with one row per column, in result column order:
        `Name` (String), the column name; `Type` (String), its Deephaven type (`long`,
        `int`, `double`, `boolean`, `String`, `Instant`, `LocalDate`, or `int[]`);
        `Description` (String), what the column holds, empty when Massive gives no
        description; and `SuppliedBy` (String), the transports whose sources fill the
        column for this dataset, spelled `REST`, `flat files`, and `WebSocket`, in that
        order, joined by ", " (`fetch()` and `poll()` are served by REST, the stream verbs
        by WebSocket, and `history()` by REST or flat files); empty for a column that only
        other datasets of the same kind supply, which this dataset always leaves null.

    Raises:
        ValueError: If called on an update-graph thread, for example from a table listener.

    Example:
        ::

            cols = m.stocks.trades(["AAPL"]).columns()
            stream_only = cols.where("SuppliedBy = `WebSocket`")
    """
```

`columns()` is not a verb and its table is not a result. It makes no session call, so it is
not tracked, `m.release()` rejects it with `ValueError` like any other foreign table, and it is
created in whatever liveness scope the caller has open, as any static table the caller makes
is. The table is the same for every query of one dataset: the arguments choose the rows a verb
fetches, never the columns. Its own four columns carry column descriptions too
([section 7.4](#74-column-descriptions)).

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
every `stocks.filings.*` but the two taxonomies, `insider_activity`, and `insider_holdings`
(running totals and current positions that Massive revises in place, so they are replaced,
keyed by issuer and day or by issuer and owner), and `partners.benzinga.news`;
`PollMode.UPSERT` for `stocks.ipos`, whose records are revised in place as an offering moves
through its statuses (keyed by `Symbol` and `IssuerName`, both required, so a ticker two
issuers used stays two rows; the key assumes Massive keeps an offering's issuer name unchanged
through its statuses, which no vendor source states, and a renamed issuer would leave its old
row behind: [section 23](#23-open-verification-items)); `PollMode.REPLACE` otherwise, the two
filing taxonomies included (a taxonomy is a reference list revised in place, not a feed of new
rows).

- `REPLACE` is a keyed table (`poll_keys`) from which keys absent in the latest cycle are
  deleted, also when the cycle is empty.
- `UPSERT` is keyed the same way and never deletes. When `poll_keys` are every column
  (`reference.related_companies`), it keeps each distinct row once and adds the new ones each
  cycle brings.
- `APPEND` is append-only and appends only rows it has not seen before. A row's identity is
  its `poll_keys` followed by its `revision_keys` (empty by default). Each cycle skips every
  row whose identity the table already holds, and a second row with the same identity in one
  cycle, and appends the rest. A later revision of an appended row that changes no identity
  column (an article's corrected text) is therefore not appended; `UPSERT` follows revisions.
  The poller remembers every identity it appended for as long as it runs, with no bound and no
  eviction: each remembered identity is a row the append-only table already holds, so the
  memory never outgrows the table, and a row the responses repeat, or that returns after any
  absence (a symbol removed from a reactive table and added back hours later), is never
  appended twice. Nothing is remembered from a cycle that fails to publish, so its rows are
  appended by the next cycle that returns them.
- `revision_keys` are what make an explicit `mode=APPEND` on a current-state dataset build a
  history: every snapshot and movers dataset declares its update time, so each new state of a
  symbol is a new row, while `REPLACE` and `UPSERT` stay keyed by `Symbol`. The stocks,
  crypto, and forex snapshots and movers declare `Updated`; the forex conversion
  `LastTimestamp`, the time of the quote it used; the indices snapshots `LastUpdated`; the
  options snapshots and chain `LastQuoteLastUpdated` and `LastTradeSipTimestamp`; the unified
  snapshots `LastUpdated`, `LastQuoteLastUpdated`, and `LastTradeLastUpdated`, which together
  cover every asset class; the futures snapshots `LastQuoteLastUpdated` and
  `LastTradeLastUpdated`; `futures.market_status` its `Timestamp`, the time of a product's
  latest market event; and `reference.market_status`, a one-row dataset with no
  `poll_keys`, its `ServerTime`. A null in a revision column is a value (a ticker not yet
  updated). On a dataset with neither unique `poll_keys` nor `revision_keys` (the default
  `Symbol`), an explicit `APPEND` appends one row per symbol.
- A dataset may name poll-key columns the vendor leaves empty on some rows in
  `nullable_poll_keys`, each with its reason from the vendor's description, its marking the
  field optional, or an authoritative regulatory source such as the SEC's own form
  instructions (the 13-F `PutCall` is empty for common stock, and its amount and voting
  authority columns are marked optional; the Form 3 and Form 4 exercise price and dates are set only for
  derivatives, and their `NatureOfOwnership` only for indirect ownership; a Form 4 holding
  record has no transaction columns, and the SEC's Form 4 instructions let a filer report the
  value owned after a transaction instead of `SharesOwnedFollowingTransaction`; no futures key
  is nullable, since no vendor description or fixture leaves one empty). A null there is a
  value. The catalog gate requires at least one poll-key column outside that map, so no row is
  identified by nulls alone.
- A null value in any other `poll_keys` column, or a NaN in a float one, fails the table in
  every mode, as a missing required key does: it would otherwise merge unrelated rows into one
  key. The `RowDataError` names the dataset, the key column, the row's position in the cycle,
  and the row's non-null values, truncated.
- Every dataset whose default is `APPEND` declares its own `poll_keys`, not the default
  `Symbol` (a catalog gate). No gate can prove that a key is unique per row; each dataset's
  tests check its key against its fixtures.

A reactive `Table` passed as
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

**`history()`.** Static. `start` and `end` are both dates (whole days, inclusive at both
ends; for futures, whole sessions) or both aware datetimes (exact instants, half-open `[start,
end)`); a call that mixes them raises `ValueError`. `end=None`, the default, reads through now.
A reversed range raises `ValueError`; two equal datetimes are an empty range and return an
empty table without any request; two equal dates are that one day. A bar is returned only when
its whole window lies inside the range and has ended, so the current day's, week's, or month's
bar never appears. The full rules are in [section 12](#12-timestamp-units-and-day-boundaries).
There is no `limit`: the library pages internally. (The aggregates endpoint's `limit` counts
base aggregates, not rows, so it cannot serve as a row cap.) A reactive `Table` passed as
`symbols` is read once, when the verb is called, on the caller thread inside the session's
execution context: its valid rows become the sorted canonical symbol tuple that every source
receives (`REST`, `FLAT_FILES`, and the `AUTO` choice), rows that are not valid symbols of the
class are ignored (logged once per process per class, at WARNING), and a table with no valid
symbol raises `ValueError`, as an empty symbol list does.

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

class _Stocks:   # same shape on m.options; m.indices, m.forex, and m.crypto have no `adjusted`
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

### 7.4 Column descriptions

Each dataset column has one description, chosen by the rule of
[section 19.4](#194-generated-schemas) from the field descriptions in `spec/descriptions.json`
(the vendor's, corrected where they are clearly wrong). It reaches users through three outlets,
all generated from the same dataset schema, so they can never disagree. All three spell the
transports one way, `REST`, `flat files`, and `WebSocket` (`schema.TRANSPORT_LABELS`): the
key-column descriptions of [section 19.4](#194-generated-schemas) rule 3, `columns()`'s
`SuppliedBy`, and the column reference's **Supplied by**, so a filter copied from a page
(``cols.where("SuppliedBy = `WebSocket`")``) works on `columns()`. The `Transport` enum values
(`REST`, `FLAT_FILES`, `WS`) stay internal names, which the diagnostics tables of
[section 17](#17-diagnostics) show.

**1. Every result is described.** Deephaven 42.7 stores column descriptions as the table
attribute `ColumnDescriptions` and a table description as `TableDescription`, set through
`io.deephaven.engine.table.GridAttributes.withColumnDescriptions(Map<String, String>)` and
`withDescription(String)`. Barrage sends the column descriptions to clients, and the web UI
shows a column's description in the column-header tooltip; whether it shows the table
description anywhere is **UNVERIFIED** ([section 23](#23-open-verification-items)), so it is
set regardless, for every API client to read. The Python package has no wrapper for them, so
`columns.describe(table, spec, schema)` (in `_tables/columns.py`, with `schema` the dataset's
`Catalog.dataset_schema(spec)`) calls them on `table.j_table`, an
`io.deephaven.engine.table.Table`, which extends `GridAttributes`, so no explicit cast and no
direct `jpy` import (`jpy` is not a declared dependency) is needed: it calls
`withColumnDescriptions` with a `java.util.HashMap` (`deephaven.jcompat.j_hashmap`) of every
column whose description is not empty, then `withDescription`, and wraps the outcome in a new
`deephaven.table.Table`, whose constructor casts it back to `Table`. Before the attributes,
it puts the table's columns in schema order with `moveColumnsUp(*schema.column_names)`, a view
that copies no data and keeps the table's attributes (the blink attribute included): a
`last_by`, the shape of `last_by()` and of an `UPSERT` or `REPLACE` `poll()`, puts its key
columns first, and `columns()` promises every verb the schema's order. The reordering checks
that every schema column exists and `withColumnDescriptions` that every named column does, so a
schema that disagrees with the table fails at once; `Table.with_attributes` skips that check and is not used. The
table description is `"<dataset>: <summary>"` (`"stocks.trades: Trades of US-listed stocks,
tick by tick."`), where `<summary>` is the one-sentence `DatasetSpec.summary`. The summary is
a required field of every spec; `DatasetSpec.__post_init__` raises `ValueError`, naming the
dataset, unless it is one line of text that ends in a period, with no leading or trailing
whitespace. The dataset method's docstring starts with the same sentence, which the summary
rule in `tests/namespaces/test_base.py` checks for every factory of the catalog; the library
never reads `__doc__` at run time, so `python -OO` changes nothing.

- **Every verb describes the exact table it returns, after the shape is derived.** Deephaven
  42.7 copies `ColumnDescriptions` through `flatten`, `sort`, filters, `reverse`,
  `partition_by`, `coalesce`, `first_by`, `last_by`, and tree and preview operations, and
  `select`, `view`, `rename`, and joins keep the descriptions of the columns they keep.
  Nothing in `BlinkTableTools` or `RingTableTools` copies attributes, so an append-only or
  ring table derived from a described blink table would lose them. The verb therefore calls
  `describe()` last: on the blink copy for `blink()`, on the append-only or ring table, on the
  `last_by` table after any `where_in`, on the `poll()` table (the shape derived from its
  publisher's blink table), and on the static table of `fetch()` and `history()`.
- **Threading and liveness.** `describe()` makes a new table, so it runs where every result
  table is made: on the caller thread, inside the result's `LivenessScope`
  ([section 6.1](#61-how-a-live-table-is-kept-alive-and-released)), never under
  `registry_lock` and never in a listener. Copying a table can briefly take the update-graph
  shared lock (its initial snapshot falls back to the lock after repeated concurrent
  attempts), like any table operation, which is one more reason it never runs under
  `registry_lock`. The described table depends on the table it copies, so the
  undescribed parent, and for a stream the blink table that owns `on_shutdown_callback`, lives
  exactly as long as the result does; a teardown test proves that releasing the result still
  shuts the stream down. A `poll()` poller writes only through the publisher it holds, never
  through the returned copy.
- The diagnostics tables of [section 17](#17-diagnostics) are not dataset results and are not
  described.

**2. `columns()` lists them in the console** ([section 7](#7-query-classes-and-verbs)): the
same descriptions, plus each column's type and the transports that supply it, as a static
table built from the dataset schema with no network call. The tooltip only exists in the web
UI; `columns()` serves a notebook, an IDE, or a script.

**3. The column reference documents them on GitHub.** `docs/datasets/<namespace>.md` holds
one generated page per namespace module (`stocks.md`, …, `partners.md`; `m.snapshots` is on
`reference.md`, beside its record `reference.snapshots`), with one section per dataset in
declaration order. Each section starts with an explicit anchor, `<a id="<dataset>"></a>`,
where `<dataset>` is the dataset's name below its namespace with dots written as hyphens
(`trades`, `financials-ratios`, `benzinga-news`); a heading naming the dataset
(`m.stocks.trades`); its `DatasetSpec.summary`; and one table with one row per column, in
result order. The table's columns are **Column**, **Deephaven type**, **Description**,
**Supplied by** (the transports of `columns()`'s `SuppliedBy`, in the same spelling), and one
**Wire field** column per transport the dataset uses (`REST field`, `Flat-file field`,
`WebSocket field`). A wire-field cell lists the distinct wire keys of that
transport's sources that feed the column, in source order and before any projection, or
`request` for a key filled from the request. Under the table, a list headed "Massive's
documentation of this dataset:" gives the vendor pages of that dataset's own sources, sorted and
distinct, from the `Provenance` passed to the renderer ([section 19.4](#194-generated-schemas));
a dataset whose sources have no URLs gets no list. A source several asset classes share (the
unified snapshot `/v3/snapshot`, `/v3/reference/tickers`) has one page per class; for a dataset
with an `asset_class`, only the pages whose URL path has a segment equal to that class's value
(`/docs/rest/options/snapshots/unified-snapshot.md` for `options.snapshots`) are listed, and all
of the source's pages when none matches, so a dataset never loses its links to a change in the
vendor's URL layout. A dataset that spans classes (`asset_class` None, such as
`reference.snapshots` or `reference.tickers`) lists every class's page. The page ends with a
footer naming the generator command and the snapshot's fetch date. Units and nullability get no columns: every
timestamp is an `Instant`, and **Supplied by** with the description already says when a
column is null. Readers can see what each table holds before they install.

- **The renderer** is `column_reference.render(catalog: Catalog, provenance: Provenance) ->
  dict[str, str]` in `_datasets/column_reference.py`, a pure function from the `catalog` (the
  `Catalog` of `_datasets/catalog.py`: every declared `DatasetSpec`, by name) and `provenance`
  it is given, and the dataset schemas, to `{file name: page text}`; it reads no
  other state, so tests pass a synthetic catalog and `Provenance`. A **Deephaven type** cell is
  the column's `ColumnType` value (`Instant`, `long`, …), so the renderer imports no
  Deephaven. It escapes `|` in cells, emits no trailing whitespace, and ends every page with one
  newline, so the pages pass the commit hooks unchanged. `column_reference.sync(directory,
  pages)`, in the same module, owns the directory: it deletes every file and subdirectory of `directory` that `pages` does not name,
  writes each page whose bytes differ, and returns the names it wrote and deleted. A missing
  `directory` counts as empty: `sync()` creates it only to write a page, so an empty `pages` on a
  missing directory succeeds and writes nothing. Neither touches anything outside the directory.
  `sync` is the only function of `_datasets` that touches the file system; it makes no network
  call and no Deephaven call, and keeping it in the module keeps it under the unit tests and
  the coverage gate, which the script is not.
- **The command** `uv run python scripts/render_reference.py` regenerates the whole column
  reference and nothing else. The script starts the embedded Deephaven server (the API imports
  Deephaven, and the library never imports `deephaven_server`), imports the library, takes the
  catalog the session module builds from every namespace module, calls
  `render(catalog, _massive.provenance())` and then `sync()` on `docs/datasets/`, and prints
  what it wrote and deleted. It holds no logic of its own. A renamed or removed namespace or dataset therefore never leaves an
  orphaned page: the one command that writes the directory also empties it of everything it
  did not render.
- **The parity test**, `tests/datasets/test_column_reference.py`, renders the pages in the test
  JVM and fails on a stale page (bytes differ), a missing page, and any extra file or directory
  under `docs/datasets/`, naming the command that fixes it. A catalog change or a refresh that moves
  a description therefore lands with its regenerated pages in the same pull request.
- **Dataset method docstrings link, never list.** A column list in a docstring is a
  hand-written schema and goes stale. Each dataset method's docstring starts with its
  `DatasetSpec.summary`, explains the dataset, states its transports, `limit`, and `ALL`, and
  points to its section,
  `https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/<namespace>.md#<dataset>`,
  to the web UI's column tooltips, and to `columns()`. `tests/datasets/test_column_reference.py`
  checks that every declared dataset's docstring names `docs/datasets/<namespace>.md#<dataset>`
  for its own namespace and dataset. The summary is the docstring's whole first line: ruff's
  D205 requires a blank line after that line, so a sentence too long to fit on it, after the
  method's indentation and the opening quotes, within the 100-character line length, is
  shortened in the spec, never wrapped in the docstring.

## 8. Namespace tree

Notation:

- `Symbols = str | Sequence[str] | Table | _All` ([section 10](#10-symbols-and-all)).
- `DateLike = str | date | datetime` ([section 11](#11-enums-and-types)).
- `…F` is the trailing keyword-only block
  `sort: str | None = None, order: Order | None = None, limit: int | None = None, filters: Mapping[str, FilterValue] | None = None`.
  The block offers only the ordering the dataset's paths accept, so a signature never offers
  one the endpoint cannot apply: a dataset whose paths have neither a `sort` nor an `order`
  parameter (futures `exchanges` and `market_status`, for example) leaves both out, and a
  dataset whose paths have `order` but no `sort` (the indicators) keeps `order` alone. How
  `sort` and `order` reach the wire is in [section 9](#9-filters).
- `…T` is `start: DateLike | None = None, end: DateLike | None = None` followed by `…F`. It
  exists only on datasets whose row has a named time axis (shown as "axis"). `start` and `end`
  address that axis with the day rules of [section 12](#12-timestamp-units-and-day-boundaries),
  as `history()` does: both bounds dates (whole days) or both aware datetimes (exact instants,
  half-open), a date mixed with a datetime raises `ValueError` at construction, and so does a
  range that ends before it starts (two dates may be equal: one whole day; two equal datetimes
  are an empty range). A calendar-date axis (dividends, splits, filings, financials, futures
  `schedules`, and every other axis whose field is a calendar date) holds whole days, so a
  datetime bound there raises `ValueError` ("`<axis> is a calendar date; pass dates`"). A
  bound left None is open: unlike `history()`, an open `end` is not sent (owner decision), so a
  list keeps rows dated in the future: `m.stocks.dividends("AAPL", start="2025-01-01")` returns
  an announced dividend whose ex-date is still to come, unless `end=` is given.
- Each namespace module declares its datasets (`DATASETS: tuple[DatasetSpec, ...]`), and
  `_session.py` composes them, once, at import, into `CATALOG`, the process's `Catalog` (every
  `DatasetSpec` by name); a test asserts that every namespace module contributes. The query
  class in each table below follows from the dataset's transports
  ([section 7](#7-query-classes-and-verbs)).
- `limit` is a total-row cap. Page size is internal. Every docstring says so. The cap is
  stored apart from the wire parameters, in `Request.limit`, so it can never be sent as the
  endpoint's page-size `limit`; the page size is `DatasetSpec.page_size`, or `Request.limit`
  when that is smaller (`snapshots(ALL, limit=10)` asks for 10 rows, not 250), and paging
  stops once `limit` rows have arrived.
- The REST column names the path template as the OpenAPI spec writes it (the spec's own
  placeholder names, such as `{stocksTicker}`, are abbreviated here to `{t}`, `{o}`, `{u}`).
  Every path is a generated REST source (`_massive.source(Transport.REST, template)`,
  [section 19.4](#194-generated-schemas)). Each dataset names its paths by role (`fetch_path`,
  `detail_path`, `fetch_paths_by_class`, `history_path`, `grouped_daily_path`, `extra_paths`),
  and declares the key columns of each
  source it reads as `KeySpec`s ([section 19.4](#194-generated-schemas), "Keys are declared
  per source"): the `symbol`, `time`, and instrument `fanout` keys become `Symbol`,
  `Timestamp`, and `Underlying`, and a non-instrument `fanout` key keeps its column. A
  `time` key is always an instant, so `Timestamp` is an `Instant`. Every axis below that is
  a calendar date (all but the instants `published_utc`, `published`, and the indicators'
  `timestamp`: the dividends, splits, IPOs, short interest and volume, financial statements,
  filings, futures schedules, economy, and TMX axes) keeps its `LocalDate` column under its
  own name, such as `Date` or `ExDividendDate`, and declares no `time` key. Datasets whose
  rows each list several tickers (a JSON `tickers` field) describe a filing, a company, or
  an article, not one instrument: the three financial statements,
  `filings.initial_insider_ownership` and `filings.insider_transactions` (Forms 3 and 4),
  `filings.insider_activity` and `filings.insider_holdings`, `filings.disclosures_8k`,
  `reference.news`, and `partners.benzinga.news` have no `Symbol`, keep their `Tickers`
  column, and use `symbols` only as a request filter ([section 10](#10-symbols-and-all)).

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
| `share_float(symbols=None, *, …F)`, no axis (the path documents no date parameter; `EffectiveDate` is a column only) | Query | | `/stocks/vX/float` | |
| `financials.balance_sheets`, `financials.income_statements`, `financials.cash_flow_statements` — each `(symbols=None, *, timeframe: Timeframe \| None = None, …T)`, axis `period_end` | Query | | `/stocks/financials/v1/{balance-sheets,income-statements,cash-flow-statements}` | |
| `financials.ratios(symbols=None, *, …F)`, no axis (the path documents no `period_end`, `timeframe`, or `date` parameter; its rows carry one `ticker`, so it has `Symbol`) | Query | | `/stocks/financials/v1/ratios` | |
| `filings.index(symbols=None, *, form_type: str \| None = None, …T)`, axis `filing_date` | Query | | `/stocks/filings/vX/index` | |
| `filings.sections_10k`, `filings.text_8k`, `filings.disclosures_8k`, `filings.risk_factors` — each `(symbols=None, …T)`, axis `filing_date` | Query | | `/stocks/filings/10-K/vX/sections`, `/stocks/filings/8-K/vX/text`, `/stocks/filings/8-K/vX/disclosures`, `/stocks/filings/vX/risk-factors` | |
| `filings.institutional_holdings(*, filer_cik: str \| None = None, …T)`, axis `filing_date` (Form 13-F) | Query | | `/stocks/filings/vX/13-F` | |
| `filings.initial_insider_ownership(symbols=None, *, issuer_cik: str \| Sequence[str] \| None = None, owner_cik: str \| Sequence[str] \| None = None, …T)` (Form 3); `filings.insider_transactions(...)` with the same signature (Form 4); axis `filing_date`. A CIK argument sends one CIK as `issuer_cik` or `owner_cik` and a sequence as its `.any_of` form; an empty sequence raises `ValueError` (as futures `product_code`) | Query | | `/stocks/filings/vX/form-3`, `/stocks/filings/vX/form-4` | |
| `filings.insider_activity(symbols=None, *, issuer_cik: str \| Sequence[str] \| None = None, …T)`, axis `transaction_date`: each issuer's open-market insider purchases and sales summed per transaction day, across every insider; `poll()` replaces its table, keyed by `IssuerCik` and `TransactionDate` (a later filing for the same day revises that day's totals) | Query | | `/stocks/insiders/vX/activity` | |
| `filings.insider_holdings(symbols=None, *, issuer_cik: str \| Sequence[str] \| None = None, owner_cik: str \| Sequence[str] \| None = None, …T)`, axis `last_filing_date`: each insider's current consolidated holding of one issuer, built by Massive from all of the owner's Forms 3, 4, and 5; `poll()` replaces its table, keyed by `IssuerCik` and `OwnerCik`. Every insider dataset takes the title of its Massive page: Form 3 is Massive's "Initial Insider Ownership" | Query | | `/stocks/insiders/vX/holdings` | |
| `filings.risk_factor_taxonomy(…F)`, `filings.disclosure_taxonomy(…F)` | Query | | `/stocks/taxonomies/vX/risk-factors`, `/stocks/taxonomies/vX/disclosures` | |

### 8.2 `m.options`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols)` | StreamHistoryLatestQuery | `T` | fetch `/v2/last/trade/{o}`; history `/v3/trades/{o}` | `us_options_opra/trades_v1` |
| `quotes(symbols)` | StreamHistoryQuery | `Q` | history `/v3/quotes/{o}` | `us_options_opra/quotes_v1` |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1, adjusted=True)` | overloads | `A`, `AM` | `/prev` (DAY); range | `us_options_opra/{minute,day}_aggs_v1` |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/options`) | | |
| `chain(underlying: str, *, contract_type: ContractType \| None = None, …F)`; `filters` takes `strike_price_*`, `expiration_date_*` | Query | | `/v3/snapshot/options/{u}` | |
| `snapshots(symbols, *, …F)`; OCC contracts; `Underlying` from `underlying_asset.ticker`; `ALL` is every contract, paginated (slow, documented) | Query | | `/v3/snapshot?ticker.any_of=O:…`, 250 per request, always with `type=options`, so `ALL` lists only options contracts; `type` is set by the library and rejected as a filter. Same fields as the per-contract endpoint, including greeks. `/v3/snapshot/options/{u}/{o}` is listed in the catalog as covered by this dataset. | |
| `contracts(*, symbols=None, underlying: str \| None = None, contract_type: ContractType \| None = None, as_of: DateLike \| None = None, expired: bool \| None = None, …F)`; `filters` takes `expiration_date_*`, `strike_price_*` | Query | | `/v3/reference/options/contracts`; with `symbols`: `/v3/reference/options/contracts/{o}` per contract | |
| `daily_open_close(symbols, date: DateLike, *, adjusted: bool = True)` | Query | | `/v1/open-close/{o}/{date}` per contract | |

### 8.3 `m.futures`

Streams always use the `/futures` cluster path. `venue` exists on the three factory methods
below and selects the flat-file prefix only: it is not part of the stream key
([section 14](#14-publishers-fan-out-and-failure-semantics)). `venue=None` reads all four prefixes.

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols, *, venue: FuturesVenue \| None = None)` | StreamHistoryQuery | `T` | history `/futures/v1/trades/{t}` (dates: one request series per weekday session, `session_end_date=<date>`; datetimes: the exact `timestamp` range; [section 12](#12-timestamp-units-and-day-boundaries)) | `us_futures_{venue}/trades_v1` |
| `quotes(symbols, *, venue: FuturesVenue \| None = None)` | StreamHistoryQuery | `Q` | history `/futures/v1/quotes/{t}` (as `trades`) | `us_futures_{venue}/quotes_v1` |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1, venue: FuturesVenue \| None = None)` | overloads | `A`, `AM` | history `/futures/v1/aggs/{t}` (`resolution`; `window_start` in ns; dates: from the first session's open less 4 days to 18:00 CT on the last date, kept by `SessionEndDate`, and bars of a session or longer with `multiplier` above 1 refused with `ValueError`; datetimes: the exact `window_start` range, bars kept by their own window; [section 12](#12-timestamp-units-and-day-boundaries)) | `us_futures_{venue}/minute_aggs_v1`, `us_futures_{venue}/session_aggs_v1` |
| `snapshots(symbols, *, product_code: str \| Sequence[str] \| None = None, …F)`; `ALL` with `product_code` is every contract of the products; `Symbol` is the contract's top-level `ticker`, where both vendor samples send it, though the OpenAPI schema nests it as `details.ticker` (a `KEY_FIXES` entry; [section 23](#23-open-verification-items)) | Query | | `/futures/v1/snapshot` | |
| `contracts(*, product_code: str \| Sequence[str] \| None = None, …F)`, `products(*, product_code: str \| Sequence[str] \| None = None, …F)`, `exchanges(*, limit: int \| None = None)`, `market_status(*, product_code: str \| Sequence[str] \| None = None, limit: int \| None = None)` (neither path has a filter beyond the typed arguments, nor `sort`) | Query | | `/futures/v1/{contracts,products,exchanges,market-status}` | |
| `schedules(*, product_code: str \| Sequence[str] \| None = None, …T)`, axis `session_end_date` | Query | | `/futures/v1/schedules` | |

`product_code` is typed on every futures dataset whose path takes it (`snapshots`,
`contracts`, `products`, `market_status`, `schedules`), so none of its forms is a
`filters` key there. One code is sent as `product_code`; a sequence of codes is sent as
`product_code.any_of`, which every one of those paths takes, so several products still
need one request. An empty sequence raises `ValueError`.
`session_end_date` on `trades` and `quotes` is not their time axis: datetime bounds of
`history()` address `timestamp`, and date bounds name sessions, which `history()` requests one
at a time as `session_end_date=<date>`. The `trades` and `quotes` factories take no `filters`;
a query built with a `session_end_date` filter (any form) and date bounds is refused with
`ValueError` before any request, whatever the source, since the dates set it.

No futures dataset has a "last" endpoint, so no futures stream query has `fetch()`.
`snapshots(symbols).fetch()` is the one-request answer. There is no futures `day_aggs` flat
file; `Timespan.DAY` is rejected ([section 7.1](#71-bars-overloads)).

### 8.4 `m.indices`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `values(symbols)` | StreamHistoryLatestQuery | `V` | fetch `/v3/snapshot/indices` projected to `Value`, with `Timestamp` from `last_updated`; history flat files only | `us_indices/values_v1` |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1)` (no `adjusted`: the endpoint has none) | overloads | `A`, `AM` | `/prev` (DAY); range | `us_indices/{minute,day}_aggs_v1` |
| `snapshots(symbols, *, …F)` | Query | | `/v3/snapshot/indices`, 250 tickers per request | |
| `daily_open_close(symbols, date: DateLike)` | Query | | `/v1/open-close/{t}/{date}` | |

### 8.5 `m.forex`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `quotes(symbols)` | StreamHistoryLatestQuery | `C` | fetch `/v1/last_quote/currencies/{from}/{to}`; history `/v3/quotes/{C:…}` | `global_forex/quotes_v1` |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1)` (no `adjusted`: currency pairs have no corporate actions, so no request sends it and flat files serve every timespan they hold; a 1-day bar is the UTC day, stamped 00:00 UTC on every source: grouped daily, `/prev`, and `day_aggs_v1`'s 23:59:59.999 UTC are moved to the day's start, [section 12](#12-timestamp-units-and-day-boundaries)) | overloads | `CAS` (1 s), `CA` (1 min) | `/prev`; range; `ALL` with DAY: `/v2/aggs/grouped/locale/global/market/fx/{date}` | `global_forex/{minute,day}_aggs_v1` |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/forex`) | | |
| `snapshots(symbols)`; `ALL` is all pairs | Query | | `/v2/snapshot/locale/global/markets/forex/tickers` | |
| `movers(direction: Direction)` | Query | | `/v2/snapshot/locale/global/markets/forex/{gainers\|losers}` | |
| `conversion(from_currency: str, to_currency: str, *, amount: float = 1.0, precision: int = 2)` | Query | | `/v1/conversion/{from}/{to}` | |

### 8.6 `m.crypto`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols)` | StreamHistoryLatestQuery | `XT` | fetch `/v1/last/crypto/{from}/{to}`; history `/v3/trades/{X:…}` | `global_crypto/trades_v1` |
| `quotes(symbols)` | StreamQuery | `XQ` | | |
| `bars(symbols, timespan=Timespan.MINUTE, *, multiplier=1)` (no `adjusted`: crypto has no corporate actions, so the parameter is never sent; a 1-day bar is the UTC day, stamped 00:00 UTC on every source: `day_aggs_v1`'s 23:59:59.999 UTC is moved to the day's start, and any stamp elsewhere in the day than its first or last millisecond (the documented REST samples sit at midnight New York time or, for grouped daily, at times such as 20:48 UTC), or a REST bar of several days or longer not at 00:00 UTC, raises `MassiveError` naming the dataset, the source, and the other source; `AUTO` chooses as for every dataset; [section 12](#12-timestamp-units-and-day-boundaries), [section 23](#23-open-verification-items) item 1) | overloads | `XAS` (1 s), `XA` (1 min) | as forex; grouped `/v2/aggs/grouped/locale/global/market/crypto/{date}` | `global_crypto/{minute,day}_aggs_v1` |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/crypto`) | | |
| `snapshots(symbols)`, `movers(direction: Direction)` | Query | | `/v2/snapshot/locale/global/markets/crypto/{tickers,gainers,losers}` | |
| `daily_open_close(symbols, date: DateLike)` (no `adjusted`, as `bars`: the parameter is never sent) | Query | | `/v1/open-close/crypto/{from}/{to}/{date}` per pair | |

### 8.7 `m.reference`

All return `Query`.

| Method | REST |
|---|---|
| `tickers(*, symbols=None, market: Market \| None = None, ticker_type: TickerType \| None = None, exchange: str \| None = None, cusip: str \| None = None, cik: str \| None = None, date: DateLike \| None = None, active: bool = True, search: str \| None = None, …F)` | `/v3/reference/tickers`. `cusip` and `cik` are `str`, as the spec types them. `ticker_type` is sent as the wire parameter `type`. |
| `ticker_details(symbols, *, date: DateLike \| None = None)` | `/v3/reference/tickers/{t}` per symbol |
| `ticker_types(*, asset_class: AssetClass \| None = None, locale: Locale \| None = None)` | `/v3/reference/tickers/types` |
| `ticker_events(symbols, *, types: Sequence[str] \| None = None)` | `/vX/reference/tickers/{t}/events` per symbol |
| `related_companies(symbols)` | `/v1/related-companies/{t}` per symbol; rows are `{"ticker": peer}`, so `Symbol` is the peer and the requested symbol is `Underlying` |
| `news(*, symbols=None, …T)`, axis `published_utc` (half-open `.gte`/`.lt`) | `/v2/reference/news`. Its `ticker` parameter takes one ticker and the path has no `ticker.any_of`, so `symbols` is one symbol: a list of several raises `ValueError` (build one query per symbol) and a `Table` raises `TypeError` |
| `exchanges(*, asset_class: AssetClass \| None = None, locale: Locale \| None = None)` | `/v3/reference/exchanges` |
| `conditions(*, asset_class: AssetClass \| None = None, data_type: ConditionDataType \| None = None, condition_id: int \| None = None, sip: Sip \| None = None, …F)` | `/v3/reference/conditions`; `condition_id` is sent as the wire parameter `id` |
| `market_status()` | `/v1/marketstatus/now`; one row; nested maps flattened |
| `market_holidays()` | `/v1/marketstatus/upcoming` |

`AssetClass.FOREX` is sent as `fx` on these endpoints. Each endpoint accepts the classes its
generated `asset_class` enum lists, and any other class raises `ValueError` naming those it
accepts: `ticker_types` takes every class but futures, `exchanges` every class but indices, and
`conditions` neither futures nor indices.

### 8.8 `m.indicators`

All return `Query`. The symbol prefix selects the asset class. The axis is `timestamp`.

```python
sma(symbols, window: int, *, timespan: Timespan = Timespan.DAY,
    series_type: SeriesType = SeriesType.CLOSE, adjusted: bool | None = None,
    start: DateLike | None = None, end: DateLike | None = None,
    order: Order | None = None, limit: int | None = None,
    filters: Mapping[str, FilterValue] | None = None)
ema(...)   # same signature as sma
rsi(...)   # same signature as sma
macd(symbols, *, short_window: int, long_window: int, signal_window: int,
     timespan: Timespan = Timespan.DAY,
     series_type: SeriesType = SeriesType.CLOSE, adjusted: bool | None = None,
     start: DateLike | None = None, end: DateLike | None = None,
     order: Order | None = None, limit: int | None = None,
     filters: Mapping[str, FilterValue] | None = None)
```

REST: `/v1/indicators/{sma,ema,rsi,macd}/{t}` per symbol. The axis is half-open
(`timestamp.gte`/`.lt`). The indicator paths have an `order` parameter but no `sort`, so the
signature keeps `order` and has no `sort` ([section 9](#9-filters)). They have no multiplier,
so none is offered. `expand_underlying` is not offered either: the underlying bars it adds
are not rows of the table, so the library always sends `expand_underlying=false`
(`DatasetSpec.fixed_params`), which also reserves it from `filters`. `Timespan.SECOND` and `Timespan.SESSION` are rejected with
`ValueError`: the endpoints' timespan enum runs from `minute` to `year`. Crypto symbols are
served by the `{cryptoTicker}` templates, chosen per symbol through
`DatasetSpec.fetch_paths_by_class` ([section 13.4](#134-rest)). Those templates have no
`adjusted` parameter, so `adjusted` defaults to None, which sends nothing and leaves Massive's
default in force. An explicit `True` or `False` is sent where the chosen path declares
`adjusted`, and raises `ValueError` for a symbol whose path does not (a crypto symbol), rather
than being dropped silently.

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
| `benzinga` | `consensus_ratings(symbols, *, filters: Mapping[str, FilterValue] \| None = None)` | `/benzinga/v1/consensus-ratings/{t}` per symbol (the ticker is a path segment); `filters` reaches the path's `date` family |
| `benzinga` | `news(*, symbols=None, …T)`, axis `published` (an instant) | `/benzinga/v2/news` |
| `etf_global` | `analytics`, `constituents`, `fund_flows`, `profiles`, `taxonomies` — `(symbols=None, …F)` | `/etf-global/v1/{analytics,constituents,fund-flows,profiles,taxonomies}` |
| `tmx` | `corporate_events(symbols=None, …T)`, axis `date` | `/tmx/v1/corporate-events` |
| `fable` | `merchant_aggregates(…T)`, `merchant_hierarchy(…F)` | `/consumer-spending/eu/v1/{merchant-aggregates,merchant-hierarchy}` |

On the `etf_global` datasets `Symbol` is the fund the row describes (for `constituents`, the
ETF, not the holding). Each partner dataset's `poll()` key is a set of its own generated
columns that is unique per row, checked against the generated fields of its path.

### 8.11 `m.snapshots`

`m.snapshots(symbols, *, …F) -> Query` calls the unified `/v3/snapshot` with `ticker.any_of`,
250 tickers per request, any mix of asset classes. Rows are the union schema of the unified
snapshot. `ALL` is not accepted here (the endpoint requires tickers). Its catalog record is
named `reference.snapshots` and is declared in `_namespaces/reference.py` with
`asset_class=None` (each ticker's prefix selects its class,
§ 10), so it is in the catalog (`CATALOG` in `_session.py`); `m.streams()` and messages show
that name.

### 8.12 Excluded endpoints

Excluded items fall into two lists.

- **Executable exclusions** are entries `(kind, item, reason)` of `EXCLUSIONS` in
  `_massive/refresh/exclusions.py`, the single list for REST paths, WebSocket channels, and
  flat-file prefixes. Each names an item present in a vendor source at the pinned snapshot
  (the OpenAPI spec, `websocket.json`, `llms.txt` and the REST pages it links, or the
  flat-file pages). An entry that names an item the snapshot lacks fails the refresh as stale
  (the `stale-entry` gate, [section 19.4](#194-generated-schemas)). A path the OpenAPI spec
  itself marks deprecated (`x-polygon-deprecation`) or draft (`x-polygon-draft`) is accounted
  for by the spec and is never an entry; the rows below that name such paths record why
  nothing uses them. An entry whose reason is that no documentation page names the item is marked
  `undocumented`, and the refresh checks that claim against the snapshot both ways: a page
  whose `**Endpoint:**` line names the item fails it, as does a documented entry no page
  names.
- **Documentation-only exclusions** name endpoints that never existed or are absent from every
  vendor source. They are recorded here so nobody re-adds them, and never appear in
  `EXCLUSIONS`. A row of the first table that the snapshot shows absent from every vendor
  source moves to the second.

| Endpoint | Reason |
|---|---|
| `/v3/reference/dividends`, `/v3/reference/splits` | Deprecated; replaced by `/stocks/v1/*`. |
| Crypto `/book` REST endpoint; WebSocket `XL2` | Deprecated and sunset (the REST path is marked deprecated in the OpenAPI spec). |
| WebSocket `LV`; Launchpad hosts and paths | Product retired. |
| `/v1/historic/*`, `/v2/ticks/*` | Deprecated 2022 (marked deprecated in the OpenAPI spec); replaced by `/v3/trades` and `/v3/quotes`. |
| Form 5, per-asset `/v1/exchanges` (`/stocks`, `/options`, `/forex`, `/crypto`), `/options/v3/*`, `/v1/reference/ipos`, `/stocks/dev/*`, `vX_0` paths | Present only in the OpenAPI spec; no page `llms.txt` links names them (`undocumented`). |
| `/v1/reference/sec/filings*` | Marked `x-polygon-draft` in the OpenAPI spec and undocumented. |
| `/v1/indicators/{sma,ema,rsi,macd}/{optionsTicker,indicesTicker,fxTicker}` | The same URL, fields, and parameters as the `{stockTicker}` template, which serves every asset class by the ticker's prefix; a refresh test checks they still repeat it. The `{cryptoTicker}` templates document no `adjusted` parameter, so they are generated in their own right. |
| Single-ticker v2 snapshots `/v2/snapshot/.../tickers/{t}` | Covered by `snapshots(symbols)` with the `tickers` filter. |
| `/v3/snapshot/options/{u}/{o}` | Covered by `m.options.snapshots`; identical fields. |

Documentation only:

| Endpoint | Reason |
|---|---|
| `/benzinga/v1/transcripts` | Never existed. |
| `/v1/summaries` | Removed from the OpenAPI spec (2026-10-28); it was never in the documentation index. |
| `/benzinga/v1/news` | Sunset; replaced by `/benzinga/v2/news`. Absent from the OpenAPI spec and the documentation at the pinned snapshot. |
| `/vX/reference/financials` | Sunset; replaced by `/stocks/financials/v1/*`. Absent from the OpenAPI spec and the documentation at the pinned snapshot. |
| WebSocket `/futures/{venue}` | Not documented: the futures WebSocket pages list `/futures` only, and `websocket.json` has no such channel at the pinned snapshot. |

`x-polygon-ignore` excludes nothing. It is the vendor's hint to its own SDK generator not to
emit a second client method for one URL (the per-asset-class templates of `/v3/trades`,
`/v3/quotes`, `/v2/last/trade`, and the indicators); each such template has its own
documentation page, with its own documented fields, so it is generated like any other path.
Likewise `x-polygon-experimental` excludes nothing: the one path that carries it
(`/vX/reference/tickers/{id}/events`) is documented, and documented endpoints are generated.
The generator knows every vendor extension it accepts, each with the reason in `refresh/rest.py`;
an unknown one stops the refresh (`shape`).

**Coverage.** Everything documented is generated or excluded, and everything generated is
used or deferred:

- **documented = generated ⊎ excluded ⊎ (deprecated ∪ draft)**, tested against the
  pinned snapshot in `tests/massive/refresh/test_generate.py` (WebSocket channels and flat-file
  prefixes) and `tests/massive/refresh/test_rest.py` (REST paths, and every path a
  documentation page names is in the OpenAPI spec);
- **generated = used ⊎ deferred**, tested in `tests/datasets/test_catalog.py`: every generated
  item is read by a declared dataset or deferred (the coverage test, with a negative test that
  drops a dataset), and every item a dataset reads is generated (the generated-sources gate).
  A deferred item is a `DEFERRED` entry of the coverage test: the generated source key, a
  reason, and the number of the issue that will add its dataset, both required. An entry that
  names an item not generated, or one a dataset reads, fails the test, so the list cannot go
  stale. A new generated item, from a refresh that takes a vendor addition, therefore fails the
  coverage test until a dataset reads it, a `DEFERRED` entry defers it, or an `EXCLUSIONS`
  entry, with its reason, keeps the refresh from generating it.

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
joined with commas (a `date` item of a list of dates, such as `date_any_of`, as its ISO-8601
day); dates are ISO-8601; an aware `datetime` becomes the parameter's documented form (an exact
integer count of the parameter's `unit` for a timestamp parameter, ISO-8601 in UTC with a `Z`
otherwise), and a naive one raises `ValueError` (§ 11).

Validation is synchronous at query construction, against the generated parameters of the
dataset's paths (`RestPath.params` of `spec/rest.json`,
[section 19.4](#194-generated-schemas)), which carry each
parameter's kind and, for an enum parameter, its values. A dataset's accepted filters are the
union over its paths, minus the parameters the library sets itself: path segments; the
equality forms of the symbol parameter (its base name, `.any_of`, and `.all_of`: `ticker`,
`ticker.any_of`, `ticker.all_of`), while its range forms (`ticker.gte`, `ticker.lt`) stay
filters, since `symbols` cannot express an alphabetical range; the whole family of the time
axis (its base name and every `.gt`, `.gte`, `.lt`, `.lte`, `.any_of`, and `.all_of` form,
so `date.any_of` is reserved too although `start` and `end` cannot express a list of dates);
`limit`, `sort`, `order`; and the whole family of every typed keyword parameter of the factory
(its base name and its `.gt`, `.gte`, `.lt`, `.lte`, `.any_of`, and `.all_of` forms, so
`product_code_any_of` is rejected on a futures dataset that types `product_code`). An unknown
key, a value of the wrong kind, or a value outside a parameter's enum raises `ValueError` naming
the valid keys or values (the valid keys are only those the caller can pass: every reserved key
is left out); a key reserved by a typed parameter names that parameter. The check
first puts each value in the form that is sent: ISO-8601 text of a `DATE` or `TIMESTAMP`
parameter becomes a `date` or an aware `datetime` (`dates.parse_date_like`), and a `LIST`
value is split into its comma-separated items, with the spaces around each removed, so
`"a,b"`, `"a, b"`, and `("a", "b")` are one value and one query key (the wire joins a list
with commas, so no item can hold one); an empty item (`"a,,b"`) raises `ValueError`. It then
runs the wire package's own encoder (`encode_value`) on that value, so everything the encoder
refuses (a non-finite number, an empty list, an instant finer than a timestamp parameter's
unit) fails here, at construction, with the encoder's message. `Request.filters` holds the
parsed values, never the caller's text, so validation and encoding cannot disagree. Every path
of the dataset that declares the key checks the value, since any of them may send it: one that
cannot encode it fails the check and is named in the message, and paths that read it as
different values (a date parameter on one, a text one on another) fail it too.

A `date` on a range filter (`_gt`, `_gte`, `_lt`, `_lte`) of a parameter that compares
instants (a timestamp parameter, or a date-typed one over an instant field, such as
`last_updated_gte` on the benzinga paths) keeps its calendar meaning and never leaves the day
boundary to Massive: the REST transport sends `_gte` a day as the day's start, `_lt` a day as
the day's start, `_gt` a day as the day's end, and `_lte` a day as the day's end, where a day
starts at 00:00 in the request's day zone ([section 12](#12-timestamp-units-and-day-boundaries))
and its end is the next day's start less one unit of the parameter. The request keeps the
date, since the zone follows each request's symbols. Where no unit can end a day exactly (a
nanosecond timestamp, or ISO-8601 text, whose precision Massive does not document), `_gt` and
`_lte` with a date raise `ValueError` at construction, naming the `_gte` and `_lt` forms. The
base name and `.any_of` keep the date. An aware `datetime` is always sent as given. The
reservation is one shared check in the filter validation, fed the dataset's typed parameters,
never a guard in a namespace module. Parameters the library always sets itself (the
`type=options` of `options.snapshots`) are reserved the same way. When Massive adds a filter,
refreshing the snapshot adds it; no code changes.

**`sort` and `order`.** Most list endpoints have no `order` parameter: the direction is an
`.asc` or `.desc` suffix inside `sort`. On such a path the REST transport folds the two, so
`sort="date", order=Order.DESC` sends `sort=date.desc`, and `sort` alone is sent unchanged. A
path that has its own `order` parameter (`/v3/snapshot`, `/v3/snapshot/indices`, `/v3/trades`,
the reference lists such as tickers, news, and conditions, the indicators) receives `sort` and
`order` as they are, and `order` alone is valid there. `order` without `sort` raises
`ValueError` only where it would have to fold and there is nothing to fold it into: at query
construction when none of the dataset's REST paths has an `order` parameter, and from the verb
when the path chosen for a request (a per-symbol detail path, a path per symbol class) has
neither. Which form applies is read from the path's generated parameters, never from a hand
list. A dataset whose paths have neither parameter offers
neither; one whose paths have `order` but no `sort` (the indicators) offers `order` alone
([section 8](#8-namespace-tree)).

`start` and `end` map to the wire parameter pair recorded per dataset in the catalog
(`DatasetSpec.time_axis`). The pair follows the axis: an instant axis is half-open, `.gte` and
`.lt` (`timestamp.gte`/`timestamp.lt`, `published_utc.gte`/`published_utc.lt`,
`published.gte`/`published.lt`), and a calendar-date axis is whole days, `.gte` and `.lte`
(`ex_dividend_date.gte`/`ex_dividend_date.lte`, `filing_date.gte`/`filing_date.lte`,
`session_end_date.gte`/`session_end_date.lte` on `futures.schedules`, and so on). A
calendar-date axis takes date bounds only; a datetime there raises `ValueError` at
construction ([section 8](#8-namespace-tree)). Whether an
axis is an instant axis follows from the generated row field its parameter filters (the field
whose wire key is the parameter's base name), not from the parameter's kind: Massive types the
`published_utc` and benzinga `published` filters as dates, but their fields are ISO-8601
instants, so they are instant axes, and a date bound on them is sent as the instant of
[section 12](#12-timestamp-units-and-day-boundaries), never as a bare date. Bars whose
range path has `{from}` and `{to}` segments declare no `time_axis`: `history()` fills those
segments instead ([section 13.4](#134-rest)). `futures.bars`, whose path
`/futures/v1/aggs/{ticker}` has no such segments, declares the instant pair
`window_start.gte`/`window_start.lt`. Passing any form of the same axis
through `filters` (as `timestamp_gte`, `timestamp_lte`, or `timestamp`, for example) raises
`ValueError`.

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
  (`BTC-USD`) can always be recovered; others raise `ValueError` at construction. A forex
  currency is three letters (`EU1/USD` raises; crypto codes such as `1INCH` keep their
  digits). An option symbol's `YYMMDD` expiry must be a real date (`AAPL250132C...` raises).
  A prefix with nothing after it (`"I:"`, `"O:"`, `"C:"`, `"X:"`) raises `ValueError`.
  WebSocket topics spell a forex pair `{from}-{to}` (`C.EUR-USD`), as Massive's WebSocket
  documentation gives every forex channel's `ticker` parameter; forex quote and aggregate
  frames spell it `EUR/USD` and FMV frames `C:EURUSD`, and every spelling maps back to
  `C:EURUSD`, so frames reach their streams. A symbol
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
  (`canonical_view` in `_tables/watchlist.py`), so `last_by()`'s `where_in` filter and the subscribed topics
  agree on `C:EURUSD` whether the row says `EUR/USD` or `C:EURUSD`. A row that is not a valid
  symbol for the class matches nothing and is logged once at WARNING.
- `Symbol` is always the instrument the row describes. When the fan-out key is a different
  instrument, it is `Underlying` on every options row (from `underlying_ticker` or
  `underlying_asset.ticker`) and on `related_companies` rows. A non-instrument fan-out key
  keeps the generated column the dataset declares as its `fanout` key (`filer_cik` becomes
  `FilerCik`), declared with `KeySpec(..., instrument=False)`
  ([section 19.4](#194-generated-schemas)). Where a source does not carry the instrument in its rows (it takes the symbol
  as a path parameter, as `/v3/trades/{t}` and `/v2/aggs/ticker/{t}/…` do, or holds it outside
  the result key, as `/v1/last/crypto` and `/v1/last_quote` do), `Symbol` is filled from the
  request ([section 19.4](#194-generated-schemas)). A dataset whose rows describe no instrument
  (`reference.exchanges`, `economy.*`) has no `Symbol` column. Nor does a dataset whose rows
  each list several tickers, because no single instrument is the row's: the three financial
  statements, Forms 3 and 4, the 8-K disclosures, `reference.news`, and
  `partners.benzinga.news` keep their `Tickers` column (JSON text), and `symbols` there only
  filters the request (`tickers.any_of` or `ticker`). `Symbol` is never a list.

## 11. Enums and types

**Python floor:** `requires-python = ">=3.11"`. `StrEnum`, `datetime.UTC`, `typing.Self`,
`tomllib`, and exception groups are native. The CI matrix is 3.11, 3.12, 3.13, 3.14, and 3.14t;
the free-threaded 3.14t job runs with `PYTHON_GIL=0` and is required. 3.14t is the only
free-threaded target: pyarrow, numpy, and pandas no longer publish cp313t wheels.

**Criterion.** Every closed set of values in a public signature is a `StrEnum` whose values are
Massive's wire strings. Signatures take only the enum. There are no string `Literal`
categoricals and no `Enum | Literal` unions. Open sets (`sort`, `form_type`) are
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
| `ContractType` | CALL, PUT | `call`, `put` | `options.chain`, `options.contracts` |
| `SeriesType` | OPEN, HIGH, LOW, CLOSE | `open`, `high`, `low`, `close` | `indicators.*` |
| `Timeframe` | ANNUAL, QUARTERLY, TRAILING_TWELVE_MONTHS | `annual`, `quarterly`, `trailing_twelve_months` (the income and cash flow statements document all three, the balance sheets the first two) | `stocks.financials` statements |
| `Order` | ASC, DESC | `asc`, `desc` | the `…F` block |
| `Market` | STOCKS, OTC, INDICES, FX, CRYPTO | `stocks`, `otc`, `indices`, `fx`, `crypto` | `reference.tickers` |
| `TickerType` | CS, ADRC, ADRP, ADRR, UNIT, RIGHT, PFD, FUND, SP, WARRANT, INDEX, ETF, ETN, OS, GDR, OTHER, NYRS, AGEN, EQLK, BOND, ADRW, BASKET, LT | the same codes, upper case | `reference.tickers` (`ticker_type`, sent as `type`); checked against the generated `type` enum of `/v3/reference/tickers` |
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
`datetime` passed as a `filters` value or a typed parameter. ISO text with more than six
fractional digits of a second raises `ValueError` too: a `datetime` holds microseconds, and
cutting the rest would move the time. A range (`history()` and the `start`/`end` of a `…T`
dataset) takes two bounds of one kind: two dates are whole days, two datetimes exact instants,
and a date mixed with a datetime raises `ValueError`
([section 12](#12-timestamp-units-and-day-boundaries)).

**ISO-8601 text.** A timestamp the wire carries as ISO-8601 text (for example `published_utc`)
becomes an `Instant`. It must carry an offset (`Z` or `±HH:MM`): text without one is an
unconvertible value ([section 19.5](#195-runtime-drift)), never read as UTC, and a field that
really is offset-less UTC needs a named `TYPE_FIXES` conversion. The generator's
representation rules ([section 19.4](#194-generated-schemas)) decide which fields are ISO
`Instant`s and which are dates; any other text timestamp needs a `TYPE_FIXES` entry.

**Column types.** Every column's type comes from the type lattice of
[section 19.4](#194-generated-schemas): each generated field has a `WireType`, the merge joins
the `WireType`s of one column across a kind's sources, and `_datasets/schema.py` maps the result to a
Deephaven column type (`long`, `double`, `boolean`, `String`, `LocalDate`, `Instant`, `int[]`).
Sizes come out as `double` wherever any source says `number`; condition codes come out as
`int[]`; exchange and tape ids come out as `long`.

## 12. Timestamp units and day boundaries

The unit of every timestamp-like wire field is a fact about Massive's wire format. It is the
hand-verified `UNITS` table in `_massive/refresh/overrides.py`, and the generator writes it onto
each generated field, so callers read it as `source(...).field(column).unit`. A timestamp-like
field without a `UNITS` entry fails the refresh (the `timestamp-unit` gate,
[section 19.4](#194-generated-schemas)). The table is complete for every generated source, so no
dataset planned PR edits it.

The OpenAPI spec tags some integer fields with a unit type in `x-polygon-go-type`
(`INanoseconds`, `IMicroseconds`, `IMilliseconds`, `IDaysPolygonDateString`), but the tags are
not reliable: all 20 `IMicroseconds` tags sit on indicator `timestamp` fields whose descriptions
say "Unix Msec", and the indicator responses carry milliseconds. The tags are a cross-check: a
`UNITS` entry that differs from its tag is listed, with its reason, in the tag-conflict list
beside the table, and the indicator `timestamp` fields are its first entries.

| Transport and fields | Unit |
|---|---|
| WebSocket stocks `T`/`Q` (`t`, `pt`, `trft`); `A`/`AM` (`s`, `e`); options `T`/`Q`; futures `T`/`Q` (`t`); indices `V` (`t`) and `A`/`AM`; forex `C`/`CA`/`CAS`; crypto `XT`/`XQ`/`XA`/`XAS` (`t`) | ms (documented as Unix ms on each documentation page; 13-digit samples) |
| WebSocket futures `Q` `bt`, `at`; crypto `XT`/`XQ` received time `r` | ms (the descriptions name no unit; the samples have 13 digits, as do the other times of the same message) |
| WebSocket `NOI` `t` | ns (documented as Unix ns; the sample has 19 digits) |
| WebSocket `LULD` `t` | **UNVERIFIED**: documented as Unix ms, but the sample (`1764086430905642800`) has 19 digits, which as ms would overflow; registered as ns. The overflow check and the bound fail loudly on a mismatch. |
| WebSocket `FMV` `t` | Documented as ns; the documentation sample is `1678220098130` (13 digits, ms). Rule, FMV only: `t >= 10**17` means ns, otherwise ms. Every ns value after 1973-03-03 is at least 10^17, and an ms value reaches 10^17 only in the year 3,170,000, so the rule cannot misclassify. Logged once; tested. |
| REST `/v3/trades`, `/v3/quotes`, `/v2/last/trade`, `/v2/last/nbbo`; snapshot `last_updated`, `fmv_last_updated`; futures `/trades` and `/quotes` (`timestamp`, `bid_timestamp`, `ask_timestamp`); futures aggregates `window_start` (documented as "nanosecond Unix timestamp"); futures snapshot `last_quote` and `last_trade` times; stocks v2 snapshot `updated`, `lastTrade.t`, `lastQuote.t` | ns |
| REST aggregates `t`; `/prev` `t`; indicators `timestamp`; futures snapshot `last_minute.last_updated`; every v2 snapshot `min.t`; forex and crypto v2 snapshot `updated`, forex `lastQuote.t`, crypto `lastTrade.t` (13-digit documentation samples) | ms |
| REST forex `/v1/last_quote` `timestamp`; crypto `/v1/last` `timestamp`; forex `/v1/conversion` `last.timestamp` (the same `last` object as `/v1/last_quote`) | **UNVERIFIED** (`unverified-ms`). The spec tags the `last.timestamp` fields `IMilliseconds`, which agrees; registered as ms with a fixture test. |
| Flat files: all tick files; `*_aggs_v1.window_start`; `us_indices/values_v1.timestamp` (the documentation says ms, the sample is 19 digits) | ns |

**The timestamp routine.** One routine in `_datasets/timestamps.py` converts every epoch-integer
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

**`history(start, end=None)` bounds.** A range has two kinds of bounds, and one call never mixes
them:

- **Dates** (`"YYYY-MM-DD"` or a `date`) are whole days, inclusive at both ends:
  `history("2025-03-03", "2025-03-03")` is all of March 3. A day is a calendar day in the class
  day zone below; for futures a date names the trading session whose `session_end_date` it is.
- **Aware datetimes** are exact instants, a half-open range `[start, end)`: a row is kept when
  `start <= Timestamp < end`.

A date mixed with a datetime raises `ValueError` ("start and end must both be dates or both be
datetimes: ..."): a date means a whole day or session and a datetime one instant, and a futures
session's first instant moves with the holiday calendar, which the library does not have. There
is no inclusive or exclusive option. `end=None`, the default, reads through now: the current
instant after a datetime `start`, and after a date `start` the current day in the day zone (for
futures, the session the current instant belongs to). A reversed range (`start` after `end`, or
after now when `end` is None) raises `ValueError`. Two equal datetimes are an empty range: the
call returns an empty table with every column and makes no request. Two equal dates are that
one day. Datetimes are compared in UTC, so the repeated hour of a daylight-saving fall-back is
ordered correctly. `dates.bounds` normalizes the bounds to a `DaySpan` (dates) or a `TimeRange`
(datetimes), and `history_source.history_window` turns them into the rows to keep, the
partition dates, and the instants a REST request covers (`HistoryWindow`).

**Bars: whole windows.** A bar, of any timespan and multiplier, is returned only when its whole
window lies inside the range. The windows:

- An intraday bar (second, minute, hour) spans `multiplier` units from its `Timestamp`, on whole
  units of UTC time (whole hours in every zone the library uses).
- A 1-day bar is its calendar day in the class day zone; a bar of `multiplier` days spans that
  many days. Datetime bounds therefore keep only whole calendar days of 1-day bars: Monday
  09:30 to Friday 16:00 New York time returns Tuesday to Thursday, so daily bars are asked
  for with dates.
- For stocks, options, and indices, which do not trade at weekends, a week runs Monday to
  Friday, and a bar Massive stamps on the Saturday or Sunday before a week names that week;
  months, quarters, and years are calendar periods less their leading and trailing weekend
  days, so March 2024 runs from Friday the 1st to Friday the 29th. Forex (which trades from
  Sunday evening UTC) and crypto (every day) keep their weekends: Monday-to-Sunday weeks and
  whole calendar months, quarters, and years. A forex week is the calendar week until the
  live check of Massive's forex week bucket ([section 23](#23-open-verification-items))
  settles where it starts. A bar of `multiplier` periods spans that many.
- A futures bar named by session dates is kept by its `SessionEndDate`: an intraday bar when
  its session lies in the range; a session, week, month, quarter, or year bar when the whole
  period of session dates its `SessionEndDate` belongs to (Monday to Friday for a week; months
  less their weekend days) lies in the range. A futures bar of a session or longer with
  `multiplier` above 1 named by dates raises `ValueError` ("pass datetimes, or
  multiplier=1"): whether Massive labels it with its first or its last session is not
  verified ([section 23](#23-open-verification-items)), and either choice keeps the wrong
  bars under the other. A futures bar named by datetimes follows the intraday rule, or for
  longer bars the sessions of the period, from the regular 17:00 Chicago open of its first
  session to that of the session after its last.

A bar whose window has not ended yet is never returned, so the current day, week, month,
quarter, or year is left out whatever `end` says (a forex month that ends on a Sunday is
unfinished until that Sunday ends); for futures named by date, only finished sessions count
(the sessions before the one the current instant belongs to). There is no holiday calendar:
a holiday is a weekday like any other, so a week whose Friday is a holiday still runs Monday
to Friday, and monthly stock bars for March 2024 need a range through Good Friday, 03-29.
The REST aggregates' `{from}` is the first instant that belongs to the first bar that fits
(the Saturday before a Monday-to-Friday week, the 1st of a month even when that is a
weekend), the start of the kept range: whether Massive selects bars by their stamp or builds
them from `{from}`, the first whole period's bar is returned, and a partial bucket begun
before it is dropped. `{to}` is the last whole millisecond before the range's end. Rows outside
the window are then dropped.

**Day labels.** A stock, option, or index 1-day bar is its New York date. A futures bar is its
`SessionEndDate`. A forex or crypto 1-day bar means the UTC day on every source, and is stamped
at 00:00 UTC of that day (`history_source.stamp_rule`, `timestamps.utc_day_stamps`): the
forex range already is, and the forex grouped daily and `/prev` (23:59:59.999 UTC in the
samples) and the `day_aggs_v1` flat files (23:59:59.999 UTC) are moved to the day's start.
Crypto's day cut is not verified: the documented range and `/prev` samples sit at midnight New
York time and the grouped-daily samples at times such as 20:48 UTC, no day boundary at all.
So a crypto 1-day bar stamped anywhere in its UTC day but its first or last millisecond, and a
crypto REST bar of several days or longer not stamped at 00:00 UTC
(`timestamps.check_utc_midnights`), raises `MassiveError` naming the dataset, the source, and
the other source to try; it is never relabelled, since it would cover other days than its
label. If live REST data follows those samples, crypto daily REST history, `dm.ALL` grouped
daily, and `/prev` all raise until the owner decides after the live check
([section 23](#23-open-verification-items) item 1). `AUTO` picks the source as for every other
dataset ([section 16](#16-historysourceauto)): the check, not a fixed source, keeps both
sources on the same UTC days.

**One-day parameters.** A dataset parameter that names one calendar day (`daily_open_close`'s
`date`, `options.contracts`' `as_of`, `reference.tickers`' and `ticker_details`' `date`)
takes a date only, like a calendar-date axis: a `datetime` raises `ValueError` ("`<name>` is a
calendar date; pass a date", `dates.calendar_day`).

The same kinds bound the time axis of a `…T` dataset on an instant axis: a timestamp
parameter, or a date-typed parameter over an instant field (`published_utc`, benzinga
`published`). A date `start` is sent as that day's 00:00 in the request's day zone and a date
`end` as the next day's 00:00, with `.gte` and `.lt`, so Massive never decides where a day
starts. A date-typed parameter takes the instant as
ISO-8601 text in UTC (`2025-06-02T04:00:00Z`); since Massive documents no
precision for that text, such an axis cannot be declared with `.gt` or `.lte` (the request
fails with `ValueError`). A calendar-date axis (`period_end`, `ex_dividend_date`, the futures
`schedules`' `session_end_date`) keeps whole-day date bounds with `.gte` and `.lte` and takes
dates only: a datetime bound there raises `ValueError` at construction ("`<axis> is a calendar
date; pass dates`"). An open bound of a `…T` dataset is not sent.

**The request's day zone** (`dates.day_class`). A dataset with an asset class follows its
class. A dataset without one uses New York days, with one exception: a dataset that declares a
path per symbol class (`fetch_paths_by_class`, the indicators, whose crypto bars have UTC
days) serves class-specific data, so each REST request, and `history()`, follows the class its
own symbols share (`symbol_class`): a crypto or forex ticker's days are UTC days, a stock,
option, or index ticker's New York days, and a request with symbols of several classes uses
New York. A filter on a publication time does not depend on which ticker the articles
mention, so `m.reference.news(symbols="X:BTCUSD", start=...)` asks for a New York window.

**Day zone per class.**

- **America/New_York** for stocks, options, and indices. Tick endpoints get ns bounds;
  flat-file partition dates are the New York dates of the first and last instant kept.
- **UTC** for forex and crypto.
- **Sessions** for futures. A date means the trading session whose `session_end_date` equals
  it. The futures trading day rolls at 17:00 America/Chicago (the weekly open is Sunday 17:00
  CT; 16:00 to 17:00 CT is the daily maintenance break), so the session of an instant `t` is
  `(t in America/Chicago + 7 h).date()`. An exchange holiday folds into the next session,
  which then opens early: CME traded Martin Luther King Day (Monday 2025-01-20) from Sunday
  17:00 CT under the trade date 2025-01-21, and after Good Friday 2024 the 2024-04-01 session
  opened on Thursday 03-28 at 17:00 CT.
  - **Trades and quotes named by dates** are requested one session at a time:
    `session_end_date=<date>` per weekday session and symbol, following `next_url`, with no
    time window, which is exact however early a holiday opened the session. Saturdays and
    Sundays end no session and are skipped. The rows come in session order and, within a
    session, in symbol order. A query that also filters `session_end_date` raises
    `ValueError`.
  - **Bars named by dates** are requested on `window_start` from the first session's regular
    open less `FUTURES_HOLIDAY_MARGIN` (4 days: the longest known fold, Good Friday's, opens
    three days early) to 18:00 CT on the last session date, and kept by `SessionEndDate` as
    above. There is no probe for earlier sessions.
  - **Datetimes** are exact: `timestamp.gte`/`.lt` for trades and quotes, `window_start.gte`/
    `.lt` for bars, with the bars kept by their own window afterwards, since Massive may
    match a candle by its start.
  - **Flat files** are assumed to be keyed by session date (**UNVERIFIED**: the futures
    flat-file overview says only "standardized in Central Time"; every block of every futures
    file is checked against its key, and a `session_end_date` that differs from the key's date,
    or is empty, fails loudly). Dates read one file per session date and never roll forward:
    a date names its own session exactly. Datetimes read the sessions from that of `start` to
    that of `end - 1 µs`, so an intraday window reads one file and an evening window only the
    next session's file. When the run of missing files that ends such a range (per venue)
    holds a weekday (a holiday, alone or followed by a weekend, such as Good Friday and the
    Saturday after it), the next existing session file within 7 days of the run's first day is
    read too, because the holiday's hours lie in it; its rows are filtered on `Timestamp` like
    the others. A run of weekend days after an existing Friday file reads nothing more. If that
    next file is not published yet, a missing weekday file on the way may still be late
    (within a day of its publication time), or no file exists within 7 days, the call raises
    `MassiveError`: the range's last hours may lie there, so it never returns a silently short
    table.

**Long downloads.** Grouped daily (`dm.ALL` 1-day bars over REST) makes one request per day,
skipping Saturdays and Sundays for stocks, options, and indices (forex and crypto keep them).
A per-session or grouped-daily download of more than five requests first logs one INFO line
with the number of requests and the minutes a Basic key's 5 requests a minute make it last
(the first five go at once). A per-session download counts at least one request per session
and symbol, more where a session spans several pages, so its line says "at least".

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
Each new decision writes the pair's `Transport=REST` row of `m.connections()`. The cache is
the one place a decision lives: the explicit-verdict rule of
[section 13.3](#133-connection-and-subscription-protocol) records its re-routed decision there
through `RestTransport.record_verdict(asset_class, family, decision)`, which replaces the cached
decision under the same leaf lock (a caller already waiting on the earlier decision still gets
that one) and rewrites the pair's row with the new host, `Recency=DELAYED`, and evidence
`"server verdict: <message>"`.

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
(`tests/ingest/test_hosts_integration.py`, skipped without a key) verifies the host.

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
reconnecting with backoff, and closing. `_ingest/ws.py` owns everything the library decides: the
desired topics, the acknowledgements, the watchdogs, the routing, and the connection's
diagnostics. Both run on the client's loop thread; other threads reach a connection only through
`loop.call_soon_threadsafe` with plain data, and through `WsClient.close()`.

The client reports to `_ingest/ws.py` through four callbacks, each called on the loop thread, in order:

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
  `WAITING_SLOT` (with the next attempt's time; the auth reply was `max_connections`, or the
  server sent a `max_connections` status before closing an authenticated socket), `FAILED`
  (with the `WireError`; also any internal failure, such as a callback that raised, with the
  original exception as its `__cause__`), or `CLOSED` (after `close()`). `FAILED` and
  `CLOSED` are terminal, and exactly one of them is reported. The client is the only owner of
  `WAITING_SLOT`; the library displays what it reports.

`_ingest/ws.py` parses no frame text. Before it first subscribes a channel on a connection, it calls
the client's loop-thread method `set_required(channel, keys, *, symbol=None)`: `keys` is a
`frozenset` of long-form key columns, built by `_ingest/ws.py` from the `KeySpec`s that the channel's
dataset declares for that channel's source, excluding `from_request` keys, and `symbol` is the
long-form column of the channel's `symbol` key (`None` when it declares none). The client keeps
both across generations and hands them to the decoder, so a row missing one of those keys is a
`Reject` carrying the row's raw symbol, while any other absent field becomes null. A channel
with no entry has no required keys and no symbol. Wire symbols are never rewritten in
`_massive`: `_ingest/ws.py` maps the raw values of rows and rejects to canonical symbols with
`_datasets/symbols.py` when it routes them, and topics use the wire's own form, as the subscribe frames
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
  even while the wildcard keeps them off the wire, so removing the wildcard never pushes a
  contract past the cap.

Responsibilities:

| Step | Owner |
|---|---|
| Open the socket (TLS, proxy, keepalive pings), read `connected`, send auth, read the auth reply, all within the handshake timeout | client |
| `auth_failed` | The client reports `FAILED` with `WireAuthError` and stops; it never retries a rejected key. The library fails every stream on the path with `AuthenticationError` (`_errors.from_wire`). |
| `max_connections` in the auth reply | The client reports `WAITING_SLOT`, lets the server close the socket, and retries after its backoff; streams stay alive. |
| `max_connections` as a status after authentication | The client reports `Status(MAX_CONNECTIONS)`, remembers it for the generation, and when that generation's socket closes reports `WAITING_SLOT` instead of `RECONNECTING`. The library only displays the state. |
| Reconnect after every close or failure that `close()` did not request and that is not fatal, after the backoff of [section 19.2](#192-the-websocket-client) | client |
| Classify a failed upgrade as fatal or retryable (section 19.2) | client; a fatal one is reported as `FAILED` with `WireError(status=<http>)`, which the library turns into a `MassiveError` naming the HTTP status and the host |
| Decode frames against `ws.json`; report rows, rejects, and drift | client |
| Map raw wire symbols to canonical symbols (`_datasets/symbols.py`) when routing rows and rejects | library |
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
   -> nothing for the library to do: the client reports WAITING_SLOT once its socket closes
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

Then: record `HostDecision(delayed host, DELAYED, "server verdict: <message>")` through
`RestTransport.record_verdict(asset_class, family, decision)`, which replaces the one cached
decision of the pair and rewrites its `Transport=REST` row; log a WARNING; move the connection's
streams to the delayed host's connection, which starts after the backoff; close the old one;
and do not fail the streams. The streams' `m.streams()` rows show the new host. Since the pair
then resolves to `DELAYED`, this happens at most once per (class, family) per session. In every other case the verdict is an
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
  `WireError(status=429)`, which becomes `MassiveError("rate limited ...")` with `status=429`. The REST transport (`_ingest/rest.py`, not the
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
- **`poll()` cycles.** A cycle that raises `EntitlementError` or `AuthenticationError`, hits
  a data failure of [section 19.5](#195-runtime-drift) (a missing required key, an
  unconvertible value, a timestamp out of bounds, or a null or NaN `poll_keys` value, in every
  mode), or plans a request with a parameter the
  chosen path does not take (a reactive symbol table's new symbol routed to a path that lacks
  an explicit parameter; retrying cannot fix it) fails the table through `publish_failure`.
  `poll()` checks synchronously whatever does not depend on the symbols: with static symbols
  every path, with a reactive table the parameters against the dataset's fetch paths, at
  least one of which must take every one of them. An emptied symbol table plans no request.
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

How a request is built from a `DatasetSpec` and a `Request`:

- **Path parameters.** The symbol fills its own segment (`{stocksTicker}`, or `{from}` and
  `{to}` of a pair). Every other path parameter (`direction`, `from`, `to`, `amount`,
  `precision`, `date`) travels in `Request.params` under its wire name and fills the path's
  `{name}` segment; it is not sent again as a query parameter. A one-day path parameter is
  turned into a calendar date by `calendar_day` in `_datasets/dates.py`: a date or
  `"YYYY-MM-DD"` as given; a `datetime` raises `ValueError`, since a calendar date is a whole
  day ([section 12](#12-timestamp-units-and-day-boundaries)).
- **History order.** REST `history()` returns each request's rows, so each symbol's, oldest
  first; the rows of several requests follow one another in request order (futures sessions
  named by date: session by session, and the symbols of one session in symbol order).
  Wherever the history path takes a direction and the caller set none, REST sends the ascending one on
  the time axis's field when the path lists it (`order=asc` with `sort=timestamp`;
  `sort=asc` on the aggregates; `sort=window_start.asc` or, where `sort` lists no values,
  `<time axis field>.asc` on the futures paths, which default to newest first). The order of
  rows inside a flat file is Massive's and is not documented.
- **Library-fixed parameters.** A parameter the library sets itself, with no argument of its
  own (the options snapshot's `type=options`), is declared on the dataset as
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
  [section 9](#9-filters) and [section 12](#12-timestamp-units-and-day-boundaries): instants
  on an instant axis (decided by the generated row field the parameter filters, so a
  date-typed `published_utc.gte` gets an instant), whole-day dates on a calendar-date axis.
  Each request's dates, its axis and its date filters on instant parameters, follow the
  request's day zone ([section 12](#12-timestamp-units-and-day-boundaries)): the dataset's
  class, or for a dataset with a path per symbol class the class of the request's own symbols,
  so a crypto indicator's `start="2025-03-03"` is UTC midnight and a stock indicator's New
  York midnight.
- **Currency pairs.** A per-symbol request on a path whose `{from}` and `{to}` nothing else
  fills (crypto's daily open-close, whose rows carry their own `symbol`) takes them from the
  request's symbol, split into its two currencies, wherever the dataset's symbol key comes
  from; the rows' symbol is made canonical as usual.
- **Bars ranges.** A bars spec whose range path has `{from}` and `{to}` segments declares no
  `time_axis`, and `history()` fills those segments as epoch milliseconds: `{from}` is the first
  instant that belongs to the first bar whose whole window fits the range (the Saturday before
  a Monday-to-Friday week, the 1st of a month) and `{to}` the last whole millisecond before the
  range's end ([section 12](#12-timestamp-units-and-day-boundaries)); the bars that do not fit
  wholly, or have not ended, are dropped afterwards.
  `futures.bars`, whose path `/futures/v1/aggs/{ticker}` has no such segments, declares the
  instant pair `window_start.gte`/`window_start.lt` instead.
- **Futures sessions.** Futures trades and quotes named by session dates are requested one
  weekday session at a time, `session_end_date=<date>` with no time window; futures bars named
  by session dates are requested on `window_start` and kept by `SessionEndDate`
  ([section 12](#12-timestamp-units-and-day-boundaries)). Grouped daily skips weekends for the
  classes that do not trade then. A per-session or grouped-daily download of more than five
  requests logs its request count (for sessions, "at least") and estimated minutes at a Basic
  key's 5 requests a minute, once, at INFO, before the first request.
- **1-day stamps.** Forex and crypto 1-day bars, from `/prev`, the range, and grouped daily,
  are stamped at 00:00 UTC of their day; a crypto 1-day stamp elsewhere in its UTC day than
  its first or last millisecond, or a crypto bar of several days or longer not stamped at
  00:00 UTC, raises `MassiveError` naming the path and `source=dm.HistorySource.FLAT_FILES`
  ([section 12](#12-timestamp-units-and-day-boundaries)).
- **Symbol batching.** `DatasetSpec.symbols_per_request: int | None = None` caps the symbols
  of one request when the symbol parameter is list-valued (`ticker.any_of`). When it is set
  (250 on the unified snapshot, the indices snapshot, and the stocks, forex, and crypto
  snapshots), the transport splits the symbol list into batches of at most that size, issues
  them in order, and concatenates the rows; None sends every symbol in one request. A
  projected `fetch()` (`indices.values`) batches the same way, so it is not one request per
  symbol.
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
  goes through `canonical(spec.asset_class, value)`, exactly as WebSocket rows do, so forex
  conversion's `AUD/USD` becomes `C:AUDUSD` and matches the symbol the caller passed.
- **Error rows in multi-ticker snapshots.** A snapshot of several tickers answers a ticker it
  cannot serve (`NOT_FOUND`, `NOT_ENTITLED`) with a row carrying `error` and `message`.
  `snapshots()` keeps such rows, with their `Error` and `Message` columns, and logs each
  ticker once. A projected `fetch()` (`indices.values`) drops an error row and logs one
  WARNING naming the ticker and the vendor's message, so its result holds only values.

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
  returns a described copy of the blink table, a new table managed in its own result scope
  ([section 6.1](#61-how-a-live-table-is-kept-alive-and-released)); `append_only()` is
  `blink_to_append_only(blink)`; `ring(n)` is `ring_table(blink, n)`; `last_by()` is as in
  [section 7](#7-query-classes-and-verbs); each shape is described last
  ([section 7.4](#74-column-descriptions)).
- **Fan-out per flush.** Per connection and per channel, the flush thread drains the channel's
  row buffer once. The rows arrive keyed by long-form column; the flush renames them
  (`rename` in `_datasets/schema.py`), converts them (`_datasets/timestamps.py`), and builds one Arrow batch in the dataset
  schema. The timestamp bound is applied as a vectorized mask: rows that fail it fail only the
  streams that would have received them ([section 19.5](#195-runtime-drift)) and are dropped
  from the batch. Rejected rows never enter a buffer. The batch is converted once with
  `deephaven.arrow.to_table`: one Python-to-Arrow-to-Java conversion per channel per flush.
  Then, for each stream subscribed to that channel: `ALL` streams get `publisher.add(batch)`;
  symbol-list streams get `publisher.add(batch.where_in(stream.filter_table, "Symbol"))`, where
  `filter_table` is a cached static one-column table rebuilt only when the stream's symbol set
  changes. Target: Python work is O(rows) per channel per flush and independent of the number of
  streams; per-stream work is one Java filter and one `add`, O(rows) in Java with no Python
  per-row cost. A symbol stream that holds none of the batch's distinct symbols gets nothing,
  and one that holds all of them (and the batch has no row without a valid symbol) gets the
  whole batch with no filter, so the common single-stream case costs one `add`. A non-gating
  benchmark test records warmed median rows per second for 1, 10, and 100 streams at 200,
  2,000, and 20,000 rows per flush, against one-pass per-stream Python grouping.
- Each flush applies the failures queued before it, then publishes, so a failed stream gets no
  row after its failure; rows buffered for it in that flush are dropped. A batch that cannot
  be converted or published fails that dataset's streams on the channel, with a `MassiveError`
  naming the cause, and the flush goes on with the other channels.
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
- Each REST `poll()` table has its own publisher, named `massive.<dataset>#<stream id>` and
  created with the result in the result scope; every mode is shaped from its blink table, so
  `publish_failure` fails the table in every mode. (A keyed `input_table` cannot be failed:
  Deephaven notifies failures only inside an update cycle.) `APPEND` is
  `blink_to_append_only(blink)`, fed only the rows whose identity (`poll_keys`, then
  `revision_keys`) the poller has not appended yet ([section 7](#7-query-classes-and-verbs)); the
  poller thread alone owns that set of identities, so it needs no lock, and adds a cycle's
  identities only once the cycle is published; `UPSERT` is `blink.last_by(poll_keys)`, or
`blink.select_distinct(poll_keys)` when they are every column (a `last_by` with nothing to
aggregate is refused, and the distinct rows are what it would keep); `REPLACE`
  is that `last_by` filtered on a hidden boolean column, to which each cycle adds one tombstone
  row for every key the previous cycle had and this one lacks, then dropped. A cycle is one `add`,
  so its rows and its removals land in one update. The `last_by` state keeps a hidden row for
  every key ever seen, the trade-off `last_by()` streams already accept: a `REPLACE` poll over a
  churning key set (an options chain, the day's movers) grows with the keys it has held. The
  `poll()` docstring says so. The poller holds the session core only weakly, so a `poll()` table
  never keeps an unclosed session alive.

## 15. Options contract cap

Massive allows 1,000 option contracts per `/options` connection. The library counts
**topics** per connection: `T.X` and `Q.X` count as two. Whether the server counts contracts or
topics is **UNVERIFIED**, so the stricter count is used (fixture test). Wildcards count zero.
Every wanted explicit topic counts, also one covered by an acknowledged wildcard and so off the
wire meanwhile ([section 13.3](#133-connection-and-subscription-protocol)): the wildcard may be
removed at any time, and a static contract the verb accepted must never be deferred then.

Because the library knows the count before sending, a frame that would cross the cap is never
sent:

- **Static input.** `len(new topics) + in use > 1000` raises `ValueError` at verb time,
  synchronously.
- **Reactive `Table` input.** Static topics are admitted first (their verb checked them);
  a reactive table's adds take the room left, in sorted order, and contracts admitted before
  keep their place, across reconnects too. The rest are deferred: not subscribed, not failed.
  `m.streams()` shows `State=RUNNING`, `Deferred=<n>`, and
  `Error="over capacity: 1234/1000; 234 deferred"`, with a WARNING on each transition. When
  removals free capacity, deferred contracts subscribe in the same order. Rows for deferred
  contracts are absent, never stale, except while a wildcard on the connection delivers them
  anyway.

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
`indices.bars`, `forex.bars`, and `crypto.bars` have no `adjusted`, so the predicate is satisfied.

The choice covers the whole range: one unpublished partition sends all of it to REST, and
nothing is split between the sources. The partition dates are those of the rows the call keeps
([section 12](#12-timestamp-units-and-day-boundaries)): for bars, the days of the bars whose
whole window fits, so the unfinished current day never needs a file. Crypto and forex 1-day bars
follow the same rule as every other dataset: the two sources may serve one call or the next
by symbol count or publication, and the loud UTC-cut check of
[section 12](#12-timestamp-units-and-day-boundaries) keeps their data on the same UTC days. If
live REST crypto bars turn out to be cut at midnight New York time, every REST crypto daily
call raises loudly (the morning before yesterday's file is published included), and the
owner decides the source rule after that live check ([section 23](#23-open-verification-items)
item 1). `source=` selects either source explicitly. A dataset without instruments counts as `ALL`.
`HistoryQuery.history()` makes the choice (`choose_source` in `_datasets/history_source.py`) and then calls one
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
returned. A futures range named by datetimes that ends on a holiday reads the next session's
file, or raises, as [section 12](#12-timestamp-units-and-day-boundaries) describes. No data is
no data: a range in which no file exists, whatever the reason (a weekend, a holiday, a range
before the dataset's first file), returns an empty table with every column, and its missing
weekdays log their WARNINGs as above. A missing file is a 404 whose body is S3's XML error
with the code `NoSuchKey`. Any other 404 (another S3 code, no code, or a body that is not S3's,
such as the HTML "Massive 404 Not Found" page Massive's front end serves for a path it does
not route, which is what a wrong `s3_bucket` meets) raises `MassiveError` naming
`s3://<bucket>/<key> at <endpoint>` and adding "check MassiveConfig s3_bucket and
s3_endpoint_url": `cannot read s3://flatfile/<key> at https://files.massive.com: HTTP 404 (404:
Not Found); check MassiveConfig s3_bucket and s3_endpoint_url`. A connection that drops
part-way through a file's body makes the read start that file again from its first byte, at
most `MAX_RETRIES` (3) more times, after the waits of the shared retry schedule (0.5, 1, 2 s
plus jitter). The rows already read from that file are discarded, and freed before the wait,
which is safe because no row leaves a file's read before the file ends. The last failure
raises `MassiveError` naming the file. Futures
`venue=None` reads four prefixes per date; a 403 on any prefix raises
`EntitlementError` naming the venue and the `venue=` parameter. A 403 whose S3 error code says
the credentials themselves are wrong (`InvalidAccessKeyId`, `SignatureDoesNotMatch`) raises
`AuthenticationError` instead, for every asset class, naming `s3_access_key_id` and
`s3_secret_access_key`.

Flat files are read through `_massive.FlatFileClient` and `FlatFile`
([section 19.3](#193-the-flat-file-client)); `_ingest/flatfiles.py` builds each file's `ReadPlan`.
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
(`pyarrow.compute.is_in`), the window's half-open `Timestamp` range through the plan's converter
to nanoseconds (for bars, the starts of the bars whose whole window fits; not applied to
futures trades, quotes, and session bars named by date, whose files hold exactly the requested
sessions), and, on the surviving rows only, the rename to long-form names and
parsing of the list columns (`Conditions`, `Indicators`). `_ingest/flatfiles.py` then applies
`rename` in `_datasets/schema.py` and the `_datasets/timestamps.py` routine, and moves forex
and crypto 1-day bar stamps to 00:00 UTC (`timestamps.utc_day_stamps`). No block is converted
to Python objects. A
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
(the client closes its connection pool once more as that request returns, so nothing it opened
outlives the close), and the call raises `MassiveError("MassiveSession #1 is closed")`,
whatever that request returned.

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
| caller (console, script, user thread) | user | verbs; the host-resolution REST call; Deephaven table creation inside the result scope; registry mutation; waits, outside every lock, for another thread's open of the same stream key; fails a new stream whose connection failed while it opened (`publish_failure`, outside every lock) | hold the registry lock while calling Deephaven or waiting |
| WS client, one per connection | library | `asyncio.run` of the connection's `WsClient` (section 19.2): socket, handshake, frames, backoff; decode each frame once against `ws.json`; route status events, rejects, and drift; append rows to per-channel `SimpleQueue`s; the reconcile loop and watchdogs; idle close | any JVM call; blocking waits |
| flush, one per connection | library | every `flush_interval`: drain queues, Arrow to Java once per channel, `where_in` and `publisher.add` per stream, `publish_failure` of failed streams; enqueue diagnostics row snapshots | touch loop-owned state directly |
| verdict move, one short-lived thread per move of a connection to the `delayed.` host (§ 13.3) | library | `RestTransport.record_verdict`; creating the new connection's client (TLS settings load from disk); moving the streams' leases under `registry_lock`; posting the hand-over to the old connection's loop | block a loop thread; call Deephaven |
| poller, one per `poll()` table, plus a `ThreadPoolExecutor(max_concurrency)` for fan-out | library | REST calls; `Table.snapshot()` of a reactive symbol table; `publisher.add` | run inside a listener |
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
- Two locks of the WebSocket flush are not leaf locks, and are never taken under
  `registry_lock` or the UG lock:
  - each connection's **flush lock** serializes its flushes (the flush thread's, and any a test
    calls); a flush calls Deephaven under it;
  - each stream's **publish lock** serializes `publisher.add` and `publish_failure` for the
    stream, which two flush threads can briefly share when a connection moves, and the caller
    thread when a stream's connection failed while it opened. Failing a stream takes
    `registry_lock` under it (`SessionCore.fail_stream`).
- The other locks are leaf locks too, each guarding one thing and making no Deephaven,
  network, or blocking call while held:
  - the REST transport's host-decision cache, which holds one future per (class, family);
    callers wait on a future outside every lock (§ 13.2);
  - `FlatFileClient`'s lock, one per session, which guards the creation and the close of the
    boto3 S3 client (creation reads local AWS configuration but makes no request, since the
    credentials are given); its closed flag is a `threading.Event`, read without the lock by
    the reader threads. The session's `SessionClients` creates the `FlatFileClient` on the
    session's first flat-file read, under `registry_lock` (the constructor only stores
    settings), and registers its close after releasing the lock;
  - the `_massive.spec` loaders, which load each generated file once;
  - `RestClient`'s not-before instant (section 19.1);
  - the session's `DriftRegistry`, whose WARNINGs are logged after the lock is released;
  - the `dataset_schema` cache of the `Catalog`.
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
  (`entered` in `_tables/liveness.py`). The `deephaven` wrapper stores its restore handle on the wrapper object,
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

The WebSocket flush adds two locks above `registry_lock`: a connection's **flush lock**, then a
stream's **publish lock**, then `registry_lock`. Nothing takes the flush or the publish lock
while holding `registry_lock` or the UG lock, so the order has no cycle.

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
generated wire schemas, the units, and the long-form names, and its `refresh/` subpackage
generates those schemas from the vendored spec snapshot. The snapshot itself lives outside the
package, in `vendor/massive/` at the top of the source checkout: it is Massive's published
content, vendored as generation input, not covered by the repository's license, and not
distributed in the packages (`vendor/README.md` says the same beside it). What stays outside
is the library's: Deephaven tables, sessions and liveness, dataset semantics
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
                     retryable classification; TLS never retried; Clock (injectable, tests
                     never sleep); an unusable Retry-After is logged at WARNING, never 0
  codec.py           JSON codec: orjson when installed (logged once), else stdlib
spec/                run-time wire vocabulary; imports nothing else in _massive
  __init__.py        source(transport, key), sources(transport): load once under a lock
  types.py           Transport, Source(key, transport, fields, memberships),
                     Membership(kind, group, position, projection), RestPath(Source + template,
                     result key, params), Field(wire key, column, WireType, unit, required,
                     conversion), Conversion (named conversions, each with its declared and
                     resulting WireType), Param(+ unit), ParamKind, WireType, WireUnit,
                     ENVELOPE_KEYS, DriftReport(extra, missing, statuses)
  convert.py         convert(wire_type, value): one checked, exact scalar per WireType, shared by
                     every JSON decoder (REST to_batch, WebSocket frames); UnconvertibleError;
                     convert_field applies a field's named conversion and raises with the
                     shared bad-value message; missing_key_message and excerpt, so both
                     decoders word a row failure alike and add only their own context
  values.py          encode_value(param, value, *, label): one REST parameter value's wire
                     text by ParamKind, for the REST client and the library's filter check
  rest.json  ws.json  flatfiles.json        generated, committed
rest/                imports net, spec
  __init__.py        RestClient, Page, to_batch
  client.py          pool, bearer auth, retries, not-before instant (leaf lock), next_url paging, close
  query.py           request targets (values encoded by spec.values), next_url checks
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
  __init__.py        docstring only: _massive's interface maps each name to its submodule,
                     so no flat-file name but FlatFileClient imports boto3
  client.py          FlatFileClient: lazy boto3 client under a leaf lock, closed flag,
                     open(key) -> FlatFile | None, S3 error mapping, prefixes(), close();
                     botocore's body read errors become OSError
  layout.py          object_key(prefix, day, venue): <prefix>/<YYYY>/<MM>/<date>.csv.gz; {venue} template
  reader.py          FlatFile.batches(plan) -> (RecordBatch, DriftReport) per block; header peek
refresh/             build time only; excluded from the wheel; may import any of the above
  __init__.py        empty
  __main__.py        [--check | --fetch]; the plain run regenerates spec/*.json
  generate.py        the refresh: snapshot -> generators -> spec/*.json, written only when every
                     gate passes and the text changes
  snapshot.py        the snapshot's location (vendor/massive/), layout, and manifest; store
                     (staged write, atomic swap) and verified load
  fetch.py           download and put each file in canonical form; stores through snapshot.py
  docs.py            llms.txt and the pages it links: each REST page's endpoint line, each
                     WebSocket page's feed (cluster and channel), the flat-file overview
                     prefix tables, "Columns" tables, and "Example Data" headers
  openapi.py         OpenAPI reading: $ref, allOf merge, one-branch choices, checked accessors
  plain_text.py      the four fixed rules that turn a vendor description into plain text
  naming.py          the naming policy shared by every transport: PascalCase columns, snake
                     case concepts, short and timestamp-like names, the dataset key columns
  fields.py          the field rules every generator shares: representation rules, names,
                     units, type fixes, documented examples, and the gates met per field
  rest.py            REST paths -> RestPath: flattening, result keys, parameters, path
                     coverage, and the gates met per parameter
  ws.py, flatfiles.py  the WebSocket and flat-file generators
  kinds.py           KINDS and PROJECTIONS -> memberships; the type lattice, type-agreement,
                     transport-agreement, and the kind column order
  gates.py           Gate (the named gates of section 19.4), Problem, RefreshError, and the
                     checks that span sources: stale entries, exclusions, concept collisions
  render.py          spec/*.json in canonical text
  hand_input.py      HandInput: the hand tables bundled for the generators
  lexicon.py         SHAPES (object shape -> lexicon), lexicons, identity entries
  overrides.py       NAMES, KINDS, PROJECTIONS, UNITS and tag conflicts, TYPE_FIXES,
                     KEY_FIXES, TRANSPORT_ONLY, RESULT_KEYS, PARAM_UNITS, VENUES
  exclusions.py      EXCLUSIONS (kind, item, reason), the executable rows of section 8.12;
                     SAMPLE_ERRORS (path, wire key, sample, reason), the wrong vendor samples
  drift.py           live-against-snapshot comparison, unaccounted items, deprecations

vendor/massive/      outside the package; Massive's published content, vendored as generation
                     input, not covered by the repository's license, in no distribution
  openapi.json  websocket.json  llms.txt  manifest.json
  rest/  websocket/  flat-files/   (the pages of each area llms.txt links)
```

**The interface.** `_massive/__init__.py` defines `__all__` and re-exports exactly these 29
names, resolving them lazily (PEP 562), so a REST-only session never imports boto3 or
websockets:

- errors: `WireError`, `WireAuthError`, `WireEntitlementError`;
- REST: `RestClient`, `Page`, `to_batch`;
- WebSocket: `WsClient`, `WsState`, `Status`, `StatusKind`, `Reject`;
- flat files: `FlatFileClient`, `FlatFile`, `ReadPlan`, `ReadColumn`, `object_key`;
- spec: `source`, `sources`, `Transport`, `Source`, `RestPath`, `Field`, `ParamKind`,
  `WireType`, `WireUnit`, `DriftReport`, `provenance`, `Provenance`, and `encode_value` (one
  parameter value's wire text by its `ParamKind`, in `spec.values`, which the REST client and
  the library's filter check both run, so a filter the check accepts always encodes; it loads
  no client, and a subprocess test checks that no vocabulary name loads a client or network
  module).

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
  `_massive`. The exceptions are `typing_extensions` (for `override` on Python 3.11) and, in
  `flatfiles` only, `types_boto3_s3` (for `S3Client`), inside `if TYPE_CHECKING:` only, which
  never runs.
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

**Not shipped.** The wheel excludes all of `refresh/`, and neither the wheel nor the sdist
contains `vendor/` (the CI build job checks both archives). `spec/*.json` ship as package data.
The tests that read the snapshot (the parity test among them) skip with a stated reason where
`vendor/massive/` is absent and the `CI` environment variable is unset, as in an unpacked
sdist. With `CI` set they never skip, so a CI checkout without the snapshot fails them.

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
Page(status: str, request_id: str | None, rows: list[dict], body: dict | list,
     drift: DriftReport)          # drift.statuses only; repr omits rows and body
to_batch(rows, rest_path, *, required) -> (pyarrow.RecordBatch, DriftReport)
```

`rest_path` is the path's `RestPath` from `spec/rest.json`; the client reads its template,
result key, and parameter kinds from that value, so its tests build `RestPath` values by hand.

- **Transport.** One `urllib3.PoolManager` (or `ProxyManager`, below) with `retries=False` (the
  client retries itself), a 10 s connect and 30 s read timeout, `maxsize` at least
  `max_concurrency` so fan-out never discards connections, and `Accept-Encoding: gzip`
  (urllib3 decodes it). `User-Agent: deephaven-massive/<version>` (`net.USER_AGENT`).
- **Authentication.** `Authorization: Bearer <key>` on every request. The key never appears in
  a URL: an `apiKey` parameter in a `next_url` is removed before the request. A key that is
  empty or holds anything but visible ASCII (a trailing newline from a key file) raises
  `ValueError` at construction, without quoting the key. The base URL must be `https`, except
  for a loopback host (`localhost`, `127.0.0.0/8`, `::1`), so the key never travels in clear
  text; it names only a scheme, host, and port (no user information, path, or query).
- **Proxies and certificates** come from `net.http`: `urllib.request.getproxies()` for
  `HTTPS_PROXY` and `urllib.request.proxy_bypass` for `NO_PROXY` select a `ProxyManager` for
  the REST host or a plain `PoolManager`, read once at construction. A user and password in
  the proxy URL are sent as `Proxy-Authorization` (`net.http.proxy_credentials`; urllib3 does
  not send them itself), and the pool holds the URL without them, so they never reach a log or
  an error. HTTPS goes through the proxy in a `CONNECT` tunnel, so the proxy sees
  `Proxy-Authorization` and never the bearer key. A proxy reached over `https` is verified
  with the same `net.http.ssl_context()` as Massive. The CA bundle is the
  system default when `SSL_CERT_FILE` or `SSL_CERT_DIR` is set (an
  `ssl.create_default_context()` honours both) and `certifi`'s bundle otherwise.
- **Query encoding.** Path parameters are encoded like query values, then percent-encoded
  whole into the template. Values, by the parameter's `ParamKind`: an enum by its value (one of
  `Param.values`), `bool` as `true`/`false` (never for an integer or number), a `date` as
  ISO-8601, an aware `datetime` for a `TIMESTAMP` parameter as an exact integer count of the
  parameter's `unit` (`Param.unit`, set exactly for `TIMESTAMP`: nanoseconds for most,
  milliseconds for the indicators' `timestamp` filters) computed from `timedelta` days,
  seconds, and microseconds (never through a float), and for a `DATE` parameter as ISO-8601
  text in UTC ending in `Z` (`2025-06-02T04:00:00Z`, the form of Massive's examples);
  a non-empty sequence of `str` joined with commas (the `.any_of` form). A naive `datetime`, an
  instant that is not a whole number of the parameter's unit (rounding would move a range
  bound), an empty sequence, and a non-finite number raise `ValueError`; a value of a type the
  kind does not accept raises `TypeError`. `None` values are omitted. A name that is not a
  query parameter of the path, or path parameters other than exactly its placeholders, raise
  `ValueError` before any request. Names arrive already in wire form (`strike_price.gte`); the
  dot mapping of [section 9](#9-filters) happens before the client.
- **Paging.** `pages` follows each page's `next_url`, which carries the cursor and the query.
  A `next_url` whose scheme, host, or port differ from `rest_base_url` raises `WireError` and
  is never requested, and the error quotes it without `apiKey` or user information; any other
  `next_url` is requested on the base URL's origin, its query kept byte for byte without
  `apiKey`. `limit` (at least 1) is the total-row cap: paging stops
  once that many rows have been yielded, and the last page is truncated. Only one page of rows
  is held at a time. A body without an envelope (`result_key` `None`, a list) ends paging.
- **Retries** follow `net/retry.py`. At most 3 retries (4 attempts); every request is a `GET`,
  so a retry is always safe. HTTP 500, 502, 503, 504 and connection errors wait 0.5, 1, then
  2 s, each with up to 20% upward jitter. HTTP 429 waits the `Retry-After` value in either form
  (delta-seconds or HTTP-date), otherwise 15, 30, then 60 s; a `Retry-After` above 60 s raises
  `WireError(status=429)` at once, naming the requested wait, and still sets the not-before
  instant 60 s ahead, so the other workers stop too. After a 429 the client sets its
  not-before instant, so concurrent workers back off together instead of multiplying 429s;
  a 429 on the last attempt still sets it (its `Retry-After`, else 60 s) before it raises. A
  TLS certificate failure (`ssl.SSLCertVerificationError`) is never retried: it raises
  `WireError` at once, naming `SSL_CERT_FILE`. Nor is a proxy's refusal of its credentials
  (HTTP 407 to the tunnel request, `net.retry.is_proxy_auth_failure`): repeating them can lock
  a corporate account, so it raises `WireError(status=407)` at once, naming `HTTPS_PROXY`. A
  connection reset in the middle of paging retries **that page** (its `next_url`), never the
  first page. Redirects are never followed
  (a 3xx is a `WireError`), so the bearer key never leaves the REST host. Every wait goes
  through an injectable clock (`net.retry.Clock`, whose `wait(event, seconds)` ends early
  when the event is set), so tests never sleep. `close()` sets the client's closed event: a
  thread waiting for the not-before instant or a retry stops at once, and every later request
  raises `WireError`. A `next_url` that repeats the target just fetched raises `WireError`
  rather than paging forever.
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
  own limit of 1024. The page's rows come from the `RestPath`'s `result_key: str | None`:
  - **A key** (`results`, or the `RESULT_KEYS` entry such as `last`), possibly dotted
    (`results.values` for the indicators, `results.events` for ticker events): the body is an
    envelope object, and the decoder walks the dots. An object there is one row and a list is
    the rows. A missing (or null) step is an empty page only when `status` is `OK` or
    `DELAYED`, and a `WireError` otherwise. Rows under a key keep every key, `status` included
    (TMX corporate events carry their own).
  - **`None`**: the body is the rows (the generator writes `null` where the OpenAPI response has
    no envelope). A JSON list is the rows, every key kept (`/v1/marketstatus/upcoming` holiday
    rows have a real `status`), with `status=""` and `request_id=None`. A JSON object is one
    row and also the envelope (`/v1/marketstatus/now`, `/v1/open-close/*`, `/v1/conversion`):
    its `status` and `request_id` describe the page, and the `ENVELOPE_KEYS`
    (`status`, `request_id`, `next_url`, `count`, one constant in `spec/types.py` that the
    refresh shares) are removed from the row before drift detection.

  **Envelope status**, for every path: `NOT_AUTHORIZED` raises `WireEntitlementError` and
  `ERROR` raises `WireError`. `OK`, `DELAYED`, and `success` (the "last" endpoints) are the
  documented good statuses, named once (`rest.response.GOOD_STATUSES`). Any other status still
  yields its rows and is reported in `Page.drift.statuses`, which the library logs at WARNING
  once per (path, status). A `status` that is not text is a `WireError`. The `request_id` is
  metadata and never fails a call: Massive's documentation samples send it as text and as a
  number (`1`), so any JSON scalar is kept as text (`"1"`), and an object or list is dropped
  (`None`), in a 200 envelope and in an error body alike. The envelope's `status` and
  `request_id` are kept on the `Page`, so callers can read `DELAYED`.
- **Batches.** `to_batch` turns a page's rows into one Arrow batch of the path's generated
  fields, in long-form column names, and returns its `DriftReport`; it validates every value
  per [section 19.5](#195-runtime-drift). It is the only code that walks a wire path; no row
  object or model class sits between the JSON and Arrow. `required` holds long-form columns
  (`KeySpec.column`); a name that is not a column of the path raises `ValueError`. Arrow
  types: `long` and `epoch` → `int64` (an epoch stays a raw count for `_datasets/timestamps.py`), `double` →
  `float64`, `bool` → `bool`, `string` and `json-text` (compact JSON) → `string`, `date` →
  `date32` (strict `YYYY-MM-DD`), `iso-instant` → `timestamp[ns, UTC]` built from exact
  integer nanoseconds (up to nine fractional digits, integer arithmetic only, never through a
  float or a microsecond `datetime`), `int-array` → `list<int32>`. Values are checked and
  converted by `spec.convert`, which the WebSocket decoder shares. ISO date-time text **without** an offset is an
  unconvertible value, never assumed to be UTC: a field that really is offset-less UTC gets a
  named `TYPE_FIXES` conversion. `DriftReport.missing` holds only the vendor-`required`
  fields that some row lacked or held as null; an extra nested object is reported by its own
  key, not by each leaf.
- **Logging.** Each request logs at DEBUG: method, path, query (with no secret), status,
  `request_id`, row count, and elapsed time. Retries log at INFO with the wait, and exhausted
  retries at ERROR. The `Authorization` header is never logged.

### 19.2 The WebSocket client

`_massive.WsClient` is one object per connection (one `wss://host/path`), created by `_ingest/ws.py`
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
  socket (or after the handshake timeout), then waits its backoff. A `max_connections` status
  received after authentication is delivered as a `Status` and also makes the client report
  `WAITING_SLOT`, not `RECONNECTING`, when that socket closes. A greeting other than
  `connected`, a reply other than `auth_success`, `auth_failed`, or `max_connections`, or a
  handshake frame that is not JSON is vendor drift: logged at ERROR and retried. The auth frame
  is never logged, by this library or by websockets: every connection is given a logger on
  which DEBUG, the level of websockets' frame log, is disabled.
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
    their Python form (an epoch keeps its integer count; an ISO-8601 instant becomes exact
    integer nanoseconds since the epoch), and symbols stay as the wire spells them. A row that fails validation becomes a `Reject(channel, symbol or None, WireError)`
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
    and the backoff, with the `Retry-After` capped at `max_reconnect_backoff`, so every wait
    still lies in `[reconnect_backoff, max_reconnect_backoff]`. This differs from REST, which
    gives up on a `Retry-After` above 60 s because a caller is blocked on the request: the
    WebSocket client never gives up, and an unbounded `Retry-After` would leave its streams
    silent for as long as the server named;
  - a TLS certificate failure (`ssl.SSLCertVerificationError`) is fatal, naming
    `SSL_CERT_FILE`, although it is an `OSError`;
  - everything else follows websockets' public rule, `websockets.client.process_exception`
    (the floor is websockets 17.1, its first release there): what it returns `None` for (EOF, `OSError`, timeouts, HTTP 500/502/503/504) is retried, and
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

`_massive.FlatFileClient(access_key_id, secret_access_key, endpoint_url, bucket, max_pool,
clock=SYSTEM_CLOCK)` is one object per session, created by the session's first flat-file read
(only after the source choice has required both credentials, which are therefore `str`) and
closed by `m.close()`; `clock` is the `net.retry.Clock` its retry waits use. It creates its
boto3 S3 client lazily, under a leaf lock, and keeps a closed flag (a `threading.Event`); once
closed, it makes no request. A request in flight at the close closes the client again as it
returns.

- **`open(key) -> FlatFile | None`.** `None` for a missing key: HTTP 404 whose body is S3's
  XML error with the code `NoSuchKey`. Any other 404 (another S3 code, no code, or a body that
  is not S3's, such as the HTML page Massive's front end serves for a path it does not route,
  which botocore reads as the code `404`) is a `WireError` naming the bucket and the key, so a
  wrong bucket or endpoint never reads as a run of missing files. `object_key(prefix, day,
  venue)` builds keys as `<prefix>/<YYYY>/<MM>/<date>.csv.gz`, filling a `{venue}` template.
  The generator folds the four concrete futures overview prefixes into that template using
  `VENUES` (`cme`, `cbot`, `nymex`, `comex`, each with its reason) in `refresh/overrides.py`,
  which a library test checks against `FuturesVenue`.
- **Error mapping.** An S3 error code `InvalidAccessKeyId` or `SignatureDoesNotMatch` is
  `WireAuthError`; any other 403 is `WireEntitlementError`; anything else is `WireError`. Each
  carries the status, the host, and the key, and `request_id` comes from `x-amz-request-id`.
  A wire message says what happened to which object (`HTTP 403 for s3://<bucket>/<key> at
  <endpoint> (<code>: <message>)`); the user-facing remedy (the `MassiveConfig` settings to
  check, the `venue=` parameter) is added by `_ingest/flatfiles.py`, whose public texts are
  those of [section 16](#16-historysourceauto). Credentials are never logged, and a message
  names a proxy by its URL without the user and password (`net.http.proxy_credentials`), never
  by botocore's masked form, which leaves a password in clear text when it also appears earlier
  in the URL. One documented exception, for a third-party logger at DEBUG only: botocore's own
  DEBUG records, emitted only when someone explicitly enables DEBUG on the `botocore` logger,
  may show the S3 access key id and the request signature (its `Authorization` header). They
  never show the secret access key or the proxy password, and a test checks both, the
  password's base64 form included, across every record.
- **TLS and proxies.** The client is built as `session.client("s3", verify=<the net.http CA
  bundle>, config=Config(proxies=<from net.http>, proxies_config={"proxy_ca_bundle": <the same
  bundle>}, retries={"total_max_attempts": 1, "mode": "standard"}, connect_timeout=60,
  read_timeout=60))` on a botocore session whose `defaults_mode` is `legacy`. The CA bundle is
  `SSL_CERT_FILE`, else certifi (`SSL_CERT_DIR` does not apply to S3), and it also verifies an
  HTTPS proxy, as the REST client's context does. `proxies` is `{"https": <url>}`, or `{}` when
  no proxy applies: `None` would make botocore look the proxy up itself, under the leaf lock and
  from more variables (`HTTP_PROXY`, `ALL_PROXY`) than the other clients read. The bundle and
  the proxy are read when the first `open` creates the boto3 client, outside the leaf lock,
  because on macOS the system proxy exceptions can resolve the endpoint's host name. The
  `legacy` defaults mode keeps `AWS_DEFAULTS_MODE=auto` from asking the instance-metadata
  service for a region under the lock.
- **Retries.** botocore's own retries are off, and the mode is pinned so `AWS_RETRY_MODE=adaptive`
  cannot add botocore's client-side rate limiter, which sleeps outside the injected `Clock`.
  The shared `net/retry.py` schedule (0.5, 1, 2 s plus jitter, waited on the client's `Clock`,
  which `close()` interrupts) retries a failure before the object's first byte:
  - the `TRANSIENT_STATUSES` (HTTP 500, 502, 503 `SlowDown`, 504) and the S3 error code
    `RequestTimeout` (HTTP 400);
  - connection errors: botocore's `ConnectionError` family, `ConnectionClosedError`, and
    `ReadTimeoutError`, but not the bare `HTTPClientError` botocore raises for any unexpected
    exception in its HTTP layer;
  - an error response whose body is reset or stalls: botocore reads that body outside its own
    error handling, so urllib3's `ProtocolError` and `ReadTimeoutError` reach the client
    unwrapped.

  Each retry is logged at INFO and giving up at ERROR, naming the object and the failure.
  Three failures are never retried:
  - A TLS failure (botocore's `SSLError`, or any error `is_tls_failure` recognises). A
    certificate failure's message names `SSL_CERT_FILE`.
  - A proxy's refusal of the client's credentials (HTTP 407 to the tunnel request, which
    `net.retry.is_proxy_auth_failure` recognises). Repeating credentials the proxy refused can
    lock a corporate account; the `WireError` has status 407 and names `HTTPS_PROXY`.
  - A failure partway through an object's body. The reader has already handed blocks on, so
    the client cannot repeat the request by itself: the read fails with a `WireError`, and the
    file's `dropped` flag is set. `_ingest/flatfiles.py` keeps a file's rows to itself until
    the file ends, so it reads such a file again from its start: it frees the file's partial
    tables (it clears the frames the error's traceback holds, `_gather`'s and the batch
    generator's, which keep the failed read's locals alive) and calls `wait_to_reread(error, retry, cancel)`, which waits on the shared
    schedule (logged at INFO), and returns `False` once `MAX_RETRIES` rereads are spent (logged
    at ERROR); the library then raises the failure. A wait whose `cancel` is already set, or on
    a closed client, returns at once without a log line. `close()` ends such a wait at once by
    setting `cancel`, as a failed or interrupted call does: `Clock.wait` waits on one event,
    and the call's event is the one that stops the call. There is no `Range` resume: a
    whole-file reread needs no byte bookkeeping and can repeat no row.
- **`prefixes()`** lists the bucket's top-level prefixes, for `--check`.

`FlatFile(body, key, location, host)` reads any binary stream whose failed reads raise
`OSError`; the client wraps the response body so, turning botocore's read errors and the
urllib3 errors botocore lets through (a TLS failure while reading the body is urllib3's
`SSLError`) into `OSError`, and the reader imports no botocore.
`FlatFile.dropped` is `True` once a read of that stream has failed (the connection dropped or
stalled part-way through the body). It tells a failed transfer from a broken file: only the
first is worth reading again.
`FlatFile.batches(plan)` yields one `(RecordBatch, DriftReport)` per block, in this order:
(1) the futures session check; (2) the symbol filter (`is_in`); (3) `[start, end)` on
`plan.to_ns(time column)`; (4) on the survivors, the rename to long-form names and list
parsing. Instants stay in their wire unit; the library converts them. The reader first peeks the
header line and reads by name ([section 16](#16-historysourceauto)). `close()` is idempotent and
safe from any thread.

```python
ReadPlan(columns: tuple[ReadColumn, ...], symbol_header, symbols, time_header, window_ns,
         to_ns, session: tuple[str, date] | None, block_size: int, required)
ReadColumn(header, column, read_type: pyarrow.DataType, list_type)
```

`session` is the futures session check: the header that holds each row's session date and the
file's date, so `_ingest/flatfiles.py` builds one plan per call and adds the session per futures
file. `block_size` is the reader's block of decompressed text, a required field with no default;
`_ingest/flatfiles.py` always passes its `BLOCK_SIZE` (1 MiB).

`_ingest/flatfiles.py` builds the plan from `source(Transport.FLAT_FILES, prefix)`. `to_ns` is the
`_datasets/timestamps.py` converter, so a wrongly declared unit still raises; the window is never converted
into wire units.

### 19.4 Generated schemas

No schema is hand-written. The refresh generates three committed, canonical files from the
vendored snapshot: `spec/rest.json` (from `openapi.json`), `spec/ws.json` (from
`websocket.json`), and `spec/flatfiles.json` (from the flat-file "Columns" tables). Each holds,
per source, an ordered list of fields `(wire key, column, WireType, unit, required)`. Every
change shows up as a reviewed diff of a generated file. Hand input is naming, grouping, and
verification, never a column list.

**Sources and flattening.**

- **REST.** Every `openapi.json` path that is not deprecated (`x-polygon-deprecation`) or
  draft (`x-polygon-draft`) and not in `EXCLUSIONS`; `x-polygon-ignore` neither excludes nor
  generates a path ([section 8.12](#812-excluded-endpoints)). The generator resolves `$ref`,
  merges `allOf` (into an object when any part is an object or no part has a type, else into
  the one scalar type its typed parts share, two different scalar types being a `shape`
  problem; three crypto response bodies put the status field's string schema beside the
  object part, and stay objects), treats a `oneOf` or `anyOf` with exactly one branch as that branch, flattens
  nested objects into dotted wire paths below the result key (`results` unless `RESULT_KEYS`
  names another key, a dotted path of keys such as `results.values`, or `None` for a body that
  is itself the rows), keeps fields in the order the vendor documents them (see the naming
  policy, rule 6), and records the parameters (wire name,
  `ParamKind`, enum values, path or query, and the unit of a `TIMESTAMP`). A JSON-object body
  without a result key is one row, and its `ENVELOPE_KEYS` are left out of the fields; one of
  those keys whose type is not the envelope's (`status`, `request_id`, `next_url` strings,
  `count` an integer) stops the refresh, since dropping it could lose data. A response with no
  rows where its result key says stops the refresh until a `RESULT_KEYS` entry says where they
  are.
- **REST parameters.** A name ending in `.any_of`, or an array, is `LIST` (sent comma-joined,
  even when its values come from an `enum`); any other `enum` is `ENUM`; booleans, integers, and numbers are `BOOLEAN`, `INTEGER`, and `NUMBER`; a
  timestamp-like string parameter (base name `timestamp`, `*_timestamp`, `updated`,
  `*_updated`, `window_start`, `window_end`, or `*_time`, the names of the `timestamp-unit`
  gate) takes its `PARAM_UNITS` entry: a unit makes it `TIMESTAMP` in that unit (`Param.unit`), and "not an
  epoch" makes it `DATE`; a string with `format: date` or `date-time`, a choice of formats, a
  base name `date`, `*_date`, `*_utc`, or `as_of`, or a description that gives the date format
  `yyyy-mm-dd` (any letter case) is `DATE`; any other string is `STRING`. The description is
  the vendor's evidence where the schema is a bare string: Massive types its date filters
  `{"type": "string"}` and states the format only in prose ("Value must be formatted
  'yyyy-mm-dd'"), whatever the parameter's name (`period_end`, `active_from`, `published`).
  The `.gt`, `.gte`, `.lt`, and `.lte` variants share their base name's entry.
- **WebSocket.** Every `websocket.json` channel not in `EXCLUSIONS`, keyed `(cluster,
  channel)` and spelled `"<cluster>/<channel>"` as its `Source.key` (`"stocks/T"`,
  `"futures/AM"`), since stocks, options, and futures share channel names. Fields are its
  message properties in spec order; `required` is copied where the vendor gives it.
- **Flat files.** Every dataset page with a row in its class's overview table. That row's
  `S3 <prefix>` is the source key, so no hand-written page-to-prefix map exists. A futures kind
  keyed by `{venue}` requires its four venue pages to list identical columns. Fields are the
  page's "Columns" table (name, type, description), in the order of the page's "Example Data"
  header row, which must name exactly the table's columns (case aside: some pages write
  `Ticker`): the vendor's tables list the columns in an order of their own (alphabetical on
  most pages), while the example header is the files' own column order (the stocks
  aggregates' `ticker, volume, open, close, high, low, window_start, transactions`). The
  example rows go through the `example-type` gate, each cell read as the file's reader reads
  it (an empty cell is null; a cell of a text or date column is text; any other cell is a
  number when it reads as one, else text). A dataset page with no overview row is reported as
  unaccounted by `--check`.
- **Representation rules** (generator rules, not hand entries): an array of strings or objects,
  a free-form object, and `oneOf`/`anyOf` become `json-text`; an array of integers becomes
  `int-array`; the WebSocket envelope key `ev` is dropped; a field with a `UNITS` entry becomes
  an `epoch` in that unit; a string with `format: date-time`, or no format and a name ending in
  `_utc`, becomes an `iso-instant`; a string with `format: date`, or named `date` or `*_date`,
  becomes a `date`.
- **Projections.** `PROJECTIONS {(kind, source): Projection}` changes how a source joins
  one kind, **inside one membership only**, in one of two forms. `fields` keeps only the
  listed keys, under the listed long names: `/v3/snapshot/indices`'s membership of the values
  kind is `{"ticker": "Ticker", "value": "Value", "last_updated": "Timestamp"}`, for
  `indices.values.fetch()`, while `indices.snapshots` uses the same source in full. `renames`
  keeps every field and renames only the listed ones, so a field the vendor adds later joins
  under its own name: the forex last quote and the crypto `XQ` stream join the quotes kind
  with `ask`/`ap` as `AskPrice` and `bid`/`bp` as `BidPrice`. The generator writes each long
  name through the naming rule, and the recorded projection maps every wire key it keeps to
  its PascalCase column, the same vocabulary as `Field.column`. A member whose projection
  renames is ordered, and gives the order, like an unprojected one (rule 6); a member that
  keeps only some fields does neither, and its source keeps its own order.
- **Memberships are written into the generated files.** `KINDS` and `PROJECTIONS` are generator
  inputs in `refresh/`, which is not shipped. The generator writes each source's memberships
  onto its committed entry in `spec/rest.json`, `ws.json`, or `flatfiles.json`, and the `spec`
  loader exposes them as `Source.memberships: tuple[Membership, ...]`, where
  `Membership(kind, group, position, projection: Mapping[wire_key, column] | None)`: the
  kind, the asset-class group, the source's order within the group, and its projection. A
  source in no kind has no memberships; a source may hold several. Everything at run time, the
  merge and the kind-membership gate included, reads memberships from there and never from
  `refresh/`. Drift detection stays per source, against the full fields, so projected-away fields
  are never reported as drift.

**The type lattice.** `WireType` is one of `long`, `double`, `bool`, `string`, `date`, `epoch`
(an integer with a unit), `iso-instant`, `int-array`, and `json-text`. OpenAPI `integer`, and
`number` with `format: integer`, map to `long`, and `number` to `double`. Doc pages map
`integer` to `long`, `number` to `double`, `string` to `string`, and `timestamp - integer` to
`epoch` (with its `UNITS` entry). Two types meet for one column name within one kind by these
joins: `long ⊔ double = double`; `long ⊔ int-array = int-array` (a scalar becomes a
one-element list); `epoch ⊔ epoch = epoch`, each source keeping its own unit. Any other pair is a
**type conflict** (the `type-agreement` gate). `_datasets/schema.py` maps `long` to `long`, `double` to
`double`, `bool` to `boolean`, `string` and `json-text` to `String`, `date` to `LocalDate`, `epoch` and
`iso-instant` to `Instant`, and `int-array` to `int[]`. An `iso-instant` is ISO-8601
date-time text with seconds and an offset (`Z` or `±HH:MM`), decoded to exact integer
nanoseconds since the epoch; text without an offset, or an instant outside the signed
64-bit nanosecond range (1677 to 2262), is an unconvertible value, never assumed UTC or
clamped. An `int-array` element outside the
`int32` range is an unconvertible value.

**Hand input.** All of it lives in `refresh/`, every entry carries a reason, and an entry that
names a source or field the snapshot lacks fails the refresh as stale (`stale-entry`).

| Kind | Where | What | Example |
|---|---|---|---|
| Lexicon | `lexicon.py` | `SHAPES` maps each object shape (aggregate row; snapshot `day`, `min`, `prevDay`, `lastTrade`, `lastQuote`; each WebSocket channel; each futures object) to its lexicon. A lexicon translates a short key to the long name Massive uses for the same field elsewhere in that asset class's REST or flat-file data, and **every entry cites that endpoint or file**; identity entries cover short real words (`id`, `cik`, `url`, `low`, `fmv`, `mic`). | trade lexicon `p` → `price`, `t` → `sip_timestamp` (`/v3/trades/{stockTicker}`); crypto trade `t` → `participant_timestamp` (`/v3/trades/{cryptoTicker}`); forex quote `a` → `ask_price` (`/v3/quotes/{fxTicker}`); segment `min` → `last_minute` (`/v3/snapshot`) |
| `NAMES` | `overrides.py` | `{(source, wire key): (long name, description)}` only for a short key Massive gives no long name anywhere: the name is derived from Massive's own description of the field, which the entry quotes exactly; a changed description makes the entry stale (`stale-entry`). `NAMES` never renames something the vendor already names, and never coins a dataset key column. | snapshot `min.av` → `accumulated_volume` ("The accumulated volume."), `min.dav` → `accumulated_decimal_volume`; WebSocket `av`, `dav`, `op` |
| `KINDS` | `overrides.py` | `{kind: {group: [sources]}}`: the sources of one dataset kind, grouped per asset class, in source order. Every dataset with a stream or flat-file source has a kind, WebSocket-only `luld`, `imbalances`, and `fmv` included. | `trades: {stocks: [/v3/trades/{stockTicker}, /v2/last/trade/{stocksTicker}, (stocks, T), us_stocks_sip/trades_v1], crypto: […]}` |
| `PROJECTIONS` | `overrides.py` | see above | indices snapshot → values, `last_updated` → `timestamp`; the forex last quote and the crypto `XQ` stream → quotes, `ask`/`ap` → `ask_price`, `bid`/`bp` → `bid_price` (2026-10-27: their sources spell the prices as Massive's last-quote objects do, and the quotes table keeps one column per field) |
| `VENUES` | `overrides.py` | the futures venues, folded into the `{venue}` template | `cme`, `cbot`, `nymex`, `comex` |
| `UNITS` | `overrides.py` | the verified unit per `(source, wire key)`, `unverified-ms` where [section 23](#23-open-verification-items) says so, or "not a timestamp" (a timestamp-like field that is not an epoch, kept as it is); with the tag-conflict list | the 20 indicator `IMicroseconds` tags are ms |
| `TYPE_FIXES` | `overrides.py` | a field whose documented samples its declared type does not fit (a wrong or missing type, or a second form the vendor sends): one named `Conversion`, field-scoped, that the decoder applies deliberately (see **Named conversions** below), each citing the sample; each is exercised by a fixture test, and one the vendor's own docs do not show goes into section 23 | every row of the conversions table below |
| `KEY_FIXES` | `overrides.py` | `{(source, declared key): (wire key, reason)}`: a field the specification declares under one key while every documented sample (the response example and the documentation sample) sends it under another that nothing declares; the field is read under the samples' key, keeping its declared type and description, and every other hand entry names it by that key. Stale once the declared key is gone, or the example no longer sends the corrected key or sends the declared one; each goes into section 23 | ticker events `event_type` → `type` (column `Type`); futures snapshot `details.ticker` → `ticker` and `details.product_code` → `product_code` |
| `DESCRIPTION_FIXES` | `overrides.py` | `{(source, wire key): (vendor, text or asset classes, reason)}`: a field, or a nested object whose description every field below it carries as a note, whose vendor description is clearly wrong for its source (another asset class's or event's text copied onto it, or a statement the vendor's own documentation contradicts), or missing where a sibling source describes the same field (the entry then quotes `""`); the entry quotes the vendor's text (as plain text, after normalization) and gives the replacement, either `text` for every use of the source, or, for a source that serves several asset classes, `asset_classes`: the text a dataset of each listed class shows, every other class keeping the vendor's. The replacement is Massive's own wording for the same field of a sibling source wherever that is right and says what the field holds; it is written here only when no sibling text is adequate, and the reason says so. Only that `(source, wire key)` changes: the field keeps its name and type, and every other source keeps its own description. Stale once no generated field looks the entry up (its source or key is gone, or the key is one a `KEY_FIXES` entry replaces or only an example sends), or the vendor's text no longer matches the quote, so a rewritten description is reviewed again | the crypto last trade's and trades flat file's `price` ("actual dollar value per whole share", stock text) → the price in the pair's second currency; the stocks last trade's `y` ("when the quote was actually generated") → "when the trade was actually generated"; the futures trades' `price` ("100 contracts × $2.00 = $200.00", no multiplier) → the `futures/T` channel's text; the unified `/v3/snapshot` `last_trade.price` → the options trades' text, for `options` only; the futures snapshot's `product_code` (no description) → the `/futures/v1/contracts` text; the unified `/v3/snapshot` object `last_trade` ("The most recent quote…") → the options chain snapshot's "The most recent trade…"; its object `last_minute` ("…for this stock") → "…for this contract", for `options` only, written in the entry since no sibling has a minute aggregate |
| `SAMPLE_ERRORS` | `exclusions.py` | `(source, wire key, sample, reason)`: a documented example value that is wrong while the type is right, or a key a WebSocket example sends that the message does not declare, which the `example-type` gate skips | IPO `us_code` example `37833100`, a CUSIP that lost its leading zero |
| `TRANSPORT_ONLY` | `overrides.py` | `{(kind, group, column): reason}`: columns legitimately on one transport within one group | `(bars, stocks, AccumulatedVolume)` (WebSocket only) |
| `RESULT_KEYS` | `overrides.py` | the paths whose rows are not under `results`: another key, a dotted path of keys, or `None` for a body that is itself the rows | `/v1/last/crypto` → `last`; indicators → `results.values`; ticker events → `results.events`; `/v1/marketstatus/now` → `None` |
| `PARAM_UNITS` | `overrides.py` | `{(path, base parameter): (unit, reason)}`: the unit of every timestamp-like string parameter, and of any other string parameter documented as taking an epoch, or "not an epoch" (sent as date or date-time text) | `/v3/trades` `timestamp` → ns; indicators `timestamp` → ms; aggregate range `{from}`/`{to}` → ms; benzinga `last_updated` → not an epoch |
| `EXCLUSIONS` | `exclusions.py` | `(kind, item, reason)`, the executable rows of section 8.12 | `/stocks/filings/vX/form-5` |

**The naming policy** (owner-approved), applied mechanically by the generator, with no
per-column exceptions:

1. **The vendor's long name.** Where Massive's data carries a long field name (REST
   `/v3/trades` `participant_timestamp`, flat-file `ask_price`), the column is its PascalCase
   form (`ParticipantTimestamp`, `AskPrice`).
2. **Abbreviated keys** (WebSocket `p`, aggregate `o`, forex `a`) take the long name Massive
   itself uses for the same field elsewhere in that asset class's REST or flat-file data. The
   lexicons are exactly this mapping, one per object shape, and every entry cites the vendor
   endpoint or file it comes from. One field gets one column on every endpoint and transport:
   crypto trade `t` is `ParticipantTimestamp`, forex quote `a`/`b` are `AskPrice`/`BidPrice`.
3. **No long name anywhere** (rare: snapshot and WebSocket `av`, `dav`, `op`): the name is
   derived from Massive's own description of the field and recorded in `NAMES` with the
   description quoted. `NAMES` holds only these entries, and an entry names only the key's own
   segment; its parents are named by rules 1 and 2.
4. **Nested objects.** A child column is the parent's name followed by the child's, with no
   exceptions: `day.o` → `DayOpen`, `min.av` → `LastMinuteAccumulatedVolume` (the v2
   snapshot's `min` is the object `/v3/snapshot` and the futures snapshot call `last_minute`),
   `lastQuote.P` → `LastQuoteAskPrice`.
5. **Dataset keys.** The only renames on top of rules 1–4 are each dataset's declared key
   columns, `Symbol`, `Timestamp`, and `Underlying`, made by the run-time key rename. A vendor
   field literally named `symbol` (the open-close and conversion paths) is generated as
   `Symbol`, its own name, and the key rename uses it as is; `NAMES` may never coin a key column
   (the `column-collision` gate). There are no `Ticker` or `Pair` stand-ins.
6. **Order.** Columns keep the vendor's natural order, recorded explicitly in each generated
   file's field lists. Massive's REST documentation lists every object's fields
   alphabetically (the specification's keys, its pages' response-attribute tables, and their
   sample responses all agree), so a REST source keeps that documented order, and a
   REST-only dataset's columns are alphabetical. The sources of each group of a dataset kind
   take their order from the group's flat-file "Example Data" headers and WebSocket messages,
   which list fields in their vendor's order (the stocks aggregate files: `ticker, volume,
   open, close, high, low, window_start, transactions`), then the group's other sources' own
   fields; every member that keeps all its fields (unprojected, or with a projection that only
   renames) takes that order. A group with no flat-file or WebSocket member keeping all its
   fields fails the refresh (`column-order`). The snapshot stores every document in the order served, never re-sorted. The
   dataset's key columns come first at run time.
7. **Consistency.** Within one dataset kind, one column for two concepts fails the refresh
   (the `concept-collision` gate); one concept always gets one column, by rules 1–4.

Names are never derived from descriptions except under rule 3, and the generator emits
`Timestamp` only where the vendor's long name is `timestamp`.

**Cross-table naming.** Massive sometimes spells one concept differently on different
endpoints, for example `day.open` and `session.open`, `ask` and `ask_price`, or a nested
`details.strike_price` and a top-level `strike_price`. Within one table every field has
exactly one column. Across tables, each table keeps the spelling of its own endpoint, so its
columns match Massive's documentation for it. Unifying the names across tables (about 30
concepts, 59 REST columns) and a documented "same concept as" map were both considered and
declined: the canonical names would shift with Massive's endpoint mix, some concepts look
alike but differ (a futures session is not a calendar day), and no automatic check can find
synonyms reliably.

**Refresh gates.** The refresh fails, printing the gate, the source, the key, and its
description for the reviewer. Every gate has a descriptive name, which is how the code
(`refresh/gates.py`, `Gate`), its messages, and its tests refer to it. It fails on:

- **`short-key`:** a short key (three characters or fewer, no underscore) with no lexicon,
  identity, or `NAMES` entry;
- **`column-collision`:** two keys of one source producing one column, or a `NAMES` entry
  coining a dataset key column (`Symbol`, `Timestamp`, `Underlying`);
- **`timestamp-unit`:** a timestamp-like field left to a default. It runs on the snake case of
  the long name, after the lexicon and `NAMES` (so `serverTime` is `server_time`): a field is
  timestamp-like when it has a unit tag, a doc type of `timestamp - integer`, or a long name
  matching `timestamp`, `*_timestamp`, `updated`, `*_updated`, `window_start`, `window_end`, or
  `*_time` (the bar lexicon gives `s` as `window_start` and `NAMES` gives `e` as
  `end_timestamp`, while a trade's `s` is `size`). A timestamp-like `long` or `double` needs a `UNITS` entry (a unit,
  which only an integer may take, or "not a timestamp"); timestamp-like text with no `format`
  needs a `UNITS` "not a timestamp" entry or a `TYPE_FIXES` entry; text with a `format` is
  decided by the representation rules. A timestamp-like string REST parameter (by its base
  name) with no `PARAM_UNITS` entry fails the same gate;
- **`stale-entry`:** a stale hand entry: a lexicon shape, `NAMES`, `UNITS`, tag-conflict,
  `TYPE_FIXES`, `KEY_FIXES`, `DESCRIPTION_FIXES`, `PROJECTIONS`, `KINDS`, `RESULT_KEYS`,
  `PARAM_UNITS`, or `EXCLUSIONS` entry naming a source, field, or parameter the snapshot lacks,
  or claiming what the snapshot contradicts (a type fix whose declared type the spec no longer
  gives, a key fix whose key the example no longer sends in place of the declared one, a tag
  conflict the tag no longer contradicts, a `NAMES` or `DESCRIPTION_FIXES` quote the
  description no longer matches, an exclusion of a path the spec already marks deprecated or
  draft); an identity entry no generated object uses is stale too, and so is a
  `DESCRIPTION_FIXES` entry no generated field looked up, which is checked against the keys the
  generators looked up rather than every key a source declares or an example sends;
- **`type-agreement`:** within one kind, one column name joins to one `WireType`, unless a
  `TYPE_FIXES` entry resolves it; the message prints both keys' descriptions, because a
  conflict is first treated as a possible naming error (stocks and futures `dv` are exactly
  that);
- **`transport-agreement`:** within one `KINDS` group whose sources span two or more
  transports, every column appears on at least two transports or is in `TRANSPORT_ONLY`,
  which catches a one-sided lexicon slip (`s` becoming `Shares` while REST says `size`);
- **`venue-agreement`:** a futures kind whose venue pages differ;
- **`column-order`:** a source whose column order cannot be taken from its kind: its
  `KINDS` group has no flat-file or WebSocket member keeping all its fields to give the
  order, or two of its groups order its columns differently (naming policy, rule 6);
- **`concept-collision` (one concept, one column):** within one kind, one column name holding
  two concepts. A concept is a field's full long-name path, each segment in snake case, so `DayClose` and
  `PrevDayClose` are two concepts, while the v2 snapshot's `lastTrade.p` and the unified
  snapshot's `last_trade.price` are one; two paths that PascalCase alike (`last.trade_price`,
  `last_trade.price`) fail. The reverse, one concept under two column names, cannot arise
  while every column is its concept's PascalCase; `PROJECTIONS`, which rename a member's
  fields, add that check.

The REST refresh also stops on: a vendor file whose SHA-256 differs from the manifest's, or a
snapshot whose files differ from the manifest's list (`snapshot`); a documented endpoint the
spec lacks, a generated path no documentation page names, a page that is not an overview yet
names no endpoint, or an `undocumented` exclusion claim the pages contradict (`docs`); an
unknown `x-polygon-*` extension on a path or operation, a parameter `oneOf`/`anyOf` that is
not a choice of date and date-time strings, or a response it cannot read (`shape`); a `UNITS`
unit that differs from the spec's unit tag without a tag-conflict entry (`units`); an
envelope key of a single-object body with a type not the envelope's (`envelope`); and a
documented example value (of the response's example, or a field's own `example`) that its
generated field refuses, or a response example with no value where the result key points
(`example-type`). A `TYPE_FIXES` entry citing the sample resolves a wrong type; a
`SAMPLE_ERRORS` entry (`exclusions.py`) records a wrong sample, such as the IPO `us_code`
example `37833100`, a CUSIP that lost its leading zero.

Descriptions differ across transports and change about twenty times as often as keys, so
`type-agreement` and `transport-agreement`, not description matching, are the mechanical
checks.

**Field descriptions.** The refresh also records the vendor's description of every field
(corrected where it is clearly wrong), so that users can read what each column means
([section 7.4](#74-column-descriptions)).

- **Where they come from.** REST: the OpenAPI `description` of each response property, after
  `$ref` and `allOf` are resolved, the description nearest the field winning (one written
  beside a `$ref`, an `allOf`, or a one-branch choice describes that use and replaces the
  referenced schema's own); at the 2026-10-27 snapshot, 1,597 of the 1,599 leaf response
  fields of the generated paths carry one (the two without are futures
  `/futures/v1/snapshot` `product_code` and `ticker`). WebSocket: the
  `description` of each message property in `websocket.json`, all 237 fields of the 28
  generated channels at the 2026-10-27 snapshot. Flat files: the description cell of the
  page's "Columns" table, all 178 fields of the 21 generated prefixes. A WebSocket source's
  URLs are the pages of its feed; a flat-file source's, its dataset page (one per venue for a
  futures template). The REST documentation pages are rendered from the same OpenAPI text and add
  nothing to it.
- **Notes of enclosing objects.** A REST field flattened out of a nested object also carries
  the description of every enclosing object that has one, outermost first, each appended to
  the field's own text as a sentence naming the object by its wire key:
  `` The closing price of the asset for the day. From the `session` object: <the object's
  text> ``. Each part, the field's text and every note, that ends without sentence
  punctuation is closed with a period, so no part runs into the next. Massive often
  documents a whole object's behavior only on the object: the unified snapshot's `session`
  omits its prices for a stock not yet traded, and its `open`, `high`, `low`, `close`, and
  `vwap` until the regular open; `lastQuote` and `last_trade` are returned only on some plans;
  `greeks` are left out for some contracts. Without the note, `SessionClose` or `GreeksDelta`
  would show nulls that nothing explains. The note is the object's whole normalized text,
  never shortened or filtered: no mechanical rule tells the sentence that explains a null from
  the sentence that introduces the object, and the explanation is often the last sentence. The
  cost is bounded by the vendor's own texts: at the 2026-10-28 snapshot the longest note, the
  unified snapshot's `session`, is 874 characters on its 16 fields, and 378 dataset columns
  carry a note. A `DESCRIPTION_FIXES` entry (below) under a field's key quotes and replaces
  only the field's own text, never the notes; one under an object's key corrects that
  object's note, on every field below it (the unified snapshot's `last_trade` is described as
  "the most recent quote", and its note says trade); one quoting `""` gives a note to an
  object Massive leaves undescribed. Per-asset-class texts combine: a class that the field's
  entry or a note's entry lists gets the field's text and each note for that class, the
  shared text where an entry lists none, and a note that is empty for a class adds nothing
  to it. A field that a `KEY_FIXES` entry moves out of an object keeps only the notes of the
  objects that still enclose its corrected key. A field at the top of a row, a WebSocket field,
  and a flat-file column have no enclosing object, so no note.
- **Normalization**, by four fixed rules and no hand input, so every outlet can show plain
  text: (1) an HTML link `<a href="url" …>text</a>`, with any other attributes and the `href`
  quoted or not, becomes `text (url)`; (2) then a Markdown link `[text](url)`, with or without a
  title, becomes `text (url)`; (3) a link URL that is root-relative (`/docs/…`) is resolved
  against the origin of the documentation index URL the manifest records
  (`https://massive.com`), so a reader in an IDE or the console can follow it; (4) every run of
  whitespace, newlines included, becomes one space, and the ends are trimmed. Nothing else
  changes: no entity is unescaped, no other markup is removed, nothing is shortened, and a URL
  is kept as fetched otherwise. At the 2026-10-27 snapshot 66 of the generated REST fields'
  descriptions contain a link, one of them root-relative (170 description strings of the whole
  specification do). `refresh/plain_text.py` is the one implementation, used by every
  generator. A test of the committed file fails when any link syntax survives, so a new vendor
  form shows on the refresh that brings it.
- **Corrections.** Where Massive's description is clearly wrong for its source (another asset
  class's or event's text copied onto it, or a statement Massive's own documentation of the
  same field contradicts, such as a price total that leaves out the contract multiplier), a
  `DESCRIPTION_FIXES` entry quotes the normalized text and replaces it for that
  `(source, wire key)` alone, with its reason. The replacement is Massive's own text for the
  same field of a sibling source wherever that text is right and says what the field holds;
  only when no sibling text is adequate (right but saying nothing useful, like the crypto
  WebSocket trade's "The price.", or wrong too) is it written in the entry, and the reason says
  why. A source that serves several asset classes (the unified `/v3/snapshot`) can be corrected
  per class: the entry gives the text for each listed class, and every other class keeps the
  vendor's. A class text that no dataset of its class shows (no such dataset reads the source,
  or an earlier source's text wins the column) fails a catalog test, since the refresh cannot
  see the datasets. A field Massive leaves without a description, where a sibling source
  describes the same field, gets an entry too: it quotes the empty text, so it goes stale once
  Massive adds one, and its text is the sibling's (the futures snapshot's
  `product_code` takes the `/futures/v1/contracts` text). That is the only hand input
  on descriptions; a description that is merely terse or imprecise is kept.
- **No gate.** A field without a description gets `""`, and nothing stops the refresh.
  Descriptions change about twenty times as often as keys, so a gate on them would be noise; a
  changed description is an ordinary line in the refresh diff, except where a
  `DESCRIPTION_FIXES` entry quotes it, which the `stale-entry` gate then sends back for review.
- **A separate generated file**, `spec/descriptions.json`, so that a refresh that only changes
  descriptions leaves `rest.json`, `ws.json`, and `flatfiles.json` byte for byte unchanged,
  and a reviewer sees at once that no schema moved. For the same reason the schema files record
  nothing about the snapshot they came from (no fetch date, no hash of a vendor file): the
  snapshot is named by `fetched` here. It is generated, committed, and stored like the other
  three, shipped as package data, and covered by the same parity test:

  ```
  {
   "generated_by": "python -m deephaven_massive._massive.refresh",
   "fetched": "<the snapshot's fetch date, from the manifest>",
   "urls": {"<transport>": {"<source key>": ["<each vendor URL that documents the source>"]}},
   "descriptions": {"<transport>": {"<source key>": {"<wire key>": "<plain text>"}}},
   "class_descriptions": {"<transport>": {"<source key>": {"<wire key>": {"<asset class>": "<plain text>"}}}}
  }
  ```

  A field without a description has no entry, and a source with no described field has no entry
  under `descriptions`; `class_descriptions` holds only the fields corrected per asset class,
  each asset class an `AssetClass` value. Every generated source has at least one URL, sorted
  and as fetched: for REST, every documentation page that names its path (every generated path
  has one, by the `docs` gate; a path several asset classes share, such as
  `/v3/reference/tickers`, has one page per asset class), as the snapshot's manifest records
  it. The entries are one per line, in generated order, so a reworded description is one
  changed line of the diff.
- **Joined at load.** `Field` gains `description: str = ""` and
  `class_descriptions: tuple[tuple[str, str], ...] = ()` (asset class and text, sorted by
  class), read through `Field.description_for(asset_class)`, which returns the class's own text
  or `description`, and the `spec` loader fills both from `descriptions.json` when it loads the
  schema files, under the same once-only lock. An entry that names a source or wire key the
  schema files lack raises at load: the files are generated together, so only a hand edit can
  cause it. The same load checks the URLs: a source without one, or a URL of a source the
  schema lacks, raises, so `sources` and `provenance` reject the same files. A description
  belongs to a `(source, wire key)`, so a projected field keeps the description of its wire
  key.
  `provenance() -> Provenance(fetched: date, urls: Mapping[tuple[Transport, str], tuple[str, ...]])`
  exposes the fetch date and the URLs of each source, for the footer of the column reference;
  it loads every transport the file gives URLs for. Each file is read once under the lock,
  whichever of `sources` and `provenance` comes first. No other name is added to the interface.

**Dataset schemas** are merged at run time, after the rename, never inferred from data.
`dataset_schema` is a method of the `Catalog` (`_datasets/catalog.py`), and its cache lives on
the catalog object: `_session.py` builds the one `CATALOG` and hands it to every core, and the
transports call `core.catalog.dataset_schema(spec)`. The merge reads the members of a kind
through `Catalog.members(kind)`, so the dataset layer needs no import of the namespace modules:

```
dataset_schema(spec):                       # cached per dataset; computed outside the lock,
                                            # published under it (first writer wins)
  members = datasets of spec.kind, ordered by the group and position of the memberships of
            their sources                         (spec alone for a dataset in no kind)
  for each member m, each source s of m (its role paths, channels, prefixes):
      fields = s.fields through the projection of s's membership of (m.kind, m's group), if any
      cols(m, s) = rename(m, s, fields)      # m's keys for s: symbol -> Symbol, time ->
                                             # Timestamp, an instrument fanout -> Underlying
                                             # (any other fanout and session keep their
                                             # columns); a key filled from the request adds
                                             # a constant column; nothing else changes
  schema = ordered union by column name of every cols(m, s):
           Symbol, then Timestamp, first where the dataset declares them; then first
           appearance in member, then source, then field order
  each column's WireType = the lattice join over every contribution
```

**Named conversions.** A `TYPE_FIXES` entry names one field of one source and one
`spec.types.Conversion`, and the generated `Field` records it as `conversion` (`rest.json`
writes its name). Each conversion has one meaning wherever it is used: the types the
specification may declare (`declarations`; a changed declaration makes the entry stale), the
type the field gets, whether it widens (accepts a form the declared type refuses) or only
retypes values the declared type accepts (`widens`), and exactly the forms it accepts, so no entry silently inherits a meaning nobody chose
for it. The plain converter of each `WireType` is never loosened, and no conversion is a
no-op. The decoder applies it deliberately: the WebSocket forex `x` is declared an integer
but sampled as a quoted decimal string (`"44"`), and `quoted-integer` reads either form as
the `long`; a value it cannot read (`"4.8"`, `"x"`) is
an unconvertible value and fails loudly like any other ([section 19.5](#195-runtime-drift)).
Every entry has a fixture test of both the conversion and the failure; a conversion that no
entry declares is coercion, which never happens. The conversions:

| `Conversion` | Declared → wire type | Accepts | Used by |
|---|---|---|---|
| `iso-text-instant` | `string` → `iso-instant` | ISO-8601 instant text | benzinga analysts and firms `last_updated`, futures market-status `timestamp`, `/v1/marketstatus/now` `serverTime`, `/v1/marketstatus/upcoming` `open` and `close` |
| `iso-date-text` | `iso-instant` → `date` | `YYYY-MM-DD` | futures products `last_updated` (sample `"2025-02-22"`) |
| `quoted-integer` | `long` → `long` | an integer, or decimal digits in text | WebSocket forex `C` `x` (sample `"44"`) |
| `text-or-integer-id` | `long` or `string` → `string` | text or an integer | the crypto trade ID, one decision: the full-market and movers snapshots `lastTrade.i`, declared text (samples `413131` and `"517478762"`), and WebSocket `XT` `i`, declared an integer (sample `14272084`); TMX `tmx_company_id`, declared an integer (sample `"2208"`) |
| `number-label` | `double` → `string` | text, or a finite number as float text (`1` gives `"1.0"`) | risk-factor `taxonomy` (sample `"1.0"`) |
| `code-list-text` | `long` → `int-array` | one integer, or integers separated by commas in text | flat-file `conditions` and stocks quotes `indicators`, one decision (sample `"12,37"`) |
| `midnight-utc-date` | `date` → `date` | `YYYY-MM-DD`, or midnight with offset `Z` or `+00:00`, giving the written date; any other clock or offset fails | indices open-close `from`, crypto open-close `day` (samples `"…T00:00:00.000Z"`); the column stays a date so the open-close sources agree |
| `date-as-epoch` | `date` → `epoch` | an integer count; its unit comes from `UNITS` | futures snapshot `details.settlement_date` (sample `1753851600000000000`, an instant in ns; `/futures/v1/contracts` sends the field as a date) |
| `integer-as-number` | `long` → `double` | any number | WebSocket crypto `XA`/`XAS` `v` (sample `951.6112`), futures `A`/`AM` `dv` (sample `1353.1`) |

The `example-type` gate converts every documented example value through its generated field:
the specification's response example (106 of the 109 generated REST paths carry one), each
WebSocket channel's message example, each flat-file page's example rows, and each field's own
`example`. A key a WebSocket example sends that its message does not declare stops the
refresh too, until the message declares it or a `SAMPLE_ERRORS` entry says the example is
wrong (the futures trade example's `z`). A type the vendor's samples contradict stops the
refresh instead of
failing at run time. It also retires fixes: a `TYPE_FIXES` entry is stale once no example
value of the fields it names (all entries sharing one decision, such as the two crypto
snapshots, one decision from one `TypeFix` object) is a value the declared type refuses. A
conversion that only retypes values its declared type accepts (`iso-text-instant`) is
exempt: the declared-type check retires
it once the specification changes the type. A `SAMPLE_ERRORS` entry is stale once the specification no
longer shows its sample, the field accepts it, or the field is gone; samples compare as JSON
text, so `1`, `1.0`, and `true` are three samples.

**Keys are declared per source.** For each source it uses, a `DatasetSpec` declares its key
columns as `KeySpec(column, from_request=False, instrument=True, required=True)`, grouped
per source in a
`SourceKeys`, one optional declaration per role (`symbol`, `time`, `fanout`, `session`):

- every source of a dataset in a kind declares `symbol` and `time`;
- every source of any other instrument dataset declares `symbol`. A dataset is an instrument
  dataset when it takes symbols: it has channels, flat files, `per_symbol`, a symbol
  filled from the request, or a `symbol_param` that filters a single-ticker field. A
  `symbol_param` over a list-valued field (the generated `tickers` field of a filing or an
  article is `json-text`) does not make one: those rows have no `Symbol`
  ([section 10](#10-symbols-and-all)). The field is the row field of the parameter's base
  name, singular or plural, read from the generated path, never from a hand list. Its asset
  class alone does not make a dataset one either: `futures.exchanges` and
  `futures.schedules` keep `FUTURES` for the session-day rule and take no symbols;
- a `symbol` key from the rows is a `string` field: a list of tickers is never `Symbol`;
- any other dataset declares only the keys its rows carry, possibly none: `reference.exchanges`,
  `conditions`, `market_status`, `market_holidays`, and `ticker_types`, `futures.exchanges`,
  and `benzinga.firms` declare none, and `economy.*` declares none either: it keeps its `Date`
  column (its axis is `time_axis` on `date`);
- a `time` key is an instant: its field (after the projection) is an `epoch` or an
  `iso-instant`, so `Timestamp` is always an `Instant`. A dataset whose row time is a
  calendar date keeps that `LocalDate` column under its own name and declares no `time` key;
- `fanout` and (futures) `session` are declared only on the sources that carry them;
- `instrument` says whether a `fanout` key names an instrument: True, the default, renames it
  to `Underlying` (the options underlying, the related company asked about); False keeps its
  generated column (`filer_cik` stays `FilerCik`). Only a `fanout` key may set it False, and
  such a key is a column of the rows, never `from_request`;
- `required` (default True) says whether every row must carry the key. False is for a symbol
  Massive documents as present only "if available" or marks optional, such as the `ticker` of
  the stocks `filings.index`, the TMX corporate events, and the Benzinga `analyst_insights`,
  `ratings`, `bulls_bears_say`, `guidance`, and `earnings` (whose only documented
  analyst-insights row has no `ticker`): a row without it keeps a null `Symbol` instead of
  failing the request as a missing required key. Only a `symbol` key read from a REST path's rows may set it
  False; flat files and streams filter and route rows by symbol, and the `time`, `fanout`,
  and `session` keys are always required. A dataset with an optional symbol key cannot put
  `Symbol` in its `poll_keys`, whatever its mode (`DatasetSpec` refuses it): a row without a
  symbol would have a null poll key, which fails the `poll()` table in every mode, so such a
  dataset declares other `poll_keys`.

A key is `from_request` when the source does not carry it in its rows: the symbol of a source
that takes it as a path parameter or holds it outside the result key
([section 10](#10-symbols-and-all)). The library fills a
`from_request` key as a constant column. `to_batch(..., required=)` and `ReadPlan.required`
(per request) and `WsClient.set_required(channel, keys, *, symbol)` (per channel, before its first
subscription, [section 13.3](#133-connection-and-subscription-protocol)) receive exactly the
source's required keys (declared, not `from_request`, and not marked `required=False`), so a futures WebSocket row without
`session_end_date` is not a missing key, while a futures flat file without it is. Every member
of a kind gets the identical
column set: crypto's `ParticipantTimestamp` is renamed to `Timestamp`, while stocks keeps its
`ParticipantTimestamp` column, so crypto rows hold `ParticipantTimestamp` null. A non-tick
dataset with several role paths (`options.contracts` list and per-contract) is the union of its
role paths under the same join.

**Column descriptions.** Each column of a dataset schema is a
`Column(name, type, description, supplied_by, wire_keys)`: `supplied_by` is the transports of
the dataset's own sources that fill the column (a key filled from the request counts), in the
order `REST`, `FLAT_FILES`, `WS`; `wire_keys` maps each of those transports to the distinct wire
keys behind the column, before any projection, in source order. `dataset_schema` picks one
description per column of each **dataset**, never per kind: a kind's union spans asset classes,
and one class's text is wrong for another (the stocks trades' `price`, "dollar value per whole
share", for a crypto trade). Picking per dataset keeps another member's text off a dataset, but
it cannot correct a dataset's own source when Massive copied the wrong text onto it: the crypto
REST last trade and trades flat file carry that same stock `price` text. The refresh corrects
those through `DESCRIPTION_FIXES` (the hand-input table above), so every source already carries
a description right for it when the rules below pick one. Each field's description in the
rules is the one for the dataset's asset class, `Field.description_for(spec.asset_class)`: a
source that serves several classes may carry a per-class correction (`options.snapshots`
shows the options text of the unified snapshot's last trade, as `options.chain` does), and
a dataset with no asset class takes the field's own description.

1. **A column the dataset's own sources supply** takes the first non-empty description among
   them, trying the transports in the order REST, flat files, WebSocket and, within one
   transport, the dataset's sources in their declared order (role paths, then channels, then
   prefixes). REST comes first because its text is the most complete.
2. **A column that exists only through the kind's union**, which no source of the dataset
   supplies, gets generated library text naming the members that do, in member order:
   `` Always null for `crypto.trades`; supplied by `stocks.trades`. `` (crypto's
   `ParticipantTimestamp` and `Tape`, for example).
3. **`Symbol`, `Timestamp`, and `Underlying`**, when they are declared key columns of the
   dataset, get a fixed library sentence ("The instrument,
   in the library's canonical symbol form.", "The row's primary time, as an Instant.", "The
   underlying instrument, in the library's canonical symbol form."), followed by one sentence
   that starts "From" and lists, per transport in the order of rule 1 and spelled as in
   [section 7.4](#74-column-descriptions), the wire key each source renames and its quoted
   field description (`` REST `sip_timestamp`: "The nanosecond accuracy SIP Unix Timestamp…" ``)
   or, for a key filled from the request, the request parameter it came from
   (`` REST: the request's `stocksTicker` parameter ``). The items are joined by "; ", so a
   lowercase label such as `flat files` never starts a sentence, and the sentence ends with a
   period unless its last quoted description already ends one inside the quote:
   `` The instrument, in the library's canonical symbol form. From REST `T`: "…"; flat files
   `ticker`: "…"; WebSocket `sym`: "The ticker symbol for the given stock." ``

A column whose sources carry no description gets `""`, and nothing fails. The sentences of
rules 2 and 3 are the library's statements about its own columns, not hand input about Massive,
so they live in `_datasets/schema.py`, not in `refresh/`. Every outlet of
[section 7.4](#74-column-descriptions) reads these `Column`s and nothing else.

**Catalog gates** (`tests/datasets/test_catalog.py`, with negative tests on synthetic `DatasetSpec`s):

- **Declared keys:** every key a dataset declares for a source is either a field of that source
  after the projection of the dataset's membership (the `indices.values` time key on
  `/v3/snapshot/indices` is `timestamp` only after it), or `from_request`; every source of a
  dataset in a kind declares `symbol` and `time`, and every source of an instrument dataset
  (one that takes symbols) declares `symbol`; a `time` key is an `epoch` or `iso-instant`
  field; a dataset's `fanout` keys agree on `instrument` across its sources and across the
  members of its kind. Its negative tests cover an undeclared kind key, an undeclared
  instrument symbol, a declared key the source lacks, a date as the time key, and reference
  datasets that declare none and pass, one of them with an asset class (`futures.exchanges`);
- **Unique columns:** after the rename, no source has a duplicate column;
- **Member types:** across members, one column name gets one `WireType`;
- **Kind membership:** each kind dataset's sources equal the sources holding a membership of its
  `(kind, group)`;
- **Wire parameters:** every declared wire name (time-axis pair, symbol parameter, path
  placeholders) is a parameter of its `RestPath`;
- **Append keys:** a dataset whose default `poll()` mode is `APPEND` declares its own
  `poll_keys`, not the default `Symbol`. The gate checks only that the key is explicit; that it
  is unique per row is checked by the dataset's own tests. (`DatasetSpec` refuses empty
  `poll_keys` on an `APPEND` dataset, an empty identity (no `poll_keys` and no
  `revision_keys`) on any dataset, since an explicit `APPEND` could not tell rows apart, and
  `revision_keys` that repeat a poll key.)
- **Nullable keys:** a dataset with `poll_keys` has at least one that is not in
  `nullable_poll_keys` (`DatasetSpec` refuses a nullable key that is no poll key, or one without
  a reason);
- **Poll keys:** every `poll_keys` and `revision_keys` column is a column of the dataset's
  schema;
- **Generated sources:** every path, channel, and prefix a dataset uses is generated; a miss names
  the dataset and says "deprecated or removed in the snapshot";
- **Coverage:** generated = used ⊎ deferred: every generated source is read by some dataset,
  or deferred with a reason and an issue
  ([section 8.12](#812-excluded-endpoints)).

`KINDS` overlaps the catalog by design: the refresh checks names before any dataset is
declared, and the kind-membership gate keeps the two in step. A test loads `spec/` with `refresh/`
absent, as in an installed wheel; the CI build job checks that the wheel holds no `refresh/`.

### 19.5 Runtime drift

Data on the wire can differ from the committed schema until the next refresh. **Detection** is in
`_massive`: each decoder compares a payload with its generated source and returns a
`DriftReport(extra, missing, statuses)` (`missing`: only vendor-`required` fields that some
row lacked or held as null; `statuses`: REST envelope statuses outside the
documented ones). A decoder never raises for drift; REST and flat files raise
`WireError` for a bad value, and WebSocket returns `Reject`s. **Policy** is in the library: the
session's `DriftRegistry` in `_tables/diagnostics.py` (a leaf lock, WARNINGs logged after release) and the
routing in `_ingest/rest.py`, `_ingest/poll.py`, `_ingest/ws.py`, `_tables/stream.py`, and `_ingest/flatfiles.py`.

| Case | REST (`rest/batch.py`) | WebSocket (`ws/protocol.py`) | Flat files (`flatfiles/reader.py`) | Library |
|---|---|---|---|---|
| **Extra field** | key not in the path's fields | key not in the channel's fields | header not in the documented headers | Dropped. WARNING once per (dataset, transport, field). Shown in `SchemaDrift`. |
| **Unknown envelope status** | a `status` outside `OK`, `DELAYED`, `success`, `ERROR`, `NOT_AUTHORIZED`: the rows are kept, `Page.drift.statuses` | | | WARNING once per (path, status). Never a failure. |
| **Missing required key** (a key the dataset declares for this source, not `from_request`; a dataset that declares no key has none) | `WireError` naming the key | `Reject(channel, symbol or None, WireError)` | `WireError` before the first block | Loud failure. |
| **Missing other field** | null | null | a null column of the declared type | A vendor-`required` field or a CSV header: null, WARNING once, `SchemaDrift`. Any other field: null, silently, because JSON omits absent values and the vendor marks almost nothing required. |
| **Unconvertible value** (the type, the `int64` range, `bool` given as a number, an `int-array` element, `int32` for `int[]`) | `WireError` naming the field, the expected type, and the value (first 50 characters) | `Reject` for that row's topic; the frame's other rows are delivered | `WireError` naming the header, the type, and Arrow's message | Loud failure. Never coerced: no undeclared conversion is applied. A lattice widening and a named `TYPE_FIXES` conversion ([section 19.4](#194-generated-schemas), for example forex `x` from a decimal string to `long`) are not coercion; a value the named conversion cannot parse fails here. |
| **Timestamp out of [1990, 2100], or overflow** | in `_datasets/timestamps.py` | in `_datasets/timestamps.py`, in the flush thread as a vectorized mask | in `_datasets/timestamps.py` | Loud failure naming the dataset, the column, the unit, and the value. |
| **Undocumented value of an interpreted field** (`timeframe`) | `_ingest/hosts.py` | | | `MassiveError` ([section 13.2](#132-host-decision)). |

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
requires it byte for byte, and checks documented = generated ⊎ excluded ⊎ (deprecated ∪
draft).

**The snapshot is stored canonically**, in `vendor/massive/`, and `refresh/snapshot.py` is the
one owner of its format: the layout names, the manifest, the staged write and atomic swap, and
the verified load. `fetch.py` puts each download in canonical form: JSON with `indent=1` and a
final newline, keeping the order in which the vendor served each object's keys (the documented
column order, never re-sorted), and text with trailing whitespace stripped and a final newline,
so refresh diffs read line by line and no hook exception is needed. `manifest.json` records the
fetch date and, per file, its URL and the SHA-256 of the fetched bytes and of the stored file.
Every refresh re-hashes every file against the manifest and requires the files on disk to be
exactly the manifest's, so a hand-edited vendor file stops it; the generated files record
only the fetch date (`descriptions.json`); the manifest committed beside them holds every hash,
and the parity test binds the two. Besides the
specs, `--fetch` stores every REST, WebSocket, and flat-file page `llms.txt` links, under
`vendor/massive/rest/`, `websocket/`, and `flat-files/`, because `llms.txt` names pages, not
paths: each REST page's `**Endpoint:**` line is what ties the documentation to an OpenAPI
path, each WebSocket page names its feed's cluster and channel, and the flat-file overview
and dataset pages are the flat-file schemas. The documentation host rate-limits a burst of page requests
with HTTP 429, which the fetch waits out (`Retry-After`, capped at 60 s, else 2 to 32 s, five
times); any other failure stops it and leaves the old snapshot in place, and the new snapshot
is swapped in through a backup directory (`vendor/massive-incoming/` and
`vendor/massive-previous/`, both gitignored). The snapshot (about 4 MB, 240 files, fetched
2026-10-27 UTC) is Massive's published content, vendored as generation input and not covered
by the repository's license, so it ships in neither the sdist nor the wheel.

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
with its action; with no drift it does nothing. GitHub notifies no one of an edited issue
body, so when the report differs from the open issue's body the job also posts one short
comment giving the new and the previous item counts, with no vendor text; an unchanged report
posts nothing, and opening or reopening the issue, which notifies, adds no comment. Its permissions are `contents: read` and
`issues: write` and nothing else: the checkout needs read access because the repository is
private, and the issue and its label need write access to issues. It never pushes, never opens
a pull request, and is never a gate.

**Closing the loop.** A person runs `--fetch` and the plain refresh, then reviews one PR holding
the snapshot diff, the `spec/*.json` diff, and any hand-input lines, and `check.sh` runs every
refresh gate and the catalog gates. What each change does:

| Change at Massive | At run time | In the refresh |
|---|---|---|
| Field added | Dropped, one WARNING, `SchemaDrift`. | It gets a name (`short-key` asks for a lexicon or `NAMES` line for a short key) and joins its kind (`type-agreement` checks the type, `transport-agreement` the transports). |
| Field removed | Null; a required field or CSV header warns; a required key fails loudly. | It leaves its source; the declared-keys gate fails if it was a declared key. |
| Field renamed | A removal plus an addition. | The column is renamed in the diff, or stays the same if the long form is unchanged; `transport-agreement` catches a one-sided rename. |
| Type changed | A value that no longer converts fails loudly. | `type-agreement` or the member-types gate fails on disagreement, resolved as a naming error first, then by a vendor fix or a `TYPE_FIXES` entry. |
| Unit changed | The overflow check or the bound fails loudly. | `--check` reports the changed tag, and the `UNITS` entry is re-verified. |
| New endpoint, channel, or file | Nothing. | `--check` reports it as unaccounted, the refresh generates it, and the coverage gate fails until a `DatasetSpec`, a `DEFERRED` entry, or an `EXCLUSIONS` entry is added. |
| Endpoint deprecated or removed | `MassiveError` with the HTTP status. | It leaves the generated set, and the generated-sources gate names every dataset that uses it. |

## 20. Quality gates

Every change must pass `scripts/check.sh full`, and every CI job the change selects, the
free-threaded 3.14t job included whenever the tests run; `ci-ok` is the required check that
enforces it. A pull request into `main` runs only the CI jobs its changed files can affect, read
from the diff between `main` and GitHub's merge of the pull request (a renamed file counts under
both paths):

- **Always:** the lint job, which lists the changed files and decides, and `ci-ok`, the single
  required check. `ci-ok` passes only when the lint job passed, each decision is exactly true or
  false, every job the change selected passed, and every other job was skipped.
- **Lint** (ruff, mypy, pyright): a file under `src/`, `tests/`, or `scripts/`, or
  `pyproject.toml`.
- **Tests** (the whole matrix, 3.14t and the lowest-direct job included): a file under `src/`,
  `tests/`, `scripts/`, `vendor/`, or `docs/datasets/`, or `pyproject.toml` or `.gitattributes`.
- **Build** (the sdist, the wheel, and the installed-wheel check): a file under `src/`,
  `tests/`, or `docs/datasets/`, or `scripts/check_wheel.py`, `pyproject.toml`, `README.md`,
  `LICENSE`, `.gitattributes`, or `.gitignore`.
- **Nothing beyond the decision:** files no job reads: `plans/`, `docs/dev/`,
  `docs/live-tests.md`, `AGENTS.md`, `.claude/`, the pull request and issue templates,
  `.github/dependabot.yml`, and `.pre-commit-config.yaml`.
- **Everything:** a change under `.github/workflows/` or `.github/actions/`; any changed path
  that matches none of the lists above, so a new input (a tool's configuration file at the
  root, a new directory, a name git must quote) fails closed until a rule covers it; a pull
  request into a branch other than `main`; every push to `main`; and a weekly scheduled run, so
  a dependency release that breaks the library is caught within a week even when nothing
  reaches `main`.

The gates, in brief ([section 20.1](#201-tooling-configuration) gives the exact configuration):

- **Coverage:** 100% statement and branch coverage. `pragma: no cover` needs a written reason
  and owner approval.
- **Ruff:** `select = ["ALL"]` with a documented ignore list, latest version, Google docstring
  convention, `E501` at 100 columns, complexity limits enforced.
- **Mypy:** `strict` plus `warn_unreachable` and the extra error codes; the only override is
  `ignore_missing_imports` for `pyarrow.*`. Pyright runs as a non-gating second checker.
- **Pytest 9:** `strict = true`, `filterwarnings = ["error"]`, `--import-mode=importlib`,
  deadlock detection through `faulthandler_timeout` and `pytest-timeout`.
- **Layout:** `tests/test_layout.py` passes: the mirror rule and naming of
  [section 4.2](#42-test-layout), the library's layers of [section 4.3](#43-layers), and the
  `_massive` boundary and layers of [section 19](#19-wire-clients-and-the-spec-snapshot).
- **Spec:** `tests/massive/refresh/test_generate.py` passes (parity of `spec/*.json` byte for
  byte, documented = generated ⊎ excluded ⊎ (deprecated ∪ draft), and every refresh gate),
  and `tests/datasets/test_catalog.py` passes (the catalog gates, generated = used ⊎ deferred) ([section
  19.4](#194-generated-schemas)). They read only vendored files, never the network.
- **Column reference:** `tests/datasets/test_column_reference.py` passes: `docs/datasets/` holds
  exactly the rendered pages, byte for byte, with no stale, missing, or extra file, and every
  dataset method's docstring names its own section; `tests/namespaces/test_base.py` passes:
  every dataset method's docstring starts with its `DatasetSpec.summary`
  ([section 7.4](#74-column-descriptions)).
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
  closed; never a duplicate), and comments once when the report of an open issue changed. The
  body lists each item with its action:
  - **unaccounted items** (a documented REST endpoint, WebSocket channel, or flat-file dataset
    neither generated nor excluded): refresh and add a dataset, or add an `EXCLUSIONS` entry
    with a reason;
  - **field, type, and column changes** (fields and parameters added, removed, or retyped;
    WebSocket keys; CSV headers; changed unit tags; keys the gates would stop on): run
    `--fetch` and the plain refresh, adding a lexicon, `NAMES`, or `UNITS` line where a gate
    asks;
  - **deprecations** (a generated path newly deprecated or removed): replace the dataset's path
    or add it to `EXCLUSIONS` with a reason.
- Its permissions are `contents: read` (the checkout of the private repository) and
  `issues: write` (the label and the standing issue), nothing else. It never pushes, never
  opens a pull request, and is never a required check, because a change at Massive must not
  block unrelated pull requests.

### 20.1 Tooling configuration

This is the exact configuration the tooling planned PR applies. Every lint ignore carries a
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
  package data, and the CI build job asserts both. The build job also installs the wheel, with
  `deephaven-server`, into a fresh environment and runs `scripts/check_wheel.py` there: inside
  a started server it imports every shipped module, resolves every public name, renders the
  column reference from the installed catalog and compares it with `docs/datasets/`, and opens
  and closes a session. Development dependencies use a PEP 735
  `[dependency-groups]` table; license metadata follows PEP 639. `__version__` comes from
  `importlib.metadata`. `deephaven-server` is a development dependency only. The runtime
  dependencies are declared directly, with floors: `deephaven-core`, `pyarrow`, `boto3`,
  `botocore` (a floor whose `Config` takes `proxies` and `retries={"total_max_attempts": …}`),
  `urllib3`,
  `websockets` (a floor whose `websockets.client` defines `process_exception`, 17.1, which
  also reads proxies from the environment in `websockets.asyncio.client.connect`), and
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
cols = m.stocks.trades(syms).columns()                      # no network: Name, Type, Description, SuppliedBy
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
mover_ratios = m.stocks.financials.ratios(movers).poll(600)               # one ticker.any_of request per cycle
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
    exactly the verbs, `columns()` (see “`columns()` on every query” below), and the namespaces,
    and the API key stays out of objects users routinely leave in console globals.
22. **Names come from the generated tables** (2026-10-26). The generator names every field from
    its wire key, the lexicon of its shape, and `NAMES`; each `DatasetSpec` declares the key
    columns its sources carry, and `rename` in `_datasets/schema.py` maps them to `Symbol`, `Timestamp`, and
    `Underlying`. Names are never derived from descriptions, and the refresh gates and the catalog
    gates fail on a wrong name, type, unit, or merge ([section 19.4](#194-generated-schemas)). The
    final names: `av` → `AccumulatedVolume`, `op` → `OfficialOpen` (2026-10-27: a neutral name
    for the stocks and options opening price and the indices opening level, as `Open` is),
    aggregate `a` → `DayVwap`,
    aggregate `z` → `AverageTradeSize`, `dav` → `AccumulatedDecimalVolume`, stocks `dv` →
    `DecimalVolume`, futures `dv` → `DollarVolume`, and the snapshot object `min` → `LastMinute…`.
    The naming policy of section 19.4 (owner decision, 2026-10-27) governs every name.
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
    helpers, and `tests/datasets/test_catalog.py` is a plain mirror ([section 4.2](#42-test-layout)).
42. **Import roots:** `pythonpath = ["."]` and `mypy_path = ["src"]`, fakes imported as
    `tests._fakes…`; ruff bans `import massive` once the dependency is gone.
43. **Module names:** `_namespaces/` (plain modules of namespace classes), `_ingest/flatfiles.py`,
    `_datasets/history_source.py`, and the wire package `_massive/flatfiles/` (PEP 8 forbids the
    underscore in a package name, and the library module follows the wire package's spelling).
44. **Release through trusted publishing, on the owner's go-ahead.** When the owner turns
    publishing on, `.github/workflows/release.yml` publishes to PyPI with trusted publishing
    (no long-lived token), and the first version is 0.1.0. Until then no publishing workflow,
    GitHub environment, or pending publisher exists. No rehearsal may push a tag that matches
    the project version, because a PyPI version can never be re-uploaded; the rehearsal method
    is chosen at the go-ahead ([section 20.1](#201-tooling-configuration)).
45. **`columns()` on every query** (2026-10-26). A public method, not a verb, returning a
    static table `Name`, `Type`, `Description`, `SuppliedBy` built from the dataset schema with
    no network call, so console and notebook users can read what each column means without the
    web UI or GitHub ([section 7](#7-query-classes-and-verbs)).
46. **Every result carries column descriptions** (2026-10-26). Each verb applies
    `ColumnDescriptions` and a `TableDescription` to the exact table it returns, after its
    shape is derived, on the caller thread inside the result scope, through `jpy`, because
    Deephaven 42.7 has no Python wrapper ([section 7.4](#74-column-descriptions)).
47. **A generated column reference that cannot hold an orphaned page** (2026-10-26).
    `docs/datasets/<namespace>.md` is generated and committed. One command,
    `uv run python scripts/render_reference.py`, owns the whole directory: it renders every
    page, deletes every file it did not render, and writes the rest. The parity test fails on a
    stale page, a missing page, and any extra file. Dataset docstrings link to their section and
    never list columns ([section 7.4](#74-column-descriptions)). The renderer and `sync` live in
    `_datasets/column_reference.py`, which takes the catalog as an argument, so the name never
    collides with the `m.reference` namespace and the renderer needs nothing from the API.
48. **Vendor descriptions in a separate generated file** (2026-10-26). `spec/descriptions.json`
    holds every field's vendor description (corrected where it is clearly wrong, decision 51),
    normalized to plain text by fixed rules, with no
    gate on a missing one, and is joined into `Field.description` at load; one rule picks each
    dataset column's description ([section 19.4](#194-generated-schemas)).
49. **Each table keeps its endpoint's spelling** (2026-10-27). One concept spelled two ways by
    Massive gets one column within a table, but no cross-table unification or synonym map
    ([section 19.4](#194-generated-schemas), "Cross-table naming").
50. **Dataset gaps settled while declaring the namespaces** (2026-10-28). `order` is sent as
    is where a path has its own `order` parameter and folded into `sort` as a `.asc`/`.desc`
    suffix where it has none, and only there is `order` without `sort` a `ValueError`; a
    dataset whose paths have neither parameter offers neither, and the indicators, whose
    paths have `order` but no `sort`, keep `order` alone ([section 9](#9-filters)). The whole family of every typed
    parameter is reserved from `filters` in one shared check. Instant axes are half-open
    (`.gte`/`.lt`), date axes whole days (`.gte`/`.lte`). Futures trades and quotes take the
    `timestamp` range widened to whole Chicago sessions, as bars do (superseded by decision 52)
    ([section 12](#12-timestamp-units-and-day-boundaries)). The REST transport fills path
    parameters from `Request.params`, fills the bars `{from}`/`{to}` from `history()`'s bounds,
    batches symbols by `DatasetSpec.symbols_per_request`, picks a path per symbol class by
    `DatasetSpec.fetch_paths_by_class`, sends only the parameters the chosen path declares,
    picks a per-symbol detail path from
    the role paths, canonicalizes symbols read from rows, and drops error rows only from a
    projected `fetch()` ([section 13.4](#134-rest)). The § 8 rows gain `product_code` on every
    futures dataset that takes it, `adjusted` on the options `daily_open_close` (the crypto
    `bars` and `daily_open_close` take none: crypto has no corporate actions), `filters` on
    `consensus_ratings`, `type=options` on `options.snapshots`, and the generated
    axis of `share_float` ([section 8](#8-namespace-tree)). The indicators take
    `adjusted: bool | None = None`: None sends nothing, an explicit value is sent where the
    chosen path declares `adjusted`, and raises `ValueError` on the crypto templates, which do
    not.
51. **Wrong vendor descriptions are corrected by hand input** (2026-10-28). Where Massive's
    own description of a field is clearly wrong for its source (another asset class's or
    event's text copied onto it, or a statement Massive's own documentation of the same field
    contradicts), a reasoned `DESCRIPTION_FIXES` entry in `refresh/overrides.py` quotes it and
    replaces it, per asset class for a source that serves several; a merely terse or imprecise
    description is kept. The replacement is Massive's own sibling text wherever that is right
    and adequate, and is written in the entry only otherwise. `spec/descriptions.json` changes
    only through the refresh ([section 19.4](#194-generated-schemas)).
52. **Date ranges: two kinds, whole bar windows, UTC days for forex and crypto** (2026-10-28).
    `history(start, end=None)` takes two dates (whole days, inclusive; for futures, whole
    sessions) or two aware datetimes (exact instants, half-open), never one of each, with no
    inclusive or exclusive option; `end=None` reads through now; a reversed range raises,
    equal datetimes are an empty table, and equal dates one day. A calendar-date axis of a
    `…T` dataset takes dates only. A bar of any timespan is returned only when its whole
    window lies inside the range and has ended: Monday-to-Friday weeks and calendar months,
    quarters, and years less their leading and trailing weekend days for stocks, options, and
    indices; whole calendar weeks and periods for forex and crypto, which trade at weekends;
    futures periods of session dates, refusing bars of several sessions or longer named by
    dates until their label is verified; there is no holiday calendar and no option to match
    a period by its start or end instead. Futures trades and quotes
    named by dates are requested one weekday session at a time by `session_end_date`, which
    replaces the widened timestamp window and the holiday-fold heuristic; futures bars named by
    dates are requested from the first open less four days to 18:00 Chicago on the last date,
    with no probe. A futures flat-file range named by datetimes that ends on a holiday rolls
    forward up to 7 days and raises `MassiveError` rather than return a short table. Forex and
    crypto 1-day bars mean the UTC day and are stamped 00:00 UTC on every source; a crypto
    stamp elsewhere in its day than its first or last millisecond, or a crypto REST bar of
    several days or longer not at 00:00 UTC, raises `MassiveError`; `AUTO` chooses the source
    for them as for every dataset, the check keeping both sources on the same UTC days, and
    the owner decides again after the crypto day-cut live check. A one-day calendar parameter
    (`daily_open_close`'s `date`) refuses datetimes like a calendar-date axis, and an open
    `end` on a list sends no upper bound, so future-dated rows such as an announced
    ex-dividend date appear ([section 12](#12-timestamp-units-and-day-boundaries),
    [section 16](#16-historysourceauto)).
53. **One spelling for the transports in user-facing text** (2026-10-28). The key-column
    descriptions, `columns()`'s `SuppliedBy`, and the column reference spell the transports
    `REST`, `flat files`, and `WebSocket`, so a filter copied from a page works on
    `columns()`; the internal `Transport` values (`REST`, `FLAT_FILES`, `WS`) are unchanged.
    A key column's description lists its sources in one sentence starting "From", joined by
    "; ", so a lowercase label never starts a sentence ([section 7.4](#74-column-descriptions),
    [section 19.4](#194-generated-schemas)).
54. **A missing description is filled from a sibling source** (2026-10-28). Where Massive
    gives a field no description but describes the same field on a sibling source, a
    `DESCRIPTION_FIXES` entry quotes the empty text and takes the sibling's, with its reason,
    so no column of a dataset is left blank for want of text Massive already wrote
    ([section 19.4](#194-generated-schemas)).
55. **Each dataset links its own Massive documentation** (2026-10-28). The column
    reference lists each dataset's vendor pages under that dataset's section instead of one
    page-wide list at the foot of the page, which tied no link to a dataset and, through a
    source several asset classes share, linked unrelated classes' pages (the options page
    listed the stocks, forex, crypto, and indices unified-snapshot pages). A dataset of one
    asset class links only that class's page of a shared source; the footer keeps the fetch
    date ([section 7.4](#74-column-descriptions)).

## 23. Open verification items

Each item is handled conservatively now, covered by a fixture test in its unit-test module, and
retired by a live integration test (`*_integration.py`, marked `integration`, skipped without
`MASSIVE_API_KEY`); [`docs/live-tests.md`](../../docs/live-tests.md) says how to run them, each
test's docstring what its evidence means, and the comment on issue #26 what each outcome
changes. Each item that a live test refutes becomes an owner decision
before 1.0. Items 17 and 18 concern Deephaven, not Massive: the work that adds column
descriptions settles them in the embedded test JVM and the web UI, and they need no live test.

| # | Item | Conservative handling | Fixture test | Live integration test |
|---|---|---|---|---|
| 1 | Day cut of crypto 1-day bars on every source, checked first: the documented REST samples stamp a daily bar at midnight New York time (the aggregates range, `/prev`) or at no day boundary at all (grouped daily, at 19:18, 20:08, and 20:48 UTC), while the `global_crypto/day_aggs_v1` sample stamps it at 23:59:59.999 UTC of its day, and both pages' prose says UTC | A 1-day bar means the UTC day: a stamp at the first or last millisecond of a UTC day is moved to its 00:00 UTC, and any other stamp, or a REST bar of several days or longer not at 00:00 UTC, raises `MassiveError` naming the dataset, the source, and the other source. `AUTO` picks the source as for every dataset, so the check alone keeps both sources on the same UTC days. If live REST follows the samples, crypto daily REST history, `dm.ALL` grouped daily, and `/prev` all raise, and the owner decides the source rule then | `tests/namespaces/test_crypto.py`, `tests/ingest/test_rest.py`: both cuts map to 00:00 UTC on every source; the documented samples and a two-day bar at midnight New York time raise | `tests/namespaces/test_crypto_integration.py`: one 1-day bar of `X:BTCUSD` from `/prev`, the range, grouped daily, and `day_aggs_v1` for one day; record each `Timestamp` and whether the OHLCV agree |
| 2 | Unit of REST forex `/v1/last_quote` `timestamp`, crypto `/v1/last` `timestamp`, and forex `/v1/conversion` `last.timestamp` | Registered as ms (`unverified-ms`); the spec tags all three fields `IMilliseconds`, which agrees; the overflow check and the [1990, 2100] bound raise `MassiveError` on a mismatch | `tests/datasets/test_timestamps.py`: ms and ns fixtures; the out-of-range path raises | `tests/datasets/test_timestamps_integration.py`: one request each; the converted `Instant` is within 1 day of now |
| 3 | Unit of WebSocket `FMV` `t` (documented ns, sample ms) | Magnitude rule for FMV only; logged once | `tests/datasets/test_timestamps.py`: both magnitudes convert correctly | `tests/namespaces/test_crypto_integration.py` (Currencies Business): one FMV frame converts to a current `Instant` |
| 4 | Futures flat files keyed by session date | Check every block of every futures file against its key; fail loudly otherwise | `tests/massive/flatfiles/test_reader.py`: matching and mismatching fixtures | `tests/ingest/test_flatfiles_integration.py` (S3 credentials): one key per venue |
| 5 | Indices `V` host (`business.` on the page; `socket.`/`delayed.` in the plan table) | `V` uses the class TICK host; a refusal names `ws_hosts[AssetClass.INDICES]` and `business.massive.com` | `tests/ingest/test_hosts.py`: both replies | `tests/ingest/test_hosts_integration.py`: subscribe `V.I:SPX` on the resolved host |
| 6 | TICK host for a base Business key | `business.massive.com` with a WARNING | `tests/ingest/test_hosts.py`: the snapshot shape with `fmv` and no trades/quotes resolves to `business.` | `tests/ingest/test_hosts_integration.py` (Business key): `AM.AAPL` is acknowledged on the resolved host |
| 7 | Futures snapshot `timeframe` fields (in the OpenAPI spec; absent from the documentation page) | Absence yields `socket.` with `Recency=UNKNOWN` and the explicit-verdict rule | `tests/ingest/test_hosts.py`: with and without the fields | `tests/ingest/test_hosts_integration.py`: record which fields the live payload carries |
| 8 | Exact text of the delayed-key verdict message ("real-time") | Case-insensitive substring match; limited to `Recency=UNKNOWN`, whole-step refusal, unpinned host | `tests/ingest/test_ws.py`: verdict and non-verdict messages | `tests/ingest/test_ws_integration.py` (delayed key): subscribe on `socket.` and capture the message |
| 9 | Unsubscribe acknowledgements | Not awaited; topics are removed immediately | `tests/ingest/test_ws.py`: removal without an acknowledgement | `tests/massive/ws/test_client_integration.py`: record whether an acknowledgement arrives |
| 10 | Subscribe frame size limit | At most 1,000 topics per frame | `tests/ingest/test_ws.py`: 2,500 topics produce three frames | `tests/massive/ws/test_client_integration.py`: a 1,000-topic frame is acknowledged |
| 11 | Options cap counted in contracts or topics | Topics (the stricter count) | `tests/ingest/test_ws.py`: `T.X` plus `Q.X` counts as two | `tests/massive/ws/test_client_integration.py` (Options Advanced): 501 contracts on `T` and `Q` |
| 12 | Exact `max_connections` status text and timing | Any status `max_connections`, in the auth reply or after it, shows `WAITING_SLOT` | `tests/ingest/test_ws.py`: both placements | `tests/ingest/test_ws_integration.py`: two sessions with one individual key |
| 13 | `orjson` free-threaded wheels | Optional extra; standard-library codec by default | `tests/massive/net/test_codec.py`: codec selection with and without `orjson` importable | CI: the 3.14 job installs the extra (`--extra orjson`) and runs the suite with it; the 3.14t job does not, because `orjson` publishes no cp314t wheel |
| 14 | Flat-file publish time (documented "approximately 11:00 AM ET") | `flat_file_publish_cutoff` defaults to 11:30 ET | `tests/datasets/test_history_source.py`: AUTO before and after the cutoff | `tests/ingest/test_flatfiles_integration.py`: yesterday's key exists after the cutoff |
| 15 | Whether the server delivers an event twice when a wildcard and an explicit topic of the same channel are both subscribed (`T.*` plus `T.AAPL`) | While a channel's acknowledged wildcard is desired, its explicit topics are off the wire; transitions make before they break, accepting a brief duplicate ([section 13.3](#133-connection-and-subscription-protocol)) | `tests/ingest/test_ws.py`: adding, refusing, and removing a wildcard over explicit topics sends the expected frames in the expected order | `tests/massive/ws/test_client_integration.py`: subscribe both on a test key and record whether duplicates arrive |
| 16 | A missing flat-file key, requested through `files.massive.com`, answers HTTP 404 with S3's XML error and the code `NoSuchKey`, not 403 `AccessDenied` (S3 answers 403 for a missing key when the credentials lack `ListBucket`, which would turn weekends into `EntitlementError`), and not a 404 page the front end rewrites (a wrong path gets an HTML "Massive 404 Not Found" page, which botocore reads as the code `404`). If it answers anything but `NoSuchKey`, the library raises on every range that holds a weekend | Only a 404 `NoSuchKey` is a missing file; any other 404 raises, naming the bucket and the endpoint settings; a 403 is an entitlement failure (section 16) | `tests/massive/flatfiles/test_client.py`: `NoSuchKey`, other-code, and HTML 404 replies, and 403 replies | `tests/massive/flatfiles/test_client_integration.py` (S3 credentials): request a weekend key and record the status, the error code, and the content type |
| 17 | Retired: `poll()` results are no longer keyed `input_table`s but shapes of a publisher's blink table ([section 14](#14-publishers-fan-out-and-failure-semantics)), so no input-table attribute is involved | | | |
| 18 | Whether the Deephaven web UI shows a table's `TableDescription` | Set on every result anyway; it costs nothing and every API client can read it | `tests/tables/test_columns.py`: every result shape carries `TableDescription` | None: the implementer opens one result in the web UI of a local server and records in the pull request whether the description appears |
| 19 | Form of WebSocket forex `C` `x`: declared an integer, sampled as the text `"44"` | `quoted-integer` reads an integer or decimal-digit text; anything else fails the row loudly | `tests/massive/spec/test_convert.py`: both forms convert, `"4.8"` and `48.5` fail | `tests/massive/ws/test_client_integration.py`: one raw `C` frame; record the form of `x` |
| 20 | Form of WebSocket crypto `XT` `i`: declared and sampled as an integer, while REST and the flat file send text | `text-or-integer-id` keeps either form as text | `tests/massive/spec/test_convert.py`: both forms convert | `tests/massive/ws/test_client_integration.py`: one raw `XT` frame; record the form of `i` |
| 21 | Unit of WebSocket `LULD` `t` (documented ms, sample of 19 digits) | Registered as ns (unverified); the overflow check and the [1990, 2100] bound fail loudly on a mismatch | `tests/massive/refresh/test_overrides.py`: the unit is ns and unverified | `tests/namespaces/test_stocks_integration.py`: one `LULD` frame converts to a current `Instant` |
| 22 | Unit of flat-file `us_indices/values_v1` `timestamp` (documented ms, example rows of 19 digits) | Registered as ns (unverified); the bound fails loudly on a mismatch | `tests/massive/refresh/test_overrides.py`: the unit is ns and unverified | `tests/ingest/test_flatfiles_integration.py` (S3 credentials): one values file's first row converts to its day |
| 23 | Where the futures snapshot carries a contract's ticker and product code: the OpenAPI response schema nests both under `details` (`details.ticker`, `details.product_code`), while the OpenAPI response example and the documentation sample both send `ticker` and `product_code` at the top level of each result, with `details` holding only `settlement_date`; the path's `ticker.any_of` filter and its default sort `ticker` also name a top-level field | Two `KEY_FIXES` entries read the fields as `ticker` and `product_code`, columns `Ticker` (the `symbol` key, so `Symbol`) and `ProductCode`; were the wire to nest them as the schema says, every row would fail loudly as a missing required key and `details.ticker` and `details.product_code` would be reported as drift ([section 19.5](#195-runtime-drift)) | `tests/namespaces/test_futures.py`: the vendor's own sample, served unchanged, fills `Symbol` and `ProductCode` from the top level | `tests/namespaces/test_futures_integration.py`: one `snapshots` request for current contracts; record where `ticker` and `product_code` appear and that `Symbol` is filled |
| 24 | Key of the REST ticker events' event type (`/vX/reference/tickers/{id}/events`): the schema declares the required `event_type`, while the response example, the documentation sample, and Massive's Python client all use `type` | A `KEY_FIXES` entry reads the field as `type`, column `Type`; were the wire to send `event_type`, `Type` would be null and `event_type` would be reported as drift | `tests/massive/refresh/test_overrides.py`: the entry reads `event_type` as `type`, column `Type` | `tests/namespaces/test_reference_integration.py`: one `ticker_events` request; record which key the events carry |
| 25 | Whether rows of the stocks `filings.index`, `filings.sections_10k`, `filings.text_8k`, and `filings.risk_factors` always carry `ticker`: no filings response schema marks it required, and the index documents it as present only "if available" | Each of the four declares its symbol key `KeySpec("Ticker", required=False)`: a row without `ticker` keeps a null `Symbol` instead of failing the request, and none keys a `poll()` on `Symbol`, in any mode | `tests/ingest/test_rest.py`: a row without its optional symbol keeps a null `Symbol`; `tests/datasets/test_catalog.py`: only a symbol key from REST rows may be optional; `tests/namespaces/test_stocks.py`: a row without `ticker` of each of the four keeps a null `Symbol` | `tests/namespaces/test_stocks_integration.py`: one request over a month of each of the four; record how many rows lack `ticker` |
| 26 | Whether Massive reads a percent-encoded colon in a path segment (`/v2/aggs/ticker/I%3ASPX/prev`) as `I:SPX` | Every path value is percent-encoded whole; a value containing `/` is refused before sending, since Massive answers 404 for `%2F` | `tests/massive/rest/test_query.py`: `I:SPX` becomes `I%3ASPX`; a `/` raises | `tests/namespaces/test_indices_integration.py`: one `I:SPX` `/prev` call returns a row for `I:SPX` |
| 27 | Spelling of an explicit forex pair topic: the WebSocket documentation gives `{from}-{to}` (`C.EUR-USD`), while quote and aggregate frames spell the pair `EUR/USD` | Topics use `EUR-USD`; frames of any spelling map back to `C:EURUSD` | `tests/datasets/test_symbols.py`, `tests/ingest/test_ws.py`: `C.EUR-USD` is subscribed and an `EUR/USD` frame reaches its stream | `tests/namespaces/test_forex_integration.py`: subscribe `C.EUR-USD` and `CA.EUR-USD`; record the acknowledgement and that frames arrive |
| 28 | Ticker form in the forex quotes flat files: the vendor sample shows `X:EUR-USD`, while REST uses `C:EURUSD` | The reader assumes the canonical form: symbol pushdown compares the file's `ticker` column with canonical symbols, and `Symbol` holds the column as written. Were the files to write `X:EUR-USD`, a symbol request would match no row; that outcome becomes an owner decision | `tests/ingest/test_flatfiles.py`: symbol pushdown keeps the rows whose ticker equals a requested canonical symbol | `tests/ingest/test_flatfiles_integration.py` (S3 credentials): read one forex quotes file and record the ticker form |
| 29 | Futures holiday sessions and candle snapping: whether a session after a holiday (2025-01-21 after Martin Luther King Day) opens at the previous regular open or earlier, whether 4 days of margin cover every holiday run of bars named by date, whether 7 days cover every flat-file roll-forward, and whether `window_start` filters on session, week, month, and year candles snap to the candle's start | Trades and quotes named by date are requested by `session_end_date`, which needs no margin; bars named by date are requested from the session's regular open less 4 days and post-filtered on `SessionEndDate`; bars named by instants are filtered locally on their own window; an instant range whose trailing missing files hold a weekday rolls forward up to 7 days and raises `MassiveError` if the next file is unpublished or absent | `tests/ingest/test_rest.py`, `tests/ingest/test_flatfiles.py`: the 2025-01-21 session's Sunday-evening rows and Good Friday 2024's rows are kept, and an unpublished or absent next file raises | `tests/namespaces/test_futures_integration.py`: minute bars of ES around the 2026-01-20 session, and `YEAR` candles from a year start and from mid-year; record the first bar's time and whether the candles arrive |
| 30 | Whether `/v3/reference/tickers` returns one row per ticker with `active=false`, when delisted listings may reuse a ticker | `reference.tickers` polls keyed by `Symbol`; were several rows to share a ticker, a `poll()` table would keep one of them while `fetch()` shows all | `tests/namespaces/test_reference.py`: the vendor sample decodes with one row per ticker | `tests/namespaces/test_reference_integration.py`: one `tickers(active=False)` request over a letter range; record any ticker on several rows |
| 31 | Whether `(asset_class, id)` identifies one row of `/v3/reference/exchanges` | `reference.exchanges` polls keyed by `AssetClass` and `Id` | `tests/namespaces/test_reference.py`: the poll keys are columns of the schema | `tests/namespaces/test_reference_integration.py`: one `exchanges()` request; record any repeated key |
| 32 | Whether `(asset_class, type, id)` identifies one row of `/v3/reference/conditions` | `reference.conditions` polls keyed by `AssetClass`, `Type`, and `Id` | `tests/namespaces/test_reference.py`: the poll keys are columns of the schema | `tests/namespaces/test_reference_integration.py`: one `conditions()` request; record any repeated key |
| 33 | The day a week bar starts on, for stocks, forex, and crypto: Monday, or the Sunday before (forex trades from Sunday evening UTC) | Stock weeks run Monday to Friday, and a bar stamped on the Saturday or Sunday before a week names that week; the REST `{from}` is that Saturday. Forex and crypto weeks are calendar weeks, Monday to Sunday; were the forex bucket to start on Sunday, its week would become the Sunday-to-Friday trading week | `tests/datasets/test_dates.py`, `tests/ingest/test_rest.py`: Sunday- and Monday-stamped stock week bars are kept for their week; forex and crypto weeks are calendar weeks | `tests/namespaces/test_stocks_integration.py`, `test_forex_integration.py`, `test_crypto_integration.py`: two weeks of week bars each; record each `Timestamp` and the first and last trade each bar holds |
| 34 | Whether the aggregates range returns a partial bar for a period `{from}` falls inside | `{from}` is the first instant that belongs to the first bar that fits wholly (the Saturday before a stock week, the 1st of a month), so a partial bucket begun before it is dropped whatever it is stamped | `tests/ingest/test_rest.py`: the week and month requests start where the first whole period begins | `tests/namespaces/test_stocks_integration.py`: month bars with `{from}` in mid-month; record the first bar's stamp and volume against the whole month's |
| 35 | How bars of `multiplier` above 1 are aligned: to clock and calendar boundaries, or to `{from}` | Aligned to boundaries (whole UTC units for intraday bars, period starts for longer ones); a bar spans `multiplier` units from its own start | `tests/datasets/test_dates.py`: five-minute and two-week bars fit by their full span | `tests/namespaces/test_stocks_integration.py`: five-minute and two-week bars from an odd `{from}`; record their stamps |
| 36 | Whether a futures session can end on a Saturday or Sunday | Sessions are requested on weekdays only; Saturday and Sunday dates are skipped without a request | `tests/ingest/test_rest.py`: a range over a weekend requests only its weekday sessions | `tests/namespaces/test_futures_integration.py`: `schedules` over a month; record every `session_end_date`'s weekday |
| 37 | The stamp of forex 1-day bars on every source: the range (00:00 UTC in the samples), grouped daily, `/prev`, and `global_forex/day_aggs_v1` | Every stamp is moved to 00:00 UTC of its UTC day, unchecked | `tests/datasets/test_timestamps.py`, `tests/ingest/test_rest.py`: range and grouped stamps map to 00:00 UTC | `tests/namespaces/test_forex_integration.py`: one 1-day bar of `C:EURUSD` from each source for one day; record each `Timestamp` and whether the OHLCV agree |
| 38 | Whether crypto range and `/prev` 1-day bars cover the UTC day: compare them with the same day rebuilt from 1-minute bars | Item 1's loud check: a stamp elsewhere than a UTC day boundary fails | `tests/namespaces/test_crypto.py`: as item 1 | `tests/namespaces/test_crypto_integration.py`: rebuild one day of `X:BTCUSD` from `minute_aggs_v1` over the UTC day and over the New York day; record which matches the range and `/prev` bars |
| 39 | Whether grouped daily and `day_aggs_v1` hold the same pairs on a day, for forex and crypto | `AUTO` keeps one source for crypto 1-day bars; nothing assumes the sets match | `tests/datasets/test_history_source.py`: crypto 1-day bars keep one source whatever the symbol count | `tests/namespaces/test_forex_integration.py`, `tests/namespaces/test_crypto_integration.py`: one day of `dm.ALL` 1-day bars from both sources; record the pairs only one holds |
| 40 | Which session a futures bar of several sessions or longer is labelled with (`SessionEndDate` of its first or last session), and how such bars are aligned | Futures bars of a session or longer with `multiplier` above 1 named by dates raise `ValueError` ("pass datetimes, or multiplier=1"); named by datetimes, they are kept by their own window | `tests/datasets/test_history_source.py`, `tests/namespaces/test_futures.py`: the refusal for 2-session, 2-week, and 2-month bars | `tests/namespaces/test_futures_integration.py`: 2-session and 2-month bars of ES over a quarter; record each bar's `Timestamp` and `SessionEndDate` |
| 41 | Whether Massive keeps an IPO's `issuer_name` unchanged while the offering moves through its statuses (rumor or pending, new, history, postponed, withdrawn); the vendor documents it only as "Name of issuer" | `stocks.ipos` upserts by `Symbol` and `IssuerName`, so two issuers of one ticker stay apart; were Massive to revise a name, the row under the old name would stay beside the new one, since `UPSERT` never deletes | `tests/namespaces/test_stocks.py`: two issuers of one ticker poll as two rows | `tests/namespaces/test_stocks_integration.py`: a year of IPOs; record every ticker whose rows carry more than one `issuer_name` |
| 42 | Whether `IssuerCik` and `TransactionDate` identify one row of the insider activity, and `IssuerCik` and `OwnerCik` one row of the insider holdings; the vendor documents one row per issuer and transaction date, and one per reporting owner per issuer, but marks none of the four fields required | Both datasets replace their `poll()` table keyed by those columns; a null key fails the table, and two rows sharing a key would keep only one | `tests/namespaces/test_stocks.py`: each key column is needed to tell the fixture's rows apart | `tests/namespaces/test_stocks_integration.py`: a year of NVDA's activity and NVDA's holdings; record and refuse any key shared by two rows |
