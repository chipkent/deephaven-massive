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
   column order and one type per column for each dataset kind, merged at run time from the
   generated tables ([section 19.4](#194-generated-schemas)): the union of the fields Massive
   documents for the kind's sources across asset classes and transports. A dataset keeps only
   the kind's columns that some source of its own fills, on any transport and for any
   arguments, in the kind's order (crypto trades have no `Tape`; stock bars have no
   `SettlementPrice`), so the columns two datasets of a kind share have one name and one type
   and their tables merge on them. A column only another transport of the same dataset fills
   stays, and is null in a table the other transport serves, so a history table and a live
   table of one dataset have the same columns. Column names come from the generated tables
   (the lexicons and `NAMES`) and from each dataset's declared key columns, never from a field
   description. Every tick, bar, and value row says when it
   happened (a tick's `Timestamp`, a bar's `TimestampStart` and exclusive `TimestampEnd`), its
   trading day (`Date`, ISO `yyyy-MM-dd` text), and when the library received it
   (`TimestampInsert`, the last column of every dataset;
   [section 12](#12-timestamp-units-and-day-boundaries)). Every calendar date in every table
   is ISO text, never a `LocalDate`. `Symbol` is the instrument on every instrument row.
2. **Shape is the user's choice; transport and host are the library's.** Verbs name the
   Deephaven table shape. No feed, host, channel, or S3 prefix appears in user code.
3. **Tier is a property of the key.** Identical code runs on every plan. The library learns
   recency (real-time or delayed) from REST responses, never by opening a connection it intends
   to abandon. A dataset the plan lacks fails loudly and names the alternative where the
   library has one; the library never models plans, so otherwise the refusal states the
   subscription, the transport, the request, and Massive's text.
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
   `TablePublisher.publish_failure`. Static verbs raise. Both appear in the diagnostics
   tables, and each is logged at the level the user must act on
   ([section 17.1](#171-the-log)): ERROR only for a defect, naming whose it is.
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
    `Symbol`, `Timestamp`, and `Underlying`, the library's time columns `Date`,
    `TimestampStart`, `TimestampEnd`, and `TimestampInsert`, the derived `OptionRoot`, and a
    dataset's note on a column whose meaning depends on the query,
    such as the split adjustment of a bar's prices). The same text reaches users three ways: the column-header tooltip of every result
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
| column description | The plain-text meaning of one dataset column: the description of the wire field behind it (the vendor's, or the refresh's correction where the vendor's is clearly wrong), or fixed library text for a key column or a column the library computes ([section 19.4](#194-generated-schemas)). |
| dataset reference | The generated, committed pages under `docs/datasets/`: an index, `README.md`, and one page per dataset, `<dataset>.md` (`stocks.trades.md`, `snapshots.md`), with its call, verbs, filters, Massive's plan tables, and columns ([section 7.4](#74-column-descriptions)). |
| UG thread | A thread of Deephaven's update graph (UG), which refreshes live tables while holding the UG lock. |
| blink table | A Deephaven table whose rows exist for exactly one update cycle. `TablePublisher` produces one. |
| result scope | A library-owned `LivenessScope` that holds exactly one result. |
| stream scope | A library-owned `LivenessScope` that holds one stream's blink table and publisher. |
| registry lock | `SessionCore.registry_lock`, the one lock that guards every session-level registry. |

## 3. Exports

`deephaven_massive/__init__.py` exports exactly these 42 names and nothing else:

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
                           # and .cause, an ErrorCause

# Sentinel
dm.ALL                     # every symbol on the stream, in the flat file, or in the snapshot

# The dataset list
dm.datasets                # () -> Table: one static row per dataset (section 7.7); no session
                           # or API key needed

# Symbol converters: other spellings to Massive's exact REST ticker (section 10)
dm.canonical_symbol              # (text, asset_class, *,
                                 #  convention=SymbolConvention.MASSIVE) -> str | None
dm.canonical_symbol_stock        # (text) -> str | None, one per class, for query strings
dm.canonical_symbol_option
dm.canonical_symbol_future        # a single futures contract
dm.canonical_symbol_future_spread # a futures spread, as Massive writes it
dm.canonical_symbol_index
dm.canonical_symbol_forex
dm.canonical_symbol_crypto
dm.canonical_symbol_stock_yahoo  # (text) -> str | None, one per stock convention
dm.canonical_symbol_stock_nasdaq
dm.canonical_symbol_stock_cms
dm.canonicalize_symbols          # (table, asset_class, *, column="Symbol",
                                 #  convention=SymbolConvention.MASSIVE,
                                 #  on_error=OnError.RAISE) -> Table

# Enums (all StrEnum; values are Massive's wire strings where the enum reaches the wire)
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
dm.SymbolConvention        # the stock conventions the converters read
dm.OnError                 # RAISE, LOG, IGNORE: what a bad symbol does, in canonicalize_symbols
                           # and in the two symbol settings of MassiveConfig
dm.Subscription            # Massive's five subscriptions; values are their names, not wire strings
dm.ErrorCause              # whose problem a MassiveError is: EXPECTED, USER_DEFECT, LIBRARY_DEFECT

# Logging
dm.enable_debug_logging    # enable_debug_logging(output=LogOutput.STDERR): the library's log,
                           # DEBUG and up, to stderr or a file path (section 17.1)
dm.LogOutput               # Enum, not a wire value: STDERR, the default output
dm.disable_debug_logging   # disable_debug_logging(): turns off what either switch turned on

# Version
dm.__version__             # from importlib.metadata
```

The four query classes (`Query`, `StreamQuery`, `HistoryQuery`, `StreamHistoryQuery`) and the
type aliases `Symbols`, `DateLike`, and `FilterValue` are importable from the public module
`deephaven_massive.queries`, for type annotations; users never construct a query. The package's
`__init__.py` never imports it, so `import deephaven_massive` stays cheap, and it is not one of
the names above: annotating a helper of one's own should not need a private import. Every query also has `columns()`
([section 7](#7-query-classes-and-verbs)), a method rather than an export, so the list above
does not change.

`MassiveError` and its two subclasses carry two read-only attributes, set when the error came
from an HTTP response and `None` otherwise: `status`, the HTTP status (`int | None`), and
`request_id`, the `request_id` of Massive's response (`str | None`). Users branch on `status`
(for example 404 or 429) instead of parsing messages. Every `MassiveError` also has `cause`, an
`ErrorCause` that says whose problem it is and sets the level it is logged at
([section 17.1](#171-the-log)): `EXPECTED` (a refusal, a plan or rate limit, a network failure, a
closed session; the default), `USER_DEFECT` (the user's code or input, such as a symbol table
that failed upstream; the message names what to fix), or `LIBRARY_DEFECT` (a bug in the library,
or vendor data it cannot read, which a wire error marks `unreadable`; the message asks for a bug
report). `ErrorCause` is a `StrEnum`
whose values are no wire strings.

`__init__.py` resolves these names lazily, through a module `__getattr__` (PEP 562), with the
eager imports under `TYPE_CHECKING` mirroring `__all__`. Importing
`deephaven_massive._massive` therefore never imports Deephaven.

A test asserts that `dm.__all__` equals this list, that `getattr(dm, name)` resolves every name
in it through the lazy `__getattr__`, and that no public signature in the package
contains a `Literal` of strings ([section 11](#11-enums-and-types)).

## 4. Package layout

### 4.1 Source modules

Every module below `deephaven_massive/` other than `__init__.py` and `queries.py` is private.
The rule: the first path component below the package has a leading underscore. `queries.py`,
the query types for annotations ([section 3](#3-exports)), is the one public module beside the
package itself; a new one needs owner approval. A module inside a private
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

The tree below is the target layout: the modules as they will be once the open feature PRs and
the package layout cleanup, a series of refactoring PRs, have all merged. Until then, some
modules named here do not exist yet, and some of today's modules do not appear here. The
cleanup plan, `LAYOUT.md`, gives each target module's source today: its ← marks name the
module each new or renamed one comes from.

```
src/deephaven_massive/
__init__.py              public names of section 3, lazy (PEP 562); TYPE_CHECKING imports mirror __all__
_config.py               MassiveConfig (section 5), HostMap, the environment readers;
                         require_positive_int and require_positive_number, the two argument
                         checks used across layers
_enums.py                the sixteen public enums, Recency, ChannelFamily, ClassFromPrefix, NotBars,
                         _require_enum, subscription_of
_types.py                Symbols, ALL/_All, DateLike, FilterValue, bars overload aliases, override
_errors.py               MassiveError (.status, .request_id, .cause), ErrorCause, AuthenticationError,
                         EntitlementError, from_wire, the library-bug and user-defect texts; RowDataError and UnsupportedParameterError, which the ingest
                         layer raises and the tables layer names
_log_limits.py           rate-limited log records: log_once, BoundedLog, log_failure (once per
                         failure, at the level its cause gives)
_debug_log.py            enable_debug_logging, disable_debug_logging, LogOutput, the
                         DEEPHAVEN_MASSIVE_LOG variables, and
                         the environment line; no Deephaven import, so it works before a server
                         exists (section 17.1)
_session.py              MassiveSession: builds the catalog, the core, and the session's clients
                         (section 6); applies the logging variables and logs the environment
                         line; NAMESPACE_MODULES and CATALOG (section 4.3)
_query.py                the four query classes and their verbs, through the core's ports
                         (section 7); query_class and bar_query_class, which choose a dataset's
                         query class; verbs(), the verbs a dataset serves
_dataset_list.py         dm.datasets(): the static table of every dataset in CATALOG (section 7.7)
queries.py               public: the four query classes and Symbols, DateLike, FilterValue, for
                         type annotations (section 3)
_namespaces/
  __init__.py            docstring only
  query_building.py      Namespace, the shared dataset factory (build_query), list_query,
                         ticker_key, argument normalization
  stocks.py              m.stocks
  stock_financials.py    m.stocks.financials
  stock_filings.py       m.stocks.filings: SEC filings and insiders
  options.py futures.py indices.py forex.py crypto.py
  reference.py indicators.py economy.py                   namespace class + DATASETS
  partners.py            m.partners and its four partner sub-namespaces
  snapshots.py           the DatasetSpec of m.snapshots(), the unified snapshot the session
                         serves itself, and its DATASETS
_datasets/               dataset semantics: no network I/O, no threads, no Deephaven calls;
                         only column_reference.sync writes files
  __init__.py            docstring only
  catalog.py             DatasetSpec (summary, role paths and fetch paths by symbol class,
                         kind, per-source key columns, channels, prefixes, poll defaults,
                         page size, symbols per request), KeySpec, SourceKeys, Catalog (the
                         specs by name, members(kind))
  filters.py             the filters= rules: filter validation (section 9)
  request.py             Request, ParamValue, StreamKey
  schema.py              ColumnType, Column, Schema, WireType -> ColumnType, dataset_schema(): the
                         merge of a dataset's sources (section 19.4)
  source_columns.py      SourceColumn: the key rename, required fields, widen
  column_descriptions.py the description rules of dataset columns, the transport labels and
                         their order (section 7.4)
  column_reference.py    the dataset reference: render(catalog, provenance, calls) -> {file
                         name: Markdown} (pure); sync(directory, pages) owns docs/datasets/
                         (section 7.4)
  symbols.py             the canonical ticker grammar and its checks (check_symbol(),
                         normalize()), the public scalar converters, option_roots(),
                         symbol_class() (section 10)
  wire_symbols.py        a symbol's transport spellings, both ways: canonical_from_wire(),
                         wire_spellings(), ws_symbol(), WireSymbols, ReadSymbols,
                         unreadable_symbol_message() (section 10)
  dates.py               DateLike parsing, [start, end) ranges and bounds, day zones, the
                         trading-day test
  futures_sessions.py    the 17:00 Chicago futures session rule, scalar and Arrow; the weekday
                         roll
  trading_hours.py       when each asset class's live feed is expected to send data (regular
                         hours, no holiday calendar), for the no-frames WARNING
  bar_periods.py         intraday spans, bar periods, bar starts, days and sessions
  timestamps.py          the one epoch-unit routine (section 12), the FMV magnitude rule
  epoch.py               the epoch, a day in nanoseconds, and the conversions between aware
                         datetimes and epoch nanoseconds
  row_times.py           the library's time columns: Date, TimestampStart, TimestampEnd,
                         TimestampInsert; RowTimes, TimeRule, and the bar-stamp rule (section 12)
  history_source.py      the history window, partition dates, the current partition, REST
                         capability, the AUTO choice (section 16)
_tables/                 the Deephaven side: tables, liveness, library threads
  __init__.py            docstring only
  session_core.py        SessionCore, registry_lock, the registries, closers, stream ids, the
                         live set, the worker-table registry, diagnostics_view, attach, close()
                         (sections 4.3, 6)
  transports.py          the transport ports (RestTransport, StreamTransport,
                         FlatFileTransport), IngestPorts, StreamHandle, HostDecision
  execution_context.py   entered(), the execution-context and update-graph probes
  liveness.py            result and stream scopes, ResultRegistry, records, Reaper,
                         QueueWorker (section 6.1)
  diagnostics.py         connections(), streams() with SchemaDrift, and rate_limits();
                         DriftRegistry, pascal_case (section 17)
  columns.py             ColumnType -> Deephaven type, column definitions; describe(),
                         column_table() (section 7.4)
  publisher.py           blink_publisher(), BlinkPublisher: the publisher of every blink
                         table, whose failure pins the table until it is delivered (section 6.1)
  background_read.py     wait=False: BackgroundRead, open_read, the merged partitioned table a
                         read's static result lands in as one constituent (section 6.1)
  stream.py              streams, flush thread, fan-out, bound-failure routing (section 14)
  poll.py                poll(): Poller, Pollers, open_poll; REPLACE/UPSERT/APPEND, fan-out, data
                         failures -> publish_failure; SymbolRowError
  time_text.py           deephaven_time_text(): Deephaven's own date and time text, read by
                         Deephaven's parser for the date arguments (section 11)
  watchlist.py           read_symbols(), SymbolWatch: the check and watch of symbol tables
                         (section 10)
  symbols.py             canonicalize_symbols(), the table form of the symbol converters
                         (section 10)
_ingest/                 the only holder of wire clients
  __init__.py            docstring only
  clients.py             SessionClients: the session's wire clients and connections, the ports
  rest.py                REST transport: RestState, RestReader, RestEndpoints; 403/429 policy,
                         drift routing, each request's subscription, per-read cancellation;
                         UnlistedPairError
  rest_requests.py       the requests of a call, from a DatasetSpec: Call, plan_calls(),
                         fetch_paths(), the history calls
  rest_rows.py           page rows to dataset batches, renamed and converted; error rows; the
                         history window's keep
  host_decision.py       host resolution from REST evidence (section 13.2): the host rule, its
                         probes and constants, HostDecisionCache (single-flight)
  rate_limits.py         the process's REST governors per (API key digest, REST origin),
                         session leases, the first-429 INFO notice, rate_limits() rows, the INFO
                         download notice, the poll() pace WARNING (section 13.4)
  flatfiles.py           flat-file transport: read plans, reader pool, cancellation, conversion,
                         merge to tables, missing-file policy, drift routing (section 16)
  futures_calendar.py    the futures schedule calendar that dates streamed futures rows by
                         their trading session (section 12)
  subscriptions.py       topics and the reconcile loop of one connection: wildcards,
                         watchdogs, the NO_ACK rule, the options cap's admission (section 13.3)
  ws.py                  WsConnections and WsTransport: the session's connections over
                         WsClient, leases, the verdict rule, rejects and drift routing, the
                         options cap's verb check (section 13.3)
  ws_connection.py       Connection and its client factories; ConnectionGroup, which shares
                         connections between sessions
_massive/                the wire package (section 19)
py.typed
```

The root holds the vocabulary every layer names (`_config`, `_enums`, `_errors`, `_log_limits`,
`_types`) and the API users touch (`__init__`, `_session`, `_query`, `_namespaces/`). The rest of
the library is grouped into three packages: `_datasets/` describes datasets (and renders their
column reference from a catalog it is given), `_tables/` is the Deephaven side, and `_ingest/` is
the only code that holds wire clients. The `__init__.py` of each of the three, and of
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
| `uv run python scripts/render_reference.py` | Regenerates the dataset reference, `docs/datasets/`, the whole directory and nothing else ([section 7.4](#74-column-descriptions)). |
| `docs/datasets/README.md`, `docs/datasets/<dataset>.md` | The generated dataset reference, committed, never edited by hand. |
| `vendor/massive/` | The vendored spec snapshot the refresh reads ([section 19](#19-wire-clients-and-the-spec-snapshot)). It is Massive's published content, vendored as generation input: not covered by the repository's license, and in neither the wheel nor the sdist. `vendor/README.md` says so beside it. |
| `.github/workflows/spec-drift.yml` | The advisory weekly drift job ([section 20](#20-quality-gates)). |
| `.github/workflows/release.yml` | Publishes to PyPI from a `v*` tag; created only when the owner turns publishing on ([section 20.1](#201-tooling-configuration)). |
| `tests/data/` | Payload fixtures. |
| `tests/_fakes/` | Fake REST server, WebSocket server, S3, clock, and stream helpers, and the shared streaming session harness ([section 4.2](#42-test-layout)). |
| `tests/_live/` | The logic behind the live tests' fixtures: the plans' coverage and history windows, the plan probe, the exchange calendar, the futures front contract, the choice of recent corporate actions, the delayed-feed timing, the week-window rule, and the open-questions summary ([section 4.2](#42-test-layout)). |
| `tests/_live/live_run/` | The live-run program, `scripts/live_run.py`'s logic ([section 4.2](#42-test-layout)). |

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
with `_`. Examples, from the target layout of [section 4.1](#41-source-modules):

| Source | Unit test | Live integration test |
|---|---|---|
| `__init__.py` | `tests/test_init.py` | |
| `_config.py` | `tests/test_config.py` | |
| `_datasets/__init__.py` | `tests/datasets/test_init.py` | |
| `_datasets/catalog.py` | `tests/datasets/test_catalog.py` | |
| `_tables/session_core.py` | `tests/tables/test_session_core.py` | |
| `_ingest/host_decision.py` | `tests/ingest/test_host_decision.py` | `tests/ingest/test_host_decision_integration.py` |
| `_ingest/flatfiles.py` | `tests/ingest/test_flatfiles.py` | `tests/ingest/test_flatfiles_integration.py` |
| `_datasets/column_reference.py` | `tests/datasets/test_column_reference.py` (also the parity test of `docs/datasets/`) | |
| `_namespaces/__init__.py` | `tests/namespaces/test_init.py` | |
| `_namespaces/stocks.py` | `tests/namespaces/test_stocks.py` | `tests/namespaces/test_stocks_integration.py` |
| `_massive/__init__.py` | `tests/massive/test_init.py` | |
| `_massive/rest/client.py` | `tests/massive/rest/test_client.py` | |
| `_massive/refresh/__main__.py` | `tests/massive/refresh/test_main.py` | |
| `_massive/refresh/generate.py` | `tests/massive/refresh/test_generate.py` (also the parity test) | |
| `_massive/refresh/rest_sources.py` | `tests/massive/refresh/test_rest_sources.py` | |

**Import roots.** `[tool.pytest] pythonpath = ["."]` and `[tool.mypy] mypy_path = ["src"]`;
test code imports helpers as `tests._fakes…`. So `tests/massive/` can never shadow a top-level
`massive` package.

Integration tests end in `_integration` and carry the `integration` marker, which `addopts`
deselects in every default run (CI's included); `uv run pytest -m integration` runs them, and
each skips unless `MASSIVE_API_KEY` is set (and always under CI). The same expression
deselects the `benchmark` marker, which the extra points of the stream fan-out benchmark carry:
that test records rates and never asserts, so the default run keeps only its default point,
and `-m benchmark` runs the other eight. `-m` applies to a node id too, so a single extra
point runs with its node id and `-m benchmark`. It deselects the `soak` marker too, which the
soak tests carry: long stress runs against the fakes (a fast fake feed, many streams, and
churn), each lasting `MASSIVE_SOAK_SECONDS` (20 by default), that fail on a lost or repeated
row, a hang, or memory or threads that keep growing; `-m soak` runs them, and
[`docs/dev/soak-tests.md`](../../docs/dev/soak-tests.md) is their runbook, overnight runs
included. No test pins the `addopts` string or the
marker list: pytest's `strict` setting already rejects an unregistered marker.
[`docs/live-tests.md`](../../docs/live-tests.md) is the runbook for them. The live tests that deliberately exceed Massive's subscription and connection
limits also carry `integration_limits`, and the smoke tier (every dataset and verb once,
checked automatically) carries `integration_smoke`: `-m integration` deselects both, and only
`-m integration_limits` or `-m integration_smoke` runs them. The first live test of a run
probes the key's plans (cached for the New York day in the git-ignored `.live-plans.json`;
`MASSIVE_LIVE_PLANS` overrides the probe), and each test's `needs` marker skips it when they
fall short. The exceptions to the mirror rule, and no
others:

- `tests/conftest.py`: starts the embedded JVM in `pytest_configure`, before collection, and
  provides fixtures: thin fixtures that build the fakes from `tests/_fakes/`, plus the
  table-to-Python and fixture-loading helpers. Test modules never import from it. An autouse
  fixture gives every unit test a registry of REST rate governors of its own on a fake clock,
  so no test inherits another's pauses or learned limits and none sleeps for them; the live
  tests share one real-clock registry for the run instead.
- `tests/test_layout.py`: walks `src/deephaven_massive/**/*.py` and asserts every module has its
  mirrored test file and that every test file maps to a module or is one of these exceptions.
  It also holds the import-boundary and layer tests of `_massive`
  ([section 19](#19-wire-clients-and-the-spec-snapshot)) and the library's layer rules
  ([section 4.3](#43-layers)), with a negative test per rule: `tests/massive/` may import any
  `_massive` module, and the rest of `tests/` only the interface.
- `tests/test_docs.py`: the user documentation's tests. It runs every Python block of
  `README.md`, in order, in the embedded JVM against the fakes (and the first-tables and split
  adjustment sections alone against scripted data), checks that every `README.md#<anchor>` and
  `docs/datasets/<dataset>.md` a source file cites exists and that every link inside the README
  resolves, and checks the README's namespace table against the catalog. It mirrors
  `README.md`, not a source module.
- `_` helper directories, of which there are two, `HELPER_PACKAGES` in `tests/test_layout.py`
  (a new one needs owner approval). The second, `tests/_live/` (`plans.py`, `probe.py`,
  `classes.py`, `calendar.py`, `contracts.py`, `events.py`, `streams.py`, `week_windows.py`,
  `summary.py`), holds the logic behind the live tests' fixtures: the Massive plans, what each
  covers and keeps, the probe that finds a key's plans and its cache, which class a request
  reads, the exchange calendar, the front futures contract, the recent splits and dividends to
  check, when a delayed stream must deliver rows, the week-window rule, and the open-questions
  summary. `tests/conftest.py` turns it into fixtures. Its subpackage `tests/_live/live_run/` holds the live-run
  program (`cli.py`, `commands.py`, and the modules they name), tested under
  `tests/live/live_run/`, which the tester runs with
  their own keys through `scripts/live_run.py`, a thin entry point: it runs every live check the
  key allows on a fixed timetable, records Massive's raw replies through the reply-capture hook,
  streams a trading day, and packs the results after a scan for the secrets. It runs against Massive only in a live
  run, so each module has a unit test against the fakes at the mirrored path
  (`tests/live/test_plans.py`), and coverage measures it with the library. Only this helper
  package has mirrored tests (`TESTED_HELPER_PACKAGES`). The first is
  `tests/_fakes/` (`__init__.py`, `rest.py`,
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
| 5 | API | the root `__init__`, `_dataset_list`, `_debug_log`, `_session`, `_query`, the public `queries`, and `_namespaces/` | What users touch. `_session.py` is the composition root. `_debug_log` imports no Deephaven. |

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
  pure functions `encode_value` and `origin_of`; a subprocess test checks that reading every
  one of them loads no client and no network module) and
  client names (`RestClient`, `Page`, `to_batch`, `RequestGovernor`, `BucketState`, `Patience`,
  `BASIC_REQUESTS_PER_MINUTE`, `RATE_WINDOW`, `Cancel`,
  `set_reply_capture`, `CapturedReply`,
  `WsClient`, `WsState`, `Status`, `StatusKind`, `Reject`, `FlatFileClient`, `FlatFile`,
  `ReadPlan`, `ReadColumn`, `object_key`); under `src/`, only `_ingest` uses a client name (by import, attribute,
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
| `api_key: str` | `MASSIVE_API_KEY` | Required. Absent or blank raises `AuthenticationError` at construction; a key with surrounding whitespace raises `ValueError` (the message never shows the key). No other variable is read: `POLYGON_API_KEY`, the vendor's former name, is ignored. |
| `s3_access_key_id: str \| None` | `MASSIVE_S3_ACCESS_KEY_ID` | Enables flat-file history. |
| `s3_secret_access_key: str \| None` | `MASSIVE_S3_SECRET_ACCESS_KEY` | The two S3 credentials are both set or both absent; exactly one raises `ValueError`, and so does a credential passed as blank text or with surrounding whitespace (None means absent). |
| `ws_hosts: Mapping[AssetClass, str]` | `{}` | Pins the hostname of the TICK family for a class, for example `{AssetClass.STOCKS: "fullmarket-business.massive.com"}`. A bare lower-case hostname, taken exactly as written: a scheme, port, path, upper-case letter, or surrounding whitespace raises `ValueError`. The FMV family is never pinned ([section 13](#13-host-resolution-and-connection-protocol)). |
| `ack_timeout: float` | `5.0` seconds | Subscribe-acknowledgement watchdog, per topic. |
| `reconnect_backoff: float` | `30.0` seconds | The first wait before the WebSocket client tries again after any failure that is not fatal: a connection that opened and closed (cleanly or not, including a `max_connections` refusal), a TCP or TLS failure, a handshake or authentication timeout, or a retryable HTTP status during the upgrade (500, 502, 503, 504). Later waits double up to `max_reconnect_backoff` ([section 19.2](#192-the-websocket-client)). Must be at least 15 s, else `ValueError` naming Massive's guidance: a shorter wait can be refused for holding the allowance the dropped connection still holds, and on an individual plan that wastes the one slot there is. Tests never need a shorter value: they drive `WsClient` directly, with its backoff as constructor arguments and an injected clock. The default waits out the window Massive's knowledge base describes: the server takes "roughly 10 to 30 seconds" to release a dropped connection, and an earlier retry "can be refused for holding the allowance it just lost", which wastes the one slot an individual plan has. |
| `max_reconnect_backoff: float` | `120.0` seconds | Cap of the doubling backoff, so a connection comes back within this long of a long outage ending. Must not be below `reconnect_backoff`. Jitter only adds up to 20% and is clamped, so every wait lies in `[reconnect_backoff, max_reconnect_backoff]` ([section 19.2](#192-the-websocket-client)). |
| `flat_file_symbol_threshold: int` | `50` | The `HistorySource.AUTO` switch point when both sources are possible. |
| `max_concurrency: int` | `8` | REST fan-out and S3 reads. |
| `rest_basic_subscriptions: AbstractSet[Subscription]` | `frozenset()` | The subscriptions the key's account holds on Massive's free Basic tier, whose REST requests are paced at 5 per minute from the first one ([section 13.4](#134-rest)). Stored as a `frozenset`. Anything but a set of `Subscription` raises `TypeError`; forex or crypto, as text or as an `AssetClass`, raises `TypeError` naming `Subscription.CURRENCIES`, the one subscription of both. |
| `flush_interval: float` | `0.05` seconds | WebSocket micro-batch flush into `TablePublisher.add`. |
| `rest_base_url: str` | `https://api.massive.com` | Rarely changed. |
| `s3_endpoint_url: str` | `https://files.massive.com` | Rarely changed. |
| `s3_bucket: str` | `flatfiles` | Rarely changed. |
| `noncanonical_symbols: OnError` | `DEEPHAVEN_MASSIVE_NONCANONICAL_SYMBOLS` (`raise`, `log`, or `ignore`, any case; other text raises `ValueError`), then `LOG` | What a row of a symbol table that is not Massive's exact ticker does, for every verb and every row, present at the call or added later ([section 10](#10-symbols-and-all)). `RAISE` raises `ValueError` for such a row present at the call and fails the result for one added later. `LOG` skips the row and logs each distinct value once at WARNING (at most 100 per process); `IGNORE` skips it with nothing logged above DEBUG. Under both, a stream's or poll's `m.streams()` row lists the skipped values in `Error` (their count and the first five), and a table whose rows are all bad raises `ValueError` rather than start an empty result. A `str` or a sequence passed as `symbols` in code always raises, whatever the setting. |
| `unreadable_symbols: OnError` | `DEEPHAVEN_MASSIVE_UNREADABLE_SYMBOLS` (`raise`, `log`, or `ignore`, any case; other text raises `ValueError`), then `LOG` | What a symbol Massive sends in a spelling the library cannot read does ([section 10](#10-symbols-and-all)). `RAISE` fails the request or stream that received it, naming the spelling, this setting, and where to report the gap. `LOG` keeps the row with a null `Symbol` and the text in the `RawSymbol` column, and logs each spelling once at ERROR with the library-bug text (at most 100 per process): the library's grammar lacks a spelling Massive uses, which is a library defect, yet every table keeps running. `IGNORE` keeps the row the same way with nothing logged above DEBUG. Every dataset that reads its symbol from rows has `RawSymbol` under `LOG` and `IGNORE`, so the log choice never changes a schema; under `RAISE` it has none. |

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
server corrects. A key holds up to five tiers, one per subscription, and the only REST fact a
tier carries is its request limit (Basic: 5 per minute; every paid tier: none), so the one
tier input is `rest_basic_subscriptions`, which the library also learns from Massive's first
refusal; the 5 per minute is a wire constant, not configurable.

Environment variables have one name each and no fallbacks. An unset or blank variable is
absent; a value with surrounding whitespace raises `ValueError` naming the variable (never a
credential's value), as an argument would. The keyword settings (`raise`, `log`, `ignore`,
and the log levels) accept any case, as their tables say: a keyword names a choice, not an
identifier Massive matches. The credentials keep Massive's
names (`MASSIVE_API_KEY`, `MASSIVE_S3_ACCESS_KEY_ID`, `MASSIVE_S3_SECRET_ACCESS_KEY`), so a key
set up for Massive's own tools works unchanged.

The log is not configured through `MassiveConfig`: it belongs to the process, not to a session.
Two environment variables turn it on, read once per process when the first session is created
([section 17.1](#171-the-log)):

| Variable | Meaning |
|---|---|
| `DEEPHAVEN_MASSIVE_LOG` | A level name (`DEBUG`, `INFO`, `WARN`, `WARNING`, `ERROR`, `CRITICAL`, any case). Unset or blank changes nothing; any other value raises `ValueError` naming the variable from `MassiveSession()`. |
| `DEEPHAVEN_MASSIVE_LOG_FILE` | Append the lines to this file (UTF-8) instead of stderr. Alone, without `DEEPHAVEN_MASSIVE_LOG`, it turns the log on at DEBUG: naming a file asks for a log in it, and silently ignoring it would leave a tester with an empty file. A file that cannot be opened raises `OSError` naming the variable. |

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
([section 13.1](#131-invariants)). Individual plans allow one connection per cluster per
account, so two sessions with connections of their own would knock each other off and both
stall in `WAITING_SLOT`. Sessions share only when the settings that shape a connection are
equal: the API key, `ws_hosts`, `ack_timeout`, `reconnect_backoff`, `max_reconnect_backoff`,
`flush_interval`, and `rest_base_url` (the group's futures schedule calendar reads through any
of its sessions' REST clients, which must read the same API); they must also run on the same Deephaven update graph, because a
connection's flush thread enters one execution context for all its streams (a worker normally
has one graph, so this rarely matters and is not warned about). A connection carries one value
of each, so a session whose settings differ never joins another session's connection, and the
library never silently applies one session's settings to another session's streams. Such a session opens connections of its own,
and its construction logs a WARNING naming the other session and the settings that differ,
because on an individual plan the two then compete for the slot. A second session with equal
settings logs nothing. Sharing is per process: a session in another process that uses the key
still takes the slot, and each affected stream's `Error` then says so
([section 17](#17-diagnostics)).

Sessions with the same API key and REST origin share one REST `RequestGovernor`, because
Massive counts their requests together ([section 13.4](#134-rest)). `SessionClients` takes the
session's lease on it from the process's registry when it is created and registers the lease's
release as a session closer, so `close()` and the collection of an unclosed session both
release it; the lease refers to the registry, never to the core.

After `close()`: `close()` itself is idempotent; verbs, `m.connections()`, `m.streams()`, and
`m.rate_limits()` raise `MassiveError("MassiveSession #1 is closed")`; `m.release(table)` keeps working, and so
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
failure would land a cycle later. A `connections()`, `streams()`, or `rate_limits()` table
obtained before `close()` keeps working and shows the final rows, `CLOSED` included
([section 17](#17-diagnostics)).

Every verb raises `ValueError` when called on a UG thread (for example from inside a table
listener), checked before anything else, so also on a closed session; `close()` likewise. The
closed check comes next, still before any argument is validated, so `q.ring(0)` on a closed
session raises `MassiveError`. Verbs create tables and take the registry lock; both are
forbidden on the UG thread ([section 18](#18-concurrency-model)).

A thread that only holds the UG lock is not a UG thread, and every call works there:
`MassiveSession()`, every verb, `columns()`, `connections()`, `streams()`, `release()`, and
`close()`. This is the main deployment path, not an edge case: Deephaven's console runs every
command holding the UG's exclusive lock (`PythonDeephavenSession.evaluate` runs the script
inside `exclusiveLock().doLockedInterruptibly`), so no update cycle runs until the command
returns. A call on such a thread never waits on a thread that may need the UG lock or an
update cycle to finish: the diagnostics writer, a poller, or another caller's stream open
([section 18.3](#183-lock-order)). What a call writes to a live table (stream rows,
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
([section 13.4](#134-rest)). An application script runs under the lock too, so the remedy is
`wait=False`, a read on a library thread of its own ([section 7](#7-query-classes-and-verbs)),
not a script.

**How long a session lives.** The rule: a live table keeps alive everything it needs, and
nothing else keeps anything alive. A session lives while the user holds it (`m`, a query, a
namespace), and while it has a running stream, a running `poll()` table, a background read
(`wait=False`) whose result is held or still in use, or a diagnostics table someone holds ([section 6.1](#61-how-a-live-table-is-kept-alive-and-released)). Only those hold
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
its connections and pollers") and runs them, and then queues the diagnostics writer's stop
behind them, so the rows they write are applied. The reaper keeps running while results are
still tracked and stops once the last one has been collected and released. `close()` empties
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
  stream registry, its worker-table registry (`poll()` tables and background reads), or its count
  of held diagnostics tables is non-empty;
  every method that changes one of them updates the set in the same `registry_lock` section.
  The set is what keeps a session with a running stream or poll alive after the user dropped
  every reference to it: nothing else in the library holds a core strongly (connections,
  flushers, stream sinks, pollers, and the clients hold it weakly). A sink can outlive its
  stream's record in a container cleaned up later (the routes of a connection a verdict moved
  the stream off, which its flush thread clears when it ends), so a strong reference there
  would keep a finished session alive; a sink whose core is gone reports nothing. A core that leaves the set is handed
  to the reaper as a job argument, so its last reference is not dropped under the lock. The
  thread that took it out still holds it while it returns, so the final release may also land
  there, on whatever thread that is; that is safe because a core's finalizer only queues work
  (its closers and the diagnostics writer's stop go to the reaper) and never blocks or calls
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
  `MassiveError` naming the stream's failure, with the hold released. `SessionCore.stream_result`
  checks the stream's recorded failure before it builds the shape, and wraps in the same
  `MassiveError` the `DHError` ("Can not listen to failed table") Deephaven raises when the
  failure lands while the shape derives from the blink table. A stream's failure is recorded
  (`SessionCore.fail_stream`) before its table is failed (`publish_failure`), so a verb that
  meets the failed table always finds the failure to name.
- **A failure pins its table.** Every blink table the library feeds (a stream's, a `poll()`
  table's) is created by `blink_publisher` (`_tables/publisher.py`), whose
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
  5. It writes the final rows, then releases the diagnostics scope, and returns once the
     update cycle that applied the rows has ended (it takes the shared update-graph lock once),
     so a held `m.streams()` view, which updates later in that cycle, shows them. On a thread
     that holds the UG lock it waits for neither: the writer's `InputTable.add` waits for an
     update cycle, which cannot run until the caller releases the lock, so `close()` asks the
     writer to apply the rows, release the scope, and end, and returns at once; the rows, and
     a held view, follow once the console command is over.
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
  looking live. The WebSocket client thread reports any internal failure as `FAILED`, which fails
  the connection's streams ([section 13.3](#133-connection-and-subscription-protocol)).

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

There is no `stop()` on queries. Release is automatic (garbage collection) or explicit
(`m.release`); `m.close()` stops anything, whatever still holds it.

## 7. Query classes and verbs

Four classes, defined in `_query.py`. Nothing touches the network until a verb is called. Queries
are immutable, hashable, and reusable. A query's repr is the call that built it, as a user would
type it: `m.options.contracts(underlying='AAPL')`, `m.stocks.trades(['AAPL', 'MSFT'])`. It is
built from the normalized arguments, so equal queries print alike:

- public parameter names, never wire names;
- `symbols` positional: one symbol as `'AAPL'`, several as the sorted list `['AAPL', 'MSFT']`,
  whose first five are followed by `… (N more)`; `dm.ALL` as `dm.ALL` and a symbol table,
  static or refreshing, as `<symbol table>`;
- an argument the method requires, such as a bar's `timespan`, by position after the symbols
  (`m.stocks.bars('AAPL', dm.Timespan.DAY)`); every other one as `name=value`;
- enum members as `dm.ContractType.CALL`, dates and times as ISO-8601 text, a set of enum members
  in its enum's order;
- only the arguments that differ from their defaults, in signature order.

A query built without its dataset method (the library's own reads, tests) carries only its
dataset's path, `m.stocks.trades()`.

The dataset method records the call when it returns the query; equality stays on the normalized
request, so the call is not part of it. The repr appears in the `Query` column of `m.streams()`
and in the messages about the query: a verb that held the update graph's lock too long, a feed's
`poll()` without a start or a limit, a symbol table that holds no ticker. Messages about the
whole dataset (a verb or column it does not have, a source that cannot serve it) name the
dataset. Queries have no other attributes: the verbs and `columns()`
are their whole API (§ 22, “Sessions and queries expose no introspection attributes” and
“`columns()` on every query”).

```python
class QueryBase:                           # the base of all four classes below
    def columns(self) -> Table: ...        # static; the dataset's columns and what they mean;
                                           # no network, not a verb, not a result
```

```python
class Query:                               # REST list, snapshot, and one-row datasets
    def fetch(self, *, wait: bool = True) -> Table: ...
                                           # every row the endpoint returns, fully paginated;
                                           # static, or with wait=False read in the background
    def poll(self, interval: float | timedelta, *, mode: PollMode = PollMode.AUTO) -> Table: ...
                                           # live; re-runs fetch() every interval

class StreamQuery:                         # WebSocket datasets
    def blink(self) -> Table: ...
    def append_only(self) -> Table: ...
    def ring(self, capacity: int) -> Table: ...
    def last_by(self, by: str | Sequence[str] = "Symbol") -> Table: ...

class HistoryQuery:                        # datasets with a historical retrieval path
    def history(self, start: DateLike, end: DateLike | None = None, *,
                source: HistorySource = HistorySource.AUTO,
                wait: bool = True) -> Table: ...   # static, or read in the background

class StreamHistoryQuery(StreamQuery, HistoryQuery): ...
```

**The query class of a dataset** is derived from its `DatasetSpec`, never declared on it, so
the dataset layer never names the query classes. With S meaning the spec has channels, H that it
has a history source (a history path, a grouped-daily path, or a flat-file prefix), and F that
it has a fetch path, `query_class(spec)` in `_namespaces/query_building.py` gives:

| S | H | F | Class (not `bars`) |
|---|---|---|---|
| no | no | yes | `Query` |
| yes | no | no | `StreamQuery` |
| no | yes | no | `HistoryQuery` |
| yes | yes | no | `StreamHistoryQuery` |
| no | no | no | rejected: no verb |
| yes or no | yes or no, not both no | yes | rejected: a fetch path is a dataset of its own |

`DatasetSpec.__post_init__` raises `ValueError`, naming the dataset, for the rejected rows. A
fetch path never shares a dataset with a stream or a history source, so `fetch()` always means
every row the endpoint returns: a REST endpoint that answers one row per symbol (the last
trade, the last quote, the previous day's bar) is a one-row dataset of its own, named after the
endpoint ([section 7.5](#75-one-row-datasets)). A
`bars` spec must have H and no F; `query_class` gives it `HistoryQuery`, the class of its general
overload, and `bar_query_class(spec, timespan, multiplier)` picks the class of each call
([section 7.1](#71-bars-overloads)). A test in `tests/namespaces/test_query_building.py` checks that every
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
        `int`, `double`, `boolean`, `String`, `Instant`, `long[]`, or `String[]`; every calendar date,
        such as `Date`, is ISO `yyyy-MM-dd` text, a `String`);
        `Description` (String), what the column holds, empty when Massive gives no
        description; and `SuppliedBy` (String), the transports whose sources fill the
        column for this dataset, spelled `REST`, `flat files`, and `WebSocket`, in that
        order, joined by ", " (`fetch()` and `poll()` are served by REST, the stream verbs
        by WebSocket, and `history()` by REST or flat files). A dataset has only the columns
        some transport of its own fills, so `SuppliedBy` is never empty.

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

**`fetch()`.** Returns every row the dataset's REST endpoint returns for the request, fully
paginated: a list, a snapshot, or a one-row endpoint ([section 7.5](#75-one-row-datasets)).
Only `Query` has it, and no streamed or history dataset has `fetch()` or `poll()`. Current
state on every tier is `m.<class>.snapshots(symbols).poll(n)`, one request per cycle.

**`poll()`.** `mode=PollMode.AUTO`, the default, uses the dataset's own mode from the catalog,
which each dataset's docstring names (`DatasetSpec.poll_mode`, never `AUTO` itself):
`PollMode.APPEND` for `reference.news`,
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

**Feeds need `start` or `limit`.** A *feed* is a dataset whose default `poll()` mode is
`APPEND`: the news datasets and the filings listed above. Massive keeps a feed's whole archive,
and a feed query's `start`, `end`, and `limit` are its only bounds on what a cycle reads, so a
`poll()` with neither `start` nor `limit` would re-read the archive every cycle, spending a
Basic subscription's five requests a minute on pages the table already holds. `poll()` on such
a feed query raises `ValueError` before the table exists, in every mode, naming the dataset and
the fix: `reference.news poll() needs a start or a limit: without either, every cycle re-reads
the whole archive Massive keeps. Build the query with a start, such as m.reference.news(...,
start=date.today()), or with a limit and a newest-first order, then call poll(). A fixed start
keeps widening the window on a poll that runs for days`. A query with a `limit` is the natural
"latest headlines" poll and is allowed. The `limit` keeps the first rows in the endpoint's
order, so the query asks for newest first where Massive documents no such default:
`m.reference.news(symbols="AAPL", sort="published_utc", order=dm.Order.DESC,
limit=50).poll(60)` (Benzinga news is documented newest first already). The library does not
check the order: the docstrings say it. The refusal logs one DEBUG line and nothing above it:
the error is the report. `fetch()` keeps reading the whole range. Every feed declares a time
axis (the catalog's feed-axis gate), so a `start` can always be given. A moving window that
reads only what is new
since the last cycle is a separate design item: it needs rules for late and revised articles
and a mark per ticker.

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
  `LastTimestamp`, the time of the quote it used; every `last_trade` and `last_quote` its
  `Timestamp`, and every `previous_day_bar` its `TimestampStart`, so an explicit `APPEND`
  records each cycle's row when it is new (a trade or quote between two cycles is never
  seen); the indices snapshots `LastUpdated`; the
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
`watched_rows(blink.last_by(keys), symbol_table)`, so rows of removed symbols are evicted from
the result. The underlying `last_by` state keeps every symbol ever seen: a symbol that is
removed and re-added shows its stale row until its next tick. With keys that span symbols (for
example `by="Exchange"`) the filter comes first, `watched_rows(blink,
symbol_table).last_by(keys)`: filtering after the aggregation would drop a whole group whenever its
latest row came from a removed symbol. Each group then shows the latest row of a symbol that was
watched when the row arrived, and keeps it after that symbol is removed until a watched symbol
ticks in the group.
`watched_rows` (`_tables/watchlist.py`) keeps what `where_in(symbol_table, "Symbol")` keeps, but
builds it from a join: `natural_join` of the table with the symbol table's distinct symbols,
marked in a hidden column, then a match filter on the mark. Deephaven 42.7's `where_in` delivers
a failure of each of its two tables to its result, and a second failure trips an assertion
whose fatal-error handler calls `System.exit`: verified for both tables failing in one update
cycle, and for the symbol table failing before the filtered one. A failed symbol table fails
its stream (below), and the stream's failure always follows the symbol table's, so a
`where_in` here ended the process every time a reactive `last_by()`'s symbol table failed, or
the session closed while one ran over another of its live tables. A join forwards the first
failure of its two tables and ignores the second; so do `merge` and `aj` (verified the same
way). A blink table stays a blink table through the join and the filter.
`last_by()` never seeds from REST. The last-trade and last-quote endpoints (the one-row
datasets of [section 7.5](#75-one-row-datasets)) are absent from Starter and base Business
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
base aggregates, not rows, so it cannot serve as a row cap.) A `Table` passed as `symbols`,
static or refreshing, is read once, when the verb is called, on the caller thread inside the
session's execution context: its rows become the sorted symbol tuple that every source
receives (`REST`, `FLAT_FILES`, and the `AUTO` choice). A row that is not Massive's exact
ticker raises `ValueError`, and so does a table with no symbol, as an empty symbol list does.

**Backfill plus live.** There is no seeding verb. The documented recipe is:

```python
q = m.stocks.trades(["AAPL"])
t = merge([q.history("2026-10-20", "2026-10-23"), q.append_only()])
```

The history range is half-open and the live stream starts when `append_only()` is called, so
rows between the two can be missing or duplicated. The user owns that boundary.

**`wait`.** `fetch()` and `history()` take `wait: bool = True`, keyword-only, on every dataset
that has them, the one-row datasets included; the live verbs already return at once. A bool,
like `adjusted` and `expired`: a yes or no, not a choice among several. `wait=True` blocks until
the whole result is read and returns a static table, as before. `wait=False` returns at once a
`Table` with the same columns and no rows, refreshing. When the whole download has landed, the
complete result appears in one update and never changes again; it is never half loaded, and
the return type is the same as `wait=True`, with no `Future`, since a `Future` would need a
later `result()` that cannot wait inside a console command. The read runs on a library thread
of its own, so it holds no lock: inside a console command the command returns at once and
every live table keeps updating while the read runs.

- **Arguments raise at the call.** Everything `wait=True` checks before its first request is
  checked on the caller thread before the table exists: types and ranges, `dm.ALL` where it is
  not allowed, a symbol table's rows (read once, now), the source choice of `history()`, every
  REST path's parameters (`RestTransport.check_fetch` and `check_history` plan the requests
  with no request made; a crypto pair is split later, by the read), a closed session, and the
  update-graph thread. A failure during the download fails the table instead
  ([section 6.1](#61-how-a-live-table-is-kept-alive-and-released)). A `history()` range no
  row can lie in still returns a background read, whose empty result lands at once, so
  `wait=False` always returns the same kind of table.
- **Telling loading from done.** The result's `m.streams()` row has `Kind=read` and `State`
  `LOADING`, then `DONE` (or `FAILED`, `RELEASED`, `CLOSED`), with `RowsPublished` set when the
  rows land. The table's description, which Deephaven fixes when the table is created, says it
  is read in the background: "no rows until the download completes, then the complete result
  in one update, which never changes; m.streams() shows LOADING, then DONE or FAILED". The
  first line of the `wait` argument's docstring warns that `to_pandas()` before the fill
  returns no rows, and that `snapshot()` gives a static copy.
- **Lifetime.** The result has its own liveness scope, like a live table. Releasing it (and
  every table derived from it), garbage collection, or `m.close()` cancels the download, and
  `m.release()` accepts it.
- **Rate limits.** The read holds no lock, so it waits out the full 10 minutes of pauses
  (`READ_PATIENCE`), not the 90 s of a console command; pacing never counts toward patience,
  so a paced Basic download of hours completes ([section 13.4](#134-rest)).
- **Logging.** The call logs `call 7 start: <query>.fetch(wait=False)` at DEBUG at once, and
  `call 7 done: <rows> rows in <s> s` (DEBUG) or `call 7 failed after <s> s: <error>` (ERROR)
  from the read's thread. The call id is the row's `StreamId`.

### 7.1 `bars()` overloads

The bar timespan decides which transport exists: `SECOND` and `MINUTE` have WebSocket channels,
and everything else is history only. The previous day's bar from `/prev` is the
`previous_day_bar` dataset, not a verb of `bars()` ([section 7.5](#75-one-row-datasets)).
Typed overloads pick the class statically so no verb-time error is needed for the timespan.
`timespan` is a required positional argument with no default: a bar's length is the first thing
a reader of the call needs, and no one length suits most uses (one-minute bars stream, daily
bars feed most studies), so every call names it.

```python
# _types.py
StreamSpan = Literal[Timespan.SECOND, Timespan.MINUTE]
FuturesSpan = Literal[Timespan.SECOND, Timespan.MINUTE, Timespan.HOUR, Timespan.SESSION,
                      Timespan.WEEK, Timespan.MONTH, Timespan.QUARTER, Timespan.YEAR]   # no DAY

class _Stocks:   # same shape on m.options; m.indices, m.forex, and m.crypto have no `adjusted`
    @overload
    def bars(self, symbols: Symbols, timespan: StreamSpan, *,
             multiplier: Literal[1] = 1, adjusted: bool = True) -> StreamHistoryQuery: ...
    @overload
    def bars(self, symbols: Symbols, timespan: Timespan, *,
             multiplier: int = 1, adjusted: bool = True) -> HistoryQuery: ...

class _Futures:
    @overload
    def bars(self, symbols: Symbols, timespan: StreamSpan, *,
             multiplier: Literal[1] = 1,
             venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue)) -> StreamHistoryQuery: ...
    @overload
    def bars(self, symbols: Symbols, timespan: FuturesSpan, *,
             multiplier: int = 1,
             venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue)) -> HistoryQuery: ...
```

The implementation returns the most specific class for the actual values. The general overload
returns the base class, so the overlapping overloads type-check (the narrower return type is a
subtype). `bars(syms, Timespan.MINUTE)` and `bars(syms, Timespan.SECOND)` are stream plus
history. `bars(syms, Timespan.DAY)` and `bars(syms, Timespan.MINUTE, multiplier=5)` are history
only. `bars(syms)` without a timespan is a static type error and a `TypeError` at the call.

`Timespan.DAY` on futures is a static type error and a synchronous `ValueError` at construction:
the futures aggregates endpoint accepts the units `sec`, `min`, `hour`, `session`, `week`,
`month`, `quarter`, and `year` only. `Timespan.SESSION` outside futures is likewise rejected at
construction. The futures `resolution` parameter is `f"{multiplier}{unit}"`; per-unit multiplier
maxima are validated by the server (HTTP 400 becomes `MassiveError`).

`multiplier: Literal[1]` is an integer literal, not a categorical, and is allowed.

### 7.2 Synchronous `ValueError`s for parameter values

- `ALL` passed to a one-row dataset, at construction (the last-trade, last-quote, and `/prev`
  endpoints take one ticker per request; [section 7.5](#75-one-row-datasets)).
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
transports one way, `REST`, `flat files`, and `WebSocket` (`column_descriptions.TRANSPORT_LABELS`): the
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
tick by tick."`), where `<summary>` is the one-sentence `DatasetSpec.summary`; a `history()`
table that read a source appends `Read from <source>: <why>.` (`describe(..., note=...)`,
[section 16](#16-historysourceauto)). The summary is
a required field of every spec; `DatasetSpec.__post_init__` raises `ValueError`, naming the
dataset, unless it is one line of text that ends in a period, with no leading or trailing
whitespace. The dataset method's docstring starts with the same sentence, which the summary
rule in `tests/namespaces/test_query_building.py` checks for every factory of the catalog; the library
never reads `__doc__` at run time, so `python -OO` changes nothing.

- **Every verb describes the exact table it returns, after the shape is derived.** Deephaven
  42.7 copies `ColumnDescriptions` through `flatten`, `sort`, filters, `reverse`,
  `partition_by`, `coalesce`, `first_by`, `last_by`, and tree and preview operations, and
  `select`, `view`, `rename`, and joins keep the descriptions of the columns they keep.
  Nothing in `BlinkTableTools` or `RingTableTools` copies attributes, so an append-only or
  ring table derived from a described blink table would lose them. The verb therefore calls
  `describe()` last: on the blink copy for `blink()`, on the append-only or ring table, on the
  `last_by` table after any symbol-table filter, on the `poll()` table (the shape derived from its
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

**3. The dataset reference documents them on GitHub.** `docs/datasets/` holds an index,
`README.md`, which GitHub shows when a reader opens the directory, and one generated page per
dataset, named after its dotted name (`stocks.trades.md`, `stocks.financials.ratios.md`, and
`snapshots.md` for `m.snapshots`, a dataset of the session itself). The index has a grid of
the dataset names two or more asset-class namespaces share (`trades`, `bars`, `snapshots`, …)
by asset class, each mark linking that class's page; the recipe that merges tables of two
asset classes on the columns they share, `merge([t.view(common) for t in tables])`, or, to
keep every column, a typed null added for each missing one first
(`update("Tape = NULL_LONG")`); and one table per namespace listing each dataset's page,
summary, and verbs. A dataset's page has its call path as title (`m.stocks.trades`), its
`DatasetSpec.summary`, its call with every argument's default (`DatasetCall.signature`), and a
table of facts: its verbs, the transports that serve it, the wire parameters its `start` and
`end` are sent as, its default `poll()` mode and keys (for a dataset with `fetch()`), the
`sort` values it takes (an enum's values, or any REST field for a free-text `sort`) and the
`filters` keys it accepts (`filters.filter_keys`, minus the families of the method's own
arguments), each only where the method takes `sort` or `filters`. Then come **Plan access**,
the plan tables of Massive's pages that document its sources (`Provenance.plans`, read by the
refresh from each page's "Plan Access", "Plan Recency", and "Plan History" sections, so they
follow every refresh): one row per plan, one column per page, each cell the plan's recency and
history, or "Not included"; and **Columns**, one row per column in result order, with
**Column**, **Deephaven type**, **Description**, and **Supplied by** (the transports of
`columns()`'s `SuppliedBy`, in the same spelling), a note on `RawSymbol` where the dataset has
it, and, folded in `<details>`, one **Wire field** column per transport the dataset uses
(`REST field`, `Flat-file field`, `WebSocket field`). A wire-field cell lists the distinct
wire keys of that transport's sources that feed the column, in source order and before any
projection, or `request` for a key filled from the request. Then a list headed "Massive's
documentation of this dataset:" gives the vendor pages of that dataset's own sources, sorted and
distinct, from the `Provenance` passed to the renderer ([section 19.4](#194-generated-schemas));
a dataset whose sources have no URLs gets no list. A source several asset classes share (the
unified snapshot `/v3/snapshot`, `/v3/reference/tickers`) has one page per class; for a dataset
with an `asset_class`, only the pages whose URL path has a segment equal to that class's value
(`/docs/rest/options/snapshots/unified-snapshot.md` for `options.snapshots`) are listed, and all
of the source's pages when none matches, so a dataset never loses its links to a change in the
vendor's URL layout. A dataset that spans classes (`asset_class` None, such as
`snapshots` or `reference.tickers`) lists every class's page; the plan table uses the same
pages. Every page ends with a footer naming the generator command and the snapshot's fetch
date. Units and nullability get no columns: every timestamp is an `Instant`, and **Supplied by**
with the description already says when a column is null. Readers can see what each table holds
and which plan serves it before they install.

- **The renderer** is `column_reference.render(catalog: Catalog, provenance: Provenance,
  calls: Mapping[str, DatasetCall]) -> dict[str, str]` in `_datasets/column_reference.py`, a
  pure function from the `catalog` (the `Catalog` of `_datasets/catalog.py`: every declared
  `DatasetSpec`, by name), `provenance`, and `calls` it is given, and the dataset schemas, to
  `{file name: page text}`; it reads no other state, so tests pass a synthetic catalog,
  `Provenance`, and calls. Each `DatasetCall` (signature, verbs, argument names, and the wire
  parameter each argument sets) comes from the API layer, `dataset_calls()` in
  `_dataset_list.py`, which walks the namespace classes, since the dataset layer may not import
  the dataset methods. A **Deephaven type** cell is
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
- **The command** `uv run python scripts/render_reference.py` regenerates the whole dataset
  reference and nothing else. The script starts the embedded Deephaven server (the API imports
  Deephaven, and the library never imports `deephaven_server`), imports the library, takes the
  catalog the session module builds from every namespace module, calls
  `render(catalog, _massive.provenance(), dataset_calls())`, prints which columns each dataset gained or lost
  against the committed pages (`column_changes`), then calls `sync()` on `docs/datasets/` and
  prints what it wrote and deleted. Run after every refresh, the column report is how a refresh
  shows what it does to each dataset's table (the wire package cannot see datasets). It holds no logic of its own. A renamed or removed namespace or dataset therefore never leaves an
  orphaned page: the one command that writes the directory also empties it of everything it
  did not render.
- **The parity test**, `tests/datasets/test_column_reference.py`, renders the pages in the test
  JVM and fails on a stale page (bytes differ), a missing page, and any extra file or directory
  under `docs/datasets/`, naming the command that fixes it. A catalog change or a refresh that moves
  a description or a plan table therefore lands with its regenerated pages in the same pull
  request.
- **Dataset method docstrings link, never list.** A column list in a docstring is a
  hand-written schema and goes stale. Each dataset method's docstring starts with its
  `DatasetSpec.summary`, explains what is unusual about the dataset, and names its page,
  `docs/datasets/<dataset>.md`. `tests/datasets/test_column_reference.py` checks that every
  declared dataset's docstring names its own page. The summary is the docstring's whole first line: ruff's
  D205 requires a blank line after that line, so a sentence too long to fit on it, after the
  method's indentation and the opening quotes, within the 100-character line length, is
  shortened in the spec, never wrapped in the docstring.
- **Dataset and verb docstrings link the README, never repeat it** (owner decision, 2026-10-30).
  `README.md` holds all hand-written user documentation and defines once each shared argument
  (`symbols`, `start` and `end`, `as_of`, `sort`, `order`, `limit`, `filters`, `adjusted`,
  `timespan`, `multiplier`, `venues`, and the verb arguments) and each concept (table shapes,
  time, bars, history sources, split adjustment, live-table lifetime, plans and errors,
  diagnostics), each under an explicit `<a id>` anchor. A dataset docstring runs about 15 to 25
  lines: the summary; what a row is and what is unusual about the dataset (its `poll()` mode
  where not the plain default, a per-symbol `limit`, a refused `dm.ALL`, vendor quirks); one
  Args line per argument, a shared one giving only this dataset's difference and `See
  README.md#<anchor>.`; a one-line Returns naming `docs/datasets/<dataset>.md`; exact Raises;
  and one or two example lines. A verb docstring stays within 40 lines. `tests/test_docs.py`
  fails on any cited `README.md#<anchor>` or `docs/datasets/<dataset>.md` that does not exist,
  runs every Python block of the README against the fakes, and checks the README's namespace
  table against the catalog. The tests that pinned the old repeated sentences check only that a
  docstring names its own page.

### 7.5 One-row datasets

A REST endpoint that answers one row per symbol is a dataset of its own, named after the
endpoint, with `fetch()` and `poll()` like every `Query`:

| Dataset | Classes | REST | Kind |
|---|---|---|---|
| `last_trade(symbols)` | stocks, options, crypto | `/v2/last/trade/{t}`, `/v2/last/trade/{o}`, `/v1/last/crypto/{from}/{to}` | trades |
| `last_quote(symbols)` | stocks, forex | `/v2/last/nbbo/{t}`, `/v1/last_quote/currencies/{from}/{to}` | quotes |
| `previous_day_bar(symbols)`, with `adjusted: bool = True` on stocks and options | stocks, options, indices, forex, crypto | `/v2/aggs/ticker/{t}/prev` | bars |

- **One request per symbol.** Each endpoint takes one ticker per request, so `ALL` raises
  `ValueError` at construction, naming the alternative: `m.<class>.snapshots(dm.ALL).fetch()`
  for the last trade or quote (`m.options.chain(underlying).fetch()` for options, whose
  snapshot of every contract is slow), and `m.<class>.bars(dm.ALL, dm.Timespan.DAY)
  .history(day, day)` for the previous day's bar (with `adjusted=False` for options and
  without `adjusted` for indices, whose `dm.ALL` daily bars only the flat files serve). A
  symbol with no data (HTTP 404) is left out with a WARNING, as for every per-symbol path.
- **The kind's columns.** Each one-row dataset is a member of its kind
  ([section 19.4](#194-generated-schemas)), and its rows have the columns of its kind that its
  endpoint fills, under the kind's names and types, so a column it shares with
  `m.stocks.trades(syms).history(...)` means the same thing in both. A kind's group may be
  split over several datasets of its class: each source of the group is read by exactly one
  of them, and the merged schema walks the group's sources in their positions, whichever
  dataset reads them, so the columns are in the same order as before the split.
- **Bars.** `previous_day_bar` rows are 1-day bars with the day rules of
  [section 12](#12-timestamp-units-and-day-boundaries): `TimestampStart` at 00:00 of the day in
  the class day zone, whatever `/prev` stamps, `TimestampEnd` 00:00 of the next day (01:00 New
  York time for indices); a stamp at a time of day Massive is not known to use is served too,
  labelled by the day it falls on, and logs one WARNING per dataset and timespan per session
  (decision 70). Stock and
  option bars are split-adjusted unless `adjusted=False`, and every page's `adjusted` echo is
  checked as for `bars()`. The query's request carries `Timespan.DAY` and multiplier 1, which
  the caller never passes, so the repr is the call alone: `m.stocks.previous_day_bar('AAPL')`.
- **`poll()`.** `REPLACE`, keyed by `Symbol`, so a polled one-row dataset holds each symbol's
  latest row; an explicit `APPEND` records each cycle's row when it is new, through its revision
  key (`Timestamp`, or `TimestampStart` for the bar). A poll sees only the latest trade or quote
  at each cycle, never the ones between two cycles: `trades()` and `quotes()` stream every one.
- **What goes.** `fetch()` on a stream or history dataset, the `StreamHistoryLatestQuery` and
  `HistoryLatestQuery` classes, the `DaySpan` overload of `bars()`, and `fetch()` of
  `indices.values`, which read the indices snapshot (`m.indices.snapshots` serves it in full).
  The indices snapshot leaves the values kind.

### 7.6 A verb a query does not have

A verb a dataset cannot serve does not exist on its query, so tab completion and `hasattr`
stay honest, and calling one raises `AttributeError`. `QueryBase.__getattr__` gives that
error a reason and an alternative when the attribute is a verb name (`fetch`, `poll`, `blink`,
`append_only`, `ring`, `last_by`, `history`): the dataset, the verbs it does have, and, where
one exists, the dataset of the same kind and class that has the verb
(`m.stocks.trades(syms).fetch()` names `m.stocks.last_trade(...).fetch()`, and
`m.stocks.bars(syms, dm.Timespan.DAY).fetch()` names `m.stocks.previous_day_bar(...).fetch()`);
`fetch()` or `poll()` of intraday bars names `history(start, end)` first, then the previous
day's bar;
`fetch()` or `poll()` of a stream dataset with no such sibling names the class's
`snapshots(...)`, and `history()` of a dataset with a time axis names its `start=` and `end=`
arguments. Any other missing attribute raises Python's plain `AttributeError`.

### 7.7 The dataset list

`dm.datasets()` returns a static table with one row per dataset of `CATALOG`, in declaration
order, so a user can browse and filter what exists from the console before any session or API
key: it reads only the catalog and makes no network call. Its columns, each with a tooltip
description:

| Column | Type | Content |
|---|---|---|
| `Dataset` | String | The catalog name, such as `stocks.trades`. |
| `Call` | String | How a session `m` reaches it (`m.stocks.trades`; `m.snapshots` for the unified snapshot). |
| `Summary` | String | `DatasetSpec.summary`, the first sentence of the method's docstring. |
| `Verbs` | String[] | The verbs its queries have, in the order `fetch`, `poll`, `blink`, `append_only`, `ring`, `last_by`, `history`; a `bars` dataset lists the stream verbs, which only its streaming timespans have. |
| `Transports` | String[] | The transports that serve it, spelled as `columns()` spells them: `REST`, `flat files`, `WebSocket`. |
| `Documentation` | String | The web address of its page of the dataset reference. |

`Verbs` and `Transports` are arrays, so a filter tests membership:
``dm.datasets().where("Arrays.asList(Verbs).contains(`blink`)")``. The function is a module-level
name rather than a session method because the catalog needs no session.

## 8. Namespace tree

Notation:

- `Symbols = str | Sequence[str] | Table | _All` ([section 10](#10-symbols-and-all)).
- `DateLike = str | date | datetime | numpy.datetime64`, plus Deephaven's Java `Instant`,
  `ZonedDateTime`, and `LocalDate` ([section 11](#11-enums-and-types)).
- `…F` is the trailing keyword-only block
  `sort: str | None = None, order: Order | None = None, limit: int | None = None, filters: Mapping[str, FilterValue] | None = None`.
  The block offers only the ordering the dataset's paths accept, so a signature never offers
  one the endpoint cannot apply: a dataset whose paths have neither a `sort` nor an `order`
  parameter (futures `exchanges` and `market_status`, for example) leaves both out, and a
  dataset whose paths have `order` but no `sort` (the indicators) keeps `order` alone. How
  `sort` and `order` reach the wire, and how `sort` is checked, is in [section 9](#9-filters).
- **One name, form, and default per parameter.** `symbols` is the first positional parameter
  of every dataset that takes it, also where it defaults to None (the list datasets, where None
  means no ticker filter). A point-in-time parameter is `as_of`, never `date`, since `date`
  means the trading day itself elsewhere (`daily_open_close(symbols, date)`). An identity
  parameter Massive takes in one-or-many form (`product_code`, `issuer_cik`, `owner_cik`,
  `filer_cik`) takes one `str` or a sequence. Where Massive's spec gives a parameter a default,
  the signature shows that default (the indicators' windows and `adjusted=True`), and a test
  compares each one with the generated `Param.default`. None means only "this parameter is not
  sent" or "no value given", never a mode: a choice among behaviours has an enum member
  (`PollMode.AUTO`) or a real default (`last_by(by="Symbol")`, `expired=False`,
  `venues=frozenset(FuturesVenue)`).
- **Exact input, never cleaned.** Every text a caller passes is taken exactly as written or
  refused with `ValueError`; the library never strips, re-cases, or otherwise repairs it, so
  a typo fails at once instead of reaching Massive as a different request. The message names
  the form to write (`from_currency=' usd' must be a three-letter upper-case currency code;
  write 'USD'`). This holds for symbols ([section 10](#10-symbols-and-all)), identifiers such
  as currency codes, date and time text, `filters` values and `sort`, and `MassiveConfig`'s
  text fields and environment variables ([section 5](#5-massiveconfig)). The converters
  (`dm.canonical_symbol*`) exist to turn another spelling into Massive's, and they convert
  only the spellings they document.
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
- `limit` is a total-row cap, except where each request names one symbol: a per-symbol path
  (the indicators) or a symbol parameter that takes one ticker
  (`reference.tickers`, `stocks.ipos`), and a dataset whose symbols overlap
  (`DatasetSpec.symbols_overlap`, `reference.news`). There it caps each symbol's request, so
  one ticker's rows never crowd out another's (owner decision 74). Page size is internal. The README states the rule once, and each per-symbol dataset's docstring says so. The cap is
  stored apart from the wire parameters, in `Request.limit`, so it can never be sent as the
  endpoint's page-size `limit`; the page size is the path's documented maximum (or
  `DatasetSpec.page_size` where the dataset sets one), or `Request.limit` when that is smaller
  (`snapshots(ALL, limit=10)` asks for 10 rows, not 250), and paging stops once `limit` rows
  have arrived.
- The page size is the endpoint's documented maximum `limit`, which the refresh generates from
  the specification (`Param.maximum`), so no dataset types it. `DatasetSpec.page_size`
  overrides it in two cases, and the catalog's `page size` gate refuses any other: a maximum
  the specification states only in prose (the aggregates range paths, 50,000), and a lower
  size where the rows are long so one page decodes to some tens of MB at most: Benzinga news
  1,000 (each article carries its whole HTML body, so 50,000 could reach hundreds of MB), Benzinga analyst
  insights and filing risk factors 5,000 (each row quotes prose), and the three financial
  statements 10,000 (some fifty line items a row, over 60 MB at 50,000). Every other page
  size is the documented maximum: 50,000 short rows decode to some 10 to 40 MB, and the
  text-heavy filings endpoints (10-K sections, 8-K texts) document at most 100 rows.
- The REST column names the path template as the OpenAPI spec writes it (the spec's own
  placeholder names, such as `{stocksTicker}`, are abbreviated here to `{t}`, `{o}`, `{u}`).
  Every path is a generated REST source (`_massive.source(Transport.REST, template)`,
  [section 19.4](#194-generated-schemas)). Each dataset names its paths by role (`fetch_path`,
  `detail_path`, `fetch_paths_by_class`, `history_path`, `grouped_daily_path`, `extra_paths`),
  and declares the key columns of each
  source it reads as `KeySpec`s ([section 19.4](#194-generated-schemas), "Keys are declared
  per source"): the `symbol`, `time`, `end`, instrument `fanout`, and `date` keys become
  `Symbol`, `Timestamp` (a bar's `TimestampStart`), `TimestampEnd`, `Underlying`, and `Date`,
  and a non-instrument `fanout` key keeps its column. A `time` or `end` key is always an
  instant, so those columns are `Instant`s. Every axis below that is a calendar date (all but
  the instants `published_utc`, `published`, and the indicators' `timestamp`: the dividends,
  splits, IPOs, short interest and volume, financial statements, filings, futures schedules,
  economy, and TMX axes) keeps its date column, ISO text, under its own name, such as `Date`
  or `ExDividendDate`, and declares no `time` key; such a dataset has no time columns of the
  library's but `TimestampInsert`, so its vendor `Date` is its own. Datasets whose
  rows each list several tickers (a JSON `tickers` field) describe a filing, a company, or
  an article, not one instrument: the three financial statements,
  `filings.initial_insider_ownership` and `filings.insider_transactions` (Forms 3 and 4),
  `filings.insider_activity` and `filings.insider_holdings`, `filings.disclosures_8k`,
  `reference.news`, and `partners.benzinga.news` have no `Symbol`, keep their `Tickers`
  column, and use `symbols` only as a request filter ([section 10](#10-symbols-and-all)).

### 8.1 `m.stocks`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols)` | StreamHistoryQuery | `T` | history `/v3/trades/{t}` | `us_stocks_sip/trades_v1` |
| `last_trade(symbols)` ([7.5](#75-one-row-datasets)) | Query | | `/v2/last/trade/{t}` per symbol | |
| `quotes(symbols)` | StreamHistoryQuery | `Q` | history `/v3/quotes/{t}` | `us_stocks_sip/quotes_v1` |
| `last_quote(symbols)` | Query | | `/v2/last/nbbo/{t}` per symbol | |
| `bars(symbols, timespan, *, multiplier=1, adjusted=True)` | overloads ([7.1](#71-bars-overloads)) | `A` (1 s), `AM` (1 min) | history `/v2/aggs/ticker/{t}/range/{m}/{span}/{from}/{to}`; `ALL` with DAY: `/v2/aggs/grouped/locale/us/market/stocks/{date}` per day | `us_stocks_sip/minute_aggs_v1`, `us_stocks_sip/day_aggs_v1` (`adjusted=False` only) |
| `previous_day_bar(symbols, *, adjusted=True)` | Query | | `/v2/aggs/ticker/{t}/prev` per symbol | |
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
| `filings.institutional_holdings(*, filer_cik: str \| Sequence[str] \| None = None, …T)`, axis `filing_date` (Form 13-F); one CIK is sent as `filer_cik` and a sequence as `filer_cik.any_of` | Query | | `/stocks/filings/vX/13-F` | |
| `filings.initial_insider_ownership(symbols=None, *, issuer_cik: str \| Sequence[str] \| None = None, owner_cik: str \| Sequence[str] \| None = None, …T)` (Form 3); `filings.insider_transactions(...)` with the same signature (Form 4); axis `filing_date`. A CIK argument sends one CIK as `issuer_cik` or `owner_cik` and a sequence as its `.any_of` form; an empty sequence raises `ValueError` (as futures `product_code`) | Query | | `/stocks/filings/vX/form-3`, `/stocks/filings/vX/form-4` | |
| `filings.insider_activity(symbols=None, *, issuer_cik: str \| Sequence[str] \| None = None, …T)`, axis `transaction_date`: each issuer's open-market insider purchases and sales summed per transaction day, across every insider; `poll()` replaces its table, keyed by `IssuerCik` and `TransactionDate` (a later filing for the same day revises that day's totals) | Query | | `/stocks/insiders/vX/activity` | |
| `filings.insider_holdings(symbols=None, *, issuer_cik: str \| Sequence[str] \| None = None, owner_cik: str \| Sequence[str] \| None = None, …T)`, axis `last_filing_date`: each insider's current consolidated holding of one issuer, built by Massive from all of the owner's Forms 3, 4, and 5; `poll()` replaces its table, keyed by `IssuerCik` and `OwnerCik`. Every insider dataset takes the title of its Massive page: Form 3 is Massive's "Initial Insider Ownership" | Query | | `/stocks/insiders/vX/holdings` | |
| `filings.risk_factor_taxonomy(…F)`, `filings.disclosure_taxonomy(…F)` | Query | | `/stocks/taxonomies/vX/risk-factors`, `/stocks/taxonomies/vX/disclosures` | |

### 8.2 `m.options`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols)` | StreamHistoryQuery | `T` | history `/v3/trades/{o}` | `us_options_opra/trades_v1` |
| `last_trade(symbols)` | Query | | `/v2/last/trade/{o}` per contract | |
| `quotes(symbols)` | StreamHistoryQuery | `Q` | history `/v3/quotes/{o}` | `us_options_opra/quotes_v1` |
| `bars(symbols, timespan, *, multiplier=1, adjusted=True)` | overloads | `A`, `AM` | range | `us_options_opra/{minute,day}_aggs_v1` |
| `previous_day_bar(symbols, *, adjusted=True)` | Query | | `/v2/aggs/ticker/{o}/prev` per contract | |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/options`) | | |
| `chain(underlying: str, *, contract_type: ContractType \| None = None, …F)`; `filters` takes `strike_price_*`, `expiration_date_*` | Query | | `/v3/snapshot/options/{u}` | |
| `snapshots(symbols, *, …F)`; OCC contracts; `Underlying` from `underlying_asset.ticker`; `ALL` is every contract, paginated (slow, documented) | Query | | `/v3/snapshot?ticker.any_of=O:…`, 250 per request. `ALL` sends `type=options` in place of tickers, so it lists only options contracts; tickers are sent without `type`, which Massive refuses beside them (HTTP 400, "Cannot specify tickers and type."). `type` is rejected as a filter. Same fields as the per-contract endpoint, including greeks. `/v3/snapshot/options/{u}/{o}` is listed in the catalog as covered by this dataset. | |
| `contracts(symbols=None, *, underlying: str \| None = None, contract_type: ContractType \| None = None, as_of: DateLike \| None = None, expired: bool = False, …F)` (`expired=False`, Massive's default, lists contracts still trading and is not sent, so the per-contract path accepts it; `True` lists expired ones and is sent); `filters` takes `expiration_date_*`, `strike_price_*` | Query | | `/v3/reference/options/contracts`; with `symbols`: `/v3/reference/options/contracts/{o}` per contract | |
| `daily_open_close(symbols, date: DateLike, *, adjusted: bool = True)` | Query | | `/v1/open-close/{o}/{date}` per contract | |

Every options dataset has `OptionRoot`, the OCC root read from `Symbol` (`SPXW`, `AAPL1`;
[section 19.4](#194-generated-schemas)). `underlying=` takes Massive's exact stock or index
ticker (`AAPL`, `BRK.B`, `I:SPX`), case kept, and is sent as given. `Underlying` is exactly the
field Massive sends: it names the instrument Massive links the contract to, not the
deliverable, and is never derived from or mapped to the root (`AAPL1` is never `AAPL`). The
contracts dataset exposes the raw deliverable fields with their vendor descriptions:
`SharesPerContract`, `AdditionalUnderlyings` (JSON text of further deliverables), `Cfi`, and
`Correction`. There is no derived "standard deliverable" flag until a live check settles the
fields' meaning ([section 23](#23-open-verification-items)); the chain's docstring gives the
recipe that keeps standard contracts (`natural_join` with `contracts` on `Symbol`, keep
`SharesPerContract == 100` and an empty `AdditionalUnderlyings`).

### 8.3 `m.futures`

Streams always use the `/futures` cluster path. `venues` exists on the three factory methods
below and selects the flat-file prefixes only: it is not part of the stream key
([section 14](#14-publishers-fan-out-and-failure-semantics)). It is a set of `FuturesVenue`,
stored as a `frozenset`; the default, `frozenset(FuturesVenue)`, reads all four prefixes, in
`FuturesVenue` order whatever the set's order. An empty set raises `ValueError`, and anything
but a set of `FuturesVenue` raises `TypeError`.

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols, *, venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue))` | StreamHistoryQuery | `T` | history `/futures/v1/trades/{t}` (dates: one request series per weekday session, `session_end_date=<date>`; datetimes: the exact `timestamp` range; [section 12](#12-timestamp-units-and-day-boundaries)) | `us_futures_{venue}/trades_v1` |
| `quotes(symbols, *, venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue))` | StreamHistoryQuery | `Q` | history `/futures/v1/quotes/{t}` (as `trades`) | `us_futures_{venue}/quotes_v1` |
| `bars(symbols, timespan, *, multiplier=1, venues: AbstractSet[FuturesVenue] = frozenset(FuturesVenue))` | overloads | `A`, `AM` | history `/futures/v1/aggs/{t}` (`resolution`; `window_start` in ns; dates: from the first session's open less 4 days to 18:00 CT on the last date, kept by their session label (`Date`), a bar of several units only when it fits under both readings of its label; datetimes: the exact `window_start` range, bars kept by their own window; [section 12](#12-timestamp-units-and-day-boundaries)) | `us_futures_{venue}/minute_aggs_v1`, `us_futures_{venue}/session_aggs_v1` |
| `snapshots(symbols, *, product_code: str \| Sequence[str] \| None = None, …F)`; `ALL` with `product_code` is every contract of the products; `Symbol` is the contract's top-level `ticker`, where both vendor samples send it, though the OpenAPI schema nests it as `details.ticker` (a `KEY_FIXES` entry; [section 23](#23-open-verification-items)) | Query | | `/futures/v1/snapshot` | |
| `contracts(symbols=None, *, product_code: str \| Sequence[str] \| None = None, as_of: DateLike \| None = None, …F)`: `Symbol` and `RawSymbol` are the row's `ticker`, as on the market data, so a contracts table feeds `trades()`, `quotes()`, `bars()`, and `snapshots()` directly; explicit `symbols` are sent as `ticker.any_of`, 250 a request (the size of the stock, option, index, forex, and crypto snapshots), so `ticker.any_of` is no filter; None lists every contract and `ALL` is refused (leave `symbols` out); the `poll()` key is `Symbol` and `Date`. `products(*, product_code: str \| Sequence[str] \| None = None, as_of: DateLike \| None = None, …F)`. On both, `as_of` is sent as `date`, the day whose point-in-time specifications are listed; None for Massive's default, today. `exchanges(*, limit: int \| None = None)`, `market_status(*, product_code: str \| Sequence[str] \| None = None, limit: int \| None = None)` (neither path has a filter beyond the typed arguments, nor `sort`) | Query | | `/futures/v1/{contracts,products,exchanges,market-status}` | |
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

Massive has no futures last-trade, last-quote, or previous-bar endpoint, so futures has no
one-row dataset ([section 7.5](#75-one-row-datasets)). `snapshots(symbols).fetch()` is the
one-request answer. There is no futures `day_aggs` flat
file; `Timespan.DAY` is rejected ([section 7.1](#71-bars-overloads)).

### 8.4 `m.indices`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `values(symbols)` | StreamHistoryQuery | `V` | none: history flat files only | `us_indices/values_v1` |
| `bars(symbols, timespan, *, multiplier=1)` (no `adjusted`: the endpoint has none) | overloads | `A`, `AM` | range | `us_indices/{minute,day}_aggs_v1` |
| `previous_day_bar(symbols)` | Query | | `/v2/aggs/ticker/{t}/prev` per index | |
| `snapshots(symbols, *, …F)` | Query | | `/v3/snapshot/indices`, 250 tickers per request | |
| `daily_open_close(symbols, date: DateLike)` | Query | | `/v1/open-close/{t}/{date}` | |

### 8.5 `m.forex`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `quotes(symbols)` | StreamHistoryQuery | `C` | history `/v3/quotes/{C:…}` | `global_forex/quotes_v1` |
| `last_quote(symbols)` | Query | | `/v1/last_quote/currencies/{from}/{to}` per pair | |
| `bars(symbols, timespan, *, multiplier=1)` (no `adjusted`: currency pairs have no corporate actions, so no request sends it and flat files serve every timespan they hold; a 1-day bar is the UTC day, `TimestampStart` 00:00 UTC on every source: grouped daily, `/prev`, and `day_aggs_v1`'s 23:59:59.999 UTC are moved to the day's start, and any other stamp is moved the same way and warns (decision 70), [section 12](#12-timestamp-units-and-day-boundaries)) | overloads | `CAS` (1 s), `CA` (1 min) | range; `ALL` with DAY: `/v2/aggs/grouped/locale/global/market/fx/{date}` | `global_forex/{minute,day}_aggs_v1` |
| `previous_day_bar(symbols)` | Query | | `/v2/aggs/ticker/{C:…}/prev` per pair | |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/forex`) | | |
| `snapshots(symbols)`; `ALL` is all pairs | Query | | `/v2/snapshot/locale/global/markets/forex/tickers` | |
| `movers(direction: Direction)` | Query | | `/v2/snapshot/locale/global/markets/forex/{gainers\|losers}` | |
| `conversion(from_currency: str, to_currency: str, *, amount: float = 1.0, precision: int = 2)`; each currency is exactly a three-letter upper-case code (`"USD"`), and anything else (`" usd"`, `"usd"`) raises `ValueError` naming the code to write | Query | | `/v1/conversion/{from}/{to}` | |

### 8.6 `m.crypto`

| Method | Class | WebSocket | REST | Flat files |
|---|---|---|---|---|
| `trades(symbols)` | StreamHistoryQuery | `XT` | history `/v3/trades/{X:…}` | `global_crypto/trades_v1` |
| `last_trade(symbols)` | Query | | `/v1/last/crypto/{from}/{to}` per pair | |
| `quotes(symbols)` | StreamQuery | `XQ` | | |
| `bars(symbols, timespan, *, multiplier=1)` (no `adjusted`: crypto has no corporate actions, so the parameter is never sent; a 1-day bar is the UTC day, `TimestampStart` 00:00 UTC on every source: `day_aggs_v1`'s 23:59:59.999 UTC is moved to the day's start, and any stamp elsewhere in the day than its first or last millisecond (the documented REST samples sit at midnight New York time or, for grouped daily, at times such as 20:48 UTC), or a REST bar of several days or longer not at 00:00 UTC, is still served, moved to 00:00 UTC of the day it falls on, and logs one WARNING per dataset and timespan per session naming the stamp (decision 70); `AUTO` chooses as for every dataset; [section 12](#12-timestamp-units-and-day-boundaries), [section 23](#23-open-verification-items) item 1) | overloads | `XAS` (1 s), `XA` (1 min) | as forex; grouped `/v2/aggs/grouped/locale/global/market/crypto/{date}` | `global_crypto/{minute,day}_aggs_v1` |
| `previous_day_bar(symbols)` | Query | | `/v2/aggs/ticker/{X:…}/prev` per pair | |
| `fmv(symbols)` | StreamQuery | `FMV` (`business.massive.com/crypto`) | | |
| `snapshots(symbols)`, `movers(direction: Direction)` | Query | | `/v2/snapshot/locale/global/markets/crypto/{tickers,gainers,losers}` | |
| `daily_open_close(symbols, date: DateLike)` (no `adjusted`, as `bars`: the parameter is never sent; a row whose `isUTC` is false is served with the UTC day's window and logs one WARNING per dataset per session, decision 77) | Query | | `/v1/open-close/crypto/{from}/{to}/{date}` per pair | |

### 8.7 `m.reference`

All return `Query`.

| Method | REST |
|---|---|
| `tickers(symbols=None, *, market: Market \| None = None, ticker_type: TickerType \| None = None, exchange: str \| None = None, cusip: str \| None = None, cik: str \| None = None, as_of: DateLike \| None = None, active: bool = True, search: str \| None = None, …F)` | `/v3/reference/tickers`. `cusip` and `cik` are `str`, as the spec types them. `ticker_type` is sent as the wire parameter `type`, and `as_of` as `date`. |
| `ticker_details(symbols, *, as_of: DateLike \| None = None)` | `/v3/reference/tickers/{t}` per symbol; `as_of` is sent as `date` |
| `ticker_types(*, asset_class: AssetClass \| None = None, locale: Locale \| None = None)` | `/v3/reference/tickers/types` |
| `ticker_events(symbols, *, types: Sequence[str] \| None = None)` | `/vX/reference/tickers/{t}/events` per symbol |
| `related_companies(symbols)` | `/v1/related-companies/{t}` per symbol; rows are `{"ticker": peer}`, so `Symbol` is the peer and the requested symbol is `Underlying` |
| `news(symbols=None, *, …T)`, axis `published_utc` (half-open `.gte`/`.lt`) | `/v2/reference/news`. Its `ticker` parameter takes one ticker and the path has no `ticker.any_of`, so `symbols` (one ticker, a list, or a watchlist table, followed as every symbol table is) is sent one request per ticker, and an article several tickers return is kept once, at its first copy, by `Id` (`symbols_overlap`); in `fetch()` an article without an `Id` is kept, never merged, while a `poll()` table, keyed by `Id`, fails on one. Each `poll()` cycle re-reads the whole range for every ticker, so the docstring recommends a recent `start`, a `limit`, or both. Rows come ticker by ticker; `limit`, `sort`, and `order` apply to each ticker's request, as the docstring says, and on a 5-requests-a-minute plan a 50-ticker `poll(60)` outruns its interval and logs the slow-poll WARNING. `symbols=None` reads every article |
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
sma(symbols, window: int = 50, *, timespan: Timespan = Timespan.DAY,
    series_type: SeriesType = SeriesType.CLOSE, adjusted: bool = True,
    start: DateLike | None = None, end: DateLike | None = None,
    order: Order | None = None, limit: int | None = None,
    filters: Mapping[str, FilterValue] | None = None)
ema(...)   # same signature as sma
rsi(...)   # same signature as sma, but window: int = 14
macd(symbols, *, short_window: int = 12, long_window: int = 26, signal_window: int = 9,
     timespan: Timespan = Timespan.DAY,
     series_type: SeriesType = SeriesType.CLOSE, adjusted: bool = True,
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
`DatasetSpec.fetch_paths_by_class` ([section 13.4](#134-rest)). Every default of the
signature is the default Massive's spec gives the parameter (the windows 50, 50, 14, and 12,
26, 9; `timespan` day; `series_type` close; `adjusted` true). `adjusted=True`, the stock and
option bars' basis too, is sent where the chosen path declares `adjusted` and left out where
it does not: the crypto templates have no such parameter, and a pair has no splits to adjust
for. `adjusted=False` is sent where the path declares it and raises `ValueError` for a symbol
whose path does not (a crypto symbol), rather than being dropped silently. The indicator responses echo no `adjusted`, so their basis cannot
be checked ([section 12](#12-timestamp-units-and-day-boundaries), "Price adjustment").

Each row is one indicator value at one bar (`DatasetSpec.row_times` is `BAR`): `Date`,
`TimestampStart`, and `TimestampEnd` follow the bar rule of the query's timespan, in the day
zone of the symbol's class (UTC for forex and crypto, New York otherwise), and `poll()` keys on
`Symbol` and `TimestampStart`. A value is computed over the aggregates range's bars and carries
the stamp of its bar: Massive's samples show the underlying aggregates at 00:00 New York time
for stocks and 01:00 for indices. So a value of a day or longer is checked by the range's rule
for its symbols' class (`row_times.stamp_rule` with the class): 00:00 in the day zone, or
the indices' 01:00, on its period's first day. A value at any other stamp is served too,
labelled by the day its stamp falls on, and logs one WARNING per dataset and timespan per
session naming the dataset, the path, and the stamp (decision 70);
[section 23](#23-open-verification-items) item 61 checks it live.

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
| `benzinga` | `consensus_ratings(symbols, *, filters: Mapping[str, FilterValue] \| None = None)`; `Symbol` and `RawSymbol` are the row's `ticker`, as on the other Benzinga datasets, and there is no separate `Ticker` column | `/benzinga/v1/consensus-ratings/{t}` per symbol (the ticker is a path segment); `filters` reaches the path's `date` family |
| `benzinga` | `news(symbols=None, *, …T)`, axis `published` (an instant) | `/benzinga/v2/news` |
| `etf_global` | `analytics`, `constituents`, `fund_flows`, `profiles`, `taxonomies` — `(symbols=None, …F)` | `/etf-global/v1/{analytics,constituents,fund-flows,profiles,taxonomies}` |
| `tmx` | `corporate_events(symbols=None, …T)`, axis `date` | `/tmx/v1/corporate-events` |
| `fable` | `merchant_aggregates(…T)`, `merchant_hierarchy(…F)` | `/consumer-spending/eu/v1/{merchant-aggregates,merchant-hierarchy}` |

On the `etf_global` datasets `Symbol` is the fund the row describes (for `constituents`, the
ETF, not the holding). Each partner dataset's `poll()` key is a set of its own generated
columns that is unique per row, checked against the generated fields of its path.

### 8.11 `m.snapshots`

`m.snapshots(symbols, *, …F) -> Query` calls the unified `/v3/snapshot` with `ticker.any_of`,
250 tickers per request, any mix of asset classes. Rows are the union schema of the unified
snapshot. An options row also has `Underlying` (its `underlying_asset.ticker`, an optional
`fanout` key, since other classes' rows have none) and `OptionRoot` (`option_rows=True`), as
`m.options.snapshots` has; both are null on the rows of other classes. `ALL` is not accepted
here (the endpoint requires tickers). The dataset is named `snapshots`, the session method's
own name with no namespace (`DatasetSpec` takes one plain name for a dataset of the session
itself), and is declared in `_namespaces/snapshots.py` with `asset_class=ClassFromPrefix.ANY`
(each ticker's prefix selects its class, § 10), so it is in the catalog (`CATALOG` in
`_session.py`); `m.streams()`, messages, and the column reference show that name.

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
The generator knows every vendor extension it accepts, each with the reason in
`refresh/rest_sources.py`; an unknown one stops the refresh (`shape`).

**Coverage.** Everything documented is generated or excluded, and everything generated is
used or deferred:

- **documented = generated ⊎ excluded ⊎ (deprecated ∪ draft)**, tested against the
  pinned snapshot in `tests/massive/refresh/test_generate.py` (WebSocket channels and flat-file
  prefixes) and `tests/massive/refresh/test_rest_sources.py` (REST paths, and every path a
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
`FilterValue = str | int | float | bool | date | datetime | Sequence[str | int | float | date]`.
A key is the wire
name with its dot written as an underscore, so every key is a Python identifier:

```python
m.options.contracts(underlying="NVDA",
                    filters={"strike_price_gte": 100, "expiration_date_lte": "2026-01-16"})
# sends strike_price.gte=100&expiration_date.lte=2026-01-16
```

The mapping is mechanical and one to one: a key ending in `_gt`, `_gte`, `_lt`, `_lte`, or
`_any_of` is sent as `<field>.<suffix>` exactly when the spec lists that dotted parameter for the
path; every other key is sent unchanged. A sequence value of an `.any_of` parameter is joined
with commas: a `str` item as given, a `date` item of a list of dates (such as `date_any_of`) as
its ISO-8601 day, and an `int` or `float` item of a list of numbers (such as
`strike_price_any_of`) as the base parameter's number is written; an item of a kind the base
parameter does not take raises `ValueError`, and a `bool` is never a number; dates are ISO-8601; an aware `datetime` becomes the parameter's documented form (an exact
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
value is split into its comma-separated items, so `"a,b"` and `("a", "b")` are one value
and one query key (the wire joins a list with commas, so no item can hold one); an empty item
(`"a,,b"`) or one with surrounding whitespace (`"a, b"`) raises `ValueError` naming the item. It then
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
`expand_underlying=false` of the indicators) are reserved the same way. When Massive adds a filter,
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

`sort` is checked at construction, like a filter, against every REST path of the dataset that
takes it (`Catalog.check_sort`): where the spec lists the path's sort fields (an `ENUM`
`sort`, such as `ex_dividend_date` and `pay_date` on `/v3/reference/dividends`), `sort` must be
one of them; where the spec documents `sort` as a comma-separated list of columns, each with an
optional `.asc` or `.desc` suffix (a `STRING` `sort`, such as `period_end.desc` on the
financial statements), each part must name a response field of the path (a generated
`Field.wire_key`), which every default the spec gives such a `sort` does; a part with
surrounding whitespace (`"date, ticker"`) is refused like any other. A part that carries
its own suffix cannot be combined with `order`. Anything else raises `ValueError` listing the
fields the path takes.

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
  exhausted generator raises `ValueError` ("no symbols: the list is empty; ..."), naming the
  next step.
- **`symbols` is required on every market-data dataset, including every `snapshots()`.**
  `None` there raises `ValueError` when the query is built, naming the forms it takes and
  `dm.ALL` where the dataset accepts it. `ALL`
  is accepted by stream verbs (the `<channel>.*` topic), by every per-class `snapshots(ALL)`
  (full market, all pairs, all contracts), and by `history()` when a flat file or a grouped-daily
  REST path exists. Everywhere else `ALL` raises `ValueError` naming the alternative.
- **Canonical only.** Every symbol a dataset takes (`symbols=`, `underlying=`, every row of a
  symbol table) must be Massive's exact REST ticker, case kept: `AAPL`, `BRK.B`, `KIMpL` (Kimco's
  series L preferred: the lower-case `p` is meaningful), `O:BRKB250117C00400000`, `ESZ5`,
  `I:SPX`, `C:EURUSD`, `X:BTCUSD`. Anything else raises `ValueError` when the query is built
  (`check_symbol` in `_datasets/symbols.py`); the message names the canonical form when the
  class's converter gives one (`'EUR/USD' is not Massive's ticker; use 'C:EURUSD' (... and
  dm.canonical_symbol_forex converts)`). A canonical ticker of another class says which class
  the dataset takes (`'I:SPX' is an index ticker; this dataset takes stock tickers`). Every
  `Symbol` column holds exactly that form.
- **The grammar** of each class: a stock is an upper-case root of letters and digits followed
  by the CQS suffixes Massive keeps, `.X` (a class or another dotted suffix, `BRK.B`,
  `NTEST.Q`, one or more, as in the NYSE warrants `AAC.WS` and `NE.WS.A`), `p` and an optional series (`KIMpL`), and the markers `r` and `w`; an option is
  `O:` and an OCC symbol whose root is 1 to 6 letters or digits (no dot) and whose `YYMMDD`
  expiry is a real date; a single futures contract is a root of upper-case letters and digits,
  a month code, and a one- or two-digit year (`ESZ5`, `6CH5`, `0NGH39`; a year is never
  rewritten, and `ESZ25` is not `ESZ5`); a futures combo is the shape below; a futures dataset
  takes a contract or a combo, the same grammar `dm.canonical_symbol(text,
  dm.AssetClass.FUTURES)` checks, and refuses any other text (`'ESZ'`, `'esz5'`, `' ESZ5'`)
  with the converter's message. A futures row keeps any ticker as Massive writes it (not
  empty, no surrounding whitespace, no control character, no class prefix), so no row is lost
  to the grammar; an index is `I:` and letters and digits, as Massive's WebSocket pattern
  `^(I:[a-zA-Z0-9]+)$` writes it, with dotted parts allowed; a forex pair is `C:` and two
  three-letter ISO 4217 codes; a crypto pair is `X:` and the two codes joined, whatever the
  quote. A stream needs the WebSocket form (`BTC-USD`), and two REST paths take the pair as
  two codes (`/v1/last/crypto/{from}/{to}` and `/v1/open-close/crypto/{from}/{to}/{date}`), so
  the library splits a pair, never by a guess: the joined ticker does not say where its quote
  starts (`X:DOTUSD` could be `DOT-USD` or `DO-TUSD`; `X:ETHWBTC` ends in `BTC` but quotes in
  `WBTC`), so every split comes from Massive's list of crypto tickers
  (`/v3/reference/tickers?market=crypto`, every page, read with `active=true` and again with
  `active=false`, since the endpoint lists only traded tickers by default and a past date of a
  delisted pair needs its split too; a ticker in both keeps its listed row; each row's
  `base_currency_symbol` and `currency_symbol`, whose join must be the ticker); the library
  keeps no list of quote currencies of its own. The session reads that list once, on first
  need, through its REST client, and keeps it for its life; a failed read is not kept, so the
  next need reads it again. A stream needs it when it starts or when a symbol table adds a
  crypto ticker (on the session's `crypto pairs` worker, section 18.1). A failed read refuses the
  stream with the read's `MassiveError`, and a ticker the list does not hold, or whose
  currencies do not join to it, refuses it with a `MassiveError` naming the ticker; nothing
  falls back to a guess. On the two REST paths the same refusal raises from `fetch()`, and
  fails a `poll()` table, since every later cycle would read the same list. Every other REST
  path and every flat file needs no split: a flat-file filter looks for a crypto pair split
  at every point. Every grammar is
  matched against the whole text, so a trailing newline never passes. A dataset that spans classes
  (indicators, reference data, the unified `m.snapshots()`) declares the internal
  `ClassFromPrefix.ANY` in place of an `AssetClass`, never None: the prefix selects the
  grammar (`O:`, `I:`, `C:`, `X:`), a symbol without one is a stock, and its days are New
  York days. The symbol rules (`is_canonical`, `check_symbol`, `canonical_from_wire`,
  `wire_spellings`) and the day rules take `AssetClass | ClassFromPrefix`; `ClassFromPrefix`
  is not exported, since no public signature takes it.
- **Futures combos.** A combo is a spread, strip, pack, or bundle: Massive's contracts list
  returns them with `type: 'combo'`, by default. Massive documents no combo grammar, and its
  live lists show more forms than its samples (`1Y:SA 03M F6`, `10YF5-10YG5XXX` beside
  `GCQ6-GCZ6`, `BOB 02-01 U6`, `CL:BF Q6-U6-V6`, `6A:XF:AUDUSD:Q6`), so the forms are not
  enumerated. A combo is any run of upper-case letters and digits joined by single `-`, `:`,
  or spaces, exactly as written. The reason: every ticker Massive itself writes must
  round-trip, from a row to `symbols` and to a symbol table, while whitespace, lower case, an
  empty part, and a truncated contract (`ESZ`, one token with no year, so no combo) are
  garbage and refused. No function builds a combo ticker from its legs: Massive's grammar is
  unverified, so a builder waits for a live check
  ([section 23](#23-open-verification-items)).
- **The converters** are the public way to another spelling. `dm.canonical_symbol(text,
  asset_class, *, convention=SymbolConvention.MASSIVE)` takes a required `AssetClass`: the class is never guessed,
  so `SPX` becomes `I:SPX` only because the caller names the class; for `FUTURES` it takes a
  single contract or a combo, as strictly as `canonical_symbol_future` and
  `canonical_symbol_future_spread`, and refuses other text with `canonical_symbol_future`'s
  message. One function per class
  (`canonical_symbol_stock`, `_option`, `_future`, `_index`, `_forex`, `_crypto`), the spread
  function `canonical_symbol_future_spread`, and one per
  stock convention (`canonical_symbol_stock_yahoo`, `_nasdaq`, `_cms`) take only the text, for
  Deephaven query strings: a formula cannot pass a keyword argument, and imported by bare name
  (`from deephaven_massive import canonical_symbol_forex`) they make a `String` column and are
  vectorized; called through the module (`dm.canonical_symbol_forex(Pair)`) Deephaven types the
  column `Object`, so the docstrings say to cast `(String)`. Each maps None to None, returns
  canonical text unchanged, converts its spellings (options: no `O:`, the space-padded OSI form,
  any case; indices: no `I:`; forex: `EUR/USD`, `EUR-USD`, `EURUSD`, with or without `C:`, any
  case; crypto: `BTC-USD`, `BTC/USD`, `BTCUSD`, with or without `X:`, any case, any quote;
  futures: nothing), and raises `ValueError` with a hint for anything else.
  `canonical_symbol_future` refuses a spread naming `canonical_symbol_future_spread`, and the
  other way round. A ticker with another class's prefix names that class's function (`'C:EURUSD'
  is a forex ticker (prefix C:), not a crypto one; call dm.canonical_symbol_forex`). Stock,
  index, and futures text is never re-cased, and nothing is guessed: `aapl` raises saying that
  tickers are case-sensitive, with an upper-case root and possibly a lower-case suffix. The
  converters are pure and thread-safe.
- **Stock conventions.** `SymbolConvention.MASSIVE`, the default of every `convention`
  parameter and the only member every class takes, is Massive's own spelling: canonical text
  passes unchanged and a class's unambiguous spellings convert (`EUR/USD`, `SPX` for the
  indices class). A stock spelling converts only from a named stock convention (`YAHOO`,
  `NASDAQ`, `CMS`), because `-` means a class in Yahoo's and a preferred in Nasdaq's; under
  `MASSIVE`, `-`, `/`, and a space raise a hint naming the conventions, and a stock convention
  for another class raises `ValueError`. No `convention` parameter takes None. Each converts exactly its plain tickers, share
  classes, and preferreds, and refuses the rest, since Massive documents no spelling for
  warrants, units, rights, when-issued, when-distributed, called, or test issues:

  | Kind | `YAHOO` | `NASDAQ` (Integrated Platform) | `CMS` | Massive |
  |---|---|---|---|---|
  | Class | `BRK-B` | `BRK.B` | `BRK B` | `BRK.B` |
  | Preferred with a series | `BAC-PL` | `BAC-L` | `BAC PRL` | `BACpL` |
  | Preferred without a series | refused (`-P` also reads as class P) | `DCOM-` | `DCOM PR` | `DCOMp` |
  | Plain | `AAPL` | `AAPL` | `AAPL` | `AAPL` |

  Under a convention only its grammar applies: text that happens to be a Massive ticker is not
  passed through, since the convention may mean something else by it. A Yahoo ticker with a
  dot (`VOD.L`, `SHOP.TO`) is refused as a listing outside the US, and Nasdaq's `ABC.U` (class
  U) is refused rather than read as Massive's unit `ABC.U`. Class `U` is refused in every
  convention (CQS and CMS write units as `U`). CQS is not a member: it is Massive's own
  spelling of classes and preferreds. Nasdaq's ACT symbology is not
  supported: it writes `.W` for warrants and `.V` for both a class and when-issued.
- **`dm.canonicalize_symbols(table, asset_class, *, column="Symbol",
  convention=dm.SymbolConvention.MASSIVE, on_error=dm.OnError.RAISE)`**, in
  `_tables/symbols.py`, is the table form of the converters. It keeps its own `on_error`
  argument, apart from the session settings below, since it is a standalone function that
  needs no session. The table's
  current rows are converted in Python first (one call per distinct value), static or
  refreshing, so `RAISE` gives a clean `ValueError` listing up to five values; every table is
  then converted by `update`, so under `RAISE` a later bad row of a refreshing table fails the
  table. `OnError.LOG` writes a null symbol and logs each bad value once at WARNING, at most
  20 per table; `OnError.IGNORE` writes a null symbol and logs nothing above DEBUG. The
  result keeps every other column, and the converted column keeps its name.
- **A symbol table** must have a `Symbol` column, and every non-null row should be canonical.
  What a row that is not does follows `MassiveConfig.noncanonical_symbols`
  ([section 5](#5-massiveconfig)), an `OnError` whose default is `LOG`; a `str` or a sequence
  passed as `symbols` in code always raises, whatever the setting, since a typo in code is
  fixed in code.
  A refreshing table is reactive: streams listen to it and subscribe or unsubscribe as rows are
  added or removed; polls re-read it each cycle; `fetch()` and `history()` read it once. A
  static (non-refreshing) table is a fixed symbol list for the stream verbs: read once when the
  verb is called, as `fetch()` and `history()` read any table, it opens the same stream as the
  equal symbol list, and one holding no symbol raises `ValueError`. `poll()` re-reads it each
  cycle, which for a static table returns the same symbols every time. Every verb reads the
  table's current rows first (`read_symbols` in `_tables/watchlist.py`); a static verb raises
  `ValueError` for a table without symbols. A null row is ignored. The setting governs every
  row read from the table, present at the call or added later, for streams and `poll()` alike:
  - **`RAISE`**: a bad row present at the call raises `ValueError` at once. A **later** bad
    row is never subscribed or requested: it fails that result (the stream through its
    session's reaper, a `poll()` table with `SymbolRowError`) and logs at ERROR, with a message
    that names the row, the reason, and the fix: correct the row, or pass the table through
    `dm.canonicalize_symbols(table, dm.AssetClass.<CLASS>, on_error=dm.OnError.LOG)` (for a
    dataset that spans classes, `dm.canonical_symbol` per row with each row's class).
  - **`LOG`** and **`IGNORE`**: a bad row is skipped, never subscribed or requested, and the
    result keeps running on the good rows. `LOG` logs each distinct value once at WARNING
    (bounded, at most 100 per process), naming the row, the reason, and the fix; `IGNORE` logs
    it at DEBUG only. A stream's or a `poll()` table's `m.streams()` row lists the values
    skipped now in `Error` (`skipping 2 symbol-table rows that are not Massive's tickers:
    'aapl', 'EUR/USD'`, the count and the first five), and the text is cleared once none is
    left. Fixing or deleting a row clears it, and a fixed row subscribes (a stream) or is
    requested on the next cycle (a poll) like any added row. A table whose current rows are all
    bad (at least one row, and no good one) raises `ValueError` at the call rather than start a
    result that follows nothing; a later moment with only bad rows simply follows no symbol.
  A stream also refuses a row whose topic cannot be built. A symbol table that **fails** (a
  `canonicalize_symbols` table under `RAISE` that met a bad row) fails the result too: the
  watch's error callback queues the stream's failure on the reaper, and `read_symbols` refuses a failed table, so a
  `poll()` table fails on its next cycle. `last_by()` filters against the table itself, with
  `watched_rows`.
- **Datasets kept as written.** A dataset whose symbols are a third party's global tickers
  rather than Massive's US-market ones declares `DatasetSpec.symbols_as_written`: its
  `symbols`, the rows of its symbol table, and its rows' symbols are taken exactly as Massive
  writes them (not empty, no surrounding whitespace, no control character, no class prefix).
  Only `partners.tmx.corporate_events` does: it is Wall Street Horizon's global events calendar,
  whose events name their venue's MIC and an ISIN (`rest/partners/tmx/corporate-events.md`).
  Benzinga covers US-listed companies and ETF Global US-listed ETFs, so both keep the stock
  grammar; ETF Global's `ConstituentTicker`, which may name a foreign holding, is not a key and
  is never checked.
- **Wire spellings.** Massive writes some symbols differently per transport: `EUR/USD` in
  WebSocket forex frames, `EUR-USD` in forex topics, `BTC-USD` in crypto frames, `X:BTC-USD`
  in FMV frames, `X:EUR-USD` in the forex quotes flat-file sample. `canonical_from_wire` reads
  every spelling the library knows, without folding case, any crypto quote included; a futures
  row keeps its ticker as written; topics use `ws_symbol`
  (`C.EUR-USD`, as the WebSocket documentation gives every forex channel's `ticker`
  parameter). A flat-file filter holds every spelling of each requested symbol
  (`wire_spellings`): Massive's knowledge base says the crypto and forex files write
  `X:BTC-USD` and `C:EUR-USD`
  (https://massive.com/knowledge-base/article/why-doesnt-a-bar-from-the-rest-api-match-the-flat-files),
  and the forex quotes sample shows `X:EUR-USD`, so a request for `X:BTCUSD` or `C:EURUSD`
  matches those rows, which hold the canonical ticker; a crypto pair is looked for split at
  every point, since its joined ticker does not say where its quote starts.
- **The index prefix comes from the endpoint, never from the name.** A ticker without `I:` on
  a dataset of the indices class is read as `I:` and the ticker; on any other dataset it is a
  stock. No conversion looks a name up (`SPX` is never recognized as an index by its name).
  Which index endpoints send the bare form is a live check ([section 23](#23-open-verification-items)).
- **Unreadable wire spellings.** What a symbol Massive sends that `canonical_from_wire`
  cannot read does follows `MassiveConfig.unreadable_symbols` ([section 5](#5-massiveconfig)),
  an `OnError` whose default is `LOG`. Such a spelling is a gap in the library's grammar, so it
  is a library defect, never the user's. Under `LOG`, the default, the row is kept with a null
  `Symbol` and the text in the `RawSymbol` column, and each spelling is logged once at ERROR
  with the library-bug text (at most 100 per process), so every table keeps running and the
  gap is reported. `IGNORE` keeps the row the same way and logs it at DEBUG only. Under
  `RAISE` it fails, naming the spelling, the setting, and where to report the gap: a REST
  request raises `RowDataError` (a `poll()` table fails), a flat-file read raises
  `MassiveError`, and a WebSocket row fails every stream of the dataset on the channel (it
  cannot be routed to one, so no symbol stream may starve on it silently). Under `LOG` and
  `IGNORE` a WebSocket row with a null `Symbol` reaches only the `dm.ALL` streams of its
  channel, since a symbol stream cannot claim it. A `poll()` table keyed on `Symbol` keys on
  `Symbol` and `RawSymbol` together, so each unreadable spelling is its own row: it neither
  fails the null-key check nor merges with another, and `REPLACE` removes it when it leaves.
- `Symbol` is always the instrument the row describes. When the fan-out key is a different
  instrument, it is `Underlying`, exactly as Massive sends it: on the options rows that carry
  one (the chain and the snapshots from `underlying_asset.ticker`, the contracts from
  `underlying_ticker`; WebSocket and flat-file options rows carry none) and on
  `related_companies` rows. A non-instrument fan-out key
  keeps the generated column the dataset declares as its `fanout` key (`filer_cik` becomes
  `FilerCik`), declared with `KeySpec(..., instrument=False)`
  ([section 19.4](#194-generated-schemas)). Where a source does not carry the instrument in its rows (it takes the symbol
  as a path parameter, as `/v3/trades/{t}` and `/v2/aggs/ticker/{t}/…` do, or holds it outside
  the result key, as `/v1/last/crypto` and `/v1/last_quote` do), `Symbol` is filled from the
  request ([section 19.4](#194-generated-schemas)). A dataset whose rows describe no instrument
  (`reference.exchanges`, `economy.*`) has no `Symbol` column. Nor does a dataset whose rows
  each list several tickers, because no single instrument is the row's: the three financial
  statements, Forms 3 and 4, the 8-K disclosures, `reference.news`, and
  `partners.benzinga.news` keep their `Tickers` column (`String[]`), and `symbols` there only
  filters the request (`tickers.any_of` or `ticker`). `Symbol` is never a list.

## 11. Enums and types

**Python floor:** `requires-python = ">=3.11"`. `StrEnum`, `datetime.UTC`, `typing.Self`,
`tomllib`, and exception groups are native. The CI matrix is 3.11, 3.12, 3.13, 3.14, and 3.14t;
the free-threaded 3.14t job runs with `PYTHON_GIL=0` and is required. 3.14t is the only
free-threaded target: pyarrow, numpy, and pandas no longer publish cp313t wheels.

**Criterion.** Every closed set of values in a public signature is a `StrEnum` whose values are
Massive's wire strings; an enum of a library setting that never reaches the wire
(`HistorySource`, `SymbolConvention`, `OnError`, `ErrorCause`, and `PollMode.AUTO`) takes its
member names in lower case. Signatures take only the enum. There are no string `Literal`
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
| `PollMode` | AUTO, REPLACE, UPSERT, APPEND | `auto`, `replace`, `upsert`, `append` | `poll()`; `AUTO`, the default, is the dataset's own mode |
| `HistorySource` | AUTO, REST, FLAT_FILES | `auto`, `rest`, `flat_files` | `history()` |
| `FuturesVenue` | CME, CBOT, NYMEX, COMEX | `cme`, `cbot`, `nymex`, `comex` | futures factory methods (`venues`, a set); flat-file prefix only |
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
| `SymbolConvention` | MASSIVE, YAHOO, NASDAQ, CMS | `massive`, `yahoo`, `nasdaq`, `cms` | `canonical_symbol`, `canonicalize_symbols` (`convention`) |
| `OnError` | RAISE, LOG, IGNORE | `raise`, `log`, `ignore` | `canonicalize_symbols` (`on_error`), `MassiveConfig.noncanonical_symbols` and `unreadable_symbols`, and their `DEEPHAVEN_MASSIVE_*` variables |
| `ErrorCause` | EXPECTED, USER_DEFECT, LIBRARY_DEFECT | `expected`, `user_defect`, `library_defect` (no wire value; defined in `_errors.py`, beside the exception it describes) | `MassiveError.cause`, read only; it sets the level a failure is logged at |

Internal, not exported: `Recency {REALTIME, DELAYED, UNKNOWN}`, `ChannelFamily {TICK, FMV}`,
`ClassFromPrefix {ANY}`, and `NotBars {ANY}`, the key under which a dataset that is not bars
declares its one WebSocket channel and flat-file prefix (a bars dataset keys them by `Timespan`).
`Transport {REST, WS, FLAT_FILES}` is defined once, in `_massive.spec.types`, and used on both
sides of the boundary.

**Dates.** Parameters accept an ISO-8601 `str`, any date or time text Deephaven's own parser
reads (such as `"2025-03-03T09:30 America/New_York"` or `"2025-03-03T09:30:00 ET"`), a `date`,
a timezone-aware `datetime` (a `pandas.Timestamp` among them, its nanoseconds kept), the Java
time values Deephaven hands its users (`java.time.Instant` and `ZonedDateTime`, which are
instants, and `LocalDate`, a date: what `dh_now()` and `to_j_local_date()` return, and what
`Instant` and `LocalDate` cells give when read as Java values, through `iter_dict` for one;
`dh_today()` returns ISO text), or a `numpy.datetime64` (a date at day precision; otherwise an
instant in UTC, as Deephaven reads it and as `to_numpy` gives an `Instant` column, which logs
at DEBUG). This is the set Deephaven's own `deephaven.time` converters take, less a bare
integer, which Deephaven reads as epoch nanoseconds and which would silently misread a date
written as `20250303`, and text Deephaven reads only as such a count. Internal code may still
pass an integer it has already decoded, such as epoch nanoseconds, where an internal API takes
one; the public arguments refuse it. ISO-8601 text is read by `_datasets/dates.py`
(`parse_date_like`), exactly as written (text with surrounding whitespace raises `ValueError`
naming the trimmed text): first as a date in any ISO form (`"2025-03-03"`, `"20250303"`,
`"2025-W10-1"`), then as a date and time. Other text is read by Deephaven's parser at the API
layer (`_tables/time_text.py`, `deephaven_time_text`): first as a date (`"2025-3-3"`), so
date-only text stays a calendar date, then as a date and time with a zone or offset, never as an
epoch count; the Java value it gives then goes down like any other. Deephaven text that names a
day and a zone but no time (`"2025-03-03 ET"`), which Deephaven reads as that midnight, is
refused, since as an `end` it would drop the day. Text neither reads raises one `ValueError`
naming both forms, with Deephaven's own reason (such as `No matching time zone: EST`) and the
zones it reads. `parse_date_like` turns each value into a `date` or an aware `datetime`,
outside the wire package; it recognizes a Java value by its jpy class name, so it imports
neither Deephaven nor jpy. An instant with nanoseconds below the microsecond, from ISO or
Deephaven text, Java, numpy, or pandas, is moved outward to a microsecond: Deephaven's instants
carry nanoseconds the user never chose (`dh_now()` on Linux, a SIP `Timestamp`), and text
follows the same rule. A lower bound (`start`) is rounded down, so it still includes the row an instant
came from; an upper bound (`end`) is rounded up, so the half-open range may include up to
999 ns more but never loses a row, and an `end` past the last microsecond of year 9999 becomes
`datetime.max`. Each rounding logs at DEBUG with its result and the amount moved. A
`numpy.datetime64` of a week, month, or year raises `ValueError` naming a day instead: as an
`end` it would shrink the range to its first day, and a numpy week starts on a Thursday. A
`NaT` (numpy or pandas) and a time outside years 1 to 9999 raise `ValueError`, with the value
as the caller passed it. Type checkers see Deephaven's Java values as `Any`, so `DateLike` names
only `numpy.datetime64` beside the Python types. A naive `datetime` or `pandas.Timestamp`, or an
ISO string with a time but no offset, raises a `ValueError` that names
the parameter, the value, and the fix, for example `start=datetime.datetime(2025, 1, 2, 9, 30)
needs a timezone: pass an aware datetime or a date`. The same rule and message apply to a naive
`datetime` passed as a `filters` value or a typed parameter. A `filters` value given as ISO text
finer than a microsecond is rounded the same way, up for an upper bound (`.lt`, `.lte`) and
down otherwise. A range (`history()` and the `start`/`end` of a `…T`
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
Deephaven column type (`long`, `double`, `boolean`, `String`, `Instant`, `long[]`,
`String[]`; a calendar date is ISO `yyyy-MM-dd` text, a `String`).
Sizes come out as `double` wherever any source says `number`. Every numeric id or code is a
`long`, alone or in a list: condition codes and indicators are `long[]`, and exchange and tape
ids `long`, so codes join `m.reference.conditions` `Id` and ids `m.reference.exchanges` `Id`
with no cast. A list of text (an article's tickers, a filing's roles) is a `String[]`, which
`ungroup` splits and the query language's `inObj` tests
(`where("Tickers != null && inObj(`AAPL`, Tickers)")`, since `inObj` refuses a null list); any
other nested value (an object, a list of objects) is JSON text, and its description says so.

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
| WebSocket `LULD` `t` | **UNVERIFIED**: documented as Unix ms, but the sample (`1764086430905642800`) has 19 digits, which as ms would overflow; registered as ns. On a mismatch the overflow check or the bound drops every row, and the stream fails after `DROPPED_RUN_LIMIT` of them ([section 19.5](#195-runtime-drift)). |
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
3. **Bound.** The result must lie in [1990, 2100].

A value that fails step 2 or 3 is null and marked in the routine's mask (`masked_instants`),
and the text `"timestamp unit mismatch for <dataset>.<column>: <value> <unit> ..."`
(`mismatch_message`) names the dataset, the column, the unit, and the value. In the time
column it is a key that cannot be read, so the row is dropped and counted; in any other column
it is an unconvertible value, null with a WARNING ([section 19.5](#195-runtime-drift)).

The bound applies to epoch-integer `Instant`s only, never to ISO-8601 text, so a pre-1990 ISO
date stays valid. The overflow check and the bound together catch every unit confusion. For
FMV `t`, the magnitude rule above picks the unit per value first, then the routine runs.

**Examples of generated time column names:** `Timestamp`, `ParticipantTimestamp`,
`TrfTimestamp`, `BidTimestamp`, `AskTimestamp` (futures quotes), `ReceivedTimestamp` (crypto
trades and quotes), `LastUpdated` (snapshots). A dataset's declared keys rename the vendor's
primary times into the library's own columns ("Time columns" below), so the bars' `s`, `t`,
and `window_start` become `TimestampStart` and the WebSocket bars' `e` becomes `TimestampEnd`.
Every calendar date, the vendor's (`ExDividendDate`, the economy `Date`) and the library's
(`Date`), is ISO `yyyy-MM-dd` text, a `String`, so a date compares and joins as text. The
library stores one shared `String` per distinct date. The Arrow conversion makes one Java
`String` per row, so every dataset table is built through `_tables/columns.py` `dataset_table`:
the other columns convert from Arrow, and each date column is built from a process-wide pool of
one `String` per date, run by run (a copy and doublings per run of rows of one date), and joins
them as an in-memory column source, with no Deephaven `update()`, whose fixed cost of a few
milliseconds would fall on every stream flush. A column of interleaved dates (more than 4,096
runs) is interned by a formula instead. A million rows of five dates hold five `String`s, a
reference per row; the fixed cost per table is under a millisecond, and large batches build
faster than a plain Arrow conversion; nothing else may build a dataset table, and only date
columns, a small set, go into the pool.

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
window lies inside the range and has ended. One rule decides both, the bar's own `TimestampEnd`
("Time columns" below): a bar is kept only when its `TimestampStart` is no earlier than the
range's start and its `TimestampEnd` is no later than the earlier of now and the range's end.
The window carries these as `HistoryWindow.bars`, a `row_times.BarBounds` whose `start_rule` is
explicit: `StartRule.RANGE` checks the start, and `StartRule.LABEL`, for futures named by dates,
keeps bars by their session label instead, since a folded session opens before its date. Ticks
have no bar window (`bars` is None). A range of dates ends where its last day's bar would
(`dates.day_end`: the next midnight in the class day zone; 01:00 New York time for indices;
17:00 Chicago on the last session's date for futures); a range of datetimes at its end. The
transports drop a later bar once its time columns are filled (`row_times.within`), so selection
and the join key can never disagree: wherever the end is conservative, so is the selection. The
windows:

- An intraday bar (second, minute, hour) spans `multiplier` units from its `Timestamp`, on whole
  units of UTC time (whole hours in every zone the library uses).
- A 1-day bar is its calendar day in the class day zone (to 01:00 New York time after it for
  indices). A bar of `multiplier` days spans that many calendar days, weekends included, for
  every class but futures: the live check of [section 23](#23-open-verification-items) item 63
  found Massive's 2-day AAPL bars stamped on Friday, Sunday, and Tuesday, the Friday bar
  holding Friday's volume alone and the Sunday bar Monday's. Only stocks were observed;
  options and indices read the same aggregates endpoint and count the same way. A futures bar
  of several sessions counts weekdays, and with no holiday calendar it ends after the latest
  day its sessions can reach, `bar_periods.last_trading_day`: `n - 1` weekdays after its
  first day plus one weekday, and one more for every five further days
  (`add_weekdays(first, n + (n - 1) // 5)`). That allowance covers every run of the NYSE and
  CME calendars of 2024 to 2026 (a test checks every start date), the tightest being six
  sessions over eight weekdays from 2024-12-24. Datetime bounds therefore
  keep only whole calendar days of 1-day bars: Monday 09:30 to Friday 16:00 New York time
  returns Tuesday to Thursday, so daily bars are asked for with dates.
- Massive starts a week on Sunday, at 00:00 in the class day zone, and stamps the week's bar
  there: the live check of [section 23](#23-open-verification-items) item 33 found every crypto
  and forex week bar at Sunday 00:00 UTC (2026-10-11T00:00Z) and every stock week bar at Sunday
  00:00 New York time (04:00Z), each holding exactly the volume from that Sunday on. So every
  class's week is named by its Sunday. For stocks and options, which do not trade at weekends,
  a week's window is its Monday to Friday, and months, quarters, and years are calendar periods
  less their leading and trailing weekend days, so March 2024 runs from Friday the 1st to
  Friday the 29th. Since an index can update around the clock (`I:AAVE100`), its window is the
  whole calendar period, at its start and its end: an index week runs Sunday to Saturday and
  ends an hour after the next Sunday begins, so a range must reach that Saturday to keep it,
  and an index month of June 2024 starts on Saturday the 1st, so a range from Monday the 3rd
  leaves it out. Forex (which trades from Sunday evening UTC) and crypto (every day) keep their
  weekends too: Sunday-to-Saturday weeks and whole calendar months, quarters, and years. A bar
  of `multiplier` periods spans that many.
- A futures bar named by session dates is kept by its session label, Massive's
  `session_end_date` (the row's `Date` before a bar of several units has it set to null): an
  intraday bar when its session lies in the range; a session, week, month, quarter, or year bar
  when the whole period of session dates its label belongs to (Monday to Friday for a week;
  months less their weekend days) lies in the range. Whether Massive labels a bar of several
  units (`multiplier` above 1) with its first or its last period's session is not verified
  ([section 23](#23-open-verification-items) item 40), so such a bar is kept only when it fits
  under both readings: its label's period with the `multiplier - 1` periods after it, and with
  the `multiplier - 1` periods before it, all lie in the range (`bar_periods.bar_sessions`).
  Sessions count as weekdays (there is no holiday calendar). The bar must also have ended by its
  `TimestampEnd`, which for several sessions carries the same holiday allowance as the days
  above, so a range of 2025-02-03 to 02-07 keeps the 2-session bars opening Monday and Tuesday
  evening, but not the one opening Wednesday evening. A futures bar named by datetimes follows
  the intraday rule, or for longer bars the sessions of the period, from the regular 17:00
  Chicago open of its first session to that of the session after its last; its `window_start` is
  its true start whatever its label, so that rule needs no reading of the label.

A bar whose window has not ended yet is never returned, so the current day, week, month,
quarter, or year is left out whatever `end` says (a forex month that ends on a Sunday is
unfinished until that Sunday ends, and an index day bar until 01:00 New York time the next day);
for futures named by date, only finished sessions count (the sessions before the one the current
instant belongs to). There is no holiday calendar: a holiday is a weekday like any other, so a
week whose Friday is a holiday still runs Monday to Friday, and monthly stock bars for March
2024 need a range through Good Friday, 03-29. The REST aggregates' `{from}` is the first instant
that belongs to the first bar that fits (a week's Sunday, the 1st of a month even when that is
a weekend), the start of the kept range: whether Massive selects bars
by their stamp or builds them from `{from}`, the first whole period's bar is returned, and a
partial bucket begun before it is dropped. `{to}` is the last whole millisecond before the
range's end. Rows outside the window are then dropped.

**Day labels and stamps.** A futures bar's `TimestampStart` is Massive's `window_start`,
unchanged, and its session is its `session_end_date`. Every other bar of a day or longer, of
any multiplier, from every source (the aggregates range, grouped daily, `/prev`, and the
`day_aggs_v1` flat files), has its stamp moved to 00:00, in the class day zone, of
the day it falls on, so New York midnight for stocks, options, and indices and UTC midnight for
forex and crypto (`row_times.stamp_rule`, `row_times.day_starts`); its time columns are
then computed from that day ("Time columns" below). The classes stamped are exactly
those of `dates.DAY_ZONES`. One call then returns the same rows whichever source answers it.
Massive stamps the same bar differently by source, so each stamp is moved back to 00:00 of the
day it falls on. Bars are always served; an unexpected stamp warns, never refuses (decision 70).
A stamp Massive is not known to use is moved the same way, the best rule available, and since
such a bar may cover other days than its label, it logs one WARNING per dataset and timespan
per session (the session's once-per-session warnings) naming the dataset, the source, the
stamp, and the stamps expected, saying that the label may cover other days, and pointing to the
project's issues page. Every stamp, expected or not, is recorded the same way: the bar's
`TimestampStart` and `Date` come from the day it falls on. The known times are
wall-clock times of day in the day zone, compared to the millisecond, so a New York session
close is 16:00 in winter and in summer; the zone's offsets come from `zoneinfo`, one
conversion per distinct stamp, not from Arrow's time-zone database. A week, month, quarter, or
year bar is expected on the day its period starts, since the whole-window rule above places a
bar by its stamp's day; one stamped on another day is placed in the period that day falls in,
and warns:

| Class | 1-day bars (`multiplier` 1) | Longer bars: time (REST range only) | Week bars: day | Month, quarter, year bars: day |
|---|---|---|---|---|
| Stocks | 00:00 (range); 16:00, the session close (grouped daily, `/prev`, `day_aggs_v1`); 13:00, a short day's close | 00:00 | Sunday | the 1st |
| Options | 00:00 (range, `day_aggs_v1`); 16:00 (`/prev`); 13:00; 16:15 and 13:15, the closes of index options such as SPXW | 00:00 | Sunday | the 1st |
| Indices | 00:00; 01:00, which is midnight in Chicago (range, `day_aggs_v1`); 16:00 (`/prev`); 13:00 | 00:00; 01:00 | Sunday | the 1st |
| Forex | 00:00 (range); 23:59:59.999, the day's last millisecond (grouped daily, `/prev`, `day_aggs_v1`) | 00:00 | Sunday | the 1st |
| Crypto | 00:00; 23:59:59.999 (`day_aggs_v1`) | 00:00 | Sunday | the 1st |

The 1st is of a month for month bars, of January, April, July, or October for quarter bars,
and of January for year bars. A bar of several days may start on any day. A time known for one
class is not accepted for another, and every class's times apply to every source, since each
class accepts 00:00 anyway. Some times are presumed rather than sampled, and the live checks
of [section 23](#23-open-verification-items) items 50 to 52 confirm them:

- The indices longer bars' 01:00 follows the range's 1-day bars; no sample shows one.
- 13:00 for stocks, options, and indices is the close of a short trading day (the day after
  Thanksgiving, Christmas Eve, July 3). A source that stamps a day at its close may stamp such
  a day there, so the check accepts it rather than fail real data on every short day.
- 16:15 and 13:15 for options are the closes of index options on full and short days, where
  `/prev` may stamp such a contract.
- The week day is the Sunday Massive starts every week on, as the live check of item 33
  found for stocks, forex, and crypto; options and indices are presumed to follow the same
  aggregates. A week bar stamped on any other day is placed in the week that day falls in,
  and warns, since it may cover another week.

Every accepted time lies inside its own day in the zone, so none of them, on any date, changes
which day a bar is labelled with, and the whole-window rule keeps or drops a bar stamped at its
day's end exactly as it does the same bar stamped at the start. Crypto's day cut is not
verified: the documented range and `/prev` samples sit at midnight New York time and the
grouped-daily samples at times such as 20:48 UTC, no day boundary at all, so crypto daily REST
history, `dm.ALL` grouped daily, and `/prev` all warn if live REST data follows those samples,
until the owner decides after the live check ([section 23](#23-open-verification-items) item
1). `AUTO` picks the source as for every other dataset ([section 16](#16-historysourceauto)):
the stamp rule, not a fixed source, keeps both sources on the same days.

The WARNING names no other source or verb to use instead: every verb serves the bar, and the
WARNING asks for the stamp to be reported at the project's issue tracker so the rule can be
settled.

**Time columns.** Every market-data row says when it happened, which trading day it belongs to,
and when the library received it (`_datasets/row_times.py`). `DatasetSpec.row_times`
(`RowTimes`) says which columns a dataset has: `TICK` for every non-bar dataset in a kind
(trades, quotes, index values, FMV, LULD, imbalances), `BAR` for the bars and the indicators,
`DAY` for the daily open-close summaries, and `NONE` for the rest (snapshots, movers, reference,
partner, and economy data). A declaration's default is the explicit member `BY_KIND`, which
`DatasetSpec.times` resolves from the dataset's kind to one of the others. The columns come
first, in the order `Symbol`, `Date`, `Timestamp` or `TimestampStart`, `TimestampEnd`, except
`TimestampInsert`, which is last.

- **`Timestamp`** is a tick's event time, Massive's own. A `NONE` dataset keeps a `Timestamp`
  only where it declares a time key (`futures.schedules` and `futures.market_status` declare
  their vendor `timestamp` as one, since the name is the library's).
- **`TimestampStart`** is where a bar's window begins and **`TimestampEnd`** its exclusive end:
  the first instant at which the window is closed. Joins key on the end
  (`aj(t, bars, on=["Symbol", "Timestamp>=TimestampEnd"])`), so an as-of join never attaches a
  bar whose window is still open. The end is when the window closes, not when Massive published
  the bar, and Massive may correct a bar afterwards. Bars have no plain `Timestamp`.
- **`Date`** is the trading day, ISO `yyyy-MM-dd` text, stored as a column (not computed on
  view): on every tick, bar, and daily-summary table, null on any bar that spans more than one
  date. A `NONE` dataset has no library `Date` (snapshots included); a vendor field named
  `date` there keeps its own column.
- **`TimestampInsert`** is the local clock (`time.time_ns()`) when the REST page, flat-file
  block, or WebSocket frame that carried the row arrived, the same for every row of one, on
  every dataset and every transport. It is not monotonic and is not Massive's `ReceivedTimestamp`;
  on `history()` rows it is the download time, and in a keyed `poll()` table the latest poll's.
  It is never part of a row's identity.

| Rows | `TimestampStart` | `TimestampEnd` | `Date` |
|---|---|---|---|
| Tick | (none: `Timestamp`) | (none) | the day zone's date of `Timestamp`; futures: `session_end_date` from REST and flat files, the WebSocket rule below on streams |
| Intraday bar, n seconds, minutes, or hours | Massive's start | start + n units; a futures one-second WebSocket bar: the later of that and Massive's `e` | the start's date, null when the bar ends on a later date (it crosses midnight); futures: as ticks, null when the bar crosses 17:00 Chicago time |
| 1-day bar | 00:00 of the day | 00:00 of the next day; indices 01:00 New York time (`dates.day_end`) | the day |
| n-day, week, month, quarter, year bar | 00:00 of the window's first day: for stocks and options a week's Monday and a month's, quarter's, or year's first weekday; for indices, forex, and crypto its first calendar day (a week's Sunday) | the end of the window's last day: n days count calendar days, weekends included, as Massive counts them; a stock or option period's last weekday; an index period's last calendar day, as an index can update around the clock; indices one hour later | null |
| Futures session bar | Massive's `window_start` | 17:00 Chicago time on its session date | `session_end_date` |
| Futures bar of several sessions | `window_start` | 17:00 Chicago time on the latest day its sessions can end on, counted from its start's session date plus one session (a folded first session opens before its holiday), with the holiday allowance (`bar_periods.last_trading_day`), or on its label if that is later | null |
| Futures week, month, quarter, year bar | `window_start` | 17:00 Chicago time on the last weekday of the latest period its label allows | null |
| Daily open-close | 00:00 of its date | as a 1-day bar | its date (`from`, crypto `day`) |
| Indicator value | the bar rule of the query's timespan, in the day zone of the symbol's class; a value of a day or longer is stamp-checked as that class's range bars | the same | the same |

The ends of the index bars and of the futures bars of several periods are conservative: an index
day bar is stamped at midnight in Chicago, and whether it covers the New York or the Chicago day
is not verified ([section 23](#23-open-verification-items) item 52); Massive does not document
whether a futures bar of several sessions, or a week or longer, is labelled with its first or
its last session (item 40), so its end is the later of the two readings, and a holiday can move
a session later. A 1-day index bar keeps its `Date` although its end is an hour into the next
day. A stock, option, or index bar of several days counts calendar days, as Massive does
([section 23](#23-open-verification-items) item 63), so a 2-day bar stamped on a Friday ends at
00:00 on Sunday, and the next one, stamped on that Sunday, at 00:00 on Tuesday. A futures bar
of several sessions counts its sessions with the holiday allowance, from the session date of its
`window_start`, plus one session: a first session a holiday folded opens on the evening before
the holiday, so that date is the holiday itself (Christmas 2024 and 2025 for 5-session bars from
12-26). The fixed four-day margin it replaces fell a full session short for a 17-session bar
across Christmas, New Year, and Martin Luther King Day. An index week, month, quarter, or year
ends after the calendar period's last day, since an index such as `I:AAVE100` updates at
weekends too. The next midnight is always the calendar's next midnight in the zone, never 24
hours later, so a day across a daylight-saving change keeps its true length. The library
computes day boundaries with `zoneinfo`, once per distinct UTC day of a batch, and applies them
with Arrow; Arrow's own time-zone database is not used.

The vendor's own time fields feed these columns and are not kept beside them: the bar start
(`t`, `s`, `window_start`) becomes `TimestampStart` (the `time` key), the WebSocket bars' `e`
becomes `TimestampEnd` (the `end` key, recomputed), and the futures `session_end_date` and the
open-close `from` and `day` become `Date` (the `date` key). Grouped daily's `{date}` needs no
column: a 1-day bar's `Date` is the day its stamp falls on, which is that date.

Joins on `Date`: a daily bar joined to another daily bar on `Date`, across symbols
(`aapl.natural_join(spy, on=["Date"], joins=["SpyClose=Close"])`), is sound. Joining an
intraday row to the same day's daily bar attaches values not known until that day ended
(`minute.natural_join(daily, on=["Symbol", "Date"], joins=["DayClose=Close"])` is lookahead).
A null `Date` matches another null in a join, so longer bars join on `TimestampEnd`. The
descriptions of `Date` and `TimestampEnd` say so.

**Futures dates on the WebSocket.** Massive's REST and flat-file futures rows carry their
session; WebSocket futures messages carry none. The flush dates each streamed futures row by the
rule the library uses everywhere, the session that ends at the next 17:00 Chicago time
(`futures_sessions.futures_session` and `futures_sessions.weekday_on_or_after`, one constant,
`FUTURES_SESSION_ROLL`, whose column forms `session_days` and `weekday_sessions` sit beside
them, and a test checks the two forms agree; a Saturday's or Sunday's moves to Monday), and corrects it with the futures schedule calendar
(`_ingest/futures_calendar.py`, `FuturesCalendar`, which reads the paths of the
`futures.contracts` and `futures.schedules` datasets), since the rule is wrong after a holiday,
which folds its trading into the next session and opens it days early, and for a product whose
pre-open starts before 17:00 Chicago time (16:45 on CBOT). The calendar looks up each contract's
product (`/futures/v1/contracts`, `ticker.any_of`) and each product's sessions from a week back
to ten days ahead (`/futures/v1/schedules`, on every futures plan, records from 2025-03-17): one
session per `session_end_date`, open from its first event (a pre-open or the open) to its last
(the close). A row inside the session the rule gave it keeps it; a row the schedules place in
another session moves there; a row between a close and the next session's first event keeps the
rule's date. The calendar belongs to the connection group, since its connections serve every
session of the group: it is created with the group's first futures connection and lives until
the group's last session leaves. One worker thread makes every request, through the REST client
of any session of the group still open (the contracts and schedules are the same for every
session of the key), when a futures stream opens with explicit symbols, when a reactive table adds some, and
when an unknown contract first streams (a `dm.ALL` stream's contracts are looked up as they
arrive); the flush never waits on it. Only a single contract ticker is looked up, by the futures
grammar of the symbols module (`symbols.is_futures_contract`); a spread or combination
(`GCQ6-GCJ7`, `BOB 02-01 U6`, `BZ:BF F7-G7-H7`, `6A:XF:AUDUSD:Q6` in the vendor's files) is
never looked up: Massive documents no grammar that names a combo's legs, so its schedule
cannot be told from its ticker. Its rows keep the rule's date, which a holiday fold can make wrong, so the
first rows of each spread log one WARNING. What is still loading keeps the rule's date silently
for up to a minute (`LOAD_PATIENCE`), so a stream's start shows no error; a contract or product
still not loaded a minute after it was first asked for (a stalled or rate-limited lookup) is a
miss, "its schedule was not loaded after 60 s". A row the loaded schedules cannot place (past
the last loaded session, a contract Massive's contracts do not list, a failed lookup) is a
**miss** too: it keeps the rule's date, the miss is logged at WARNING once per product (or
contract) and date, and the `Error` of every stream that receives the contract shows it in
`m.streams()` until a later batch of the stream's rows all place; the stream keeps running. An
answer the calendar cannot read, or any other failure of a lookup, is a miss of its contracts'
rows too, and the worker carries on. A miss past the loaded sessions asks for a reload of its
product, at most once per 10 minutes (`RELOAD_SPACING`, so a stream of misses never spends a
Basic key's 5 requests a minute), and a product's sessions are reloaded once a day as the dates
move on. Subscribing never waits on the calendar and never refuses a product it lacks.

**Price adjustment.** Massive adjusts stock and option prices and volumes for splits (forward
and reverse; whether it also adjusts for `stock_dividend` events is
[section 23](#23-open-verification-items) item 56) when a request says `adjusted=true`, and never
for cash dividends. Its flat files are "unadjusted… not adjusted for stock splits, dividends,
or other corporate actions" (`vendor/massive/flat-files/stocks/overview.md`) and carry no
factor; its WebSocket bars are as traded and never re-adjusted. Index, forex, crypto, and
futures bars have no corporate actions and take no `adjusted`.

- **One basis per `history()`.** With `adjusted=True`, the default of the stock and option bars
  and daily open-close, a call is split-adjusted, as Massive stands at the time of the call,
  and served by REST only: `AUTO` never chooses flat files for adjusted bars
  (`_flat_file_problems`), and an explicit `FLAT_FILES` raises. With `adjusted=False` it is as
  traded, from REST or the flat files. One call never splits its range across sources
  ([section 16](#16-historysourceauto)).
- **Every page is checked.** The aggregates (range, grouped daily, `/prev`) echo
  `adjusted` on every response, `next_url` pages included (`Page.adjusted`). A page whose echo
  differs from the request raises `MassiveError` naming both; a page without one logs a
  WARNING once per dataset, path, and session. The indicators and daily open-close echo
  nothing, so their basis cannot be checked; the indicators' `adjusted=None` default sends
  nothing, and Massive's default is adjusted.
- **What stays open, documented in every place below.** `bars(dm.ALL, MINUTE).history()` with
  the default `adjusted=True` raises, since the flat files serve only `adjusted=False` and REST
  needs explicit symbols; pass `adjusted=False`. A download of many requests that runs while
  Massive applies a split overnight can mix bases, which nothing can detect. Live bars are as
  traded, so `merge([history, live])` of adjusted history joins its live part seamlessly until
  the next split's execution date (pre-market included: "on the execution date, all trading is
  already adjusted"), and from then on the history is on the old basis: build it again after a
  split. The jump at a split in a live table, or in such a merge, is the data, not an error.
- **Adjusting as-traded data yourself.** The splits dataset gives each split's cumulative
  `HistoricalAdjustmentFactor`; Massive's rule is "for a price on date D, find the first split
  whose `execution_date` is after date D and multiply the unadjusted price by the
  `historical_adjustment_factor`". Since `Date` and `ExecutionDate` are both ISO text, a
  reverse as-of join does it: `bars.raj(splits, on=["Symbol", "Date<ExecutionDate"],
  joins=["F=HistoricalAdjustmentFactor"])`, then `Close * F` and `Volume / F`, a null `F` (no
  later split) meaning 1. A bar with a null `Date` (several days or longer) finds no match.
- **Where it is documented.** `bars()` of stocks and options says all of it, and the other
  classes' `bars()` say they are never adjusted; the `adjusted` argument; `history()` (the
  guarantee and what stays open); the stream verbs (the jumps at splits); `splits()` (the
  recipe); the description of every price and volume column of `stocks.bars`, `options.bars`,
  and both daily open-close datasets, through a library note (`DatasetSpec.column_notes`), so
  `columns()`, the tooltips, and the column reference show it; and the README's "Split
  adjustment" section, whose examples run in `tests/test_docs.py`.

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
    three days early) to 18:00 CT on the last session date, and kept by their label as
    above. There is no probe for earlier sessions.
  - **Datetimes** are exact: `timestamp.gte`/`.lt` for trades and quotes, `window_start.gte`/
    `.lt` for bars, with the bars kept by their own window afterwards, since Massive may
    match a candle by its start.
  - **Flat files** are read one file per session date (**UNVERIFIED** that a file holds only
    its own session: the futures flat-file overview says only "standardized in Central Time").
    Each row keeps its own `session_end_date` in `Date`, whichever file it came from, so no row
    is ever labeled with a session it does not have; a call named by dates keeps the rows whose
    own session it requested, and one named by datetimes keeps rows by time. A file that holds
    rows of another session logs one WARNING per dataset per session, naming the file and the
    session, and saying that rows of a requested session may then sit in a neighbouring day's
    file, which a call by dates does not read, and be missing. The call then names those
    sessions on the table: when a file held an earlier session's rows, the range's last
    session may be incomplete, and when one held a later session's, its first; the table's
    description and a WARNING name them (`FlatFileRead.uncertain`); a row whose `session_end_date` is empty or does not parse is dropped and counted
    ([section 19.5](#195-runtime-drift)). Dates read one file per session date and never roll forward:
    a date names its own session exactly. Datetimes read the sessions from that of `start` to
    that of `end - 1 µs`, so an intraday window reads one file and an evening window only the
    next session's file. Under `AUTO` with REST able to serve the request, a missing weekday
    session is read from REST, whose instants hold a holiday's hours
    ([section 16](#16-historysourceauto)). Otherwise, when the run of missing files that ends
    such a range (per venue) holds a weekday (a holiday, alone or followed by a weekend, such
    as Good Friday and the Saturday after it), the next existing session file within 7 days of
    the run's first day is read too, because the holiday's hours lie in it; its rows are
    filtered on `Timestamp` like the others. A run of weekend days after an existing Friday
    file reads nothing more. When no later session file exists yet (before the current
    session, within 7 days), the run's first missing weekday may not be published yet, and the
    call raises: the range's last hours may lie there, so it never returns a silently short
    table.

**Long downloads.** Grouped daily (`dm.ALL` 1-day bars over REST) makes one request per day,
skipping the days without trading (`dates.is_trading_day`): Saturdays and Sundays for stocks,
options, and indices, Saturdays for forex, which trades from Sunday evening to Friday evening
UTC; crypto keeps every day.
A per-session or grouped-daily download of more than five requests first logs one INFO line
with its number of requests and, when its subscription is not paced, the minutes it would take
were the subscription on the Basic tier (the first five go at once); a paced subscription's
line gives the least time the pacing makes it take ([section 13.4](#134-rest)). A per-session
download counts one request per session and symbol, more where a session spans several pages,
and its line says so.

## 13. Host resolution and connection protocol

### 13.1 Invariants

1. One connection per `wss://host/path` per API key and connection settings in a process,
   shared by every session that has them ([section 6](#6-massivesession-and-lifecycle)). A
   class may therefore have up to two connections: one for the TICK family and one for the
   FMV family. (Business accounts have three connection slots per cluster; individual plans
   have no FMV.) A shared connection is reference-counted by the streams of every session on
   it: closing one session releases only that session's streams and never closes a connection
   another session leases. Three consequences are by design:
   - The options cap of [section 15](#15-options-contract-cap) is the connection's, so the
     sessions on one `/options` connection share its 1,000 topics, and a static symbol list
     can raise `ValueError` because of another session's streams; the message counts the
     topics in use.
   - A refusal or a fatal failure acts on the connection, not on a session: a refused topic
     fails every stream that wants it, in every session, and a rejected key or a fatal
     handshake error fails every stream on the connection, in every session.
   - The server's real-time verdict moves the connection with every session's streams, and is
     recorded in each such session's host decisions. The group keeps it by (class, family), so
     a session of the group that never leased the moved connection takes the delayed host for
     its first stream of the class too, records the verdict then, and never asks the
     real-time host again.
   Diagnostics stay per session: each session's `m.connections()` shows the rows of the
   connections its streams use, and its `m.streams()` only its own streams.
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
the REST call and completes the future, and later callers wait on it outside every lock, each
within its own patience (section 13.4, Giving up). A failed resolution completes the future
with the error and removes it, so nothing is cached; every waiting caller raises a copy of it.
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
          -> raise EntitlementError("<class> plan has no WebSocket access: ...")   # Basic plans
             (the message says the refused request was the library's own host check, names
             its path, and quotes Massive's text)
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
(`tests/ingest/test_host_decision_integration.py`, skipped without a key) verifies the host.

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
reconnecting with backoff, and closing. `_ingest/ws_connection.py` owns everything the library
decides: the
desired topics, the acknowledgements, the watchdogs, the routing, and the connection's
diagnostics. Both run on the client's loop thread; other threads reach a connection only through
`loop.call_soon_threadsafe` with plain data, and through `WsClient.close()`.

The client reports to `_ingest/ws_connection.py` through four callbacks, each called on the loop thread, in order:

- `on_open(generation)`: Massive accepted the key (`auth_success`) on a new socket. A
  **generation** is one authenticated socket; the number increases by one per `on_open`.
- `on_status(generation, status)`: one decoded status object (`Status`: `kind` `ACK`,
  `REFUSED`, `MAX_CONNECTIONS`, or `OTHER`; `topics`; `message`; `realtime_verdict`).
- `on_data(generation, channel, rows, rejects, drift)`: one channel's data from one frame,
  decoded against `ws.json`. `rows` are keyed by **long-form** column (the library renames
  them), with symbols as the wire spells them (`EUR/USD`, `BTC-USD`). `rejects` are the rows
  dropped because a required key is absent, null, or does not fit, one
  `Reject(channel, symbol or None, WireError, raw)` each, whose `symbol` is the raw wire value
  of the channel's declared symbol column and `raw` the whole row's JSON text (up to 2,000
  characters); `drift` is the frame's `DriftReport` ([section 19.5](#195-runtime-drift)).
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
generation re-sends the whole desired set. `reconnect()` closes the current socket so the client
reconnects under its backoff rules. Rows from `on_data` go to the per-channel row buffer;
rejects never enter a buffer and fail no stream: they are counted in the frame's drift, which
goes to the flush thread, which reports it to the session's `DriftRegistry` and counts the
drops toward `DROPPED_RUN_LIMIT` ([section 19.5](#195-runtime-drift)).

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
| Decode frames against `ws.json`; report rows, rejects (the rows whose key cannot be read), and drift | client |
| Read raw wire symbols as canonical symbols (`WireSymbols` in `_datasets/wire_symbols.py`) when routing rows; fail or keep an unreadable one (`unreadable_symbols`) | library |
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
   -> topic state NO_ACK; WARNING logged; the first NO_ACK of this topic on the cluster's
      connections calls client.reconnect(), so the backoff rules apply; a later NO_ACK of the same topic only
      logs at WARNING and counts in NoAckTopics. The topic stays desired; streams never fail
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
has not ended. `m.close()` closes only the connections no other session leases; on a shared
one it removes the session's streams (their routes at once, their topics through the loop) and
leaves the connection to the other sessions. Removing the routes also breaks the cycle between
Python and Java that a stream's publisher, its shutdown callback, and the connection's routes
would otherwise form. A connection's loop-confined state is never touched from another thread:
the client's terminal state (`CLOSED` or `FAILED`), its last callback on the loop, clears the
connection's streams, and a call posted after it is dropped or ignored, so a post that lands
just before the loop closes cannot leave a stream behind. The flush thread clears the routes
when it ends, after its final flush, which also lets go of the streams a verdict moved off the
connection.

**Waiting for a slot.** While a connection is `WAITING_SLOT`, every stream on it shows in its
`Error`: "waiting for a WebSocket slot on <host><path>: another connection with this API key,
perhaps from another process, holds it; the plan allows one connection per cluster". Sessions
in one process share connections, so the other holder is another process, or a session whose
connection settings differ. The text is cleared, and an options-cap warning shown again, once
the connection authenticates.

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
then resolves to `DELAYED`, this happens at most once per (class, family) per session. A step can
be refused more than once: a step of more than 1,000 topics goes out in several subscribe frames,
and each frame may be refused. A later verdict for a step whose connection is already moving is
part of that move: it neither starts another move nor fails the streams. In every other case the
verdict is an `EntitlementError` for that step's streams.

`publish_failure` is used only for `AuthenticationError`, `EntitlementError` (an explicit server
`error` for the stream's topic, or REST 403 for `poll()`), a fatal WebSocket handshake error
(`MassiveError` with the HTTP status and host), and the data failures of
[section 19.5](#195-runtime-drift) (a run of `DROPPED_RUN_LIMIT` dropped rows, a symbol
spelling the library cannot read under `RAISE`). `max_connections`, closes, and `NO_ACK` are
diagnostic states.

### 13.4 REST

One host, one REST client per session ([section 19.1](#191-the-rest-client)), shared by every
caller thread, poller, and fan-out worker. Every REST dataset reaches its paths through it. The
REST transport turns each `WireError` into a public exception with `_errors.from_wire`, which
keeps the message and fills `status` and `request_id` ([section 3](#3-exports)). What a user
sees:

- **HTTP 401** raises `AuthenticationError`. **HTTP 403**, and a 200 response whose envelope
  `status` is `NOT_AUTHORIZED`, raise `EntitlementError` from `fetch()` and `history()` and fail
  the table from `poll()`.
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
  request of a request series drops that series, with one WARNING per symbol and session (the
  aggregates and tick endpoints answer an unknown ticker with an empty 200 instead, and a flat
  file simply has no rows for it). "First request" means each series's own: futures trades and
  quotes named by dates are one series per symbol and session, so a 404 on a later session,
  after earlier sessions returned rows, drops only that session, with its own WARNING ("ESH5
  has no data for the session 2025-01-22"), since a session can have no data. HTTP 404 on a
  later page of one series means its data was cut short and raises `MassiveError` naming the
  symbol, the path, and the pages received. Every other error fails the call.
- **`poll()` cycles.** A cycle that raises `EntitlementError` or `AuthenticationError`, hits
  a data failure of [section 19.5](#195-runtime-drift) (a page whose every row was dropped,
  or a null or NaN `poll_keys` value, in every mode), or plans a request with a parameter the
  chosen path does not take (a reactive symbol table's new symbol routed to a path that lacks
  an explicit parameter; retrying cannot fix it) fails the table through `publish_failure`.
  `poll()` checks synchronously whatever does not depend on the symbols: with static symbols
  every path, with a reactive table the parameters against the dataset's fetch paths, at
  least one of which must take every one of them. An emptied symbol table plans no request.
  Any other error sets the stream's `Error` in `m.streams()`, keeps the previous rows, and the
  next cycle runs as scheduled. The first failing cycle of a run of failures logs one WARNING
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
  ([section 12](#12-timestamp-units-and-day-boundaries), "Price adjustment").
- **Time columns.** Every page's rows get the library's time columns (`row_times.TimeRule`),
  with `TimestampInsert` the instant the page arrived, after the stamps are checked and the rows
  outside the window dropped. The day zone of each call follows its own symbols' class, so a
  crypto indicator's `Date` is a UTC date and a stock indicator's a New York date.
- Every error message names the HTTP status, the path, and the response's `request_id`, and is
  logged with the same text at the level its cause gives ([section 17.1](#171-the-log)); the API
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
  bucket, whatever their filters: Massive's knowledge base ("What is the request limit for
  Massive's RESTful APIs?", vendored as
  `vendor/massive/knowledge-base/what-is-the-request-limit-for-massives-restful-apis.md`)
  says reference and cross-asset endpoints are counted in a bucket of their own. So `m.reference.tickers(market=dm.Market.CRYPTO)` goes to `other`, as does
  the session's own read of Massive's crypto ticker list, its pages in one lane of their own.
  `other` cannot be declared, since no setting names it, but it is paced like a subscription
  once a 429 names the per-minute limit.
- **Sharing.** One `RequestGovernor` ([section 19.1](#191-the-rest-client)) per (SHA-256 of the
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
  subscription names what the Basic tier would cost ([section 12](#12-timestamp-units-and-day-boundaries)).
  A `poll()` whose cycle makes more paced requests than its interval allows (each request
  holding its slot 61 s) logs one WARNING for the table ("each cycle makes 3 stocks requests,
  but they are paced at 5 per minute, so a cycle takes at least 37 s, longer than the 30 s
  interval; cycles run late").
- **Diagnostics.** `m.rate_limits()` shows every bucket ([section 17](#17-diagnostics)).

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
  instant that belongs to the first bar whose whole window fits the range (a week's Sunday, the
  1st of a month) and `{to}` the last whole millisecond before the
  range's end ([section 12](#12-timestamp-units-and-day-boundaries)); the bars that do not fit
  wholly, or have not ended, are dropped afterwards.
  `futures.bars`, whose path `/futures/v1/aggs/{ticker}` has no such segments, declares the
  instant pair `window_start.gte`/`window_start.lt` instead.
- **Futures sessions.** Futures trades and quotes named by session dates are requested one
  weekday session at a time, `session_end_date=<date>` with no time window; futures bars named
  by session dates are requested on `window_start` and kept by their session label
  ([section 12](#12-timestamp-units-and-day-boundaries)). Grouped daily skips weekends for the
  classes that do not trade then. A long download is announced once, at INFO, before the
  first request (the rate limits below).
- **Day stamps.** Bars of a day or longer from `/prev`, the range, and grouped daily, of every
  class but futures, start at 00:00 of their day in the class day zone: every stamp is moved
  there, and one Massive is not known to use for the class logs one WARNING per dataset and
  timespan per session naming the path and the stamp
  ([section 12](#12-timestamp-units-and-day-boundaries), decision 70). Bars are stamped before
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
  ([section 10](#10-symbols-and-all)).
- **Error rows in multi-ticker snapshots.** A snapshot of several tickers answers a ticker it
  cannot serve (`NOT_FOUND`, `NOT_ENTITLED`) with a row carrying `error` and `message`.
  `snapshots()` keeps such rows, with their `Error` and `Message` columns, and logs each
  ticker once.

## 14. Publishers, fan-out, and failure semantics

- **One stream per unique (dataset, channel, symbol key).** The symbol key is the sorted tuple
  of canonical symbols, `ALL`, or `id(table)` for a reactive table. A second verb on the same
  query, or an equal query, reuses the stream: `m.stocks.quotes(s).last_by()` twice costs one
  publisher and two derived tables. A futures `venue` is not part of the key.
- **Reactive symbol tables** are held strongly by the stream and managed in the stream scope;
  the stream's record also holds the watchlist listener strongly until the stream shuts down,
  and a failed watchlist fails its stream ([section 6.1](#61-how-a-live-table-is-kept-alive-and-released)).
  Two Python wrappers of one Java table have different ids and do not share a stream.
- **Library threads never create refreshing tables outside an explicit scope.** Refreshing
  tables are created only on the caller thread inside a result scope or a stream scope, and in
  the session's diagnostics scope. The flush thread creates only static tables.
- Each stream owns one
  `blink_publisher(name=f"massive.{dataset}#{stream_id}", columns=<dataset schema>, on_shutdown=..., defer=<the session's reaper>)`
  (a `table_publisher` whose failure pins its table, section 6.1), created inside the stream scope under the session's captured `ExecutionContext` by
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
  (`rename` in `_datasets/source_columns.py`), converts them (`_datasets/timestamps.py`), builds one
  Arrow batch in the dataset schema, and fills its time columns (`row_times.TimeRule`), each
  row's `TimestampInsert` the instant its frame arrived (each frame is buffered with that
  instant) and a futures row's `Date` corrected by the connection group's schedule calendar
  ([section 12](#12-timestamp-units-and-day-boundaries)); a schedule miss sets the `Error` of
  the streams that receive the contract and fails nothing. The timestamp bound is applied as a vectorized mask: a row whose time column fails it is dropped
  from the batch and counted, and any other value that fails it is null
  ([section 19.5](#195-runtime-drift)); neither fails a stream. Rows the client dropped never
  enter a buffer. The batch is converted once with
  `deephaven.arrow.to_table`: one Python-to-Arrow-to-Java conversion per channel per flush.
  Then, for each stream subscribed to that channel: `ALL` streams get `publisher.add(batch)`;
  symbol-list streams get the part of the batch they hold. A symbol stream that holds none of
  the batch's distinct symbols gets nothing, and one that holds all of them (and the batch has
  no row without a valid symbol) gets the whole batch with no filter, so the common
  single-stream case costs one `add`. One that holds exactly one of them gets that symbol's
  rows from `batch.partition_by("Symbol")`, built once per flush the first time a stream needs
  it and shared by every stream of the flush (`SharedBatch`), when the batch holds at least
  5,000 rows and at least 8 of the flush's streams take one symbol each. A filter scans the
  whole batch (about 0.25 ms at 2,000 rows, 1.5 ms at 20,000), while the partition costs work
  for each distinct symbol of the batch and then about 0.3 ms per stream, so below either
  threshold each such stream filters; a non-gating benchmark records both paths. Every other
  stream that holds some of them gets
  `publisher.add(batch.where_in(stream.filter_table, "Symbol"))`, which keeps arrival order
  across its symbols, where `filter_table` is a cached static one-column table rebuilt only when
  the stream's symbol set changes. Target: Python work is O(rows) per channel per flush plus a
  set intersection per stream; Java work is at most one partition per flush, which grows with
  the batch's distinct symbols, plus one lookup or filter and one `add` per stream, with no
  Python per-row cost. A non-gating
  benchmark test records warmed median rows per second for 1, 10, and 100 streams at 200,
  2,000, and 20,000 rows per flush, against one-pass per-stream Python grouping. The default
  run records only 10 streams at 2,000 rows; the other points are marked `benchmark` and
  deselected by default, and `-m benchmark` runs them.
- Each flush applies the failures queued before it, then publishes, so a failed stream gets no
  row after its failure; rows buffered for it in that flush are dropped. A batch that cannot
  be converted or published fails that dataset's streams on the channel, with a `MassiveError`
  naming the cause, and the flush goes on with the other channels.
- `TablePublisher.add` is safe from the flush thread: the Java side snapshots the static batch
  and guarantees non-interleaved adds. `on_flush_callback` is not used (it blocks the update
  cycle).
- After its `add` calls, each flush reports once to each session whose streams it published:
  `SessionCore.update_streams({stream_id: StreamUpdate(rows_published=..., ...)})`, where
  `StreamUpdate` is a frozen keyword-only record whose fields left as None keep their values.
  One short critical section under `registry_lock`, about 20 times a second per connection at
  the default `flush_interval`; it updates the rows of every stream still in the registry
  (writing a row only once the stream has opened), ignores ids that have left it, and never
  changes `State` (only `fail_stream` does). An `error` there is a warning, and
  `StreamUpdate(clear_error=True)` sets `Error` back to null, for example once the options
  contract cap no longer defers anything. Neither touches the `Error` of a `FAILED` or `CLOSED`
  row: the cause of a stop is final, so a flush or a poll cycle that races `close()` or a
  failure never replaces it. `poll()` rows take their `StreamId` from the same
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
  identities only once the cycle is published; `UPSERT` is `blink.last_by(poll_keys)`, which
  always has a column to aggregate, since every schema ends with `TimestampInsert` and no
  dataset keys on it (so `reference.related_companies`, keyed on every vendor column, keeps
  each distinct row with the time of the latest poll that returned it); `REPLACE`
  is that `last_by` filtered on a hidden boolean column, to which each cycle adds one tombstone
  row for every key the previous cycle had and this one lacks, then dropped. Deephaven keys no
  aggregation on an array column, so when a poll key is a list (`long[]` or `String[]`, the
  13-F `OtherManagers`), the `last_by` keys on a hidden text column in its place, the JSON text
  of the row's list keys, which the poller writes beside every row and tombstone and the result
  drops (a DEBUG line names the keys the table uses); the poller's identities hold a list as
  a tuple. A list key compares item by item and in order, as Deephaven compares arrays: the
  same items in another order are another key, since the library never reorders what Massive
  sends. A cycle is one `add`,
  so its rows and its removals land in one update. The `last_by` state keeps a hidden row for
  every key ever seen, the trade-off `last_by()` streams already accept: a `REPLACE` poll over a
  churning key set (an options chain, the day's movers) grows with the keys it has held. The
  `poll()` docstring says so. The poller holds the session core only weakly; the poll's record
  in the session's registry keeps the session alive while the poll runs. The poller runs until
  the poll's blink table shuts down (nothing uses the table any more), it fails, or the session
  closes, which fails the table.

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
              and the dataset has a prefix (futures: one per venue of `venues`, all four by default)
              and (not bars, or ((adjusted is False or the dataset has no adjusted parameter)
                                  and (multiplier, timespan) in {(1, MINUTE), (1, DAY)}
                                      or, for futures, (1, SESSION)))
REST possible iff the dataset has a REST history endpoint
              and (symbols is not ALL or (bars and (1, DAY)))        # grouped daily
none possible -> ValueError listing, per source, the violated constraint
one possible  -> use it
both          -> S3 if symbols is ALL or len(symbols) >= flat_file_symbol_threshold, else REST
AUTO on S3, then, when REST is possible:
  S3 refuses the files (403) or the S3 credentials -> the whole call from REST, one WARNING
  each trading day whose file is missing -> that day from REST, clipped to the range, one WARNING
FLAT_FILES, or AUTO with REST not possible:
  a refusal -> raise
  a missing trading day with a later day's file -> a day without trading, skipped with a WARNING
  a missing trading day with no later file -> raise ("not published yet")
```

**AUTO serves from whichever source can; it errors only when none can** (decision 78).
Massive says a day's files land "by approximately 11:00 AM ET the following day", and the time
varies: weekday files landed at about 00:45 New York time in the live check of 2026-10-30, but
Friday's on Saturday at 10:27 and 11:17. So the library never guesses publication from the
clock, and has no publish cutoff setting. When REST can serve the request under `AUTO`,
nothing is guessed at all: every trading day whose flat file is missing is read from REST, one
day at a time, as a one-day read clipped to the call's range (for datetimes, the day's
instants within the range; for futures, its session's hours), and its rows are merged in date
order with the files' rows. No row of that day is kept from the files (for futures, of any
venue), so no day comes from both sources. A day REST returns no rows for was a day without
trading. Sending everything after a gap to REST instead would turn one holiday into a REST read
of the rest of a long range. A missing weekday with files on both sides is therefore never
skipped silently: if Massive missed it, REST serves it. One WARNING per call names the days REST
served and the columns REST leaves null that flat files fill, and a blocking read's table
description names REST and those days (`Read from flat files, and from REST for 2025-03-04,
whose files are missing.`); a background read's description is written before the read, so it
says that a day the flat files cannot serve is read from REST, and the WARNING names the days.
Only under `FLAT_FILES`, or when REST cannot serve the request, does the flat-file read infer:
Massive writes a day's file after the day ends, in date order, so a missing trading day's file
with a later day's file after it (in the range, or looked for after it, trading days only, up
to 7 days past the first missing one and never as far as the current day, whose file cannot
exist) is a day without trading, skipped with a WARNING, and one with no later file may simply
not be published yet: the call raises `MassiveError("<dataset>: the flat file for <date> is not
published yet. Massive usually publishes a day's flat files by about 11:00 ET the next day,
but the time varies. Retry later, or read REST with source=dm.HistorySource.REST")` (or, when
REST cannot serve it, `Retry later; REST cannot serve this read: <reason>`). Likewise, when S3
refuses the flat files a read needs (HTTP 403: the plan lacks them) or rejects the S3
credentials under `AUTO` and REST can serve the request, the whole call is read from REST,
with one WARNING naming the refusal; under `FLAT_FILES`, or when REST cannot
serve the request, the refusal is raised. When neither source can serve the request, the
`ValueError` lists every violated constraint per source; for split-adjusted bars of `dm.ALL`
other than 1-day bars, it says exactly why (only REST serves adjusted bars, and REST reads
them one symbol at a time, while flat files hold as-traded prices) and what to do: pass
`adjusted=False` for the as-traded flat files, or name the symbols. Futures files are named by
session date, and their day ends at the next midnight in America/Chicago, where Massive
standardizes futures data. `indices.values` is flat-file only.
`indices.bars`, `forex.bars`, and `crypto.bars` have no `adjusted`, so the predicate is satisfied.

The choice covers the whole range, except for the days whose flat files are missing, which
`AUTO` reads from REST as above. The partition dates are those of the rows the call keeps
([section 12](#12-timestamp-units-and-day-boundaries)): for bars, the days of the bars whose
whole window fits, so the unfinished current day never needs a file. Bars of a day or longer
follow the same rule as every other dataset: the two sources may serve one call or the next by
symbol count, and the stamp check of
[section 12](#12-timestamp-units-and-day-boundaries) stamps their bars alike and warns on a stamp
it does not know, so the choice never changes which day a bar is labelled with. If live REST
crypto bars turn out to be cut at midnight New York time, every REST crypto daily call
warns (the morning before yesterday's file is published included), and the owner
decides the source rule after that live check ([section 23](#23-open-verification-items)
item 1). `source=` selects either source explicitly. A dataset without instruments counts as `ALL`.
`HistoryQuery.history()` makes the choice (`choose_source` in `_datasets/history_source.py`) and then calls one
transport; the transports only do I/O. The REST predicate depends only on the dataset's REST
capabilities (`DatasetSpec.has_rest_history`, `has_grouped_daily`), and the `adjusted` check on
the typed `Request.adjusted`.

An `AUTO` call logs its choice and the reason once, for example
`stocks.trades history: AUTO chose REST (2 symbols < flat_file_symbol_threshold 50)`.

**The source is never silent.** The two sources of one dataset do not always fill the same
columns: a REST history endpoint and a flat file can each carry fields the other lacks (REST
trades have `DecimalSize`; futures files have `Exchange`, and futures trade files
`Correction`), and a column a source does not carry is null. Under `AUTO` the source changes
with the symbol count, so the same query could return a column filled for many symbols and
null for few. `choose_source` therefore classifies the source it passed over (`Alternative`):
`AVAILABLE` when it can serve the request; `UNAVAILABLE` when it cannot (no S3 credentials, no
file or endpoint for the request, adjusted bars, `dm.ALL` over REST). Unless the other source is
`UNAVAILABLE`, the query compares the columns of the chosen source's history sources (REST: the
history and grouped-daily paths; flat files: every prefix of the dataset) with the other
source's, by the generated schemas (`Catalog.history_columns`). When the other source fills
columns the chosen one leaves null, it logs one WARNING per session, dataset, and set of such
columns (`SessionCore.warnings`), naming them, the choice and its reason, and what fills them:
`stocks.trades history: AUTO read flat files (60 symbols >= flat_file_symbol_threshold 50); the
read leaves DecimalSize null, which the other source, REST, fills: pass
source=dm.HistorySource.REST to get them (logged once per session)`. An `UNAVAILABLE` alternative means the result cannot change
and the caller cannot pick the other source, so nothing is logged, as with an explicit
`source=`; a range no row can lie in reads nothing and logs nothing either. Every `history()`
table records its source in its table description ([section 7.4](#74-column-descriptions)),
after the summary: `stocks.trades: Trades of US-listed stocks, tick by tick. Read from REST:
AUTO chose it (2 symbols < flat_file_symbol_threshold 50).`, or `Read from flat files:
requested with source=...`. The flat-file refusal of adjusted bars reads `adjusted=True needs
REST, since flat-file bars are unadjusted (as traded); pass adjusted=False for the flat files`,
so the description says why an adjusted read used REST. A range no row can lie in returns its
empty table before any source is read, with the summary alone. Each
transport then logs its own work: the flat-file transport `source=S3 files=<n> skipped=<dates>
unpublished_from=<date>` (`none` for either when nothing was missing or unpublished), the REST
transport `source=REST requests=<n>`.
Missing keys on non-trading dates are skipped. A trading date is a weekday, a forex Sunday, or
any crypto day, by the one test `dates.is_trading_day` that
the REST transport's weekend skipping uses too. A missing key on a trading date is read from
REST under `AUTO` when REST can serve the request, and otherwise judged as above (there is no
holiday calendar). A futures range named by datetimes that ends on a holiday reads the next
session's file in the inferring case, as [section 12](#12-timestamp-units-and-day-boundaries)
describes, and REST's hours of the holiday otherwise. A range in which no file exists and none
follows raises under `FLAT_FILES` (or reads REST under `AUTO`) rather than return an empty
table, and also logs the WARNING that the bucket, prefix, or venue may be wrong. A futures
venue whose file of a day is missing sends that day of every venue to REST; a partial series
of venues is not handled separately.
A missing file is a 404 whose body is S3's XML error with the code `NoSuchKey`. Any other
404 (another S3 code, no code, or a body that is not S3's, such as the HTML "Massive 404 Not
Found" page Massive's front end serves for a path it does not route, which is what a wrong
`s3_bucket` meets) raises `MassiveError` naming `s3://<bucket>/<key> at <endpoint>` and adding
"check MassiveConfig s3_bucket and s3_endpoint_url": `cannot read s3://flatfile/<key> at
https://files.massive.com: HTTP 404 (404: Not Found); check MassiveConfig s3_bucket and
s3_endpoint_url`. A connection that drops part-way through a file's body makes the read start
that file again from its first byte, at most `MAX_RETRIES` (3) more times, after the waits of
the shared retry schedule (0.5, 1, 2 s plus jitter). The rows already read from that file are
discarded, and freed before the wait, which is safe because no row leaves a file's read before
the file ends. The last failure raises `MassiveError` naming the file. Futures `venues`
reads one prefix per venue per date, four by default; a 403 on any prefix raises
`EntitlementError` naming the venue and the `venues=` parameter. A 403 whose S3 error code says the credentials themselves are wrong
(`InvalidAccessKeyId`, `SignatureDoesNotMatch`) raises `AuthenticationError` instead, for every
asset class, naming `s3_access_key_id` and `s3_secret_access_key`.

Flat files are read through `_massive.FlatFileClient` and `FlatFile`
([section 19.3](#193-the-flat-file-client)); `_ingest/flatfiles.py` builds each file's `ReadPlan`.
`pyarrow.csv.open_csv` reads each `.csv.gz` key in 1 MiB blocks (gzip only: every documented
key is `.csv.gz`). Columns are matched **by name, never by position**: the reader first reads
the header line, parses it with the `csv` module (quotes and `\r\n` included), and passes those
names as `ReadOptions(column_names=…)`, then sets
`include_columns` to the documented headers with `include_missing_columns=True`, so a reordered
file reads identically, an extra header is reported as drift and ignored, a missing header
becomes a null column of its declared type, and a missing declared key header raises at once
([section 19.5](#195-runtime-drift)). The CSV reader reads every header as text, and only an
empty field is null: `NA` and `NULL` are text, since both are valid tickers. Each block then
goes through pure Arrow operations in this order: the `Symbol` filter
(`pyarrow.compute.is_in`); the key check, which drops and counts a row whose key header is
empty or does not parse, or whose time the plan's converter cannot place, and fails the file
after its last block when it dropped every row of the requested symbols; the window's half-open `Timestamp` range in nanoseconds (for bars, the
starts of the bars whose whole window fits; not applied to futures trades, quotes, and session
bars named by date, which `_ingest/flatfiles.py` keeps by each row's own session); and, on the
surviving rows only, the conversion of each header to its column's type (epoch integers for
`Instant`s), the parsing of the list columns (`Conditions`, `Indicators`), where a value that
does not parse is null and reported, and the rename to long-form names. `_ingest/flatfiles.py` then applies
`rename` in `_datasets/source_columns.py` and the `_datasets/timestamps.py` routine, and moves each
1-day bar's stamp to 00:00 of its day in the class day zone, warning on a stamp it does not know
(`row_times.day_starts`). No block is converted
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
`m.close()` releases. `m.connections()` and `m.streams()` each return a new view of one of
them, built in its own result scope and tracked like any live result: while the view is held it
keeps the session alive and current, and after `close()` it keeps showing the final rows. They
are written by one diagnostics writer thread per session, which
applies immutable row snapshots from a queue in order; other threads only enqueue, never wait on
the update graph for diagnostics, and never write from UG callbacks. Each time it wakes, the
writer takes everything queued, keeps the latest row per key, and applies it with one `add` per
table, so the queue stays bounded however fast rows arrive.

**`m.connections()`**: one row per connection plus one `Transport=REST` row per resolved
(class, family).

| Column | Meaning |
|---|---|
| `Transport` | `WebSocket` or `REST`: the spelling `columns()` and the column reference use for transports |
| `AssetClass` | enum value |
| `Family` | `TICK` or `FMV` |
| `Host`, `Path` | resolved host and cluster path |
| `Recency` | `REALTIME`, `DELAYED`, `UNKNOWN` |
| `Evidence` | the evidence string of [section 13.2](#132-host-decision) |
| `State` | `RESOLVING`, `CONNECTING`, `CONNECTED`, `WAITING_SLOT`, `RECONNECTING`, `FAILED`, `CLOSED` |
| `Topics`, `AckedTopics`, `PendingTopics`, `NoAckTopics` | counts; rewritten after every frame of acknowledgements and refusals and every `NO_ACK`, also while a subscription step is in flight |
| `BufferedRows` | WebSocket connections only (0 on `REST` rows): rows received that no flush of the connection has taken yet; every family row of a connection shows the same count. The row is rewritten at least once a second while the count is above zero. Rows are never dropped; above `BACKLOG_WARNING_ROWS` (1,000,000) a WARNING says the flushes are falling behind, logged again only after the backlog has fallen below half of it |
| `Reconnects` | count |
| `LastMessageAt`, `NextReconnectAt`, `UpdatedAt` | `Instant`; `NextReconnectAt` is set only while the connection waits for its next attempt (`RECONNECTING`, `WAITING_SLOT`) |
| `LastError` | text; for a REST failure it names the HTTP status, the path, and the `request_id` |
| `SchemaDrift` | text; WebSocket connections only: the drift seen on the connection, as `channel:field+` (extra) and `channel:field-` (missing), sorted, truncated at 200 characters |

**`m.streams()`**: one row per stream, per `poll()` table, and per background read
(`wait=False`), told apart by `Kind`. A row is written when the stream has opened and on
every change until the stream leaves the registry (its holds released and its blink table shut
down), so a released stream still kept alive by a derived table keeps reporting
`RowsPublished` and a late failure; its last row (`RELEASED`, or `FAILED` if the stream failed)
stays in the table while the session is open. At `m.close()` every stream
still in the registry is written `CLOSED`, with the close's error text, and then
the diagnostics scope is released; a view obtained before the close keeps showing those rows.

| Column | Meaning |
|---|---|
| `StreamId` | key; a read's is its call id |
| `Kind` | `stream` (a WebSocket stream), `poll` (a `poll()` table), or `read` (a background `fetch()` or `history()`) |
| `Dataset` | `"stocks.trades"` |
| `Query` | the `repr` of the query whose verb opened the stream or `poll()`, the call that built it, such as `m.stocks.trades(['AAPL', 'MSFT'])`, so two streams of one dataset can be told apart, truncated at 200 characters like `SchemaDrift`; a stream reused by an equal query keeps the opener's text |
| `SymbolCount`, `Symbols` | count and a truncated list |
| `HeldResults` | results currently held by the library (result records not yet released) |
| `Deferred` | options contracts deferred by the cap |
| `Transport`, `Host`, `Path` | `Transport` is `WebSocket` for a stream, `REST` for a `poll()`, and for a read the source it reads, `REST` or `flat files`; a read's `Host` and `Path` are null |
| `RowsPublished` | count; a read's is set once, when its rows land |
| `RequestsPerCycle` | `poll()` fan-out |
| `State` | `RUNNING` (a stream or poll), `LOADING` (a read still downloading), `DONE` (a read whose rows have landed), `FAILED`, `RELEASED`, `CLOSED` |
| `Error` | text; also a futures stream's schedule miss, a warning that leaves it `RUNNING` ([section 12](#12-timestamp-units-and-day-boundaries)) |
| `CreatedAt` | `Instant` |
| `SchemaDrift` | text: the stream's drift, `poll()` rows included, as `transport:field+` (extra) and `transport:field-` (missing), with the transport spelled as in `Transport` (`WebSocket:zz+`), sorted, truncated at 200 characters |

**`m.rate_limits()`**: one row per REST rate-limit bucket ([section 13.4](#134-rest)), keyed by
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

`RELEASED` means "no library-held results; still publishing until the blink table dies"
([section 6.1](#61-how-a-live-table-is-kept-alive-and-released)). `FAILED` can follow `RUNNING`
or `RELEASED`; a failed stream's row keeps `FAILED` when its last result is released. `CLOSED`
is final: no later failure replaces it. A read's `RELEASED` means its result was released
while it was loading; it becomes `DONE` if a derived table kept it going until its rows landed.
`DONE` is final like `FAILED`: neither a release nor `m.close()` changes it.

`Error` also carries warnings that do not fail the stream: the options cap's deferral
([section 15](#15-options-contract-cap)) and the wait for a WebSocket slot
([section 13.3](#133-connection-and-subscription-protocol)).

Symbol problems show in `Error` with their own text. Under `noncanonical_symbols=LOG` or
`IGNORE` (`LOG` is the default), the rows of a stream's or poll's symbol table that are skipped
because they are not Massive's exact tickers are listed as a warning (`skipping 1 symbol-table
value that is not Massive's ticker: 'msft'; fix or delete that row`), cleared once none is
left. That text is kept apart from the transport's own warning (a wait for a connection slot,
deferred options contracts, a poll cycle's error), and `Error` shows both, joined by `; `, so
neither hides or clears the other. Under `RAISE`, such a
later row fails the result (`a symbol table holds 'msft' (...). Fix the row, or pass the table
through dm.canonicalize_symbols(table, dm.AssetClass.STOCKS, on_error=dm.OnError.LOG), ...`),
and under `unreadable_symbols=RAISE` a symbol Massive sent in a spelling the library cannot
read does (`stocks.trades: Massive sent the symbol 'BRK/B', ... This is a bug in
deephaven-massive. ...`). Under `unreadable_symbols=LOG` an unreadable spelling is logged once
at ERROR and its rows keep the text in `RawSymbol`; it is not shown here, since a WebSocket row
with no symbol belongs to no single symbol stream.

`SchemaDrift` is separate from `Error` and `LastError`, so drift never overwrites a failure. The
session's `DriftRegistry` collects it ([section 19.5](#195-runtime-drift)). Static verbs
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
  `MassiveSession` is created ([section 5](#5-massiveconfig)). Not at import: a script that
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
`poll-<stream id> rest_<n>`, `crypto pairs`, and the call pools below. The threads of a
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
([section 19.2](#192-the-websocket-client)). The planted-secret test
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
- Each `DriftReport` carries a sample per drifted key ([section 19.5](#195-runtime-drift)).

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
  running count ([section 19.5](#195-runtime-drift)); it fails no stream.
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
  calendar, so the message says a holiday explains it); a stream backlog over its threshold; a
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

## 18. Concurrency model

Free-threaded CPython (`PYTHON_GIL=0`) is a gating target. The library's locking must be correct
without the GIL.

### 18.1 Threads

| Thread | Created by | Does | Must not |
|---|---|---|---|
| caller (console, script, user thread); a console command holds the UG's exclusive lock throughout | user | verbs; the host-resolution REST call; Deephaven table creation inside the result scope; registry mutation; waits, outside every library lock and only when it does not hold the UG lock, for another thread's open of the same stream key; fails a new stream whose connection failed while it opened (`publish_failure`, outside every library lock) | hold the registry lock while calling Deephaven or waiting; while holding the UG lock, wait for an update cycle or on a thread that may need the lock (the diagnostics writer, a poller, another caller's stream open), or join a closing connection |
| WS client, one per connection | library | `asyncio.run` of the connection's `WsClient` (section 19.2): socket, handshake, frames, backoff; decode each frame once against `ws.json`; route status events, rejects, and drift; append rows to per-channel `SimpleQueue`s; the reconcile loop and watchdogs; idle close | any JVM call; blocking waits |
| flush, one per connection | library | every `flush_interval`: drain queues, Arrow to Java once per channel, `where_in` and `publisher.add` per stream, `publish_failure` of failed streams; enqueue diagnostics row snapshots | touch loop-owned state directly |
| verdict move, one short-lived thread per move of a connection to the `delayed.` host (§ 13.3) | library | `RestTransport.record_verdict`; creating the new connection's client (the first TLS context of a certificate setting loads from disk); moving the streams' leases under the connection group's lock; posting the hand-over to the old connection's loop | block a loop thread; call Deephaven |
| futures schedules, one per connection group with a futures stream (`FuturesCalendar`) | library | REST requests of `/futures/v1/contracts` and `/futures/v1/schedules` through the REST client of any session of the group still open, charged to the `futures` bucket, each lookup in a lane of its own that the calendar's close ends; swaps in each new map whole under its leaf lock; closed when the group's last session leaves | call Deephaven; hold its lock while it waits on the network |
| poller, one per `poll()` table, plus a `ThreadPoolExecutor(max_concurrency)` for fan-out | library | REST calls, waiting in the governor for a slot or a pause (ended by its stop token); `Table.snapshot()` of a reactive symbol table; `publisher.add` | run inside a listener |
| background read, one per `fetch(wait=False)` or `history(..., wait=False)`, ended when the read lands, fails, or is cancelled | library | the transport call `wait=True` makes (REST requests, with their fan-out pool, or the flat-file reader pool), listening to the read's stop token; the static conversion; `publisher.add` of one row that holds the static table, or `publish_failure`, under the read's publish lock; `SessionCore.finish_read` | hold the UG lock; take `registry_lock` across a Deephaven call |
| REST fan-out, a `ThreadPoolExecutor(min(max_concurrency, calls))` per static read that makes several calls (`fetch()` per symbol, REST `history()`), ended before the read returns | library | one call each: its pages through the session's `RestClient`, waiting in the governor for a slot or a pause; each page first checks the read's `Cancel` token, which the read sets on its first failure or an interrupt, so the other calls stop after the page already on the wire | take `registry_lock`; call Deephaven; outlive the read |
| flat-file reader, a `ThreadPoolExecutor(min(max_concurrency, keys))` per `history()` call, ended before the call returns | library | S3 requests through the session's `FlatFileClient`; Arrow CSV parsing (Arrow's own I/O threads read ahead from the response stream); `deephaven.arrow.to_table` of static tables, inside the session's execution context; checks the call's cancel event and the client's closed flag before each request and after each block | take `registry_lock`; outlive the call |
| reaper, one per session | library | scope releases (`release_scope`, under the UG shared lock, outside every library lock): results, streams, and failure pins; stream held-result counts; for a session freed without `close()`, the session's closers, which close the connections no other session leases and wait up to `JOIN_TIMEOUT` (5 s) for each one's threads; stopping a shut-down stream's watchlist listener; failing a stream whose watchlist failed; usually the final release of a core that left the live set | anything else |
| diagnostics writer, one per session | library | applies row snapshots from its queue to the `connections()` and `streams()` input tables: each batch coalesced to the latest row per key, one `add` per table; pulls the `rate_limits()` rows from the session's governor when told it changed, and when its queue wait times out at the rows' next time-driven change; at exit, releases the tables' scope under the UG shared lock | anything else |
| crypto pairs, one per session, started by the first crypto watchlist change | library | reads Massive's crypto ticker list (REST requests, once per session, charged to the `currencies` bucket) for each crypto ticker a symbol table adds that the session's connection group has not looked up (the group keeps the pairs, vendor facts shared by its sessions); then posts the change to the connection's loop, or fails the stream (`Flusher.fail`); changes run one at a time, in order | hold `registry_lock` or the group's lock across the request; call Deephaven |
| UG thread (Deephaven) | Deephaven | watchlist listeners (`listen`): compute symbol adds and removes and hand them to a worker; a watchlist row that is not canonical, or a failed watchlist, queues the stream's failure on the reaper | Deephaven table operations; blocking I/O; waiting on library threads |
| Java-originated (shutdown callback, UI export release, liveness cleanup) | Deephaven | `SessionCore.stream_shut_down` and `SessionCore.worker_shut_down` (under the registry lock: unmap the key; delete the record, or mark it for the last hold to delete); queuing work on the reaper; `call_soon_threadsafe` | sends; JVM calls; joins; blocking |

Every thread and pool the library creates is named `massive #<session> <role>`, or
`massive group #<group> <role>` for a connection group's, so each log line names its session or
group and its call or stream ([section 17.1](#171-the-log)); `tests/test_layout.py`
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
  - the live set of session cores ([section 6.1](#61-how-a-live-table-is-kept-alive-and-released)),
    taken only under `registry_lock`;
  - the process's registry of shared connection groups, taken to find or create a group when
    a session opens its first stream and to drop it when its last session leaves;
  - the REST transport's host-decision cache, which holds one future per (class, family);
    callers wait on a future outside every lock (§ 13.2);
  - `FlatFileClient`'s lock, one per session, which guards the creation and the close of the
    boto3 S3 client (creation reads local AWS configuration but makes no request, since the
    credentials are given); its closed flag is a `threading.Event`, read without the lock by
    the reader threads. The only lock taken under it is the model-loader lock below, while the
    client is built. The session's `SessionClients` creates the `FlatFileClient` on the
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
registry deadlock. Verbs called on the UG thread raise `ValueError`.

**A caller holding the UG lock never waits on a thread that may need the UG lock or an update
cycle** (the diagnostics writer, a poller, another caller's stream open, the reaper). The
caller may hold the UG lock without being a UG thread: the console holds the exclusive lock
for every command, and a user thread may hold the shared lock. No update cycle runs meanwhile,
and any other thread that needs the lock blocks, so a wait on such a thread could never end.
Which library threads can need the lock: the diagnostics writer (`InputTable.add` waits for the cycle that applies it, and its exit
releases the tables' scope under the shared lock), a poller (`Table.snapshot()` of a
watchlist), another caller's stream open (starting a watchlist's listener, reading a crypto
watchlist), and the reaper (every scope release takes the shared lock; nothing joins the reaper
or waits for its jobs). The flush threads, the WebSocket client threads, the REST and flat-file
reader pools, and the crypto-pairs and futures-schedule workers never need it: they call no
Deephaven operation that locks (static tables, `publisher.add` of a static table,
`publish_failure`, which hands every scope it would release to the reaper, listener removal).
So on a thread that holds the lock (`holds_update_graph_lock`, checked outside `registry_lock`,
since it is a Java call), a verb takes over the key of a stream still opening on another thread
instead of waiting for it ([section 6.1](#61-how-a-live-table-is-kept-alive-and-released)), and `close()` waits for
neither the diagnostics writer nor the cycle that applies the final rows. It does not join the
connections it closes or the futures-schedule worker either, though none of those threads
needs the lock: while it waits no live table updates, and against an unresponsive server each
join could last its full `JOIN_TIMEOUT`. They are daemons and end on their own, and the
closing thread writes each closing connection's final `CLOSED` row itself
([section 6.1](#61-how-a-live-table-is-kept-alive-and-released), `close()` step 4); a caller
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
`MassiveConfig`, so they are tested alone against the fakes of `tests/_fakes/`. The listing
below, like the tree of [section 4.1](#41-source-modules), is the target layout.

```
src/deephaven_massive/_massive/
__init__.py          lazy interface: __all__ and re-exports only
net/                 shared by every client; imports nothing else in _massive
  __init__.py        USER_AGENT; re-exports
  errors.py          WireError, WireAuthError, WireEntitlementError; linked_errors (an
                     exception's whole chain of arguments, causes, and contexts), and
                     is_tls_failure and is_proxy_auth_failure, which search it
  http.py            CA bundle (SSL_CERT_FILE/SSL_CERT_DIR, else certifi); one shared TLS context
                     per process, consumer, and certificate setting; proxy from HTTPS_PROXY/NO_PROXY
  cancel.py          Cancel: a one-way threading.Event that follows parent tokens and runs
                     callbacks when set
  origins.py         origin_of(url): the (scheme, host, port) a URL reaches, default port
                     filled in; the REST client's next_url check and the governor registry's
                     key both compare origins with it
  retry.py           the retry schedule (0.5/1/2 s, +20 % jitter; Retry-After; an unpaced 429
                     pauses,
                     retryable classification; TLS never retried (net.errors finds it);
                     Clock (injectable, tests never sleep); an unusable Retry-After is logged
                     at WARNING, never 0
  codec.py           JSON codec: orjson when installed (logged once), else stdlib
  redaction.py       Redactor, redact: a client's secrets, however JSON
                     spells them, become *** in whatever it receives, before it is decoded,
                     quoted, or logged
  capture.py         set_reply_capture, CapturedReply: the optional sink every cleaned REST
                     body and WebSocket frame is handed to
spec/                run-time wire vocabulary; imports nothing else in _massive
  __init__.py        source(transport, key), sources(transport), provenance(): load once under
                     a lock, and the cache that holds them
  generated_schemas.py  parsing of the generated spec/*.json files into the types of spec.types
  types.py           Transport, Source(key, transport, fields, memberships),
                     Membership(kind, group, position, projection), RestPath(Source + template,
                     result key, params), Field(wire key, column, WireType, unit, required,
                     conversion), Conversion (named conversions, each with its declared and
                     resulting WireType), Param(+ unit), ParamKind, WireType, WireUnit,
                     ENVELOPE_KEYS, DriftReport(extra, missing, statuses, samples),
                     DriftSample(symbol, value); WireType.common_type, the narrowest wire type
                     that holds values of two types, which the refresh and the run-time schema
                     merge both call
  convert.py         convert(wire_type, value): one checked, exact scalar per WireType, shared by
                     every JSON decoder (REST to_batch, WebSocket frames); UnconvertibleError;
                     convert_field applies a field's named conversion and raises with the
                     shared bad-value message; field_converter gives a decoder each field's
                     converter once, and unconvertible_message, missing_key_message and
                     excerpt word an odd value or a dropped row alike in both decoders, which
                     add only their own context
  values.py          encode_value(param, value, *, label): one REST parameter value's wire
                     text by ParamKind, for the REST client and the library's filter check
  rest.json  ws.json  flatfiles.json        generated, committed
rest/                imports net, spec
  __init__.py        RestClient, Page, to_batch
  client.py          pool, bearer auth, retries, a governor permit per attempt, next_url paging,
                     close
  governor.py        RequestGovernor: per-bucket pacing (Basic: 5 per minute), pauses after
                     429, learning, give-up; BucketState snapshots
  query.py           request targets (values encoded by spec.values), next_url checks
  response.py        strict decoding, envelopes, result keys, error mapping (http_error);
                     RequestTarget, the request a page answers
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
  client.py          FlatFileClient: lazy boto3 client under its lock, shared model loader,
                     closed flag, open(key) -> FlatFile | None, S3 error mapping, prefixes(),
                     close(); botocore's body read errors become OSError
  object_keys.py     object_key(prefix, day, venue): <prefix>/<YYYY>/<MM>/<date>.csv.gz; {venue} template
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
  knowledge_base.py  ARTICLES (slug -> what the library takes from it), the knowledge-base
                     articles the library relies on; the Markdown of an article's HTML page
  plan_access.py     each vendor page's "Plan Access", "Plan Recency", and "Plan History"
                     sections -> PlanAccess, stored in descriptions.json by page URL
  openapi.py         OpenAPI reading: $ref, allOf merge, one-branch choices, checked accessors
  plain_text.py      the four fixed rules that turn a vendor description into plain text
  naming.py          the naming policy shared by every transport: PascalCase columns, snake
                     case concepts, short and timestamp-like names, the dataset key columns
  fields.py          the field rules every generator shares: representation rules, names,
                     units, type fixes, documented examples, and the gates met per field
  rest_sources.py    REST paths -> RestPath: flattening, result keys, parameters, path
                     coverage, and the gates met per parameter
  ws_sources.py, flat_file_sources.py  the WebSocket and flat-file generators
  kinds.py           KINDS and PROJECTIONS -> memberships; type-agreement (by
                     WireType.common_type), transport-agreement, and the kind column order
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
  spec_diff.py       flatten the paths of an OpenAPI specification and describe how two
                     copies differ

vendor/massive/      outside the package; Massive's published content, vendored as generation
                     input, not covered by the repository's license, in no distribution
  openapi.json  websocket.json  llms.txt  manifest.json
  rest/  websocket/  flat-files/   (the pages of each area llms.txt links)
  knowledge-base/  (the articles the library relies on, as Markdown extracted from HTML)
```

**The interface.** `_massive/__init__.py` defines `__all__` and re-exports exactly these 39
names, resolving them lazily (PEP 562), so a REST-only session never imports boto3 or
websockets:

- errors: `WireError`, `WireAuthError`, `WireEntitlementError`;
- REST: `RestClient`, `Page`, `to_batch`, `RequestGovernor`, `BucketState`, `Patience`,
  `BASIC_REQUESTS_PER_MINUTE`, `RATE_WINDOW`, and `Cancel` (the token that ends a request's
  waits, from `net.cancel`);
- WebSocket: `WsClient`, `WsState`, `Status`, `StatusKind`, `Reject`;
- flat files: `FlatFileClient`, `FlatFile`, `ReadPlan`, `ReadColumn`, `object_key`;
- spec: `source`, `sources`, `Transport`, `Source`, `RestPath`, `Field`, `ParamKind`,
  `WireType`, `WireUnit`, `DriftReport`, `DriftSample`, `provenance`, `Provenance`, and
  `encode_value` (one parameter value's wire text by its `ParamKind`, in `spec.values`, which
  the REST client and the library's filter check both run, so a filter the check accepts
  always encodes; it loads no client, and a subprocess test checks that no vocabulary name
  loads a client or network module);
- origins: `origin_of` (from `net.origins`), the vocabulary name the library uses to key a
  shared governor by REST origin.
- reply capture: `set_reply_capture` and `CapturedReply` (from `net.capture`), an optional
  process-wide sink that receives every reply the REST and WebSocket clients read, each REST
  body (refusals included) and each frame after authentication, already cleaned of the
  clients' secrets by their `Redactor`; off by default, at the cost of one `None` check per
  reply. Only the live-run program uses it, to record Massive's real replies.

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
shared mutable state is urllib3's connection pool, which locks internally, the
`RequestGovernor` it takes a permit from before every attempt, which locks its own state and
may be shared with other clients, and its closed `Cancel` token. Every call is independent; no
call mutates an argument (the parameters are copied before they are encoded).

```python
RestClient(api_key, *, base_url, max_concurrency, governor, on_refused=None)
client.get(rest_path, path_params, query_params, *, bucket, cancel=None,
           patience=UNLIMITED) -> Page                               # one request
client.pages(rest_path, path_params, query_params, *, bucket, cancel=None,
             patience=UNLIMITED, limit=None) -> Iterator[Page]       # follows next_url
RequestGovernor(buckets, *, learn_only=(), clock=SYSTEM_CLOCK, labels={})
    # declare, admission(bucket, *, lane, patience=UNLIMITED) -> Admission, done, release,
    # refused, charge_pause, snapshot, next_change, estimate, idle, subscribe,
    # subscribe_bucket, unsubscribe
Admission.acquire(cancel) -> Permit   # inside `with governor.admission(...)`; exit reports it
Patience(seconds, *, advice="")       # one read's budget of pause time, shared by its requests;
                                      # advice ends its give-up message
Cancel(*parents)                                  # a threading.Event set once, following parents
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
  The body is streamed (`preload_content=False`) into one buffer in 1 MiB decoded reads. A
  body that decodes to more than `MAX_BODY_BYTES` (256 MiB, a module constant, not a setting)
  raises `WireError` with the response's status, never retried, so a gzip bomb from a broken
  or hostile server or proxy cannot exhaust memory. A body that does not decompress is
  treated the same way: both are faults of the reply's content, which the same request would
  bring again. A body that breaks off or times out is a transient failure, retried like a
  reset. The governor still hears of every attempt whose status arrived: a 429 is reported
  as a refusal (the bucket pauses) whether its body was read, was too large, would not
  decompress, or broke off, and any other unreadable reply as answered. A body read in full
  returns its connection to the pool; one abandoned part way (the cap, a reset, a decode
  error) closes it first, so its unread rest is never taken for the next reply. The body read
  in full passes through `Redactor.received` once, as the same `bytearray` (never copied to
  `bytes`), before the 429, error, and page branches read it. The cap sits far above the largest page the datasets ask for (section 8):
  pages of short rows (trades, quotes, bars, ratings) decode to some 10 to 40 MB at 50,000
  rows, and pages of long rows stay at tens of MB because their page size is lowered. The cap
  bounds a decompression bomb, not every memory risk: a body under it still parses into
  Python objects several times its size.
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
  `ssl.create_default_context()` honours both) and `certifi`'s bundle otherwise. One context is
  built per process for each consumer (urllib3 or `websockets`) and certificate setting, under
  a lock, and shared by every client of that consumer and every connection: building one parses
  the bundle, a few milliseconds each, and a configured `SSLContext` is safe to share between
  threads (CPython guards each context's OpenSSL state with its own lock). The certificate
  setting is the values of `SSL_CERT_FILE`, `SSL_CERT_DIR`, and `SSLKEYLOGFILE`, plus the
  modification time, size, and inode of the file and directory the first two name (nothing
  when the path does not exist), so a bundle created, replaced, or edited at runtime, the
  recovery the certificate error messages advise, takes effect for the next client without a
  restart. urllib3 and `websockets` never share a context because urllib3 writes its
  verification mode and ALPN list onto the context it is given before every connection. The
  proxy settings do not shape the context, so they are not part of its key. Every context
  offers ALPN `http/1.1`: REST and the WebSocket upgrade are both HTTP/1.1, and live handshakes
  with Massive's REST, real-time, and delayed hosts select it over verified TLS 1.3.
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
- **Retries** follow `net/retry.py`. At most 3 retries (4 attempts) of a transient failure;
  every request is a `GET`, so a retry is always safe. HTTP 500, 502, 503, 504 and connection
  errors wait 0.5, 1, then 2 s, each with up to 20% upward jitter. A TLS certificate failure
  (`ssl.SSLCertVerificationError`) is never retried: it raises `WireError` at once, naming
  `SSL_CERT_FILE`. Nor is a proxy's refusal of its credentials (HTTP 407 to the tunnel request,
  `net.errors.is_proxy_auth_failure`): repeating them can lock a corporate account, so it raises
  `WireError(status=407)` at once, naming `HTTPS_PROXY`. A connection reset in the middle of
  paging retries **that page** (its `next_url`), never the first page. Redirects are never
  followed (a 3xx is a `WireError`), so the bearer key never leaves the REST host. Every wait
  goes through an injectable clock (`net.retry.Clock`, whose `wait(event, seconds)` ends early
  when the event is set; the client uses its governor's), so tests never sleep. A `next_url`
  that repeats the target just fetched raises `WireError` rather than paging forever.
- **The governor.** Every attempt, every page included, first takes a permit of its `bucket`
  from the `RequestGovernor` (`rest/governor.py`), in its lane (the caller's `cancel` token,
  or the call itself when it has none), through a claim (`with governor.admission(...)`,
  then `Admission.acquire`), and reports how it ended, exactly once: answered, never sent
  (released: a `ConnectTimeoutError` or a `ProxyError` anywhere in the error's chain, so a
  connection that could not be made or a proxy or tunnel that failed), or refused with HTTP
  429; anything else, an exception on the wire or an interrupt included, is reported as not
  answered by the claim's exit. The governor ignores any report after the first. A slot is
  released on cancellation, failure, and interrupts at wait points; an interrupt inside a
  lock wait, or a second one during the claim's exit, is beyond what Python can make safe. The refusal hook runs before the governor hears of a 429, and the
  governor hears of it even when the hook raises. The governor's rules are those of
  [section 13.4](#134-rest): per bucket, lanes in turn and first in, first out within a lane; a
  paced bucket (declared, or learned from a 429 whose message names the per-minute maximum)
  admits a request only while fewer than 5 slots are held, a slot lasting from the request's
  start until 61 s after its response; a 429 pauses the bucket (its `Retry-After` in either
  form, an HTTP date measured against the response's own `Date` header, up to 10 minutes
  (`MAX_RETRY_AFTER`; a longer one is clamped and logged at INFO), else a whole window
  when paced, else 15, 30, then 60 s by the run's refusals; a `Retry-After` of zero counts as
  none), the burst of 429s in flight when the pause began counting once; after a pause one
  probe request goes alone until it is answered; a run ends with an answer to a request
  started after the latest pause, or a window after its pause ended with no refusal; a 429 is
  not a retry, and the attempt is made again once the governor allows it; the bucket never
  gives up, and a call whose read's `Patience` cannot cover a pause raises
  `WireError(status=429)` naming the bucket's requests, when the next one goes, the patience
  and what it has spent, the usual cause, the remedies (ending with the patience's `advice`, when it
  has one), and the latest refusal. A bucket named `learn_only` at construction
  (`other`) cannot be declared and is paced once a refusal names the limit. A
  request that may start at once with nothing queued before it is granted without queuing,
  and a wait without a deadline is woken only by a change of its bucket, never by a timer;
  the system clock waits it out in half-second chunks, so Ctrl-C ends it on every operating
  system. Every permit is taken through `RequestGovernor.admission`, a claim opened before the
  grant whose exit reports the permit, so no interrupt leaves one unreported, short of one
  inside a lock wait or a second one during the claim's exit, which no Python code can make
  safe. A
  bucket the governor was not built with raises `ValueError`. Before the governor pauses the bucket,
  the client calls its `on_refused(bucket, error, *, basic)` hook (the library's first-429
  WARNING).
- **Cancellation.** Each `get` and each page waits on a `Cancel` token of its own, made from
  the client's closed token and the caller's `cancel`, and closed again when the call ends, so
  nothing keeps a reference to it. `close()` sets the client's token: a thread waiting for the
  governor or a retry stops at once and raises `WireError("... the REST client is closed")`,
  and every later request raises it too; a caller's `cancel` ends the waits of its own requests
  only (`WireError("... cancelled")`), and closing one client ends none of another client's
  waits on the same governor. A request admitted just as its token is set is not sent.
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
  float or a microsecond `datetime`), `long-array` → `list<int64>`, `string-array` →
  `list<string>`. Values are checked and
  converted by `spec.convert`, which the WebSocket decoder shares. ISO date-time text **without** an offset is an
  unconvertible value, never assumed to be UTC: a field that really is offset-less UTC gets a
  named `TYPE_FIXES` conversion. `DriftReport.missing` holds only the vendor-`required`
  fields that some row lacked or held as null; an extra nested object is reported by its own
  key, not by each leaf. A page is read a column at a time: each field's values are gathered
  into one list, the types of the whole list are checked at once, and a column of plain values
  goes to Arrow without a Python call per value. A page that this reading cannot vouch for (a
  bad row, a value only its converter can judge and refuses, an integer Arrow cannot hold
  exactly as a double) is read again row by row, which gives the same batch, drift, and error
  as before; the fallback logs its reason at DEBUG, once per page.
- **Logging.** Each request logs at DEBUG: method, path, query (with no secret), status,
  `request_id`, row count, and elapsed time. Retries log at INFO with the wait, and exhausted
  retries at DEBUG: the call that raises the error is what the user sees. The `Authorization` header is never logged.

### 19.2 The WebSocket client

`_massive.WsClient` is one object per connection (one `wss://host/path`), created by `_ingest/ws_connection.py`
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
  handshake frame that is not JSON is vendor drift: logged at ERROR with the library-bug text
  and retried. Each backoff, refused handshake, and reconnect attempt logs at DEBUG; the
  library's connection logs one WARNING per outage ([section 17.1](#171-the-log)). The auth frame
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
  - `on_data(generation, channel, rows, rejects, drift)` for each channel's data objects in
    the frame. Rows are keyed by **long-form** column (the library renames them) and hold
    every column of the channel's source, null when absent or when the value does not fit;
    values are validated and converted to their Python form (an epoch keeps its integer
    count; an ISO-8601 instant becomes exact integer nanoseconds since the epoch), and symbols
    stay as the wire spells them. A row whose required key is absent, null, or does not fit
    becomes a `Reject(channel, symbol or None, WireError, raw)` instead, with the raw value of
    the declared symbol column (`None` when none is declared or the value is missing); the
    frame's other rows are delivered. `drift` is the frame's `DriftReport`
    ([section 19.5](#195-runtime-drift)).

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
    still lies in `[reconnect_backoff, max_reconnect_backoff]`. This differs from REST, whose
    reads give up once a pause outlasts their patience, because a caller is blocked on the
    request: the WebSocket client never gives up, and an unbounded `Retry-After` would
    leave its streams silent for as long as the server named;
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
boto3 S3 client lazily, under its own lock, and keeps a closed flag (a `threading.Event`);
once closed, it makes no request. A request in flight at the close closes the client again as
it returns. Every S3 client in the process is built on one shared botocore model loader, so
botocore parses its S3 model files once per process instead of once per client; the loader
stays in memory for the life of the process, and its lock serializes client builds (section
18).

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
  check, the `venues=` parameter) is added by `_ingest/flatfiles.py`, whose public texts are
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

  Each retry is logged at INFO and giving up at DEBUG, naming the object and the failure;
  the call that fails is what the user sees.
  Three failures are never retried:
  - A TLS failure (botocore's `SSLError`, or any error `is_tls_failure` recognises). A
    certificate failure's message names `SSL_CERT_FILE`.
  - A proxy's refusal of the client's credentials (HTTP 407 to the tunnel request, which
    `net.errors.is_proxy_auth_failure` recognises). Repeating credentials the proxy refused can
    lock a corporate account; the `WireError` has status 407 and names `HTTPS_PROXY`.
  - A failure partway through an object's body. The reader has already handed blocks on, so
    the client cannot repeat the request by itself: the read fails with a `WireError`, and the
    file's `dropped` flag is set. `_ingest/flatfiles.py` keeps a file's rows to itself until
    the file ends, so it reads such a file again from its start: it frees the file's partial
    tables (it clears the frames the error's traceback holds, `_gather`'s and the batch
    generator's, which keep the failed read's locals alive) and calls `wait_to_reread(error, retry, cancel)`, which waits on the shared
    schedule (logged at INFO), and returns `False` once `MAX_RETRIES` rereads are spent (logged
    at DEBUG); the library then raises the failure. A wait whose `cancel` is already set, or on
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
(1) the symbol filter (`is_in`); (2) the key check: a row whose `required` header is empty or
does not parse, or whose time `plan.to_ns` cannot place, is dropped and counted, and a file
whose every row of the requested symbols is dropped raises after its last block; (3) `[start, end)` on `plan.to_ns(time column)`; (4) on the
survivors, the conversion of every header from text to its type (a value that does not parse
is null and reported in `unconvertible`), list parsing, and the rename to long-form names.
Instants stay in their wire unit; the library converts them. The reader first peeks the
header line and reads by name ([section 16](#16-historysourceauto)). `close()` is idempotent and
safe from any thread.

```python
ReadPlan(columns: tuple[ReadColumn, ...], symbol_header, symbols, time_header, window_ns,
         to_ns, block_size: int, required)
ReadColumn(header, column, read_type: pyarrow.DataType, list_type)
```

`_ingest/flatfiles.py` builds one plan per call. `block_size` is the reader's block of
decompressed text, a required field with no default;
`_ingest/flatfiles.py` always passes its `BLOCK_SIZE` (1 MiB).

`_ingest/flatfiles.py` builds the plan from `source(Transport.FLAT_FILES, prefix)`. `to_ns` is the
`_datasets/timestamps.py` routine, returning the nanoseconds, null where a value fails, and the first
failure's message, so a wrongly declared unit drops every row and fails the file; the window is
never converted into wire units.

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
  is itself the rows), keeps fields in the order the vendor documents them, then moves the price
  columns first (see the naming policy, rules 6a and 6b), and records the parameters (wire name,
  `ParamKind`, enum values, path or query, the unit of a `TIMESTAMP`, and the default the spec
  gives the parameter, as a JSON scalar or `null`). A JSON-object body
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
- **Representation rules** (generator rules, not hand entries): an array whose items the
  scalar rules make a `long` becomes `long-array`, and one whose items are plain `string`
  becomes `string-array`; an array of anything else (objects, other numbers, instants, dates,
  arrays), a free-form object, and `oneOf`/`anyOf` become `json-text`; the WebSocket envelope key `ev` is dropped; a field with a `UNITS` entry becomes
  an `epoch` in that unit; a string with `format: date-time`, or no format and a name ending in
  `_utc`, becomes an `iso-instant`; a string with `format: date`, or named `date` or `*_date`,
  becomes a `date`.
- **Projections.** `PROJECTIONS {(kind, source): Projection}` changes how a source joins
  one kind, **inside one membership only**, in one of two forms. `fields` keeps only the
  listed keys, under the listed long names; no membership uses it since the indices snapshot
  left the values kind ([section 7.5](#75-one-row-datasets)). `renames`
  keeps every field and renames only the listed ones, so a field the vendor adds later joins
  under its own name: the forex last quote and the crypto `XQ` stream join the quotes kind
  with `ask`/`ap` as `AskPrice` and `bid`/`bp` as `BidPrice`. The generator writes each long
  name through the naming rule, and the recorded projection maps every wire key it keeps to
  its PascalCase column, the same vocabulary as `Field.column`. A member whose projection
  renames is ordered, and gives the order, like an unprojected one (rule 6a); a member that
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
(an integer with a unit), `iso-instant`, `long-array`, `string-array`, and `json-text`. OpenAPI
`integer`, and `number` with `format: integer`, `int32`, or `int64` (the option chain's quote
exchange ids), map to `long`, and `number` to `double`. Doc pages map
`integer` to `long`, `number` to `double`, `string` to `string`, and `timestamp - integer` to
`epoch` (with its `UNITS` entry). Two types meet for one column name within one kind by these
joins: `long ⊔ double = double`; `long ⊔ long-array = long-array` (a scalar becomes a
one-element list); `epoch ⊔ epoch = epoch`, each source keeping its own unit. Any other pair is a
**type conflict** (the `type-agreement` gate). `WireType.common_type` is the one
implementation of these joins: the refresh's `type-agreement` gate and the run-time merge
both call it, so a kind the refresh accepts always merges. `_datasets/schema.py` maps `long` to `long`, `double` to
`double`, `bool` to `boolean`, `string`, `json-text`, and `date` to `String` (a date as ISO
`yyyy-MM-dd` text), `epoch` and
`iso-instant` to `Instant`, `long-array` to `long[]`, and `string-array` to `String[]`. An `iso-instant` is ISO-8601
date-time text with seconds and an offset (`Z` or `±HH:MM`), decoded to exact integer
nanoseconds since the epoch; text without an offset, or an instant outside the signed
64-bit nanosecond range (1677 to 2262), is an unconvertible value, never assumed UTC or
clamped. A `long-array` element outside the
`int64` range, and a `string-array` element that is not text, is an unconvertible value; a
null element is kept, as Deephaven's null in the array.

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
   columns, `Symbol`, `Timestamp` (a bar's `TimestampStart`), `TimestampEnd`, `Underlying`, and
   `Date`, made by the run-time key rename. A vendor field literally named `symbol` (the
   open-close and conversion paths) is generated as `Symbol`, its own name, and the key rename
   uses it as is. The library owns `Timestamp`, every `Timestamp<Qualifier>`, and, on tick, bar,
   and daily-summary datasets, `Date`: `NAMES` may never coin one (the `column-collision` gate,
   `naming.is_key_column`), and a catalog test requires every such column of a dataset to come
   from a declared key or from the library's time columns, never from a vendor field that merely
   carries the name. The test lives with the catalog because the roles are declared in
   `_datasets`, which `_massive` cannot import. There are no `Ticker` or `Pair` stand-ins.
6. **Order.** Each generated file's field lists record the column order explicitly, set in two
   steps.

   6a. **Vendor order.** Columns first take the vendor's natural order. Massive's REST documentation lists every object's fields
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

   6b. **Price columns lead** (owner-approved, 2026-10-30). One rule, after the vendor order,
   orders every dataset of every asset class and transport alike, so a table opens on its
   prices, and two feeds of one kind put them in the same place:
   1. The key columns (`Symbol`, `Date`, `Timestamp*`, `OptionRoot`, `RawSymbol`) come first,
      unchanged; the generator never moves a field the library treats as a key column.
   2. A column's group is its nested-object parent, known from its long-name path: the top
      level, `Day`, `PrevDay`, `LastMinute`, `LastTrade`, `LastQuote`, `Session`,
      `UnderlyingAsset`, and so on.
   3. Each lead field moves to the start of its group, in this order: `Price`, `Size`,
      `BidPrice` or `Bid`, `BidSize`, `AskPrice` or `Ask`, `AskSize`, `Open`, `High`, `Low`,
      `Close`, `Volume`, `Vwap`, `Value`. A field leads only when its own name, without its
      parents', is exactly one of these: `DayClose` leads the `Day` group, while
      `DayDecimalVolume` and a top-level `PreviousClose` do not lead.
   4. Every other `Bid<X>` moves to just before its `Ask<X>` partner in the same group
      (`BidExchange` before `AskExchange`). Nothing else moves.

   So bars read `Open High Low Close Volume Vwap …`, trades `Price Size …`, quotes
   `BidPrice BidSize AskPrice AskSize BidExchange AskExchange …`, and snapshots
   `DayOpen DayHigh DayLow DayClose DayVolume DayVwap …`. The refresh applies the rule last
   (`naming.lead_order`): to each kind group's order, and to every REST source as generated,
   so a source in no kind follows it too.
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
  coining a library column (`Symbol`, `Underlying`, `Date`, `Timestamp`, or any
  `Timestamp<Qualifier>`);
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
  order, or two of its groups order its columns differently (naming policy, rule 6a);
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
  `provenance() -> Provenance(fetched: date, urls: Mapping[tuple[Transport, str], tuple[str, ...]], plans: Mapping[str, PlanAccess])`
  exposes the fetch date, the URLs of each source, and each vendor page's plan tables (by page
  URL: its title, the access, recency, and history summaries, and one `PlanTerms` per plan,
  which the refresh's `plan_access` reads into `descriptions.json`), for the dataset reference;
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
  for each group, each source s of the group in its position, read by the member m:
      fields = s.fields through the projection of s's membership of (m.kind, m's group), if any
      cols(m, s) = rename(m, s, fields)      # m's keys for s: symbol -> Symbol, time ->
                                             # Timestamp, an instrument fanout -> Underlying
                                             # (any other fanout and session keep their
                                             # columns); a key filled from the request adds
                                             # a constant column; nothing else changes
  order  = ordered union by column name of every cols(m, s):
           Symbol, then Timestamp, first where the dataset declares them; then first
           appearance in group, then source position, then field order
  schema = the columns of order that some cols(spec, s) of the dataset's own sources holds,
           or that the library fills (its key and time columns), in that order
  each column's WireType = WireType.common_type over every contribution of the kind
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
an unconvertible value like any other ([section 19.5](#195-runtime-drift)).
Every entry has a fixture test of both the conversion and the failure; a conversion that no
entry declares is coercion, which never happens. The conversions:

| `Conversion` | Declared → wire type | Accepts | Used by |
|---|---|---|---|
| `iso-text-instant` | `string` → `iso-instant` | ISO-8601 instant text | benzinga analysts and firms `last_updated`, futures market-status `timestamp`, `/v1/marketstatus/now` `serverTime`, `/v1/marketstatus/upcoming` `open` and `close` |
| `iso-date-text` | `iso-instant` → `date` | `YYYY-MM-DD` | futures products `last_updated` (sample `"2025-02-22"`) |
| `quoted-integer` | `long` → `long` | an integer, or decimal digits in text | WebSocket forex `C` `x` (sample `"44"`) |
| `text-or-integer-id` | `long` or `string` → `string` | text or an integer | the crypto trade ID, one decision: the full-market and movers snapshots `lastTrade.i`, declared text (samples `413131` and `"517478762"`), and WebSocket `XT` `i`, declared an integer (sample `14272084`); TMX `tmx_company_id`, declared an integer (sample `"2208"`) |
| `number-label` | `double` → `string` | text, or a finite number as float text (`1` gives `"1.0"`) | risk-factor `taxonomy` (sample `"1.0"`) |
| `code-list-text` | `long` → `long-array` | one integer, or integers separated by commas in text | flat-file `conditions` and stocks quotes `indicators`, one decision (sample `"12,37"`) |
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
`SourceKeys`, one optional declaration per role (`symbol`, `time`, `end`, `fanout`, `date`):

- every source of a dataset in a kind declares `symbol` and `time`;
- every source of any other instrument dataset declares `symbol`. A dataset is an instrument
  dataset when it takes symbols: it has channels, flat files, `per_symbol`, a symbol
  filled from the request, or a `symbol_param` that filters a single-ticker field. A
  `symbol_param` over a list-valued field (the generated `tickers` field of a filing or an
  article is `string-array`) does not make one: those rows have no `Symbol`
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
  `iso-instant`, so `Timestamp` (a bar's `TimestampStart`) is always an `Instant`; so is an
  `end` key, the WebSocket bars' `e`, which becomes `TimestampEnd`. A dataset whose row time is a
  calendar date keeps that date column, ISO text, under its own name and declares no `time`
  key;
- a `date` key is a `date` field, the row's trading day as the source sends it (the futures
  `session_end_date`, the open-close `from` and crypto `day`), and becomes `Date`; only a
  dataset whose rows have a `Date` (`row_times` other than `NONE`) declares one, and a daily
  summary (`DAY`) declares one on every source;
- `fanout`, `date`, and `end` are declared only on the sources that carry them; only a bars
  dataset declares `end`;
- `instrument` says whether a `fanout` key names an instrument: True, the default, renames it
  to `Underlying` (the options underlying, the related company asked about); False keeps its
  generated column (`filer_cik` stays `FilerCik`). Only a `fanout` key may set it False, and
  such a key is a column of the rows, never `from_request`;
- `required` (default True) says whether every row must carry the key. False is for a symbol
  Massive documents as present only "if available" or marks optional, such as the `ticker` of
  the stocks `filings.index`, the TMX corporate events, and the Benzinga `analyst_insights`,
  `ratings`, `bulls_bears_say`, `guidance`, and `earnings` (whose only documented
  analyst-insights row has no `ticker`): a row without it keeps a null `Symbol` instead of
  failing the request as a missing required key. It is also for an underlying only some rows
  carry: the unified snapshot's `fanout` key `UnderlyingAssetTicker`, which only its options
  rows have. Only a `symbol` or `fanout` key read from a REST path's rows may set it False;
  flat files and streams filter and route rows by symbol, and the `time`, `end`, and `date`
  keys are always required. A dataset with an optional symbol key cannot put
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
`session_end_date` is not a missing key, while a futures flat file without it is. A member of
a kind keeps only the columns its own sources fill: crypto's `ParticipantTimestamp` is renamed
to `Timestamp`, so crypto trades have no `ParticipantTimestamp`, while stocks keeps its
`ParticipantTimestamp` column. The columns the members share keep the kind's type and order,
so their tables merge on them. A non-tick
dataset with several role paths (`options.contracts` list and per-contract) is the union of its
role paths under the same join.

**Column descriptions.** Each column of a dataset schema is a
`Column(name, type, description, supplied_by, wire_keys)`: `supplied_by` is the transports of
the dataset's own sources that fill the column (a key filled from the request counts), in the
order `REST`, `FLAT_FILES`, `WS`, never empty (a generated test checks every dataset); `wire_keys` maps each of those transports to the distinct wire
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
a dataset of `ClassFromPrefix.ANY` takes the field's own description.

1. **A column the dataset's own sources supply** takes the first non-empty description among
   them, trying the transports in the order REST, flat files, WebSocket and, within one
   transport, the dataset's sources in their declared order (role paths, then channels, then
   prefixes). REST comes first because its text is the most complete.
2. **The library's own columns**: `Symbol`, `Timestamp`, and `Underlying` when they are
   declared key columns of the dataset, and the time columns the library fills (`Date`,
   `TimestampStart`, `TimestampEnd`, `TimestampInsert`), get a library sentence ("The
   instrument, in the library's canonical symbol form.", "The row's primary time, as an
   Instant.", "The instrument Massive links the row to, exactly as Massive sends it; for an
   option, the linked instrument, not what the contract delivers."; a bar's
   `TimestampStart` "The start of the bar's window, as an Instant." and, for every class with a
   day zone, where a bar of a day or longer starts, since the library's start is not the
   end-of-day stamp some sources' descriptions quote; `TimestampEnd`, `Date`, and
   `TimestampInsert` what [section 12](#12-timestamp-units-and-day-boundaries) says of them for
   the dataset's row times and class, the join caveats included). The key columns, `Date`, and
   `TimestampStart` go on with one
   sentence that starts "From" and lists, per transport in the order of rule 1 and spelled as in
   [section 7.4](#74-column-descriptions), the wire key each source renames and its quoted
   field description (`` REST `sip_timestamp`: "The nanosecond accuracy SIP Unix Timestamp…" ``)
   or, for a key filled from the request, the request parameter it came from
   (`` REST: the request's `stocksTicker` parameter ``). The items are joined by "; ", so a
   lowercase label such as `flat files` never starts a sentence, and the sentence ends with a
   period unless its last quoted description already ends one inside the quote:
   `` The instrument, in the library's canonical symbol form. From REST `T`: "…"; flat files
   `ticker`: "…"; WebSocket `sym`: "The ticker symbol for the given stock." ``

A library column lists every transport of the dataset in `supplied_by`, since the library
fills it on each. A dataset's note on a column (`DatasetSpec.column_notes`) follows the
description chosen by these rules: the split-adjustment note of every price and volume column
of `stocks.bars`, `options.bars`, and both daily open-close datasets
([section 12](#12-timestamp-units-and-day-boundaries), "Price adjustment"), since the same column
holds either basis depending on the query and no vendor text says so. A column whose sources
carry no description gets `""`, and nothing fails. A column of nested values kept as JSON text
(`json-text`) ends its description with "The column holds Massive's nested value as JSON text."
The sentences of rule 2 and the notes
are the library's statements about its own columns, not hand input about Massive,
so they live in `_datasets/column_descriptions.py`, not in `refresh/`.

**Derived columns.** Two columns no source carries are computed by every transport from the
symbols it reads, and follow the leading key columns with the library's own description and
the transports and wire keys of `Symbol`: `OptionRoot`, on every options dataset with a
`Symbol` and on a dataset of every class whose rows include options contracts
(`DatasetSpec.option_rows`, the unified snapshot), is the symbol without `O:` and its last 15
characters (`symbols.option_roots`, after the OCC grammar check), null for a symbol that is no
options contract; `RawSymbol`, in a session whose `unreadable_symbols` is `LOG` (the default)
or `IGNORE` and only on a dataset that reads its symbol from rows, holds the text of a symbol
the library could not read and is null elsewhere. It is the one column whose presence depends
on a setting, and `columns()` and the column reference say so.
`Catalog.dataset_schema(spec, raw_symbol=...)` caches each
shape, and `SessionCore.dataset_schema` picks the session's, so a result, its `columns()`, and
every transport plan agree. Every outlet of
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
- **Kind membership:** each kind dataset reads only sources holding a membership of its
  `(kind, group)`, and the datasets of one `(kind, group)` together read each of those sources
  exactly once (a group split over a stream dataset and its one-row dataset,
  [section 7.5](#75-one-row-datasets));
- **Wire parameters:** every declared wire name (time-axis pair, symbol parameter, path
  placeholders) is a parameter of its `RestPath`;
- **Append keys:** a dataset whose default `poll()` mode is `APPEND` declares its own
  `poll_keys`, not the default `Symbol`. The gate checks only that the key is explicit; that it
  is unique per row is checked by the dataset's own tests. (`DatasetSpec` refuses empty
  `poll_keys` on an `APPEND` dataset, an empty identity (no `poll_keys` and no
  `revision_keys`) on any dataset, since an explicit `APPEND` could not tell rows apart, and
  `revision_keys` that repeat a poll key.)
- **Feed axis:** a dataset whose default `poll()` mode is `APPEND` declares a time axis, since
  its `poll()` needs `start`;
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

Data on the wire can differ from the committed schema until the next refresh, and Massive's
data drifts in ways no schema predicts. The rule: a valid request returns its data whenever it
can, and serves only what is right. One odd value never costs its row, and one odd row never
costs its call or its stream: a value the library cannot read is null and a row it cannot place
is dropped, each with a WARNING, the way a missing field already reads as null. Wrong data is
worse than an error, so nothing is ever coerced or guessed, and only a payload in which no row
can be placed fails.

**Detection** is in `_massive`: each decoder compares a payload with its generated source and
returns a `DriftReport(extra, missing, unconvertible, statuses, dropped_rows, dropped_reason,
samples)` (`missing`: only vendor-`required` fields that some row lacked or held as null;
`unconvertible`: the fields outside the key columns that held, in some row, a value that does
not fit, decoded as null; `statuses`: REST envelope statuses outside the documented ones;
`dropped_rows`: the rows dropped because a key could not be read, with the first one's reason;
`samples`: for each extra, missing, or unconvertible key, the first row's symbol as the wire
spells it, known to the WebSocket decoder only, and the value of an extra or unconvertible key,
so a WARNING can quote them; `dropped_reason` and `samples` are outside equality and the
`repr`). `DriftReport.merge` combines the reports of two payloads read as one. A decoder never
raises for drift or for a row. **Policy** is in the library: the session's `DriftRegistry` in
`_tables/diagnostics.py` (a leaf lock, WARNINGs logged after release) and the routing in
`_ingest/rest.py`, `_ingest/rest_rows.py`, `_tables/poll.py`, `_ingest/ws_connection.py`,
`_tables/stream.py`, and `_ingest/flatfiles.py`.

A **key** here is a key the dataset declares for the source, not filled from the request and
not marked optional (`KeySpec.required`; `source_columns.required_fields`): the row's symbol,
its time column, and where declared its end, fan-out, and date (the futures session). A dataset
that declares no key has none.

| Case | REST (`rest/batch.py`) | WebSocket (`ws/protocol.py`) | Flat files (`flatfiles/reader.py`) | Library |
|---|---|---|---|---|
| **Extra field** | key not in the path's fields | key not in the channel's fields | header not in the documented headers | Dropped. WARNING once per (dataset, transport, field), naming where it was seen, the first symbol, and the first value ([section 17.1](#171-the-log)). Shown in `SchemaDrift`. |
| **Unknown envelope status** | a `status` outside `OK`, `DELAYED`, `success`, `ERROR`, `NOT_AUTHORIZED`: the rows are kept, `Page.drift.statuses` | | | WARNING once per (path, status). Never a failure. |
| **Key that cannot be read** (absent, null, or a value that does not fit) | the row is dropped and counted (`dropped_rows`), the whole row logged at DEBUG (first 2,000 characters) | the row is dropped as a `Reject(channel, symbol or None, WireError, raw)`, logged whole at DEBUG; the library counts it in the frame's `dropped_rows` | a file without a key header: `WireError` before the first block; a row whose key field is empty or does not parse: dropped and counted | The row is dropped. One WARNING per dataset per session for the first dropped row, naming the transport, where, and the reason (the key, the expected type, and the value); every later drop logs the dataset's running count at DEBUG. Never a failure, unless every row of a payload is dropped (below). |
| **Missing other field** | null | null | a null column of the declared type | A vendor-required field or a CSV header: null, WARNING once, `SchemaDrift`. Any other field: null, silently, because JSON omits absent values and the vendor marks almost nothing required. |
| **Unconvertible value** outside the key columns (the type, the `int64` range, `bool` given as a number, a `long-array` element, `int64` for `long[]`, a `string-array` element that is not text, a nested object that is not an object) | null, the field in `unconvertible` with its first value, the whole row logged at DEBUG | the same | every header is read as text and converted afterwards, a typed value's surrounding spaces trimmed as Arrow's typed parsing does, so a value that does not parse is null, the header in `unconvertible` with its first text; the odd values are found by vectorized pattern, range, and calendar tests, never a Python call per value | Null. WARNING once per (dataset, transport, field), quoting the first value; shown in `SchemaDrift` as `field?`. Never coerced: no undeclared conversion is applied. A lattice widening and a named `TYPE_FIXES` conversion ([section 19.4](#194-generated-schemas), for example forex `x` from a decimal string to `long`) are not coercion; a value the named conversion cannot parse is unconvertible. |
| **Timestamp out of [1990, 2100], or overflow** | in `_datasets/timestamps.py` (`masked_instants`), in `_ingest/rest_rows.py` | in `_datasets/timestamps.py`, in the flush thread as a vectorized mask | the time header in the reader, through `ReadPlan.to_ns`; the others in `_ingest/flatfiles.py` | In the time column, a key that cannot be read: the row is dropped and counted, its reason naming the dataset, the column, the unit, and the value. In any other column, an unconvertible value: null, `unconvertible`, WARNING once. |
| **Undocumented value of an interpreted field** (`timeframe`) | `_ingest/host_decision.py` | | | `MassiveError` ([section 13.2](#132-host-decision)). |
| **Symbol spelling the library cannot read** (`canonical_from_wire` returns None) | `RowDataError` naming the spelling | the row is dropped and fails every stream of the dataset on the channel | `MassiveError` naming the spelling | Under `unreadable_symbols=LOG`, the default, the row is kept with a null `Symbol` and the text in `RawSymbol`, ERROR once per spelling with the library-bug text (at most 100); under `IGNORE` the same at DEBUG; under `RAISE`, a loud failure whose message names the setting and the bug report. |

**When every row is dropped.** One dropped row is drift; a payload in which no row can be
placed is the sign of a change in Massive's format (a renamed key, a new timestamp unit), and
serving it would quietly return an empty table. So a loud failure, a defect of the library
(`ErrorCause.LIBRARY_DEFECT`), names the count and the first row's reason when:

- **REST**: a page that had rows keeps none of them, its error rows included (`RowDataError`
  from `rest_rows.RowConverter.convert`). A one-row page whose row is dropped is such a page.
- **Flat files**: a file holds rows of the requested symbols, and every one of them is
  dropped (`WireError` from the reader after the file's last block, then `MassiveError` naming
  the file). Rows the symbol filter removes do not count. The rule is per file, never per
  block: a block can hold a single row of an illiquid symbol, and its one odd row must not
  fail the call.
- **WebSocket**: `DROPPED_RUN_LIMIT` (100) rows of one dataset on one channel of a connection
  have been dropped in a row, across flushes and symbols, with no row of the dataset published
  between them (`Flusher._counted`, counting the client's drops and the flush's). A flush is
  no unit here: a quiet stream's flush often holds one row, so a per-flush rule would let one
  odd row fail it. A published row starts the count again. Each symbol also has a run of its
  own (`Flusher._counted_symbols`), which other symbols' published rows do not end: after
  `DROPPED_RUN_LIMIT` of one symbol's rows in a row are dropped with none of its rows
  published, the streams that name that symbol fail, so it never freezes them without an
  error. A `dm.ALL` stream is never failed for one symbol; the dataset's WARNING and count
  name the drops.

**A loud failure** depends on the verb: static verbs (`fetch()`, `history()`) raise
`MassiveError` through `from_wire` or `RowDataError`; `poll()` fails its table through
`publish_failure`; a WebSocket failure fails, through `publish_failure`, every stream of the
dataset on the channel. A row dropped for its key is never published, and fails no stream; the
rest of the batch is published. A drift failure goes through `SessionCore.fail_stream`, like an
entitlement failure.

**`SchemaDrift`** is a text column of both diagnostics tables ([section 17](#17-diagnostics)),
sorted and truncated at 200 characters: `m.streams()` uses `transport:field+` for extra,
`transport:field-` for missing, and `transport:field?` for a field that held a value that does
not fit, with the transport spelled `WebSocket` or `REST`, and `m.connections()` uses
`channel:field+`, `channel:field-`, and `channel:field?`. Dropped rows add no entry: the log
counts them.

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
and dataset pages are the flat-file schemas. It also stores, under
`vendor/massive/knowledge-base/<slug>.md`, each knowledge-base article the library relies on
(`refresh/knowledge_base.py`, `ARTICLES`: a short, reviewed list of slugs, each with what the
library takes from the article). Articles are published as HTML only, so the fetch extracts
the title and the `<article>` element of each page as Markdown with the standard library's
HTML parser; the manifest records the page's URL and the hash of the HTML as fetched. The
first is "What is the request limit for Massive's RESTful APIs?", the source of the Basic 429
wording and of the `other` bucket. The documentation host rate-limits a burst of page requests
with HTTP 429, which the fetch waits out (`Retry-After`, capped at 60 s, else 2 to 32 s, five
times); any other failure stops it and leaves the old snapshot in place, and the new snapshot
is swapped in through a backup directory (`vendor/massive-incoming/` and
`vendor/massive-previous/`, both gitignored). The snapshot (about 4 MB, 243 files, fetched
2026-10-30 UTC) is Massive's published content, vendored as generation input and not covered
by the repository's license, so it ships in neither the sdist nor the wheel.

**`--check`** downloads the live `openapi.json`, `websocket.json`, `llms.txt`, documentation
pages, and knowledge-base articles, compares them semantically with the snapshot (`refresh/drift.py`), prints a
report, changes nothing, and exits non-zero on drift. It reports REST paths new, removed, or
newly deprecated (including deprecated paths still in `rest.json`); fields and parameters
added, removed, or retyped; WebSocket channels and keys; flat-file pages, headers, and overview
rows; knowledge-base articles whose text changed, naming what the library takes from each;
changed unit tags; the keys the gates would stop on; and **unaccounted items**:
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
| Field removed | Null; a required field or CSV header warns; rows without a required key are dropped, and a payload with none left fails loudly. | It leaves its source; the declared-keys gate fails if it was a declared key. |
| Field renamed | A removal plus an addition. | The column is renamed in the diff, or stays the same if the long form is unchanged; `transport-agreement` catches a one-sided rename. |
| Type changed | A value that no longer converts is null with a WARNING; in a key, its row is dropped, and a payload with none left fails loudly. | `type-agreement` or the member-types gate fails on disagreement, resolved as a naming error first, then by a vendor fix or a `TYPE_FIXES` entry. |
| Unit changed | The overflow check or the bound drops every row, and the payload, or a run of `DROPPED_RUN_LIMIT` WebSocket rows, fails loudly. | `--check` reports the changed tag, and the `UNITS` entry is re-verified. |
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
  deadlock detection through `faulthandler_timeout` and `pytest-timeout`, and parallel
  workers (pytest-xdist) in the local full check.
- **Layout:** `tests/test_layout.py` passes: the mirror rule and naming of
  [section 4.2](#42-test-layout), the library's layers of [section 4.3](#43-layers), and the
  `_massive` boundary and layers of [section 19](#19-wire-clients-and-the-spec-snapshot).
- **Spec:** `tests/massive/refresh/test_generate.py` passes (parity of `spec/*.json` byte for
  byte, documented = generated ⊎ excluded ⊎ (deprecated ∪ draft), and every refresh gate),
  and `tests/datasets/test_catalog.py` passes (the catalog gates, generated = used ⊎ deferred) ([section
  19.4](#194-generated-schemas)). They read only vendored files, never the network.
- **Dataset reference:** `tests/datasets/test_column_reference.py` passes: `docs/datasets/` holds
  exactly the rendered pages, byte for byte, with no stale, missing, or extra file, and every
  dataset method's docstring names its own page; `tests/namespaces/test_query_building.py` passes:
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
    selected by name: `DOC201` and `DOC501` (docstring Returns and Raises sections must cover
    the code), and `PLR0904`, `PLR0914`, and `PLR0916` (the Pylint limits on public
    methods, local variables, and boolean expressions, which are still preview rules). Preview
    also switches some stable rules to their preview behavior.
  - Global ignores: `COM812`, which conflicts with `ruff format` (it is the only one of ruff's
    documented formatter-overlap rules that reports anything on formatted code; the others stay
    enabled), plus `TRY003`, `EM101`, `EM102`, `PLR0913`, `CPY001`, and `DOC502` (many functions raise
    through helpers, which it cannot follow; `tests/test_errors.py` checks the public API's
    Raises sections in both directions instead).
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
  `addopts = ["-ra", "--import-mode=importlib", "-m",
  "not integration and not benchmark and not soak"]`, `pythonpath = ["."]` (test code imports
  helpers as `tests._fakes…`, so `tests/massive/` never shadows a top-level `massive`),
  `faulthandler_timeout = 120` with
  `faulthandler_exit_on_timeout = true`, and `pytest-timeout` at 60 seconds. Neither coverage
  nor `-n` is in `addopts`, so single-test runs and debuggers work in one process.
- **Parallel workers.** `scripts/check.sh full` runs pytest with `-n auto --maxprocesses=4`
  (pytest-xdist): each worker is its own process with its own embedded JVM, and the controller
  that hands out the tests starts none. pytest-cov combines the workers' coverage, so the 100%
  gate is unchanged. A run that only collects or lists fixtures collects in the main process,
  as xdist does, so that process starts a JVM. A parallel run that selects any live test is a
  usage error, since live tests share rate limits and one evidence log: once a worker has
  collected, the controller reads the ids of the tests it selected (after `-m`, `-k`, and every
  other deselection) and ends the run before handing out any test if one is in a
  `*_integration.py` file. The decision rests on the selection, never on the `-m` expression,
  so `-m ""` and `-m "integration and needs"` are refused like `-m integration`. CI runs the
  suite in one process: its standard runners have 2 CPUs, where two workers gained nothing
  once their startup was paid.
- **Coverage.** `branch = true`, `fail_under = 100`, `exclude_also = ["if TYPE_CHECKING:",
  "@(typing\\.)?overload"]`, each with a reason.
- **Deephaven imports.** Module-level wherever used at run time; imports used only in
  annotations go under `TYPE_CHECKING` (enforced by `TC`). `tests/conftest.py` starts the
  embedded JVM in `pytest_configure`, before collection.
- **One entry point.** `scripts/check.sh [fast|full] [--no-fix]`. Both tiers first run
  `uv sync --upgrade`, so they use the newest versions; offline, a local run warns and keeps
  what is installed, and CI fails. The fast tier then runs ruff fix, ruff format, and mypy; the
  full tier adds pytest with coverage, on up to four worker processes. Local hooks and CI call
  the same script; CI passes `--output-format github` to ruff.
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
a test in `tests/test_docs.py` ([section 4.2](#42-test-layout)).

**1. Stocks Starter (15-minute delayed).**

```python
import deephaven_massive as dm

m = dm.MassiveSession()                                     # MASSIVE_API_KEY from the environment
syms = ["AAPL", "MSFT", "NVDA"]
bars = m.stocks.bars(syms, dm.Timespan.MINUTE).append_only()   # snapshot has no trades/quotes
                                                               # -> delayed.massive.com/stocks
snap = m.stocks.snapshots(syms).poll(30)                    # REST; current state on every tier
daily = m.stocks.bars(syms, dm.Timespan.DAY).history("2025-01-01", "2025-06-30")   # 3 symbols -> REST
prev = m.stocks.previous_day_bar(syms).fetch()              # previous completed session (/prev)
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
fx = m.forex.quotes(["C:EURUSD", "C:USDJPY"]).last_by()     # socket.massive.com/forex, no REST call
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
xfmv = m.crypto.fmv(["X:BTCUSD", "X:ETHUSD"]).append_only() # business.massive.com/crypto
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
    es = m.futures.bars(dm.ALL, dm.Timespan.SESSION, venues={dm.FuturesVenue.CME}).history(
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

**7. Split adjustment.** The README's "Split adjustment" section explains the bases
([section 12](#12-timestamp-units-and-day-boundaries), "Price adjustment") and holds these
examples; `tests/test_docs.py` runs every Python block of that section, in order, against the
fake REST server.

```python
import deephaven_massive as dm

m = dm.MassiveSession()
adjusted = m.stocks.bars("NVDA", dm.Timespan.DAY).history("2024-06-03", "2024-06-14")
traded = m.stocks.bars("NVDA", dm.Timespan.DAY, adjusted=False).history(
    "2024-06-03", "2024-06-14"
)
splits = m.stocks.splits("NVDA").fetch()
mine = traded.raj(
    splits, on=["Symbol", "Date<ExecutionDate"], joins=["Factor=HistoricalAdjustmentFactor"]
).update(
    [
        "Factor = isNull(Factor) ? 1.0 : Factor",
        "AdjustedClose = Close * Factor",
        "AdjustedVolume = Volume / Factor",
    ]
)
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
    messages, and a query's dataset and arguments only in its repr, the call that built it
    (`m.stocks.trades('AAPL')`). The public surface stays
    exactly the verbs, `columns()` (see “`columns()` on every query” below), and the namespaces,
    and the API key stays out of objects users routinely leave in console globals.
22. **Names come from the generated tables** (2026-10-26). The generator names every field from
    its wire key, the lexicon of its shape, and `NAMES`; each `DatasetSpec` declares the key
    columns its sources carry, and `rename` in `_datasets/source_columns.py` maps them to `Symbol`, `Timestamp`, and
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
30. **Wire behaviour settled in review** (2026-10-26; the REST 429 half revised 2026-10-28 by
    decision 60). A REST 429 pauses its subscription's requests: without a usable
    `Retry-After`, for a whole 61 s window when the subscription is paced and for 15, 30, then
    60 s when it is not; with one, for as long as it asks, up to 10 minutes; and for at most 10 minutes of pauses
    in a run; a TLS certificate failure is never
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
35. **The runtime drift policy** of [section 19.5](#195-runtime-drift) (2026-10-30): a valid
    request returns its data whenever it can, and serves only what is right. An extra field is
    dropped and reported; a value that does not fit, outside the key columns, is null with one
    WARNING per dataset and field; a row whose key cannot be read is dropped, with one WARNING
    per dataset and a running count. Only a payload in which no row can be placed fails
    loudly: a REST page, a flat file's rows of the requested symbols, or a run of
    `DROPPED_RUN_LIMIT` (100) WebSocket rows of a dataset on a channel. A futures flat-file
    row keeps its own session. The table never silently lies.
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
40. **REST rate limits and host resolution are shared.** One `RequestGovernor` per API key and
    REST origin, shared by the process's sessions (decision 60), and single-flight host
    resolution through one future per (class, family)
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
    axis of `share_float` ([section 8](#8-namespace-tree)). The indicators took
    `adjusted: bool | None = None`, None sending nothing; decision 67 makes it
    `adjusted: bool = True`, with the same meaning, under the rule that None is never a mode.
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
    several days or longer not at 00:00 UTC, raises `MassiveError` (replaced by decision 70:
    such a bar is served and warns); `AUTO` chooses the source
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
56. **Every bar of a day or longer is stamped at its window's start** (2026-10-28). One
    `history()` call returns the same rows whichever source answers it, so a bar of a day or
    longer of every class but futures, from every source, is stamped at 00:00 of the day it
    starts on in the class day zone: New York for stocks, options, and indices, UTC for forex
    and crypto. This extends decision 52's UTC stamping of forex and crypto 1-day bars to
    stocks, options, and indices, whose grouped daily, `/prev`, and stock `day_aggs_v1` bars
    Massive stamps at the 16:00 New York session close while the range stamps the day's start.
    Only the stamps the vendored samples show for a class, the 13:00 close of a short trading
    day for stocks, options, and indices, and the 16:15 and 13:15 closes of index options are
    moved (the table of [section 12](#12-timestamp-units-and-day-boundaries)): each lies
    inside its own New York day, so no calendar check is needed and a known close never fails
    real data. A week, month, quarter, or year bar must also be stamped on the day its period
    starts (a week on its Sunday, where the live check found Massive starts the weeks of every
    class it read, a longer period on its 1st), so the whole-window rule never
    places it in another period. Any other stamp, forex included, raises `MassiveError` as
    decision 52's crypto check does (replaced by decision 70: such a bar is served, labelled by
    the day its stamp falls on, and warns), instead of being relabelled, and ends with the way out of
    the verb that read it: the flat files for 1-day `history()` (with `adjusted=False` for an
    adjusted request), 1-day bars for longer ones, `history()` for `fetch()`, bars for an
    indicator, explicit symbols for index `dm.ALL` from the files. Indicator values of a day or
    longer are checked by their symbols' class's range rule, since they carry the stamps of the
    range bars they are computed over. Futures bars keep Massive's `window_start` and their
    `session_end_date` label. The checked and moved stamp is a bar's `TimestampStart` (decision
    57).
57. **Every row says when it happened, its trading day, and when it arrived; one `history()`
    has one split basis** (2026-10-28). Ticks keep `Timestamp`; bars, indicator values, and
    daily open-close summaries have `TimestampStart` and an exclusive `TimestampEnd`, the join
    key, and no plain `Timestamp`; `Date`, the trading day, is on every tick and bar table,
    stored as ISO `yyyy-MM-dd` text and null on any bar that spans more than one date; and
    `TimestampInsert`, the local clock when the page, file block, or frame arrived, is the last
    column of every dataset on every transport, snapshots included (which have no `Date`). Every
    calendar date the library produces, vendor dates included, is ISO text, and the library
    stores one shared string per distinct date, so a date costs a reference per row; one type
    lets every date join and compare. Forex and crypto use Massive's UTC day; futures use
    `session_end_date`, or on the WebSocket the 17:00 Chicago rule corrected by
    `/futures/v1/schedules`, a row the schedules cannot place keeping the rule's date with a
    WARNING and its miss shown in `m.streams()`, never failing the stream and never refusing a subscription; a
    contract or product still loading a minute after it was asked for is such a miss, and a
    spread is never looked up. The ends are conservative where Massive's labels are not
    verified: an index bar of a day or longer ends at 01:00 New York time, and an index week or
    longer after its calendar period, weekend included; a bar of several futures sessions
    (revised 2026-10-30: a stock, option, or index bar of several days counts calendar days,
    as the live check of item 63 found) ends after the latest day they can reach with one
    holiday allowed, and one more per five further days (`bar_periods.last_trading_day`, which
    holds for every run of the NYSE and CME calendars of 2024 to 2026); a futures week or longer at
    17:00 Chicago on the last weekday of the latest period its label allows; a futures
    one-second WebSocket bar at the later of its length and Massive's `e`. `history()` keeps a
    bar only when its start lies in the range and its `TimestampEnd` is no later than the
    range's end (for dates, the last day's end) or now, so selection and the join key apply the
    same conservative end; a futures label must also fit the range under both readings, so the
    refusal of futures bars of several sessions named by dates is gone. A symbol with no data
    returns no rows; HTTP 404 after the first page of a request series is an error, and a
    futures session with no data is that session's. `adjusted=True` stays the default of stock
    and option bars and open-close, `None` (Massive's default, adjusted) that of the indicators;
    one `history()` never mixes bases, every aggregates page's `adjusted` echo is checked, and
    the jumps at splits in live tables are the data, documented with the factors to adjust
    as-traded data. `Timestamp`, `Timestamp<Qualifier>`, and `Date` are the library's names
    ([section 12](#12-timestamp-units-and-day-boundaries), [section 13.4](#134-rest), [section
    19.4](#194-generated-schemas)).

58. **Canonical symbols only, converters for the rest** (2026-10-28). Every symbol a dataset
    takes and every `Symbol` column is Massive's exact REST ticker, case kept (`KIMpL`); the
    old normalization (upper-casing, `EUR/USD` to `C:EURUSD`, `SPX` to `I:SPX` on input) is
    gone, and a non-canonical symbol raises `ValueError` naming the canonical form. Conversion
    is explicit and public: `canonical_symbol` with a required `AssetClass`, one function per
    class and per stock convention for query strings, and `canonicalize_symbols`, the only
    lenient path (`OnError.NULL`, now `LOG` or `IGNORE`: decision 66). Stock separators convert only from a named
    `SymbolConvention` (`YAHOO`, `NASDAQ`, `CMS`), each exactly for plain tickers, classes,
    and preferreds; `SymbolConvention.MASSIVE`, Massive's own spelling, is the default, and
    no convention parameter takes None. A later watchlist row that is not canonical failed that
    result, and a symbol Massive sends in a spelling the library cannot read failed by default,
    unless a setting kept the row with a null `Symbol` and the text in `RawSymbol`; decision 66
    replaces both defaults. The index
    prefix is added only from an endpoint of the indices class, never from a name. Every
    options dataset gets `OptionRoot`; the contracts dataset exposes the raw deliverable fields
    and no derived flag; no security identifiers are added, since Massive has no universal one
    ([section 10](#10-symbols-and-all)). After review: futures spreads are Massive's tickers as
    written, with their own converter `canonical_symbol_future_spread` and no builder from legs;
    a futures dataset takes any ticker as written; a dataset of global third-party tickers
    (TMX) keeps them as written; under a convention only that convention's grammar applies;
    any `X:` ticker is canonical, and wherever a pair must be split (a stream, the last-trade
    and daily open/close paths) its base and quote come only from Massive's crypto ticker
    list, read once per session, and a ticker the list does not hold is refused rather than
    guessed; under `WARN` a keyed `poll()` adds `RawSymbol` to its key, so an unreadable row
    is kept by its text; a failed symbol table fails the result; an unreadable WebSocket
    spelling fails every stream of the channel.

59. **Live tables keep their session running, and `close()` fails them** (2026-10-28). A live
    table keeps alive everything it needs (its stream or poller, the connection, and the
    session), and nothing else keeps anything alive, so an inline session's derived table keeps
    ticking and an unused session is freed at once. `m.close()` fails every live table and
    everything derived from it with "MassiveSession #1 was closed; this table no longer
    updates", instead of leaving tables that look live but never change; a failed table keeps
    its rows for `snapshot()`, `to_pandas`, and `to_arrow`. Sessions with one API key and equal
    connection settings share their WebSocket connections, reference-counted, so a console
    session and a long-running job no longer knock each other off the one slot an individual
    plan allows; sessions whose settings differ never share and are warned about, because a
    connection can carry only one value of each setting. A stream waiting for its slot says so
    in its `Error`. The library never calls `System.gc()`: a console-derived table stops after
    the JVM's next collection, and `m.close()` stops everything at once
    ([section 6](#6-massivesession-and-lifecycle),
    [section 6.1](#61-how-a-live-table-is-kept-alive-and-released),
    [section 13.1](#131-invariants)).
60. **A REST rate governor per subscription** (2026-10-28; giving up, the `other` bucket,
    and what is learned revised 2026-10-29 by decision 62). Massive limits REST requests per
    subscription (Basic: 5 per minute; paid: none), shared by every key of the account, so the
    library keeps one `RequestGovernor` per (API key digest, REST origin) in the process,
    shared by its sessions, with one bucket per `dm.Subscription` and an unpaced `other` bucket
    for the datasets Massive assigns to none. `MassiveConfig.rest_basic_subscriptions` (a set of
    the new `dm.Subscription` enum, default empty) paces a subscription from its first request;
    the 5 per minute is one wire constant; validation names `Subscription.CURRENCIES` for forex
    and crypto. A 429 whose message names the per-minute maximum makes the subscription
    learned Basic, remembered for the key and origin for the life of the process; any other 429
    only pauses, and `other` is never paced, whatever its 429 says. A 429 pauses and resumes
    even when nothing is declared and honours `Retry-After` up to 10 minutes; without a usable
    one (absent, zero, or in the past) a paced bucket pauses one 61 s window and an unpaced one
    15, 30, then 60 s by the run's refusals. After a pause one probe request goes alone until
    Massive answers it, so a 429 that does not name the Basic limit costs one refused request
    per pause, never a burst. A run gives up once its pauses add up to more than 10 minutes
    (`RATE_LIMIT_PATIENCE`, not configurable), and ends by itself a window after its pause
    ends with no refusal, so an abandoned run never makes a later 429 give up at once; a stale
    refusal that lengthens the pause starts a run; pacing waits never give up. Waiting
    requests take round-robin turns by lane (a read, a poll cycle, a host decision) and start
    first in, first out within a lane; they end their waits on `Cancel` tokens (client close, a
    read's end, a poller's stop); the stricter declaration of the sharing sessions wins. One
    INFO line per session and subscription at the first 429, one INFO line for a long static
    read, one WARNING for a `poll()` whose cycles outlast their interval and one per run of
    cycles that end late, and a new
    `m.rate_limits()` diagnostics table, one row per bucket. There is no plan enum and no
    user-created governor object: a key holds up to five tiers, and the only REST fact a tier
    carries is its limit. Live tests share one real-clock governor per key for the run, with
    the classes the run's plans leave on Basic declared ([section 13.4](#134-rest),
    [section 17](#17-diagnostics), [section 19.1](#191-the-rest-client)).
61. **A debug log good enough to fix a bug from** (2026-10-28). Standard `logging` only, off
    by default; `DEEPHAVEN_MASSIVE_LOG` (and `DEEPHAVEN_MASSIVE_LOG_FILE`), read at the first
    session, and `dm.enable_debug_logging()` turn it on; stderr by default and a file
    only when asked for. Log too much rather than too little: offending values, whole rows, and
    error bodies at DEBUG, the same values in error messages, with no truncation that hides the
    problem. Secrets are never logged. Every change logs what it decides (the rules in the
    `_debug_log` docstring). The bug report asks for the log and `m.streams()`, and adds no
    warning about what a log contains ([section 17.1](#171-the-log)).
62. **Each read gives up on its own; reference data has its own bucket; learned Basic lives
    with its governor** (2026-10-29, revising decision 60 after an adversarial review; the
    owner decided most of it, the coordinator three details, each named below).
    *The owner decided:*
    - Each read gives up on its own, with no setting: after about 10 minutes, or about 90 s
      when its thread holds the update graph's lock (a console command), with a clearer
      error. The bucket never gives up: it keeps pausing and probing, so one session giving
      up no longer fails another's requests, a stream's host decision included. The give-up
      error names the subscription, when its next request, a probe, goes (in UTC and in local
      time), the patience and how much of it the read spent, the usual cause (another program
      or process using the same account), and the remedies (retry after that time, stop the
      other program, and for a subscription declare it Basic or upgrade it).
    - `other` learns Basic from a 429 that names the per-minute limit, as a subscription
      does; declaring it still raises.
    - Massive's knowledge-base article "What is the request limit for Massive's RESTful
      APIs?", which says reference and cross-asset endpoints are counted in a bucket of their
      own, is vendored by the refresh as
      `vendor/massive/knowledge-base/what-is-the-request-limit-for-massives-restful-apis.md`,
      and `--check` reports any change to its text.
    - Reference reads filtered by market and the session's crypto ticker list are charged to
      `other`, not to the class they filter by.
    - What a governor learned is forgotten with the governor, once the last session of its
      key has gone and it holds no slot or pause, rather than kept for the life of the
      process: relearning costs one refused request and one WARNING. The `REFUSED` state of
      `m.rate_limits()` goes, since no bucket gives up.

    *The coordinator decided (the owner may object):*
    - Every other reference endpoint (`/v3/reference/...`, `/vX/reference/...`,
      `/v2/reference/news`) is charged to `other` too, whatever its dataset's class, options
      contracts and IPOs included: consistent with the owner's approval of a separate
      reference bucket. Item 66's live check is to confirm it.
    - A read's patience counts only time spent waiting out pauses, each stretch once; slot
      waits never count: the coordinator's reading of the owner's approval to give up after
      about 10 minutes paused.
    - A caller that joins a shared lookup (the crypto ticker list, a host decision) bounds
      its own wait with its own patience and keeps sharing the lookup, and tries again when
      the caller making it gives up or is interrupted (issue #206).
    ([section 13.4](#134-rest), [section 17](#17-diagnostics)).
63. **One-row endpoints are datasets of their own** (2026-10-29; the coordinator decided after
    an adversarial review by a Deephaven quant user and a library maintainer; the owner may object).
    The last trade, the last quote, and the previous day's bar become `last_trade`, `last_quote`,
    and `previous_day_bar`, each with `fetch()` and `poll()`, in every class Massive has them;
    `fetch()` leaves the stream and daily-bar queries, and with it the `StreamHistoryLatestQuery`
    and `HistoryLatestQuery` classes and `indices.values.fetch()`, which was the indices snapshot
    that `m.indices.snapshots` already serves. `fetch()` then always means every row the endpoint
    returns. The one-row datasets have the columns of their kind that their endpoint fills, and a
    missing verb's `AttributeError` names the dataset that has it. Reason: datasets are named by
    what they are and verbs name table shapes; a new verb such as `latest()` would break the second
    ([section 7.5](#75-one-row-datasets), [section 7.6](#76-a-verb-a-query-does-not-have)).
64. **Columns join, merge, and show only what a dataset fills** (2026-10-29, from the API
    review). The owner decided one part, the coordinator the rest after an adversarial review,
    each named below.
    *The owner decided:*
    - A dataset keeps only the columns of its kind that some source of its own (REST,
      WebSocket, or flat files, across all its overloads) can fill, in the kind's order; a
      column only another transport of the same dataset fills stays, so a history table and a
      live table of one dataset still merge for a backfill, and a dataset's columns never
      depend on a call's arguments. The library's own columns stay wherever their inputs exist,
      and a dataset that spans classes (`m.snapshots()`, `m.indicators.*`) keeps the union of
      its classes. There is no merge helper: every page of the column reference whose datasets
      share a kind gives the recipe `merge([t.view(common) for t in tables])`. A generated test
      checks that every column of every dataset names a transport in `SuppliedBy`, and the
      column-reference command reports each dataset's added, removed, and retyped columns.
      This replaces principle 1's union rule ("a column a class does not supply is null") and
      removes principle 12's "a column only other members of its kind supply" text, and its
      "Always null" description, from the merge.
    *The coordinator decided (the owner may object):*
    - Every numeric id or code is a `long`, alone or in a list: `int-array` becomes
      `long-array` (`long[]`, 64-bit elements), so condition codes and indicators join
      `m.reference.conditions` `Id` with no cast; a `number` of format `int32` or `int64` maps
      to `long`, which makes the option chain's quote exchange ids `long`.
    - A list of plain text is `string-array` (`String[]`), by one generator rule; any other
      nested value stays JSON text, and its description ends with "The column holds Massive's
      value as JSON text." A null list element is kept rather than failing the page.
    - A poll key may be a list (the 13-F `OtherManagers`): the keyed `poll()` tables key their
      `last_by` on a hidden JSON-text column of the list keys, since Deephaven keys no
      aggregation on an array.
    - The unified snapshot is named `snapshots` everywhere (a dataset the session serves itself
      has one plain name), is declared in its own module, `_namespaces/snapshots.py`, and has
      its own column-reference page, `snapshots.md`. Its options rows gain `Underlying` (an
      optional `fanout` key, a new allowance for a REST key only some rows carry) and
      `OptionRoot` (`DatasetSpec.option_rows`), as `m.options.snapshots` has.
    - The per-dataset column report runs in the column-reference command, not in the refresh:
      the wire package cannot see datasets, and the command is run after every refresh.
    ([section 7.4](#74-column-descriptions), [section 8.11](#811-msnapshots),
    [section 11](#11-enums-and-types), [section 19.4](#194-generated-schemas)).
65. **The library reports what streams and reads are doing** (2026-10-29; the coordinator
    decided each point after an adversarial review by a Deephaven quant user and a library
    maintainer; the owner may object to any of them, and an objection reopens it).
    - The diagnostics tables spell transports as `columns()` does: `WebSocket`, `REST`, and
      `flat files`, in `Transport` and in a stream's `SchemaDrift` entries; the internal
      enum names (`WS`, `FLAT_FILES`) appear in no table. This edits a detail of this
      document under the owner's rule of one spelling in user-facing text.
    - `m.streams()` gains a `Query` column, the `repr` of the query that opened the stream or
      `poll()`, so two streams of one dataset can be told apart.
    - `dm.disable_debug_logging()` turns off what `enable_debug_logging()` or the environment
      variables turned on, at any level; it takes no argument.
    - The `POLYGON_API_KEY` fallback goes; the credentials keep Massive's names. One name per
      setting, no fallbacks, and nothing warns about the old name, since the library has no
      users yet.
    - `history()` under `AUTO` warns once per session, dataset, and set of columns when the
      chosen source leaves null columns the other fills and the other source could serve the
      call, now (naming the `source=` that fills them) or once a day's file is published
      (saying the call changes source the next day); every `history()` table's description
      records its source and why. A table must never silently change what it fills; a global
      sort, or a REST-only `AUTO`, costs far more than it buys.
    - `poll()` on a feed (a dataset whose default mode is `APPEND`: news and filings) refuses
      to start with neither `start` nor `limit`, naming the fix, since every cycle would
      otherwise re-read Massive's whole archive; a `limit` alone is the "latest headlines"
      poll. A moving window that reads only new rows is a separate design item.
    ([section 5](#5-massiveconfig), [section 7](#7-query-classes-and-verbs),
    [section 16](#16-historysourceauto), [section 17](#17-diagnostics),
    [section 17.1](#171-the-log)).
66. **One setting enum for bad symbols, and both default to log and keep running** (2026-10-29;
    the owner's decisions, turned into a design by the coordinator after an adversarial review
    by a Deephaven quant user and a library maintainer). One enum, `dm.OnError {RAISE, LOG,
    IGNORE}`, replaces `OnError {RAISE, NULL}` and `UnknownSymbols {FAIL, WARN}` for every
    symbol problem. `RAISE` fails (raises for a static input, fails a live table); `LOG` keeps
    going and logs each distinct value once, bounded, at the level the cause takes (WARNING for
    a user's row, ERROR with the library-bug text for a spelling the library cannot read);
    `IGNORE` keeps going with nothing logged above DEBUG. Two `MassiveConfig` settings use it,
    both `LOG` by default (the owner's decision, which replaces the earlier "fails by
    default"): `noncanonical_symbols`, for a row of a symbol table that is not Massive's exact
    ticker, and `unreadable_symbols` (renamed from `unknown_symbols`), for a symbol Massive
    sends in a spelling the library cannot read. Their environment variables are
    `DEEPHAVEN_MASSIVE_NONCANONICAL_SYMBOLS` and `DEEPHAVEN_MASSIVE_UNREADABLE_SYMBOLS`, taking
    `raise`, `log`, or `ignore`, under the library's own prefix. A `str` or a list in code
    always raises, whatever the setting, and `canonicalize_symbols(on_error=OnError.RAISE)`
    keeps its own argument, since it is a standalone function. No per-dataset parameter: it
    would act only when a symbol table is passed, and nobody wants a different strictness per
    query. The setting governs every row read from a symbol table, present at the call or added
    later, for streams and `poll()`: under `RAISE` a bad row present at the call raises at
    once; under `LOG` or `IGNORE` the row is skipped and listed in the result's `m.streams()`
    `Error`, a table whose rows are all bad raises `ValueError` rather than start an empty
    result, and fixing or deleting a row clears it, a fixed row being subscribed. An unreadable
    spelling under `LOG` or `IGNORE` keeps its row with a null `Symbol` and its text in
    `RawSymbol`, which every dataset that reads its symbol from rows has under both, so the log
    choice never changes the schema: the one exception the owner's column rule (each dataset
    keeps only the columns its sources can fill) allows, and `columns()` and the column
    reference say so ([section 5](#5-massiveconfig), [section 10](#10-symbols-and-all)).
67. **One name, form, and default per parameter** (2026-10-29; the coordinator decided after an
    adversarial review by a Deephaven quant user and a library maintainer; the owner may
    object). `symbols` is positional on every dataset, the four list datasets that took it by
    keyword included (`reference.tickers`, `reference.news`, `options.contracts`,
    `partners.benzinga.news`). `as_of` replaces `date` as the point-in-time parameter of
    `reference.tickers` and `reference.ticker_details`, since `date` names the trading day
    itself elsewhere. `stocks.filings.institutional_holdings(filer_cik=)` takes one CIK or a
    sequence, like `issuer_cik` and `owner_cik`. `FilterValue` admits lists of numbers and
    dates, which documented filters need. Where Massive's spec gives a parameter a default, the
    signature shows it (the indicators' windows and `adjusted=True`), checked against the
    generated `Param.default`. `sort` is checked at construction against the spec's sort
    fields, as `filters` is. None means only "not sent" or "no value given", never a mode:
    `poll(mode=)` defaults to `PollMode.AUTO` (each dataset's own mode, which its docstring
    names, as `HistorySource.AUTO` is for `history()`), `last_by(by=)` to `"Symbol"`,
    `options.contracts(expired=)` is `bool = False`, the futures `venue=None` becomes
    `venues=frozenset(FuturesVenue)`, and the indicators' `adjusted` is `bool = True`, with
    `False` on a crypto symbol still raising. `order`, `sort`, `start`, `end`, and a list
    dataset's `symbols` keep None, which there means "not sent"; `order` and `sort` are sent
    only together on most paths, so a default for one would force the other
    ([section 8](#8-namespace-tree), [section 9](#9-filters)).
68. **Long downloads read in the background with `wait=False`** (2026-10-29; the owner chose a
    parameter that says whether the call blocks, the coordinator the details). `fetch()` and
    `history()` take `wait: bool = True`. `wait=False` returns at once a refreshing `Table` with
    the dataset's columns and no rows; the complete result lands in one update, as one
    constituent of a merged partitioned table built off the update-graph lock, so no rows are
    copied inside a cycle, and never changes after. No `Future`: its `result()` could not wait
    inside a console command. Argument errors raise at the call; a failed download fails the
    table. `m.streams()` gains `Kind` (`stream`, `poll`, `read`) and the states `LOADING` and
    `DONE`. The result has its own liveness scope; releasing it, garbage collection, or
    `close()` cancels the download. The read holds no lock, so the console's 90 s patience does
    not apply, and the console's give-up error and pause warning name `wait=False`. The table
    description cannot change once Deephaven has published the table, so it states the
    behaviour instead of moving from "loading" to "complete"; `m.streams()` shows the change
    ([section 6.1](#61-how-a-live-table-is-kept-alive-and-released),
    [section 7](#7-query-classes-and-verbs), [section 17](#17-diagnostics)).
69. **More inputs and a list of datasets** (2026-10-30; the owner approved each item in the
    API review and decided the three questions it raised, marked *owner*; the coordinator
    decided the other details after an adversarial review by a Deephaven quant user and a
    library maintainer, and the owner may object to any).
    - Date and time arguments (`history()`'s bounds and every `start=`, `end=`, `as_of=`, and
      single day) take the natural native inputs, the set Deephaven's own `deephaven.time`
      converters take (*owner*): Java `Instant`, `ZonedDateTime`, and `LocalDate`, a
      `numpy.datetime64`, and a `pandas.Timestamp`, beside text, `date`, and `datetime`. A
      Deephaven library takes the values Deephaven hands its users, rather than making them
      format text in a second grammar. The conversion lives in `_datasets/dates.py`, outside
      the wire package.
    - Integers are refused in every public date argument (*owner*): Deephaven reads one as
      epoch nanoseconds, which would silently misread a date written as `20250303`. Internal
      APIs may pass integers, such as epoch nanoseconds, directly as an optimization; nothing
      is added for that, and nothing blocks it.
    - Deephaven's own date and time text is accepted (*owner*), read by Deephaven's parser
      rather than a second hand-written grammar: any text `to_j_local_date` or `to_j_zdt`
      reads, such as `"2025-03-03T09:30 America/New_York"`. ISO-8601 is read exactly as
      before, and date-only text stays a calendar date. The API layer converts the text
      (`_tables/time_text.py`), since the dataset layer imports Deephaven only for type
      checking; it is called where the arguments arrive, which is simpler than passing a
      parser object down through the dataset layer. Deephaven's instant parser is not used, so
      bare digits are never read as an epoch count. Text neither form reads raises one error
      naming both.
    - A `numpy.datetime64` finer than a day is an instant in UTC, as Deephaven reads it
      (*owner*); a day is a date. A week, month, or year is refused, naming a day: as an `end`
      it would shrink the range to its first day, and a numpy week starts on a Thursday.
    - A `pandas.Timestamp` without a timezone is refused like a naive `datetime`: read as UTC,
      a New York user's 09:30 would silently become 04:30. A `numpy.datetime64` has no zone
      slot at all, so it stays UTC.
    - An instant with nanoseconds below the microsecond (Java, numpy, or pandas) is moved
      outward to a microsecond, `start` down and `end` up, so no row is lost and `end` may
      include up to 999 ns more (*owner*): Deephaven's instants carry nanoseconds the user
      never chose. An `end` past the last microsecond of year 9999 becomes `datetime.max`.
      ISO-8601 text finer than a microsecond, refused before, is rounded the same way, so text
      and values follow one rule (coordinator, following the owner's wish for
      Deephaven-like inputs).
    - Every ISO-8601 date form is a date (`"20250303"`, `"2025-W10-1"`), as Python reads it,
      and Deephaven's own date text (`"2025-3-3"`) too. Deephaven text that names a day and a
      zone but no time (`"2025-03-03 ET"`) is refused: Deephaven reads it as that midnight,
      so as an `end` it would drop the day. A refusal keeps Deephaven's own reason and names
      the zones it reads (coordinator).
      `filters=` values are left to the change that settles their type.
    - `tests/test_layout.py` lists the public `queries` module and the API-layer
      `_dataset_list` module (*owner*).
    - `dm.datasets()`, a static table of every dataset with its summary, verbs, transports,
      and documentation link, at module level, since the catalog needs no key or session
      ([section 7.7](#77-the-dataset-list)).
    - The query classes and the argument aliases are public, in `deephaven_massive.queries`,
      which the package never imports: annotating a helper should not need a private import.
      The module also exports `Symbols`, `DateLike`, and `FilterValue`, since a helper that
      takes a query usually takes its arguments too.
    - `m.reference.news` follows a watchlist: `symbols` stays plural (one ticker, a list, or a
      live symbol table), the library sends one request per ticker and keeps an article
      several tickers return once, by `Id`. `limit`, `sort`, and `order` apply per ticker, so
      one ticker's articles never crowd out another's. A singular `symbol` would have removed
      watchlist support, which must stay (decision 4).
    ([section 3](#3-exports), [section 8.7](#87-mreference), [section 11](#11-enums-and-types)).
70. **Bars are always served; an unexpected stamp warns, never refuses** (2026-10-30; the owner
    decided: "The library should be serving up bars, not excluding them"). No `history()`,
    `fetch()`, or `poll()` call fails, and no row is dropped, because a bar's stamp is not one
    Massive is known to use for its class and timespan. Every bar of a day or longer of a class
    with a day zone is stamped the same way, at 00:00 of the day its stamp falls on in the class
    day zone, the best rule available, and its `Date`, `TimestampStart`, and `TimestampEnd`
    follow from that day ([section 12](#12-timestamp-units-and-day-boundaries)). An unexpected
    stamp logs one WARNING per dataset and timespan per session, through the session's
    once-per-session warnings: it names the dataset, the source, the stamp, and the stamps
    expected, says that the bar's date label may cover other days than the bar does, and points
    to the project's issues page. The WARNING names no other source or verb to use instead,
    since every source and verb serves the bar. This replaces the refusals of decisions 52 and
    56. Refusals of the request's own arguments still raise at the call, and the adjusted-basis
    guard (a page whose `adjusted` echo differs from the request raises `MassiveError`) stays:
    it keeps one table from mixing split-adjusted and as-traded prices, which is not a stamp
    question.
71. **Every `bars()` call names its bar size** (2026-10-30, owner). `timespan` is required,
    with no default, on the stocks, options, futures, indices, forex, and crypto `bars()`; a
    call without it is a static type error and a `TypeError`. No length suits most uses, and
    the length is the first thing a reader of the call needs. The indicators keep
    `timespan=Timespan.DAY` ([section 7.1](#71-bars-overloads)).
72. **A futures contract is a symbol** (2026-10-30, owner). `futures.contracts` names the row's
    `ticker` `Symbol` (with `RawSymbol`), takes optional positional `symbols` sent as
    `ticker.any_of`, polls by `Symbol` and `Date`, and with `products` takes `as_of`, sent as
    `date`. A contracts table then feeds the market-data datasets directly. Benzinga consensus
    ratings take `Symbol` from the row's own `ticker` (`KeySpec("Ticker")`), as the other
    Benzinga datasets do ([section 8.3](#83-mfutures)).
73. **A query's repr is the call the user typed** (2026-10-30, owner): public names, `symbols`
    first and positionally, `… (N more)` after five symbols, a required argument by position,
    and only the arguments that differ from their defaults
    ([section 7](#7-query-classes-and-verbs)).
74. **`limit` caps each symbol's rows where a request names one symbol** (2026-10-30, owner),
    so a few symbols with many rows never crowd out the rest; elsewhere it stays a total cap.
75. **Price columns lead every table** (2026-10-30, owner): the naming policy's rule 6b,
    applied at refresh time by `naming.lead_order`.
76. **The debug log says what it needs to** (2026-10-30, owner). A rate-limit pause logs its
    length, its reason, and its end, and each GET its wait for a permit; an HTTP 429 logs once.
    The WebSocket client's `max_connections` warning and its reconnect attempts log at DEBUG,
    since the connection already warns once per outage, and its close line gives the server's
    close reason (without the key) and how long the connection was up. The bug-report form
    asks for the log as an attached file. The environment line names the git commit and the
    platform; `DEEPHAVEN_MASSIVE_LOG_FILE` alone turns the log on at DEBUG; the line that turns
    it on gives the file's resolved path; a session's configuration shows `s3_access_key_id`
    only as set or unset. The library never rotates the log file
    ([section 17.1](#171-the-log)).
77. **A crypto day summary not in UTC is served with a warning** (2026-10-30, owner). A crypto
    `daily_open_close` row whose `isUTC` is false is returned with the UTC day's window and
    logs one WARNING per dataset per session, through the session's once-per-session warnings
    as decision 70's bar stamps do: its day boundary differs from the other crypto tables', so
    its dates may not line up with theirs. It never raises.
78. **`AUTO` serves from whichever source can; it errors only when none can** (2026-10-30,
    owner). `history()` must return the data whenever any source can, and Massive's publish
    times are approximate and change, so nothing is guessed from the clock: the
    `flat_file_publish_cutoff` setting, the publication time, and its one-day grace are gone.
    Under `AUTO`, when REST can serve the request, each trading day whose flat file is
    missing is read from REST as a one-day read merged in date order (no holiday is
    inferred, so a day Massive missed is never dropped), and a flat-file refusal (HTTP 403,
    or rejected S3 credentials) reads the whole call from REST, each with one WARNING; the
    table description names REST and its days. `FLAT_FILES`, or a request REST cannot serve, raises, with a
    message that says the publication time is approximate and what to do. Mixing sources in
    one call can leave null, on REST's rows, a column only flat files fill; returning the data
    outweighs that ([section 16](#16-historysourceauto)).

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
| 1 | Day cut of crypto 1-day bars on every source, checked first: the documented REST samples stamp a daily bar at midnight New York time (the aggregates range, `/prev`) or at no day boundary at all (grouped daily, at 19:18, 20:08, and 20:48 UTC), while the `global_crypto/day_aggs_v1` sample stamps it at 23:59:59.999 UTC of its day, and both pages' prose says UTC | A 1-day bar means the UTC day: a stamp at the first or last millisecond of a UTC day is moved to its 00:00 UTC, and any other stamp, or a REST bar of several days or longer not at 00:00 UTC, is moved to 00:00 UTC of the day it falls on and served, with one WARNING per dataset and timespan per session (decision 70). `AUTO` picks the source as for every dataset, so the stamp rule alone keeps both sources on the same UTC days. If live REST follows the samples, crypto daily REST history, `dm.ALL` grouped daily, and `/prev` all warn, and the owner decides the source rule then | `tests/namespaces/test_crypto.py`, `tests/ingest/test_rest.py`: both cuts map to 00:00 UTC on every source; the documented samples and a two-day bar at midnight New York time are served with one WARNING | `tests/namespaces/test_crypto_integration.py`: one 1-day bar of `X:BTCUSD` from `/prev`, the range, grouped daily, and `day_aggs_v1` for one day; record each `TimestampStart` and whether the OHLCV agree |
| 2 | Unit of REST forex `/v1/last_quote` `timestamp`, crypto `/v1/last` `timestamp`, and forex `/v1/conversion` `last.timestamp` | Registered as ms (`unverified-ms`); the spec tags all three fields `IMilliseconds`, which agrees; on a mismatch the overflow check and the [1990, 2100] bound drop every row and the page raises `RowDataError` | `tests/datasets/test_timestamps.py`: ms and ns fixtures; the out-of-range path is masked | `tests/datasets/test_timestamps_integration.py`: one request each; the converted `Instant` is within 1 day of now |
| 3 | Unit of WebSocket `FMV` `t` (documented ns, sample ms) | Magnitude rule for FMV only; logged once | `tests/datasets/test_timestamps.py`: both magnitudes convert correctly | `tests/namespaces/test_crypto_integration.py` (Currencies Business): one FMV frame converts to a current `Instant` |
| 4 | Futures flat files keyed by session date | Check every block of every futures file against its key; fail loudly otherwise | `tests/massive/flatfiles/test_reader.py`: matching and mismatching fixtures | `tests/ingest/test_flatfiles_integration.py` (S3 credentials): one key per venue |
| 5 | Indices `V` host (`business.` on the page; `socket.`/`delayed.` in the plan table) | `V` uses the class TICK host; a refusal names `ws_hosts[AssetClass.INDICES]` and `business.massive.com` | `tests/ingest/test_host_decision.py`: both replies | `tests/ingest/test_host_decision_integration.py`: subscribe `V.I:SPX` on the resolved host |
| 6 | TICK host for a base Business key | `business.massive.com` with a WARNING | `tests/ingest/test_host_decision.py`: the snapshot shape with `fmv` and no trades/quotes resolves to `business.` | `tests/ingest/test_host_decision_integration.py` (Business key): `AM.AAPL` is acknowledged on the resolved host |
| 7 | Futures snapshot `timeframe` fields (in the OpenAPI spec; absent from the documentation page) | Absence yields `socket.` with `Recency=UNKNOWN` and the explicit-verdict rule | `tests/ingest/test_host_decision.py`: with and without the fields | `tests/ingest/test_host_decision_integration.py`: record which fields the live payload carries |
| 8 | Exact text of the delayed-key verdict message ("real-time") | Case-insensitive substring match; limited to `Recency=UNKNOWN`, whole-step refusal, unpinned host | `tests/ingest/test_ws.py`: verdict and non-verdict messages | `tests/ingest/test_ws_integration.py` (delayed key): subscribe on `socket.` and capture the message |
| 9 | Unsubscribe acknowledgements | Not awaited; topics are removed immediately | `tests/ingest/test_ws.py`: removal without an acknowledgement | `tests/massive/ws/test_client_integration.py`: record whether an acknowledgement arrives |
| 10 | Subscribe frame size limit | At most 1,000 topics per frame | `tests/ingest/test_ws.py`: 2,500 topics produce three frames | `tests/massive/ws/test_client_integration.py`: a 1,000-topic frame is acknowledged |
| 11 | Options cap counted in contracts or topics | Topics (the stricter count) | `tests/ingest/test_ws.py`: `T.X` plus `Q.X` counts as two | `tests/massive/ws/test_client_integration.py` (Options Advanced): 501 contracts on `T` and `Q` |
| 12 | Exact `max_connections` status text and timing | Any status `max_connections`, in the auth reply or after it, shows `WAITING_SLOT` | `tests/ingest/test_ws.py`: both placements | `tests/ingest/test_ws_integration.py`: two sessions with one individual key |
| 13 | `orjson` free-threaded wheels | Optional extra; standard-library codec by default | `tests/massive/net/test_codec.py`: codec selection with and without `orjson` importable | CI: the 3.14 job installs the extra (`--extra orjson`) and runs the suite with it; the 3.14t job does not, because `orjson` publishes no cp314t wheel |
| 14 | Flat-file publish time (documented "approximately 11:00 AM ET") | Settled by owner decision 78: no publish time is assumed. The live check of 2026-10-30 found weekday files landing at about 00:45 ET, but Friday's on Saturday at 10:27 and 11:17 ET, so the time varies. A missing trading day's file with a later day's file is a day without trading; one with no later file is not published yet, and `AUTO` reads it from REST when REST can serve the request, else the call raises with a message that the time is approximate | `tests/ingest/test_flatfiles.py`: a missing day with and without a later file, under `FLAT_FILES` and `AUTO`; `tests/test_query.py`: `AUTO` reads the unpublished days from REST | `tests/massive/flatfiles/test_client_integration.py`: record when each of the last ten weekdays' files was published, Fridays' included |
| 15 | Whether the server delivers an event twice when a wildcard and an explicit topic of the same channel are both subscribed (`T.*` plus `T.AAPL`) | While a channel's acknowledged wildcard is desired, its explicit topics are off the wire; transitions make before they break, accepting a brief duplicate ([section 13.3](#133-connection-and-subscription-protocol)) | `tests/ingest/test_ws.py`: adding, refusing, and removing a wildcard over explicit topics sends the expected frames in the expected order | `tests/massive/ws/test_client_integration.py`: subscribe both on a test key and record whether duplicates arrive |
| 16 | A missing flat-file key, requested through `files.massive.com`, answers HTTP 404 with S3's XML error and the code `NoSuchKey`, not 403 `AccessDenied` (S3 answers 403 for a missing key when the credentials lack `ListBucket`, which would turn weekends into `EntitlementError`), and not a 404 page the front end rewrites (a wrong path gets an HTML "Massive 404 Not Found" page, which botocore reads as the code `404`). If it answers anything but `NoSuchKey`, the library raises on every range that holds a weekend | Only a 404 `NoSuchKey` is a missing file; any other 404 raises, naming the bucket and the endpoint settings; a 403 is an entitlement failure (section 16) | `tests/massive/flatfiles/test_client.py`: `NoSuchKey`, other-code, and HTML 404 replies, and 403 replies | `tests/massive/flatfiles/test_client_integration.py` (S3 credentials): request a weekend key and record the status, the error code, and the content type |
| 17 | Retired: `poll()` results are no longer keyed `input_table`s but shapes of a publisher's blink table ([section 14](#14-publishers-fan-out-and-failure-semantics)), so no input-table attribute is involved | | | |
| 18 | Whether the Deephaven web UI shows a table's `TableDescription` | Set on every result anyway; it costs nothing and every API client can read it | `tests/tables/test_columns.py`: every result shape carries `TableDescription` | None: the implementer opens one result in the web UI of a local server and records in the pull request whether the description appears |
| 19 | Form of WebSocket forex `C` `x`: declared an integer, sampled as the text `"44"` | `quoted-integer` reads an integer or decimal-digit text; anything else fails the row loudly | `tests/massive/spec/test_convert.py`: both forms convert, `"4.8"` and `48.5` fail | `tests/massive/ws/test_client_integration.py`: one raw `C` frame; record the form of `x` |
| 20 | Form of WebSocket crypto `XT` `i`: declared and sampled as an integer, while REST and the flat file send text | `text-or-integer-id` keeps either form as text | `tests/massive/spec/test_convert.py`: both forms convert | `tests/massive/ws/test_client_integration.py`: one raw `XT` frame; record the form of `i` |
| 21 | Unit of WebSocket `LULD` `t` (documented ms, sample of 19 digits) | Registered as ns (unverified); on a mismatch the overflow check and the [1990, 2100] bound drop every row, and the stream fails after `DROPPED_RUN_LIMIT` of them | `tests/massive/refresh/test_overrides.py`: the unit is ns and unverified | `tests/namespaces/test_stocks_integration.py`: one `LULD` frame converts to a current `Instant` |
| 22 | Unit of flat-file `us_indices/values_v1` `timestamp` (documented ms, example rows of 19 digits) | Registered as ns (unverified); the bound fails loudly on a mismatch | `tests/massive/refresh/test_overrides.py`: the unit is ns and unverified | `tests/ingest/test_flatfiles_integration.py` (S3 credentials): one values file's first row converts to its day |
| 23 | Where the futures snapshot carries a contract's ticker and product code: the OpenAPI response schema nests both under `details` (`details.ticker`, `details.product_code`), while the OpenAPI response example and the documentation sample both send `ticker` and `product_code` at the top level of each result, with `details` holding only `settlement_date`; the path's `ticker.any_of` filter and its default sort `ticker` also name a top-level field | Two `KEY_FIXES` entries read the fields as `ticker` and `product_code`, columns `Ticker` (the `symbol` key, so `Symbol`) and `ProductCode`; were the wire to nest them as the schema says, every row would fail loudly as a missing required key and `details.ticker` and `details.product_code` would be reported as drift ([section 19.5](#195-runtime-drift)) | `tests/namespaces/test_futures.py`: the vendor's own sample, served unchanged, fills `Symbol` and `ProductCode` from the top level | `tests/namespaces/test_futures_integration.py`: one `snapshots` request for current contracts; record where `ticker` and `product_code` appear and that `Symbol` is filled |
| 24 | Key of the REST ticker events' event type (`/vX/reference/tickers/{id}/events`): the schema declares the required `event_type`, while the response example, the documentation sample, and Massive's Python client all use `type` | A `KEY_FIXES` entry reads the field as `type`, column `Type`; were the wire to send `event_type`, `Type` would be null and `event_type` would be reported as drift | `tests/massive/refresh/test_overrides.py`: the entry reads `event_type` as `type`, column `Type` | `tests/namespaces/test_reference_integration.py`: one `ticker_events` request; record which key the events carry |
| 25 | Whether rows of the stocks `filings.index`, `filings.sections_10k`, `filings.text_8k`, and `filings.risk_factors` always carry `ticker`: no filings response schema marks it required, and the index documents it as present only "if available" | Each of the four declares its symbol key `KeySpec("Ticker", required=False)`: a row without `ticker` keeps a null `Symbol` instead of failing the request, and none keys a `poll()` on `Symbol`, in any mode | `tests/ingest/test_rest.py`: a row without its optional symbol keeps a null `Symbol`; `tests/datasets/test_catalog.py`: only a symbol key from REST rows may be optional; `tests/namespaces/test_stocks.py`: a row without `ticker` of each of the four keeps a null `Symbol` | `tests/namespaces/test_stocks_integration.py`: one request over a month of each of the four; record how many rows lack `ticker` |
| 26 | Whether Massive reads a percent-encoded colon in a path segment (`/v2/aggs/ticker/I%3ASPX/prev`) as `I:SPX` | Every path value is percent-encoded whole; a value containing `/` is refused before sending, since Massive answers 404 for `%2F` | `tests/massive/rest/test_query.py`: `I:SPX` becomes `I%3ASPX`; a `/` raises | `tests/namespaces/test_indices_integration.py`: one `I:SPX` `/prev` call returns a row for `I:SPX` |
| 27 | Spelling of an explicit forex pair topic: the WebSocket documentation gives `{from}-{to}` (`C.EUR-USD`), while quote and aggregate frames spell the pair `EUR/USD` | Topics use `EUR-USD`; frames of any spelling map back to `C:EURUSD` | `tests/datasets/test_symbols.py`, `tests/ingest/test_ws.py`: `C.EUR-USD` is subscribed and an `EUR/USD` frame reaches its stream | `tests/namespaces/test_forex_integration.py`: subscribe `C.EUR-USD` and `CA.EUR-USD`; record the acknowledgement and that frames arrive |
| 28 | Ticker form in the forex quotes flat files: the vendor sample shows `X:EUR-USD`, while REST uses `C:EURUSD` | The filter holds every spelling of each requested pair (`wire_spellings`), and each row's ticker is read with `canonical_from_wire`, so `X:EUR-USD` rows hold `C:EURUSD`; any other spelling follows `unreadable_symbols` | `tests/namespaces/test_forex.py`: a file written `X:EUR-USD` matches `C:EURUSD` and every row holds the canonical pair; `tests/ingest/test_flatfiles.py`: an unreadable ticker fails the read, or is kept in `RawSymbol` under `LOG` | `tests/ingest/test_flatfiles_integration.py` (S3 credentials): read one forex quotes file and record the ticker form |
| 29 | Futures holiday sessions and candle snapping: whether a session after a holiday (2025-01-21 after Martin Luther King Day) opens at the previous regular open or earlier, whether 4 days of margin cover every holiday run of bars named by date, whether 7 days cover every flat-file roll-forward, and whether `window_start` filters on session, week, month, and year candles snap to the candle's start | Trades and quotes named by date are requested by `session_end_date`, which needs no margin; bars named by date are requested from the session's regular open less 4 days and post-filtered on their `session_end_date` (`Date`); bars named by instants are filtered locally on their own window; an instant range whose trailing missing files hold a weekday rolls forward up to 7 days and raises `MassiveError` if the next file is unpublished or absent | `tests/ingest/test_rest.py`, `tests/ingest/test_flatfiles.py`: the 2025-01-21 session's Sunday-evening rows and Good Friday 2024's rows are kept, and an unpublished or absent next file raises | `tests/namespaces/test_futures_integration.py`: minute bars of ES around the 2026-01-20 session, and `YEAR` candles from a year start and from mid-year; record the first bar's time and whether the candles arrive |
| 30 | Whether `/v3/reference/tickers` returns one row per ticker with `active=false`, when delisted listings may reuse a ticker | `reference.tickers` polls keyed by `Symbol`; were several rows to share a ticker, a `poll()` table would keep one of them while `fetch()` shows all | `tests/namespaces/test_reference.py`: the vendor sample decodes with one row per ticker | `tests/namespaces/test_reference_integration.py`: one `tickers(active=False)` request over a letter range; record any ticker on several rows |
| 31 | Whether `(asset_class, id)` identifies one row of `/v3/reference/exchanges` | `reference.exchanges` polls keyed by `AssetClass` and `Id` | `tests/namespaces/test_reference.py`: the poll keys are columns of the schema | `tests/namespaces/test_reference_integration.py`: one `exchanges()` request; record any repeated key |
| 32 | Whether `(asset_class, type, id)` identifies one row of `/v3/reference/conditions` | Settled by the live check of 2026-10-30: it does not. Of 130 conditions, Massive lists stocks `quote_condition` 30 twice, and the evidence does not say which column tells the two apart. `reference.conditions` is therefore keyed by every column Massive sends (`AssetClass`, `Type`, `Id` first), its optional fields nullable keys, so a `poll()` table keeps every distinct condition and merges only rows equal in every column; nothing depends on guessing the distinguishing column | `tests/namespaces/test_reference.py`: the poll keys are every vendor column, and a poll of two conditions sharing one id keeps both | `tests/namespaces/test_reference_integration.py`: one `conditions()` request; record, for each repeated `(AssetClass, Type, Id)`, the columns its rows differ in; fail if two rows are equal in every column |
| 33 | The day a week bar starts on, for stocks, options, indices, forex, and crypto: Monday, or the Sunday before (forex trades from Sunday evening UTC) | Settled by the live check of 2026-10-30: Massive stamps every week bar at Sunday 00:00 in the class day zone (UTC for crypto and forex, New York time for stocks), and the bar holds that Sunday-to-Saturday week. Every week is named by its Sunday and the REST `{from}` is that Sunday; a stock or option week's window is its Monday to Friday, and an index, forex, or crypto week's is its whole Sunday-to-Saturday week. A week bar stamped on any other day than its Sunday is served in the week that day falls in, with a WARNING | `tests/datasets/test_bar_periods.py`, `tests/datasets/test_row_times.py`, `tests/ingest/test_rest.py`, `tests/namespaces/test_indices.py`: Sunday-stamped week bars of every class are kept for their week; a Monday-stamped crypto week is served with a WARNING | `tests/namespaces/test_stocks_integration.py`, `test_forex_integration.py`, `test_crypto_integration.py`: two weeks of week bars each; record each `TimestampStart` and the first and last trade each bar holds |
| 34 | Whether the aggregates range returns a partial bar for a period `{from}` falls inside, and on which day a month bar is stamped | `{from}` is the first instant that belongs to the first bar that fits wholly (a week's Sunday, the 1st of a month), so a partial bucket begun before it is dropped whatever it is stamped; a month, quarter, or year bar stamped on another day than its 1st is served in the period that day falls in, with a WARNING | `tests/ingest/test_rest.py`: the week and month requests start where the first whole period begins; a crypto month bar stamped on the 2nd is served with a WARNING | `tests/namespaces/test_stocks_integration.py`, `test_forex_integration.py`, `test_crypto_integration.py`: month bars with `{from}` in mid-month; record the first bar's stamp and volume against the whole month's |
| 35 | How bars of `multiplier` above 1 are aligned: to clock and calendar boundaries, or to `{from}` | Aligned to boundaries (whole UTC units for intraday bars, period starts for longer ones); a bar spans `multiplier` units from its own start, and a multi-week or multi-month bar must still be stamped on a day its period starts | `tests/datasets/test_bar_periods.py`: five-minute and two-week bars fit by their full span | `tests/namespaces/test_stocks_integration.py`, `test_forex_integration.py`, `test_crypto_integration.py`: five-minute and two-week bars from an odd `{from}`; record their stamps |
| 36 | Whether a futures session can end on a Saturday or Sunday | Sessions are requested on weekdays only; Saturday and Sunday dates are skipped without a request | `tests/ingest/test_rest.py`: a range over a weekend requests only its weekday sessions | `tests/namespaces/test_futures_integration.py`: `schedules` over a month; record every `session_end_date`'s weekday |
| 37 | The stamp of forex 1-day bars on every source: the range (00:00 UTC in the samples), grouped daily, `/prev`, and `global_forex/day_aggs_v1` | A stamp at 00:00 or 23:59:59.999 UTC is moved to 00:00 UTC of its day; any other stamp is moved the same way and served, with a WARNING naming the dataset, the source, and the stamp | `tests/datasets/test_row_times.py`, `tests/ingest/test_rest.py`, `tests/namespaces/test_forex.py`: range, grouped, `/prev`, and flat-file stamps map to 00:00 UTC; a stamp at midnight New York time is served with a WARNING | `tests/namespaces/test_forex_integration.py`: one 1-day bar of `C:EURUSD` from each source for one day, and two weeks of its week bars from the range; record each `TimestampStart`, the week bars' weekday, and whether the OHLCV agree |
| 38 | Whether crypto range and `/prev` 1-day bars cover the UTC day: compare them with the same day rebuilt from 1-minute bars | Item 1's rule: a stamp elsewhere than a UTC day boundary is served with a WARNING | `tests/namespaces/test_crypto.py`: as item 1 | `tests/namespaces/test_crypto_integration.py`: rebuild one day of `X:BTCUSD` from `minute_aggs_v1` over the UTC day and over the New York day; record which matches the range and `/prev` bars, and the weekday of two weeks of `X:BTCUSD` week bars |
| 39 | Whether grouped daily and `day_aggs_v1` hold the same pairs on a day, for forex and crypto | `AUTO` keeps one source for crypto 1-day bars; nothing assumes the sets match | `tests/datasets/test_history_source.py`: crypto 1-day bars keep one source whatever the symbol count | `tests/namespaces/test_forex_integration.py`, `tests/namespaces/test_crypto_integration.py`: one day of `dm.ALL` 1-day bars from both sources; record the pairs only one holds |
| 40 | Which session a futures bar of several sessions or longer is labelled with (`session_end_date` of its first or last session), and how such bars are aligned | Named by dates, such a bar is kept only when it fits the range under both readings of its label, and its `Date` is null; its `TimestampEnd` is the later of the two readings (17:00 Chicago on the latest day its sessions can end on, counted from the session date of `window_start`, plus one session for a fold, with one holiday allowed and one more per five further sessions, or on its label if later; for a week or longer, the last weekday of the latest period its label allows), and a bar is kept only when that end is no later than the range's end and now | `tests/datasets/test_bar_periods.py`, `tests/datasets/test_row_times.py`, `tests/namespaces/test_futures.py`: 2-session, 2-week, and 2-month bars kept under both readings, with their conservative ends; the allowance holds for every start date of the CME calendar of 2024 to 2026; a 2-session bar that could still be open is left out | `tests/namespaces/test_futures_integration.py`: 2-session and 2-month bars of ES over a quarter; record each bar's `TimestampStart`, its label, and the first and last minute bar it holds |
| 41 | Whether Massive keeps an IPO's `issuer_name` unchanged while the offering moves through its statuses (rumor or pending, new, history, postponed, withdrawn); the vendor documents it only as "Name of issuer" | `stocks.ipos` upserts by `Symbol` and `IssuerName`, so two issuers of one ticker stay apart; were Massive to revise a name, the row under the old name would stay beside the new one, since `UPSERT` never deletes | `tests/namespaces/test_stocks.py`: two issuers of one ticker poll as two rows | `tests/namespaces/test_stocks_integration.py`: a year of IPOs; record every ticker whose rows carry more than one `issuer_name` |
| 42 | Whether `IssuerCik` and `TransactionDate` identify one row of the insider activity, and `IssuerCik` and `OwnerCik` one row of the insider holdings; the vendor documents one row per issuer and transaction date, and one per reporting owner per issuer, but marks none of the four fields required | Both datasets replace their `poll()` table keyed by those columns; a null key fails the table, and two rows sharing a key would keep only one | `tests/namespaces/test_stocks.py`: each key column is needed to tell the fixture's rows apart | `tests/namespaces/test_stocks_integration.py`: a year of NVDA's activity and NVDA's holdings; record and refuse any key shared by two rows |
| 43 | Massive's spelling of every US stock ticker: the documentation shows `BRK.A` and `KIMpL` only; warrants, units, rights, when-issued, and test issues are undocumented, and Benzinga and ETF Global rows may spell tickers otherwise (the TMX calendar is global and keeps its tickers as written) | Stock part settled by the live check of 2026-10-30: of the `WARRANT`, `UNIT`, `RIGHT`, and `PFD` tickers, the only spellings outside a root and one suffix were 61 NYSE warrants, 60 written `ROOT.WS` (`AAC.WS`) and one `ROOT.WS.A` (`NE.WS.A`), which the canonical grammar already reads: an upper-case root and the CQS suffixes `.X` (one or more dotted parts), `pX`, `r`, and `w`. The live check had used a stricter pattern of its own; it now asks the library's converter. The partner datasets were not entitled on the test keys, so their spellings stay open. The conventions convert plain tickers, classes, and preferreds only; a row spelled otherwise follows `unreadable_symbols` (kept in `RawSymbol` and logged by default) | `tests/datasets/test_symbols.py`: the grammar, `AAC.WS` and `NE.WS.A` among the canonical tickers, each convention's table and refusals, and the unreadable spellings | `tests/namespaces/test_reference_integration.py`: `tickers(ticker_type=...)` for `WARRANT`, `UNIT`, `RIGHT`, and `PFD`, and one page of each partner dataset; record every spelling `dm.canonical_symbol_stock` refuses |
| 44 | Which endpoints of the indices class send an index without its `I:` prefix | Every dataset of the indices class reads a bare ticker as `I:` and the ticker; datasets of other classes and cross-class datasets never add it | `tests/datasets/test_wire_symbols.py`: `canonical_from_wire` adds the prefix only for the indices class, and no conversion depends on the name | `tests/namespaces/test_indices_integration.py`: one request of each indices dataset, the unified snapshot, and an indicator for `I:SPX`; record the spelling of each row's ticker |
| 45 | `Underlying` of the options datasets for `BRK.B`, `SPX`/`SPXW`, `NDX`/`NDXP`, `VIX`/`VIXW`, and an adjusted contract (`AAPL1`, `DELL1`), and whether the chain takes `I:SPX` as its underlying | `Underlying` is Massive's field as sent; `underlying=` is sent as given; nothing maps a root to an underlying | `tests/namespaces/test_options.py`: the chain's `Underlying` is the fixture's, `underlying=` keeps case and refuses other classes | `tests/namespaces/test_options_integration.py`: one chain and one contracts request for each; record `Underlying` and `OptionRoot` |
| 46 | The deliverable fields of `/v3/reference/options/contracts`: what `correction` means, whether a contract without further deliverables omits `additional_underlyings` or sends an empty list, and what index options report as `shares_per_contract` | The raw fields only (`SharesPerContract`, `AdditionalUnderlyings` as JSON text, `Cfi`, `Correction`), no derived flag; the chain's docstring recipe treats null and `[]` alike | `tests/namespaces/test_options.py`: the contracts schema carries the four fields with their descriptions and no `StandardDeliverable` | `tests/namespaces/test_options_integration.py`: the contracts of DELL1 (2021-11), FF1 (2024-04), an all-cash merger, and SPX; record the four fields, then decide on a derived flag |
| 47 | The shapes of single futures contract tickers: two-digit years (`0NGH39`; the documentation's `ESZ24`) | A root of upper-case letters and digits, a month code, and a one- or two-digit year; a year is never rewritten, and `ESZ25` is not `ESZ5`; combos are item 48. The live check of 2026-10-30 read `contracts(product_code='NG', limit=20)` as 20 rows of `NG:BF F0-G0-H0`: the library reads those 20 rows as one page (its page size is the cap) and refuses a repeated `next_url`, so it repeats no page; Massive sent them. Without `date`, the endpoint returns each contract's point-in-time history, and Massive's raw page of 1,000 NG rows held only 3 tickers, so the rows are most likely one contract on 20 days; the next run records each row's `Date` to settle it | `tests/datasets/test_symbols.py`: `ESZ5`, `6CH5`, `0NGH39`, and `ESZ25` are canonical, `ESZ` and `esz5` are not | `tests/namespaces/test_futures_integration.py`: `contracts(type='single')` for one product over several years; record every ticker shape, and the `(ticker, date)` pairs of the raw page and of the library's rows |
| 48 | The grammar of Massive's futures spread tickers: whether the forms of the vendored samples (calendar `GCQ6-GCZ6`, CBOT inter-commodity `BOB 02-01 U6`, NYMEX strategy `CL:BF Q6-U6-V6` and `CL:BZ U6-V6`, CME cross `6A:XF:AUDUSD:Q6`) are all of them, and how each is built from its legs | Any run of upper-case letters and digits joined by single `-`, `:`, or spaces is a combo, taken as written, so every ticker Massive lists round-trips; a futures row keeps any ticker as written; no function builds a spread from its legs | `tests/datasets/test_symbols.py`: every futures ticker of the vendored samples and the live evidence round-trips as `symbols` and as a symbol-table row, a made-up combo is accepted, and a single contract and a combo refuse each other's function; `tests/namespaces/test_futures.py`: a file of spreads reads every ticker as written, and a spread filters it | `tests/namespaces/test_futures_integration.py`: `products(type='combo')` and `contracts(type='combo')` on every venue; record every ticker form and its legs, then decide on a builder |
| 49 | Whether Massive's crypto ticker list (`/v3/reference/tickers?market=crypto`, read with `active=true` and with `active=false`) holds every pair, delisted ones included, its WebSocket channels and the last-trade and daily open/close paths serve, with a `base_currency_symbol` and `currency_symbol` that join to the ticker | Any `X:` ticker is canonical; wherever a pair must be split (a stream, `/v1/last/crypto/{from}/{to}`, `/v1/open-close/crypto/{from}/{to}/{date}`) the split comes only from that list, both reads once per session (a ticker in both keeps its listed row), and a pair the list does not hold, or whose currencies do not join to it, is refused there; every other path serves it | `tests/ingest/test_ws.py`, `tests/ingest/test_rest.py`, `tests/namespaces/test_crypto.py`: `X:DOTUSD`, `X:SHIBUSD`, `X:USDTUSD`, `X:BTCPYUSD`, and `X:ETHWBTC` subscribe the topic the list gives, `X:BTCXYZ`, unlisted, is refused, and a delisted `X:FTTUSD` splits from the delisted read | `tests/namespaces/test_crypto_integration.py`: both reads of the list; record every row whose currencies do not join to its ticker, and stream a sample of pairs to confirm their topics |
| 50 | The stamps of stock 1-day and longer bars on every source: the range (00:00 New York time in the samples), grouped daily, `/prev`, and `us_stocks_sip/day_aggs_v1` (16:00 New York time, the session close, in the samples), on a full day and on a day that closes early at 13:00 (the Friday after Thanksgiving), and of week and month bars from the range; `/prev`'s short-day stamp can be seen only when a run falls on the trading day after a half day, and every short-day stamp the library accepts (13:00, 16:00, 13:15, and 16:15 New York time) maps to 00:00 of the same New York day | A 1-day stamp at 00:00, 13:00 (a short day's close, presumed), or 16:00 New York time is moved to 00:00 New York time of its day, a longer bar is expected at 00:00 New York time on the day its period starts, and any other stamp is served, labelled by the day it falls on, with a WARNING naming the dataset, the source, and the stamp | `tests/datasets/test_row_times.py`, `tests/namespaces/test_stocks.py`: the documented stamps of every source and a 13:00 close map to 00:00 New York time; 14:30 is served with a WARNING | `tests/namespaces/test_stocks_integration.py`: one 1-day bar of AAPL for the Wednesday before and the Friday after (a short day, closing at 13:00) the most recent Thanksgiving whose week has ended at run time, computed from the exchanges' holiday rule so the dates stay inside every plan's history, from the range, grouped daily, and `day_aggs_v1`; `/prev`, which serves only the most recent completed trading day, read at run time beside the range bar of that same day and marked when that day is a half day; and the week bars of that Thanksgiving week and the week before and the month bars of the two whole months before the run's month from the range, one bar for each period; record each `TimestampStart` and which day's range bar each other source matches, with its largest relative error |
| 51 | The stamps of option 1-day and longer bars on every source: the range and `us_options_opra/day_aggs_v1` (00:00 New York time in the samples) and `/prev` (16:00 New York time); and whether `/prev` stamps an index option (SPXW) at its own 16:15 close, or 13:15 on a short day; `/prev`'s short-day stamp can be seen only when a run falls on the trading day after a half day, and every short-day stamp the library accepts (13:00, 16:00, 13:15, and 16:15 New York time) maps to 00:00 of the same New York day | As item 50, for options, with 16:15 and 13:15 also accepted for 1-day bars (presumed) | `tests/datasets/test_row_times.py`, `tests/namespaces/test_options.py`: the documented stamps and a 13:00 close map to 00:00 New York time; another stamp is served with a WARNING | `tests/namespaces/test_options_integration.py`: one 1-day bar of a listed SPY contract and of a listed SPXW contract from the range and `day_aggs_v1` for the Wednesday before and the Friday after (a short day, closing at 13:00) the most recent Thanksgiving whose week has ended at run time, computed from the exchanges' holiday rule so the dates stay inside every plan's history, and their week bars of that Thanksgiving week and the week before from the range; the contracts expire on the third Friday of that December and are struck near SPY's close of that Wednesday (ten times it for SPXW); `/prev` of a SPY and an SPXW call listed at run time, found in the contracts reference, for the most recent completed trading day, beside the range bar of that same day and marked when that day is a half day; record each `TimestampStart` |
| 52 | The stamps of index 1-day and longer bars on every source: the range and `us_indices/day_aggs_v1` (01:00 New York time, midnight in Chicago, in the samples) and `/prev` (16:00 New York time), on a full day and on a short day (the Friday after Thanksgiving); whether a bar stamped 01:00 New York time covers the New York day, the Chicago day, or the trading session; whether week, month, quarter, and year bars are stamped 01:00 too; and whether an index that updates around the clock has weekend values in its week bars; `/prev`'s short-day stamp can be seen only when a run falls on the trading day after a half day, and every short-day stamp the library accepts (13:00, 16:00, 13:15, and 16:15 New York time) maps to 00:00 of the same New York day | A 1-day stamp at 00:00, 01:00, 13:00 (presumed), or 16:00 New York time is moved to 00:00 New York time of its day, a longer bar stamped 00:00 or 01:00 New York time on the day its period starts to 00:00, and any other stamp is served, labelled by the day it falls on, with a WARNING naming the dataset, the source, and the stamp; an index week, month, quarter, or year ends an hour after its calendar period, weekend included | `tests/namespaces/test_indices.py`: the documented stamps of every source map to 00:00 New York time; another stamp is served with a WARNING; a week ends an hour after the next Monday begins | `tests/namespaces/test_indices_integration.py`: one 1-day bar of `I:SPX` and of `I:AAVE100` (which updates around the clock) from the range and `day_aggs_v1` for the Wednesday before and the Friday after (a short day, closing at 13:00) the most recent Thanksgiving whose week has ended at run time, computed from the exchanges' holiday rule so the dates stay inside every plan's history, rebuilt from minute bars over the New York and the Chicago day; `/prev` of each, for the most recent completed trading day at run time, beside the range bar of that same day and marked when that day is a half day; the week bars of that Thanksgiving week and the week before, each against its weekdays, its whole calendar week, and the seven days from its stamp rebuilt from minute bars; and, anchored to the run's day so they stay inside the plan's history, the month bars of the two whole months and the quarter bars of the two whole quarters before the current ones and this year's unfinished year bar, read from January 1 to the run's day (left out, and recorded as such, before the year's first trading day); in the two weeks before the next Thanksgiving week ends, the day and week reads sit just past one year old and a plan that keeps one year of index history refuses them, failing the check until that week ends; record each `TimestampStart` and which day the OHLC match |
| 53 | Split adjustment of stock bars across a split: NVDA's 10-for-1 split executed on 2024-06-10 | `adjusted=True` is REST only and each page's `adjusted` echo is checked; flat files and `adjusted=False` are as traded | `tests/ingest/test_rest.py`, `tests/namespaces/test_stocks.py`: a mismatched echo raises; flat files refuse `adjusted=True` | `tests/namespaces/test_stocks_integration.py`: NVDA's 2024-06-07 day bar from `day_aggs_v1` and REST `adjusted=false` (about 1208) and REST `adjusted=true` (about 120.9, volume about 10 times), and 2024-06-10 as the control, on which all three agree |
| 54 | Whether every page echoes `adjusted`, `next_url` pages and each grouped-daily day included | A page without the echo logs a WARNING once per dataset, path, and session; a different echo raises | `tests/ingest/test_rest.py`: absent and mismatched echoes, on a second page too | `tests/namespaces/test_stocks_integration.py`: a multi-page minute download and a grouped-daily download; record each page's `adjusted` |
| 55 | That Massive never adjusts for cash dividends: COST's special dividend (ex-date 2023-12-27) | The documentation says splits only; nothing adjusts for dividends | none (documentation) | `tests/namespaces/test_stocks_integration.py`: COST day bars around 2023-12-27 with `adjusted=true` and `adjusted=false`; record whether any price differs |
| 56 | Whether `adjusted=true` applies `stock_dividend` events of the splits dataset | Documented as unknown: the README's recipe applies every factor the splits dataset lists | none (documentation) | `tests/namespaces/test_stocks_integration.py`: one ticker with a `stock_dividend` event; compare both bases across its execution date |
| 57 | Whether option bars are adjusted for splits of the underlying in full (`adjusted=true`) | Options take `adjusted` like stocks, with the same guarantee | `tests/namespaces/test_options.py`: the parameter is sent and its echo checked | `tests/namespaces/test_options_integration.py`: a contract of NVDA across 2024-06-10 under both bases; record the prices |
| 58 | Futures session labels and the roll: whether REST and flat-file `session_end_date` match the 17:00 Chicago rule outside holidays, which session a 16:45 CBOT pre-open row belongs to, and when the schedules of the next session are published | WebSocket rows take the 17:00 Chicago rule corrected by `/futures/v1/schedules`; a row the schedules cannot place keeps the rule's date with a WARNING and its miss in `m.streams()` | `tests/ingest/test_futures_sessions.py`, `tests/tables/test_stream.py`: holiday folds, the pre-open, gaps, and misses | `tests/namespaces/test_futures_integration.py`: one day of ZC and ES trades from REST and the stream across 16:00 to 17:30 Chicago, and Good Friday 2024; record each row's `Date` against the schedules |
| 59 | Whether Massive's futures one-second WebSocket bars span 60 seconds (`e - s` in the documented sample) | `TimestampEnd` is the later of `s` plus one second and `e`, and a disagreement is logged once | `tests/datasets/test_row_times.py`: both ends | `tests/namespaces/test_futures_integration.py`: one minute of ES `A` bars; record `s` and `e` |
| 60 | The index day zone: whether an index bar of a day or longer covers the New York or the Chicago day (item 52) | Its `TimestampEnd` is 01:00 New York time, an hour after the New York day, and its `Date` the New York day; `history()` returns it only from 01:00, and a range of dates ends at 01:00 after its last day | `tests/datasets/test_row_times.py`, `tests/namespaces/test_indices.py`: the extra hour; at 00:30 the previous day's bar is left out, at 01:00 kept | `tests/namespaces/test_indices_integration.py` (item 52's evidence) |
| 61 | The stamps of indicator values of a day or longer: at which time of day, and in which zone, Massive stamps them per class | Checked as the class's range bars, whose stamps the samples' underlying aggregates show (00:00 New York time for stocks, 01:00 for indices, 00:00 UTC for forex and crypto, on the period's first day); any other stamp is served, labelled by the day it falls on, with a WARNING | `tests/datasets/test_row_times.py`, `tests/namespaces/test_indicators.py`, `tests/ingest/test_rest.py`: realistic stamps labelled by their class's day; a day value at 22:00 New York time and a week value on a Wednesday are served with a WARNING | `tests/namespaces/test_stocks_integration.py`, `test_crypto_integration.py`: one week of daily SMA for AAPL and `X:BTCUSD`; record each `TimestampStart` |
| 62 | What an unknown ticker gets: HTTP 404 or an empty 200, per endpoint, and whether a 404 can follow a good first page | A symbol with no data returns no rows; a 404 after the first page of a request series raises `MassiveError`; a futures session is its own series, so a 404 on a later session drops that session with its own WARNING | `tests/ingest/test_rest.py`: a first-page 404 drops the symbol; a later 404 raises; a later futures session's 404 is left out with a per-session WARNING | `tests/namespaces/test_stocks_integration.py`: an invalid ticker on `/prev`, the range, `/v3/trades`, and `ticker_details`; record each status |
| 63 | Whether a stock, option, or index bar of several days (`multiplier` above 1) counts calendar days or trading days | Settled by the live check of 2026-10-30: calendar days. AAPL's 2-day bars were stamped on Monday, Wednesday, Friday, Sunday, Tuesday, and Thursday; the Friday bar's volume was Friday's alone and the Sunday bar's Monday's. A bar of `n` days spans `n` calendar days from its stamp, weekends included, and `history()` keeps it when that end lies in the range. Only stocks were observed; options and indices read the same aggregates endpoint and are treated alike. Forex and crypto bars count calendar days too | `tests/datasets/test_row_times.py`, `tests/datasets/test_bar_periods.py`, `tests/namespaces/test_stocks.py`: 2-day bars stamped Friday, Sunday, and Tuesday end two calendar days later, and `history()` of Friday to Wednesday keeps all three | `tests/namespaces/test_stocks_integration.py`: AAPL's 2-day bars over two weeks beside its 1-day bars; fail unless each 2-day bar's volume is that of its two calendar days |
| 64 | Whether Massive publishes forex flat files for a Saturday and a Sunday (forex trades from Sunday evening to Friday evening UTC) | A forex Saturday is a day without trading (`dates.is_trading_day`): its missing file is skipped silently and is never taken for a file not published yet, and REST grouped daily does not request it; a missing Sunday file before a later file warns like a missing weekday | `tests/datasets/test_dates.py`, `tests/ingest/test_flatfiles.py`, `tests/ingest/test_rest.py`: a missing Saturday file is silent even as the last day before any later file can exist, a Sunday's warns, and grouped daily skips the Saturday | `tests/namespaces/test_forex_integration.py`: `global_forex/day_aggs_v1` for a Saturday and a Sunday; record whether each file exists and its row count |
| 65 | Exact text of Massive's 429 for a Basic subscription ("You've exceeded the maximum requests per minute"), and whether any other 429 carries it | A case-insensitive substring match makes the subscription learned Basic; any other 429 only pauses, without learning | `tests/massive/rest/test_governor.py`: the message, any case, and a 429 without it | `tests/massive/rest/test_client_integration.py` (a class on Basic): seven requests within a minute; record each answer's status, rate-limit headers, and message |
| 66 | Whether reference data and the other datasets Massive assigns to no subscription (cross-market reference data, economy, partner data, the unified snapshot) count in a bucket of their own, as Massive's knowledge base says ("What is the request limit for Massive's RESTful APIs?": reference and cross-asset endpoints are "counted in their own bucket"), rather than against a subscription's limit, and whether that bucket has the Basic limit of 5 per minute | They go to the `other` bucket, every reference endpoint included (options contracts, IPOs, and reference reads filtered by market); `other` cannot be declared and is paced once a 429 names the per-minute limit | `tests/ingest/test_rest.py`: such a request, filtered by market or not, is charged to `other`; `tests/ingest/test_rate_limits.py`: a Basic-worded refusal paces `other` | `tests/massive/rest/test_client_integration.py` (a class on Basic): spend the class's minute, then one request each of market status, ticker types, and treasury yields; and ten such requests alone within a minute; record which are refused |
| 67 | Whether Massive's minute is a sliding 60 s window from each request, and whether a refused request counts against it | A slot is held until 61 s after each response, refused ones included, and a 429 without `Retry-After` pauses a paced bucket for a whole 61 s window. The live check of 2026-10-30 settled the window: after 5 answered requests and refusals every 5 s, the first answer came at 47.3 s, as the clock minute turned, so Massive counts by the clock minute. No clock minute holds more starts than a 61 s window, so the hold stays: never refused for pacing, at worst a minute slower; waiting for the next minute instead would trust the local clock to the second. Whether a refusal counts is moot within a clock minute | `tests/massive/rest/test_governor.py`: the sixth request starts 61 s after the first answer; a refused request holds its slot | `tests/massive/rest/test_client_integration.py` (a class on Basic): requests until the first 429, then one every 5 s; record when the first is answered again |
| 68 | Whether every row of Benzinga's consensus ratings (`/benzinga/v1/consensus-ratings/{ticker}`) carries `ticker`, which the vendor's schema marks optional | Each row's `ticker` is its `Symbol` and its `poll()` key, so a row without one fails the call rather than keeping a null `Symbol` | `tests/namespaces/test_partners.py`: a consensus row without its ticker fails | `tests/namespaces/test_partners_integration.py` (the Benzinga add-on, $99 a month; skipped without it): three stocks' consensus ratings read raw; every row names the requested ticker |
