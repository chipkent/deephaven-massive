## 4. Package layout

### 4.1 Source modules

Every module below `deephaven_massive/` other than `__init__.py` and `queries.py` is private.
The rule: the first path component below the package has a leading underscore. `queries.py`,
the query types for annotations ([section 3](03-exports.md#3-exports)), is the one public module beside the
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
_config.py               MassiveConfig (section 5), HostMap, the environment readers, the
                         settings-file reader (MassiveConfig.from_file);
                         require_positive_int and require_positive_number, the two argument
                         checks used across layers
_enums.py                the sixteen public enums, Recency, ChannelFamily, ClassFromPrefix, NotBars,
                         _require_enum, subscription_of
_types.py                Symbols, ALL/_All, DateLike, FilterValue, bars overload aliases, override
_errors.py               MassiveError (.status, .request_id, .cause), ErrorCause, AuthenticationError,
                         EntitlementError, from_wire, the library-bug and user-defect texts; RowDataError, which the
                         ingest layer raises and the tables layer names
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
                         check_symbols()), the public scalar converters, option_roots(),
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
  timestamps.py          the one epoch-unit routine (section 12), the magnitude rule
  epoch.py               the epoch, a day in nanoseconds, and the conversions between aware
                         datetimes and epoch nanoseconds
  row_times.py           the library's time columns: Date, TimestampStart, TimestampEnd,
                         TimestampInsert; RowTimes, TimeRule, and the bar-stamp rule (section 12)
  history_source.py      the history window, partition dates, the current partition, REST
                         capability, the AUTO choice and its unfilled-columns WARNING
                         (section 16)
_tables/                 the Deephaven side: tables, liveness, library threads
  __init__.py            docstring only
  session_core.py        SessionCore, registry_lock, the registries, closers, stream ids, the
                         live set, the worker-table registry, diagnostics_view, attach, close()
                         (sections 4.3, 6)
  transports.py          the transport ports (RestTransport, StreamTransport,
                         FlatFileTransport), IngestPorts, StreamHandle, HostDecision; the
                         outcomes of a read by symbol (SymbolRows, SymbolFailure, FailureKind)
  execution_context.py   entered(), the execution-context and update-graph probes
  liveness.py            result and stream scopes, ResultRegistry, records, Reaper,
                         QueueWorker (section 6.1)
  diagnostics.py         connections(), streams() with SchemaDrift, and rate_limits();
                         DriftRegistry (section 17)
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
  host_decision.py       host resolution from configuration and REST evidence (section 13.2):
                         the host rule and its probes
  shared_lookup.py       SharedLookup: a session's lookups one caller makes while the others
                         wait, each within its own patience (the host decisions, the crypto
                         ticker list)
  crypto_tickers.py      CryptoTickerList: Massive's crypto ticker list, read once per session,
                         which splits each crypto pair into its two currencies
  recency_labels.py      the DELAYED and REAL-TIME labels of a REST response (envelope status,
                         timeframe fields) and the refusal of delayed data not asked for
                         (sections 13.2 and 13.4)
  rate_limits.py         the process's REST governors per (API key digest, REST origin),
                         session leases, the first-429 INFO notice, rate_limits() rows, the INFO
                         download notice, the poll() pace WARNING (section 13.4)
  flatfiles.py           flat-file transport: read plans, reader pool, cancellation, conversion,
                         merge to tables, missing-file policy, drift routing (section 16)
  history_reads.py       a flat-file history() call's fallback to REST: the days and refused
                         files REST serves under AUTO, the errors otherwise (section 16)
  symbol_reads.py        reads by symbol for fetch() and history(): their requests, each
                         symbol's rows or failure, and whether a failure is transient or
                         permanent
  futures_calendar.py    the futures schedule calendar that dates streamed futures rows by
                         their trading session (section 12)
  subscriptions.py       topics and the reconcile loop of one connection: wildcards,
                         watchdogs, the NO_ACK rule, the options cap's admission (section 13.3)
  ws.py                  WsConnections, the session's stream transport: its connections over
                         WsClient, leases, refusals, rejects and drift routing, the
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
| `python -m deephaven_massive._massive.refresh [--check \| --fetch]` | The refresh command, run from a source checkout ([section 19.6](19-wire-clients-and-the-spec-snapshot.md#196-refresh-and-drift)). |
| `uv run python scripts/render_reference.py` | Regenerates the dataset reference, `docs/datasets/`, the whole directory and nothing else ([section 7.4](07-query-classes-and-verbs.md#74-column-descriptions)). |
| `docs/datasets/README.md`, `docs/datasets/<dataset>.md` | The generated dataset reference, committed, never edited by hand. |
| `vendor/massive/` | The vendored spec snapshot the refresh reads ([section 19](19-wire-clients-and-the-spec-snapshot.md#19-wire-clients-and-the-spec-snapshot)). It is Massive's published content, vendored as generation input: not covered by the repository's license, and in neither the wheel nor the sdist. `vendor/README.md` says so beside it. |
| `.github/workflows/spec-drift.yml` | The advisory weekly drift job ([section 20](20-quality-gates.md#20-quality-gates)). |
| `.github/workflows/release.yml` | Publishes to PyPI from a `v*` tag; created only when the owner turns publishing on ([section 20.1](20-quality-gates.md#201-tooling-configuration)). |
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
equals [section 3](03-exports.md#3-exports) and `__version__` equals the installed distribution's version).
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
| `_ingest/recency_labels.py` | `tests/ingest/test_recency_labels.py` | |
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
[`docs/dev/soak-tests.md`](../../../docs/dev/soak-tests.md) is their runbook, overnight runs
included. No test pins the `addopts` string or the
marker list: pytest's `strict` setting already rejects an unregistered marker.
[`docs/live-tests.md`](../../../docs/live-tests.md) is the runbook for them. The live tests that deliberately exceed Massive's subscription and connection
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
  ([section 19](19-wire-clients-and-the-spec-snapshot.md#19-wire-clients-and-the-spec-snapshot)) and the library's layer rules
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
| 0 | wire | `_massive/` | The only network I/O, and no Deephaven ([section 19](19-wire-clients-and-the-spec-snapshot.md#19-wire-clients-and-the-spec-snapshot)). |
| 1 | vocabulary | the root `_config`, `_enums`, `_errors`, `_types`, `_log_limits` | The public value types every layer names. No network I/O and no threads (`_config` reads the settings file, a local read at the caller's request); Deephaven appears only as the `Table` in `Symbols`. |
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
of [section 19](19-wire-clients-and-the-spec-snapshot.md#19-wire-clients-and-the-spec-snapshot). The rules read source, so they cannot
see an import built by `exec`, `importlib.util.find_spec`, or an attribute read through the
root package (`deephaven_massive._ingest`); review keeps those out.
