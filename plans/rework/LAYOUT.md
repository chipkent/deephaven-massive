# deephaven-massive: package layout cleanup plan

**Status:** approved by the owner. Nothing has moved yet.
**Companion:** [`DESIGN.md`](DESIGN.md) is the specification. It already carries the placement
rule (§ 4.1), the amendment "one namespace and its sub-namespaces per module" (§ 4.1) and
`_log_limits` in the vocabulary layer (§ 4.3); each cleanup PR below updates the rest of its
slice of `DESIGN.md` as it moves the code. This file is the schedule. When the two disagree,
`DESIGN.md` wins and this file gets a `docs:` fix. Like the rest of `plans/rework/`, this file
is temporary: once B1–B11 have all merged, delete it together with its pointers in `AGENTS.md`
("Where to look") and `PLAN.md`.

## Why

The six layers of `DESIGN.md` § 4.3 hold, and `tests/test_layout.py` enforces them. The problem
is one level down. Inside a layer, a module's name records the first thing put in it, and later
code went to whichever module was nearest. The clearest case was `canonicalize_symbols`, a
public function that landed in `_tables/watchlist.py`, a module about watching symbol tables.
Its docstring was widened with "…, and `canonicalize_symbols`" to admit it, and a reader looking
for the symbol converters would never have opened that file. (The canonical-symbols PR, #138,
has since moved it to `_tables/symbols.py`.)

The same drift produced three kinds of damage:

- **Grab-bags.** `_ingest/rest.py` (about 1,900 lines) lists seven subjects in its own
  docstring; `_datasets/dates.py` holds six, `_datasets/schema.py` four, and
  `_datasets/symbols.py` three once #138 merges.
- **Scattered concepts.** The futures session rule lives in four modules, the WebSocket host
  decision in four, and the bar-stamp rule in three. The rule that merges two wire types is
  written twice, once for the refresh and once for the run-time schema merge, although the two
  must agree.
- **Modules in the wrong layer or named by pattern.** `_ingest/poll.py` holds no wire client: it
  is Deephaven-side code, the REST counterpart of `_tables/stream.py`. `_tables/core.py` and
  `_namespaces/base.py` are named by pattern words and hold several subjects each.

## Owner decisions

1. **The target layout below is approved**, including the `DESIGN.md` § 4.1 amendment "one
   namespace and its sub-namespaces per module" and `_log_limits.py` in the vocabulary layer.
2. **New features are paused in `_ingest/rest.py`, `_query.py` and `_tables/core.py`** while the
   cleanup of those files lands (see [Feature pause](#feature-pause)).
3. **No automated placement checks.** Banned module names, shared-word checks on exports,
   duplicate-body detection, exception-location checks and package-docstring listings were all
   judged brittle, a maintenance burden, and of little protection. Placement is enforced by
   review: the pre-push review's "Placement and organization" check and the one-subject module
   docstring rule of `DESIGN.md` § 4.1. The cleanup adds no check to `tests/test_layout.py`;
   its one edit there places `_log_limits.py` in the vocabulary layer (B9).

The layout review behind this plan proposed two more review rules, which are not adopted:

- **"A package `__init__.py` holds only what `DESIGN.md` assigns it."** `DESIGN.md` § 4.1 and
  § 19 already say what each package `__init__.py` holds, and `tests/test_layout.py` already
  checks the docstring-only ones, so a separate rule adds nothing.
- **"'Where to look' in `AGENTS.md` maps every concept to its module."** `AGENTS.md` must stay
  lean; the target tree below and the first line of each module's docstring are the concept
  map.

One rename that review proposed without assigning it to a PR is added to B1: the wire class
`Request` in `_massive/rest/response.py` becomes `RequestTarget`, so it no longer shares a name
with the dataset `Request`.

## Where code goes

These are review rules, applied by the pre-push review and by reviewers, not by a test.

1. **One subject per module, named in the first line of its docstring.** New code goes in the
   module whose first line already names its subject. If none does, create a module named for
   that subject. Never widen a summary with "…, and X" to admit unrelated code.
2. **Modules are named for their content**, never by a pattern word (`base`, `core`, `common`,
   `helpers`, `util`, `manager`, `misc`).
3. **One rule, one body.** A rule or helper written twice is a defect: move it to the module that
   owns the concept and call it from both places.
4. **An exception is defined in the module that raises it.** One raised by several modules goes
   to the layer's errors module: `_errors.py`, or `_massive/net/errors.py` for the wire clients.
5. **Size is a smell, not a rule.** A module above about 1,000 lines, or one whose first line
   lists more than two subjects, needs a split or a sentence in its docstring saying why it is
   one subject.

## Target layout

One line per module. ← marks a module renamed or moved from the named source, NEW a module the
cleanup creates, and `#n` a module an open PR creates. `__init__.py` of `_namespaces`,
`_datasets`, `_tables` and `_ingest` holds only a docstring, as today.

```
src/deephaven_massive/
__init__.py                    public names, lazy; the front door
# vocabulary (rank 1)
_config.py                     MassiveConfig, HostMap, environment readers; require_positive_* (see Out of scope)
_enums.py                      public enums, Recency, ChannelFamily, _require_enum, subscription_of (#143)
_errors.py                     MassiveError hierarchy, from_wire, RowDataError (raised by several modules)
_types.py                      Symbols, ALL, DateLike, FilterValue, span literals, override
_log_limits.py        NEW      rate-limited log records: log_once, BoundedWarnings (← _errors)
# datasets (rank 2): no I/O, threads or Deephaven calls
_datasets/
  catalog.py                   DatasetSpec, KeySpec, SourceKeys, Catalog
  filters.py          NEW      the filters= rules (← catalog)
  request.py                   Request, ParamValue, StreamKey
  schema.py                    ColumnType, Column, Schema, dataset_schema (the merge)
  source_columns.py   NEW      SourceColumn, the key rename, required fields, join, widen (← schema)
  column_descriptions.py NEW   description rules, transport labels and order (← schema, column_reference)
  column_reference.py          docs/datasets/*.md render and sync
  symbols.py                   ticker grammar and checks, the scalar converters, option_roots
  wire_symbols.py     NEW      transport spellings both ways: canonical_from_wire, ws_symbol (← symbols)
  dates.py                     DateLike parsing, ranges, bounds, day zones, the trading-day test (#142)
  futures_sessions.py NEW      the 17:00-Chicago session rule, scalar and Arrow; weekday roll (← dates, row_times)
  bar_periods.py      NEW      intraday, bar periods, bar starts, days and sessions (← dates)
  timestamps.py                the epoch-unit routine only
  row_times.py        #142     the library's time columns and the bar-stamp rule (← history_source, timestamps)
  history_source.py            window, partition dates, publication (← dates), choose_source
# tables (rank 3)
_tables/
  session_core.py     ← core   SessionCore and its registries
  transports.py       NEW      the transport protocols, IngestPorts, StreamHandle (← liveness), HostDecision (← _datasets/request)
  execution_context.py NEW     entered (← liveness), the execution-context and update-graph probes (← core)
  liveness.py                  result scopes, ResultRegistry, Reaper, QueueWorker
  diagnostics.py               the diagnostics tables and writer, DriftRegistry, pascal_case (← schema)
  columns.py                   Deephaven column types, describe, column_table
  stream.py                    stream sinks, Flusher, fan-out
  poll.py             ← _ingest/poll  Poller, Pollers, open_poll, SymbolRowError
  watchlist.py                 read_symbols, SymbolWatch
  symbols.py          #138     canonicalize_symbols: the table form of the converters
# ingest (rank 4): the only holder of wire clients
_ingest/
  clients.py                   SessionClients
  rest.py                      RestState, RestReader, RestEndpoints, UnsupportedParameterError, UnlistedPairError
  rest_requests.py    NEW      the requests of a call: Call, plan_calls, fetch_paths, history calls (← rest)
  rest_rows.py        NEW      page rows to dataset batches, error rows, the history window keep (← rest, hosts)
  host_decision.py    ← hosts  the host rule, its probes and constants, HostDecisionCache (← RestState)
  rate_limits.py      #143     governor sharing, 429 warnings, m.rate_limits() rows
  flatfiles.py                 FlatFileHistory
  futures_calendar.py #142     FuturesCalendar: Massive's futures schedules
  subscriptions.py             topics and the reconcile loop of one connection
  ws.py                        WsConnections, WsTransport
  ws_connection.py    NEW      Connection and its client factories (← ws)
# API (rank 5)
_debug_log.py         #158     the log switch, enable_debug_logging
_session.py                    MassiveSession, NAMESPACE_MODULES, CATALOG
_query.py                      the six query classes and verbs; query_class, bar_query_class (← base)
_namespaces/
  query_building.py   ← base   Namespace, build_query, list_query, ticker_key, argument normalization
  stocks.py                    m.stocks
  stock_financials.py NEW      m.stocks.financials (← stocks)
  stock_filings.py    NEW      m.stocks.filings: SEC filings and insiders (← stocks)
  options.py                   m.options
  futures.py                   m.futures
  indices.py                   m.indices
  forex.py                     m.forex
  crypto.py                    m.crypto
  reference.py                 m.reference
  indicators.py                m.indicators
  economy.py                   m.economy
  partners.py                  m.partners and its four partner sub-namespaces (one subject; see Module size)
# wire (rank 0)
_massive/
  __init__.py                  the lazy wire interface and its __all__
  net/__init__.py              USER_AGENT; what every wire client shares
  net/errors.py                WireError, WireAuthError, WireEntitlementError
  net/codec.py                 strict JSON decoding
  net/http.py                  CA bundle, TLS context, proxy
  net/retry.py                 retry waits, Clock, never-retried failures
  net/cancel.py       #143     the Cancel token
  net/redaction.py    #158     redaction of keys and secrets
  spec/__init__.py             source, sources, provenance and their cache
  spec/generated_schemas.py NEW parsing of the generated spec/*.json files (← spec/__init__)
  spec/types.py                wire value types, WireType.join (← _datasets/schema, refresh/kinds)
  spec/values.py               parameter wire text
  spec/convert.py              JSON value conversion per WireType
  rest/client.py               RestClient: pool, retries, paging
  rest/batch.py                page rows to an Arrow batch
  rest/query.py                request targets, origin_of
  rest/response.py             strict page decoding, http_error, RequestTarget (renamed from Request)
  rest/governor.py    #143     RequestGovernor
  ws/client.py                 WsClient: one endpoint's thread and loop
  ws/protocol.py               frames, statuses, FrameDecoder
  ws/backoff.py                reconnect Backoff
  flatfiles/client.py          FlatFileClient
  flatfiles/reader.py          one gzip CSV to Arrow batches
  flatfiles/object_keys.py ← layout  object_key, the bucket's key scheme
  refresh/rest_sources.py ← rest     generator of REST sources
  refresh/ws_sources.py   ← ws       generator of WebSocket sources
  refresh/flat_file_sources.py ← flatfiles  generator of flat-file sources
  refresh/*                    every other refresh module unchanged
```

Two smaller renames go with the moves: the registry entry `_StreamState` in the session core
becomes `_StreamEntry` (B5), so it no longer reads like the diagnostics row state `StreamState`,
and the wire class `Request` becomes `RequestTarget` (B1).

## Feature pause

New features do not touch `_ingest/rest.py`, `_query.py` or `_tables/core.py` until the cleanup
PR that splits each file merges: B5 for `_tables/core.py`, B6 for `_ingest/rest.py`, B8 for
`_query.py`. The open PRs listed below are not new work and land first. The pause covers
features; a fix to those files still lands, and the cleanup PR that follows rebases over it.

## Changes asked of the open PRs

Each open PR fixes only what it itself creates.

| PR | Change | Status |
|---|---|---|
| #138 | Define `UnlistedPairError` in `_ingest/rest.py`, the only module that raises it; `canonicalize_symbols` in `_tables/symbols.py`. | done |
| #142 | Name the schedule module `_ingest/futures_calendar.py`, with `FuturesCalendar` and `ScheduledSession`. | done |
| #143 | Drop `rate_limits._origin`, a copy of `_massive/rest/query.origin_of`; export `origin_of` from `_massive`, classified as a vocabulary name in `tests/test_layout.py`. | open |

#151, #158 and #159 create nothing misplaced and need no change.

## Cleanup PRs

Every cleanup PR is a `refactor:` with no change in behavior, and every one:

- moves code verbatim where it can, so `git diff --color-moved` shows the move;
- updates its slice of `DESIGN.md` wherever the moved modules are named: the § 4.1 tree, the
  § 4.2 examples, § 4.3, § 19 for the wire package, and every other section that names them;
  and every docstring that names moved code, including the docstring map in each package
  `__init__.py` it touches;
- moves the tests under the mirror rule: a split module splits its test file along the same
  seam, and a renamed module renames its test file, its integration test, and its fixture
  directory under `tests/data/`;
- passes `./scripts/check.sh` (full).

"Waits for" names the open PRs that edit the same files; a cleanup PR is cut after they merge.

| # | Title | Content | Waits for | Test moves |
|---|---|---|---|---|
| B1 | `refactor: one wire-type join, and the spec parser and object keys in modules named for them` | One `WireType.join` in `_massive/spec/types.py`, exported as a vocabulary name, replacing the two copies in `_datasets/schema.py` and `refresh/kinds.py` (the refresh and the run-time merge must agree). The generated-file parsing moves out of `spec/__init__.py` into `spec/generated_schemas.py`. `flatfiles/layout.py` becomes `flatfiles/object_keys.py`. The wire class `Request` in `rest/response.py` becomes `RequestTarget`. Parity tests for the two copies of the key column names (`_datasets/schema.py`, `refresh/naming.py`) and of the S3 endpoint and bucket (`_config.py`, `refresh/drift.py`). `DESIGN.md` § 19. | #138 (`_datasets/schema.py`); #142 (`_datasets/schema.py`, `refresh/naming.py`, `rest/response.py`); #143 (`rest/`); #159 (`spec/types.py`, `rest/`) | `tests/massive/spec/test_init.py` → parsing tests to `test_generated_schemas.py`; `tests/massive/flatfiles/test_layout.py` → `test_object_keys.py`; the `join` tests in `tests/massive/refresh/test_kinds.py` and `tests/datasets/test_schema.py` → `tests/massive/spec/test_types.py`; parity tests in `tests/massive/refresh/test_naming.py` and `test_drift.py` |
| B2 | `refactor: name the refresh generators after the sources they produce` | `refresh/rest.py`, `ws.py` and `flatfiles.py` become `rest_sources.py`, `ws_sources.py` and `flat_file_sources.py`, so no refresh module shares a name with a client. `DESIGN.md` § 4.2 (its `refresh/rest.py` example row) and § 19. Adds the one-subject sentence to the docstring of `refresh/overrides.py` (see Module size). | #142, B1 | `tests/massive/refresh/test_rest.py` → `test_rest_sources.py`, `test_ws.py` → `test_ws_sources.py`, `test_flatfiles.py` → `test_flat_file_sources.py` |
| B3 | `refactor: split the dataset time rules by subject` | `_datasets/dates.py` splits into `dates.py`, `futures_sessions.py` and `bar_periods.py` along the call graph (`bar_periods` → `futures_sessions` → `dates`); the Arrow copy of the session rule in `row_times.py` joins its scalar form; publication moves to `history_source.py`; the bar-stamp rule and stamping move to `row_times.py`; `session_end_date` is read from the generated source instead of being typed twice. | #138, #142, #143, #158 | `tests/datasets/test_dates.py` → `test_dates.py`, `test_futures_sessions.py`, `test_bar_periods.py`; publication tests → `test_history_source.py`; stamp-rule tests from `test_history_source.py` and `test_timestamps.py` → `test_row_times.py`. `test_timestamps_integration.py` stays: it tests the unit routine. |
| B4 | `refactor: give filters, source columns and column descriptions their own modules` | The `filters=` rules leave `catalog.py` for `filters.py`. `schema.py` keeps the types and the merge; the key rename, `join` and `widen` go to `source_columns.py`; the description rules and one transport order go to `column_descriptions.py`, replacing the copy in `column_reference.py`; `pascal_case` moves to its one caller, `_tables/diagnostics.py`. If `catalog.py` is still above about 1,000 lines, its docstring gains the one-subject sentence. | #138, #142, #143, #151, #158; can run alongside B3 | filter tests from `tests/datasets/test_catalog.py` → `test_filters.py`; `test_schema.py` → `test_schema.py`, `test_source_columns.py`, `test_column_descriptions.py`; `pascal_case` tests → `tests/tables/test_diagnostics.py` |
| B5 | `refactor: separate the transport protocols and execution checks from the session core` | `_tables/core.py` becomes `session_core.py`; the transport protocols, `IngestPorts`, `StreamHandle` and `HostDecision` go to `transports.py`; `entered` and the three runtime probes go to `execution_context.py`; `_StreamState` becomes `_StreamEntry`. If `session_core.py` is still above about 1,000 lines, its docstring gains the one-subject sentence. Lifts the feature pause on `_tables/core.py`. | #138, #142, #151, #158 (`core.py`); #143 (`liveness.py`) | `tests/tables/test_core.py` → `test_session_core.py`, plus `test_transports.py` and `test_execution_context.py`; `HostDecision` tests from `tests/datasets/test_request.py` → `tests/tables/test_transports.py`; `entered` tests from `tests/tables/test_liveness.py` → `test_execution_context.py` |
| B6 | `refactor: split the REST transport, and move poll() to the tables layer` | Request planning goes to `rest_requests.py` (pure, testable without a fake server); row conversion, the error-row rule and the history keep go to `rest_rows.py`; `hosts.py` becomes `host_decision.py` and takes the host-decision cache, moved verbatim from `RestState` into `HostDecisionCache`; `_ingest/poll.py` moves to `_tables/poll.py`; `RowDataError` moves to `_errors.py` and `UnsupportedParameterError` to `rest.py`. If `rest.py` is still above about 1,000 lines, its docstring gains the one-subject sentence. Lifts the feature pause on `_ingest/rest.py`. | B3, B5, and every open PR (all edit `rest.py`) | `tests/ingest/test_rest.py` → `test_rest.py`, `test_rest_requests.py`, `test_rest_rows.py`; `tests/ingest/test_hosts.py` and `test_hosts_integration.py` → `test_host_decision.py` and `test_host_decision_integration.py` (plus the cache tests from `test_rest.py`); `tests/data/ingest/hosts/` → `tests/data/ingest/host_decision/`; `tests/ingest/test_poll.py` → `tests/tables/test_poll.py`; `RowDataError` tests → `tests/test_errors.py` |
| B7 | `refactor: split a WebSocket connection from the session's registry of them` | `Connection` and its client factories leave `_ingest/ws.py` for `ws_connection.py`; `ws.py` keeps `WsConnections` and `WsTransport`. If either is still above about 1,000 lines, its docstring gains the one-subject sentence. | #138, #142, #151, #158 (`ws.py`) | `tests/ingest/test_ws.py` → `test_ws.py` and `test_ws_connection.py`; fixtures under `tests/data/ingest/ws/` follow their owner test. `test_ws_integration.py` stays: it tests the transport end to end. |
| B8 | `refactor: name the namespaces' query building, and choose query classes in _query` | `query_class` and `bar_query_class` move to `_query.py`, beside the classes they choose; the rest of `_namespaces/base.py` becomes `query_building.py`, with one `list_query` and one `ticker_key` replacing the copies in `stocks.py`, `partners.py` and `crypto.py`, and `optional_text` replacing `reference._text`. If `_query.py` is now above about 1,000 lines, its docstring gains the one-subject sentence. Lifts the feature pause on `_query.py`. | #138, #142, #143, #151, #158 (`_query.py`, `_namespaces/`) | `tests/namespaces/test_base.py` → `test_query_building.py`; query-class tests → `tests/test_query.py`; in `tests/test_session.py:515` (`test_every_namespace_module_feeds_the_catalog`), the module left out of `NAMESPACE_MODULES` becomes `query_building` instead of `base` |
| B9 | `refactor: move the rate-limited log helpers out of the errors module` | `log_once` and `BoundedWarnings` move to the new vocabulary module `_log_limits.py`. `DESIGN.md` § 4.3 already places it in the vocabulary layer; B9 adds it to the § 4.1 tree, to the vocabulary list in the "Layers" section of `AGENTS.md`, and to the vocabulary layer in `tests/test_layout.py`. | every open PR: all of them import `log_once` or edit `_errors.py` | `log_once` and `BoundedWarnings` tests from `tests/test_errors.py` → `tests/test_log_limits.py` |
| B10 | `refactor: split m.stocks into its three namespaces` | `_namespaces/stocks.py` splits into `stocks.py`, `stock_financials.py` and `stock_filings.py`. `partners.py` stays whole, and its docstring gains the one-subject sentence (see Module size). | B8, #142 (`stocks.py`) | `tests/namespaces/test_stocks.py` and `test_stocks_integration.py` split three ways; fixtures under `tests/data/namespaces/stocks/` follow their owner test |
| B11 | `refactor: move symbols' transport spellings to wire_symbols` | The reading and writing of a symbol's transport spellings (`canonical_from_wire`, `wire_spellings`, `ws_symbol`, `WireSymbols`, `ReadSymbols`, `unknown_symbol_message`) leave `_datasets/symbols.py` for `_datasets/wire_symbols.py`; the private helpers they share with the converters become public names in `symbols.py`, and `option_roots` stays there. Its callers are the transport files (`_ingest/rest.py`, `flatfiles.py`, `subscriptions.py`, `ws.py`, `_tables/stream.py`). Adds the one-subject sentence to the docstrings of `symbols.py`, `_ingest/flatfiles.py` and `_tables/stream.py` if they are still above about 1,000 lines. | #138, #151, B6 and B7 (the transport files; through B6, every open PR) | transport-spelling tests from `tests/datasets/test_symbols.py` → `test_wire_symbols.py` |

**Merge order.**

1. This plan and the changes asked of the open PRs, now.
2. B1 and B2, once #138, #142, #143 and #159 merge.
3. B3 and B4 in parallel, once #138, #142, #143 and #158 merge (B4 also after #151).
4. B5, then B6, then B7, then B11.
5. B8, then B10.
6. B9 once every open PR has merged.

B6 is the largest, at about 2,000 lines moved, but it moves code without rewriting it.

## Out of scope

- **One row converter.** `_ingest/rest.py`, `_ingest/flatfiles.py` and `_tables/stream.py` each
  turn rows into a dataset batch. The column plan is shared, but the failure policies differ
  (error rows, read plans, per-row masks), so unifying them is a design question for its own
  issue, not a move.
- **`_config.py` keeps `require_positive_int` and `require_positive_number`.** They are the
  two argument checks used across layers, and a root module for two functions costs more than it
  saves.
- **`refresh/gates.py`** stays whole: its docstring already says why the error vocabulary and
  the cross-source checks share a module.
- **Automated placement checks** (owner decision 3).

## Module size

These modules are above about 1,000 lines on `main` or an open PR and are not split below that
by the cleanup, or may not be. Each gets the one-subject sentence in its docstring, saying why
it is one subject, from the PR named.

| Module | Largest size | Why it can stay one module | Sentence added by |
|---|---|---|---|
| `_namespaces/partners.py` | about 1,600 (#138) | one namespace and its four partner sub-namespaces, all REST list endpoints built the same way | B10 |
| `_massive/refresh/overrides.py` | about 1,550 (#142) | hand-verified wire facts, one entry each; it grows with Massive's API, not with subjects | B2 |
| `_datasets/symbols.py` | about 1,400 (#138), less after B11 | ticker grammar, checks and the scalar converters built on it | B11 |
| `_datasets/catalog.py` | about 1,360 (#142), less after B4 | the dataset specs and their catalog | B4, if still above |
| `_tables/stream.py` | about 1,070 (#142) | one stream's path from publisher to fan-out | B11 |
| `_ingest/flatfiles.py` | about 1,070 (#142) | one transport, from read plan to table | B11 |
| `_ingest/rest.py`, `_ingest/ws.py`, `_tables/session_core.py`, `_query.py`, `_namespaces/stocks.py` | below or near 1,000 after their split | | B6, B7, B5, B8, B10, if still above |
