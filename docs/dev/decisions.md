# Decisions

The owner's decisions behind `deephaven-massive`, one entry each: the decision, its reason, and
its status. README.md and the public docstrings are the user contract; this file records why the
contract is what it is.

The file is append-only. A changed decision gets a new entry with the next free number, and the
old entry's status becomes "superseded by N"; no entry is otherwise rewritten or removed.
Entries 1 to 94 are condensed from the design documents this file replaced; their full text is
in git history, in the commit before the one that added this file.

Status is **accepted** unless the entry says otherwise.

**1. No SDK: the library owns its REST and WebSocket clients** (2026-10-26). It speaks
Massive's documented protocols on `urllib3` and `websockets`. Massive's SDK needed about
fourteen workaround layers and kept defects (dropped wildcard siblings, instant reconnects,
silently dropped fields), and a fork that fixed them could not be a PyPI dependency.

**2. Every schema is generated from the vendor's specs.** The refresh generates
`spec/rest.json`, `spec/ws.json`, and `spec/flatfiles.json` from the pinned vendor snapshot; hand
input is naming, grouping, and verification only. No schema is hand-written or inferred from
data.

**3. No seeding.** `last_by()` never seeds from REST and there is no `seed_since`; the recipe is
`merge([q.history(start, end), q.append_only()])`.

**4. Reactive watchlists are kept.** A live Deephaven table of symbols drives subscriptions.

**5. No embedded server.** The library needs an existing `ExecutionContext` and fails fast
without one; `deephaven-server` is a development dependency only.

**6. Lifecycle is automatic plus explicit.** A garbage-collected result releases its scope
through a weakref and the reaper; `m.release(table)` and `m.close()` give deterministic control.
Topic reference counts follow the blink table's real shutdown, so derived tables keep a stream
alive. Queries have no `stop()`.

**7. Configuration is `MassiveSession(MassiveConfig(...))` only**, so defaults live in one place.

**8. Flat files are reachable only through `history(source=...)`**; there is no public
`flat_files` namespace.

**9. Coverage is 100% of statements and branches.** `pragma: no cover` needs a written reason
and owner approval.

**10. Partners stay nested** under `m.partners.<vendor>`.

**11. An entitlement refusal fails the affected table** through `publish_failure`, is logged,
and shows in the diagnostics tables.

**12. Endpoint policy.** Every documented endpoint is supported, plus new ones that lag in the
documentation; deprecated and sunset endpoints are excluded with a reason; undocumented ones are
unsupported.

**13. Python 3.11 and above, with 3.14t a required CI job.** It is the only free-threaded target,
because pyarrow, numpy, and pandas publish no cp313t wheels.

**14. Closed sets are enums.** Every closed categorical set is a `StrEnum` with wire-string
values, and a plain string raises `TypeError`: one spelling per value, tab completion, and typo
safety at type-check and run time.

**15. A base Business key streams from `business.massive.com` with a WARNING.** *Superseded by
37 and 79:* such a key now streams from `socket.massive.com` like any snapshot without a
`timeframe`, and a refusal names `websocket_hosts`.

**16. WebSocket frames are decoded by the client.** `WsClient` decodes each frame once against
`ws.json` into status events, rows, rejects, and drift. A `max_connections` status shows
`WAITING_SLOT`, and streams stay alive. *Its `max_connections` part is superseded by 97.*

**17. The context manager is kept**, documented as for static results only.

**18. `m.options.snapshots(dm.ALL)` is allowed.** It pages through the whole OPRA universe and
is documented as slow, so the rule that every per-class `snapshots()` accepts `ALL` stays
uniform.

**19. No live testing as design input.** An unverifiable fact gets the conservative choice and a
fixture test; live tests then settle it (`verification-items.md`).

**20. Only the owner merges, and only through pull requests.** `uv.lock` is not committed, so
every sync resolves afresh.

**21. Sessions and queries expose no introspection attributes; their reprs are defined.** A
session has no `name` or `config` and a query no `dataset`; the label shows only in reprs,
warnings, and errors. The public surface stays the verbs, `columns()`, and the namespaces, and
the API key stays out of objects users leave in console globals.

**22. Names come from the generated tables** (2026-10-26). Every field is named from its wire
key, its shape's lexicon, and `NAMES`, never from descriptions; gates fail a wrong name, type,
unit, or merge. Final names include `AccumulatedVolume`, `OfficialOpen`, `DayVwap`,
`AverageTradeSize`, `DecimalVolume` (stocks), and `DollarVolume` (futures).

**23. Spec drift is reported, not gated** (2026-10-26). A weekly workflow runs the refresh's
`--check` and keeps one standing "Massive API drift" issue; it never opens a pull request or
blocks a merge.

**24. No client object is shared unless it is proven thread-safe** (2026-10-26). The REST client
is shared by the session's threads; each WebSocket client is confined to its loop thread.

**25. No SDK pull requests; upstream issues are optional** (2026-10-26). The library never
depends on a fix in `massive-com/client-python`.

**26. One wire package** (2026-10-26). `_massive` owns all wire access, the vendored snapshot,
and the generated schemas; it imports nothing from Deephaven or the rest of the library, and an
AST test enforces the boundary.

**27. `MassiveError` carries `status` and `request_id`** (2026-10-26), read-only and `None` when
the error did not come from an HTTP response.

**28. `reconnect_backoff` is at least 15 s** (2026-10-26); a smaller value raises `ValueError`.

**29. `refresh/` is not shipped** (2026-10-26). The wheel excludes `refresh/` and the snapshot;
`spec/*.json` ship.

**30. Wire behavior** (2026-10-26). A TLS certificate failure is never retried, a 429 at the
WebSocket upgrade is retried, and wildcard transitions make before they break, accepting a brief
duplicate rather than a gap. *Its REST 429 part is superseded by 60 and 62.*

**31. No hand-written schema anywhere**: no hand tick schemas, key maps, column overrides, or
name lock. Every schema change is a reviewed diff of a generated file.

**32. Dataset schemas are merged at run time, after the rename**, from the generated files and
the catalog, cached per dataset, never inferred from data.

**33. Schemas are fixed before data arrives.** A live table needs its full column set before its
first row, the first message may omit optional fields, and two sessions must agree on a shape.

**34. Hand input is naming, grouping, and verification, never a column list.** It lives in
`refresh/`, every entry has a reason, and a stale entry fails the refresh.

**35. Runtime drift serves what is right** (2026-10-30). An extra field is dropped and reported;
a value that does not fit is null with one WARNING per dataset and field; a row whose key cannot
be read is dropped and counted. Only a payload in which no row can be placed fails, so the table
never silently lies.

**36. One timestamp routine for every transport**: epoch 0 to null outside the time column, a
checked multiply to nanoseconds, and a [1990, 2100] bound on epoch integers.

**37. No stream moves to delayed data by inference.** An unknown `timeframe` or a refused
snapshot connects to the real-time host with a WARNING, and no connection ever moves host.

**38. A never-acknowledged topic never reconnects its connection.** After one re-send it shows
as `NO_ACK` in its streams' `Error` and is re-sent alone every 60 s.

**39. S3 follows the shared network rules**: the shared CA bundle and proxy, botocore retries
off in favor of the shared schedule, TLS failures never retried.

**40. REST rate limits and host resolution are shared**: one `RequestGovernor` per API key and
REST origin across the process's sessions, and one in-flight host decision per (class, family).

**41. Test layout mirrors the package**, one leading underscore dropped per path component;
`__init__.py` maps to `test_init.py`, `__main__.py` to `test_main.py`, and `_` directories under
`tests/` are helpers.

**42. Import roots**: `pythonpath = ["."]` and `mypy_path = ["src"]`; fakes are imported as
`tests._fakes…`.

**43. Module names** `_namespaces/`, `_ingest/flatfiles.py`, `_datasets/history_source.py`,
and the wire package `_massive/flatfiles/` (PEP 8 forbids an underscore in a package name).

**44. Releases are one manual workflow run; PyPI through trusted publishing, off until the
owner turns it on** (2026-11-02). The run tests `main`, tags `v<version>`, and creates the GitHub
Release; the git tag is the only version source, and there is no hand-written changelog.

**45. `columns()` on every query** (2026-10-26): a static table of `Name`, `Type`,
`Description`, and `SuppliedBy`, built without a network call, so console users can read what
each column means.

**46. Every result carries column descriptions** (2026-10-26), applied through `jpy` to the
exact table a verb returns, on the caller thread inside the result scope.

**47. The column reference cannot hold an orphaned page** (2026-10-26).
`scripts/render_reference.py` owns `docs/datasets/`: it writes every page and deletes any file
it did not render, and a parity test fails on any difference.

**48. Vendor descriptions live in a separate generated file** (2026-10-26),
`spec/descriptions.json`, normalized to plain text and joined into each field at load.

**49. Each table keeps its endpoint's spelling** (2026-10-27). A concept Massive spells two ways
gets one column within a table, but there is no cross-table synonym map.

**50. Dataset parameter gaps** (2026-10-28). `order` is sent as is where a path has it and
folded into `sort` where it does not; instant axes are half-open and date axes whole days; the
whole family of every typed parameter is reserved from `filters`. *Its futures trade and quote
window is superseded by 52, and the indicators' `adjusted` by 67.*

**51. Clearly wrong vendor descriptions are corrected by hand input** (2026-10-28): a reasoned
`DESCRIPTION_FIXES` entry quotes and replaces text that another class's or event's text was
copied into, or that Massive's own documentation contradicts. Terse text is kept.

**52. Date ranges: two kinds, whole bar windows, UTC days for forex and crypto** (2026-10-28).
`history()` takes two dates (whole days or sessions, inclusive) or two aware datetimes
(half-open), never one of each. A bar is returned only when its whole window is inside the range
and has ended. Futures trades and quotes named by dates are read one session at a time. *Its
refusal of unexpected crypto stamps is superseded by 70.*

**53. One spelling for the transports in user-facing text** (2026-10-28): `REST`, `flat files`,
and `WebSocket`, so a filter copied from a page works on `columns()`.

**54. A missing description is filled from a sibling source** (2026-10-28) through a reasoned
`DESCRIPTION_FIXES` entry, so no column is blank when Massive already wrote the text elsewhere.

**55. Each dataset links its own Massive documentation** (2026-10-28) in its section of the
column reference, and only its own class's page of a shared source.

**56. Every bar of a day or longer is stamped at its window's start** (2026-10-28): 00:00 of its
first day in the class day zone (New York for stocks, options, and indices; UTC for forex and
crypto), so one `history()` returns the same rows from any source. Futures keep `window_start`
and `session_end_date`. *Its refusal of unexpected stamps is superseded by 70, and their
handling by 85.*

**57. Every row says when it happened, its trading day, and when it arrived; one `history()` has
one split basis** (2026-10-28). Ticks have `Timestamp`; bars and summaries `TimestampStart` and
an exclusive `TimestampEnd`; tick and bar tables a `Date` (ISO text); every dataset ends with
`TimestampInsert`. Ends are conservative where Massive's labels are unverified, and every
aggregates page's `adjusted` echo is checked.

**58. Canonical symbols only, with explicit converters** (2026-10-28). Every symbol in and out
is Massive's exact REST ticker; a non-canonical one raises `ValueError` naming the canonical
form. `canonical_symbol`, per-class and per-convention converters, and `canonicalize_symbols`
convert explicitly. A crypto pair is split only from Massive's crypto ticker list. *Its failure
defaults are superseded by 66.*

**59. Live tables keep their session running, and `close()` fails them** (2026-10-28). A live
table keeps alive what it needs and nothing else does; `m.close()` fails every live table with a
clear message rather than leave tables that look live. Sessions with one key and equal
connection settings share WebSocket connections, so they do not knock each other off a plan's
one slot.

**60. A REST rate governor per subscription** (2026-10-28). One bucket per `dm.Subscription`
plus an unpaced `other` bucket; `rest_basic_subscriptions` paces a subscription from its first
request, and a 429 naming the per-minute limit teaches the bucket it is Basic. A 429 pauses
(honoring `Retry-After` up to 10 minutes), then one probe goes alone. *Revised by 62.*

**61. A debug log good enough to fix a bug from** (2026-10-28). Standard `logging`, off by
default, turned on by `DEEPHAVEN_MASSIVE_LOG`, `DEEPHAVEN_MASSIVE_LOG_FILE`, or
`dm.enable_debug_logging()`. Log offending values and whole rows, never secrets.

**62. Each read gives up on its own; reference data has its own bucket; learned Basic lives
with its governor** (2026-10-29). A read gives up after about 10 minutes of pauses (about 90 s
under the update-graph lock) with an error that names the cause and remedies; the bucket never
gives up. Every reference endpoint is charged to `other`, which learns Basic like any bucket.
What a governor learned is forgotten with it.

**63. One-row endpoints are datasets of their own** (2026-10-29): `last_trade`, `last_quote`, and
`previous_day_bar`, each with `fetch()` and `poll()`. Datasets are named by what they are and
verbs name table shapes, so a `latest()` verb would break the second rule.

**64. Columns join, merge, and show only what a dataset fills** (2026-10-29). A dataset keeps the
columns of its kind that some source of its own can fill, never depending on a call's
arguments. Every numeric id is a `long`, a list of text a `String[]`, and other nested values
JSON text. The unified snapshot is `snapshots` everywhere.

**65. The library reports what streams and reads are doing** (2026-10-29). Diagnostics spell
transports as `columns()` does; `m.streams()` has a `Query` column; `dm.disable_debug_logging()`
exists; the `POLYGON_API_KEY` fallback is gone; `AUTO` warns when its source leaves columns null
that the other fills; a feed's `poll()` needs `start` or `limit`.

**66. One `dm.OnError {RAISE, LOG, IGNORE}` for symbol problems, `LOG` by default**
(2026-10-29). `MassiveConfig.noncanonical_symbols`, `unreadable_symbols`, and
`unavailable_symbols` use it. A bad row is skipped (or kept with a null `Symbol` and its
`RawSymbol`) and listed in `m.streams()` `Error`; a symbol table whose rows are all bad raises.
A `str` or list in code always raises.

**67. One name, form, and default per parameter** (2026-10-29). `symbols` is positional
everywhere; `as_of` is the point-in-time parameter; Massive's spec defaults show in signatures;
`None` means only "not sent", never a mode (`PollMode.AUTO`, `venues=frozenset(FuturesVenue)`,
indicators' `adjusted: bool = True`).

**68. Long downloads read in the background with `wait=False`** (2026-10-29). `fetch()` and
`history()` return at once a refreshing, empty table that receives the complete result in one
update. No `Future`, because its `result()` could not wait inside a console command.

**69. More inputs and a list of datasets** (2026-10-30). Date arguments take what
`deephaven.time` takes (Java instants and dates, numpy, pandas, Deephaven's own text) but never
integers or naive times; sub-microsecond instants round outward. `dm.datasets()` lists every
dataset; `deephaven_massive.queries` exposes the query types; `m.reference.news` follows a
watchlist.

**70. Bars are always served; an unexpected stamp warns, never refuses** (2026-10-30): "The
library should be serving up bars, not excluding them." *Its relabeling of such a stamp is
superseded by 85.*

**71. Every `bars()` call names its bar size** (2026-10-30). `timespan` is required: no length
suits most uses, and it is the first thing a reader of the call needs.

**72. A futures contract is a symbol** (2026-10-30). `futures.contracts` names the ticker
`Symbol` and takes `symbols`, so a contracts table feeds the market-data datasets directly.

**73. A query's repr is the call the user typed** (2026-10-30): public names, `symbols` first,
`… (N more)` after five, and only non-default arguments.

**74. `limit` caps each symbol's rows where a request names one symbol** (2026-10-30).
*Superseded by 87.*

**75. Price columns lead every table** (2026-10-30), ordered at refresh time.

**76. The debug log says what it needs to** (2026-10-30): pauses, permit waits, close reasons,
connection uptime, the git commit and platform; `DEEPHAVEN_MASSIVE_LOG_FILE` alone turns it on;
the library never rotates the file.

**77. A crypto day summary not in UTC is served with a warning** (2026-10-30), never raised.
*Its window is superseded by 85.*

**78. `AUTO` serves from whichever source can; it errors only when none can** (2026-10-30).
Nothing is guessed from Massive's approximate publish time: a missing day file is read from REST
and a flat-file refusal reads the whole call from REST, each with a WARNING.

**79. The user chooses real-time or delayed data, per subscription** (2026-10-31).
`MassiveConfig.delayed_subscriptions` lists the subscriptions that stream from the delayed feed;
every other one uses the real-time feed and fails loudly when the plan cannot serve it. The
library never moves a stream between feeds, and shows nothing as real-time without proof.

**80. Settings can come from a YAML file** (2026-10-31) through `MassiveConfig.from_file`. Code
overrides the file, which overrides the defaults; credentials are refused in the file; every
value goes through one explicit converter.

**81. A symbol problem is logged and the table continues, with an `OnError` setting for other
behavior** (2026-11-02). An unlisted crypto pair follows `MassiveConfig.unlisted_symbols`.

**82. Every API accepts a live table and follows it** (2026-11-02): never refused and never read
once without the user asking for it.

**83. History and live data are never mixed** (2026-11-02). No verb backfills a stream;
`merge` stays the user's recipe.

**84. `history()` and `fetch()` follow a live symbol table** (2026-11-02). An added symbol is
read over the same fixed range and lands whole in one cycle; a removed one leaves.
`reread=dm.Reread.ALWAYS` is the default; `m.symbol_reads()` shows each read. *Its per-symbol
`limit` is superseded by 90.*

**85. A period stamped at an unexpected time keeps Massive's stamp** (2026-11-02). It has a null
`Date` and ends a period after its stamp, with one WARNING per form of stamp, instead of being
moved to a guessed day that could cause look-ahead.

**86. Indicators keep the current period's unfinished value** (2026-11-02); filter with
`TimestampEnd <= TimestampInsert`.

**87. `limit` is Massive's total cap per request** (2026-11-02). Use it to sample; read without
it for complete data. One request per symbol made reads of many symbols take minutes on Basic.

**88.** Not assigned.

**89.** Not assigned.

**90. A row that lists several tickers is listed under each named one** (2026-11-02). The ten
datasets whose rows list tickers gain a `Symbol` column, one row per named ticker the row lists.
An unlisted crypto pair follows `unlisted_symbols` in every REST verb.

**91. `last_by()` on a live symbol table shows a row by update cycle, not by receive time**
(2026-11-02), so a quiet symbol's first row is not hidden until its next tick.

**92. The library never uses input tables** (2026-11-02). They are for people editing data by
hand; the diagnostics tables are blink tables with `last_by`.

**93. `dm.canonicalize_symbols` skips and logs a bad row by default** (2026-11-03). Its
`on_error` default is `LOG`, following the rule that one bad row never fails a table.

**94. `poll()` refuses an interval shorter than 1 second** (2026-11-03). A shorter interval is
almost always a typo that would send requests back to back; a stream serves anything faster.

**95. `dm.datasets()` names its query-method column `Methods`** (2026-11-05). The docstrings call
`fetch()`, `history()`, and the stream methods "methods", Python's word, so the column and the
dataset pages' row say `Methods` too.

**96. OPRA's admin messages are a stream without symbols, kept as sent** (2026-11-09).
`m.options.admin_messages()` serves Massive's `options/AD` channel: OPRA's administrative
messages and FLEX trade notices, one row per message with `Exchange`, `MessageId`,
`SequenceNumber`, the SIP time as `Timestamp`, and the text as `Message`. Massive sends them per
exchange, not per contract, so the call takes no symbols, subscribes every exchange (`AD.*`),
and its rows have no `Symbol`. A dataset marks this with `names_symbols=False`: its query is a
plain `StreamQuery` with `blink()`, `append_only()`, and `ring()`, and no `last_by()`, whose
default key is `Symbol`. Every stream whose rows name their symbol is a `SymbolStreamQuery`,
which adds `last_by()`; `StreamHistoryQuery` builds on it. The text is never parsed: Massive
documents no format, its sample does not match OPRA's FLEX layout, and a parse would guess at
Massive's data. The channel is real-time only, with no history; a dataset Massive streams only
in real time (`real_time_only`, also every fair-market-value stream) refuses a session that
reads its subscription from the delayed feed with `ValueError`, and a key whose data Massive
labels delayed with an `EntitlementError` that points to the dataset's page of plans. A
subscription Massive refuses fails the table with the same pointer.

**97. A taken connection fails its live tables by default: `MassiveConfig.connection_taken`**
(2026-11-10). Massive keeps the newer of two WebSocket connections with one key for an asset class. In
three live runs (2026-11-03 and twice on 2026-11-10), 2.6 to 8.3 s after the newer connection
authenticated, the older, authenticated socket got the status `max_connections` ("Maximum number
of websocket connections exceeded. You have reached the connection limit for your account...")
and then close code 1008. Treated as `WAITING_SLOT`, the older client reconnected after
`reconnect_backoff` and evicted the newer, so two clients took turns indefinitely, each outage
(about 35 s) shorter than `max_outage`, and no table ever failed. A `max_connections` reply to
the key has never been observed. The wire client now reports a `max_connections` status on an
authenticated socket as its own state, `TAKEN`, distinct from a refusal at the key
(`WAITING_SLOT`), and the library applies `dm.ConnectionTaken`. Under `FAIL`, the default, the
tables fail at once with a `MassiveError` naming the host, the time, and the fix (stop the other
program, or set `RECONNECT`), through the flush thread's normal failure path. Under `RECONNECT`,
the connection reconnects after its reconnect backoff, which takes it back; it shows
`RECONNECTING`, and each table's `Error` says when it was taken and when it is taken back.
The setting is applied per table, from its session, and is no connection setting
(`CONNECTION_SETTINGS`): sessions differing only in it share connections, so they never evict
each other. When a shared connection is taken, the tables of `FAIL` sessions fail; it reconnects
only while a table of a `RECONNECT` session remains, and otherwise closes, showing `CLOSED`. A
plain drop still reconnects, and a refusal at the key still waits (`WAITING_SLOT`) and fails at
`max_outage`. `reconnect_backoff` keeps its 15 s minimum, now for limiting reconnect churn, not
because Massive refuses a sooner reconnect.

**98. Every type a public signature names is public: `deephaven_massive.namespaces`**
(2026-11-10). A private class as a public return type is a major API failure. The classes of
`m.stocks` and the other fifteen namespaces were private (`_Stocks`), so API references, IDE
hovers, and users' annotations could not name the type that holds every dataset method. The
namespace classes are now public under the names of their properties (`Stocks`,
`StockFinancials`, `EtfGlobal`), exported from a second public module,
`deephaven_massive.namespaces`, which mirrors `queries`: for annotations only, and not imported
by `import deephaven_massive`. `queries` holds what a dataset method returns; `namespaces` holds
what the session's namespace properties return; the root holds what users construct or pass.
`queries` also exports `AllSymbols` (the type of `dm.ALL`, named by `Symbols`) and
`StreamBarTimespan` and `FuturesBarTimespan` (the `timespan` types of the `bars()` overloads).
Users never construct a namespace or a query: a constructor parameter whose name starts with an
underscore (`_core`) is internal, and its type may be private. Out of scope: private base
classes (`Namespace`, `QueryBase`), which no signature names, and the private `MassiveError`
subclasses raised at run time, which users catch as `dm.MassiveError`. A test walks the public
names, the members of every class they reach, and every signature, overload, default, and
alias, and fails on any private type.
