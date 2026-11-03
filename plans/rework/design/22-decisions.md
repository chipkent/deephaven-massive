## 22. Decisions

The owner settled each of the following. They are recorded here as rationale; the body of this
document already reflects them.

1. **No SDK: own clients over Massive's documented APIs** (revised 2026-10-26; this replaces
   "official SDK" and the fork). The library has its own REST client on `urllib3` and its own
   WebSocket client on `websockets`, and speaks Massive's documented protocols directly
   ([section 19](19-wire-clients-and-the-spec-snapshot.md#19-wire-clients-and-the-spec-snapshot)). Using Massive's SDK would have meant
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
   ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)). Hand input is naming, grouping, and verification
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
    and names `websocket_hosts`; it cannot misbehave silently, and it spares every base Business user a
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
    gates fail on a wrong name, type, unit, or merge ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)). The
    final names: `av` → `AccumulatedVolume`, `op` → `OfficialOpen` (2026-10-27: a neutral name
    for the stocks and options opening price and the indices opening level, as `Open` is),
    aggregate `a` → `DayVwap`,
    aggregate `z` → `AverageTradeSize`, `dav` → `AccumulatedDecimalVolume`, stocks `dv` →
    `DecimalVolume`, futures `dv` → `DollarVolume`, and the snapshot object `min` → `LastMinute…`.
    The naming policy of section 19.4 (owner decision, 2026-10-27) governs every name.
23. **Spec drift is reported, not gated** (2026-10-26). A weekly workflow runs
    `python -m deephaven_massive._massive.refresh --check` and keeps one standing "Massive API
    drift" issue, creating its label when missing; with no drift it does nothing, and it never
    opens a pull request or becomes a gate ([section 19.6](19-wire-clients-and-the-spec-snapshot.md#196-refresh-and-drift)).
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
    and the layers ([section 19](19-wire-clients-and-the-spec-snapshot.md#19-wire-clients-and-the-spec-snapshot)).
27. **`MassiveError` carries `status` and `request_id`** (2026-10-26). Both are read-only and
    `None` when the error did not come from an HTTP response ([section 3](03-exports.md#3-exports)).
28. **`reconnect_backoff` is at least 15 s** (2026-10-26). `MassiveConfig` rejects a smaller
    value with `ValueError` ([section 5](05-massiveconfig.md#5-massiveconfig)).
29. **`refresh/` is not shipped** (2026-10-26). The wheel excludes all of `refresh/`, the
    snapshot included; `spec/*.json` ship ([section 19](19-wire-clients-and-the-spec-snapshot.md#19-wire-clients-and-the-spec-snapshot)).
30. **Wire behaviour settled in review** (2026-10-26; the REST 429 half revised 2026-10-28 by
    decision 60). A REST 429 pauses its subscription's requests: without a usable
    `Retry-After`, for a whole 61 s window when the subscription is paced and for 15, 30, then
    60 s when it is not; with one, for as long as it asks, up to 10 minutes; and for at most 10 minutes of pauses
    in a run; a TLS certificate failure is never
    retried; a 429 at the WebSocket upgrade is retried; wildcard transitions make before they
    break, accepting a brief duplicate rather than a gap
    ([section 13.3](13-host-resolution-and-connection-protocol.md#133-connection-and-subscription-protocol),
    [section 19](19-wire-clients-and-the-spec-snapshot.md#19-wire-clients-and-the-spec-snapshot)).
31. **No hand-written schema anywhere.** There are no hand tick schemas, key maps, column
    overrides, or name lock; every change to a schema is a reviewed diff of a generated file
    ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)).
32. **Dataset schemas are merged at run time, after the rename**, from the committed generated
    files and the catalog, cached per dataset; never inferred from data
    ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)).
33. **Schemas are fixed before data arrives** (principle 11): a live table needs its full column
    set before its first row, the first message may omit optional fields, and two sessions must
    agree on a dataset's shape.
34. **Hand input is naming, grouping, and verification, never a column list.** It lives in
    `refresh/`, every entry carries a reason, and a stale entry fails the refresh. The
    page-to-prefix map is derived from the vendor's overview tables
    ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)).
35. **The runtime drift policy** of [section 19.5](19-wire-clients-and-the-spec-snapshot.md#195-runtime-drift) (2026-10-30): a valid
    request returns its data whenever it can, and serves only what is right. An extra field is
    dropped and reported; a value that does not fit, outside the key columns, is null with one
    WARNING per dataset and field; a row whose key cannot be read is dropped, with one WARNING
    per dataset and a running count. Only a payload in which no row can be placed fails
    loudly: a REST page, a flat file's rows of the requested symbols, or a run of
    `DROPPED_RUN_LIMIT` (100) WebSocket rows of a dataset on a channel. A futures flat-file
    row keeps its own session. The table never silently lies.
36. **One timestamp routine** for every transport: epoch 0 to null except in the time column,
    a checked multiply to nanoseconds, and the [1990, 2100] bound on epoch integers only
    ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)).
37. **No stream is moved to delayed data by inference.** An unknown `timeframe` value, like a
    refused snapshot, connects the class to the real-time host with `REALTIME` recency and a
    WARNING, and no connection ever moves to another host (decision 79) ([section 13.2](13-host-resolution-and-connection-protocol.md#132-host-decision), [section 13.3](13-host-resolution-and-connection-protocol.md#133-connection-and-subscription-protocol)).
38. **A never-acknowledged topic** never reconnects its connection: after one re-send it counts
    as `NO_ACK`, shows in its streams' `Error`, and is re-sent alone every 60 s until it is
    acknowledged ([section 13.3](13-host-resolution-and-connection-protocol.md#133-connection-and-subscription-protocol)).
39. **S3 follows the shared network rules:** the `net` CA bundle and proxy, botocore retries off
    in favour of the shared schedule, TLS never retried ([section 19.3](19-wire-clients-and-the-spec-snapshot.md#193-the-flat-file-client)).
40. **REST rate limits and host resolution are shared.** One `RequestGovernor` per API key and
    REST origin, shared by the process's sessions (decision 60), and single-flight host
    resolution through one future per (class, family)
    ([section 13.2](13-host-resolution-and-connection-protocol.md#132-host-decision), [section 19.1](19-wire-clients-and-the-spec-snapshot.md#191-the-rest-client)).
41. **Test layout:** every path component drops one leading underscore, `__init__.py` maps to
    `test_init.py` and `__main__.py` to `test_main.py`, `_` directories under `tests/` are
    helpers, and `tests/datasets/test_catalog.py` is a plain mirror ([section 4.2](04-package-layout.md#42-test-layout)).
42. **Import roots:** `pythonpath = ["."]` and `mypy_path = ["src"]`, fakes imported as
    `tests._fakes…`; ruff bans `import massive` once the dependency is gone.
43. **Module names:** `_namespaces/` (plain modules of namespace classes), `_ingest/flatfiles.py`,
    `_datasets/history_source.py`, and the wire package `_massive/flatfiles/` (PEP 8 forbids the
    underscore in a package name, and the library module follows the wire package's spelling).
44. **Release by one manual run that tests, tags, and releases; PyPI through trusted
    publishing, off until the owner turns it on** (2026-11-02). `.github/workflows/release.yml`
    is started by hand on `main`; once the full CI workflow passes on `main`'s newest commit,
    it tags that commit `v<version>` and creates the GitHub Release, with generated notes and
    the wheel and sdist. Its PyPI job (trusted publishing, no long-lived token) runs only when
    the repository variable `PUBLISH_TO_PYPI` is `true`, release candidates included. The git
    tag is the only source of the version (hatch-vcs); the run takes the version as an input,
    in PEP 440 form, greater than every tagged version, and builds the files with it. Candidate
    tags and pre-releases stay after the final release. No hand-written changelog. The first
    release candidate is 0.1.0rc1 ([section 20.1](20-quality-gates.md#201-tooling-configuration)).
45. **`columns()` on every query** (2026-10-26). A public method, not a verb, returning a
    static table `Name`, `Type`, `Description`, `SuppliedBy` built from the dataset schema with
    no network call, so console and notebook users can read what each column means without the
    web UI or GitHub ([section 7](07-query-classes-and-verbs.md#7-query-classes-and-verbs)).
46. **Every result carries column descriptions** (2026-10-26). Each verb applies
    `ColumnDescriptions` and a `TableDescription` to the exact table it returns, after its
    shape is derived, on the caller thread inside the result scope, through `jpy`, because
    Deephaven 42.7 has no Python wrapper ([section 7.4](07-query-classes-and-verbs.md#74-column-descriptions)).
47. **A generated column reference that cannot hold an orphaned page** (2026-10-26).
    `docs/datasets/<namespace>.md` is generated and committed. One command,
    `uv run python scripts/render_reference.py`, owns the whole directory: it renders every
    page, deletes every file it did not render, and writes the rest. The parity test fails on a
    stale page, a missing page, and any extra file. Dataset docstrings link to their section and
    never list columns ([section 7.4](07-query-classes-and-verbs.md#74-column-descriptions)). The renderer and `sync` live in
    `_datasets/column_reference.py`, which takes the catalog as an argument, so the name never
    collides with the `m.reference` namespace and the renderer needs nothing from the API.
48. **Vendor descriptions in a separate generated file** (2026-10-26). `spec/descriptions.json`
    holds every field's vendor description (corrected where it is clearly wrong, decision 51),
    normalized to plain text by fixed rules, with no
    gate on a missing one, and is joined into `Field.description` at load; one rule picks each
    dataset column's description ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)).
49. **Each table keeps its endpoint's spelling** (2026-10-27). One concept spelled two ways by
    Massive gets one column within a table, but no cross-table unification or synonym map
    ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas), "Cross-table naming").
50. **Dataset gaps settled while declaring the namespaces** (2026-10-28). `order` is sent as
    is where a path has its own `order` parameter and folded into `sort` as a `.asc`/`.desc`
    suffix where it has none, and only there is `order` without `sort` a `ValueError`; a
    dataset whose paths have neither parameter offers neither, and the indicators, whose
    paths have `order` but no `sort`, keep `order` alone ([section 9](09-filters.md#9-filters)). The whole family of every typed
    parameter is reserved from `filters` in one shared check. Instant axes are half-open
    (`.gte`/`.lt`), date axes whole days (`.gte`/`.lte`). Futures trades and quotes take the
    `timestamp` range widened to whole Chicago sessions, as bars do (superseded by decision 52)
    ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)). The REST transport fills path
    parameters from `Request.params`, fills the bars `{from}`/`{to}` from `history()`'s bounds,
    batches symbols by `DatasetSpec.symbols_per_request`, picks a path per symbol class by
    `DatasetSpec.fetch_paths_by_class`, sends only the parameters the chosen path declares,
    picks a per-symbol detail path from
    the role paths, canonicalizes symbols read from rows, and drops error rows only from a
    projected `fetch()` ([section 13.4](13-host-resolution-and-connection-protocol.md#134-rest)). The § 8 rows gain `product_code` on every
    futures dataset that takes it, `adjusted` on the options `daily_open_close` (the crypto
    `bars` and `daily_open_close` take none: crypto has no corporate actions), `filters` on
    `consensus_ratings`, `type=options` on `options.snapshots`, and the generated
    axis of `share_float` ([section 8](08-namespace-tree.md#8-namespace-tree)). The indicators took
    `adjusted: bool | None = None`, None sending nothing; decision 67 makes it
    `adjusted: bool = True`, with the same meaning, under the rule that None is never a mode.
51. **Wrong vendor descriptions are corrected by hand input** (2026-10-28). Where Massive's
    own description of a field is clearly wrong for its source (another asset class's or
    event's text copied onto it, or a statement Massive's own documentation of the same field
    contradicts), a reasoned `DESCRIPTION_FIXES` entry in `refresh/overrides.py` quotes it and
    replaces it, per asset class for a source that serves several; a merely terse or imprecise
    description is kept. The replacement is Massive's own sibling text wherever that is right
    and adequate, and is written in the entry only otherwise. `spec/descriptions.json` changes
    only through the refresh ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)).
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
    ex-dividend date appear ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries),
    [section 16](16-historysourceauto.md#16-historysourceauto)).
53. **One spelling for the transports in user-facing text** (2026-10-28). The key-column
    descriptions, `columns()`'s `SuppliedBy`, and the column reference spell the transports
    `REST`, `flat files`, and `WebSocket`, so a filter copied from a page works on
    `columns()`; the internal `Transport` values (`REST`, `FLAT_FILES`, `WS`) are unchanged.
    A key column's description lists its sources in one sentence starting "From", joined by
    "; ", so a lowercase label never starts a sentence ([section 7.4](07-query-classes-and-verbs.md#74-column-descriptions),
    [section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)).
54. **A missing description is filled from a sibling source** (2026-10-28). Where Massive
    gives a field no description but describes the same field on a sibling source, a
    `DESCRIPTION_FIXES` entry quotes the empty text and takes the sibling's, with its reason,
    so no column of a dataset is left blank for want of text Massive already wrote
    ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)).
55. **Each dataset links its own Massive documentation** (2026-10-28). The column
    reference lists each dataset's vendor pages under that dataset's section instead of one
    page-wide list at the foot of the page, which tied no link to a dataset and, through a
    source several asset classes share, linked unrelated classes' pages (the options page
    listed the stocks, forex, crypto, and indices unified-snapshot pages). A dataset of one
    asset class links only that class's page of a shared source; the footer keeps the fetch
    date ([section 7.4](07-query-classes-and-verbs.md#74-column-descriptions)).
56. **Every bar of a day or longer is stamped at its window's start** (2026-10-28). One
    `history()` call returns the same rows whichever source answers it, so a bar of a day or
    longer of every class but futures, from every source, is stamped at 00:00 of the day it
    starts on in the class day zone: New York for stocks, options, and indices, UTC for forex
    and crypto. This extends decision 52's UTC stamping of forex and crypto 1-day bars to
    stocks, options, and indices, whose grouped daily, `/prev`, and stock `day_aggs_v1` bars
    Massive stamps at the 16:00 New York session close while the range stamps the day's start.
    Only the stamps the vendored samples show for a class, the 13:00 close of a short trading
    day for stocks, options, and indices, and the 16:15 and 13:15 closes of index options are
    moved (the table of [section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)): each lies
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
    `/futures/v1/schedules`, a row the schedules cannot place (a spread, which is never looked
    up, a lookup still running or failed, a row outside the loaded sessions) getting a null
    `Date`, never failing the stream and never refusing a subscription; a futures stream verb
    waits up to 15 s for its contracts' schedules before it subscribes (1 s when it holds the
    update graph's lock, as a console command does). The ends are conservative where Massive's labels are not
    verified: an index bar of a day or longer ends at 01:00 New York time, and an index week or
    longer after its calendar period, weekend included (revised 2026-10-30: a stock, option, or
    index bar of several days counts calendar days, as the live check of item 63 found); a
    futures bar of a session or longer (revised 2026-11-02, after the live check of item 40
    found its buckets and labels) at 17:00 Chicago on its label, its last session, returned by
    `history()` only once it has certainly ended; a futures
    one-second WebSocket bar at Massive's `e` (revised 2026-11-02 by the owner: the library
    serves `e` as sent and logs a WARNING when it is not the start plus one second; the start
    plus one second only when Massive sends no `e`). `history()` keeps a
    bar only when its start lies in the range and its `TimestampEnd` is no later than the
    range's end (for dates, the last day's end) or now, so selection and the join key apply the
    same end; a futures bar named by dates must have every session in the range, so the
    refusal of futures bars of several sessions named by dates is gone. A symbol with no data
    returns no rows; HTTP 404 after the first page of a request series is an error, and a
    futures session with no data is that session's. `adjusted=True` stays the default of stock
    and option bars and open-close, `None` (Massive's default, adjusted) that of the indicators;
    one `history()` never mixes bases, every aggregates page's `adjusted` echo is checked, and
    the jumps at splits in live tables are the data, documented with the factors to adjust
    as-traded data. `Timestamp`, `Timestamp<Qualifier>`, and `Date` are the library's names
    ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries), [section 13.4](13-host-resolution-and-connection-protocol.md#134-rest), [section
    19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)).

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
    ([section 10](10-symbols-and-all.md#10-symbols-and-all)). After review: futures spreads are Massive's tickers as
    written, with their own converter `canonical_symbol_future_spread` and no builder from legs;
    a futures dataset takes any ticker as written; a dataset of global third-party tickers
    (TMX) keeps them as written; under a convention only that convention's grammar applies;
    any `X:` ticker is canonical, and wherever a pair must be split (a stream, the last-trade
    and daily open/close paths) its base and quote come only from Massive's crypto ticker
    list, read on first need and again, at most once an hour, when it lacks a ticker asked
    for, and a ticker the list does not hold then is refused rather than guessed; under `WARN` a keyed `poll()` adds `RawSymbol` to its key, so an unreadable row
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
    ([section 6](06-massivesession-and-lifecycle.md#6-massivesession-and-lifecycle),
    [section 6.1](06-massivesession-and-lifecycle.md#61-how-a-live-table-is-kept-alive-and-released),
    [section 13.1](13-host-resolution-and-connection-protocol.md#131-invariants)).
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
    the classes the run's plans leave on Basic declared ([section 13.4](13-host-resolution-and-connection-protocol.md#134-rest),
    [section 17](17-diagnostics.md#17-diagnostics), [section 19.1](19-wire-clients-and-the-spec-snapshot.md#191-the-rest-client)).
61. **A debug log good enough to fix a bug from** (2026-10-28). Standard `logging` only, off
    by default; `DEEPHAVEN_MASSIVE_LOG` (and `DEEPHAVEN_MASSIVE_LOG_FILE`), read at the first
    session, and `dm.enable_debug_logging()` turn it on; stderr by default and a file
    only when asked for. Log too much rather than too little: offending values, whole rows, and
    error bodies at DEBUG, the same values in error messages, with no truncation that hides the
    problem. Secrets are never logged. Every change logs what it decides (the rules in the
    `_debug_log` docstring). The bug report asks for the log and `m.streams()`, and adds no
    warning about what a log contains ([section 17.1](17-diagnostics.md#171-the-log)).
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
    ([section 13.4](13-host-resolution-and-connection-protocol.md#134-rest), [section 17](17-diagnostics.md#17-diagnostics)).
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
    ([section 7.5](07-query-classes-and-verbs.md#75-one-row-datasets), [section 7.6](07-query-classes-and-verbs.md#76-a-verb-a-query-does-not-have)).
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
    ([section 7.4](07-query-classes-and-verbs.md#74-column-descriptions), [section 8.11](08-namespace-tree.md#811-msnapshots),
    [section 11](11-enums-and-types.md#11-enums-and-types), [section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)).
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
    ([section 5](05-massiveconfig.md#5-massiveconfig), [section 7](07-query-classes-and-verbs.md#7-query-classes-and-verbs),
    [section 16](16-historysourceauto.md#16-historysourceauto), [section 17](17-diagnostics.md#17-diagnostics),
    [section 17.1](17-diagnostics.md#171-the-log)).
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
    sends in a spelling the library cannot read. A third, `unavailable_symbols` (owner
    standard, 2026-11-02: log and continue with an `OnError` option), governs a symbol a
    `poll()` cycle cannot read from Massive (HTTP 404, or a path that lacks a parameter the
    poll sends). They have no environment variables: code or
    the settings file sets them (decision 80). A `str` or a list in code
    always raises, whatever the setting, and `canonicalize_symbols` keeps its own `on_error`
    argument, since it is a standalone function; its default is `LOG` too (decision 93). No per-dataset parameter: it
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
    reference say so ([section 5](05-massiveconfig.md#5-massiveconfig), [section 10](10-symbols-and-all.md#10-symbols-and-all)).
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
    ([section 8](08-namespace-tree.md#8-namespace-tree), [section 9](09-filters.md#9-filters)).
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
    ([section 6.1](06-massivesession-and-lifecycle.md#61-how-a-live-table-is-kept-alive-and-released),
    [section 7](07-query-classes-and-verbs.md#7-query-classes-and-verbs), [section 17](17-diagnostics.md#17-diagnostics)).
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
      ([section 7.7](07-query-classes-and-verbs.md#77-the-dataset-list)).
    - The query classes and the argument aliases are public, in `deephaven_massive.queries`,
      which the package never imports: annotating a helper should not need a private import.
      The module also exports `Symbols`, `DateLike`, and `FilterValue`, since a helper that
      takes a query usually takes its arguments too.
    - `m.reference.news` follows a watchlist: `symbols` stays plural (one ticker, a list, or a
      live symbol table), the library sends one request per ticker and keeps an article
      several tickers return once, by `Id`. `limit`, `sort`, and `order` apply per ticker, so
      one ticker's articles never crowd out another's. A singular `symbol` would have removed
      watchlist support, which must stay (decision 4).
    ([section 3](03-exports.md#3-exports), [section 8.7](08-namespace-tree.md#87-mreference), [section 11](11-enums-and-types.md#11-enums-and-types)).
70. **Bars are always served; an unexpected stamp warns, never refuses** (2026-10-30; the owner
    decided: "The library should be serving up bars, not excluding them"). No `history()`,
    `fetch()`, or `poll()` call fails, and no row is dropped, because a bar's stamp is not one
    Massive is known to use for its class and timespan. Every bar of a day or longer of a class
    with a day zone is stamped the same way, at 00:00 of the day its stamp falls on in the class
    day zone, the best rule available, and its `Date`, `TimestampStart`, and `TimestampEnd`
    follow from that day ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)); decision 85
    replaces this for a stamp Massive is not known to use, which is now kept as sent. An
    unexpected stamp logs one WARNING per dataset and timespan per session, through the session's
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
    `timespan=Timespan.DAY` ([section 7.1](07-query-classes-and-verbs.md#71-bars-overloads)).
72. **A futures contract is a symbol** (2026-10-30, owner). `futures.contracts` names the row's
    `ticker` `Symbol` (with `RawSymbol`), takes optional positional `symbols` sent as
    `ticker.any_of`, polls by `Symbol` and `Date`, and with `products` takes `as_of`, sent as
    `date`. A contracts table then feeds the market-data datasets directly. Benzinga consensus
    ratings take `Symbol` from the row's own `ticker` (`KeySpec("Ticker")`), as the other
    Benzinga datasets do ([section 8.3](08-namespace-tree.md#83-mfutures)).
73. **A query's repr is the call the user typed** (2026-10-30, owner): public names, `symbols`
    first and positionally, `… (N more)` after five symbols, a required argument by position,
    and only the arguments that differ from their defaults
    ([section 7](07-query-classes-and-verbs.md#7-query-classes-and-verbs)).
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
    ([section 17.1](17-diagnostics.md#171-the-log)).
77. **A crypto day summary not in UTC is served with a warning** (2026-10-30, owner). A crypto
    `daily_open_close` row whose `isUTC` is false is returned with the UTC day's window and
    logs one WARNING per dataset per session, through the session's once-per-session warnings
    as decision 70's bar stamps do: its day boundary differs from the other crypto tables', so
    its dates may not line up with theirs. It never raises. Decision 85 replaces its window.
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
    outweighs that ([section 16](16-historysourceauto.md#16-historysourceauto)).
79. **The user chooses real-time or delayed data, per subscription** (2026-10-31, owner).
    `MassiveConfig.delayed_subscriptions` lists the subscriptions whose streams use Massive's delayed feed,
    even on a real-time plan; every other subscription uses the real-time feed and fails loudly
    when the plan cannot serve it: the stream verb raises `EntitlementError` when the snapshot
    shows delayed data, and a refusal from Massive fails the refused streams, each naming the
    evidence and the exact fix. The choice is deterministic: the library never moves a stream
    between feeds, and the server-verdict move to the delayed feed is gone. A listed
    subscription whose snapshot shows real-time data logs one WARNING saying so. `CURRENCIES`
    cannot be listed (Massive has no delayed forex or crypto feed), FMV cannot stream on a
    listed subscription (it is real-time only), and a `websocket_hosts` pin that contradicts the list
    is refused at construction. `fetch()` and `poll()` of a subscription's current data fail
    when Massive labels a response or a row `DELAYED` and the subscription it labels is not
    listed (each row of the cross-class snapshot is held to its own class;
    `underlying_asset.timeframe` is never read; reference endpoints are never checked); REST
    cannot ask for delayed data, so a listed subscription's REST reads return what the plan
    serves. `history()` is past data, not a way to get current data, and is not
    affected. `m.streams()` shows each stream's `Recency`, and each live table's description
    names it; nothing is shown as real-time without a label or a host that proves it
    ([section 5](05-massiveconfig.md#5-massiveconfig), [section 13.2](13-host-resolution-and-connection-protocol.md#132-host-decision),
    [section 13.4](13-host-resolution-and-connection-protocol.md#134-rest)).
80. **Settings can come from a YAML file** (2026-10-31, owner). `MassiveConfig.from_file`
    reads one, from a path or from `DEEPHAVEN_MASSIVE_CONFIG_FILE`; nothing reads a file
    unasked. Code overrides the file, which overrides the defaults. Credentials stay in code or
    the environment and are refused in the file. Every scalar is text and every value goes
    through one explicit converter per field, so YAML's type guessing never reaches a setting,
    and enum words are taken exactly as spelled. The two symbol-policy environment variables
    are removed: a setting has one source outside code, the file. The log's two variables stay,
    since the log belongs to the process, not to a session. The vocabulary layer's rule becomes
    "no network I/O and no threads", since `_config` reads the file
    ([section 5](05-massiveconfig.md#5-massiveconfig)).
81. **A symbol problem is logged and the table continues, with an `OnError` setting for other
    behavior** (2026-11-02, owner standard). Every per-symbol problem follows the pattern of
    `noncanonical_symbols`: `LOG`, the default, skips the symbol, warns once per value, lists
    it in the stream's `m.streams()` `Error`, and keeps running; `IGNORE` skips it quietly;
    `RAISE` raises at the call and fails the result for a symbol added later; a table whose
    symbols are all bad raises at the call. A crypto pair of a symbol table that Massive's
    crypto ticker list does not hold follows it through `MassiveConfig.unlisted_symbols`, for
    pairs present at the call and added later; a temporary failure to read the list is retried,
    bounded, and then treated the same way, with an `Error` that says the list could not be
    read ([section 10](10-symbols-and-all.md#10-symbols-and-all)).
82. **Every API accepts a live table and follows it** (2026-11-02, owner). Wherever an
    argument takes symbols or another key value, a refreshing Deephaven table is accepted and
    followed as it changes, the Deephaven-idiomatic way: never refused, and never read once
    without the user asking for it (`table.snapshot()` does).
83. **History and live data are never mixed** (2026-11-02, owner). No verb returns history
    followed by live rows, and nothing backfills a stream table; `merge` of a `history()` and a
    stream verb stays the user's recipe.
84. **`history()` and `fetch()` follow a live symbol table** (2026-11-02, owner), as
    [section 7.8](07-query-classes-and-verbs.md#78-following-a-live-symbol-table) specifies. Only a refreshing table is
    followed, and its result is always refreshing, with an `m.streams()` row that gains a
    `Loading` column. The range is fixed at the call. An added symbol is read over the same
    window and lands whole in one cycle; a removed one leaves in the same cycle. Under `AUTO`
    a followed read uses REST whenever REST can serve it. A symbol's 404 is `DONE` with no
    rows, transient errors retry after 1, 5, and 15 minutes, permanent ones fail that symbol,
    and only authentication, a closed session, a failed symbol table, `RAISE`, or a library
    bug fails the table. `wait=True` raises only when every call-time symbol fails. The
    single-value inputs `underlying=` and `product_code=` follow tables too. Within it:
    - **`limit`** in a followed read is sent as on any read, once on each request; decision
      90 replaces the per-symbol cap this first said.
    - **`reread=dm.Reread.ALWAYS`** on `history()` (owner decision D-1, 2026-11-02: `ALWAYS`
      the default, `NEVER` opt-in): `ALWAYS` downloads a returning symbol again, and `NEVER`
      reuses its earlier download for the life of the table. There is no automatic member,
      because no date or time rule can tell that Massive's data is final
      ([section 7.8](07-query-classes-and-verbs.md#78-following-a-live-symbol-table) cites the evidence). `fetch()` always
      reads again.
    - **`m.symbol_reads()`** shows each symbol's read: its state, source, row count, attempts,
      and error, kept for the session.
    The alternatives were a table that fails when its symbols change, which reads the table
    once and leaves the user no idiomatic way to react (decision 82), and reading `dm.ALL` and
    filtering it, which most datasets and plans cannot serve and which would turn a
    three-symbol read into a read of the whole market; it stays a documented recipe.
85. **A period stamped at an unexpected time keeps Massive's stamp** (owner decision D4,
    2026-11-02; replaces part of decisions 70 and 77). A bar or indicator value of a day or
    longer whose stamp Massive is not known to use for its class and timespan is no longer
    moved to 00:00 of the day it falls on, which guessed the day and, when the guess was wrong,
    could end the window before the data closed, a look-ahead. It keeps the stamp as
    `TimestampStart`, has a null `Date`, and ends at the stamp plus its period, a conservative
    end. One WARNING per form of stamp per session (the dataset, zone, bar length, and the
    stamp's time of day, weekday, or day of the month) names the dataset, the source, the
    symbol, the raw stamp, the known stamps, and that `Date` is left empty. Keeping such a stamp
    fails and skips nothing: the row is supplied with Massive's stamp and a null `Date`, so it
    is not a one-bad-row failure and needs no `OnError` setting; the WARNING is right. A crypto
    summary not in UTC follows the same rule of keeping Massive's data rather than guessing: a
    crypto `daily_open_close` row whose `isUTC` is false keeps Massive's date and `IsUtc`, and
    its window is the widest its date has in any time zone, 00:00 at UTC+14 to 24:00 at UTC-12,
    with the same kind of WARNING. Known stamps still move to 00:00. The bars stay served
    (decision 70); `history()` keeps them by the whole-window rule, on the conservative window. The runtime drift records
    (`SchemaDrift`) hold field drift of the wire payloads only, so the WARNING is the record
    ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)).
86. **Indicators keep the current period's unfinished value** (owner decision D2, 2026-11-02:
    keep the unfinished value; filter with `TimestampEnd <= TimestampInsert`). `fetch()` and
    `poll()` return what Massive reports now, today's still-changing value included, as
    before. Every `TimestampEnd` description says that a row read before its window ended has
    `TimestampInsert` before `TimestampEnd` and gives the filter, and when it is approximate;
    each indicator docstring gives the filter in one line. A `poll(mode=APPEND)` table keeps
    the first unfinished value it sees for a period ([section 8.8](08-namespace-tree.md#88-mindicators)).
87. **`limit` is Massive's total cap per request** (2026-11-02, owner). Reversed by the owner
    on 2026-11-02: `limit` is Massive's total cap per request; one request for the named
    symbols. Use `limit` to sample; read without it for complete data. The earlier decision
    (D3(a), the same day) sent one request per named symbol under a `limit`, which made a read
    of many symbols take minutes on a 5-requests-a-minute plan; the data is small enough to
    read whole ([section 8](08-namespace-tree.md#8-namespace-tree)).
90. **A row that lists several tickers is listed under each named one, and an unlisted crypto
    pair follows `unlisted_symbols` in every REST verb** (owner decision 2026-11-02, option
    (a)). A row returned for two symbols "is the data listed by symbol", not a duplicate. The
    ten datasets whose rows list their tickers (the three financial statements, Forms 3 and
    4, insider activity and holdings, the 8-K disclosures, `reference.news`,
    `partners.benzinga.news`) gain a `Symbol` column: each ticker the request named that the
    row lists, one row per such ticker, so an article about AAPL and MSFT read for both is
    listed under AAPL and under MSFT. A request that named one ticker gives it to every row;
    a row listing none of the named tickers keeps a null `Symbol`, with one WARNING per
    dataset, and is never dropped; `symbols=None` gives a null `Symbol`. Requests stay
    batched, with no extra request. `limit` caps the rows Massive returns before the split,
    so the split can give more table rows than `limit`. A static `fetch()` and a followed one
    list rows the same way, and nothing merges copies by `Id`. Each dataset's `poll()` keys
    add `Symbol` (news: `Id` and `Symbol`). In a followed read, `limit` is sent as on any
    read, once on a per-symbol request and once on a batch. Rule 24 covers an unlisted
    crypto pair of a symbol table in `fetch()` and `poll()` as it does in streams: `LOG` and
    `IGNORE` leave it out, and `RAISE` fails; before, both verbs always failed
    ([section 5](05-massiveconfig.md#5-massiveconfig)).
91. **`last_by()` on a live symbol table shows a row by update cycle, not by receive time**
    (owner decision, 2026-11-02). A symbol's row shows when it reached the table in or after
    the update cycle in which the symbol last joined the symbol table; it was "received after
    the symbol was added", which compared the library's `TimestampInsert` with the engine's
    `now()` and hid a symbol's first row until its next tick, for hours on a quiet option
    contract. A symbol removed and added again still shows no row until its next tick
    ([section 7](07-query-classes-and-verbs.md#7-query-classes-and-verbs)).
92. **The library never uses input tables** (owner decision, 2026-11-02). An input table is
    for a person editing data by hand, such as a watchlist typed in the web UI, not for code
    and not for volume. The diagnostics tables, the library's last keyed input tables, are
    now a `blink_publisher` blink table each with `last_by` on its keys: the same columns,
    keys, and rows, and the same memory, one row per key; nobody can edit them in the web UI.
    Every blink table is created under the UG shared lock, because a table registered while a
    cycle runs can be refreshed in the step its creation stamp claims, and a first change in
    that step is fatal ([section 14](14-publishers-fan-out-and-failure-semantics.md#14-publishers-fan-out-and-failure-semantics)). Tests may
    still use an input table to stand in for a user's watchlist.
93. **`dm.canonicalize_symbols` skips and logs a bad row by default** (owner decision,
    2026-11-03, applying the rule that one bad symbol or row never fails the table). Its
    `on_error` default changes from `RAISE` to `LOG`: a value that does not convert gets a null
    symbol, which every dataset ignores, and is logged once at WARNING, at most 20 per table,
    and the converted table and every table that follows it keep updating. `RAISE` stays
    available and keeps its behavior: a current bad value raises `ValueError` at the call, and
    a later bad row of a refreshing table fails the table and every result that follows it.
    The refusal of a bad symbol-table row now suggests `dm.canonicalize_symbols(table,
    dm.AssetClass.<CLASS>)` without an `on_error` argument.
