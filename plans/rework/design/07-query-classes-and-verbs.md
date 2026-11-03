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
                                           # static, or with wait=False read in the background;
                                           # follows a refreshing symbol table (section 7.8)
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
                reread: Reread = Reread.ALWAYS,
                wait: bool = True) -> Table: ...   # static, or read in the background;
                                           # follows a refreshing symbol table (section 7.8)

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
row behind: [section 23](23-open-verification-items.md#23-open-verification-items)); `PollMode.REPLACE` otherwise, the two
filing taxonomies included (a taxonomy is a reference list revised in place, not a feed of new
rows).

**The interval is at least 1 second.** `poll(interval)` takes seconds as a number or a
`timedelta`, and refuses one shorter than 1 second with a `ValueError` at the call that names
the minimum and the value given: `interval must be at least 1 second, got 0.01: each poll()
cycle sends a request; use a stream for faster updates`. A shorter interval is almost always a
typo, such as `poll(0.01)` for `poll(10)`, and on a plan Massive does not pace it would send
requests back to back. A stream serves anything faster
([section 22](22-decisions.md#22-decisions), decision 94).

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
`latest_rows_since_added(blink, symbol_table, keys)`, so rows of removed symbols are evicted
from the result. The underlying `last_by` state keeps every symbol ever seen, so the filter also
hides a row that reached the table before the update cycle in which its symbol last joined the
symbol table: a symbol that is removed and re-added shows no row until its next tick, while a
symbol's first row shows as soon as it reaches the table, in or after the cycle in which the
symbol joined (decision 91, owner decision, 2026-11-02). `latest_rows_since_added`
(`_tables/watchlist.py`) compares update-cycle numbers, the logical clock's step
(`ExecutionContext.getContext().getUpdateGraph().clock().currentStep()` in a formula), never
wall-clock times. Under one hold of the update graph's shared lock it creates the symbol
table's distinct symbols stamped with the step (`select_distinct("Symbol").update(...)`, first)
and the blink table stamped with the step before the aggregation
(`blink.update(...).last_by(keys)`), then joins the two and keeps rows whose row step is at
least their symbol's added step, dropping both hidden columns. The watchlist side comes first,
so a symbol's added step is never later than the step of a row the aggregation starts from,
even if a cycle ran between the two creations; the lock makes the two equal. The stamp precedes
the aggregation because a formula that reads no column is not recomputed when a row is
modified, and both sides use `update`, never `update_view`, so the step is the cycle in which
the row changed, not the one in which it is read. With keys that span symbols (for
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
bar never appears. The full rules are in [section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries).
There is no `limit`: the library pages internally. (The aggregates endpoint's `limit` counts
base aggregates, not rows, so it cannot serve as a row cap.) A static `Table` passed as
`symbols` is read once, when the verb is called, on the caller thread inside the session's
execution context: its rows become the sorted symbol tuple that every source receives (`REST`,
`FLAT_FILES`, and the `AUTO` choice). A row that is not Massive's exact ticker raises
`ValueError`, and so does a table with no symbol, as an empty symbol list does. A refreshing
`Table` is followed instead, and the result is refreshing
([section 7.8](#78-following-a-live-symbol-table)); `reread` applies only then.

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
the whole result is read and returns a static table, as before, except for a followed symbol
table ([section 7.8](#78-following-a-live-symbol-table)), whose result is always refreshing. `wait=False` returns at once a
`Table` with the same columns and no rows, refreshing. When the whole download has landed, the
complete result appears in one update and never changes again; it is never half loaded, and
the return type is the same as `wait=True`, with no `Future`, since a `Future` would need a
later `result()` that cannot wait inside a console command. The read runs on a library thread
of its own, so it holds no lock: inside a console command the command returns at once and
every live table keeps updating while the read runs.

- **Arguments raise at the call.** Everything `wait=True` checks before its first request is
  checked on the caller thread before the table exists: types and ranges, `dm.ALL` where it is
  not allowed, a symbol table's current rows, the source choice of `history()`, every
  REST path's parameters (`RestTransport.check_fetch` and `check_history` plan the requests
  with no request made; a crypto pair is split later, by the read), a closed session, and the
  update-graph thread. A failure during the download fails the table instead
  ([section 6.1](06-massivesession-and-lifecycle.md#61-how-a-live-table-is-kept-alive-and-released)). A `history()` range no
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
  so a paced Basic download of hours completes ([section 13.4](13-host-resolution-and-connection-protocol.md#134-rest)).
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
  ([section 16](16-historysourceauto.md#16-historysourceauto)).
- A static symbol list that would exceed the options cap ([section 15](15-options-contract-cap.md#15-options-contract-cap)).
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
[section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas) from the field descriptions in `spec/descriptions.json`
(the vendor's, corrected where they are clearly wrong). It reaches users through three outlets,
all generated from the same dataset schema, so they can never disagree. All three spell the
transports one way, `REST`, `flat files`, and `WebSocket` (`column_descriptions.TRANSPORT_LABELS`): the
key-column descriptions of [section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas) rule 3, `columns()`'s
`SuppliedBy`, and the column reference's **Supplied by**, so a filter copied from a page
(``cols.where("SuppliedBy = `WebSocket`")``) works on `columns()`. The `Transport` enum values
(`REST`, `FLAT_FILES`, `WS`) stay internal names, which the diagnostics tables of
[section 17](17-diagnostics.md#17-diagnostics) show.

**1. Every result is described.** Deephaven 42.7 stores column descriptions as the table
attribute `ColumnDescriptions` and a table description as `TableDescription`, set through
`io.deephaven.engine.table.GridAttributes.withColumnDescriptions(Map<String, String>)` and
`withDescription(String)`. Barrage sends the column descriptions to clients, and the web UI
shows a column's description in the column-header tooltip; whether it shows the table
description anywhere is **UNVERIFIED** ([section 23](23-open-verification-items.md#23-open-verification-items)), so it is
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
[section 16](16-historysourceauto.md#16-historysourceauto)). The summary is
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
  ([section 6.1](06-massivesession-and-lifecycle.md#61-how-a-live-table-is-kept-alive-and-released)), never under
  `registry_lock` and never in a listener. Copying a table can briefly take the update-graph
  shared lock (its initial snapshot falls back to the lock after repeated concurrent
  attempts), like any table operation, which is one more reason it never runs under
  `registry_lock`. The described table depends on the table it copies, so the
  undescribed parent, and for a stream the blink table that owns `on_shutdown_callback`, lives
  exactly as long as the result does; a teardown test proves that releasing the result still
  shuts the stream down. A `poll()` poller writes only through the publisher it holds, never
  through the returned copy.
- The diagnostics tables of [section 17](17-diagnostics.md#17-diagnostics) are not dataset results and are not
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
distinct, from the `Provenance` passed to the renderer ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas));
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
  symbol with no data (HTTP 404) follows `MassiveConfig.unavailable_symbols`, as on every
  per-symbol path: left out with a WARNING by default, or a failure under `RAISE`.
- **The kind's columns.** Each one-row dataset is a member of its kind
  ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)), and its rows have the columns of its kind that its
  endpoint fills, under the kind's names and types, so a column it shares with
  `m.stocks.trades(syms).history(...)` means the same thing in both. A kind's group may be
  split over several datasets of its class: each source of the group is read by exactly one
  of them, and the merged schema walks the group's sources in their positions, whichever
  dataset reads them, so the columns are in the same order as before the split.
- **Bars.** `previous_day_bar` rows are 1-day bars with the day rules of
  [section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries): `TimestampStart` at 00:00 of the day in
  the class day zone, whatever `/prev` stamps, `TimestampEnd` 00:00 of the next day (01:00 New
  York time for indices); a stamp at a time of day Massive is not known to use is served as
  sent, with `TimestampEnd` a day later and a null `Date`, and logs one WARNING per form of
  stamp per session (decision 85). Stock and
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

### 7.8 Following a live symbol table

Every verb follows a refreshing symbol table, `fetch()` and `history()` included: a stream
subscribes and unsubscribes, `poll()` re-reads the table each cycle, and `fetch()` and
`history()` read each added symbol and drop each removed one. History and live data are never
mixed: a followed `history()` reads only its fixed past range, and no verb backfills a stream
(§ 22, decisions 82 and 83).

**Scope.** Only a refreshing `Table` passed as `symbols` (or as `underlying=` or
`product_code=`, below) is followed. A string, a list, `dm.ALL`, `None`, and a static table
behave exactly as [section 7](#7-query-classes-and-verbs) describes. `history(wl.snapshot())`
reads a live table once.

```python
wl = input_table({"Symbol": dht.string}, key_cols="Symbol")      # holds AAPL and MSFT
bars = m.stocks.bars(wl, dm.Timespan.MINUTE).history("2026-10-01")
wl.add(new_table([string_col("Symbol", ["NVDA"])]))     # NVDA's bars of the same range land later
wl.delete(new_table([string_col("Symbol", ["MSFT"])]))  # MSFT's rows leave in the same cycle
```

**The result** is always a refreshing table, with its own liveness scope, result record, and
`m.streams()` row (`Kind=read`), for both values of `wait`. It holds, for each canonical symbol
now in the table, the complete rows of that symbol's latest completed read, or none while the
symbol's first read is loading or after it failed. A symbol's rows land whole, in one update
cycle: never half a symbol. One symbol is never read twice at once; an outcome of a read that a
later removal or re-add has overtaken is discarded. Null rows are ignored, a symbol in several
rows counts once, and rows that are not Massive's tickers follow
`MassiveConfig.noncanonical_symbols` as they do for streams
([section 10](10-symbols-and-all.md#10-symbols-and-all)). The mechanism is in
[section 6.1](06-massivesession-and-lifecycle.md#61-how-a-live-table-is-kept-alive-and-released).

**The range is fixed at the call.** `start`, `end`, and the call instant are fixed when the
verb is called, and every read, of a call-time symbol or of one added hours later, covers the
same window. `end=None` means through the call instant: the "bar has ended" cutoff and "today"
are the call's ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)). The table description
prints the absolute end (`Follows its symbol table over 2026-10-01 through
2026-11-02T14:05:12Z.`), so the window is never read as "now". `fetch()` has no window: its
`start`, `end`, and `as_of` are sent as built, and each read is Massive's answer as of that
read (a snapshot of a symbol added at 14:00 is the 14:00 snapshot).

**Adds and removes.**

- **Add:** the symbol is queued; its rows land when its read completes, over the same window.
- **Remove:** its rows leave in the same update cycle as its row leaves the user's table.
- **Re-add:** `history()` follows `reread` (below); `fetch()` always reads again.

**`reread`** (`history()` only; `dm.Reread`, default `ALWAYS`) says whether a symbol that
returns to the table after a removal is downloaded again or gets its latest completed download
back with no request. It has no effect when `symbols` is not followed, and `fetch()` has no
such option: it always reads again.

| Member | A returning symbol |
|---|---|
| `ALWAYS` (default) | Is downloaded again and shows Massive's current answer. A removal drops its rows in the same cycle; a re-added symbol has no rows until its new download lands. |
| `NEVER` (opt-in) | Gets its earlier download back with no request; the download is kept for the life of the result. |

A symbol whose read `FAILED` is downloaded again on every re-add, under both members. Each
symbol's rows are Massive's answer as of its `ReadAt` in `m.symbol_reads()`.

**Costs.** Under `ALWAYS`, a table whose symbols churn re-reads a symbol every time it returns.
Under `NEVER`, the result keeps the rows of every symbol it has ever held, in memory, and a
returning symbol shows data as old as its first download.

**Why there is no automatic choice.** No date or time rule can tell that Massive's data for a
range is final, so the library never guesses that a reuse is safe (research of 2026-11-02):

- Cancellations reach REST days after the session (Massive's knowledge base, "How much does
  Massive's feeds handle canceled trades").
- Published flat files are rewritten (knowledge base, "What time are Massive's flat files ready
  each day").
- Massive backfills history (changelog entries of 2025-11-03, 2026-07-31, and 2026-10-16).
- Splits are applied overnight, at a time Massive does not publish
  (`vendor/massive/rest/stocks/corporate-actions/splits.md`, `execution_date`).
- FINRA allows trades to be reported late, with no limit.

A reuse that is provably safe, for a flat-file download whose every file still shows the same
ETag or Last-Modified in a fresh listing, is a separate follow-up (issue #454).

**The source under `AUTO`.** For a followed table, `HistorySource.AUTO` reads REST whenever
REST can serve the request, and flat files otherwise ([section 16](16-historysourceauto.md#16-historysourceauto)).
The symbol count is ignored, so adding one symbol never re-reads a year of flat files, and the
source is the same for every symbol whenever it is read. `source=` wins: `FLAT_FILES` reads
every symbol from flat files. Symbols waiting together for a flat-file read are read in one
scan, which downloads the same files whatever the number of symbols. The description names
the source (`Read from REST: AUTO reads a followed symbol table from REST, which can serve
it.`).

**`fetch()` request shapes.**

- **Per-symbol paths** (one symbol in the path): one read per symbol.
- **`ticker.any_of` and `tickers` batches:** the waiting symbols are batched as today, and each
  row is assigned to its symbol by its `Symbol` column, which the filter matches exactly.
- **List-column datasets** (rows with a `Tickers` list: the financial statements, Forms 3 and
  4, insider activity and holdings, the 8-K disclosures, `reference.news`,
  `partners.benzinga.news`): requested as a static `fetch()` requests them, and each row is
  listed under each named ticker it lists, with that ticker in `Symbol`, as a static
  `fetch()` lists it ([section 10](10-symbols-and-all.md#10-symbols-and-all), decision 90). Nothing removes a
  copy across requests. A row that lists none of a batch's named tickers keeps its null
  `Symbol`, as in a static `fetch()`, and goes with the batch's first symbol: it leaves the
  result when that symbol does.

**`limit`** is sent on each request of a followed read as Massive's own cap on that request's
rows: once on a per-symbol request, and once on a `ticker.any_of` batch (decision 90).

**One-value inputs** follow tables too: `options.chain(underlying=)` and
`options.contracts(underlying=)` take a table with a `Symbol` column, and every futures
`product_code=` a table with a `ProductCode` column (as `futures.products` rows carry it). A
refreshing one is followed with the same machinery, keyed by that value; `poll()` of such a
query re-reads it each cycle, as for `symbols`.

- **Each value is read as if passed alone**, with the requests that one value's call makes:
  one request series per value, `limit` sent on each, and an index underlying sent bare to
  the contracts list as `underlying="I:SPX"` is. Values are never batched into one request,
  so every row a value's requests return goes with that value, whatever its `Symbol`. The `m.symbol_reads()` rows of a followed `fetch()` are keyed by the value.
- **A static table** is a fixed list, read once: the result joins each value's rows, and a
  value that fails fails the call, as a failed request of a list call does.
- **A value the dataset does not take** follows `noncanonical_symbols`, as a symbol table's
  row does: an underlying must be Massive's exact stock or index ticker; a product code is
  kept as written (no surrounding whitespace, no class prefix), since Massive publishes no
  grammar for product codes.
- **`poll()`** reads the table's values each cycle and each value as if passed alone; a value
  Massive answers with HTTP 404 follows `unavailable_symbols`, as a symbol does.
- **One followed table a result.** A `fetch()` whose `symbols` and `product_code=` are both
  refreshing tables raises `ValueError` at the call; a static `symbols` table beside a
  followed one is read once.

**`wait`.**

- **`wait=True`** blocks while the call-time symbols get their first attempt, read as one call
  of them, and returns a refreshing table that already holds their rows. Symbols added later,
  and retries, load in the background. It raises only when every call-time symbol fails, with
  the error a list call would raise; a partial failure logs one WARNING naming the failed
  symbols and their causes, and they show `FAILED` or `RETRYING` in `m.symbol_reads()`.
- **`wait=False`** returns at once an empty refreshing table; every symbol, the call-time ones
  included, loads in the background and lands on its own.
- **Both** raise at the call for bad arguments, the update-graph thread, a closed session, a
  failed symbol table, and a bad row under `noncanonical_symbols=RAISE`.

**Errors of one symbol** leave the table following:

| Cause | The symbol |
|---|---|
| HTTP 404 on the first request (Massive has no data) | `DONE` with 0 rows, and the existing WARNING (at DEBUG under `unavailable_symbols=IGNORE`); `FAILED`, permanent, under `unavailable_symbols=RAISE` |
| Transient: a rate limit beyond the read's patience, a network failure, or a 5xx after the REST client's own retries | `RETRYING`, then read again 1, 5, and 15 minutes later; `FAILED` after the last |
| Permanent: `EntitlementError`, any other refusal, a later page's 404, unreadable data (`RowDataError`), a crypto pair Massive's list does not hold (logged as `MassiveConfig.unlisted_symbols` says; under `RAISE` it fails the table) | `FAILED` at once |

A read of several symbols (an `any_of` batch, a flat-file scan) fails as a unit: every symbol
of it gets the same state and error. Deleting and re-adding a `FAILED` symbol's row reads it
again.

**Errors of the whole table** fail the result through its publisher's `publish_failure`: `AuthenticationError` (a
401, or S3's verdict that the keys are wrong) on any read; a closed session; the symbol table
failing upstream; a bad row added under `noncanonical_symbols=RAISE`; and a library bug, a
`MassiveError` naming it.

**Diagnostics.** The result's `m.streams()` row is `LOADING` while any current symbol is
queued, loading, or waiting to retry, and `RUNNING` once every current symbol has landed or
failed; it keeps following. `FAILED`, `RELEASED`, and `CLOSED` mean what they mean for a stream.
`SymbolCount` and `Symbols` are the table's current symbols, `RowsPublished` the rows now in
the result, `Transport` the sources used, and the new `Loading` column counts the symbols
queued, loading, or waiting to retry. `Error` groups the failed symbols by cause, bounded like
every `Error`. `m.symbol_reads()` has one row per symbol ([section 17](17-diagnostics.md#17-diagnostics)). The
log has the call's `call 7 start` line, one DEBUG line per symbol read (`call 7 AAPL: 1234
rows in 0.8 s from REST`), and one WARNING per result and cause of failure.

**Cost.** Each added symbol costs what `history()` or `fetch()` of that one symbol costs, and
each re-download pays again; the shared rate governor paces them, and on Basic (5 requests a
minute) a year of minute bars is about 5 requests per symbol. A large refreshing result also
costs downstream: every table derived from it updates when a symbol lands or leaves, and a
refreshing `sort` of tens of millions of rows takes seconds per change. `snapshot()` of the
result gives a static copy to analyze.

**The `dm.ALL` recipe.** Where a dataset serves `dm.ALL` (daily bars, reference lists, one day
of minute bars), reading every symbol once and filtering by the live table follows it at engine
speed, with no request per symbol: `day = m.stocks.bars(dm.ALL, dm.Timespan.DAY).history(...)`,
a data index on `Symbol` (`deephaven.experimental.data_index.data_index(day, ["Symbol"])`),
then `day.where_in(wl, "Symbol")`. It is a documented recipe, never the library's automatic
behaviour, which would turn a three-symbol read into a read of the whole market. It does not
scale to long ranges of minute bars, trades, or quotes, whose whole-market rows do not fit in
memory.
