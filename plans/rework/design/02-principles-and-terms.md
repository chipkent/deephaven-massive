## 2. Principles and terms

### 2.1 Principles

1. **Datasets are named by what they are, not how they travel.** One method per dataset. One
   column order and one type per column for each dataset kind, merged at run time from the
   generated tables ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)): the union of the fields Massive
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
   [section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)). Every calendar date in every table
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
   ([section 17.1](17-diagnostics.md#171-the-log)): ERROR only for a defect, naming whose it is.
8. **Direct over Massive's documented APIs, in one package.** The library owns its REST,
   WebSocket, and S3 flat-file clients. They live with the vendored spec snapshot, the
   generator, and the generated schemas in the private package `_massive`, split into six
   subpackages (`net/`, `spec/`, `rest/`, `ws/`, `flatfiles/`, `refresh/`). `_massive` imports
   nothing from Deephaven or the rest of the library and is reached only through its interface
   ([section 19](19-wire-clients-and-the-spec-snapshot.md#19-wire-clients-and-the-spec-snapshot)). A parity test keeps the generated
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
    covers only the cases [section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas) defines (the key columns
    `Symbol`, `Timestamp`, and `Underlying`, the library's time columns `Date`,
    `TimestampStart`, `TimestampEnd`, and `TimestampInsert`, the derived `OptionRoot`, and a
    dataset's note on a column whose meaning depends on the query,
    such as the split adjustment of a bar's prices). The same text reaches users three ways: the column-header tooltip of every result
    in the Deephaven web UI, `columns()` in the console, and the generated pages under
    `docs/datasets/` ([section 7.4](07-query-classes-and-verbs.md#74-column-descriptions)). Nobody writes a column list by hand.

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
| column description | The plain-text meaning of one dataset column: the description of the wire field behind it (the vendor's, or the refresh's correction where the vendor's is clearly wrong), or fixed library text for a key column or a column the library computes ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)). |
| dataset reference | The generated, committed pages under `docs/datasets/`: an index, `README.md`, and one page per dataset, `<dataset>.md` (`stocks.trades.md`, `snapshots.md`), with its call, verbs, filters, Massive's plan tables, and columns ([section 7.4](07-query-classes-and-verbs.md#74-column-descriptions)). |
| UG thread | A thread of Deephaven's update graph (UG), which refreshes live tables while holding the UG lock. |
| blink table | A Deephaven table whose rows exist for exactly one update cycle. `TablePublisher` produces one. |
| result scope | A library-owned `LivenessScope` that holds exactly one result. |
| stream scope | A library-owned `LivenessScope` that holds one stream's blink table and publisher. |
| registry lock | `SessionCore.registry_lock`, the one lock that guards every session-level registry. |
