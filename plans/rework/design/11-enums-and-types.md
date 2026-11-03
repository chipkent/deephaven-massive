## 11. Enums and types

**Python floor:** `requires-python = ">=3.11"`. `StrEnum`, `datetime.UTC`, `typing.Self`,
`tomllib`, and exception groups are native. The CI matrix is 3.11, 3.12, 3.13, 3.14, and 3.14t;
the free-threaded 3.14t job runs with `PYTHON_GIL=0` and is required. 3.14t is the only
free-threaded target: pyarrow, numpy, and pandas no longer publish cp313t wheels.

**Criterion.** Every closed set of values in a public signature is a `StrEnum` whose values are
Massive's wire strings; an enum of a library setting that never reaches the wire
(`HistorySource`, `Reread`, `SymbolConvention`, `OnError`, `ErrorCause`, and `PollMode.AUTO`)
takes its member names in lower case. Signatures take only the enum. There are no string `Literal`
categoricals and no `Enum | Literal` unions. Open sets (`sort`, `form_type`) are
`str`. The one spelling per value gives tab completion in every IDE, including the Deephaven web
console, and typo safety at both type-check time and run time.

**Runtime.** Passing a `str` raises `TypeError` naming the expected member
([section 7.3](07-query-classes-and-verbs.md#73-runtime-enum-check)).

**Enforcement.** `tests/test_enums.py` asserts that no public signature in the package contains a
`Literal` of strings, and that every enum that mirrors a parameter enum of `spec/rest.json`
has the same value set as that parameter.

| Enum | Members | Wire values | Used by |
|---|---|---|---|
| `AssetClass` | STOCKS, OPTIONS, FUTURES, INDICES, FOREX, CRYPTO | `stocks`, `options`, `futures`, `indices`, `forex`, `crypto` (the cluster paths; `FOREX` is sent as `fx` on REST reference endpoints) | `ws_hosts`, diagnostics, `reference.ticker_types`, `reference.exchanges`, `reference.conditions` |
| `Timespan` | SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR, SESSION | `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`, `session` | `bars`, `indicators`; selects channel, REST path or resolution, flat-file prefix, and overload |
| `PollMode` | AUTO, REPLACE, UPSERT, APPEND | `auto`, `replace`, `upsert`, `append` | `poll()`; `AUTO`, the default, is the dataset's own mode |
| `HistorySource` | AUTO, REST, FLAT_FILES | `auto`, `rest`, `flat_files` | `history()` |
| `Reread` | ALWAYS, NEVER | `always`, `never` (no wire value) | `history()` (`reread`): whether a symbol that returns to a followed symbol table is downloaded again ([section 7.8](07-query-classes-and-verbs.md#78-following-a-live-symbol-table)) |
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
| `OnError` | RAISE, LOG, IGNORE | `raise`, `log`, `ignore` | `canonicalize_symbols` (`on_error`), `MassiveConfig.noncanonical_symbols`, `unlisted_symbols`, `unreadable_symbols`, and `unavailable_symbols` |
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
([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)).

**ISO-8601 text.** A timestamp the wire carries as ISO-8601 text (for example `published_utc`)
becomes an `Instant`. It must carry an offset (`Z` or `±HH:MM`): text without one is an
unconvertible value ([section 19.5](19-wire-clients-and-the-spec-snapshot.md#195-runtime-drift)), never read as UTC, and a field that
really is offset-less UTC needs a named `TYPE_FIXES` conversion. The generator's
representation rules ([section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)) decide which fields are ISO
`Instant`s and which are dates; any other text timestamp needs a `TYPE_FIXES` entry.

**Column types.** Every column's type comes from the type lattice of
[section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas): each generated field has a `WireType`, the merge joins
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
