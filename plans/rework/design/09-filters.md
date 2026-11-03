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
[section 19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)), which carry each
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
starts at 00:00 in the request's day zone ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries))
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
([section 8](08-namespace-tree.md#8-namespace-tree)).

`sort` is checked at construction, like a filter, against every REST path of the dataset that
takes it (`Catalog.check_sort`): where the spec lists the path's sort fields (an `ENUM`
`sort`, such as `ex_dividend_date` and `pay_date` on `/v3/reference/dividends`), `sort` must be
one of them; where the spec documents `sort` as a comma-separated list of columns, each with an
optional `.asc` or `.desc` suffix (a `STRING` `sort`, such as `period_end.desc` on the
financial statements), each part must name a response field of the path (a generated
`Field.wire_key`) or a filter of the path (the base name of a query parameter other than
`limit`, `sort`, and `order`) that names the last part of such a field's key, which every
default the spec gives such a `sort` does (the
futures snapshot's default `ticker` names its filter, while its rows nest the ticker as
`details.ticker`); a part with
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
construction ([section 8](08-namespace-tree.md#8-namespace-tree)). Whether an
axis is an instant axis follows from the generated row field its parameter filters (the field
whose wire key is the parameter's base name), not from the parameter's kind: Massive types the
`published_utc` and benzinga `published` filters as dates, but their fields are ISO-8601
instants, so they are instant axes, and a date bound on them is sent as the instant of
[section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries), never as a bare date. Bars whose
range path has `{from}` and `{to}` segments declare no `time_axis`: `history()` fills those
segments instead ([section 13.4](13-host-resolution-and-connection-protocol.md#134-rest)). `futures.bars`, whose path
`/futures/v1/aggs/{ticker}` has no such segments, declares the instant pair
`window_start.gte`/`window_start.lt`. Passing any form of the same axis
through `filters` (as `timestamp_gte`, `timestamp_lte`, or `timestamp`, for example) raises
`ValueError`.
