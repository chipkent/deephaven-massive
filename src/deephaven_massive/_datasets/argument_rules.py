"""The rules of the arguments many datasets share, which the dataset reference prints.

Many dataset methods take the same arguments, such as `symbols`, `start`, `end`, `sort`, and
`filters`, and each follows one rule whichever dataset takes it. This module holds that rule
once, as plain user-facing text, so that nothing repeats it: `column_reference.render` writes it
into the Arguments table of every dataset page that takes the argument, beside the dataset's
own line from its method's docstring. A dataset docstring gives each argument one line about
what is particular to that dataset and links its page, where the reader finds the full rule.

Rules about a public type, such as `dm.Order`, `dm.Timespan`, or `dm.OnError`, live in that
type's own docstring; a rule here names the type and says what the argument does with it.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Final

__all__ = ["ARGUMENT_RULES"]

_DATE_VALUES: Final = 'text such as `"2025-06-02"`, a Python `date`, or a Deephaven `LocalDate`'
"""The values a calendar-date argument takes, as a rule says them."""

_TIME_ZONES: Final = (
    "the day in the market's time zone: New York for stocks, options, and indices; UTC for "
    "forex and crypto; for futures, the trading session that ends that day"
)
"""Which day a date names, by asset class, as a rule says it."""

ARGUMENT_RULES: Final = MappingProxyType(
    {
        "symbols": (
            "Massive's ticker for each symbol, spelled exactly as Massive spells it: `AAPL` or "
            "`BRK.B` for a stock, `O:SPY271217C00700000` (the OCC code) for an option, `I:SPX` "
            "for an index, `C:EURUSD` for a forex pair, `X:BTCUSD` for a crypto pair, and "
            "`ESZ6` for a future. Pass one symbol as text, several as a list, or a table with a "
            "`Symbol` column. A live table is followed: each symbol you add is read, and each "
            "one you delete stops being read. `last_by()`, `fetch()`, `history()`, and most "
            "polls also drop a deleted symbol's rows. `dm.ALL` reads every symbol, and "
            "leaving the argument out reads every record with no filter by symbol, where this "
            "dataset takes them. A misspelled symbol in text or a list raises `ValueError` at "
            "the call; by default, a misspelled row of a table is skipped and logged, and the "
            "table keeps running. `dm.canonical_symbol` converts other spellings to Massive's. "
            "A record that lists several symbols, such as a news article, appears once for "
            "each symbol you named that it lists."
        ),
        "start": (
            f"The first day or moment of the range. A date ({_DATE_VALUES}) means "
            f"{_TIME_ZONES}. An instant is an exact time: a Python `datetime` with a time "
            "zone, a `pandas.Timestamp`, a `numpy.datetime64`, a Deephaven `Instant` or "
            '`ZonedDateTime`, or Deephaven time text such as `"2025-06-02T09:30 ET"`. Give '
            "`start` and `end` as two dates or two instants. A `datetime` without a time zone "
            "raises `ValueError`, and a calendar-date field, such as an ex-dividend date, takes "
            "dates only."
        ),
        "end": (
            "The last day, or the moment the range stops, in the same form as `start`. A date "
            'includes its whole day, so `start="2025-06-02", end="2025-06-02"` reads all of '
            "June 2; an instant is excluded. Leave it out to read through now; a list dataset "
            "then sends Massive no end, so it also returns records still to come, such as "
            "dividends announced but not yet paid."
        ),
        "date": (
            f"One calendar day, as {_DATE_VALUES}. It means {_TIME_ZONES}. A `datetime` or an "
            "instant is refused at the call."
        ),
        "as_of": (
            f"The day to read the data as it stood on, as {_DATE_VALUES}. Leave it out for "
            "today's state. How far back you can go depends on your plan; this page's plan "
            "table says how much history each plan holds."
        ),
        "sort": (
            "The field to order the rows by, named as Massive names it, such as "
            '`"ex_dividend_date"`. Where the endpoint allows, name several, separated by '
            'commas, each with an optional `.asc` or `.desc`: `"ticker,ex_dividend_date.desc"`. '
            "This page lists the values the dataset takes."
        ),
        "order": "The direction of `sort`: `dm.Order.ASC` or `dm.Order.DESC`.",
        "limit": (
            "The most rows Massive returns for the whole request, across every symbol you name, "
            "kept in Massive's order: ask for newest first (`order=dm.Order.DESC`) to keep the "
            "latest. A symbol that comes first can use up the whole limit, so leave `limit` out "
            "when you need every row of each symbol. A `fetch()` that follows a table of "
            "symbols applies it to each symbol's request, and this dataset's own line says "
            "where else it applies to each symbol."
        ),
        "filters": (
            "Any other parameter Massive documents for this dataset, as a mapping from the "
            "parameter's name to its value, with each dot in the name written as `_`: "
            "Massive's `strike_price.gte` is `strike_price_gte`. This page lists the keys the "
            "dataset takes. A key it doesn't take, or one that has its own argument, such as "
            "`start`, raises `ValueError` naming the keys it does take. A date or time value "
            "takes ISO 8601 text, or any value `start` takes."
        ),
        "adjusted": (
            "`True`, the default, gives split-adjusted prices, restated in terms of every later "
            "stock split; `False` gives prices as traded. Only REST adjusts: flat files and "
            "live bars are always as traded."
        ),
        "timespan": (
            "The unit of each bar, a `dm.Timespan` member: `SECOND`, `MINUTE`, `HOUR`, `DAY`, "
            "`WEEK`, `MONTH`, `QUARTER`, or `YEAR`; futures take `SESSION` in place of `DAY`. "
            'Text such as `"day"` raises `TypeError`: write `dm.Timespan.DAY`, or convert text '
            'with `dm.Timespan("day")`. One-second and one-minute bars with `multiplier=1` also '
            "stream; every other length is history only."
        ),
        "multiplier": (
            "How many `timespan` units each bar spans; the default is 1. "
            "`timespan=dm.Timespan.MINUTE, multiplier=5` gives five-minute bars."
        ),
        "venues": (
            "The exchanges whose futures flat files `history()` reads, as a set of "
            "`dm.FuturesVenue` members: `CME`, `CBOT`, `NYMEX`, and `COMEX`; the default is all "
            "four. Massive sells futures flat files by exchange, so pass the ones your plan "
            "holds. Streams and REST reads ignore it."
        ),
    }
)
"""The shared rule of each argument, by its name in the dataset methods' signatures.

Each value is one Markdown table cell: plain sentences, no line break and no `|`. An argument
missing here, such as `underlying`, has only its dataset's own line.
"""
