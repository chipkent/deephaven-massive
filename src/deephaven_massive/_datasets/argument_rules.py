"""The rules of the arguments many datasets share, which the dataset index prints.

Many dataset methods take the same arguments, such as `symbols`, `start`, `end`, `sort`, and
`filters`, and each follows one rule whichever dataset takes it. This module holds that rule
once, as user-facing Markdown, so that nothing repeats it: `column_reference.render` prints
every rule once, under its argument's name and anchor, on the index of the dataset reference,
and each dataset page's Arguments table links the rule of each such argument beside the
dataset's own line from its method's docstring. A dataset docstring gives each argument one
line about what is particular to that dataset and links its page.

Rules about a public type, such as `dm.Order`, `dm.Timespan`, or `dm.OnError`, live in that
type's own docstring; a rule here names the type and says what the argument does with it.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Final

__all__ = ["ARGUMENT_RULES"]

ARGUMENT_RULES: Final = MappingProxyType(
    {
        "symbols": (
            "Each symbol is Massive's ticker, spelled exactly:\n"
            "\n"
            "| Asset class | Ticker |\n"
            "| --- | --- |\n"
            "| Stocks | `AAPL`, `BRK.B` |\n"
            "| Options | `O:SPY271217C00700000`: `O:` and the OCC option symbol |\n"
            "| Indices | `I:SPX` |\n"
            "| Forex | `C:EURUSD` |\n"
            "| Crypto | `X:BTCUSD` |\n"
            "| Futures | `ESZ6` |\n"
            "\n"
            "- Pass one symbol as text, several as a list, or a table with a `Symbol` column.\n"
            "- A live table of symbols works as a watchlist. Each symbol you add is read, and "
            "each one you delete stops being read. `fetch()`, `history()`, `last_by()` keyed "
            "by `Symbol`, and a `poll()` in `dm.PollMode.REPLACE` mode also drop a deleted "
            "symbol's rows. `APPEND` and `UPSERT` polls keep them.\n"
            "- `dm.ALL` reads every symbol. Leaving `symbols` out reads every record with no "
            "filter by symbol. Each dataset's page says which of the two it takes.\n"
            "- Text or a list with a symbol that isn't Massive's exact ticker, such as `aapl`, "
            "raises `ValueError` at the call. Such a row of a table is skipped and logged by "
            "default, and the table keeps running. `MassiveConfig.noncanonical_symbols` changes "
            "that. A table with no valid row is refused at the call.\n"
            "- `dm.canonical_symbol` converts other spellings to Massive's: "
            '`dm.canonical_symbol("BRK-B", dm.AssetClass.STOCKS, '
            'convention=dm.SymbolConvention.YAHOO)` returns `"BRK.B"`.\n'
            "- A record that lists several symbols, such as a news article, appears once for "
            "each symbol you named that it lists."
        ),
        "start": (
            "The first day or moment of the range. Give `start` and `end` as two dates or two "
            "instants.\n"
            "\n"
            '- A date is text such as `"2025-06-02"`, a Python `date`, or a Deephaven '
            "`LocalDate`. It means the day in the market's time zone: New York for stocks, "
            "options, and indices; UTC for forex and crypto; for futures, the trading session "
            "that ends that day.\n"
            "- An instant is a Python `datetime` or `pandas.Timestamp` with a time zone, a "
            "`numpy.datetime64` finer than a day, a Deephaven `Instant` or `ZonedDateTime`, or "
            'a Deephaven time string such as `"2025-06-02T09:30 ET"`. A `datetime` or '
            "`pandas.Timestamp` without a time zone raises `ValueError`.\n"
            "- A dataset whose range is a calendar date, such as an ex-dividend date, takes "
            "dates only."
        ),
        "end": (
            "The last day, or the moment the range stops, in the same form as `start`.\n"
            "\n"
            '- A date includes its whole day: `start="2025-06-02", end="2025-06-02"` reads all '
            "of June 2. An instant `end` is excluded from the range.\n"
            "- Leave it out to include records still to come, such as dividends announced but "
            "not yet paid. A query's `history()` without "
            "`end` reads through now."
        ),
        "date": (
            "One calendar day, in the same forms as a `start` date, and meaning the same day. A "
            "`datetime` or an instant raises `ValueError` at the call."
        ),
        "as_of": (
            "The day to read the data as it stood on, in the same forms as a `start` date. Leave "
            "it out for today's data. The dataset's plan table says how far back each plan goes."
        ),
        "sort": (
            "The field to order the rows by, as Massive names it, such as "
            '`"ex_dividend_date"`. Each dataset\'s page lists the fields it takes. Where '
            "Massive allows, name several, separated by commas, each with an optional `.asc` "
            'or `.desc`: `"ticker,ex_dividend_date.desc"`.'
        ),
        "order": (
            "The direction of `sort`: `dm.Order.ASC` or `dm.Order.DESC`. Give it with `sort`. On "
            "some datasets, `order` alone raises "
            "`ValueError`."
        ),
        "limit": (
            "The most rows Massive returns for the whole request, across every symbol you "
            "name, in Massive's order.\n"
            "\n"
            "- To keep the latest rows, sort newest first, such as "
            '`sort="ex_dividend_date", order=dm.Order.DESC`.\n'
            "- A symbol that comes first can use up the whole limit, so leave `limit` out when "
            "you need every row of each symbol.\n"
            "- A `fetch()` that follows a table of symbols applies the limit to each request it "
            "sends. A request carries one symbol, or a group of symbols on a dataset that takes "
            "several at once. A dataset's page notes any other case where it applies to each "
            "symbol."
        ),
        "filters": (
            "Any other parameter Massive documents for the dataset, as a `dict` from the "
            "parameter's name to its value. Write each dot in a name as `_`: Massive's "
            "`strike_price.gte` is `strike_price_gte`.\n"
            "\n"
            "- Each dataset's page lists the keys it takes. Any other key raises `ValueError` "
            "listing them. A key that an argument sets, such as `ex_dividend_date_gte` for "
            "`start`, raises `ValueError` naming that argument.\n"
            "- A date or time value is ISO 8601 text, with an offset for a time, or a date or "
            "time object that `start` takes. A Deephaven time string isn't read here."
        ),
        "adjusted": (
            "`True`, the default, adjusts prices and volumes for splits: a value from before a "
            "split is restated in the split's terms. `False` gives values as traded. Only REST "
            "adjusts, so bars from flat files and live bars are always as traded."
        ),
        "timespan": (
            "The unit of each bar, a `dm.Timespan` member: `SECOND`, `MINUTE`, `HOUR`, `DAY`, "
            "`WEEK`, `MONTH`, `QUARTER`, or `YEAR`. Futures take `SESSION` in place of `DAY`.\n"
            "\n"
            '- Text such as `"day"` raises `TypeError`. Write `dm.Timespan.DAY`, or convert '
            'text with `dm.Timespan("day")`.\n'
            "- One-second and one-minute bars with `multiplier=1` also stream. Bars of every "
            "other length are history only."
        ),
        "multiplier": (
            "How many `timespan` units each bar spans, 1 by default. "
            "`timespan=dm.Timespan.MINUTE, multiplier=5` gives five-minute bars."
        ),
        "venues": (
            "The exchanges whose futures flat files `history()` reads, as a set of "
            "`dm.FuturesVenue` members: `CME`, `CBOT`, `NYMEX`, and `COMEX`. The default is all "
            "four. Massive sells futures flat files by exchange, so pass the ones your plan "
            "includes. Streams and REST reads ignore it."
        ),
    }
)
"""The shared rule of each argument, by its name in the dataset methods' signatures.

Each value is Markdown for the index to print under the argument's heading: paragraphs, then
any bullet list or table, separated by blank lines, with no heading of its own and no trailing
whitespace. An argument missing here, such as `underlying`, has only its dataset's own line.
"""
