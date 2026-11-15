"""The rules of the arguments many datasets share, which the dataset index prints.

Many dataset methods take the same arguments, such as `symbols`, `start`, `end`, `sort`, and
`filters`, and each follows one rule whichever dataset takes it. This module holds that rule
once, as user-facing Markdown, so that nothing repeats it: `column_reference.render` prints
every rule once, under its argument's name and anchor, on the index of the dataset reference,
and each dataset page's Arguments table links the rule of each such argument beside the
dataset's own line from its method's docstring. A dataset docstring gives each argument one
line about what is particular to that dataset and links its page.

A rule says what a reader needs to call the method correctly and no more. Edge cases and the
full list of accepted forms stay in the methods' and the public types' docstrings, which
`help()` shows; a rule here names a type, such as `dm.Order` or `dm.Timespan`, and says what
the argument does with it.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Final

__all__ = ["ARGUMENT_RULES"]

ARGUMENT_RULES: Final = MappingProxyType(
    {
        "symbols": (
            "Write each symbol as Massive's ticker:\n"
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
            "- A live table works as a watchlist. Symbols you add are read, and symbols you "
            "delete stop being read.\n"
            "- A dataset's page says when `dm.ALL` or leaving `symbols` out reads every symbol.\n"
            "- Each symbol is sent as you write it, in any case, and Massive decides whether it "
            "names an instrument: `KIMpL` is a Kimco preferred share. A symbol Massive has no "
            "data for gives no rows.\n"
            "- An options, index, forex, or crypto symbol needs its class's prefix: `O:`, `I:`, "
            "`C:`, or `X:`. A forex or crypto pair joins its two currencies, as `C:EURUSD`, "
            "never `C:EUR-USD` or `C:EUR/USD`, since flat files and streams match only the "
            "joined ticker.\n"
            "- A crypto stream, last trade, or daily open and close reads only pairs on Massive's "
            "crypto ticker list. By default, any other pair gives no rows; "
            "[`unlisted_symbols`](../configuration.md#unlisted_symbols) changes that.\n"
            "- A symbol that is empty, starts or ends with whitespace, holds a control "
            "character, `,`, or `*`, or breaks the rule above can't be sent: in text or a list "
            "it raises `ValueError`. In a table its row is skipped and logged by default, and "
            "[`unsendable_symbols`](../configuration.md#unsendable_symbols) changes that.\n"
            "- To convert another source's tickers, use `dm.canonical_symbol`. For a stock, also "
            "pass the source's convention: `convention=dm.SymbolConvention.YAHOO` converts "
            "Yahoo's `BRK-B` to `BRK.B`."
        ),
        "start": (
            "The first day or moment of the range. Give `start` and `end` as two dates or two "
            "instants.\n"
            "\n"
            '- A date is text such as `"2025-06-02"`, a Python `date`, or a Deephaven '
            "`LocalDate`. It's the day in the market's time zone: New York for stocks, options, "
            "and indices, and UTC for forex and crypto. For futures, it's the trading session "
            "that ends that day.\n"
            "- An instant is a Python `datetime` with a time zone, a Deephaven `Instant`, or "
            'Deephaven time text such as `"2025-06-02T09:30 ET"`.\n'
            "- A dataset whose range is a calendar date, such as an ex-dividend date, takes "
            "dates only."
        ),
        "end": (
            "The last day of the range, included, or the instant the range stops, excluded. Give "
            "it in the same form as `start`. Leave it out for no end. Rows dated "
            "in the future, such as upcoming dividends, then come back too."
        ),
        "date": "One calendar day, in the same forms as a `start` date.",
        "as_of": (
            "Read the data as it stood on this date. Leave it out for Massive's latest data."
        ),
        "sort": (
            "The field to order the rows by, as Massive names it, such as "
            '`"ex_dividend_date"`. Each dataset\'s page lists the fields it takes.'
        ),
        "order": (
            "The direction of the rows: `dm.Order.ASC` or `dm.Order.DESC`. On a dataset with "
            "`sort`, give the two together."
        ),
        "limit": (
            "The most rows to return. Each dataset's page says whether the limit counts each "
            "symbol's rows or all of them. To keep the latest rows, sort newest first, such as "
            '`sort="ex_dividend_date", order=dm.Order.DESC`.'
        ),
        "filters": (
            "Any other parameter Massive documents for the dataset, as a `dict`. Write each "
            "dot in a name as `_`: Massive's `strike_price.gte` is `strike_price_gte`. Each "
            "dataset's page lists the keys it takes, and any other key raises `ValueError`."
        ),
        "adjusted": (
            "`True`, the default, adjusts prices and volumes for splits. `False` gives them as "
            "traded. Live bars and bars from flat files are always as traded."
        ),
        "timespan": (
            "The unit of each bar, a `dm.Timespan` member such as `dm.Timespan.MINUTE` or "
            "`dm.Timespan.DAY`. `SESSION`, a trading session, is for futures only; a futures "
            "`DAY` bar is one session. On a bars dataset, "
            "one-second and one-minute bars with `multiplier=1` also stream. Bars of other "
            "lengths are history only."
        ),
        "multiplier": (
            "How many `timespan` units each bar spans, 1 by default. "
            "`timespan=dm.Timespan.MINUTE, multiplier=5` gives five-minute bars."
        ),
        "venues": (
            "The futures exchanges whose flat files `history()` reads, as a set of "
            "`dm.FuturesVenue` members, such as `{dm.FuturesVenue.CME, dm.FuturesVenue.CBOT}`. "
            "Each exchange's file of a day holds every contract it trades, so naming fewer "
            "exchanges downloads less.\n"
            "\n"
            "Massive refuses the files of an exchange your plan doesn't include. `history()` "
            "reads REST instead when REST can serve the read. When it must read flat files "
            "(`dm.ALL`, or `source=dm.HistorySource.FLAT_FILES`):\n"
            "\n"
            "- Left out, the read tries every exchange and skips each one Massive refuses. It "
            "logs one warning naming them. The table's description names them too. For a "
            "watchlist, each symbol's `Transport` in `m.symbol_reads()` shows them instead.\n"
            "- An exchange you name follows [`refused_venues`](#refused_venues).\n"
            "- If Massive refuses every exchange, the read fails."
        ),
        "refused_venues": (
            "What a flat-file read does when Massive refuses the files of an exchange you name "
            "in [`venues`](#venues). It takes a `dm.OnError`. Left out, it follows "
            "`MassiveConfig.refused_venues`, which is `RAISE` unless you set it.\n"
            "\n"
            "- `RAISE` fails the read with `EntitlementError`, naming the exchange.\n"
            "- `LOG` skips each refused exchange for the whole range, logs one warning naming "
            "them, and lists them in the table's description.\n"
            "- `IGNORE` skips them too, logging only at DEBUG."
        ),
    }
)
"""The shared rule of each argument, by its name in the dataset methods' signatures.

Each value is Markdown for the index to print under the argument's heading: paragraphs, then
any bullet list or table, separated by blank lines, with no heading of its own and no trailing
whitespace. An argument missing here, such as `underlying`, has only its dataset's own line.
"""
