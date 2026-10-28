"""The closed categorical sets of the public API, as `StrEnum`s, and their runtime check.

Every closed set of values that appears in a public signature is an enum defined here. Each member's
value is the exact string Massive puts on the wire, so a member can be sent to the server unchanged:
`Timespan.MINUTE == "minute"`. Signatures accept only the enum, never a plain string, which gives
one spelling per value, tab completion in every IDE and in the Deephaven web console, and typo
safety at type-check time. At run time `_require_enum` turns a plain string into a `TypeError` that
names the member to use; `Timespan("minute")` is the documented way to convert text.
An enum of a library setting that never reaches the wire (`HistorySource`, `SymbolConvention`,
`OnError`, `UnknownSymbols`) takes its member names in lower case as values.

Two further enums are internal and not exported: `Recency` (how fresh a WebSocket host's
data is) and `ChannelFamily` (which host a channel is served from). Both appear as values in
the diagnostics tables, beside the wire package's `Transport`, which names WebSocket, REST, or
flat files once for both sides of the wire boundary.

The eighteen public enums are re-exported by `deephaven_massive/__init__.py`; this module is
private.
"""

from __future__ import annotations

import logging
from enum import StrEnum
from typing import TypeVar

__all__ = [
    "AssetClass",
    "ChannelFamily",
    "ConditionDataType",
    "ContractType",
    "Direction",
    "FuturesVenue",
    "HistorySource",
    "Locale",
    "Market",
    "OnError",
    "Order",
    "PollMode",
    "Recency",
    "SeriesType",
    "Sip",
    "SymbolConvention",
    "TickerType",
    "Timeframe",
    "Timespan",
    "UnknownSymbols",
]

logger = logging.getLogger(__name__)

_E = TypeVar("_E", bound=StrEnum)


class AssetClass(StrEnum):
    """An asset class, named by its WebSocket cluster path.

    Used to pin a WebSocket host (`MassiveConfig.ws_hosts`), in the diagnostics tables, and by
    the reference endpoints that filter by asset class (`m.reference.ticker_types`,
    `m.reference.exchanges`, `m.reference.conditions`). The values are the cluster paths
    (`wss://<host>/stocks`, ...). On the REST reference endpoints `FOREX` is sent as `fx`.

    Example:
        ::

            import deephaven_massive as dm

            cfg = dm.MassiveConfig(ws_hosts={dm.AssetClass.STOCKS: "delayed.massive.com"})
    """

    STOCKS = "stocks"
    OPTIONS = "options"
    FUTURES = "futures"
    INDICES = "indices"
    FOREX = "forex"
    CRYPTO = "crypto"


class Timespan(StrEnum):
    """The unit of a bar or an indicator window.

    For `bars()` the timespan also selects the transport: `SECOND` and `MINUTE` (with
    `multiplier=1`) stream over WebSocket, `DAY` adds the previous-session `fetch()`, and every
    other combination is history only. `SESSION` exists only for futures, whose aggregates
    endpoint has a trading-session unit and no `DAY`.

    Example:
        ::

            import deephaven_massive as dm

            span = dm.Timespan("minute")  # text converts to a member
            assert span is dm.Timespan.MINUTE
    """

    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"
    SESSION = "session"


class PollMode(StrEnum):
    """How a `poll()` table combines successive request cycles.

    - `REPLACE`: a keyed table that holds exactly the latest cycle; keys absent from it are
      deleted, including when the cycle returns no rows.
    - `UPSERT`: a keyed table that adds and updates rows but never deletes.
    - `APPEND`: an append-only table that keeps every row of every cycle.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.PollMode("append") is dm.PollMode.APPEND
    """

    REPLACE = "replace"
    UPSERT = "upsert"
    APPEND = "append"


class HistorySource(StrEnum):
    """Where `history()` reads from.

    - `AUTO`: the library picks flat files or REST from credentials, the dataset, the bar
      shape, publication dates, and the number of symbols.
    - `REST`: the REST history endpoint, paged internally.
    - `FLAT_FILES`: the gzip CSV files on S3 (`files.massive.com`); needs S3 credentials.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.HistorySource("flat_files") is dm.HistorySource.FLAT_FILES
    """

    AUTO = "auto"
    REST = "rest"
    FLAT_FILES = "flat_files"


class FuturesVenue(StrEnum):
    """A futures exchange, used only to pick the futures flat-file prefix.

    Futures streams always use the `/futures` cluster path whatever the venue, so the venue is
    not part of a stream's identity. `venue=None` on a futures dataset reads all four prefixes.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.FuturesVenue("cme") is dm.FuturesVenue.CME
    """

    CME = "cme"
    CBOT = "cbot"
    NYMEX = "nymex"
    COMEX = "comex"


class Direction(StrEnum):
    """Which side of the market `movers()` returns.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            gainers = m.crypto.movers(dm.Direction.GAINERS).poll(60)
            assert dm.Direction("losers") is dm.Direction.LOSERS
    """

    GAINERS = "gainers"
    LOSERS = "losers"


class ContractType(StrEnum):
    """The type of an options contract, as `options.chain` and `options.contracts` filter it.

    Its values are the `contract_type` values both endpoints accept: calls and puts.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            puts = m.options.chain("AAPL", contract_type=dm.ContractType.PUT).fetch()
            assert dm.ContractType("call") is dm.ContractType.CALL
    """

    CALL = "call"
    PUT = "put"


class SeriesType(StrEnum):
    """The bar price an indicator is computed from.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.SeriesType("close") is dm.SeriesType.CLOSE
    """

    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"


class Timeframe(StrEnum):
    """The reporting period of the `m.stocks.financials` statements.

    `ANNUAL` is a fiscal year and `QUARTERLY` a fiscal quarter. `TRAILING_TWELVE_MONTHS` sums
    the four most recent quarters; the income and cash flow statements offer it, while a
    balance sheet, a snapshot at the period's end, has no trailing form.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.Timeframe("annual") is dm.Timeframe.ANNUAL
    """

    ANNUAL = "annual"
    QUARTERLY = "quarterly"
    TRAILING_TWELVE_MONTHS = "trailing_twelve_months"


class Order(StrEnum):
    """The sort direction of a REST list endpoint (the `order` parameter).

    Example:
        ::

            import deephaven_massive as dm

            assert dm.Order("desc") is dm.Order.DESC
    """

    ASC = "asc"
    DESC = "desc"


class Market(StrEnum):
    """The market `m.reference.tickers` filters by.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.Market("otc") is dm.Market.OTC
    """

    STOCKS = "stocks"
    OTC = "otc"
    INDICES = "indices"
    FX = "fx"
    CRYPTO = "crypto"


class TickerType(StrEnum):
    """The type code of a ticker, which `m.reference.tickers` filters by.

    The values are Massive's codes, upper case as on the wire; `m.reference.ticker_types()`
    lists them with their descriptions.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.TickerType("ETF") is dm.TickerType.ETF
    """

    CS = "CS"  # common stock
    ADRC = "ADRC"  # American depositary receipt, common
    ADRP = "ADRP"  # American depositary receipt, preferred
    ADRR = "ADRR"  # American depositary receipt, rights
    UNIT = "UNIT"  # unit
    RIGHT = "RIGHT"  # rights
    PFD = "PFD"  # preferred stock
    FUND = "FUND"  # fund
    SP = "SP"  # structured product
    WARRANT = "WARRANT"  # warrant
    INDEX = "INDEX"  # index
    ETF = "ETF"  # exchange-traded fund
    ETN = "ETN"  # exchange-traded note
    OS = "OS"  # ordinary shares
    GDR = "GDR"  # global depositary receipt
    OTHER = "OTHER"  # other security type
    NYRS = "NYRS"  # New York registry shares
    AGEN = "AGEN"  # agency bond
    EQLK = "EQLK"  # equity-linked bond
    BOND = "BOND"  # corporate bond
    ADRW = "ADRW"  # American depositary receipt, warrants
    BASKET = "BASKET"  # basket
    LT = "LT"  # liquidating trust


class Locale(StrEnum):
    """The locale the reference endpoints filter by.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.Locale("us") is dm.Locale.US
    """

    US = "us"
    GLOBAL = "global"


class ConditionDataType(StrEnum):
    """The kind of tick a condition code applies to (`m.reference.conditions`).

    Example:
        ::

            import deephaven_massive as dm

            assert dm.ConditionDataType("trade") is dm.ConditionDataType.TRADE
    """

    TRADE = "trade"
    BBO = "bbo"
    NBBO = "nbbo"


class Sip(StrEnum):
    """The securities information processor a condition code belongs to.

    The values are upper case because Massive's wire values are.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.Sip("OPRA") is dm.Sip.OPRA
    """

    CTA = "CTA"
    UTP = "UTP"
    OPRA = "OPRA"


class SymbolConvention(StrEnum):
    """A US stock ticker convention another source writes, which `canonical_symbol` converts.

    Massive writes a share class as `.B` (`BRK.B`) and a preferred as a lower-case `p` and its
    series (`KIMpL`), as the CTA's CQS convention does, so CQS text needs no conversion. Each
    member names the exact grammar of one other convention; only plain tickers, share classes,
    and preferreds convert, because Massive documents no spelling for warrants, units, rights,
    or when-issued shares.

    - `YAHOO`: Yahoo Finance; a class is `-B` (`BRK-B`), a preferred `-P` and its series
      (`BAC-PL`).
    - `NASDAQ`: the Nasdaq Integrated Platform symbology (the "NASDAQ Symbol" column of
      `nasdaqtraded.txt`); a class is `.B`, a preferred `-` and its optional series (`KIM-L`).
    - `CMS`: the root, a space, and a suffix, as many broker and FIX systems write it; a class
      is ` B` (`BRK B`), a preferred ` PR` and its optional series (`BAC PRL`).

    Example:
        ::

            import deephaven_massive as dm

            dm.canonical_symbol(
                "BAC-PL", dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO
            )  # 'BACpL'
    """

    YAHOO = "yahoo"
    NASDAQ = "nasdaq"
    CMS = "cms"


class OnError(StrEnum):
    """What `canonicalize_symbols` does with a row it cannot convert.

    - `RAISE`: fail. A static table raises `ValueError` naming the bad values; a refreshing
      table fails when such a row arrives.
    - `NULL`: write a null symbol for that row and log the value at WARNING, a bounded number
      of times, so one typo in a watchlist does not stop it.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.OnError("null") is dm.OnError.NULL
    """

    RAISE = "raise"
    NULL = "null"


class UnknownSymbols(StrEnum):
    """What a session does with a symbol Massive sends in a spelling the library cannot read.

    The setting of `MassiveConfig.unknown_symbols`, and of the `MASSIVE_UNKNOWN_SYMBOLS`
    environment variable (`fail` or `warn`).

    - `FAIL`: fail the request or the stream that received the row, naming the spelling.
    - `WARN`: keep the row with a null `Symbol` and the text in an added `RawSymbol` column,
      and log each spelling once at WARNING (a bounded number of spellings).

    Example:
        ::

            import deephaven_massive as dm

            assert dm.UnknownSymbols("warn") is dm.UnknownSymbols.WARN
    """

    FAIL = "fail"
    WARN = "warn"


class Recency(StrEnum):
    """How fresh the data of a WebSocket host is, as the host decision found it (internal).

    `UNKNOWN` means the evidence did not say (a pinned host, or a futures snapshot without
    timeframe fields); only then may an explicit server verdict re-route the connection.
    """

    REALTIME = "REALTIME"
    DELAYED = "DELAYED"
    UNKNOWN = "UNKNOWN"


class ChannelFamily(StrEnum):
    """Which group of channels a WebSocket connection carries (internal).

    `FMV` (fair market value) is Business-only and always served from `business.massive.com`;
    every other channel is in the `TICK` family, whose host depends on the key's plan.
    """

    TICK = "TICK"
    FMV = "FMV"


def _require_enum(value: object, enum_type: type[_E], name: str) -> _E:
    """Return `value` if it is a member of `enum_type`; otherwise raise a `TypeError`.

    Every enum parameter of the public API goes through this check at query construction.
    Because a `StrEnum` member is also a `str`, the test is `isinstance(value, enum_type)`, so
    a plain string such as `"minute"` is rejected even when it equals a member's value. The
    message names the parameter, the enum, and the member to use: the member whose value or
    name matches the string (ignoring case) when there is one, otherwise the first member.

    Args:
        value: The argument as the caller passed it.
        enum_type: The enum the parameter requires.
        name: The parameter's name, used in the message.

    Returns:
        `value`, narrowed to `enum_type`.

    Raises:
        TypeError: If `value` is not a member of `enum_type`, for example
            `TypeError("timespan must be dm.Timespan, e.g. dm.Timespan.MINUTE")`.
    """
    if isinstance(value, enum_type):
        return value
    members = list(enum_type)
    example = members[0]
    if isinstance(value, str):
        wanted = value.casefold()
        for member in members:
            if wanted in {member.value.casefold(), member.name.casefold()}:
                example = member
                break
    qualified = f"dm.{enum_type.__name__}"
    raise TypeError(f"{name} must be {qualified}, e.g. {qualified}.{example.name}")
